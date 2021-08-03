// functions for managing forked children and ports on pipes
//
// this is all quite rendundant with the various existing rust process
// wrappers, especially tokio::process however, we need to be inside
// the child fork before exec to add the management descriptors,
// otherwise most of this could go

use {
    crate::{
        async_error,
        broadcast::Broadcast,
        broadcast::PubSub,
        error::Error,
        fact,
        json_framer::JsonFramer,
        record_batch::{deserialize_record_batch, serialize_record_batch, RecordBatch},
        Batch, Evaluator, Port, Transport,
        Node,
        function,
        Instance,
        send_error,
    },
    differential_datalog::record::*,
    nix::unistd::*,
    std::{
        borrow::Cow,
        collections::HashMap,
        convert::TryFrom,
        ffi::CString,
        sync::{Arc, Mutex},
    },
    tokio::{io::AsyncReadExt, io::AsyncWriteExt, spawn},
    tokio_fd::AsyncFd,
};

type Fd = std::os::unix::io::RawFd;

pub const MANAGEMENT_INPUT_FD: Fd = 3;
pub const MANAGEMENT_OUTPUT_FD: Fd = 4;

#[derive(Clone)]
pub struct FileDescriptorPort {
    pub fd: Fd,
    pub eval: Evaluator,
    pub management: Port,
}

impl Transport for FileDescriptorPort {
    fn send(&self, b: Batch) {
        let js = async_error!(
            self.eval.clone(),
            serialize_record_batch(RecordBatch::from(self.eval.clone(), b))
        );
        // keep this around, would you?
        let mut pin = async_error!(self.eval.clone(), AsyncFd::try_from(self.fd));
        spawn(async move { pin.write_all(&js).await });
    }
}

async fn read_output<F>(fd: Fd, mut callback: F) -> Result<(), Error>
where
    F: FnMut(&[u8]),
{
    let mut pin = AsyncFd::try_from(fd)?;
    let mut buffer = [0; 1024];
    loop {
        let res = pin.read(&mut buffer).await?;
        callback(&buffer[0..res]);
    }
}

#[derive(Clone)]
pub struct ProcessInstance {
    instance: Arc<Instance>,
    new_evaluator: Arc<dyn Fn(Node, Port) -> Result<(Evaluator, Batch), Error> + Send + Sync>,
    manager: Arc<Mutex<HashMap<Node, (isize, Option<Pid>)>>>,
    processes: Arc<Mutex<HashMap<Pid, Arc<Mutex<Child>>>>>,
}

impl Transport for ProcessInstance {
    fn send(&self, b: Batch) {
        // we think the dispatcher has given only facts from our relation
        // this should be from, is that not so?
        for (_, p, weight) in &RecordBatch::from(self.instance.eval.clone(), b) {
            let uuid_record = p.get_struct_field("id").unwrap();
            let uuid = async_error!(self.instance.eval.clone(), Node::from_record(uuid_record));

            let mut manager = self.manager.lock().expect("lock");
            let value = manager
                .entry(uuid)
                .or_insert_with(|| (weight, None));
            let w = value.0;
            let child_pid = value.1;
            if w > 0 {
                // Start instance if one is not already present
                if child_pid.is_none() {
                    self.make_child(p).expect("fork failure");
                }
            } else if w <= 0 {
                // what about other values of weight?
                // kill if we can find the uuid..i guess and if the total weight is 1
                if let Some(pid) = child_pid {
                    // TODO: send SIGKILL to pid and wait for the child?
                    // Update the value to None
                }
            }
        }
    }
}

struct Child {
    uuid: u128,
    eval: Evaluator,
    #[allow(dead_code)]
    pid: Pid,
    management: Port,
    #[allow(dead_code)]
    management_to_child: Port,
}

impl Child {
    pub fn new(uuid: u128, pid: Pid, instance: Arc<Instance>, management_in_w: Fd) -> Self {
        Self {
            eval: instance.eval.clone(),
            uuid,
            pid,
            management: instance.broadcast.clone(),
            management_to_child: Arc::new(FileDescriptorPort {
                eval: instance.eval.clone(),
                management: instance.broadcast.clone(),
                fd: management_in_w,
            }),
        }
    }

    pub fn report_status(&self) {
        self.management.send(fact!(
            d3_application::InstanceStatus,
            id => self.uuid.into_record(),
            memory_bytes => 0.into_record(),
            threads => 0.into_record(),
            time => self.eval.now().into_record()));
    }
}

impl ProcessInstance {
    pub fn new(
        instance: Arc<Instance>,
        new_evaluator: Arc<dyn Fn(Node, Port) -> Result<(Evaluator, Batch), Error> + Send + Sync>,
    ) -> Result<(), Error> {
        // allocate wait thread
        instance.dispatch.clone().register(
            "d3_application::Process",
            Arc::new(ProcessInstance {
                instance: instance.clone(),
                new_evaluator: new_evaluator.clone(),
                manager: Arc::new(Mutex::new(HashMap::new())),
                processes: Arc::new(Mutex::new(HashMap::new())),
            }),
        )
    }

    // arrange to listen to management channels if they exist
    // this should manage a filesystem resident cache of executable images,
    // potnetially addressed with a uuid or a url

    // since this is really an async error maybe deliver it here
    pub fn make_child(&self, process: Record) -> Result<(), Error> {
        // ideally we wouldn't allocate the management pair
        // unless we were actually going to use it..really we should have
        // two input relations, one for d3log programs and one for other things

        let (management_in_r, management_in_w) = pipe().unwrap();
        let (management_out_r, management_out_w) = pipe().unwrap();

        let (standard_in_r, _standard_in_w) = pipe().unwrap();
        let (standard_out_r, standard_out_w) = pipe().unwrap();
        let (standard_err_r, standard_err_w) = pipe().unwrap();

        let id = process.get_struct_field("id").unwrap();

        match unsafe { fork() } {
            Ok(ForkResult::Parent { child }) => {
                let child_obj = Arc::new(Mutex::new(Child::new(
                    u128::from_record(id).unwrap(),
                    child,
                    self.instance.clone(),
                    management_in_w,
                )));

                if process.get_struct_field("management").is_some() {
                    let c2 = child_obj.clone();
                    let management_clone = self.instance.broadcast.clone();
                    let rt = self.instance.rt.clone();
                    let eval_clone = self.instance.eval.clone();
                    rt.spawn(async move {
                        let mut jf = JsonFramer::new();
                        let mut first = true;
                        let management_clone = management_clone.clone();
                        let management_clone2 = management_clone.clone();

                        let sh_management =
                            management_clone.subscribe(
                                Arc::new(FileDescriptorPort {
                                management: management_clone2.clone(),
                                eval: eval_clone.clone(),
                                fd: management_out_r,
                            }));

                        let a = management_clone2.clone();
                        let eval_clone2 = eval_clone.clone();
                        async_error!(
                            eval_clone.clone(),
                            read_output(management_out_r, move |b: &[u8]| {
                                let management_clone = management_clone2.clone();
                                for i in async_error!(eval_clone2.clone(), jf.append(b)) {
                                    let v = async_error!(
                                        eval_clone2.clone(),
                                        deserialize_record_batch(i)
                                    );

                                    sh_management.clone().send(v);

                                    // XXX assume the child needs to announce something before we recognize it
                                    // as started. assume its tcp address information so we dont need to
                                    // implement the join
                                    if first {
                                        c2.clone().lock().expect("lock").report_status();
                                        first = false;
                                    }
                                }
                            })
                            .await
                        );
                    });
                }

                self.instance.rt.spawn(async move {
                    read_output(standard_out_r, |b: &[u8]| {
                        // utf8 framing issues! - feed this into a relation
                        print!("child {} {}", child, std::str::from_utf8(b).expect(""));
                    })
                    .await
                });

                self.instance.rt.spawn(async move {
                    read_output(standard_err_r, |b: &[u8]| {
                        println!("child error {}", std::str::from_utf8(b).expect(""));
                    })
                    .await
                });
                self.processes
                    .lock()
                    .expect("lock")
                    .insert(child, child_obj);
                Ok(())
            }

            Ok(ForkResult::Child) => {
                // xxx - should be if true..but if some process has some junk fds it doesn't really notice
                if process.get_struct_field("management").is_some() {
                    dup2(management_out_w, MANAGEMENT_OUTPUT_FD)?;
                    dup2(management_in_r, MANAGEMENT_INPUT_FD)?;
                }

                dup2(standard_in_r, 0)?;
                dup2(standard_out_w, 1)?;
                dup2(standard_err_w, 2)?;

                if let Some(exec) = process.get_struct_field("executable") {
                    // FIXME: Temporary fix. this should be fixed ddlog-wide
                    let exec = exec.to_string().replace("\"", "");
                    if let Some(id) = process.get_struct_field("id") {
                        let path = CString::new(exec.clone()).expect("CString::new failed");
                        let arg0 = CString::new(exec).expect("CString::new failed");
                        let u = format!("uuid={}", id);
                        let env1 = CString::new(u).expect("CString::new failed");
                        execve(&path, &[arg0], &[env1])?;
                    }
                } else {
                    return Err(Error::new("malformed process record".to_string()));
                }

                Ok(())
            }
            Err(_) => {
                panic!("Fork failed!");
            }
        }
    }
}
