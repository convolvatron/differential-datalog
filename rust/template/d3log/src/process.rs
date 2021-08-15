// functions for managing forked children and ports on pipes
//
// this is all quite rendundant with the various existing rust process
// wrappers, especially tokio::process however, we need to be inside
// the child fork before exec to add the management descriptors,
// otherwise most of this could go

use {
    crate::{
        async_error, broadcast::PubSub, error::Error, fact, function, json_framer::JsonFramer,
        send_error, Batch, Evaluator, FactSet, Instance, Node, Port, RecordSet, Transport,
    },
    differential_datalog::record::*,
    nix::sys::signal::*,
    nix::unistd::*,
    std::{
        borrow::Cow,
        collections::HashMap,
        convert::TryFrom,
        ffi::CString,
        sync::{Arc, Mutex},
    },
    tokio::sync::Mutex as AsyncMutex,
    tokio::{io::AsyncReadExt, io::AsyncWriteExt},
    tokio_fd::AsyncFd,
};

type Fd = std::os::unix::io::RawFd;

pub const MANAGEMENT_INPUT_FD: Fd = 3;
pub const MANAGEMENT_OUTPUT_FD: Fd = 4;

#[derive(Clone)]
pub struct FileDescriptorPort {
    pub fd: Arc<AsyncMutex<AsyncFd>>,
    pub instance: Arc<Instance>,
}

impl Transport for FileDescriptorPort {
    fn send(&self, b: Batch) {
        let afd = self.fd.clone();
        let e = self.instance.eval.clone();

        // no vals today, check back tomorrow
        let b = Batch::new(
            FactSet::Record(RecordSet::from(e.clone(), b.clone().meta)),
            FactSet::Record(RecordSet::from(e.clone(), b.clone().data)),
        );
        let js = async_error!(
            self.instance.eval.clone(),
            serde_json::to_string(&b.clone())
        );
        self.instance.rt.spawn(async move {
            async_error!(e, afd.lock().await.write_all(&js.as_bytes()).await);
        });
    }
}

pub async fn read_output<F>(fd: Fd, mut callback: F) -> Result<(), Error>
where
    F: FnMut(&[u8]),
{
    let mut pin = AsyncFd::try_from(fd).expect("async fd failed");
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
        for (_, p, weight) in &RecordSet::from(self.instance.eval.clone(), b.data) {
            let uuid_record = p.get_struct_field("id").unwrap();
            let uuid = async_error!(self.instance.eval.clone(), Node::from_record(uuid_record));

            let mut manager = self.manager.lock().expect("lock");
            let value = manager.entry(uuid).or_insert_with(|| (0, None));
            // XXX: Consolidate weights manually?
            let w = value.0 + weight;
            let child_pid = value.1;
            value.0 = w;
            if w > 0 {
                // Start instance if one is not already present
                if child_pid.is_none() {
                    let pid = self.make_child(p).expect("fork failure");
                    value.1 = Some(pid);
                }
            } else if w <= 0 {
                // xxxx retract InstanceStatus!!
                // what about other values of weight?
                // kill if we can find the uuid..i guess and if the total weight is 1
                if let Some(pid) = child_pid {
                    // send SIGKILL to pid and wait for the child?
                    kill(pid, SIGKILL).expect("kill failed");
                    // Update the value to None
                    value.1 = None;
                }
            }
        }
    }
}

pub struct Child {
    uuid: u128,
    eval: Evaluator,
    management: Port,
}

impl Child {
    pub fn read_management(instance: Arc<Instance>) {
        let instance_clone = instance.clone();
        let instance_clone2 = instance.clone();
        instance.rt.block_on(async move {
            let management_from_parent =
                instance_clone
                    .broadcast
                    .clone()
                    .subscribe(Arc::new(FileDescriptorPort {
                        instance: instance_clone.clone(),
                        fd: Arc::new(AsyncMutex::new(
                            AsyncFd::try_from(MANAGEMENT_OUTPUT_FD).expect("asyncfd"),
                        )),
                    }));

            instance_clone.rt.spawn(async {
                let instance_clone3 = instance_clone2.clone();
                let mut jf = JsonFramer::new();
                async_error!(
                    instance_clone3.clone().eval.clone(),
                    read_output(MANAGEMENT_INPUT_FD, move |b: &[u8]| {
                        let x = async_error!(instance_clone2.clone().eval.clone(), jf.append(b));
                        for i in x {
                            let v = async_error!(
                                instance_clone2.clone().eval.clone(),
                                Batch::deserialize(i)
                            );
                            management_from_parent.clone().send(v);
                        }
                    })
                    .await
                );
            });
        });
    }

    pub fn new(uuid: u128, _pid: Pid, instance: Arc<Instance>) -> Self {
        Self {
            eval: instance.eval.clone(),
            uuid,
            management: instance.broadcast.clone(),
        }
    }

    // fix nega vs posi
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
    pub fn make_child(&self, process: Record) -> Result<Pid, Error> {
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
                )));

                let i2 = self.instance.clone();
                if process.get_struct_field("management").is_some() {
                    let c2 = child_obj.clone();
                    let i3 = i2.clone();
                    i2.clone().rt.spawn(async move {
                        let mut jf = JsonFramer::new();
                        let mut first = true;

                        let management_to_child = Arc::new(FileDescriptorPort {
                            instance: i3.clone(),
                            fd: Arc::new(AsyncMutex::new(
                                AsyncFd::try_from(management_in_w).expect("async fd failed"),
                            )),
                        });

                        let sh_management =
                            i3.broadcast.clone().subscribe(management_to_child.clone());

                        let i4 = i3.clone();
                        async_error!(
                            i3.eval.clone(),
                            read_output(management_out_r, move |b: &[u8]| {
                                for i in async_error!(i4.eval.clone(), jf.append(b)) {
                                    let s = async_error!(i4.eval.clone(), std::str::from_utf8(&i));
                                    let b: Batch =
                                        async_error!(i4.eval.clone(), serde_json::from_str(s));
                                    sh_management.clone().send(b);
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

                let i2 = self.instance.clone();
                let i3 = self.instance.clone();
                let id2 = id.clone();
                self.instance.clone().rt.spawn(async move {
                    read_output(standard_out_r, |b: &[u8]| {
                        i2.clone().broadcast.send(fact!(d3_application::TextStream,
                                                        t => i3.clone().eval.now().into_record(),
                                                        kind => "stdout".into_record(),
                                                        id => id2.clone(),
                                                        body => std::str::from_utf8(b).expect("").into_record()));
                    }).await
                });

                let i2 = self.instance.clone();
                let i3 = self.instance.clone();
                let id2 = id.clone();
                i3.clone().rt.spawn(async move {
                    read_output(standard_err_r, |b: &[u8]| {
                        i2.clone().broadcast.send(fact!(d3_application::TextStream,
                                                        t => i3.clone().eval.now().into_record(),
                                                        kind => "stderr".into_record(),
                                                        id => id2.clone(),
                                                        body => std::str::from_utf8(b).expect("").into_record()));
                    }).await
                });
                self.processes
                    .lock()
                    .expect("lock")
                    .insert(child, child_obj);
                Ok(child)
            }

            Ok(ForkResult::Child) => {
                if process.get_struct_field("management").is_some() {
                    dup2(management_out_w, MANAGEMENT_OUTPUT_FD)?;
                    dup2(management_in_r, MANAGEMENT_INPUT_FD)?;
                }

                dup2(standard_in_r, 0)?;
                dup2(standard_out_w, 1)?;
                dup2(standard_err_w, 2)?;

                if let Some(exec) = process.get_struct_field("path") {
                    // FIXME: Temporary fix. this should be fixed ddlog-wide
                    let exec = exec.to_string().replace("\"", "");
                    if let Some(id) = process.get_struct_field("id") {
                        let path = CString::new(exec.clone()).expect("CString::new failed");
                        let arg0 = CString::new(exec).expect("CString::new failed");
                        let u = format!("uuid={}", id);
                        let env1 = CString::new(u).expect("CString::new failed");
                        let env2 = CString::new("RUST_BACKTRACE=1").expect("CString::new failed");
                        execve(&path, &[arg0], &[env1, env2])?;
                    }
                } else {
                    return Err(Error::new("malformed process record".to_string()));
                }
                // XXX: should never reach
                panic!("exec failed?");
            }
            Err(_) => {
                panic!("Fork failed!");
            }
        }
    }
}
