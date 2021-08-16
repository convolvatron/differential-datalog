// functions for managing forked children and ports on pipes
//
// this is all quite rendundant with the various existing rust process
// wrappers, especially tokio::process however, we need to be inside
// the child fork before exec to add the management descriptors,
// otherwise most of this could go

use {
    crate::{
        async_error, broadcast::PubSub, error::Error, fact, function, json_framer::JsonFramer,
        recfact, send_error, Batch, Evaluator, FactSet, Instance, Node, Port, RecordSet, Transport,
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

#[derive(Clone)]
pub struct Child {
    uuid: u128,
    instance: Arc<Instance>,
    pid: Option<Pid>,
    w: isize,
    management: bool,
    last_announcement: Option<RecordSet>,
    executable: String,
}

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
    manager: Arc<Mutex<HashMap<Node, Child>>>,
}

impl Transport for ProcessInstance {
    fn send(&self, b: Batch) {
        for (_, p, weight) in &RecordSet::from(self.instance.eval.clone(), b.data) {
            let uuid_record = p.get_struct_field("id").unwrap();
            let uuid = async_error!(self.instance.eval.clone(), Node::from_record(uuid_record));

            let mut manager = self.manager.lock().expect("lock");
            // just use the child structure

            let mut child = if let Some(exec) = p.get_struct_field("path") {
                // FIXME: Temporary fix. we are using some UI value to_string that adds quotes
                let executable = exec.to_string().replace("\"", "");
                let management = p.get_struct_field("management").is_some(); // isn't this a boolean from dd?
                manager.entry(uuid).or_insert_with(|| Child {
                    instance: self.instance.clone(),
                    uuid,
                    management,
                    pid: None,
                    executable,
                    w: 0,
                    last_announcement: None,
                })
            } else {
                panic!("process record format");
            };

            child.w += weight;

            if child.w > 0 {
                // Start instance if one is not already present .. what if one is present .. someone changed exec?
                if child.pid.is_none() {
                    child.start().expect("fork failure");
                }
            } else {
                if let Some(pid) = child.pid {
                    // wait for the child?
                    kill(pid, SIGKILL).expect("kill failed");
                    child.pid = None;
                    // lock?
                    child.report_status();
                }
            }
        }
    }
}

impl Child {
    // both inputs in a parent child relation into batch sends
    fn reader(instance: Arc<Instance>, infd: Fd, outfd: Fd) {
        let i2 = instance.clone();

        instance.clone().rt.spawn(async move {
            let outp = Arc::new(FileDescriptorPort {
                instance: i2.clone(),
                fd: Arc::new(AsyncMutex::new(
                    AsyncFd::try_from(outfd).expect("async fd failed"),
                )),
            });

            let mut jf = JsonFramer::new();
            let management_input = instance.broadcast.clone().subscribe(outp.clone());
            let i3 = i2.clone();
            async_error!(
                i2.clone().eval.clone(),
                read_output(infd, move |b: &[u8]| {
                    let i4 = i3.clone();
                    for i in async_error!(i3.clone().eval.clone(), jf.append(b)) {
                        let b: Batch = async_error!(i4.clone().eval.clone(), Batch::deserialize(i));
                        management_input.clone().send(b);
                    }
                })
                .await
            );
        });
    }

    pub fn read_management(instance: Arc<Instance>) {
        Child::reader(instance, MANAGEMENT_INPUT_FD, MANAGEMENT_OUTPUT_FD);
    }

    pub fn read_output_fact(self, fd: Fd, kind: String) {
        let i2 = self.instance.clone();
        self.instance.clone().rt.spawn(async move {
            let i3 = i2.clone();
            let i4 = i2.clone();
            async_error!(
                i3.eval.clone(),
                read_output(fd, move |b: &[u8]| {
                    self.instance.broadcast.clone().send(fact!(d3_application::TextStream,
                                                               t => i4.clone().eval.now().into_record(),
                                                               kind => kind.clone().into_record(),
                                                               id => self.uuid.clone().into_record(),
                                                               body => std::str::from_utf8(b).expect("").into_record()));
                })
                .await
            );
        });
    }

    pub fn report_status(&mut self) {
        if let Some(la) = self.last_announcement.clone() {
            self.instance
                .broadcast
                .send(Batch::new(FactSet::Empty(), FactSet::Record(la.negate())));
        }
        if self.pid.is_some() {
            let f = recfact!(
                d3_application::InstanceStatus,
                id => self.uuid.into_record(),
                memory_bytes => 0.into_record(),
                threads => 0.into_record(),
                time => self.instance.eval.now().into_record());
            self.last_announcement = Some(f.clone());
            self.instance
                .broadcast
                .send(Batch::new(FactSet::Empty(), FactSet::Record(f.clone())));
        }
    }

    // this should manage a filesystem resident cache of executable images,
    // potnetially addressed with a uuid or a url

    pub fn start(&mut self) -> Result<(), Error> {
        // ideally we wouldn't allocate the management pair
        // unless we were actually going to use it..really we should have
        // two input relations, one for d3log programs and one for other things

        let (management_in_r, management_in_w) = pipe().unwrap();
        let (management_out_r, management_out_w) = pipe().unwrap();

        let (standard_in_r, _standard_in_w) = pipe().unwrap();
        let (standard_out_r, standard_out_w) = pipe().unwrap();
        let (standard_err_r, standard_err_w) = pipe().unwrap();

        let s2 = self.clone();
        match unsafe { fork() } {
            Ok(ForkResult::Parent { child }) => {
                if self.management {
                    // instance status!
                    Child::reader(s2.instance.clone(), management_out_r, management_in_w);
                }
                self.pid = Some(child);
                s2.clone()
                    .read_output_fact(standard_out_r, "stdout".to_string());
                s2.clone()
                    .read_output_fact(standard_err_r, "stderr".to_string());

                // this used to be conditional on the first mgmt msg from the child - which was both
                // better and worse, but ultimately hard to wire up
                self.report_status();
                Ok(())
            }

            Ok(ForkResult::Child) => {
                if self.management {
                    dup2(management_out_w, MANAGEMENT_OUTPUT_FD)?;
                    dup2(management_in_r, MANAGEMENT_INPUT_FD)?;
                }

                dup2(standard_in_r, 0)?;
                dup2(standard_out_w, 1)?;
                dup2(standard_err_w, 2)?;

                let e = CString::new(self.executable.clone()).expect("CString::new failed");
                let env1 =
                    CString::new(format!("uuid={}", self.uuid)).expect("CString::new failed");
                let env2 = CString::new("RUST_BACKTRACE=1").expect("CString::new failed");
                execve(&e.clone(), &[e.clone()], &[env1, env2])?;
                //
                panic!("exec failed?");
            }
            Err(_) => {
                panic!("Fork failed!");
            }
        }
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
            }),
        )
    }
}
