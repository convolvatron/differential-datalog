// functions for managing forked children.
// - start_node(), which is the general d3log runtime start, and probably doesn't belong here
// - implementation of Port over pipes
//
// this needs to get broken apart or shifted a little since the children in the future
// will be other ddlog executables

use crate::{
    dispatch::Dispatch, json_framer::JsonFramer, tcp_network::ArcTcpNetwork,
    transact::ArcTransactionManager, Batch, Node, Port, Transport,
};

use d3_supervisor_ddlog::typedefs::Process; /*ProcessStatus*/

use std::sync::{Arc, Mutex};
use tokio::{io::AsyncReadExt, io::AsyncWriteExt, runtime::Runtime, spawn};
use tokio_fd::AsyncFd;

use rand::Rng;
use std::collections::HashMap;
use std::convert::TryFrom;
// use std::io::{Error, ErrorKind};

type Fd = std::os::unix::io::RawFd;
use nix::unistd::*;

const CHILD_INPUT_FD: Fd = 3;
const CHILD_OUTPUT_FD: Fd = 4;

// xxx - this should follow trait Network
// really we probably want to have a forwarding table
// xxx child read input

#[derive(Clone)]
struct FileDescriptor {
    input: Fd,
    output: Fd,
}

impl Transport for FileDescriptor {
    fn send(&self, _nid: Node, b: Batch) {
        // there doesn't seem to be a nice unwrap for error rewriting - i guess a macro
        let js = match serde_json::to_string(&b) {
            Ok(x) => x,
            Err(x) => panic!("encoding error {}", x),
        };
        let mut pin = AsyncFd::try_from(self.output).expect("asynch");
        tokio::spawn(async move { pin.write_all(js.as_bytes()).await });
    }
}

// this is the self-framed json input from stdout of one of my children
// would rather hide h
//
// there is an event barrier here, but its pretty lacking. the first
// batch to arrive is assumed to contain everything needed.  so once
// we get that from everyone, we kick off evaluation

// should take a closure..we'd also like to assert its stdout - maybe as an entirety?
// naw..incremental
async fn read_json_batch_output(
    t: ArcTransactionManager,
    f: Box<Fd>,
) -> Result<(), std::io::Error> {
    let mut jf = JsonFramer::new();
    let mut pin = AsyncFd::try_from(*f)?;
    let mut buffer = [0; 64];
    loop {
        let res = pin.read(&mut buffer).await?;
        for i in jf.append(&buffer[0..res])? {
            let v: Batch = serde_json::from_str(&i)?;
            // shouldn't exit on eval error
            t.forward(t.clone().eval(v)?)?
        }
    }
}

pub fn start_node() {
    let rt = Runtime::new().unwrap();
    let _eg = rt.enter();

    // if we care about locating persistent data on this node across reboots,
    // or using quorum this uuid will have to be stable
    let uuid = u128::from_be_bytes(rand::thread_rng().gen::<[u8; 16]>());

    // this should be allocated from outside, primary has this
    // routed to a broadcast (and inputs should be routed to that broadcast)
    let m: Port = Arc::new(FileDescriptor {
        input: CHILD_INPUT_FD,
        output: CHILD_OUTPUT_FD,
    });

    let am = Arc::new(m);
    let tm = ArcTransactionManager::new(uuid, am.clone());
    let tn = Arc::new(ArcTcpNetwork::new(uuid, am.clone(), tm.clone()));
    tm.clone().set_network(tn.clone());
    println!("start");
    rt.block_on(async move {
        for i in f {
            let tmclone = tm.clone();
            spawn(async move {
                read_json_batch_output(tmclone, Box::new(i))
                    .await
                    .unwrap_or_else(|error| {
                        panic!("err {}", error);
                    })
            });
        }

        // return address through here?
        match tn.bind().await {
            Ok(_) => (),
            Err(x) => {
                panic!("bind failure {}", x);
            }
        };
    });
}

#[derive(Clone)]
pub struct ProcessManager {
    processes: Arc<Mutex<HashMap<u128, u32>>>,
}

impl Transport for ProcessManager {
    fn send(&self, _nid: Node, b: Batch) {
        for (r, v, w) in b {
            let p = v as Process;
            // what about other values of w?
            if w == -1 {
                // kill if we can find the uuid
            }
            if w == 1 {
                let pid = self.make_child(v as Process).expect("fork failure");
                self.processes.lock().expect("lock").insert(v.uuid, pid);
            }
        }
    }
}

impl ProcessManager {
    pub fn new(d: Dispatch) -> ProcessManager {
        let p = ProcessManager {
            processes: Arc::new(Mutex::new(HashMap::new())),
        };
        d.register("Process", Arc::new(p.clone()));
        p
    }

    // arrange to listen to management channels if they exist
    // this should manage a filesystem resident cache of executable images,
    // potnetially addressed with a uuid or a url

    // in the earlier model, we deliberately refrained from
    // starting the multithreaded tokio runtime until after
    // we'd forked all the children. lets see if we can
    // can fork this executable w/o running exec if tokio has been
    // started
    pub fn make_child(v: Process) -> Result<u32, nix::Error> {
        let (in_r, in_w) = pipe().unwrap();
        let (out_r, out_w) = pipe().unwrap();

        match unsafe { fork()? } {
            ForkResult::Parent { pid } => {
                let f = FileDescriptor {
                    input: out_r,
                    output: in_w,
                };
                // this goes to
                // register i/o here
            }

            // maybe it makes sense to run the management json over different
            // file descriptors so we can use stdout for ad-hoc debugging
            // without confusing the json parser
            ForkResult::Child => {
                dup2(out_w, CHILD_OUTPUT_FD)?;
                dup2(in_r, CHILD_INPUT_FD)?;
                start_node(vec![0]);
                Ok((in_w, out_r))
            }
        }
    }
}
