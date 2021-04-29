use crate::{
    json_framer::JsonFramer, tcp_network::ArcTcpNetwork, transact::ArcTransactionManager, Batch,
    Node, Transport,
};

use tokio::{io::AsyncReadExt, io::AsyncWriteExt, runtime::Runtime, spawn};
use tokio_fd::AsyncFd;

use std::convert::TryFrom;
use std::io::{Error, ErrorKind};

type Fd = std::os::unix::io::RawFd;
use nix::unistd::*;

const CHILD_INPUT_FD: Fd = 3;
const CHILD_OUTPUT_FD: Fd = 4;

// xxx - this should follow trait Network
// really we probably want to have a forwarding table
// xxx child read input

struct FileDescriptor {
    input: Fd,
    output: Fd,
}

impl Transport for FileDescriptor {
    fn send(&self, _nid: Node, b: Batch) -> Result<(), std::io::Error> {
        // there doesn't seem to be a nice unwrap for error rewriting - i guess a macro
        let js = match serde_json::to_string(&b) {
            Ok(x) => x,
            Err(_x) => return Err(Error::new(ErrorKind::Other, "oh no!")),
        };
        let mut pin = AsyncFd::try_from(self.output)?;
        tokio::spawn(async move { pin.write_all(js.as_bytes()).await });
        Ok(())
    }
}

// this is the self-framed json input from stdout of one of my children
// would rather hide h
// there is an event barrier here, but its pretty lacking. the first
// batch to arrive is assumed to contain everything needed.  so once
// we get that from everyone, we kick off evaluation

async fn read_output(t: ArcTransactionManager, f: Box<Fd>) -> Result<(), std::io::Error> {
    let mut jf = JsonFramer::new();
    let mut pin = AsyncFd::try_from(*f)?;
    let mut buffer = [0; 64];
    loop {
        let res = pin.read(&mut buffer).await?;
        for i in jf.append(&buffer[0..res])? {
            let v: Batch = serde_json::from_str(&i)?;
            println!("eval{}", v);
            match t.clone().eval(v).await {
                Ok(b) => {
                    // println!("eval completez {}", b);
                    // shouldn't exit on eval error
                    t.forward(b).await?; // not really dude
                }

                Err(x) => {
                    println!("erraru rivalu!");
                    return Err(Error::new(ErrorKind::Other, x));
                }
            };
        }
    }
}

pub fn start_node(f: Vec<Fd>) {
    let rt = Runtime::new().unwrap();
    let _eg = rt.enter();
    let tm = ArcTransactionManager::new();
    let tn = ArcTcpNetwork::new(tm.clone());
    // set tm.management!

    rt.block_on(async move {
        for i in f {
            let tmclone = tm.clone();
            spawn(async move {
                read_output(tmclone, Box::new(i))
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

pub fn make_child() -> Result<(Fd, Fd), nix::Error> {
    let (in_r, in_w) = pipe().unwrap();
    let (out_r, out_w) = pipe().unwrap();

    match unsafe { fork()? } {
        // child was here before .. we'll want that for kills from here, i guess we
        // could close stdin
        ForkResult::Parent { .. } => Ok((in_w, out_r)),

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

// parameterize network..with i guess a factory!
// i would kind of prefer to kick off init from inside ddlog, but
// odaat

//Shouldn't this function return a Result that tells you whether or
// not it succeeded in starting all children? If so that'd make some
// of the inner logic a lot cleaner

pub fn start_children(n: usize, _init: Batch) -> Result<(), std::io::Error> {
    let mut children_in = Vec::<Fd>::new();
    let mut children_out = Vec::<Fd>::new();

    // 0 is us
    for _i in 1..n {
        match make_child() {
            Ok((to, from)) => {
                children_in.push(from);
                children_out.push(to);
            }
            Err(x) => return Err(Error::new(ErrorKind::Other, format!("oh no! {}", x))),
        }
    }
    // wire up nid 0s address..no one is listening to my stdin!
    start_node(children_in);
    Ok(())
}
