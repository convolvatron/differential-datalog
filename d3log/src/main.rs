// temporary main that runs the trivial matrix multiply example. this
// will become supervisor main

mod child;

// library
mod batch;
mod dispatch;
mod json_framer;
mod tcp_network;
mod transact;

use crate::{batch::Batch, child::ProcessManager, dispatch::Dispatch};
use differential_datalog::D3logLocationId;
use std::sync::Arc;

//use rustop::opts;
//use std::str;

use std::time::{SystemTime, UNIX_EPOCH};

type Node = D3logLocationId;

pub trait Transport {
    // since most of these errors are async, we're adopting a general
    // policy for the moment of making all errors async and reported out
    // of band
    fn send(&self, nid: Node, b: Batch);
}

// doesn't belong here. but we'd like a monotonic wallclock
// to sequence system events. Also - it would be nice if ddlog
// had some basic time functions (format)
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

type Port = Arc<(dyn Transport + Send + Sync)>;

fn main() {
    //    let (args, _) = opts! {
    //        synopsis "D3log multiprocess test harness.";
    //        auto_shorts false;
    //        // --nodes or -n
    //        opt nodes:usize=1, short:'n', desc:"The number of worker processes. Default is 1.";
    //    }
    //    .parse_or_exit();

    let d = Dispatch::new();
    ProcessManager::new(d);
}
