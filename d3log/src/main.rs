// temporary main that runs the trivial matrix multiply example. this
// will become supervisor main

mod batch;
mod child;
mod dispatch;
mod json_framer;
mod tcp_network;
mod transact;

use crate::{batch::Batch, child::start_children, typedefs::matrix::Matrix};
use differential_datalog::ddval::DDValConvert;
use differential_datalog::D3logLocationId;
use std::sync::Arc;

use mm_ddlog::*;
use rustop::opts;
use std::str;

type Node = D3logLocationId;
use std::time::{SystemTime, UNIX_EPOCH};

pub trait Transport {
    // since most of these errors are async, we're adopting a general
    // policy for the moment of making all errors async and reported out
    // of band
    fn send(&self, nid: Node, b: Batch);
}

// doesn't belong in main. but we'd like a monotonic wallclock
// to sequence system events. Also - it would be nice if ddlog
// had some basic time functions (format)

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

type Port = Arc<(dyn Transport + Send + Sync)>;

use std::convert::TryFrom;
fn matrix(mat: Vec<Vec<u64>>) -> Result<Batch, String> {
    let mut b = Batch::new();
    let relid = Relations::try_from("matrix::Matrix")
        .map_err(|_| format!("Unknown relation {}", "Matrix"))?;

    for (j, c) in mat.iter().enumerate() {
        for (i, v) in c.iter().enumerate() {
            b.insert(
                relid as usize,
                Matrix {
                    i: i as u32,
                    j: j as u32,
                    v: (*v) as u32,
                }
                .into_ddvalue(),
                1,
            );
        }
    }
    Ok(b)
}

fn main() {
    let (args, _) = opts! {
        synopsis "D3log multiprocess test harness.";
        auto_shorts false;
        // --nodes or -n
        opt nodes:usize=1, short:'n', desc:"The number of worker processes. Default is 1.";
    }
    .parse_or_exit();

    start_children(
        args.nodes,
        match matrix(vec![vec![1, 2, 3], vec![7, 12, 19], vec![5, 3, 1]]) {
            Ok(x) => x,
            Err(x) => {
                println!("matrix construction error {}", x);
                return;
            }
        },
    )
    .expect("start children failed");
}
