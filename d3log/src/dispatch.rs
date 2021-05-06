// a temporary shim to direct updates to functions as a placeholder for sinks. route
// sub-batches to ports. ideally this would be some kind of general query which includes
// the relation id and the envelope. This would replace Transact.forward, which seems
// correct

use crate::{batch::Batch, Node, Port, Transport};
use differential_datalog::program::RelId;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::{Error, ErrorKind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use d3_supervisor_ddlog::Relations; // bad linkage

pub struct Dispatch {
    count: AtomicUsize,
    handlers: Arc<Mutex<HashMap<RelId, Vec<(u64, Port)>>>>,
}

impl Transport for Dispatch {
    // having send() take a nid makes this a bit strange for internal plumbing. if we close
    // the nid under Port - then need to expose it in the evelope. we're going to assign
    // a number to each otuput.. why does that seem wrong

    fn send(&self, _nid: Node, b: Batch) {
        let mut output = HashMap::<u64, (Port, Batch)>::new();

        for (rel, v, weight) in b {
            if let Some(ports) = self.handlers.lock().expect("lock").get(&rel) {
                for (i, p) in ports {
                    output
                        .entry(*i)
                        .or_insert_with(|| (p.clone(), Batch::new()))
                        .1
                        .insert(rel, v.clone(), weight);
                }
            }
        }
        for (_, (p, b)) in output {
            p.send(0, b);
        }
    }
}

// probably need to wrap in an arcmutex
impl Dispatch {
    pub fn new() -> Dispatch {
        Dispatch {
            handlers: Arc::new(Mutex::new(HashMap::new())),
            count: AtomicUsize::new(0),
        }
    }

    // deregstration? return a handle?
    pub fn register(self, relation_name: &str, p: Port) -> Result<(), std::io::Error> {
        let id = self.count.fetch_add(1, Ordering::SeqCst);

        let relid = match Relations::try_from(relation_name) {
            Ok(x) => x as usize,
            Err(_x) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("bad relation {}", relation_name),
                ))
            }
        };

        self.handlers
            .lock()
            .expect("lock")
            .entry(relid)
            .or_insert_with(|| Vec::new())
            .push((id as u64, p));
        Ok(())
    }
}
