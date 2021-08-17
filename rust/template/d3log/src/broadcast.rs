// global broadcast for metadata facts. this includes 'split horizon' as a temporary
// fix for simple pairwise loops. This will need an additional distributed coordination mechanism in
// order to maintain a consistent spanning tree (and a strategy for avoiding storms for temporariliy
// inconsistent topologies)

// broadcast manages an accumulator to seed new peers (its really a union, they replay against each other)
// and attempts to make an atomic swtich between stored and live updates (?). so its really a broadcast across
// time and space

use crate::{Batch, Error, Evaluator, FactSet, Node, Port, Transport, ValueSet};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct Broadcast {
    id: Node, // just for debugging
    accumulator: Arc<Mutex<ValueSet>>,
    count: Arc<AtomicUsize>,
    ports: Arc<Mutex<Vec<(Port, usize)>>>,
}

impl Broadcast {
    // accumualtor gets allocated outside and passed through because of some
    // bringup dependencies (eval <- broadcast, AccumulatePort <- eval)
    pub fn new(id: Node) -> Arc<Broadcast> {
        Arc::new(Broadcast {
            id,
            accumulator: Arc::new(Mutex::new(ValueSet::new())),
            count: Arc::new(AtomicUsize::new(0)),
            ports: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

struct AccumulatePort {
    eval: Evaluator,
    accumulator: Arc<Mutex<ValueSet>>,
}

impl Transport for AccumulatePort {
    fn send(&self, b: Batch) {
        for (r, f, w) in &(ValueSet::from(&(*self.eval), b.data).expect("iterator")) {
            self.accumulator.lock().expect("lock").insert(r, f, w);
        }
    }
}

pub trait PubSub {
    fn subscribe(self, p: Port) -> Port;
    // xxx - we shouldn't be referring to the narrow object type Broadcast here,
    // but its not clear where else to wire this
    fn couple(self, b: Arc<Broadcast>) -> Result<(), Error>;
    // this one too :)
    fn setup_accumulator(self, e: Evaluator);
}

impl PubSub for Arc<Broadcast> {
    fn setup_accumulator(self, e: Evaluator) {
        self.clone().subscribe(Arc::new(AccumulatePort {
            accumulator: self.accumulator.clone(),
            eval: e.clone(),
        }));
    }

    fn subscribe(self, p: Port) -> Port {
        let index = self.count.fetch_add(1, Ordering::Acquire);
        let mut ports = self.ports.lock().expect("lock ports");
        ports.push((p.clone(), index));

        p.clone().send(Batch::new(
            FactSet::Empty(),
            FactSet::Value(self.accumulator.lock().expect("lock").clone()),
        ));

        Arc::new(Ingress {
            broadcast: self.clone(),
            index,
        })
    }

    fn couple(self, b: Arc<Broadcast>) -> Result<(), Error> {
        let (p, batch) = {
            let index = self.count.fetch_add(1, Ordering::Acquire);
            let p1 = Arc::new(Ingress {
                broadcast: self.clone(),
                index,
            });

            //            let p1 = Trace::new(b.clone().id, "up".to_string(), p1);
            let p2 = b.subscribe(p1);
            //            let p2 = Trace::new(b.clone().id, "down".to_string(), p2);

            let mut ports = self.ports.lock().expect("lock");
            ports.push((p2.clone(), index));
            (
                p2.clone(),
                Batch::new(
                    FactSet::Empty(),
                    FactSet::Value(self.accumulator.lock().expect("lock").clone()),
                ),
            )
        };
        p.send(batch);
        Ok(())
    }
}

impl Transport for Broadcast {
    fn send(&self, b: Batch) {
        // We clone this map to have a read-only copy, else, we'd open up the possiblity of a
        // deadlock, if this `send` forms a cycle.
        let ports = { &*self.ports.lock().expect("lock").clone() };
        for (port, _) in ports {
            port.send(b.clone())
        }
    }
}

// an Ingress port couples an output with an input, to avoid redistributing
// facts back to the source. proper cycle detection will require spanning tree
pub struct Ingress {
    index: usize,
    broadcast: Arc<Broadcast>,
}

impl Ingress {
    pub fn remove(&mut self, _p: Port) {}
}

impl Transport for Ingress {
    fn send(&self, b: Batch) {
        let ports = { &*self.broadcast.ports.lock().expect("lock").clone() };
        for (port, index) in ports {
            if *index != self.index {
                port.send(b.clone())
            }
        }
    }
}
