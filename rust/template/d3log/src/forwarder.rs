// fowarder takes a batch and uses the hddlog interface to extract those facts with
// locality annotations, groups them by destination, and calls the registered send
// method for that destination

use crate::{
    async_error, function, send_error, Batch, Dispatch, Error, Evaluator, FactSet, Node, Port,
    RecordSet, Transport, ValueSet,
};
use differential_datalog::record::*;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

struct ForwardingEntryHandler {
    eval: Evaluator,
    forwarder: Arc<Forwarder>,
}

fn scan(e: Evaluator, f: FactSet, s: String) -> Option<Record> {
    for (r, f, _w) in &RecordSet::from(e.clone(), f.clone()) {
        if r == s {
            return Some(f);
        }
    }
    None
}

impl Transport for ForwardingEntryHandler {
    fn send(&self, b: Batch) {
        // reconcile
        for (_r, f, _w) in &RecordSet::from(self.eval.clone(), b.data) {
            let target = async_error!(
                self.eval,
                u128::from_record(f.get_struct_field("target").expect("target"))
            );
            let z = f.get_struct_field("intermediate").expect("intermediate");
            let intermediate = async_error!(self.eval, u128::from_record(z));
            if intermediate != self.eval.clone().myself() {
                let e = self.forwarder.lookup(intermediate);
                let mut e2 = { e.lock().expect("lock") };
                match &e2.port {
                    Some(p) => self.forwarder.register(target, p.clone()),
                    None => {
                        e2.registrations.push_back(target);
                    }
                }
            }
        }
    }
}

struct Under {
    forwarder: Arc<Forwarder>,
    eval: Evaluator,
    up: Port,
}

impl Transport for Under {
    fn send(&self, b: Batch) {
        let f2 = self.forwarder.clone();
        scan(self.eval.clone(), b.clone().meta, "destination".to_string());

        if let Some(f) = scan(self.eval.clone(), b.clone().meta, "destination".to_string()) {
            if let Some(d) = f.get_struct_field("uuid") {
                let n = async_error!(self.eval, u128::from_record(d));
                if n != self.eval.clone().myself() {
                    async_error!(self.eval, f2.clone().out(n, b.clone()));
                    return;
                }
            }
        }
        self.up.send(b);
    }
}

#[derive(Clone)]
struct Entry {
    port: Option<Port>,
    batches: VecDeque<Batch>,
    registrations: VecDeque<Node>,
}

pub struct Forwarder {
    eval: Evaluator,
    // xxx - reader writer lock
    fib: Arc<Mutex<HashMap<Node, Arc<Mutex<Entry>>>>>,
}

impl Forwarder {
    pub fn new(
        eval: Evaluator,
        dispatch: Arc<Dispatch>,
        eval_port: Port,
    ) -> (Port, Arc<Forwarder>) {
        let f = Arc::new(Forwarder {
            eval: eval.clone(),
            fib: Arc::new(Mutex::new(HashMap::new())),
        });
        dispatch
            .clone()
            .register(
                "d3_application::Forward",
                Arc::new(ForwardingEntryHandler {
                    eval: eval.clone(),
                    forwarder: f.clone(),
                }),
            )
            .expect("register");
        let inp = Arc::new(Under {
            eval,
            forwarder: f.clone(),
            up: eval_port,
        });
        (inp, f.clone())
    }

    fn lookup(&self, n: Node) -> Arc<Mutex<Entry>> {
        self.fib
            .lock()
            .expect("lock")
            .entry(n)
            .or_insert_with(|| {
                Arc::new(Mutex::new(Entry {
                    port: None,
                    batches: VecDeque::new(),
                    registrations: VecDeque::new(),
                }))
            })
            .clone()
    }

    pub fn register(&self, n: Node, p: Port) {
        // overwrite warning?

        let entry = self.lookup(n);
        {
            entry.lock().expect("lock").port = Some(p.clone());
        }

        while let Some(b) = { entry.lock().expect("lock").batches.pop_front() } {
            p.clone().send(b);
        }
        while let Some(r) = { entry.lock().expect("lock").registrations.pop_front() } {
            self.register(r, p.clone());
        }
    }

    pub fn out(&self, nid: Node, b: Batch) -> Result<(), Error> {
        let p = {
            match self.lookup(nid).lock() {
                Ok(mut x) => match &x.port {
                    Some(x) => x.clone(),
                    None => {
                        println!("queuing {} {}", nid, b);
                        x.batches.push_front(b);
                        return Ok(());
                    }
                },
                Err(_) => panic!("lock"),
            }
        };

        let b = if scan(self.eval.clone(), b.clone().meta, "destination".to_string()).is_none() {
            let mut r = RecordSet::from(self.eval.clone(), b.clone().meta);
            r.insert(
                "destination".to_string(),
                Record::NamedStruct(
                    Cow::from("destination".to_string()),
                    vec![(Cow::from("uuid".to_string()), nid.into_record())],
                ),
                1,
            );
            Batch::new(FactSet::Record(r), b.data)
        } else {
            b
        };
        p.send(b);
        Ok(())
    }
}

use std::ops::DerefMut;

impl Transport for Forwarder {
    fn send(&self, b: Batch) {
        let mut output = HashMap::<Node, Box<ValueSet>>::new();

        for (rel, v, weight) in &(ValueSet::from(&(*self.eval), b.clone().data).expect("iterator"))
        {
            if let Some((loc_id, in_rel, inner_val)) = self.eval.localize(rel, v.clone()) {
                output
                    .entry(loc_id)
                    .or_insert_with(|| Box::new(ValueSet::new()))
                    .deref_mut()
                    .insert(in_rel, inner_val, weight);
            }
        }
        for (nid, nb) in output.drain() {
            self.out(nid, Batch::new(b.meta.clone(), FactSet::Value(*nb)));
        }
    }
}
