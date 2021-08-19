// Initial version of DRED
// TODO:
// 1) Enumerate the performance bottlenecks on the current DRED
// 2) Write tests (Both in module and global)

use crate::{basefact, Batch, Evaluator, FactSet, Port, RecordSet, Transport, ValueSet};
use differential_datalog::record::IntoRecord;
use differential_datalog::record::Record;
use std::borrow::Cow;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Dred {
    eval: Evaluator,
    out_port: Port,
    accumulator: Arc<Mutex<ValueSet>>,
    demo_accumulator: Arc<Mutex<Vec<Batch>>>,
}

unsafe impl Sync for Dred {}

impl Dred {
    pub fn new(eval: Evaluator, out_port: Port) -> (Self, Port) {
        let d = Dred {
            eval,
            out_port,
            accumulator: Arc::new(Mutex::new(ValueSet::new())),
            demo_accumulator: Arc::new(Mutex::new(Vec::new())),
        };

        (d.clone(), Arc::new(d))
    }

    pub fn close(&self) {
        let batch = Batch::new(
            FactSet::Empty(),
            FactSet::Value(self.accumulator.lock().expect("lock").clone()),
        );
        self.out_port.send(batch);
    }

    // xxx - split this out into metadred.rs
    pub fn close_with_metadata(&self) {
        for batch in self.demo_accumulator.lock().expect("lock").iter() {
            let mut value_set = ValueSet::new();
            for (r, v, mut w) in &ValueSet::from(&*self.eval, batch.clone().data).expect("iterator")
            {
                w = -w;
                value_set.insert(r, v, w);
            }

            let m = batch.clone().meta;
            let mut r = RecordSet::from(self.eval.clone(), m);
            r.insert(
                "d3_supervisor::Trace".to_string(),
                basefact!(d3_supervisor::Trace, on => true.into_record()),
                1,
            );
            let out_batch = Batch::new(FactSet::Record(r), FactSet::Value(value_set));
            self.out_port.send(out_batch.clone());
        }
    }
}

impl Transport for Dred {
    fn send(&self, b: Batch) {
        for (r, v, mut w) in &ValueSet::from(&*self.eval, b.clone().data).expect("iterator") {
            // Invert the weight and add to the accumulator
            w = -w;
            self.accumulator.lock().expect("lock").insert(r, v, w);
        }
        // demo accumulator contains the metadata batch as well
        self.demo_accumulator.lock().expect("lock").push(b.clone());
        // Send it out to the output port
        self.out_port.send(b);
    }
}
