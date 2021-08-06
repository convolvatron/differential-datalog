// Initial version of DRED
// TODO:
// 1) Enumerate the performance bottlenecks on the current DRED
// 2) Write tests (Both in module and global)

use crate::{Batch, Evaluator, FactSet, Port, Transport, ValueSet};
use std::cell::RefCell;
use std::sync::Arc;

#[derive(Clone)]
pub struct Dred {
    eval: Evaluator,
    out_port: Port,
    accumulator: RefCell<ValueSet>,
}

unsafe impl Sync for Dred {}

impl Dred {
    pub fn new(eval: Evaluator, out_port: Port) -> (Self, Port) {
        let d = Dred {
            eval,
            out_port,
            accumulator: RefCell::new(ValueSet::new()),
        };

        (d.clone(), Arc::new(d))
    }

    pub fn close(&self) {
        let batch = Batch::new(
            FactSet::Empty(),
            FactSet::Value(self.accumulator.borrow().clone()),
        );
        self.out_port.send(batch);
    }

    fn inspect_acc(&self) {
        for (_r, _v, w) in &*self.accumulator.borrow() {
            println!("{}", w);
        }
    }
}

impl Transport for Dred {
    fn send(&self, b: Batch) {
        for (r, v, mut w) in &ValueSet::from(&*self.eval, b.clone().data).expect("iterator") {
            // Invert the weight and add to the accumulator
            w = -w;
            self.accumulator.borrow_mut().insert(r, v, w);
        }
        // Send it out to the output port
        self.out_port.send(b);
    }
}
