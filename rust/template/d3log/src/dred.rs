// Initial version of DRED
// TODO:
// 1) Enumerate the performance bottlenecks on the current DRED
// 2) Write tests (Both in module and global)

use crate::{Batch, DDValueBatch, Evaluator, Port, Transport};
use std::cell::RefCell;
use std::sync::Arc;

struct Dred {
    eval: Evaluator,
    out_port: Port,
    accumulator: RefCell<DDValueBatch>,
}

unsafe impl Sync for Dred {}

impl Dred {
    fn new(eval: Evaluator, out_port: Port) -> Port {
        Arc::new(Dred {
            eval,
            out_port,
            accumulator: RefCell::new(DDValueBatch::new()),
        })
    }

    fn close(&self) {
        let batch = Batch::Value(self.accumulator.borrow().clone());
        self.out_port.send(batch);
    }

    fn inspect_acc(&self) {
        for (r, v, w) in &*self.accumulator.borrow() {
            println!("{}", w);
        }
    }
}

impl Transport for Dred {
    fn send(&self, b: Batch) {
        for (r, v, mut w) in &DDValueBatch::from(&*self.eval, b.clone()).expect("iterator") {
            // Invert the weight and add to the accumulator
            w = -w;
            self.accumulator.borrow_mut().insert(r, v, w);
        }
        // Send it out to the output port
        self.out_port.send(b);
    }
}

#[cfg(test)]
mod tests {
    // Write all the DRED tests here!
    use super::*;
    use crate::{error::Error, EvaluatorTrait, Node, RecordBatch};
    use differential_datalog::{ddval::DDValue, program::Update, record::Record};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct D3Test {}

    impl EvaluatorTrait for D3Test {
        fn ddvalue_from_record(&self, id: usize, r: Record) -> Result<DDValue, Error> {
            Err(Error::new("not implemented!".to_string()))
        }

        //  can demux on record batch and call the record interface instead of translating - is that
        // desirable in some way? record in record out?
        fn eval(&self, input: Batch) -> Result<Batch, Error> {
            Ok(Batch::Value(DDValueBatch::new()))
        }

        fn id_from_relation_name(&self, s: String) -> Result<usize, Error> {
            Ok(0)
        }

        fn localize(&self, rel: usize, v: DDValue) -> Option<(Node, usize, DDValue)> {
            None
        }

        // doesn't belong here. but we'd like a monotonic wallclock
        // to sequence system events. Also - it would be nice if ddlog
        // had some basic time functions (format)
        fn now(&self) -> u64 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as u64
        }

        fn record_from_ddvalue(&self, d: DDValue) -> Result<Record, Error> {
            Err(Error::new("not implemented!".to_string()))
        }

        fn relation_name_from_id(&self, id: usize) -> Result<String, Error> {
            Err(Error::new("unknown relation id".to_string()))
        }

        // xxx - actually we want to parameterize on format, not on internal representation
        fn serialize_batch(&self, b: DDValueBatch) -> Result<Vec<u8>, Error> {
            Ok("none".as_bytes().to_vec())
        }

        fn deserialize_batch(&self, s: Vec<u8>) -> Result<DDValueBatch, Error> {
            Ok(DDValueBatch::new())
        }
    }

    struct inport {}
    struct outport {}
    impl Transport for inport {
        fn send(&self, b: Batch) {
            println!("Inport send");
        }
    }

    impl Transport for outport {
        fn send(&self, b: Batch) {
            println!("outport send");
        }
    }
    #[test]
    fn create_dred() {
        println!("hello test");
        let t = D3Test {};
        let inp = inport {};

        let d = Dred::new(Arc::new(t), Arc::new(inp));
    }
}
