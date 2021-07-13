// Initial version of DRED
// TODO:
// 1) Enumerate the performance bottlenecks on the current DRED
// 2) Write tests (Both in module and global)

use crate::{
    Batch, DDValueBatch, Evaluator, Port, RecordBatch, Transport,
};


struct Dred {
    eval: Evaluator,
    out_port : Port,
    accumulator : DDValueBatch,
}

impl Dred {
    fn new(eval: Evaluator, out_port: Port) -> Port {
        Dred {
            eval,
            out_port,
        }
    }

    fn close(&self) {
        // TODO: Encap accumulator in a Batch
        out_port.send(accumulator);
    }
}

impl Transport for Dred {
    fn send(&self, b: Batch) {
        for (r, v, w) in &DDValueBatch::from(self.e.clone(), b) {
            // Invert the weight and add to the accumulator
            w = -w;
            accumulator.insert(r, v, w);
        }
        // Send it out to the output port
        self.out_port.send(b);
    }
}

mod tests {
    // Write all the DRED tests here!
}
