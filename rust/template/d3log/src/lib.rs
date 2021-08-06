pub mod broadcast;
pub mod datfile;
pub mod ddvalue_batch;
mod dispatch;
pub mod dred;
pub mod error;
mod forwarder;
pub mod json_framer;
pub mod process;
pub mod record_batch;
pub mod tcp_network;
mod thread_instance;

use core::fmt;
use differential_datalog::{ddval::DDValue, record::*, D3logLocationId};
use std::borrow::Cow;
use std::fmt::Display;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

use crate::{
    broadcast::{Broadcast, PubSub},
    ddvalue_batch::DDValueBatch,
    dispatch::Dispatch,
    dred::Dred,
    error::Error,
    forwarder::Forwarder,
    process::ProcessInstance,
    record_batch::RecordBatch,
    tcp_network::tcp_bind,
    thread_instance::ThreadInstance,
};

pub type Node = D3logLocationId;

pub trait EvaluatorTrait {
    fn ddvalue_from_record(&self, id: String, r: Record) -> Result<DDValue, Error>;
    fn eval(&self, input: Batch) -> Result<Batch, Error>;
    fn id_from_relation_name(&self, s: String) -> Result<usize, Error>;
    fn localize(&self, rel: usize, v: DDValue) -> Option<(Node, usize, DDValue)>;
    fn now(&self) -> u64;
    fn myself(&self) -> Node;
    fn error(&self, text: Record, line: Record, filename: Record, functionname: Record, uuid: Record);
    fn record_from_ddvalue(&self, d: DDValue) -> Result<Record, Error>;
    fn relation_name_from_id(&self, id: usize) -> Result<String, Error>;

    // these is ddvalue/relationid specific
    fn serialize_batch(&self, b: DDValueBatch) -> Result<Vec<u8>, Error>;
    fn deserialize_batch(&self, s: Vec<u8>) -> Result<DDValueBatch, Error>;
}

#[derive(Clone)]
pub struct Instance {
    pub uuid: Node,
    pub broadcast: Arc<Broadcast>,
    pub init_batch: Batch,
    pub eval_port: Port,
    pub eval: Evaluator,
    pub dispatch: Arc<Dispatch>,
    pub forwarder: Arc<Forwarder>,
    pub rt: Arc<tokio::runtime::Runtime>,
}

pub type Evaluator = Arc<(dyn EvaluatorTrait + Send + Sync)>;

#[derive(Clone)]
pub enum Batch {
    Value(DDValueBatch),
    Rec(RecordBatch),
}

impl Display for Batch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Batch::Value(b) => b.fmt(f),
            Batch::Rec(b) => b.fmt(f),
        }
    }
}

pub trait Transport {
    // since most of these errors are async, we're adopting a general
    // policy for the moment of making all errors async and reported out
    // of band.

    // should really be type parametric shouldn't it?
    fn send(&self, b: Batch);
}

pub type Port = Arc<(dyn Transport + Send + Sync)>;

// shouldn't evaluator just implement Transport?
struct EvalPort {
    eval: Evaluator,
    dispatch: Port,
    s: Arc<Mutex<Sender<Batch>>>,
}

impl Transport for EvalPort {
    fn send(&self, b: Batch) {
        for (_r, v, _w) in &RecordBatch::from(self.eval.clone(), b.clone()) {
            println!("uuid {} v {}", self.eval.clone().myself(), v); 
            if let Some(text) = v.get_struct_field("text") {
                println!("Error @ node {} -> {} @ {}:{}:{}",
                         v.get_struct_field("uuid").or(Some(&0xbad_ffff_u128.into_record())).unwrap(),
                         text,
                         v.get_struct_field("filename").or(Some(&"unknown.rs".into_record())).unwrap(),
                         v.get_struct_field("functionname").or(Some(&"unknown_fn".into_record())).unwrap(),
                         v.get_struct_field("line").or(Some(&0u64.into_record())).unwrap(),
                );
            }
        }

        self.dispatch.send(b.clone());
        async_error!(
            self.eval.clone(),
            self.s.lock().expect("lock").send(b.clone())
        );
    }
}

struct Trace {
    uuid: Node,
    head: String,
    p: Port,
}

impl Trace {
    fn new(uuid: Node, head: String, p: Port) -> Port {
        Arc::new(Trace {
            uuid,
            head,
            p: p.clone(),
        })
    }
}

impl Transport for Trace {
    fn send(&self, b: Batch) {
        println!("{} {} {} ", self.uuid, self.head, b);
        self.p.clone().send(b);
    }
}

struct DebugPort {
    eval: Evaluator,
}

impl Transport for DebugPort {
    fn send(&self, b: Batch) {
        for (_r, f, w) in &RecordBatch::from(self.eval.clone(), b) {
            println!("{} {} {}", self.eval.clone().myself(), f, w);
        }
    }
}

struct AccumulatePort {
    eval: Evaluator,
    accumulator: Arc<Mutex<DDValueBatch>>,
}

impl Transport for AccumulatePort {
    fn send(&self, b: Batch) {
        for (r, f, w) in &DDValueBatch::from(&(*self.eval), b).expect("iterator") {
            self.accumulator.lock().expect("lock").insert(r, f, w);
        }
    }
}

impl Instance {
    pub fn new(
        rt: Arc<Runtime>,
        new_evaluator: Arc<dyn Fn(Node, Port) -> Result<(Evaluator, Batch), Error> + Send + Sync>,
        uuid: u128,
    ) -> Result<Arc<Instance>, Error> {
        let accumulator = Arc::new(Mutex::new(DDValueBatch::new()));
        let broadcast = Broadcast::new(uuid, accumulator.clone());
        let (eval, init_batch) = new_evaluator(uuid, broadcast.clone())?;
        let dispatch = Arc::new(Dispatch::new(eval.clone()));
        let (esend, erecv) = channel();
        let forwarder = Forwarder::new(eval.clone(), dispatch.clone(), broadcast.clone());

        broadcast.clone().subscribe(Arc::new(AccumulatePort {
            accumulator,
            eval: eval.clone(),
        }));

        let eval_port = Arc::new(EvalPort {
            dispatch: dispatch.clone(),
            eval: eval.clone(),
            s: Arc::new(Mutex::new(esend)),
        });

        let instance = Arc::new(Instance {
            uuid,
            broadcast: broadcast.clone(),
            eval: eval.clone(),
            eval_port: eval_port.clone(),
            init_batch: init_batch.clone(),
            dispatch: dispatch.clone(),
            forwarder: forwarder.clone(),
            rt: rt.clone(),
        });

        let instance_clone = instance.clone();
        rt.spawn(async move {
            loop {
                let x = async_error!(instance_clone.eval, erecv.recv());
                let out = async_error!(
                    instance_clone.eval.clone(),
                    instance_clone.eval.eval(x.clone())
                );
                instance_clone.dispatch.send(out.clone());
                instance_clone.forwarder.send(out.clone());
            }
        });

        // self registration leads to trouble
        //    forwarder.clone().register(uuid, instance.eval_port.clone());

        broadcast.clone().subscribe(instance.eval_port.clone());

        ThreadInstance::new(instance.clone(), new_evaluator.clone())?;

        ProcessInstance::new(instance.clone(), new_evaluator.clone())?;
        //        broadcast
        //            .clone()
        //            .subscribe(Arc::new(DebugPort { eval: eval.clone() }));

        instance
            .eval_port
            .send(fact!(d3_application::Myself, me => uuid.into_record()));

        Ok(instance)
    }
}
