pub mod broadcast;
pub mod ddvalue_batch;
mod dispatch;
pub mod dred;
pub mod error;
mod forwarder;
mod json_framer;
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
    fn error(&self, text: Record, line: Record, filename: Record, functionname: Record);
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
        writeln!(f, "Batch [").unwrap();
        match self {
            Batch::Value(b) => b.fmt(f),
            Batch::Rec(b) => b.fmt(f),
        }
        .unwrap();
        writeln!(f, "\n]\n")
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
    forwarder: Port,
    dispatch: Port,
    //    queue: Arc<Mutex<VecDeque<Batch>>>,
    s: Arc<Mutex<Sender<Batch>>>,
    r: Arc<Mutex<Receiver<Batch>>>,
}

impl Transport for EvalPort {
    fn send(&self, b: Batch) {
        self.dispatch.send(b.clone());
        async_error!(
            self.eval.clone(),
            self.s.lock().expect("lock").send(b.clone())
        );

        loop {
            match self.r.lock().expect("lock").try_recv() {
                Ok(x) => {
                    let out = async_error!(self.eval.clone(), self.eval.eval(x.clone()));
                    self.forwarder.send(out.clone());
                }
                Err(_) => return,
            }
        }
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

impl Instance {
    pub fn new(
        rt: Arc<Runtime>,
        new_evaluator: Arc<dyn Fn(Node, Port) -> Result<(Evaluator, Batch), Error> + Send + Sync>,
        uuid: u128,
    ) -> Result<Arc<Instance>, Error> {
        let broadcast = Broadcast::new(uuid);
        let (eval, init_batch) = new_evaluator(uuid, broadcast.clone())?;
        let dispatch = Arc::new(Dispatch::new(eval.clone()));
        let (esend, erecv) = channel();
        let forwarder = Forwarder::new(eval.clone(), dispatch.clone(), broadcast.clone());

        let eval_port = Arc::new(EvalPort {
            forwarder: forwarder.clone(),
            dispatch: dispatch.clone(),
            eval: eval.clone(),
            s: Arc::new(Mutex::new(esend)),
            r: Arc::new(Mutex::new(erecv)),
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

        // self registration leads to trouble
        //    forwarder.clone().register(uuid, instance.eval_port.clone());

        broadcast.clone().subscribe(instance.eval_port.clone());

        ThreadInstance::new(instance.clone(), new_evaluator)?;

        broadcast
            .clone()
            .subscribe(Arc::new(DebugPort { eval: eval.clone() }));
        instance
            .eval_port
            .send(fact!(d3_application::Myself, me => uuid.into_record()));

        Ok(instance)
    }
}
