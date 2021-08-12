pub mod batch;
pub mod broadcast;
mod dispatch;
pub mod display;
pub mod dred;
pub mod error;
pub mod factset;
mod forwarder;
pub mod json_framer;
pub mod process;
pub mod record_set;
pub mod tcp_network;
mod thread_instance;
pub mod value_set;

use differential_datalog::{ddval::DDValue, record::*, D3logLocationId};
use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{channel, Sender};

use crate::{
    batch::Batch,
    broadcast::{Broadcast, PubSub},
    dispatch::Dispatch,
    dred::Dred,
    error::Error,
    factset::FactSet,
    forwarder::Forwarder,
    process::ProcessInstance,
    record_set::RecordSet,
    tcp_network::tcp_bind,
    thread_instance::ThreadInstance,
    value_set::ValueSet,
};

pub type Node = D3logLocationId;

pub trait EvaluatorTrait {
    fn ddvalue_from_record(&self, id: String, r: Record) -> Result<DDValue, Error>;
    fn eval(&self, input: Batch) -> Result<Batch, Error>;
    fn id_from_relation_name(&self, s: String) -> Result<usize, Error>;
    fn localize(&self, rel: usize, v: DDValue) -> Option<(Node, usize, DDValue)>;
    fn now(&self) -> u64;
    fn myself(&self) -> Node;
    fn error(
        &self,
        text: Record,
        line: Record,
        filename: Record,
        functionname: Record,
        uuid: Record,
    );
    fn record_from_ddvalue(&self, d: DDValue) -> Result<Record, Error>;
    fn relation_name_from_id(&self, id: usize) -> Result<String, Error>;
}

#[derive(Clone)]
pub struct Instance {
    pub uuid: Node,
    pub broadcast: Arc<Broadcast>,
    pub init_batch: Batch,
    pub eval_port: Port,
    pub under: Port,
    pub eval: Evaluator,
    pub dispatch: Arc<Dispatch>,
    pub forwarder: Arc<Forwarder>,
    pub rt: Arc<tokio::runtime::Runtime>,
}

pub type Evaluator = Arc<(dyn EvaluatorTrait + Send + Sync)>;

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
    rt: Arc<tokio::runtime::Runtime>,
    eval: Evaluator,
    dispatch: Port,
    s: Sender<Batch>,
}

impl Transport for EvalPort {
    fn send(&self, b: Batch) {
        self.dispatch.send(b.clone());
        let eclone = self.eval.clone();
        let sclone = self.s.clone();
        self.rt.spawn(async move {
            async_error!(eclone, sclone.send(b.clone()).await);
        });
    }
}

pub struct Trace {
    uuid: Node,
    head: String,
    p: Port,
}

impl Trace {
    #[allow(dead_code)]
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

pub struct DebugPort {
    pub eval: Evaluator,
}

impl Transport for DebugPort {
    fn send(&self, b: Batch) {
        // print meta
        for (_r, f, w) in &RecordSet::from(self.eval.clone(), b.data) {
            println!("Stdout: {} {}", f, w);
        }
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

impl Instance {
    pub fn new(
        rt: Arc<Runtime>,
        new_evaluator: Arc<dyn Fn(Node, Port) -> Result<(Evaluator, Batch), Error> + Send + Sync>,
        uuid: u128,
    ) -> Result<Arc<Instance>, Error> {
        let accumulator = Arc::new(Mutex::new(ValueSet::new()));
        let broadcast = Broadcast::new(uuid, accumulator.clone());
        let (eval, init_batch) = new_evaluator(uuid, broadcast.clone())?;
        let dispatch = Arc::new(Dispatch::new(eval.clone()));
        let (esend, mut erecv) = channel(10);
        let eval_port = Arc::new(EvalPort {
            rt: rt.clone(),
            dispatch: dispatch.clone(),
            eval: eval.clone(),
            s: esend,
        });
        let (under, forwarder) = Forwarder::new(eval.clone(), dispatch.clone(), eval_port.clone());

        broadcast.clone().subscribe(Arc::new(AccumulatePort {
            accumulator,
            eval: eval.clone(),
        }));

        let instance = Arc::new(Instance {
            uuid,
            broadcast: broadcast.clone(),
            eval: eval.clone(),
            eval_port: eval_port.clone(),
            init_batch: init_batch.clone(),
            dispatch: dispatch.clone(),
            under,
            forwarder: forwarder.clone(),
            rt: rt.clone(),
        });

        let instance_clone = instance.clone();
        rt.spawn(async move {
            loop {
                // this will spin on close
                if let Some(x) = erecv.recv().await {
                    println!("eval in {}", x.clone().format(instance_clone.eval.clone()));
                    let out = async_error!(
                        instance_clone.eval.clone(),
                        instance_clone.eval.eval(x.clone())
                    );
                    println!(
                        "eval out {}",
                        out.clone().format(instance_clone.eval.clone())
                    );
                    instance_clone.dispatch.send(out.clone());
                    instance_clone.forwarder.send(out.clone());
                }
            }
        });

        // self registration leads to trouble
        //    forwarder.clone().register(uuid, instance.eval_port.clone());

        broadcast.clone().subscribe(instance.eval_port.clone());

        ThreadInstance::new(instance.clone(), new_evaluator.clone())?;
        ProcessInstance::new(instance.clone(), new_evaluator.clone())?;

        instance
            .eval_port
            .send(fact!(d3_application::Myself, me => uuid.into_record()));

        Ok(instance)
    }
}
