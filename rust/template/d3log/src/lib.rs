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
    pub trace: Arc<Mutex<isize>>,
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
            if let Err(_) = sclone.send(b.clone()).await {
                panic!("channel closed");
            }
        });
    }
}

// a facility, which didn't find much use, to put a printf across a Port send
pub struct PortTrace {
    uuid: Node,
    head: String,
    p: Port,
}

impl PortTrace {
    #[allow(dead_code)]
    fn new(uuid: Node, head: String, p: Port) -> Port {
        Arc::new(PortTrace {
            uuid,
            head,
            p: p.clone(),
        })
    }
}

impl Transport for PortTrace {
    fn send(&self, b: Batch) {
        println!("{} {} {} ", self.uuid, self.head, b);
        self.p.clone().send(b);
    }
}

// a uuid addressed service that prints facts out the processes stdout
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

struct NodeTrace {
    #[warn(dead_code)]
    instance: Arc<Instance>,
}
impl Transport for NodeTrace {
    fn send(&self, b: Batch) {
        // fix extraction and loop
        for (_r, f, w) in &RecordSet::from(self.instance.eval.clone(), b.data) {
            let uuid_record = f.get_struct_field("id").unwrap();
            let uuid = async_error!(self.instance.eval.clone(), Node::from_record(uuid_record));
            if self.instance.uuid == uuid {
                let mut i = self.instance.trace.lock().expect("lock");
                *i = *i + w;
            }
        }
    }
}

// facilities?
fn trace(instance: Arc<Instance>, key: &str, prefix: &str, b: Batch) {
    if b.clone().meta.scan(key.to_string()).is_some() || (*instance.trace.lock().expect("lock") > 0)
    {
        println!(
            "{} {} {}",
            prefix.to_string(),
            instance.uuid,
            b.format(instance.eval.clone())
        );
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
        let (esend, mut erecv) = channel(20);
        let eval_port = Arc::new(EvalPort {
            rt: rt.clone(),
            dispatch: dispatch.clone(),
            eval: eval.clone(),
            s: esend,
        });
        let (under, forwarder) = Forwarder::new(eval.clone(), dispatch.clone(), eval_port.clone());

        broadcast.clone().setup_accumulator(eval.clone());
        let instance = Arc::new(Instance {
            trace: Arc::new(Mutex::new(0)),
            uuid,
            broadcast: broadcast.clone(),
            eval: eval.clone(),
            eval_port,
            init_batch,
            dispatch,
            under,
            forwarder,
            rt: rt.clone(),
        });

        let instance_clone = instance.clone();
        rt.spawn(async move {
            loop {
                match erecv.recv().await {
                    Some(b) => {
                        let e = instance_clone.eval.clone();

                        trace(
                            instance_clone.clone(),
                            "d3_supervisor::Trace",
                            "eval in",
                            b.clone(),
                        );
                        let out = async_error!(e.clone(), e.eval(b.clone()));
                        trace(
                            instance_clone.clone(),
                            "d3_supervisor::TraceOut",
                            "eval out",
                            out.clone(),
                        );
                        instance_clone.dispatch.send(out.clone());
                        instance_clone.forwarder.send(out.clone());

                        //
                        // provisional metadata support -
                        //    shift the meta into the data leaving the meta empty and
                        //    throw into the local chute
                        // xxx - this is semi-deprecated
                        let shiftb = Batch::new(FactSet::Empty(), b.meta.clone());
                        trace(
                            instance_clone.clone(),
                            "d3_supervisor::MetaTrace",
                            "meval in",
                            shiftb.clone(),
                        );
                        let mout = async_error!(e.clone(), e.eval(shiftb.clone()));
                        trace(
                            instance_clone.clone(),
                            "d3_supervisor::MetaTrace",
                            "meval in",
                            mout.clone(),
                        );
                        instance_clone.dispatch.send(mout.clone());
                        instance_clone.forwarder.send(mout.clone());
                    }
                    None => panic!("evaluator closed the channel"),
                }
            }
        });

        // self registration leads to trouble
        //    forwarder.clone().register(uuid, instance.eval_port.clone());

        broadcast.subscribe(instance.eval_port.clone());

        ThreadInstance::new(instance.clone(), new_evaluator.clone())?;
        ProcessInstance::new(instance.clone(), new_evaluator.clone())?;

        instance.dispatch.register(
            "d3_application::NodeTrace",
            Arc::new(NodeTrace {
                instance: instance.clone(),
            }),
        )?;

        instance
            .eval_port
            .send(fact!(d3_application::Myself, me => uuid.into_record()));

        instance.dispatch.send(instance.init_batch.clone()); //?
        Ok(instance)
    }
}
