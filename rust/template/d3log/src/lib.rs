pub mod broadcast;
pub mod ddvalue_batch;
mod dispatch;
pub mod dred;
pub mod error;
mod forwarder;
mod json_framer;
pub mod record_batch;
pub mod tcp_network;

use core::fmt;
use differential_datalog::{ddval::DDValue, record::*, D3logLocationId};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::mpsc::{self, channel, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
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
};

pub type Node = D3logLocationId;
pub type Uuid = u128;

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

struct AccumulatePort {
    eval: Evaluator,
    b: Arc<Mutex<DDValueBatch>>,
}

impl Transport for AccumulatePort {
    fn send(&self, b: Batch) {
        for (r, f, w) in &DDValueBatch::from(&(*self.eval), b).expect("iterator") {
            self.b.lock().expect("lock").insert(r, f, w);
        }
    }
}

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

pub struct ThreadManager {
    threads: HashMap<Uuid, (isize, Option<Sender<()>>)>,
}

impl ThreadManager {
    pub fn new() -> Self {
        Self {
            threads: HashMap::new(),
        }
    }
}

struct ThreadInstance {
    instance: Arc<Instance>,
    new_evaluator: Arc<dyn Fn(Node, Port) -> Result<(Evaluator, Batch), Error> + Send + Sync>,
    // since all kinds of children will need a copy of the management state,
    // consider generalizing. this needs to move to the broadcaster for atomicity
    // reasons anyways
    accumulator: Arc<Mutex<DDValueBatch>>,
    manager: Arc<Mutex<ThreadManager>>,
}

// we're just throwing this into the same runtime - do we want/need scheduling isolation?
// xxx handle deletes
impl Transport for Arc<ThreadInstance> {
    fn send(&self, b: Batch) {
        for (_, p, weight) in &RecordBatch::from(self.instance.eval.clone(), b) {
            // async_error variant for Some
            let uuid_record = p.get_struct_field("id").unwrap();
            let uuid = async_error!(self.instance.eval.clone(), Uuid::from_record(uuid_record));

            let mut manager = self.manager.lock().expect("lock");
            let value = manager
                .threads
                .entry(uuid)
                .or_insert_with(|| (weight, None));
            let w = value.0;
            let thread_handle = &value.1;
            if w > 0 {
                // Start instance if one is not already present
                if thread_handle.is_none() {
                    let (tx, rx) = mpsc::channel();
                    let self_clone = self.clone();
                    value.1 = Some(tx.clone());
                    thread::spawn(move || {
                        let new_instance = async_error!(
                            self_clone.instance.eval.clone(),
                            // make a new runtime?
                            Instance::new(
                                Arc::new(Runtime::new().unwrap()),
                                self_clone.new_evaluator.clone(),
                                uuid
                            )
                        );
                        new_instance.eval_port.send(Batch::Value(
                            self_clone.accumulator.lock().expect("lock").clone(),
                        ));

                        async_error!(
                            self_clone.instance.eval.clone(),
                            self_clone
                                .instance
                                .broadcast
                                .clone()
                                .couple(new_instance.broadcast.clone())
                        );

                        async_error!(new_instance.eval.clone(), tcp_bind(new_instance.clone()));

                        /* make transport here configurable
                        new_self.forwarder.register(uuid, ep.clone());
                        forwarder
                        .register(new_self.eval.clone().myself(), new_self.evalport.clone());
                         */

                        loop {
                            match rx.try_recv() {
                                Ok(_) | Err(TryRecvError::Disconnected) => {
                                    println!("Terminating.");
                                    break;
                                }
                                Err(TryRecvError::Empty) => {}
                            }
                        }
                    });
                }
            } else if w <= 0 {
                // TODO: check if thread termination works
                if let Some(tx) = thread_handle {
                    let _ = tx.send(());
                }
            }

            let threads: u64 = 1;
            let bytes: u64 = 1;

            self.instance
                .broadcast
                .send(fact!(d3_application::InstanceStatus,
                            time => self.instance.eval.clone().now().into_record(),
                            id => uuid.into_record(),
                            memory_bytes => bytes.into_record(),
                            threads => threads.into_record()));
        }
    }
}

struct DebugPort {
    eval: Evaluator,
}

impl Transport for DebugPort {
    fn send(&self, b: Batch) {
        for (_r, f, w) in &RecordBatch::from(self.eval.clone(), b) {
            println!("{} {}", f, w);
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
            forwarder,
            rt: rt.clone(),
        });

        let accu_batch = Arc::new(Mutex::new(DDValueBatch::new()));
        dispatch.clone().register(
            "d3_application::ThreadInstance",
            Arc::new(Arc::new(ThreadInstance {
                instance: instance.clone(),
                accumulator: accu_batch.clone(),
                new_evaluator: new_evaluator.clone(),
                manager: Arc::new(Mutex::new(ThreadManager::new())),
            })),
        )?;

        broadcast.clone().subscribe(instance.eval_port.clone());
        broadcast.clone().subscribe(Arc::new(AccumulatePort {
            eval: eval.clone(),
            b: accu_batch.clone(),
        }));

        instance
            .eval_port
            .send(fact!(d3_application::Myself, me => uuid.into_record()));

        Ok(instance)
    }
}
