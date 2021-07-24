pub mod broadcast;
pub mod ddvalue_batch;
mod dispatch;
pub mod dred;
pub mod error;
mod forwarder;
mod json_framer;
pub mod record_batch;
mod tcp_network;

use colored::Colorize;
use core::fmt;
use differential_datalog::{ddval::DDValue, record::*, D3logLocationId};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::mpsc::{self, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;

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

use std::collections::VecDeque;

struct EvalPort {
    eval: Evaluator,
    forwarder: Port,
    dispatch: Port,
    queue: Arc<Mutex<VecDeque<Batch>>>,
}

impl Transport for EvalPort {
    fn send(&self, b: Batch) {
        println!(
            "{} [uuid:{}] {}",
            "[EVALPORT]".yellow(),
            self.eval.clone().myself().to_string().red(),
            RecordBatch::from(self.eval.clone(), b.clone())
        );
        self.dispatch.send(b.clone());

        {
            self.queue.lock().expect("lock").push_back(b.clone());
        }

        loop {
            let b = {
                match self.queue.try_lock() {
                    Ok(mut x) => match x.pop_front() {
                        Some(x) => x.clone(),
                        None => {
                            return;
                        }
                    },
                    Err(_) => {
                        return;
                    }
                }
            };
            let out = async_error!(self.eval.clone(), self.eval.eval(b.clone()));
            self.forwarder.send(out.clone());
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
    rt: Arc<tokio::runtime::Runtime>,
    eval: Evaluator,
    evalport: Port,
    new_evaluator: Arc<dyn Fn(Node, Port) -> Result<(Evaluator, Batch), Error> + Send + Sync>,
    forwarder: Arc<Forwarder>,
    broadcast: Arc<Broadcast>,
    // since all kinds of children will need a copy of the management state,
    // consider generalizing
    accumulator: Arc<Mutex<DDValueBatch>>,
    manager: Arc<Mutex<ThreadManager>>,
}

// we're just throwing this into the same runtime - do we want/need scheduling isolation?
// xxx handle deletes
impl Transport for Arc<ThreadInstance> {
    fn send(&self, b: Batch) {
        for (_, p, weight) in &RecordBatch::from(self.eval.clone(), b) {
            // async_error variant for Some
            let uuid_record = p.get_struct_field("id").unwrap();
            let uuid = async_error!(self.clone().eval.clone(), Uuid::from_record(uuid_record));

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
                    let new_self = self.clone();
                    println!("[THREAD_INSTANCE] Spawning a thread");
                    value.1 = Some(tx.clone());
                    thread::spawn(move || {
                        let th_name = String::from("async-rt-") + &uuid.to_string();
                        let rt = Arc::new(
                            Builder::new_multi_thread()
                                .worker_threads(1)
                                .max_blocking_threads(1)
                                .thread_name(th_name)
                                .enable_all()
                                .build()
                                .unwrap(),
                        );

                        let (_p, _init_batch, ep, _jh, _dispatch, forwarder) = async_error!(
                            new_self.eval,
                            start_instance(rt.clone(), new_self.new_evaluator.clone(), uuid)
                        );
                        println!(
                            "[THREAD_INSTANCE] Started a new instance with thread id {:?}",
                            thread::current().id()
                        );
                        ep.send(Batch::Value(
                            new_self.accumulator.lock().expect("lock").clone(),
                        ));

                        new_self.forwarder.register(uuid, ep.clone());
                        //forwarder.register(eval.clone().myself(), ep.clone());

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

            self.broadcast.send(fact!(d3_application::InstanceStatus,
                                      time => self.eval.clone().now().into_record(),
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
        for (r, f, w) in &RecordBatch::from(self.eval.clone(), b) {
            if let Some(name) = match f.clone() {
                Record::NamedStruct(name, _) => Some(name),
                _ => None,
            } {
                let new_name = if name == "PrintMSResult" {
                    String::from(name).green()
                } else {
                    String::from("").green()
                };

                println!(
                    "{} [uuid={}] {} {} {}",
                    "[DEBUG_PORT]".blue().bold(),
                    self.eval.clone().myself().to_string().red(),
                    new_name,
                    f,
                    w
                );
            } else {
                println!(
                    "{} [uuid={}] {} {}",
                    "[DEBUG_PORT]".blue().bold(),
                    self.eval.clone().myself().to_string().red(),
                    f,
                    w
                );
            }
        }
    }
}

pub fn start_instance(
    rt: Arc<Runtime>,
    new_evaluator: Arc<dyn Fn(Node, Port) -> Result<(Evaluator, Batch), Error> + Send + Sync>,
    uuid: u128,
) -> Result<(Port, Batch, Port, JoinHandle<()>, Port, Arc<Forwarder>), Error> {
    let broadcast = Broadcast::new();
    let (eval, init_batch) = new_evaluator(uuid, broadcast.clone())?;
    let dispatch = Arc::new(Dispatch::new(eval.clone()));

    broadcast.clone().subscribe(dispatch.clone());
    broadcast
        .clone()
        .subscribe(Arc::new(DebugPort { eval: eval.clone() }));
    let forwarder = Forwarder::new(eval.clone(), dispatch.clone(), broadcast.clone());
    let accu_batch = Arc::new(Mutex::new(DDValueBatch::new()));

    // shouldn't evaluator just implement Transport?
    let eval_port = Arc::new(EvalPort {
        forwarder: forwarder.clone(),
        dispatch: dispatch.clone(),
        eval: eval.clone(),
        queue: Arc::new(Mutex::new(VecDeque::new())),
    });

    dispatch.clone().register(
        "d3_application::ThreadInstance",
        Arc::new(Arc::new(ThreadInstance {
            rt: rt.clone(),
            accumulator: accu_batch.clone(),
            eval: eval.clone(),
            evalport: eval_port.clone(),
            forwarder: forwarder.clone(),
            new_evaluator: new_evaluator.clone(),
            broadcast: broadcast.clone(),
            manager: Arc::new(Mutex::new(ThreadManager::new())),
        })),
    )?;

    broadcast.clone().subscribe(eval_port.clone());
    broadcast.clone().subscribe(Arc::new(AccumulatePort {
        eval: eval.clone(),
        b: accu_batch.clone(),
    }));
    eval_port.send(fact!(d3_application::Myself, me => uuid.into_record()));

    let management_clone = broadcast.clone();
    let forwarder_clone = forwarder.clone();
    let eval_clone = eval.clone();
    let dispatch_clone = dispatch.clone();
    let rt_clone = rt.clone();

    println!("[{}] calling tcp_bind", function!().blue().bold());
    let handle = rt_clone.spawn(async move {
        async_error!(
            eval.clone(),
            tcp_bind(
                dispatch_clone,
                uuid,
                forwarder_clone,
                management_clone.clone(),
                eval_clone.clone(),
                management_clone.clone(),
                rt.clone(),
            )
            .await
        );
    });
    Ok((
        broadcast, init_batch, eval_port, handle, dispatch, forwarder,
    ))
}