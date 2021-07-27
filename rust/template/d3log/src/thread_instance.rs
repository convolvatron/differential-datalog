// support creating instances (of the same program) on a separate thread in the same address space
use crate::{
    async_error, broadcast::PubSub, fact, function, send_error, tcp_bind, Batch, Error, Evaluator,
    Instance, Node, Port, RecordBatch, Transport,
};
use differential_datalog::record::*;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::runtime::Runtime;

pub struct ThreadManager {
    threads: HashMap<Node, (isize, Option<Sender<()>>)>,
}

impl ThreadManager {
    pub fn new() -> Self {
        Self {
            threads: HashMap::new(),
        }
    }
}

pub struct ThreadInstance {
    instance: Arc<Instance>,
    new_evaluator: Arc<dyn Fn(Node, Port) -> Result<(Evaluator, Batch), Error> + Send + Sync>,
    // since all kinds of children will need a copy of the management state,
    // consider generalizing. this needs to move to the broadcaster for atomicity
    // reasons anyways
    manager: Arc<Mutex<ThreadManager>>,
}

// we're just throwing this into the same runtime - do we want/need scheduling isolation?
// xxx handle deletes
impl Transport for Arc<ThreadInstance> {
    fn send(&self, b: Batch) {
        for (_, p, weight) in &RecordBatch::from(self.instance.eval.clone(), b) {
            // async_error variant for Some
            let uuid_record = p.get_struct_field("id").unwrap();
            let uuid = async_error!(self.instance.eval.clone(), Node::from_record(uuid_record));

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

                        // rx.recv blocks
                        match rx.recv() {
                            Ok(_) => println!("Terminating thread!"),
                            Err(_) => println!("Error receving msg! Terminating thread!"),
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

impl ThreadInstance {
    pub fn new(
        instance: Arc<Instance>,
        new_evaluator: Arc<dyn Fn(Node, Port) -> Result<(Evaluator, Batch), Error> + Send + Sync>,
    ) -> Result<(), Error> {
        instance.dispatch.clone().register(
            "d3_application::ThreadInstance",
            Arc::new(Arc::new(ThreadInstance {
                instance: instance.clone(),
                new_evaluator: new_evaluator.clone(),
                manager: Arc::new(Mutex::new(ThreadManager::new())),
            })),
        )
    }
}
