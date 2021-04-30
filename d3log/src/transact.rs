// general module for managing transactions into and out of ddlog. currently
// this means just feeding batches and splitting them up for distribution - but
// single-value-serialization/progress would go here

use crate::{batch::Batch, Node, Port};
use differential_datalog::{
    ddval::{DDValConvert, DDValue},
    program::Update,
    D3log, DDlog, DDlogDynamic, DDlogInventory,
};
use mm_ddlog::api::HDDlog;
use mm_ddlog::typedefs::d3::Workers;

use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::Mutex as SyncMutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub type Timestamp = u64;

pub struct TransactionManager {
    // network uses a transaction manger for queries,
    // which causes this small mess
    network: Option<Port>,

    management: Arc<Port>,
    evaluator: HDDlog,
    me: Node,
}

// ok, Instant is monotonic, but SystemTime is not..we need something
// which is going to track across restarts and as monotonic, so we
// will burtally coerce SytemTime into monotonicity

fn current_time() -> Timestamp {
    let now = SystemTime::now();
    let delta = now
        .duration_since(UNIX_EPOCH)
        // fix this with a little sequence number
        .expect("Monotonicity violation");
    let ms = delta.subsec_millis();
    ms as u64
}

#[derive(Clone)]
pub struct ArcTransactionManager {
    t: Arc<SyncMutex<TransactionManager>>,
}

impl ArcTransactionManager {
    pub fn new(uuid: Node, management: Arc<Port>) -> ArcTransactionManager {
        let tm = ArcTransactionManager {
            t: Arc::new(SyncMutex::new(TransactionManager::new(
                uuid,
                management.clone(),
            ))),
        };

        management.send(
            0,
            Batch::singleton("d3::Workers", &Workers { location: uuid }.into_ddvalue())
                .expect("workers"),
        );
        tm
    }

    pub fn myself(&self) -> Node {
        (*self.t.lock().expect("lock")).me
    }

    pub fn forward(&self, input: Batch) -> Result<(), std::io::Error> {
        let mut output = HashMap::<Node, Box<Batch>>::new();

        for (rel, v, weight) in input {
            let tma = &*self.t.lock().expect("lock");

            match tma.evaluator.d3log_localize_val(rel, v.clone()) {
                Ok((loc_id, in_rel, inner_val)) => {
                    // if loc_id is null, we aren't to forward
                    if let Some(loc) = loc_id {
                        output
                            .entry(loc)
                            .or_insert_with(|| Box::new(Batch::new()))
                            .insert(in_rel, inner_val, weight as u32)
                    }
                }
                Err(val) => println!("{} {:+}", val, weight),
            }
        }

        for (nid, b) in output.drain() {
            let tma = &*self.t.lock().expect("lock");
            if let Some(n) = &tma.network {
                n.send(nid, *b)
            }
        }
        Ok(())
    }

    pub fn eval(self, input: Batch) -> Result<Batch, std::io::Error> {
        let tm = self.t.lock().expect("lock");
        let h = &(*tm).evaluator;

        let mut upd = Vec::new();
        for (relid, v, _) in input {
            upd.push(Update::Insert { relid, v });
        }

        // wrapper to translate hddlog's string error to our standard-by-default std::io::Error
        match (|| -> Result<Batch, String> {
            h.transaction_start()?;
            h.apply_updates(&mut upd.clone().drain(..))?;
            Ok(Batch::from(h.transaction_commit_dump_changes()?))
        })() {
            Ok(x) => Ok(x),
            Err(e) => Err(Error::new(
                ErrorKind::Other,
                format!("Failed to update differential datalog: {}", e),
            )),
        }
    }

    // its not so much that this belongs in TransactionManager, but that
    // can access to the evaluator. return _some_ matching element.
    pub fn lookup(self, index_name: &str, key: DDValue) -> Result<Option<DDValue>, String> {
        let tma = &*self.t.lock().expect("lock");
        let results = tma
            .evaluator
            .query_index(tma.evaluator.get_index_id(&index_name)?, key)?;
        // insert method on batch please
        Ok(results.into_iter().next())
    }

    // to be deleted
    pub fn set_network(self, p: Port) {
        self.t.lock().unwrap().network = Some(p);
    }
}

impl TransactionManager {
    fn start() {}

    pub fn new(me: Node, management: Arc<Port>) -> TransactionManager {
        let (hddlog, _init_output) = HDDlog::run(1, false)
            .unwrap_or_else(|err| panic!("Failed to run differential datalog: {}", err));

        TransactionManager {
            network: None,
            management,
            evaluator: hddlog,
            me,
        }
    }
}
