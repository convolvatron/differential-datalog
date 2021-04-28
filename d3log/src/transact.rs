// general module for managing transactions into and out of ddlog

use crate::tcp_network::ArcTcpNetwork;
use crate::{batch::{Batch, singleton}, Node, Transport, child::output_json};
use differential_datalog::{program::Update, D3log, DDlog, DDlogDynamic, DDlogInventory,
                           ddval::{DDValConvert, DDValue}};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as SyncMutex;
use std::time::{SystemTime, UNIX_EPOCH};
use rand::Rng;
use mm_ddlog::typedefs::d3::{Workers};
use mm_ddlog::api::HDDlog;


pub type Timestamp = u64;

pub struct TransactionManager {
    // svcc needs the membership, so we are going to assign nids
    // progress: Vec<Timestamp>,
    n: Option<Box<(dyn Transport + Send + Sync)>>,
    
    // need access to some hddlog to call d3log_localize_val
    h: HDDlog,

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
    pub fn new() -> ArcTransactionManager {
        let tm = ArcTransactionManager {
            t: Arc::new(SyncMutex::new(TransactionManager::new())),
        };
        
        // fix network and tm mutual reference
        tm.clone().t.lock().expect("lock").n = Some(Box::new(ArcTcpNetwork::new(tm.clone())));
        
        // race between this and tcp network
        
        tm.clone().metadata("d3::Workers", Workers{location: tm.myself()}.into_ddvalue());
        tm
    }

    pub fn myself(&self) -> Node {
        (*self.t.lock().expect("lock")).me
    }

    pub async fn forward<'a>(&self, input: Batch) -> Result<(), std::io::Error> {
        let mut output = HashMap::<Node, Box<Batch>>::new();

        
        for (rel, v, weight) in input {
            let tma = &*self.t.lock().expect("lock");            
            // we dont really need an instance here, do we? - i guess the state comes from
            // prog. we certainly dont need a lock on tm
            match tma.h.d3log_localize_val(rel, v.clone()) {
                Ok((loc_id, in_rel, inner_val)) => {
                    // if loc_id is null, we aren't to forward
                    if let Some(loc) = loc_id {
                        output
                            .entry(loc)
                            .or_insert(Box::new(Batch::new()))
                            .insert(in_rel, inner_val, weight as u32)
                    }
                }
                Err(val) => println!("{} {:+}", val, weight),
            }
        }

        for (nid, b) in output.drain() {
            let tma = &*self.t.lock().expect("lock");
            if let Some(x) = &tma.n {  // fix
                x.send(nid, *b)?
            }
        }
        Ok(())
    }

    pub async fn eval(self, input: Batch) -> Result<Batch, String> {
        let tm = self.t.lock().expect("lock");
        let h = &(*tm).h;

        // kinda harsh that we feed ddlog updates and get out a deltamap
        let mut upd = Vec::new();
        for (relid, v, _) in input {
            upd.push(Update::Insert { relid, v });
        }
        h.transaction_start()?;
        match h.apply_updates(&mut upd.clone().drain(..)) {
            Ok(()) => Ok(Batch::from(h.transaction_commit_dump_changes()?)),
            Err(err) => {
                println!("Failed to update differential datalog: {}", err);
                Err(err)
            }
        }
    }

    // its not so much that this belongs in TransactionManager, but that
    // can access to the evaluator. return _some_ matching element.
    pub fn lookup(self, index_name:&str, key:DDValue) -> Result<Option<DDValue>, String> {
        let tma = &*self.t.lock().expect("lock");            
        let results = tma.h.query_index(tma.h.get_index_id(&index_name)?, key)?;
        // insert method on batch please
        Ok((||{
            for x in results {
                return Some(x)
            }
            None
        })())
    }

    // this is kind of just a convenience function, but the plumbing around this
    // is a bit fraught
    pub fn metadata(self, relation: &'static str, v: DDValue) {
        // collect completions 
        tokio::spawn(async move {output_json(&(singleton(relation, &v).expect("bad metadata relation"))).await});
    }
}

impl TransactionManager {
    fn start() {}

    // pass network dyn. build a router network.
    pub fn new() -> TransactionManager {
        let (hddlog, _init_output) = HDDlog::run(1, false)
            .unwrap_or_else(|err| panic!("Failed to run differential datalog: {}", err));
        let uuid = u128::from_be_bytes(rand::thread_rng().gen::<[u8; 16]>());
        TransactionManager {
            n: None, // fix
            h: hddlog, // arc?
            // if we care about locating persistent data on this node across reboots,
            // this uuid will have to be stable. not sure if belongs in TM
            me: uuid,
        }
    }
}
