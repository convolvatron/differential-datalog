use crate::{relid2name, relval_from_record, Relations, UpdateSerializer};
use d3log::{
    async_error,
    broadcast::PubSub,
    ddvalue_batch::DDValueBatch,
    error::Error,
    fact, function,
    json_framer::JsonFramer,
    nega_fact,
    process::read_output,
    process::{FileDescriptorPort, MANAGEMENT_INPUT_FD, MANAGEMENT_OUTPUT_FD},
    record_batch::{
        deserialize_record_batch, read_record_json_file, serialize_record_batch, RecordBatch,
    },
    send_error,
    tcp_network::tcp_bind,
    Batch, Evaluator, EvaluatorTrait, Instance, Node, Port, Transport,
};
use differential_datalog::program::config::{Config, ProfilingConfig};

use differential_datalog::{
    api::HDDlog, ddval::DDValue, program::Update, record::IntoRecord, record::Record,
    record::RelIdentifier, D3log, DDlog, DDlogDynamic,
};
use rand::Rng;
use serde::{de, de::SeqAccess, de::Visitor, Deserialize, Deserializer};
use serde::{ser::SerializeTuple, Serialize, Serializer};
use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json;
use tokio::runtime::Runtime;
use tokio::sync::Mutex as AsyncMutex;
use tokio_fd::AsyncFd;

pub struct Null {}
impl Transport for Null {
    fn send(&self, _rb: Batch) {}
}

pub struct Print(pub Port);

impl Transport for Print {
    fn send(&self, b: Batch) {
        println!("{}", b);
        self.0.send(b);
    }
}

pub struct D3 {
    uuid: u128,
    error: Port,
    h: HDDlog,
}

struct SerializeBatchWrapper {
    b: DDValueBatch,
}

struct BatchVisitor {}

impl<'de> Visitor<'de> for BatchVisitor {
    type Value = DDValueBatch;

    // his just formats an error message..in advance?
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "batch")
    }

    fn visit_seq<E>(self, mut e: E) -> Result<Self::Value, E::Error>
    where
        E: SeqAccess<'de>,
    {
        let bn = DDValueBatch::new();
        {
            let mut b = bn.0.lock().unwrap();

            let timestamp: Option<u64> = e.next_element()?;
            match timestamp {
                Some(timestamp) => b.timestamp = timestamp,
                None => return Err(de::Error::custom("expected integer timestamp")),
            }

            let updates: Option<Vec<UpdateSerializer>> = e.next_element()?;
            match updates {
                Some(updates) => {
                    for i in updates {
                        let u = Update::<DDValue>::from(i);
                        match u {
                            // insert method?
                            Update::Insert { relid, v } => b.deltas.update(relid, &v, 1),
                            _ => return Err(de::Error::custom("invalid value")),
                        }
                    }
                }
                None => return Err(de::Error::custom("unable to parse update set")),
            }
        }
        Ok(bn)
    }
}

impl<'de> Deserialize<'de> for SerializeBatchWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let b: DDValueBatch = deserializer.deserialize_any(BatchVisitor {})?;
        Ok(SerializeBatchWrapper { b })
    }
}

impl Serialize for SerializeBatchWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut tup = serializer.serialize_tuple(3)?; //maybe map at the top level is better
        let mut updates = Vec::new();
        let b = &self.b;
        for (relid, v, _) in b {
            updates.push(UpdateSerializer::from(Update::Insert {
                relid,
                v: v.clone(),
            }));
        }
        tup.serialize_element(&b.0.lock().unwrap().timestamp)?;
        tup.serialize_element(&updates)?;
        tup.end()
    }
}

impl D3 {
    pub fn new(uuid: u128, error: Port) -> Result<(Evaluator, Batch), Error> {
        let config = Config::new()
            .with_timely_workers(1)
            .with_profiling_config(ProfilingConfig::SelfProfiling);
        let (h, init_output) = crate::run_with_config(config, false)?;
        let ad = Arc::new(D3 { h, uuid, error });
        Ok((ad, DDValueBatch::from_delta_map(init_output)))
    }
}

impl EvaluatorTrait for D3 {
    fn ddvalue_from_record(&self, name: String, r: Record) -> Result<DDValue, Error> {
        let id = self.id_from_relation_name(name.clone())?;
        let t: RelIdentifier = RelIdentifier::RelId(id);
        let rel = Relations::try_from(&t).expect("huh");
        relval_from_record(rel, &r)
            .map_err(|x| Error::new("bad record conversion: ".to_string() + &x.to_string()))
    }

    fn myself(&self) -> Node {
        self.uuid
    }

    fn error(
        &self,
        text: Record,
        line: Record,
        filename: Record,
        functionname: Record,
        uuid: Record,
    ) {
        let f = fact!(d3_application::Error,
                      text => text,
                      line => line,
                      uuid => uuid,
                      filename => filename,
                      functionname => functionname);
        self.error.clone().send(f);
    }

    // does it make sense to try to use the HDDLog record evaluation?
    fn eval(&self, input: Batch) -> Result<Batch, Error> {
        // would like to implicitly convert batch to ddvalue_batch, but i cant, because i need an
        // evaluator, and its been deconstructed before we get here...
        let mut upd = Vec::new();
        let b = DDValueBatch::from(self, input)?;

        for (relid, v, _) in &b {
            upd.push(Update::Insert { relid, v });
        }

        self.h.transaction_start()?;
        self.h.apply_updates(&mut upd.clone().drain(..))?;
        Ok(DDValueBatch::from_delta_map(
            self.h.transaction_commit_dump_changes()?,
        ))
    }

    fn id_from_relation_name(&self, s: String) -> Result<usize, Error> {
        let s: &str = &s;
        match Relations::try_from(s) {
            Ok(r) => Ok(r as usize),
            Err(_) => Err(Error::new(format!("bad relation {}", s))),
        }
    }

    fn localize(&self, rel: usize, v: DDValue) -> Option<(Node, usize, DDValue)> {
        match self.h.d3log_localize_val(rel, v.clone()) {
            Ok((Some(n), r, v)) => Some((n, r, v)),
            Ok((None, _, _)) => None,
            Err(_) => None,
        }
    }

    // doesn't belong here. but we'd like a monotonic wallclock
    // to sequence system events. Also - it would be nice if ddlog
    // had some basic time functions (format)
    fn now(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64
    }

    fn record_from_ddvalue(&self, d: DDValue) -> Result<Record, Error> {
        Ok(d.into_record())
    }

    fn relation_name_from_id(&self, id: usize) -> Result<String, Error> {
        match relid2name(id) {
            Some(x) => Ok(x.to_string()),
            None => Err(Error::new("unknown relation id".to_string())),
        }
    }

    // xxx - actually we want to parameterize on format, not on internal representation
    fn serialize_batch(&self, b: DDValueBatch) -> Result<Vec<u8>, Error> {
        let w = SerializeBatchWrapper { b };
        let encoded = serde_json::to_string(&w)?;
        Ok(encoded.as_bytes().to_vec())
    }

    fn deserialize_batch(&self, s: Vec<u8>) -> Result<DDValueBatch, Error> {
        let s = std::str::from_utf8(&s)?;
        let v: SerializeBatchWrapper = serde_json::from_str(&s)?;
        Ok(v.b)
    }
}

pub fn start_d3log(inputfile: Option<String>) -> Result<(), Error> {
    let (uuid, is_parent) = if let Some(uuid) = std::env::var_os("uuid") {
        if let Some(uuid) = uuid.to_str() {
            let my_uuid = uuid.parse::<u128>().unwrap();
            (my_uuid, false)
        } else {
            panic!("bad uuid");
        }
    } else {
        // use uuid crate
        (
            u128::from_be_bytes(rand::thread_rng().gen::<[u8; 16]>()),
            true,
        )
    };

    let d =
        move |id: u128, error: Port| -> Result<(Evaluator, Batch), Error> { D3::new(id, error) };

    let rt = Arc::new(Runtime::new()?);
    let instance = Instance::new(rt.clone(), Arc::new(d), uuid)?;

    let instance_clone4 = instance.clone();
    println!("calling tcp_bind: {}", uuid);
    tcp_bind(instance.clone())?;
    if is_parent {
        /*let debug_uuid = u128::from_be_bytes(rand::thread_rng().gen::<[u8; 16]>());
        // batch union?
        instance
            .broadcast
            .clone()
            .send(fact!(d3_application::Stdout, target=>debug_uuid.into_record()));
        instance.broadcast.clone().send(
            fact!(d3_application::Forward, target=>debug_uuid.into_record(), intermediate => uuid.into_record()),
        );*/
    } else {
        // does this really belong here?
        println!("{} subscribe for fdport", uuid);
        // XXX: Do we need to really enter?
        instance.clone().rt.enter();
        let instance_clone = instance.clone();
        let instance_clone2 = instance.clone();
        instance.rt.block_on(async move {
            let management_from_parent =
                instance_clone
                    .broadcast
                    .clone()
                    .subscribe(Arc::new(FileDescriptorPort {
                        management: instance_clone.broadcast.clone() as Port,
                        eval: instance_clone.eval.clone(),
                        fd: Arc::new(AsyncMutex::new(
                            AsyncFd::try_from(MANAGEMENT_OUTPUT_FD).expect("asyncfd"),
                        )),
                        rt: instance_clone.rt.clone(),
                    }));

            instance_clone.rt.spawn(async {
                let instance_clone3 = instance_clone2.clone();
                let instance_clone4 = instance_clone2.clone();
                let mut jf = JsonFramer::new();
                async_error!(
                    instance_clone3.clone().eval.clone(),
                    read_output(MANAGEMENT_INPUT_FD, move |b: &[u8]| {
                        for i in async_error!(instance_clone2.clone().eval.clone(), jf.append(b)) {
                            let v = async_error!(
                                instance_clone2.clone().eval.clone(),
                                deserialize_record_batch(i)
                            );

                            for (r, f, w) in
                                &RecordBatch::from(instance_clone4.clone().eval.clone(), v.clone())
                            {
                                println!(
                                    "incoming (from parent) management fact @{} r: {} w: {}",
                                    instance_clone4.clone().eval.clone().myself(),
                                    r,
                                    w
                                );
                                if let Some(loc) = f.get_struct_field("location") {
                                    println!("from loc {}", loc);
                                }
                                if let Some(dest) = f.get_struct_field("destination") {
                                    println!("dest {}", dest);
                                }
                            }
                            management_from_parent.clone().send(v);
                        }
                    })
                    .await
                );
            });
        });
    }

    if let Some(f) = inputfile {
        read_record_json_file(f, &mut |b: Batch| {
            // XXX - fields in b that aren't present in the target relation are ignored,
            // fields specified for the target that aren't in the source zre zeroed (?)

            let d = DDValueBatch::from(&*instance.eval, b.clone()).expect("ddv");
            for (_r, f, w) in &RecordBatch::from(instance.eval.clone(), Batch::Value(d)) {
                println!("{} {} {}", instance.eval.clone().myself(), f, w);
            }
            instance.eval_port.send(b);
        });
    }

    if is_parent {
        std::thread::sleep(std::time::Duration::from_secs(7));
        instance_clone4.clone()
        .dispatch
        .send(nega_fact!(d3_application::Process, id=> 2u64.into_record(), path => "".to_string().into_record(), management => false.into_record()));
    }

    println!("Parking thread {}", uuid);
    loop {
        std::thread::park();
    }
}
