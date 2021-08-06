use crate::{relid2name, relval_from_record, Relations};
use d3log::{
    batch::Batch,
    broadcast::PubSub,
    display::Display,
    error::Error,
    fact,
    factset::FactSet,
    record_set::{read_record_json_file, RecordSet},
    tcp_network::tcp_bind,
    value_set::ValueSet,
    DebugPort, Evaluator, EvaluatorTrait, Instance, Node, Port, Transport,
};
use differential_datalog::program::config::{Config, ProfilingConfig};

use differential_datalog::{
    api::HDDlog, ddval::DDValue, program::Update, record::IntoRecord, record::Record,
    record::RelIdentifier, D3log, DDlog, DDlogDynamic,
};
use rand::Rng;
use std::borrow::Cow;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;

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

impl D3 {
    pub fn new(uuid: u128, error: Port) -> Result<(Evaluator, Batch), Error> {
        let config = Config::new()
            .with_timely_workers(1)
            .with_profiling_config(ProfilingConfig::SelfProfiling);
        let (h, init_output) = crate::run_with_config(config, false)?;
        let ad = Arc::new(D3 { h, uuid, error });
        Ok((ad, ValueSet::from_delta_map(init_output)))
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

    fn error(&self, text: Record, line: Record, filename: Record, functionname: Record) {
        let f = fact!(d3_application::Error,
                      text => text,
                      line => line,
                      instance => self.uuid.clone().into_record(),
                      filename => filename,
                      functionname => functionname);
        self.error.clone().send(f);
    }

    // does it make sense to try to use the HDDLog record evaluation?
    fn eval(&self, input: Batch) -> Result<Batch, Error> {
        let mut upd = Vec::new();
        let b = ValueSet::from(self, input.data)?;

        for (relid, v, _) in &b {
            upd.push(Update::Insert { relid, v });
        }

        self.h.transaction_start()?;
        self.h.apply_updates(&mut upd.clone().drain(..))?;
        Ok(ValueSet::from_delta_map(
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

    tcp_bind(instance.clone())?;

    // xxx under a command line flag
    instance.broadcast.clone().subscribe(Arc::new(DebugPort {
        eval: instance.eval.clone(),
    }));

    if is_parent {
        let debug_uuid = u128::from_be_bytes(rand::thread_rng().gen::<[u8; 16]>());
        // batch union?
        instance
            .broadcast
            .clone()
            .send(fact!(d3_application::Stdout, target=>debug_uuid.into_record()));
        instance.broadcast.clone().send(
            fact!(d3_application::Forward, target=>debug_uuid.into_record(), intermediate => uuid.into_record()),
        );

        //xxx feature
        Display::new(instance.clone(), 8080);
    }

    if let Some(f) = inputfile {
        read_record_json_file(f, &mut |b: RecordSet| {
            // XXX - fields in b that aren't present in the target relation are ignored,
            // fields specified for the target that aren't in the source zre zeroed (?)
            instance
                .eval_port
                .send(Batch::new(FactSet::Empty(), FactSet::Record(b)));
        });
    }

    loop {
        std::thread::park();
    }
}
