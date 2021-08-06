// functions to allow a set of Records, a dynamically typed alternative to DDValue, to act as
// Batch for interchange between different ddlog programs

#![allow(dead_code)]
use crate::{error::Error, json_framer::JsonFramer, Evaluator, FactSet};
use differential_datalog::record::{CollectionKind, Record};
use num::bigint::ToBigInt;
use num::BigInt;
use num::ToPrimitive;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fs;
use std::string::String;

use serde::{
    de::SeqAccess, de::Visitor, ser::SerializeTuple, Deserialize, Deserializer, Serialize,
    Serializer,
};

use serde_json::{Value, Value::*};

pub fn read_record_json_file(filename: String, cb: &mut dyn FnMut(RecordSet)) -> Result<(), Error> {
    let body = fs::read_to_string(filename.clone())?;
    let mut jf = JsonFramer::new();
    for i in jf.append(body.as_bytes())?.into_iter() {
        let k = match deserialize_record_set(i) {
            Ok(x) => x,
            Err(x) => {
                println!("json err {}", x);
                panic!("z");
            }
        };
        cb(k);
    }
    Ok(())
}

#[derive(Clone, Default)]
pub struct RecordSet {
    pub records: Vec<(Record, isize)>,
}

#[macro_export]
macro_rules! basefact {
     ( $rel:path,  $($n:ident => $v:expr),* ) => {
         Record::NamedStruct(
             Cow::from(stringify!($rel).to_string()),
             vec![$((Cow::from(stringify!($n)), $v),)*]), 1}
     }

#[macro_export]
macro_rules! fact {
    ( $rel:path,  $($n:ident => $v:expr),* ) => {
        Batch::new(FactSet::Empty(),
                   FactSet::Record(RecordSet::singleton(
                       Record::NamedStruct(
                           Cow::from(stringify!($rel).to_string()),
                           vec![$((Cow::from(stringify!($n)), $v),)*]), 1)))}
    }

#[macro_export]
macro_rules! nega_fact {
    ( $rel:path,  $($n:ident => $v:expr),* ) => {
        Batch::new(FactSet::Empty(),
                   FactSet::Record(RecordSet::singleton(
                       Record::NamedStruct(
                           Cow::from(stringify!($rel).to_string()),
                           vec![$((Cow::from(stringify!($n)), $v),)*]), -1)))
    }
}

impl Display for RecordSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut m = HashMap::new();
        for (r, _w) in self.records.clone() {
            match r {
                Record::Bool(_b) => println!("bad record type bool"),
                Record::Int(_i) => println!("bad record type int"),
                Record::Float(_f) => println!("bad record type float"),
                Record::Double(_dbl) => println!("bad record type double"),
                Record::String(string_name) => println!("{}", string_name),
                Record::Serialized(name, _s) => println!("serialized {}", name),
                Record::Tuple(t) => {
                    println!("tuple {:?}", t);
                }
                Record::Array(_, _record_vec) => println!("bad record type array"),
                Record::PosStruct(name, _record_vec) => println!("{}", name),

                Record::NamedStruct(r, _attributes) => *m.entry(r).or_insert(0) += 1,
            }
        }

        f.write_str(&"<")?;
        for (r, c) in m {
            f.write_str(&format!("({} {})", r, c))?;
        }
        f.write_str(&">")?;
        Ok(())
    }
}

fn value_to_record(v: Value) -> Result<Record, Error> {
    match v {
        Null => panic!("we dont null here"),
        Value::Bool(b) => Ok(Record::Bool(b)),
        // going to have to deal with floats and i guess maybe bignums ?
        // serde wants a u64 or a float here...does this even get generated?
        Value::Number(n) => Ok(Record::Int(n.as_u64().unwrap().to_bigint().unwrap())),
        Value::String(s) => Ok(Record::String(s)),
        Value::Array(a) => {
            let mut values = Vec::new();
            for v in a {
                values.push(value_to_record(v)?);
            }
            Ok(Record::Array(CollectionKind::Vector, values))
        }
        Value::Object(m) => {
            for (k, v) in m {
                match k.as_str() {
                    "Serialized" => {
                        if let Value::Array(x) = &v {
                            if let Value::String(x) = &x[1] {
                                if let Some(x) = BigInt::parse_bytes(x.as_bytes(), 10) {
                                    return Ok(Record::Int(x));
                                }
                            }
                        }
                        return Err(Error::new("unhandled serialized format".to_string()));
                    }
                    // there should be a way to extract Some and error otherwise
                    "String" => return Ok(Record::String(v.as_str().unwrap().to_string())),
                    _ => println!("non int value"),
                };
            }
            Err(Error::new("bad record json".to_string()))
        }
    }
}

fn record_to_value(r: Record) -> Result<Value, Error> {
    match r {
        Record::Bool(b) => Ok(Value::Bool(b)),
        Record::Int(n) => {
            let num = n
                .to_bigint()
                .ok_or_else(|| Error::new("json bigint conversion".to_string()))?;
            let fixed = num
                .to_i64()
                .ok_or_else(|| Error::new("json bigint conversion".to_string()))?;
            Ok(serde_json::Value::Number(serde_json::Number::from(fixed)))
        }

        Record::String(s) => Ok(Value::String(s)),
        Record::Array(_i, _v) => panic!("foo"),
        Record::NamedStruct(_collection_kind, _v) => panic!("bbar"),
        _ => Err(Error::new("unhanded record format".to_string())),
    }
}

struct RecordSetVisitor {}

impl<'de> Visitor<'de> for RecordSetVisitor {
    type Value = RecordSet;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "batch")
    }

    fn visit_seq<E>(self, mut e: E) -> Result<Self::Value, E::Error>
    where
        E: SeqAccess<'de>,
    {
        {
            let mut bn = RecordSet::new();

            let records: Option<HashMap<String, Vec<HashMap<String, Value>>>> = e.next_element()?;
            match records {
                Some(r) => {
                    let mut records = Vec::new();
                    for (r, valueset) in r.into_iter() {
                        for fact in valueset {
                            let mut properties = Vec::new();
                            for (k, v) in fact {
                                properties.push((
                                    Cow::from(k),
                                    value_to_record(v).expect("value translation"),
                                ));
                            }
                            // xxx weight
                            records
                                .push((Record::NamedStruct(Cow::from(r.clone()), properties), 1));
                        }
                    }
                    bn.records = records;
                }
                // can't figure out how to throw an error here Err(Error::new("bad record batch syntax".to_string())),
                None => panic!("bad record batch syntax"),
            }
            Ok(bn)
        }
    }
}

impl<'de> Deserialize<'de> for RecordSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let b: RecordSet = deserializer.deserialize_any(RecordSetVisitor {})?;
        Ok(b)
    }
}

impl Serialize for RecordSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut m = HashMap::<String, Vec<HashMap<String, Record>>>::new();
        let mut tup = serializer.serialize_tuple(2)?;

        // need to encode w !
        for (v, _w) in &self.records {
            match v {
                Record::NamedStruct(relname, v) => {
                    m.entry(relname.to_string()).or_insert_with(Vec::new).push({
                        // wanted to use map...but some trait bound something something
                        let mut out = HashMap::<String, Record>::new();
                        for (k, v) in v {
                            out.insert(
                                k.to_string(),
                                match v {
                                    Record::Int(x) => {
                                        Record::Serialized(Cow::from("Bigint"), x.to_string())
                                    }
                                    _ => v.clone(),
                                },
                            );
                        }
                        out
                    })
                }
                _ => panic!("weird stuff in record batch"),
            }
        }
        tup.serialize_element(&m)?;
        tup.end()
    }
}

impl RecordSet {
    pub fn new() -> RecordSet {
        RecordSet {
            records: Vec::new(),
        }
    }

    pub fn singleton(rec: Record, weight: isize) -> RecordSet {
        RecordSet {
            records: vec![(rec, weight)],
        }
    }

    // Record::NamedStruct((_r, _)) = v
    pub fn insert(&mut self, _r: String, v: Record, weight: isize) {
        self.records.push((v, weight))
    }

    // tried to use impl From<Batch> for RecordSet, but no error path, other type issues
    // why no err?
    pub fn from(eval: Evaluator, f: FactSet) -> RecordSet {
        match f {
            FactSet::Value(x) => {
                let mut rb = RecordSet::new();
                for (record, val, weight) in &x {
                    let rel_name = eval.clone().relation_name_from_id(record).unwrap();
                    let _record: Record = eval.clone().record_from_ddvalue(val).unwrap();
                    let val = match _record {
                        // [ weight, actual_record ]
                        Record::Tuple(t) => t[1].clone(),
                        Record::NamedStruct(name, rec) => Record::NamedStruct(name, rec),
                        _ => panic!("unknown type!"),
                    };
                    rb.insert(rel_name, val, weight);
                }
                rb
            }
            FactSet::Record(x) => x,
            FactSet::Empty() => RecordSet::new(),
        }
    }
}

pub struct RecordSetIterator<'a> {
    items: Box<dyn Iterator<Item = (Record, isize)> + Send + 'a>,
}

impl<'a> Iterator for RecordSetIterator<'a> {
    type Item = (String, Record, isize);

    fn next(&mut self) -> Option<(String, Record, isize)> {
        match self.items.next() {
            Some((Record::NamedStruct(name, val), w)) => {
                Some(((*name).to_string(), Record::NamedStruct(name, val), w))
            }
            _ => None,
        }
    }
}

impl<'a> IntoIterator for &'a RecordSet {
    type Item = (String, Record, isize);
    type IntoIter = RecordSetIterator<'a>;

    fn into_iter(self) -> RecordSetIterator<'a> {
        RecordSetIterator {
            items: Box::new(self.records.clone().into_iter()),
        }
    }
}

// idk why i dont want to make these associated...i guess holding on to the idea
// that the external representation doesn't need to be tied to the internal.
pub fn serialize_record_set(r: RecordSet) -> Result<Vec<u8>, Error> {
    let encoded = serde_json::to_string(&r)?;
    Ok(encoded.as_bytes().to_vec())
}

pub fn deserialize_record_set(v: Vec<u8>) -> Result<RecordSet, Error> {
    let s = std::str::from_utf8(&v)?;
    Ok(serde_json::from_str(&s)?)
}
