use crate::{Error, Evaluator, FactSet, RecordSet};
use core::fmt;
use core::fmt::Display;
use differential_datalog::record::IntoRecord;
use differential_datalog::record::Record;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

// should be able to derive
#[derive(Clone, Serialize, Deserialize)]
pub struct Batch {
    pub meta: FactSet,
    pub data: FactSet,
}

impl Batch {
    // no meta
    pub fn into_record(self) -> Record {
        let v = Vec::new();
        Record::NamedStruct(Cow::from("batch".to_string()), v)
    }

    pub fn new(meta: FactSet, data: FactSet) -> Batch {
        Batch { meta, data }
    }

    pub fn serialize(self) -> Result<Vec<u8>, Error> {
        let encoded = serde_json::to_string(&self)?;
        Ok(encoded.as_bytes().to_vec())
    }

    pub fn deserialize(buffer: Vec<u8>) -> Result<Batch, Error> {
        let s = std::str::from_utf8(&buffer)?;
        let b: Batch = serde_json::from_str(s)?;
        Ok(b)
    }

    pub fn format(self, eval: Evaluator) -> String {
        let mut output = String::new();
        for (_r, f, w) in &RecordSet::from(eval.clone(), self.meta) {
            fmt::write(&mut output, format_args!("meta {} {}\n", f, w)).expect("fmt");
        }
        for (_r, f, w) in &RecordSet::from(eval.clone(), self.data) {
            fmt::write(&mut output, format_args!("data {} {}\n", f, w)).expect("fmt");
        }
        output
    }
}

impl Display for Batch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format!("({} {})", self.meta, self.data))
    }
}
