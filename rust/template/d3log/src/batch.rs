use crate::{Error, Evaluator, FactSet, RecordSet};
use core::fmt;
use core::fmt::Display;
use serde::{Deserialize, Serialize};

// should be able to derive
#[derive(Clone, Serialize, Deserialize)]
pub struct Batch {
    pub meta: FactSet,
    pub data: FactSet,
}

impl Batch {
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
        return Ok(b);
    }

    pub fn format(self, eval: Evaluator) -> String {
        let mut output = String::new();
        for (_r, f, w) in &RecordSet::from(eval.clone(), self.data) {
            fmt::write(&mut output, format_args!("{} {}\n", f, w)).expect("fmt");
        }
        output
    }
}

impl Display for Batch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format!("({} {})", self.meta, self.data))
    }
}
