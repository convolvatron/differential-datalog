use crate::{Error, FactSet};
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
        return Ok(Batch::new(FactSet::Empty(), FactSet::Empty()));
    }
}

impl Display for Batch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format!("({} {})", self.meta, self.data))
    }
}
