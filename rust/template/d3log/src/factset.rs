use crate::{RecordSet, ValueSet};
use core::fmt;
use core::fmt::Display;

use serde::{de::SeqAccess, de::Visitor, Deserialize, Deserializer};
use serde::{ser::SerializeTuple, Serialize, Serializer};

#[derive(Clone)]
pub enum FactSet {
    Value(ValueSet),
    Record(RecordSet),
    Empty(),
}

struct FactSetVisitor {}

impl<'de> Visitor<'de> for FactSetVisitor {
    type Value = FactSet;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "batch")
    }

    fn visit_seq<E>(self, mut e: E) -> Result<Self::Value, E::Error>
    where
        E: SeqAccess<'de>,
    {
        {
            let bn = RecordSet::new();
            Ok(FactSet::Record(bn))
        }
    }
}

impl<'de> Deserialize<'de> for FactSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let b = FactSet::Record(RecordSet::new());
        Ok(b)
    }
}

impl Serialize for FactSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let tup = serializer.serialize_tuple(2)?;
        tup.end()
    }
}

impl Display for FactSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FactSet::Value(b) => b.fmt(f),
            FactSet::Record(b) => b.fmt(f),
            FactSet::Empty() => f.write_str("<>"),
        }
    }
}
