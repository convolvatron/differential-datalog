use crate::{RecordSet, ValueSet};
use core::fmt;
use core::fmt::Display;

use serde::{de::SeqAccess, de::Visitor, Deserialize, Deserializer};
use serde::{ser::SerializeMap, ser::SerializeTuple, Serialize, Serializer};

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
        // demux type
        let rs = RecordSet::deserialize(deserializer)?;
        Ok(FactSet::Record(rs))
    }
}

impl Serialize for FactSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            FactSet::Record(b) => b.serialize(serializer),
            FactSet::Value(_) => panic!("serialize value"),
            FactSet::Empty() => serializer.serialize_map(None)?.end(),
        }
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
