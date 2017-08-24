use chrono::{DateTime, Utc, Timelike};
use byteorder::{WriteBytesExt, LittleEndian};
use fnv::FnvHashMap;

use term_vector::TermVector;
use schema::FieldId;
use segment::SegmentId;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct DocId(pub SegmentId, pub u16);

#[derive(Debug, Clone)]
pub enum FieldValue {
    String(String),
    Integer(i64),
    Boolean(bool),
    DateTime(DateTime<Utc>),
}

impl FieldValue {
    pub fn to_bytes(&self) -> Vec<u8> {
        match *self {
            FieldValue::String(ref string) => {
                let mut bytes = Vec::with_capacity(string.len());

                for byte in string.as_bytes() {
                    bytes.push(*byte);
                }

                bytes
            }
            FieldValue::Integer(value) => {
                let mut bytes = Vec::with_capacity(8);
                bytes.write_i64::<LittleEndian>(value).unwrap();
                bytes
            }
            FieldValue::Boolean(value) => {
                if value {
                    vec![b't']
                } else {
                    vec![b'f']
                }
            }
            FieldValue::DateTime(value) => {
                let mut bytes = Vec::with_capacity(0);
                let timestamp = value.timestamp();
                let micros = value.nanosecond() / 1000;
                let timestamp_with_micros = timestamp * 1000000 + micros as i64;
                bytes.write_i64::<LittleEndian>(timestamp_with_micros).unwrap();
                bytes
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Document {
    pub key: String,
    pub indexed_fields: FnvHashMap<FieldId, TermVector>,
    pub stored_fields: FnvHashMap<FieldId, FieldValue>,
}
