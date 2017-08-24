use std::collections::HashMap;
use std::ops::Deref;
use std::fmt;

use serde::{Serialize, Deserialize, Serializer, Deserializer};
use fnv::FnvHashMap;

bitflags! {
    pub flags FieldFlags: u32 {
        const FIELD_INDEXED = 0b00000001,
        const FIELD_STORED  = 0b00000010,
        const FIELD_UNIQUE  = 0b00000100,
        const FIELD_DELETED  = 0b00001000,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldDataType {
    Text,
    PlainString,
    Integer,
    Boolean,
    DateTime,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct FieldId(pub u16);

#[derive(Debug, Clone)]
pub struct FieldDefinition {
    name: String,
    data_type: FieldDataType,
    flags: FieldFlags,
}

impl FieldDefinition {
    pub fn new(name: String, data_type: FieldDataType, flags: FieldFlags) -> FieldDefinition {
        FieldDefinition {
            name: name,
            data_type: data_type,
            flags: flags,
        }
    }
}

#[derive(Debug)]
pub enum AddFieldError {
    FieldAlreadyExists(String),
}
