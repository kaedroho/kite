use std::collections::HashMap;
use std::ops::Deref;
use std::fmt;

use serde::{Serialize, Deserialize, Serializer, Deserializer};
use fnv::FnvHashMap;

bitflags! {
    pub flags FieldFlags: u32 {
        const FIELD_INDEXED = 0b00000001,
        const FIELD_STORED  = 0b00000010,
    }
}

impl Serialize for FieldFlags {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let mut flag_strings = Vec::new();

        if self.contains(FIELD_INDEXED) {
            flag_strings.push("INDEXED");
        }

        if self.contains(FIELD_STORED) {
            flag_strings.push("STORED");
        }

        serializer.serialize_str(&flag_strings.join("|"))
    }
}

impl<'a> Deserialize<'a> for FieldFlags {
    fn deserialize<D>(deserializer: D) -> Result<FieldFlags, D::Error>
        where D: Deserializer<'a>
    {
        struct Visitor;

        impl<'a> ::serde::de::Visitor<'a> for Visitor {
            type Value = FieldFlags;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string of flag names separated by a '|' character")
            }

            fn visit_str<E>(self, value: &str) -> Result<FieldFlags, E>
                where E: ::serde::de::Error
            {
                let mut flags = FieldFlags::empty();

                for flag_s in value.split("|") {
                    match flag_s {
                        "INDEXED" => {
                            flags |= FIELD_INDEXED;
                        }
                        "STORED" => {
                            flags |= FIELD_STORED;
                        }
                        _ => {} // TODO: error
                    }
                }

                Ok(flags)
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldType {
    Text,
    PlainString,
    I64,
    Boolean,
    DateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldInfo {
    name: String,
    pub field_type: FieldType,
    pub field_flags: FieldFlags,
}

impl FieldInfo {
    pub fn new(name: String, field_type: FieldType, field_flags: FieldFlags) -> FieldInfo {
        FieldInfo {
            name: name,
            field_type: field_type,
            field_flags: field_flags,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct FieldRef(u32);

// FieldRef needs to be serialised as a string as it's used as a mapping key
impl Serialize for FieldRef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'a> Deserialize<'a> for FieldRef {
    fn deserialize<D>(deserializer: D) -> Result<FieldRef, D::Error>
        where D: Deserializer<'a>
    {
        struct Visitor;

        impl<'a> ::serde::de::Visitor<'a> for Visitor {
            type Value = FieldRef;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string containing an integer")
            }

            fn visit_str<E>(self, value: &str) -> Result<FieldRef, E>
                where E: ::serde::de::Error
            {
                match value.parse() {
                    Ok(value) => Ok(FieldRef::new(value)),
                    Err(_) => Err(E::invalid_value(::serde::de::Unexpected::Str(value), &"a string containing an integer")),
                }
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

impl FieldRef {
    pub fn new(ord: u32) -> FieldRef {
        FieldRef(ord)
    }

    pub fn ord(&self) -> u32 {
        self.0
    }
}

#[derive(Debug)]
pub enum AddFieldError {
    FieldAlreadyExists(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    next_field_id: u32,
    fields: FnvHashMap<FieldRef, FieldInfo>,
    field_names: HashMap<String, FieldRef>,
}

impl Schema {
    pub fn new() -> Schema {
        Schema {
            next_field_id: 1,
            fields: FnvHashMap::default(),
            field_names: HashMap::new(),
        }
    }

    fn new_field_ref(&mut self) -> FieldRef {
        let field_ref = FieldRef(self.next_field_id);
        self.next_field_id += 1;

        field_ref
    }

    pub fn get_field_by_name(&self, name: &str) -> Option<FieldRef> {
        self.field_names.get(name).cloned()
    }

    pub fn add_field(&mut self, name: String, field_type: FieldType, field_flags: FieldFlags) -> Result<FieldRef, AddFieldError> {
        if self.field_names.contains_key(&name) {
            return Err(AddFieldError::FieldAlreadyExists(name));
        }

        let field_ref = self.new_field_ref();
        let field_info = FieldInfo::new(name.clone(), field_type, field_flags);

        self.fields.insert(field_ref, field_info);
        self.field_names.insert(name, field_ref);

        Ok(field_ref)
    }

    pub fn remove_field(&mut self, field_ref: &FieldRef) -> bool {
        match self.fields.remove(field_ref) {
            Some(removed_field) => {
                self.field_names.remove(&removed_field.name);
                true
            }
            None => false
        }
    }
}

impl Deref for Schema {
    type Target = FnvHashMap<FieldRef, FieldInfo>;

    fn deref(&self) -> &FnvHashMap<FieldRef, FieldInfo> {
        &self.fields
    }
}
