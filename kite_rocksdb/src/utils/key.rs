use kite::schema::FieldId;
use kite::term::TermId;
use kite::document::{SegmentId, DocId};

pub enum StatisticKey {

}

pub enum Key {
    /// Contains metadata about the index
    Metadata {
        name: String,
    },

    /// Field definition
    FieldDefinition {
        field_id: FieldId,
    },

    /// Active segment
    /// If present, indicates the segment is active
    ActiveSegment {
        segment_id: SegmentId,
    },

    /// Stores the value of a field in a document
    DocumentFieldValue {
        doc_id: DocId,
        field_id: FieldId,
    },

    /// Indicates the document has been deleted
    /// This key has no value
    DeletedDocument {
        doc_id: DocId,
    },

    /// Stores a term dictionary, a mapping of term bytestrings to their IDs
    /// The term dictionary also contains a reference to the "parent" term dictionary
    /// if it has one, and the ID of the latest term in the dictionary
    /// The term dictionary itself is an FST data structure
    TermDictionary {
        term_dictionary_id: TermDictionaryId,
    },

    /// Stores a term directory, which is a list of document IDs that contain
    /// the term in the specified field/segment
    /// The directory itself is a roaring bitmap segment
    TermDirectory {
        field_id: FieldId,
        term_id: TermId,
        segment_id: SegmentId,
    },

    /// A unique key
    /// This maps key fields to documents in the index
    UniqueKey {
        field_id: FieldID,
        key: String,
    },

    /// A precomputed statistic
    Statistic(StatisticKey),
}

impl Key {
    pub fn from_bytes(bytes: &[u8]) -> Option<Key> {
        None
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        unimplemented!()
    }

    pub fn metadata(name: String) -> Key {
        Key::Metadata { name: name }
    }

    pub fn field_definition(field_id: FieldId) -> Key {
        Key::FieldDefinition { field_id: field_id }
    }

    pub fn active_segment(segment_id: SegmentId) -> Key {
        Key::ActiveSegment { segment_id: segment_id }
    }

    pub fn document_field_value(doc_id: DocId, field_id: FieldId) -> Key {
        DocumentFieldValue {
            doc_id: doc_id,
            field_id: field_id,
        }
    }

    pub fn deleted_document(doc_id: DocId) -> Key {
        DeletedDocument { doc_id: doc_id }
    }

    pub fn term_dictionary(term_dictionary_id: TermDictionaryId) -> Key {
        TermDictionary {
            term_dictionary_id: term_dictionary_id,
        }
    }

    pub fn term_directory(field_id: FieldId, term_id: TermId, segment_id: SegmentId) -> Key {
        TermDirectory {
            field_id: field_id,
            term_id: term_id,
            segment_id: segment_id,
        }
    }

    pub fn unique_key(field_id: FieldId, key: String) -> Key {
        TermDirectory {
            field_id: field_id,
            key: key,
        }
    }
}
