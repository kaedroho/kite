#![feature(integer_atomics)]
#![feature(slice_patterns)]

extern crate kite;
extern crate rocksdb;
extern crate rustc_serialize;
#[macro_use]
extern crate maplit;
extern crate byteorder;
extern crate chrono;

pub mod errors;
pub mod key_builder;
pub mod segment;
pub mod segment_merge;
pub mod term_dictionary;
pub mod document_index;
pub mod search;

use std::str;
use std::sync::{Arc, RwLock};
use std::collections::{BTreeMap, HashMap};

use rocksdb::{DB, WriteBatch, Writable, Options, MergeOperands};
use rocksdb::rocksdb::Snapshot;
use kite::Document;
use kite::document::FieldValue;
use kite::schema::{Schema, FieldType, FieldFlags, FieldRef, AddFieldError};
use rustc_serialize::json;
use byteorder::{ByteOrder, BigEndian};
use chrono::{NaiveDateTime, DateTime, UTC};

use errors::{RocksDBReadError, RocksDBWriteError};
use key_builder::KeyBuilder;
use segment::SegmentManager;
use term_dictionary::TermDictionaryManager;
use document_index::{DocumentIndexManager, DocRef};


fn merge_keys(key: &[u8], existing_val: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
    match key[0] {
        b'd' | b'x' => {
            // Sequence of two byte document ids
            // d = directory
            // x = deletion list

            // Allocate vec for new Value
            let new_size = match existing_val {
                Some(existing_val) => existing_val.len(),
                None => 0,
            } + operands.size_hint().0 * 2;

            let mut new_val = Vec::with_capacity(new_size);

            // Push existing value
            existing_val.map(|v| {
                for b in v {
                    new_val.push(*b);
                }
            });

            // Append new entries
            for op in operands {
                for b in op {
                    new_val.push(*b);
                }
            }

            new_val
        }
        b's' => {
            // Statistic
            // An i64 number that can be incremented or decremented
            let mut value = match existing_val {
                Some(existing_val) => BigEndian::read_i64(existing_val),
                None => 0
            };

            for op in operands {
                value += BigEndian::read_i64(op);
            }

            let mut buf = [0; 8];
            BigEndian::write_i64(&mut buf, value);
            buf.iter().cloned().collect()
        }
        _ => {
            // Unrecognised key, fallback to emulating a put operation (by taking the last value)
            operands.last().unwrap().iter().cloned().collect()
        }
    }
}


pub enum DocumentInsertError {
    /// The specified field name doesn't exist
    FieldDoesntExist(String),

    /// A RocksDB error occurred while reading from the disk
    RocksDBReadError(RocksDBReadError),

    /// A RocksDB error occurred while writing to the disk
    RocksDBWriteError(RocksDBWriteError),
}


impl From<RocksDBReadError> for DocumentInsertError {
    fn from(e: RocksDBReadError) -> DocumentInsertError {
        DocumentInsertError::RocksDBReadError(e)
    }
}


impl From<RocksDBWriteError> for DocumentInsertError {
    fn from(e: RocksDBWriteError) -> DocumentInsertError {
        DocumentInsertError::RocksDBWriteError(e)
    }
}


pub struct RocksDBIndexStore {
    schema: Arc<Schema>,
    db: DB,
    term_dictionary: TermDictionaryManager,
    segments: SegmentManager,
    document_index: DocumentIndexManager,
    doc_key_mapping: RwLock<BTreeMap<Vec<u8>, DocRef>>,
}


impl RocksDBIndexStore {
    pub fn create(path: &str) -> Result<RocksDBIndexStore, String> {
        let mut opts = Options::default();
        opts.add_merge_operator("merge operator", merge_keys);
        opts.create_if_missing(true);
        let db = try!(DB::open(&opts, path));

        // Schema
        let schema = Schema::new();
        db.put(b".schema", json::encode(&schema).unwrap().as_bytes());

        // Segment manager
        let segments = SegmentManager::new(&db);

        // Term dictionary manager
        let term_dictionary = TermDictionaryManager::new(&db);

        // Document index
        let document_index = DocumentIndexManager::new(&db);

        Ok(RocksDBIndexStore {
            schema: Arc::new(schema),
            db: db,
            term_dictionary: term_dictionary,
            segments: segments,
            document_index: document_index,
            doc_key_mapping: RwLock::new(BTreeMap::new()),
        })
    }

    pub fn open(path: &str) -> Result<RocksDBIndexStore, String> {
        let mut opts = Options::default();
        opts.add_merge_operator("merge operator", merge_keys);
        let db = try!(DB::open(&opts, path));

        let schema = match db.get(b".schema") {
            Ok(Some(schema)) => {
                let schema = schema.to_utf8().unwrap().to_string();
                json::decode(&schema).unwrap()
            }
            Ok(None) => Schema::new(),  // TODO: error
            Err(_) => Schema::new(),  // TODO: error
        };

        // Segment manager
        let segments = SegmentManager::open(&db);

        // Term dictionary manager
        let term_dictionary = TermDictionaryManager::open(&db);

        // Document index
        let document_index = DocumentIndexManager::open(&db);

        Ok(RocksDBIndexStore {
            schema: Arc::new(schema),
            db: db,
            term_dictionary: term_dictionary,
            segments: segments,
            document_index: document_index,
            doc_key_mapping: RwLock::new(BTreeMap::new()),
        })
    }

    pub fn add_field(&mut self, name: String, field_type: FieldType, field_flags: FieldFlags) -> Result<FieldRef, AddFieldError> {
        let mut schema_copy = (*self.schema).clone();
        let field_ref = try!(schema_copy.add_field(name, field_type, field_flags));
        self.schema = Arc::new(schema_copy);

        self.db.put(b".schema", json::encode(&self.schema).unwrap().as_bytes());

        Ok(field_ref)
    }

    pub fn remove_field(&mut self, field_ref: &FieldRef) -> bool {
        let mut schema_copy = (*self.schema).clone();
        let field_removed = schema_copy.remove_field(field_ref);

        if field_removed {
            self.schema = Arc::new(schema_copy);
            self.db.put(b".schema", json::encode(&self.schema).unwrap().as_bytes());
        }

        field_removed
    }

    pub fn insert_or_update_document(&self, doc: Document) -> Result<(), DocumentInsertError> {
        // Allocate a new segment for the document
        // Segment merges are very slow so we should avoid doing them at runtime
        // which is why each new document is created in a fresh segment.
        // Later on, a background process will come and merge any small segments
        // together. (For best performance, documents should be
        // inserted/updated in batches)
        let segment = self.segments.new_segment(&self.db);

        // Create doc ref
        let doc_ref = DocRef::from_segment_ord(segment, 0);

        // Start write batch
        let write_batch = WriteBatch::default();

        // Set segment active flag, this will activate the segment as soon as the
        // write batch is written
        let kb = KeyBuilder::segment_active(doc_ref.segment());
        if let Err(e) = write_batch.put(&kb.key(), b"") {
            return Err(RocksDBWriteError::new_put(kb.key().to_vec(), e).into());
        }

        // Insert contents

        // Indexed fields
        let mut term_frequencies = HashMap::new();
        for (field_name, tokens) in doc.indexed_fields.iter() {
            let field_ref = match self.schema.get_field_by_name(field_name) {
                Some(field_ref) => field_ref,
                None => return Err(DocumentInsertError::FieldDoesntExist(field_name.clone())),
            };

            let mut field_token_count = 0;

            for token in tokens.iter() {
                field_token_count += 1;

                let term_ref = self.term_dictionary.get_or_create(&self.db, &token.term);

                // Term frequency
                let mut term_frequency = term_frequencies.entry(term_ref).or_insert(0);
                *term_frequency += 1;

                // Write directory list
                let kb = KeyBuilder::segment_dir_list(doc_ref.segment(), field_ref.ord(), term_ref.ord());
                let mut doc_id_bytes = [0; 2];
                BigEndian::write_u16(&mut doc_id_bytes, doc_ref.ord());
                if let Err(e) = write_batch.merge(&kb.key(), &doc_id_bytes) {
                    return Err(RocksDBWriteError::new_merge(kb.key().to_vec(), e).into());
                }
            }

            // Term frequencies
            for (term_ref, frequency) in term_frequencies.drain() {
                // Write term frequency
                // 1 is by far the most common frequency. At search time, we interpret a missing
                // key as meaning there is a term frequency of 1
                if frequency != 1 {
                    let mut value_type = vec![b't', b'f'];
                    value_type.extend(term_ref.ord().to_string().as_bytes());
                    let kb = KeyBuilder::stored_field_value(doc_ref.segment(), doc_ref.ord(), field_ref.ord(), &value_type);
                    let mut frequency_bytes = [0; 8];
                    BigEndian::write_i64(&mut frequency_bytes, frequency);
                    if let Err(e) = write_batch.merge(&kb.key(), &frequency_bytes) {
                        return Err(RocksDBWriteError::new_merge(kb.key().to_vec(), e).into());
                    }
                }

                // Increment term document frequency
                let kb = KeyBuilder::segment_stat_term_doc_frequency(doc_ref.segment(), field_ref.ord(), term_ref.ord());
                let mut inc_bytes = [0; 8];
                BigEndian::write_i64(&mut inc_bytes, 1);
                if let Err(e) = write_batch.merge(&kb.key(), &inc_bytes) {
                    return Err(RocksDBWriteError::new_merge(kb.key().to_vec(), e).into());
                }
            }

            // Field length
            // Used by the BM25 similarity model
            let length = ((field_token_count as f64).sqrt() - 1.0) * 3.0;
            let length = if length > 255.0 { 255.0 } else { length } as u8;
            if length != 0 {
                let kb = KeyBuilder::stored_field_value(doc_ref.segment(), doc_ref.ord(), field_ref.ord(), b"len");
                if let Err(e) = write_batch.merge(&kb.key(), &[length]) {
                    return Err(RocksDBWriteError::new_merge(kb.key().to_vec(), e).into());
                }
            }

            // Increment total field docs
            let kb = KeyBuilder::segment_stat_total_field_docs(doc_ref.segment(), field_ref.ord());
            let mut inc_bytes = [0; 8];
            BigEndian::write_i64(&mut inc_bytes, 1);
            if let Err(e) = write_batch.merge(&kb.key(), &inc_bytes) {
                return Err(RocksDBWriteError::new_merge(kb.key().to_vec(), e).into());
            }

            // Increment total field tokens
            let kb = KeyBuilder::segment_stat_total_field_tokens(doc_ref.segment(), field_ref.ord());
            let mut inc_bytes = [0; 8];
            BigEndian::write_i64(&mut inc_bytes, field_token_count);
            if let Err(e) = write_batch.merge(&kb.key(), &inc_bytes) {
                return Err(RocksDBWriteError::new_merge(kb.key().to_vec(), e).into());
            }
        }

        // Stored fields
        for (field_name, value) in doc.stored_fields.iter() {
            let field_ref = match self.schema.get_field_by_name(field_name) {
                Some(field_ref) => field_ref,
                None => {
                    // TODO: error?
                    continue;
                }
            };

            let kb = KeyBuilder::stored_field_value(doc_ref.segment(), doc_ref.ord(), field_ref.ord(), b"val");
            if let Err(e) = write_batch.merge(&kb.key(), &value.to_bytes()) {
                return Err(RocksDBWriteError::new_merge(kb.key().to_vec(), e).into());
            }
        }

        // Increment total docs
        let kb = KeyBuilder::segment_stat(doc_ref.segment(), b"total_docs");
        let mut inc_bytes = [0; 8];
        BigEndian::write_i64(&mut inc_bytes, 1);
        if let Err(e) = write_batch.merge(&kb.key(), &inc_bytes) {
            return Err(RocksDBWriteError::new_merge(kb.key().to_vec(), e).into());
        }

        // Write document data
         if let Err(e) = self.db.write(write_batch) {
            return Err(RocksDBWriteError::new_commit_write_batch(e).into());
        }

        // Update document index
        try!(self.document_index.insert_or_replace_key(&self.db, &doc.key.as_bytes().iter().cloned().collect(), doc_ref));

        Ok(())
    }

    pub fn remove_document_by_key(&self, doc_key: &str) -> Result<bool, RocksDBWriteError> {
        match try!(self.document_index.delete_document_by_key(&self.db, &doc_key.as_bytes().iter().cloned().collect())) {
            Some(_doc_ref) => Ok(true),
            None => Ok(false),
        }
    }

    pub fn reader<'a>(&'a self) -> RocksDBIndexReader<'a> {
        RocksDBIndexReader {
            store: &self,
            snapshot: self.db.snapshot(),
        }
    }
}


pub enum StoredFieldReadError {
    /// The provided FieldRef wasn't valid for this index
    InvalidFieldRef(FieldRef),

    /// A RocksDB error occurred while reading from the disk
    RocksDBReadError(RocksDBReadError),

    /// A UTF-8 decode error occured while reading a Text field
    TextFieldUTF8DecodeError(Vec<u8>, str::Utf8Error),

    /// A boolean field was read but the value wasn't a boolean
    BooleanFieldDecodeError(Vec<u8>),

    /// An integer/datetime field was read but the value wasn't 8 bytes
    IntegerFieldValueSizeError(usize),
}


impl From<RocksDBReadError> for StoredFieldReadError {
    fn from(e: RocksDBReadError) -> StoredFieldReadError {
        StoredFieldReadError::RocksDBReadError(e)
    }
}


pub struct RocksDBIndexReader<'a> {
    store: &'a RocksDBIndexStore,
    snapshot: Snapshot<'a>
}


impl<'a> RocksDBIndexReader<'a> {
    pub fn schema(&self) -> &Schema {
        &self.store.schema
    }

    pub fn contains_document_key(&self, doc_key: &str) -> bool {
        // TODO: use snapshot
        self.store.document_index.contains_document_key(&doc_key.as_bytes().iter().cloned().collect())
    }

    pub fn read_stored_field(&self, field_ref: FieldRef, doc_ref: DocRef) -> Result<Option<FieldValue>, StoredFieldReadError> {
        let field_info = match self.schema().get(&field_ref) {
            Some(field_info) => field_info,
            None => return Err(StoredFieldReadError::InvalidFieldRef(field_ref)),
        };

        let kb = KeyBuilder::stored_field_value(doc_ref.segment(), doc_ref.ord(), field_ref.ord(), b"val");

        match self.snapshot.get(&kb.key()) {
            Ok(Some(value)) => {
                match field_info.field_type {
                    FieldType::Text | FieldType::PlainString => {
                        match str::from_utf8(&value) {
                            Ok(value_str) => {
                                Ok(Some(FieldValue::String(value_str.to_string())))
                            }
                            Err(e) => {
                                Err(StoredFieldReadError::TextFieldUTF8DecodeError(value.to_vec(), e))
                            }
                        }
                    }
                    FieldType::I64 => {
                        if value.len() != 8 {
                            return Err(StoredFieldReadError::IntegerFieldValueSizeError(value.len()));
                        }

                        Ok(Some(FieldValue::Integer(BigEndian::read_i64(&value))))
                    }
                    FieldType::Boolean => {
                        match value[..] {
                            [b't'] => Ok(Some(FieldValue::Boolean(true))),
                            [b'f'] => Ok(Some(FieldValue::Boolean(false))),
                            _ => {
                                Err(StoredFieldReadError::BooleanFieldDecodeError(value.to_vec()))
                            }
                        }
                    }
                    FieldType::DateTime => {
                        if value.len() != 8 {
                            return Err(StoredFieldReadError::IntegerFieldValueSizeError(value.len()))
                        }

                        let timestamp_with_micros = BigEndian::read_i64(&value);
                        let timestamp = timestamp_with_micros / 1000000;
                        let micros = timestamp_with_micros % 1000000;
                        let nanos = micros * 1000;
                        let datetime = NaiveDateTime::from_timestamp(timestamp, nanos as u32);
                        Ok(Some(FieldValue::DateTime(DateTime::from_utc(datetime, UTC))))
                    }
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(RocksDBReadError::new(kb.key().to_vec(), e).into())
        }
    }
}


#[cfg(test)]
mod tests {
    use std::fs::remove_dir_all;

    use rocksdb::{DB, Options, IteratorMode};
    use kite::{Term, Token, Document};
    use kite::document::FieldValue;
    use kite::schema::{FieldType, FIELD_INDEXED, FIELD_STORED};
    use kite::query::Query;
    use kite::query::term_scorer::TermScorer;
    use kite::collectors::top_score::TopScoreCollector;

    use super::RocksDBIndexStore;

    #[test]
    fn test_create() {
        remove_dir_all("test_indices/test_create");

        let store = RocksDBIndexStore::create("test_indices/test_create");
        assert!(store.is_ok());
    }

    #[test]
    fn test_open() {
        remove_dir_all("test_indices/test_open");

        let store = RocksDBIndexStore::open("test_indices/test_open");
        assert!(store.is_err());

        // Create DB
        let mut opts = Options::default();
        opts.create_if_missing(true);
        DB::open(&opts, "test_indices/test_open").unwrap();

        let store = RocksDBIndexStore::open("test_indices/test_open");
        assert!(store.is_ok());
    }

    fn make_test_store(path: &str) -> RocksDBIndexStore {
        let mut store = RocksDBIndexStore::create(path).unwrap();
        store.add_field("title".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
        store.add_field("body".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
        store.add_field("pk".to_string(), FieldType::I64, FIELD_STORED).unwrap();

        store.insert_or_update_document(Document {
            key: "test_doc".to_string(),
            indexed_fields: hashmap! {
                "title".to_string() => vec![
                    Token { term: Term::String("hello".to_string()), position: 1 },
                    Token { term: Term::String("world".to_string()), position: 2 },
                ],
                "body".to_string() => vec![
                    Token { term: Term::String("lorem".to_string()), position: 1 },
                    Token { term: Term::String("ipsum".to_string()), position: 2 },
                    Token { term: Term::String("dolar".to_string()), position: 3 },
                ],
            },
            stored_fields: hashmap! {
                "pk".to_string() => FieldValue::Integer(1),
            }
        });

        store.insert_or_update_document(Document {
            key: "another_test_doc".to_string(),
            indexed_fields: hashmap! {
                "title".to_string() => vec![
                    Token { term: Term::String("howdy".to_string()), position: 1 },
                    Token { term: Term::String("partner".to_string()), position: 2 },
                ],
                "body".to_string() => vec![
                    Token { term: Term::String("lorem".to_string()), position: 1 },
                    Token { term: Term::String("ipsum".to_string()), position: 2 },
                    Token { term: Term::String("dolar".to_string()), position: 3 },
                ],
            },
            stored_fields: hashmap! {
                "pk".to_string() => FieldValue::Integer(2),
            }
        });

        store.merge_segments(vec![1, 2]);

        store
    }

    pub fn print_keys(db: &DB) {
        fn bytes_to_string(bytes: &Box<[u8]>) -> String {
            use std::char;

            let mut string = String::new();

            for byte in bytes.iter() {
                if *byte < 128 {
                    // ASCII character
                    string.push(char::from_u32(*byte as u32).unwrap());
                } else {
                    string.push('?');
                }
            }

            string
        }

        for (key, value) in db.iterator(IteratorMode::Start) {
            println!("{} = {:?}", bytes_to_string(&key), value);
        }
    }

    #[test]
    fn test() {
        remove_dir_all("test_indices/test");

        make_test_store("test_indices/test");

        let store = RocksDBIndexStore::open("test_indices/test").unwrap();

        let index_reader = store.reader();

        print_keys(&store.db);


        let query = Query::Disjunction {
            queries: vec![
                Query::MatchTerm {
                    field: "title".to_string(),
                    term: Term::String("howdy".to_string()),
                    scorer: TermScorer::default_with_boost(2.0f64),
                },
                Query::MatchTerm {
                    field: "title".to_string(),
                    term: Term::String("partner".to_string()),
                    scorer: TermScorer::default_with_boost(2.0f64),
                },
                Query::MatchTerm {
                    field: "title".to_string(),
                    term: Term::String("hello".to_string()),
                    scorer: TermScorer::default_with_boost(2.0f64),
                }
            ]
        };

        let mut collector = TopScoreCollector::new(10);
        index_reader.search(&mut collector, &query);

        let docs = collector.into_sorted_vec();
        println!("{:?}", docs);
    }
}
