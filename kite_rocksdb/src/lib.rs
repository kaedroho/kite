extern crate kite;
extern crate rocksdb;
extern crate serde_json;
extern crate roaring;
extern crate byteorder;
extern crate chrono;
extern crate fnv;

pub mod utils;
pub mod segment;
pub mod segment_builder;
pub mod segment_ops;
pub mod segment_stats;
pub mod search;

use std::str;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicUSize};

use rocksdb::{DB, Options, MergeOperands, Snapshot};
use kite::{Document, SegmentId, DocId, TermId};
use kite::document::FieldValue;
use kite::schema::{Schema, FieldType, FieldFlags, FieldId, AddFieldError};
use byteorder::{ByteOrder, LittleEndian};
use chrono::{NaiveDateTime, DateTime, Utc};
use fnv::FnvHashMap;

use utils::key::{Key, StatisticsKey};
use utils::write_batch::WriteBatch;

#[derive(Debug)]
pub enum DocumentInsertError {
    /// A RocksDB error occurred
    RocksDBError(rocksdb::Error),

    /// The segment is full
    SegmentFull,
}

impl From<rocksdb::Error> for DocumentInsertError {
    fn from(e: rocksdb::Error) -> DocumentInsertError {
        DocumentInsertError::RocksDBError(e)
    }
}

impl From<segment_builder::DocumentInsertError> for DocumentInsertError {
    fn from(e: segment_builder::DocumentInsertError) -> DocumentInsertError {
        match e {
            segment_builder::DocumentInsertError::SegmentFull => DocumentInsertError::SegmentFull,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct TermDictionaryId(u32);


pub struct TermDictionary {
    parent_id: Option<TermDictionaryId>,
    dictionary: HashMap<Term, TermId>,
}

pub struct RocksDBStore {
    db: DB,
    next_field_id: AtomicUSize,
    next_segment_id: AtomicUSize,
    next_term_dictionary_id: AtomicUSize,
}

impl RocksDBStore {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<RocksDBStore, String> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = try!(DB::open(&opts, path));

        db.put(b".next_field_id", b"1")?;
        db.put(b".next_segment_id", b"0")?;
        db.put(b".next_term_dictionary_id", b"1")?;

        Ok(RocksDBStore {
            db: db,
            next_field_id: AtomicUSize::new(1),
            next_segment_id: AtomicUSize::new(0),
            next_term_dictionary_id: AtomicUSize::new(1),
        })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<RocksDBStore, String> {
        let mut opts = Options::default();
        let db = try!(DB::open(&opts, path));

        let next_field_id = match db.get(b".next_field_id")? {
            Some(id) => {
                id.to_utf8().unwrap().parse::<usize>().unwrap()
            }
            None => 1,  // TODO: error
        };

        let next_segment_id = match db.get(b".next_segment_id")? {
            Some(id) => {
                id.to_utf8().unwrap().parse::<usize>().unwrap()
            }
            None => 1,  // TODO: error
        };

        let next_term_dictionary_id = match db.get(b".next_term_dictionary_id")? {
            Some(id) => {
                id.to_utf8().unwrap().parse::<usize>().unwrap()
            }
            None => 1,  // TODO: error
        };

        Ok(RocksDBStore {
            db: db,
            next_segment_id: AtomicUSize::new(next_segment_id),
            next_term_dictionary_id: AtomicUSize::new(next_term_dictionary_id),
        })
    }

    pub fn path(&self) -> &Path {
        self.db.path()
    }

    pub fn new_field_id(&self) -> Result<FieldId, rocksdb::Error> {
        let field_id = self.next_field_id.fetch_add(1, Ordering::SeqCst) as u32;
        self.db.put(b".next_field_id", (field_id + 1).to_string().as_bytes())?;
        Ok(field_id)
    }

    pub fn new_segment_id(&self) -> Result<SegmentId, rocksdb::Error> {
        let segment_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst) as u32;
        self.db.put(b".next_segment_id", (segment_id + 1).to_string().as_bytes())?;
        Ok(segment_id)
    }

    pub fn new_term_dictionary_id(&self) -> Result<TermDictionaryId, rocksdb::Error> {
        let term_dictionary_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst) as u32;
        self.db.put(b".next_term_dictionary_id", (term_dictionary_id + 1).to_string().as_bytes())?;
        Ok(term_dictionary_id)
    }

    pub fn add_field(&mut self, name: String, field_type: FieldType, field_flags: FieldFlags) -> Result<FieldId, AddFieldError> {
        unimplemented!()
    }

    pub fn delete_field(&mut self, field_ref: &FieldRef) -> bool {
        unimplemented!()
    }

    pub fn insert_or_update_document(&self, doc: &Document) -> Result<(), DocumentInsertError> {
        // Build segment in memory
        let mut builder = segment_builder::SegmentBuilder::new();
        builder.add_document(doc)?;

        // Write the segment
        let _ = self.write_segment(&builder)?;

        Ok(())
    }

    pub fn write_segment(&self, builder: &segment_builder::SegmentBuilder) -> Result<SegmentId, rocksdb::Error> {
        // Allocate a segment ID
        let segment_id = self.new_segment_id(&self.db)?;

        // Start write batch
        let mut write_batch = WriteBatch::new();

        // Set segment active flag, this will activate the segment as soon as the
        // write batch is written
        write_batch.put(&Key::segment_active(segment_id) , b"")?;

        // Merge the term dictionary
        // Writes new terms to disk and generates mapping between the builder's term dictionary and the real one
        let mut term_dictionary_map: FnvHashMap<TermRef, TermRef> = FnvHashMap::default();
        for (term, current_term_ref) in builder.term_dictionary.iter() {
            let new_term_ref = try!(self.term_dictionary.get_or_create(&self.db, term));
            term_dictionary_map.insert(*current_term_ref, new_term_ref);
        }

        // Write term directories
        for (&(field_id, term_id), term_directory) in builder.term_directories.iter() {
            let new_term_id = term_dictionary_map.get(&term_id).expect("TermRef not in term_dictionary_map");

            // Serialise
            let mut term_directory_bytes = Vec::new();
            term_directory.serialize_into(&mut term_directory_bytes).unwrap();

            // Write
            write_batch.put(&Key::term_directory(field_id, new_term_id, segment_id) , &term_directory_bytes)?;
        }

        // Write stored fields
        /*
        for (&(field_ref, doc_id, ref value_type), value) in builder.stored_field_values.iter() {
            let kb = KeyBuilder::stored_field_value(segment, doc_id, field_ref.ord(), value_type);
            try!(write_batch.put(&kb.key(), value));
        }
        */

        // Write statistics
        /*
        for (name, value) in builder.statistics.iter() {
            let kb = KeyBuilder::segment_stat(segment, name);

            let mut value_bytes = [0; 8];
            LittleEndian::write_i64(&mut value_bytes, *value);
            try!(write_batch.put(&kb.key(), &value_bytes));
        }
        */

        // Write data
        self.db.write(write_batch.inner);

        Ok(segment_id)
    }

    pub fn delete_document(&self, doc_id: DocId) -> Result<bool, rocksdb::Error> {
        // Release unique keys

        // Mark document as deleted
    }

    pub fn reader<'a>(&'a self) -> RocksDBReader<'a> {
        RocksDBReader {
            store: &self,
            snapshot: self.db.snapshot(),
        }
    }
}

impl fmt::Debug for RocksDBStore {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RocksDBStore {{ path: {:?} }}", self.db.path())
    }
}

pub enum StoredFieldReadError {
    /// The provided FieldRef wasn't valid for this index
    InvalidFieldRef(FieldRef),

    /// A RocksDB error occurred while reading from the disk
    RocksDBError(rocksdb::Error),

    /// A UTF-8 decode error occured while reading a Text field
    TextFieldUTF8DecodeError(Vec<u8>, str::Utf8Error),

    /// A boolean field was read but the value wasn't a boolean
    BooleanFieldDecodeError(Vec<u8>),

    /// An integer/datetime field was read but the value wasn't 8 bytes
    IntegerFieldValueSizeError(usize),
}

impl From<rocksdb::Error> for StoredFieldReadError {
    fn from(e: rocksdb::Error) -> StoredFieldReadError {
        StoredFieldReadError::RocksDBError(e)
    }
}

pub struct RocksDBReader<'a> {
    store: &'a RocksDBStore,
    snapshot: Snapshot<'a>
}

impl<'a> RocksDBReader<'a> {


/*
    pub fn read_stored_field(&self, field_ref: FieldRef, doc_ref: DocRef) -> Result<Option<FieldValue>, StoredFieldReadError> {
        let field_info = match self.schema().get(&field_ref) {
            Some(field_info) => field_info,
            None => return Err(StoredFieldReadError::InvalidFieldRef(field_ref)),
        };

        let kb = KeyBuilder::stored_field_value(doc_ref.segment(), doc_ref.ord(), field_ref.ord(), b"val");

        match try!(self.snapshot.get(&kb.key())) {
            Some(value) => {
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

                        Ok(Some(FieldValue::Integer(LittleEndian::read_i64(&value))))
                    }
                    FieldType::Boolean => {
                        if value[..] == [b't'] {
                            Ok(Some(FieldValue::Boolean(true)))
                        } else if value[..] == [b'f'] {
                            Ok(Some(FieldValue::Boolean(false)))
                        } else {
                            Err(StoredFieldReadError::BooleanFieldDecodeError(value.to_vec()))
                        }
                    }
                    FieldType::DateTime => {
                        if value.len() != 8 {
                            return Err(StoredFieldReadError::IntegerFieldValueSizeError(value.len()))
                        }

                        let timestamp_with_micros = LittleEndian::read_i64(&value);
                        let timestamp = timestamp_with_micros / 1000000;
                        let micros = timestamp_with_micros % 1000000;
                        let nanos = micros * 1000;
                        let datetime = NaiveDateTime::from_timestamp(timestamp, nanos as u32);
                        Ok(Some(FieldValue::DateTime(DateTime::from_utc(datetime, Utc))))
                    }
                }
            }
            None => Ok(None),
        }
    }
*/
}

#[cfg(test)]
mod tests {
    use std::fs::remove_dir_all;
    use std::path::Path;

    use rocksdb::DB;
    use fnv::FnvHashMap;
    use kite::{Term, Token, Document};
    use kite::document::FieldValue;
    use kite::schema::{FieldType, FIELD_INDEXED, FIELD_STORED};
    use kite::query::Query;
    use kite::query::term_scorer::TermScorer;
    use kite::collectors::top_score::TopScoreCollector;

    use super::RocksDBStore;

    fn remove_dir_all_ignore_error<P: AsRef<Path>>(path: P) {
        match remove_dir_all(&path) {
            Ok(_) => {}
            Err(_) => {}  // Don't care if this fails
        }
    }

    #[test]
    fn test_create() {
        remove_dir_all_ignore_error("test_indices/test_create");

        let store = RocksDBStore::create("test_indices/test_create");
        assert!(store.is_ok());
    }

    #[test]
    fn test_open() {
        remove_dir_all_ignore_error("test_indices/test_open");

        // Check that it fails to open a DB which doesn't exist
        let store = RocksDBStore::open("test_indices/test_open");
        assert!(store.is_err());

        // Create the DB
        RocksDBStore::create("test_indices/test_open").expect("failed to create test DB");

        // Now try and open it
        let store = RocksDBStore::open("test_indices/test_open");
        assert!(store.is_ok());
    }

    fn make_test_store(path: &str) -> RocksDBStore {
        let mut store = RocksDBStore::create(path).unwrap();
        let title_field = store.add_field("title".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
        let body_field = store.add_field("body".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
        let pk_field = store.add_field("pk".to_string(), FieldType::I64, FIELD_STORED).unwrap();


        let mut indexed_fields = FnvHashMap::default();
        indexed_fields.insert(
            title_field,
            vec![
                Token { term: Term::from_string("hello"), position: 1 },
                Token { term: Term::from_string("world"), position: 2 },
            ].into()
        );
        indexed_fields.insert(
            body_field,
            vec![
                Token { term: Term::from_string("lorem"), position: 1 },
                Token { term: Term::from_string("ipsum"), position: 2 },
                Token { term: Term::from_string("dolar"), position: 3 },
            ].into()
        );

        let mut stored_fields = FnvHashMap::default();
        stored_fields.insert(
            pk_field,
            FieldValue::Integer(1)
        );

        store.insert_or_update_document(&Document {
            key: "test_doc".to_string(),
            indexed_fields: indexed_fields,
            stored_fields: stored_fields,
        }).unwrap();

        let mut indexed_fields = FnvHashMap::default();
        indexed_fields.insert(
            title_field,
            vec![
                Token { term: Term::from_string("howdy"), position: 1 },
                Token { term: Term::from_string("partner"), position: 2 },
            ].into()
        );
        indexed_fields.insert(
            body_field,
            vec![
                Token { term: Term::from_string("lorem"), position: 1 },
                Token { term: Term::from_string("ipsum"), position: 2 },
                Token { term: Term::from_string("dolar"), position: 3 },
            ].into()
        );

        let mut stored_fields = FnvHashMap::default();
        stored_fields.insert(
            pk_field,
            FieldValue::Integer(2)
        );

        store.insert_or_update_document(&Document {
            key: "another_test_doc".to_string(),
            indexed_fields: indexed_fields,
            stored_fields: stored_fields,
        }).unwrap();

        store.merge_segments(&vec![1, 2]).unwrap();
        store.purge_segments(&vec![1, 2]).unwrap();

        store
    }

    pub fn print_keys(db: &DB) {
        fn bytes_to_string(bytes: &[u8]) -> String {
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

        let mut iter = db.raw_iterator();
        iter.seek_to_first();
        while iter.valid() {
            println!("{} = {:?}", bytes_to_string(&iter.key().unwrap()), iter.value().unwrap());

            iter.next();
        }
    }

    #[test]
    fn test() {
        remove_dir_all_ignore_error("test_indices/test");

        make_test_store("test_indices/test");

        let store = RocksDBStore::open("test_indices/test").unwrap();
        let title_field = store.schema.get_field_by_name("title").unwrap();

        let index_reader = store.reader();

        print_keys(&store.db);


        let query = Query::Disjunction {
            queries: vec![
                Query::Term {
                    field: title_field,
                    term: Term::from_string("howdy"),
                    scorer: TermScorer::default_with_boost(2.0f32),
                },
                Query::Term {
                    field: title_field,
                    term: Term::from_string("partner"),
                    scorer: TermScorer::default_with_boost(2.0f32),
                },
                Query::Term {
                    field: title_field,
                    term: Term::from_string("hello"),
                    scorer: TermScorer::default_with_boost(2.0f32),
                }
            ]
        };

        let mut collector = TopScoreCollector::new(10);
        index_reader.search(&mut collector, &query).unwrap();

        let docs = collector.into_sorted_vec();
        println!("{:?}", docs);
    }
}
