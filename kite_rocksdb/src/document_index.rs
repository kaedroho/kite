use std::sync::RwLock;
use std::collections::HashMap;
use std::io::Cursor;

use rocksdb::{self, DB, WriteBatch};
use roaring::RoaringBitmap;
use kite::document::DocRef;
use byteorder::{ByteOrder, LittleEndian};

use key_builder::KeyBuilder;
use segment_ops::SegmentMergeError;


/// Manages the index's "document index"
pub struct DocumentIndexManager {
    primary_key_index: RwLock<HashMap<Vec<u8>, DocRef>>,
}


impl DocumentIndexManager {
    /// Generates a new document index
    pub fn new(_db: &DB) -> Result<DocumentIndexManager, rocksdb::Error> {
        Ok(DocumentIndexManager {
            primary_key_index: RwLock::new(HashMap::new()),
        })
    }

    /// Loads the document index from an index
    pub fn open(db: &DB) -> Result<DocumentIndexManager, rocksdb::Error> {
        // Read primary key index
        let mut primary_key_index = HashMap::new();
        let mut iter = db.raw_iterator();
        iter.seek(b"k");
        while iter.valid() {
            let k = iter.key().unwrap();

            if k[0] != b'k' {
                break;
            }

            let v = iter.value().unwrap();
            let segment = LittleEndian::read_u32(&v[0..4]);
            let ord = LittleEndian::read_u16(&v[4..6]);
            let doc_ref = DocRef::from_segment_ord(segment, ord);

            primary_key_index.insert(k[1..].to_vec(), doc_ref);

            iter.next();
        }

        Ok(DocumentIndexManager {
            primary_key_index: RwLock::new(primary_key_index),
        })
    }

    fn delete_document_by_ref_unchecked(&self, write_batch: &mut WriteBatch, doc_ref: DocRef) -> Result<(), rocksdb::Error> {
        let kb = KeyBuilder::segment_del_list(doc_ref.segment());
        let mut previous_doc_id_bytes = [0; 2];
        LittleEndian::write_u16(&mut previous_doc_id_bytes, doc_ref.ord());
        try!(write_batch.merge(&kb.key(), &previous_doc_id_bytes));

        // Increment deleted docs
        let kb = KeyBuilder::segment_stat(doc_ref.segment(), b"deleted_docs");
        let mut inc_bytes = [0; 8];
        LittleEndian::write_i64(&mut inc_bytes, 1);
        try!(write_batch.merge(&kb.key(), &inc_bytes));

        Ok(())
    }

    pub fn insert_or_replace_key(&self, db: &DB, key: &Vec<u8>, doc_ref: DocRef) -> Result<Option<DocRef>, rocksdb::Error> {
        // Update primary_key_index
        let mut write_batch = WriteBatch::default();
        let previous_doc_ref = self.primary_key_index.write().unwrap().insert(key.clone(), doc_ref);

        let kb = KeyBuilder::primary_key_index(key);
        let mut doc_ref_bytes = [0; 6];
        LittleEndian::write_u32(&mut doc_ref_bytes, doc_ref.segment());
        LittleEndian::write_u16(&mut doc_ref_bytes[4..], doc_ref.ord());
        try!(write_batch.put(&kb.key(), &doc_ref_bytes));

        // If there was a document there previously, delete it
        if let Some(previous_doc_ref) = previous_doc_ref {
            try!(self.delete_document_by_ref_unchecked(&mut write_batch, previous_doc_ref));
        }

        // Write document data
        try!(db.write(write_batch));

        Ok(previous_doc_ref)
    }

    pub fn delete_document_by_key(&self, db: &DB, key: &Vec<u8>) -> Result<Option<DocRef>, rocksdb::Error> {
        // Remove document from index
        let doc_ref = self.primary_key_index.write().unwrap().remove(key);

        if let Some(doc_ref) = doc_ref {
            let mut write_batch = WriteBatch::default();

            try!(self.delete_document_by_ref_unchecked(&mut write_batch, doc_ref));

            try!(db.write(write_batch));
        }

        Ok(doc_ref)
    }

    pub fn contains_document_key(&self, key: &Vec<u8>) -> bool {
        self.primary_key_index.read().unwrap().contains_key(key)
    }

    pub fn commit_segment_merge(&self, db: &DB, mut write_batch: WriteBatch, source_segments: &Vec<u32>, dest_segment: u32, doc_ref_mapping: &HashMap<DocRef, u16>) -> Result<(), SegmentMergeError> {
        // Lock the primary key index
        let mut primary_key_index = self.primary_key_index.write().unwrap();

        // Update primary keys to point to their new locations
        let mut keys_to_update: HashMap<Vec<u8>, DocRef> = HashMap::with_capacity(doc_ref_mapping.len());
        for (key, doc_ref) in primary_key_index.iter() {
            if doc_ref_mapping.contains_key(&doc_ref) {
                keys_to_update.insert(key.clone(), *doc_ref);
            }
        }

        for (key, doc_ref) in keys_to_update {
            let new_doc_ord = doc_ref_mapping.get(&doc_ref).unwrap();
            let new_doc_ref = DocRef::from_segment_ord(dest_segment, *new_doc_ord);

            let kb = KeyBuilder::primary_key_index(&key);
            let mut doc_ref_bytes = [0; 6];
            LittleEndian::write_u32(&mut doc_ref_bytes, new_doc_ref.segment());
            LittleEndian::write_u16(&mut doc_ref_bytes[4..], new_doc_ref.ord());
            try!(write_batch.put(&kb.key(), &doc_ref_bytes));

            primary_key_index.insert(key, new_doc_ref);
        }

        // Merge deletion lists
        // Must be done while the primary_key_index is locked as this prevents any more documents being deleted
        let mut deletion_list = RoaringBitmap::new();
        for source_segment in source_segments {
            let kb = KeyBuilder::segment_del_list(*source_segment);
            match try!(db.get(&kb.key())) {
                Some(bitmap) => {
                    let bitmap = RoaringBitmap::deserialize_from(Cursor::new(&bitmap[..])).unwrap();
                    for doc_id in bitmap.iter() {
                        let doc_ref = DocRef::from_segment_ord(*source_segment, doc_id as u16);
                        let new_doc_id = doc_ref_mapping.get(&doc_ref).unwrap();
                        deletion_list.insert(*new_doc_id as u32);
                    }
                }
                None => {},
            }
        }

        let mut dl_vec = Vec::new();
        deletion_list.serialize_into(&mut dl_vec).unwrap();

        let kb = KeyBuilder::segment_del_list(dest_segment);
        try!(db.put(&kb.key(), &dl_vec));

        // Commit!
        try!(db.write_without_wal(write_batch));

        Ok(())
    }
}
