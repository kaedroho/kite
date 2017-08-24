use rocksdb::WriteBatch as RocksDBWriteBatch;

use super::key::Key;

pub struct WriteBatch {
    pub inner: rocksdb::WriteBatch,
}

impl WriteBatch {
    pub fn new() -> WriteBatch {
        WriteBatch {
            inner: rocksdb::WriteBatch::default(),
        }
    }

    pub fn put(&mut self, key: &Key, value: &[u8]) -> Result<(), rocksdb::Error> {
        self.inner.put(&key.to_bytes(), value)
    }

    pub fn delete(&mut self, key: &Key) -> Result<(), rocksdb::Error> {
        self.inner.delete(&key.to_bytes(), value)
    }
}
