use std::fmt;
use std::io::{Cursor, Read};

use roaring::bitmap::{RoaringBitmap, Iter as RoaringBitmapIter};
use byteorder::{ByteOrder, BigEndian};


#[derive(Clone)]
pub struct DocIdSet {
    data: RoaringBitmap,
}


impl DocIdSet {
    pub fn new() -> DocIdSet {
        DocIdSet {
            data: RoaringBitmap::new()
        }
    }

    pub fn from_bytes(data: Vec<u8>) -> DocIdSet {
        let mut roaring_data: RoaringBitmap = RoaringBitmap::new();
        let mut cursor = Cursor::new(data);

        loop {
            let mut buf = [0, 2];
            match cursor.read_exact(&mut buf) {
                Ok(()) => {
                    let doc_id = BigEndian::read_u16(&buf);
                    roaring_data.insert(doc_id as u32);
                }
                Err(_) => break,
            }
        }

        DocIdSet {
            data: roaring_data
        }
    }

    pub fn insert(&mut self, doc_id: u16) {
        self.data.insert(doc_id as u32);
    }

    pub fn iter<'a>(&'a self) -> DocIdSetIterator<'a> {
        DocIdSetIterator {
            inner: self.data.iter(),
        }
    }

    pub fn contains_doc(&self, doc_id: u16) -> bool {
        self.data.contains(doc_id as u32)
    }

    pub fn union(&self, other: &DocIdSet) -> DocIdSet {
        let mut data: RoaringBitmap = self.data.clone();
        data.union_with(&other.data);

        DocIdSet {
            data: data
        }
    }

    pub fn intersection(&self, other: &DocIdSet) -> DocIdSet {
        let mut data: RoaringBitmap = self.data.clone();
        data.intersect_with(&other.data);

        DocIdSet {
            data: data
        }
    }

    pub fn exclusion(&self, other: &DocIdSet) -> DocIdSet {
        let mut data: RoaringBitmap = self.data.clone();
        data.difference_with(&other.data);

        DocIdSet {
            data: data
        }
    }
}


impl fmt::Debug for DocIdSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut iterator = self.iter();

        try!(write!(f, "["));

        let first_item = iterator.next();
        if let Some(first_item) = first_item {
            try!(write!(f, "{:?}", first_item));
        }

        for item in iterator {
            try!(write!(f, ", {:?}", item));
        }

        write!(f, "]")
    }
}


pub struct DocIdSetIterator<'a> {
    inner: RoaringBitmapIter<'a>,
}


impl<'a> Iterator for DocIdSetIterator<'a> {
    type Item = u16;

    fn next(&mut self) -> Option<u16> {
        self.inner.next().map(|v| v as u16)
    }
}
