use kite::segment::Segment;

use RocksDBStore;

#[derive(Debug)]
pub struct SegmentStatistics {
    total_docs: i64,
    deleted_docs: i64,
}

impl SegmentStatistics {
    fn read<S: Segment>(segment: &S) -> Result<SegmentStatistics, String> {
        let total_docs = try!(segment.load_statistic(b"total_docs")).unwrap_or(0);
        let deleted_docs = try!(segment.load_statistic(b"deleted_docs")).unwrap_or(0);

        Ok(SegmentStatistics {
            total_docs: total_docs,
            deleted_docs: deleted_docs,
        })
    }

    #[inline]
    pub fn total_docs(&self) -> i64 {
        self.total_docs
    }

    #[inline]
    pub fn deleted_docs(&self) -> i64 {
        self.deleted_docs
    }
}

impl RocksDBStore {
    pub fn get_segment_statistics(&self) -> Result<Vec<(u32, SegmentStatistics)>, String> {
        let mut segment_stats = Vec::new();
        let reader = self.reader();

        for segment in self.segments.iter_active(&reader) {
            let stats = try!(SegmentStatistics::read(&segment));
            segment_stats.push((segment.id().0, stats));
        }

        Ok(segment_stats)
    }
}
