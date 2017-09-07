use roaring::RoaringBitmap;

use schema::FieldId;
use term::TermId;
use document::DocId;

pub trait Segment {
    fn load_statistic(&self, stat_name: &[u8]) -> Result<Option<i64>, String>;
    fn load_stored_field_value_raw(&self, doc_ord: u16, field_id: FieldId, value_type: &[u8]) -> Result<Option<Vec<u8>>, String>;
    fn load_term_directory(&self, field_id: FieldId, term_id: TermId) -> Result<Option<RoaringBitmap>, String>;
    fn load_deletion_list(&self) -> Result<Option<RoaringBitmap>, String>;
    fn id(&self) -> u32;

    fn doc_id(&self, ord: u16) -> DocId {
        DocId::from_segment_ord(self.id(), ord)
    }
}
