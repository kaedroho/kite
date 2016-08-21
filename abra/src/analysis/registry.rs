use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use analysis::AnalyzerSpec;
use analysis::tokenizers::TokenizerSpec;
use analysis::filters::FilterSpec;


#[derive(Debug)]
pub struct AnalyzerRegistry {
    analyzers: HashMap<String, AnalyzerSpec>,
    tokenizers: HashMap<String, TokenizerSpec>,
    filters: HashMap<String, FilterSpec>,
}


impl AnalyzerRegistry {
    pub fn new() -> AnalyzerRegistry {
        AnalyzerRegistry {
            analyzers: HashMap::new(),
            tokenizers: HashMap::new(),
            filters: HashMap::new(),
        }
    }
}


impl Deref for AnalyzerRegistry {
    type Target = HashMap<String, AnalyzerSpec>;

    fn deref(&self) -> &HashMap<String, AnalyzerSpec> {
        &self.analyzers
    }
}


impl DerefMut for AnalyzerRegistry {
    fn deref_mut(&mut self) -> &mut HashMap<String, AnalyzerSpec> {
        &mut self.analyzers
    }
}
