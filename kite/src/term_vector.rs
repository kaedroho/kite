use std::ops::{Deref, DerefMut};
use std::collections::HashMap;

use term::Term;
use token::Token;


#[derive(Debug, Clone, PartialEq)]
pub struct TermVector(HashMap<Term, Vec<u32>>);


impl TermVector {
    pub fn new() -> TermVector {
        TermVector(HashMap::new())
    }
}


impl Deref for TermVector {
    type Target = HashMap<Term, Vec<u32>>;

    fn deref(&self) -> &HashMap<Term, Vec<u32>> {
        &self.0
    }
}


impl DerefMut for TermVector {
    fn deref_mut(&mut self) -> &mut HashMap<Term, Vec<u32>> {
        &mut self.0
    }
}


impl Into<TermVector> for Vec<Token> {
    fn into(self) -> TermVector {
        let mut map = HashMap::new();

        for token in self {
            let mut positions = map.entry(token.term).or_insert_with(Vec::new);
            positions.push(token.position);
        }

         TermVector(map)
    }
}
