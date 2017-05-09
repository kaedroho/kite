use term::Term;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Token {
    pub term: Term,
    pub position: u32,
}
