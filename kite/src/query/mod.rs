pub mod multi_term_selector;
pub mod term_scorer;

use term::Term;
use schema::FieldRef;
use query::multi_term_selector::MultiTermSelector;
use query::term_scorer::TermScorer;


#[derive(Debug, PartialEq)]
pub enum Query {
    All {
        score: f64,
    },
    None,
    Term {
        field: FieldRef,
        term: Term,
        scorer: TermScorer,
    },
    MultiTerm {
        field: FieldRef,
        term_selector: MultiTermSelector,
        scorer: TermScorer,
    },
    Conjunction {
        queries: Vec<Query>,
    },
    Disjunction {
        queries: Vec<Query>,
    },
    DisjunctionMax {
        queries: Vec<Query>,
    },
    Filter {
        query: Box<Query>,
        filter: Box<Query>
    },
    Exclude {
        query: Box<Query>,
        exclude: Box<Query>
    },
}


impl Query {
    pub fn new() -> Query {
        Query::All {
            score: 1.0f64,
        }
    }

    pub fn boost(&mut self, add_boost: f64) {
        if add_boost == 1.0f64 {
            // This boost query won't have any effect
            return;
        }

        match *self {
            Query::All{ref mut score} => {
                *score *= add_boost;
            },
            Query::None => (),
            Query::Term{ref mut scorer, ..} => {
                scorer.boost *= add_boost;
            }
            Query::MultiTerm{ref mut scorer, ..} => {
                scorer.boost *= add_boost;
            }
            Query::Conjunction{ref mut queries} => {
                for query in queries {
                    query.boost(add_boost);
                }
            }
            Query::Disjunction{ref mut queries} => {
                for query in queries {
                    query.boost(add_boost);
                }
            }
            Query::DisjunctionMax{ref mut queries} => {
                for query in queries {
                    query.boost(add_boost);
                }
            }
            Query::Filter{ref mut query, ..} => {
                query.boost(add_boost);
            }
            Query::Exclude{ref mut query, ..} => {
                query.boost(add_boost);
            }
        }
    }
}
