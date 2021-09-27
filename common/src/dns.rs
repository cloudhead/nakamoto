//! Seed resolution.
use std::{iter};


/// DNS feature
#[derive(Debug, Copy, Clone)]
pub enum SeedFeature {
    /// x1, Full Node
    NODE_NETWORK,
    /// x5, all include in x1 plus Node network
    NODE_BLOOM,
    /// x9, all included in x1 plus Node Witness
    NODE_WITNESS,
    /// xd (aka x13), all included in x9 and x5
    FULL_NODE,
}

impl Default for SeedFeature {
    fn default() -> Self {
        Self::NODE_NETWORK
    }
}

impl SeedFeature {

    pub fn as_code(&self) -> &'static str{
        match self {
            Self::NODE_NETWORK => &"x1",
            Self::NODE_BLOOM => &"x5",
            Self::NODE_WITNESS => &"x9",
            Self::FULL_NODE => &"xd",
        }
    }
}

/// Seeds implementation
pub struct Seeds<'a> {
    pub seeds: Vec<&'a str>,
}

impl<'a> Seeds<'a> {
    /// Create a new seed with the requirements
    pub fn new(features: &[SeedFeature]) -> Self {
        Seeds {
            seeds: vec![]
        }
    }

    // FIXME(vincenzopalazzo): Return the iterator of the vetor
    // to make easy the change in the interface.
    pub fn iter(&self) -> &std::slice::Iter<'a, str> {
        self.seeds.iter();
    }
}

