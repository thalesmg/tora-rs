#[macro_use]
extern crate serde_derive;

pub mod entries;
pub mod search;

#[derive(Debug)]
pub struct ToraError;
