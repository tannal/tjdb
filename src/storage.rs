// src/storage.rs
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Tuple(pub Vec<String>); // 简单起见，所有数据先存为 String

pub struct Table {
    pub name: String,
    pub columns: Vec<String>,
    pub data: Vec<Tuple>,
}

pub struct Database {
    pub tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Self { tables: HashMap::new() }
    }
}