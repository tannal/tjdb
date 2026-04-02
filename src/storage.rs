// src/storage.rs
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, PartialOrd)] // PartialOrd 自动支持 <, >, <=, >=
pub enum Value {
    Int(i32),
    Text(String),
    Bool(bool),
    Null,
}

impl Value {
    pub fn from_str_typed(lit: &str, target: &Value) -> Self {
        match target {
            Value::Int(_) => Value::Int(lit.parse().unwrap_or(0)),
            Value::Text(_) => Value::Text(lit.to_string()),
            Value::Bool(_) => Value::Bool(lit.to_lowercase() == "true"),
            _ => Value::Null,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Tuple(pub Vec<Value>);

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