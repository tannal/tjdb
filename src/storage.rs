// src/storage.rs
use std::collections::HashMap;

use crate::parser::InsertStatement;

#[derive(Debug, Clone, PartialEq, PartialOrd)] // PartialOrd 自动支持 <, >, <=, >=
pub enum Value {
    Int(i32),
    Text(String),
    Bool(bool),
    Null,
}

#[derive(Debug, PartialEq, Clone)]
pub enum DataType {
    Int,
    Text,
    Bool,
}

impl Value {
    pub fn matches_type(&self, dtype: &DataType) -> bool {
        match (self, dtype) {
            (Value::Int(_), DataType::Int) => true,
            (Value::Text(_), DataType::Text) => true,
            (Value::Bool(_), DataType::Bool) => true,
            (Value::Null, _) => true, // 允许 Null 写入任何列
            _ => false,
        }
    }
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

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType, // 使用你已经定义的 DataType 枚举
}

pub struct Table {
    pub name: String,
    pub columns: Vec<ColumnDefinition>, // 这里的改变是核心：不再只是 String
    pub data: Vec<Tuple>,
}



pub struct Database {
    pub tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Self { tables: HashMap::new() }
    }

    pub fn apply_insert(&mut self, stmt: InsertStatement) -> Result<usize, String> {
        let table = self.tables.get_mut(&stmt.table_name)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table_name))?;

        // --- 核心修复：数量检查 ---
        if stmt.values.len() != table.columns.len() {
            return Err(format!(
                "Insert Error: Column count mismatch. Expected {}, got {}",
                table.columns.len(),
                stmt.values.len()
            ));
        }

        // TODO: 这里以后还可以加入类型检查 (DataType vs Value)
        for (i, val) in stmt.values.iter().enumerate() {
            if !val.matches_type(&table.columns[i].data_type) {
                return Err(format!("Type mismatch for column '{}'", table.columns[i].name));
            }
        }
        
        table.data.push(Tuple(stmt.values));
        Ok(1)
    }
}