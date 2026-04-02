// src/storage.rs
use std::{collections::HashMap, fs::File, io::{BufRead, BufReader}, path::Path};

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
use std::io::Write;

impl Value {

    // 转换为存入 CSV 的字符串
    pub fn to_csv_string(&self) -> String {
        match self {
            Value::Int(n) => n.to_string(),
            Value::Text(s) => s.clone(), // 简单实现，暂不处理逗号转义
            Value::Bool(b) => b.to_string(),
            Value::Null => "NULL".to_string(),
        }
    }

    // 从 CSV 字符串解析回 Value (需要知道目标类型)
    pub fn from_csv_string(s: &str, data_type: &DataType) -> Result<Self, String> {
        match data_type {
            DataType::Int => s.parse::<i32>()
                .map(Value::Int)
                .map_err(|_| format!("Invalid integer: {}", s)),
            DataType::Text => Ok(Value::Text(s.to_string())),
            DataType::Bool => s.parse::<bool>()
                .map(Value::Bool)
                .map_err(|_| format!("Invalid boolean: {}", s)),
        }
    }

    pub fn apply_binary_op(&self, other: &Value, op: &str) -> Result<Value, String> {
        match (self, other, op) {
            (Value::Int(a), Value::Int(b), "+") => Ok(Value::Int(a + b)),
            (Value::Int(a), Value::Int(b), "*") => Ok(Value::Int(a * b)),
            (Value::Text(a), Value::Text(b), "+") => Ok(Value::Text(format!("{}{}", a, b))), // 字符串拼接
            // 可以在这里扩展更多的后端计算逻辑
            _ => Err(format!("Invalid operation: {:?} {} {:?}", self, op, other)),
        }
    }

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
    pub data_type: DataType,
    pub is_nullable: bool,
    // 未来可以扩展：pub is_primary_key: bool
}

pub struct Table {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    pub data: Vec<Tuple>, // Tuple 本质是 Vec<Value>
}

impl Table {
    /// 将整个表保存到文件 (table_name.csv)
    pub fn save_to_csv(&self) -> std::io::Result<()> {
        let filename = format!("{}.csv", self.name);
        let mut file = File::create(filename)?;

        // 1. 写入表头 (列名)
        let header: Vec<String> = self.columns.iter().map(|c| c.name.clone()).collect();
        writeln!(file, "{}", header.join(","))?;

        // 2. 写入数据行
        for tuple in &self.data {
            let row: Vec<String> = tuple.0.iter().map(|v| v.to_csv_string()).collect();
            writeln!(file, "{}", row.join(","))?;
        }

        Ok(())
    }

    /// 从文件加载数据
    pub fn load_from_csv(name: &str, columns: Vec<ColumnDefinition>) -> Result<Self, String> {
        let filename = format!("{}.csv", name);
        if !Path::new(&filename).exists() {
            return Ok(Table { name: name.to_string(), columns, data: Vec::new() });
        }

        let file = File::open(filename).map_err(|e| e.to_string())?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        // 跳过表头
        lines.next();

        let mut data = Vec::new();
        for line in lines {
            let line = line.map_err(|e| e.to_string())?;
            let values: Vec<&str> = line.split(',').collect();
            
            if values.len() != columns.len() {
                continue; // 简单的错误处理
            }

            let mut row = Vec::new();
            for (idx, val_str) in values.iter().enumerate() {
                row.push(Value::from_csv_string(val_str, &columns[idx].data_type)?);
            }
            data.push(Tuple(row));
        }

        Ok(Table { name: name.to_string(), columns, data })
    }
}


