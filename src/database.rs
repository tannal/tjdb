use std::collections::HashMap;

use crate::{parser::InsertStatement, storage::{Table, Tuple}};

pub struct Database {
    pub tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Self { tables: HashMap::new() }
    }

    /// 模拟数据库停机，保存所有数据
    pub fn shutdown(&self) {
        for table in self.tables.values() {
            if let Err(e) = table.save_to_csv() {
                eprintln!("Failed to save table {}: {}", table.name, e);
            }
        }
        println!("Database saved successfully.");
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