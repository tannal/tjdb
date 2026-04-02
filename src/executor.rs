// src/executor.rs
use crate::parser::{Expression, SelectStatement, Statement};
use crate::storage::{Database, Table, Tuple};

pub struct Executor<'a> {
    db: &'a Database,
}

impl<'a> Executor<'a> {
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    pub fn execute(&self, stmt: Statement) -> Result<Vec<Tuple>, String> {
        match stmt {
            Statement::Select(s) => self.execute_select(s),
        }
    }

    fn evaluate(&self, expr: &Expression, tuple: &Tuple, table: &Table) -> Result<bool, String> {
        match expr {
            Expression::BinaryOp { left, op, right } => {
                // 找到左侧列在原表中的索引
                let idx = table.columns.iter().position(|c| c == left)
                    .ok_or(format!("Column {} not found in WHERE", left))?;
                
                let val_in_tuple = &tuple.0[idx];
                
                if op == "=" {
                    Ok(val_in_tuple == right)
                } else {
                    Err(format!("Unsupported operator: {}", op))
                }
            }
        }
    }

    fn execute_select(&self, stmt: SelectStatement) -> Result<Vec<Tuple>, String> {
        // 1. 找到表
        let table = self
            .db
            .tables
            .get(&stmt.table)
            .ok_or(format!("Table {} not found", stmt.table))?;

        // 2. 找到列的索引映射
        let col_indices: Vec<usize> = stmt
            .columns
            .iter()
            .map(|col_name| {
                table
                    .columns
                    .iter()
                    .position(|c| c == col_name)
                    .ok_or(format!("Column {} not found", col_name))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // 3. 扫描数据并投影 (Projection)
        let mut result = Vec::new();
        for tuple in &table.data {
            // --- WHERE 过滤逻辑 ---
            if let Some(expr) = &stmt.where_clause {
                if !self.evaluate(expr, tuple, table)? {
                    continue;
                }
            }
            let mut projected_data = Vec::new();
            for &idx in &col_indices {
                projected_data.push(tuple.0[idx].clone());
            }
            result.push(Tuple(projected_data));
        }

        Ok(result)
    }
}
