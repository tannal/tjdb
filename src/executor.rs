use crate::operator::Operator;
use crate::operator::filter::FilterOperator;
use crate::operator::project::ProjectOperator;
use crate::operator::scan::ScanOperator;
// src/executor.rs
use crate::parser::{Expression, InsertStatement, SelectStatement, Statement};
use crate::storage::{DataType, Database, Table, Tuple, Value};
pub struct Executor<'a> {
    db: &'a Database,
}

impl<'a> Executor<'a> {
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    fn get_expression_type(&self, expr: &Expression, schema: &Table) -> Result<DataType, String> {
        match expr {
            Expression::Literal(v) => match v {
                Value::Int(_) => Ok(DataType::Int),
                Value::Text(_) => Ok(DataType::Text),
                Value::Bool(_) => Ok(DataType::Bool),
                Value::Null => todo!(),
            },
            Expression::Column(name) => {
                // 查找列定义，比如 age 是 Int
                Ok(DataType::Int)
            }
            Expression::BinaryOp { left, op, right } => {
                let lt = self.get_expression_type(left, schema)?;
                let rt = self.get_expression_type(right, schema)?;

                match op.as_str() {
                    "+" | "-" | "*" | "/" => Ok(DataType::Int),
                    "=" | ">" | "<" | "<=" | ">=" => Ok(DataType::Bool),
                    _ => Err(format!("Unsupported op: {}", op)),
                }
            }
        }
    }

    // 返回值使用 Box<dyn Operator + 'a> 确保迭代器在引用数据期间有效
    pub fn build_plan(
        &self,
        stmt: SelectStatement,
        db: &'a Database,
    ) -> Result<Box<dyn Operator + 'a>, String> {
        let table = db
            .tables
            .get(&stmt.table)
            .ok_or_else(|| format!("Table not found: {}", stmt.table))?;

        // 1. 创建 Scan (最底层)
        let mut plan: Box<dyn Operator + 'a> = Box::new(ScanOperator::new(&table.data));

        // 2. 包装 Filter (中间层)
        if let Some(cond) = stmt.where_clause {
            // --- 新增：类型检查逻辑 ---
            let return_type = self.get_expression_type(&cond, table)?;
            if return_type != DataType::Bool {
                return Err(format!(
                    "WHERE clause must evaluate to Bool, but found {:?}",
                    return_type
                ));
            }
            // -----------------------

            plan = Box::new(FilterOperator::new(plan, cond, table));
        }

        // 3. 包装 Project (最顶层)
        let col_indices = stmt
            .columns
            .iter()
            .map(|name| {
                table
                    .columns
                    .iter()
                    .position(|c| c == name)
                    .expect("Column not found")
            })
            .collect();

        Ok(Box::new(ProjectOperator::new(plan, col_indices)))
    }
}
