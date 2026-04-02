use crate::operator::Operator;
use crate::operator::filter::FilterOperator;
use crate::operator::project::ProjectOperator;
use crate::operator::scan::ScanOperator;
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

    // 返回值使用 Box<dyn Operator + 'a> 确保迭代器在引用数据期间有效
    pub fn build_plan(stmt: SelectStatement, db: &'a Database) -> Box<dyn Operator + 'a> {
        let table = db.tables.get(&stmt.table).expect("Table not found");

        // 1. 创建 Scan (最底层)
        let mut plan: Box<dyn Operator + 'a> = Box::new(ScanOperator::new(&table.data));

        // 2. 包装 Filter (中间层)
        if let Some(cond) = stmt.where_clause {
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

        Box::new(ProjectOperator::new(plan, col_indices))
    }

}
