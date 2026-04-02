use crate::operator::filter::{FilterOperator, PhysicalExpression};
use crate::operator::project::ProjectOperator;
use crate::operator::scan::ScanOperator;
use crate::operator::{self, Operator};
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

    fn bind_expression(expr: &Expression, table: &Table) -> Result<PhysicalExpression, String> {
        match expr {
            // 字面量直接转换
            Expression::Literal(v) => Ok(PhysicalExpression::Literal(v.clone())),

            // 列名映射为索引
            Expression::Column(name) => {
                let idx = table
                    .columns
                    .iter()
                    .position(|c| &c.name == name)
                    .ok_or_else(|| {
                        format!("Column '{}' not found in table '{}'", name, table.name)
                    })?;

                Ok(PhysicalExpression::BoundColumn(idx))
            }

            // 递归处理二元运算
            Expression::BinaryOp { left, op, right } => {
                let l = Self::bind_expression(left, table)?;
                let r = Self::bind_expression(right, table)?;
                Ok(PhysicalExpression::BinaryOp {
                    left: Box::new(l),
                    op: op.clone(),
                    right: Box::new(r),
                })
            }
        }
    }

    // 返回值使用 Box<dyn Operator + 'a> 确保迭代器在引用数据期间有效
    pub fn build_plan(
        &self,
        stmt: SelectStatement,
        db: &'a Database,
    ) -> Result<Box<dyn Operator + 'a>, String> {
        // 0. 查找表定义
        let table = db
            .tables
            .get(&stmt.table)
            .ok_or_else(|| format!("Table not found: {}", stmt.table))?;

        // 1. 创建 Scan (最底层：数据源)
        let mut plan: Box<dyn Operator + 'a> = Box::new(ScanOperator::new(&table.data));

        // 2. 包装 Filter (中间层：条件过滤)
        if let Some(cond) = stmt.where_clause {
            // --- 核心修复点 A：深层递归类型检查 ---
            // 这一步会递归检查整个表达式树（例如：name + 1 这一层就会直接报错）
            let return_type = self.get_expression_type(&cond, table)?;
            if return_type != DataType::Bool {
                return Err(format!(
                    "WHERE clause must evaluate to Bool, but found {:?}",
                    return_type
                ));
            }

            // --- 核心修复点 B：物理绑定 (逻辑列名 -> 物理索引) ---
            let physical_cond = Self::bind_expression(&cond, table)?;

            plan = Box::new(FilterOperator::new(plan, physical_cond, table));
        }

        // 3. 包装 Project (最顶层：列筛选)
        let col_indices: Vec<usize> = if stmt.columns.is_empty() {
            // 支持 SELECT *
            (0..table.columns.len()).collect()
        } else {
            stmt.columns
                .iter()
                .map(|name| {
                    table.columns.iter()
                        .position(|c| &c.name == name)
                        .ok_or_else(|| format!("Column '{}' not found", name))
                })
                .collect::<Result<Vec<_>, String>>()?
        };

        Ok(Box::new(ProjectOperator::new(plan, col_indices)))
    }

    /// 递归推导表达式类型并进行静态语义校验
    fn get_expression_type(&self, expr: &Expression, table: &Table) -> Result<DataType, String> {
        match expr {
            Expression::Literal(v) => match v {
                Value::Int(_) => Ok(DataType::Int),
                Value::Text(_) => Ok(DataType::Text),
                Value::Bool(_) => Ok(DataType::Bool),
                Value::Null => Err("Literal Null type inference not implemented".into()),
            },
            Expression::Column(name) => {
                table.columns.iter()
                    .find(|c| &c.name == name)
                    .map(|c| c.data_type.clone())
                    .ok_or_else(|| format!("Column '{}' not found", name))
            }
            Expression::BinaryOp { left, op, right } => {
                let lt = self.get_expression_type(left, table)?;
                let rt = self.get_expression_type(right, table)?;

                match op.as_str() {
                    // 算术运算：必须是 Int 和 Int
                    "+" | "-" | "*" | "/" => {
                        if lt != DataType::Int || rt != DataType::Int {
                            return Err(format!("Operator '{}' only supports Integers, but found {:?} and {:?}", op, lt, rt));
                        }
                        Ok(DataType::Int)
                    }
                    // 比较运算：左右类型必须一致
                    "=" | "!=" | ">" | "<" | ">=" | "<=" => {
                        if lt != rt {
                            return Err(format!("Type mismatch: cannot compare {:?} with {:?}", lt, rt));
                        }
                        Ok(DataType::Bool)
                    }
                    _ => Err(format!("Unknown operator: {}", op)),
                }
            }
        }
    }
}
