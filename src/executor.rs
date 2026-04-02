use crate::operator::filter::{FilterOperator, PhysicalExpression};
use crate::operator::project::ProjectOperator;
use crate::operator::scan::ScanOperator;
use crate::operator::Operator;
use crate::parser::{Expression, SelectStatement, UpdateStatement};
use crate::storage::{DataType, Database, Table, Value};

pub struct Executor;

impl Executor {
    pub fn new() -> Self {
        Self
    }

    /// 执行 UPDATE 操作：直接修改 Database 中的数据
    pub fn execute_update(
        &self,
        stmt: UpdateStatement,
        db: &mut Database,
    ) -> Result<usize, String> {
        // 1. 获取表的可变引用
        let table = db
            .tables
            .get_mut(&stmt.table_name)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table_name))?;

        // 2. 校验目标列是否存在
        let col_idx = table
            .columns
            .iter()
            .position(|c| c.name == stmt.column_name)
            .ok_or_else(|| format!("Column '{}' not found", stmt.column_name))?;
        
        let target_type = &table.columns[col_idx].data_type;

        // 3. 语义校验：新值的类型是否匹配目标列类型
        let val_type = self.get_expression_type(&stmt.new_value, table)?;
        if &val_type != target_type {
            return Err(format!(
                "Type mismatch: cannot assign {:?} to {:?}",
                val_type, target_type
            ));
        }

        // 4. 绑定物理表达式
        let phys_val_expr = Self::bind_expression(&stmt.new_value, table)?;
        let phys_where_expr = if let Some(w) = stmt.where_clause {
            let where_type = self.get_expression_type(&w, table)?;
            if where_type != DataType::Bool {
                return Err("WHERE clause must evaluate to Bool".into());
            }
            Some(Self::bind_expression(&w, table)?)
        } else {
            None
        };

        // 5. 迭代更新
        let mut updated_count = 0;
        for tuple in &mut table.data {
            let should_update = match &phys_where_expr {
                Some(w) => match w.evaluate(tuple)? {
                    Value::Bool(b) => b,
                    _ => false,
                },
                None => true,
            };

            if should_update {
                let new_val = phys_val_expr.evaluate(tuple)?;
                tuple.0[col_idx] = new_val;
                updated_count += 1;
            }
        }

        Ok(updated_count)
    }

    /// 构建查询计划：返回一个物理算子树
    /// 注意：这里的 'a 生命周期绑定在输入的 db 上
    pub fn build_plan<'a>(
        &self,
        stmt: SelectStatement,
        db: &'a Database,
    ) -> Result<Box<dyn Operator + 'a>, String> {
        let table = db
            .tables
            .get(&stmt.table)
            .ok_or_else(|| format!("Table not found: {}", stmt.table))?;

        // 1. Scan
        let mut plan: Box<dyn Operator + 'a> = Box::new(ScanOperator::new(&table.data));

        // 2. Filter
        if let Some(cond) = stmt.where_clause {
            let return_type = self.get_expression_type(&cond, table)?;
            if return_type != DataType::Bool {
                return Err(format!(
                    "WHERE clause must evaluate to Bool, but found {:?}",
                    return_type
                ));
            }

            let physical_cond = Self::bind_expression(&cond, table)?;
            plan = Box::new(FilterOperator::new(plan, physical_cond, table));
        }

        // 3. Project
        let col_indices: Vec<usize> = if stmt.columns.is_empty() {
            (0..table.columns.len()).collect()
        } else {
            stmt.columns
                .iter()
                .map(|name| {
                    table
                        .columns
                        .iter()
                        .position(|c| &c.name == name)
                        .ok_or_else(|| format!("Column '{}' not found", name))
                })
                .collect::<Result<Vec<_>, String>>()?
        };

        Ok(Box::new(ProjectOperator::new(plan, col_indices)))
    }

    /// 将逻辑表达式转换为基于索引的物理表达式
    fn bind_expression(expr: &Expression, table: &Table) -> Result<PhysicalExpression, String> {
        match expr {
            Expression::Literal(v) => Ok(PhysicalExpression::Literal(v.clone())),
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

    /// 递归检查表达式的类型安全性
    fn get_expression_type(&self, expr: &Expression, table: &Table) -> Result<DataType, String> {
        match expr {
            Expression::Literal(v) => match v {
                Value::Int(_) => Ok(DataType::Int),
                Value::Text(_) => Ok(DataType::Text),
                Value::Bool(_) => Ok(DataType::Bool),
                Value::Null => Err("Type inference for Null is not supported".into()),
            },
            Expression::Column(name) => table
                .columns
                .iter()
                .find(|c| &c.name == name)
                .map(|c| c.data_type.clone())
                .ok_or_else(|| format!("Column '{}' not found", name)),
            Expression::BinaryOp { left, op, right } => {
                let lt = self.get_expression_type(left, table)?;
                let rt = self.get_expression_type(right, table)?;

                match op.as_str() {
                    "+" | "-" | "*" | "/" => {
                        if lt != DataType::Int || rt != DataType::Int {
                            return Err(format!(
                                "Operator '{}' expects Int, found {:?} and {:?}",
                                op, lt, rt
                            ));
                        }
                        Ok(DataType::Int)
                    }
                    "=" | "!=" | ">" | "<" | ">=" | "<=" => {
                        if lt != rt {
                            return Err(format!(
                                "Type mismatch: cannot compare {:?} with {:?}",
                                lt, rt
                            ));
                        }
                        Ok(DataType::Bool)
                    }
                    _ => Err(format!("Unknown operator: {}", op)),
                }
            }
        }
    }
}