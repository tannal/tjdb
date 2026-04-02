use crate::database::Database;
use crate::operator::aggregate::AggregateOperator;
use crate::operator::filter::{FilterOperator, PhysicalExpression};
use crate::operator::project::ProjectOperator;
use crate::operator::scan::ScanOperator;
use crate::operator::Operator;
use crate::parser::{DeleteStatement, Expression, SelectItem, SelectStatement, UpdateStatement};
use crate::storage::{DataType, Table, Tuple, Value};

pub struct Executor;

impl Executor {
    pub fn new() -> Self {
        Self
    }

    pub fn execute_delete(
        &self,
        stmt: DeleteStatement,
        db: &mut Database,
    ) -> Result<usize, String> {
        // 1. 获取表
        let table = db
            .tables
            .get_mut(&stmt.table_name)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table_name))?;

        // 2. 绑定 WHERE 表达式
        let phys_where_expr = if let Some(w) = &stmt.where_clause {
            let where_type = self.get_expression_type(w, table)?;
            if where_type != DataType::Bool {
                return Err("WHERE clause must evaluate to Bool".into());
            }
            Some(Self::bind_expression(w, table)?)
        } else {
            None
        };

        // 3. 执行物理删除
        let initial_count = table.data.len();
        
        table.data.retain(|tuple| {
            let should_delete = match &phys_where_expr {
                Some(w) => match w.evaluate(tuple) {
                    Ok(Value::Bool(b)) => b,
                    _ => false, 
                },
                None => true, 
            };
            
            !should_delete 
        });

        let deleted_count = initial_count - table.data.len();
        Ok(deleted_count)
    }

    pub fn execute_update(
        &self,
        stmt: UpdateStatement,
        db: &mut Database,
    ) -> Result<usize, String> {
        let table = db
            .tables
            .get_mut(&stmt.table_name)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table_name))?;
    
        struct AssignmentPlan {
            col_idx: usize,
            phys_expr: PhysicalExpression,
        }
    
        let mut assignment_plans = Vec::new();
    
        for (col_name, expr) in &stmt.assignments {
            let col_idx = table
                .columns
                .iter()
                .position(|c| &c.name == col_name)
                .ok_or_else(|| format!("Column '{}' not found", col_name))?;
    
            let target_type = &table.columns[col_idx].data_type;
            let val_type = self.get_expression_type(expr, table)?;
            if &val_type != target_type {
                return Err(format!(
                    "Type mismatch for column '{}': cannot assign {:?} to {:?}",
                    col_name, val_type, target_type
                ));
            }
    
            let phys_expr = Self::bind_expression(expr, table)?;
            assignment_plans.push(AssignmentPlan {
                col_idx,
                phys_expr,
            });
        }
    
        let phys_where_expr = if let Some(w) = &stmt.where_clause {
            let where_type = self.get_expression_type(w, table)?;
            if where_type != DataType::Bool {
                return Err("WHERE clause must evaluate to Bool".into());
            }
            Some(Self::bind_expression(w, table)?)
        } else {
            None
        };
    
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
                let mut pending_updates = Vec::with_capacity(assignment_plans.len());
                for plan in &assignment_plans {
                    let new_val = plan.phys_expr.evaluate(tuple)?;
                    pending_updates.push((plan.col_idx, new_val));
                }
    
                for (idx, val) in pending_updates {
                    tuple.0[idx] = val;
                }
                updated_count += 1;
            }
        }
    
        Ok(updated_count)
    }

    pub fn build_plan<'a>(
        &self,
        stmt: SelectStatement,
        db: &'a Database,
    ) -> Result<Box<dyn Operator + 'a>, String> {
        let table = db
            .tables
            .get(&stmt.table_name)
            .ok_or_else(|| format!("Table not found: {}", stmt.table_name))?;
    
        // 1. 基础算子：Scan
        let mut plan: Box<dyn Operator + 'a> = Box::new(ScanOperator::new(&table.data));
    
        // 2. 过滤算子：Filter
        if let Some(cond) = stmt.where_clause {
            let return_type = self.get_expression_type(&cond, table)?;
            if return_type != DataType::Bool {
                return Err(format!("WHERE clause must evaluate to Bool, but found {:?}", return_type));
            }
    
            let physical_cond = Self::bind_expression(&cond, table)?;
            plan = Box::new(FilterOperator::new(plan, physical_cond, table));
        }
    
        // 3. 判断聚合
        let has_aggregate = stmt.select_items.iter().any(|item| matches!(item, SelectItem::Aggregate(_)));
    
        if has_aggregate {
            for item in &stmt.select_items {
                if let SelectItem::Column(_) | SelectItem::Wildcard = item {
                    return Err("Mixing aggregate and non-aggregate columns is not supported without GROUP BY".into());
                }
            }
            Ok(Box::new(AggregateOperator::new(plan, stmt.select_items, table)?))
        } else {
            // --- 修改后的投影逻辑：处理 Wildcard ---
            let mut col_indices = Vec::new();
            
            // 如果 select_items 为空（SELECT * 的另一种表现形式）或包含 Wildcard
            for item in &stmt.select_items {
                match item {
                    SelectItem::Column(name) => {
                        let idx = table.columns.iter().position(|c| &c.name == name)
                            .ok_or_else(|| format!("Column '{}' not found", name))?;
                        col_indices.push(idx);
                    }
                    SelectItem::Wildcard => {
                        // 展开所有列索引
                        for i in 0..table.columns.len() {
                            col_indices.push(i);
                        }
                    }
                    SelectItem::Aggregate(_) => unreachable!(),
                }
            }

            // 如果 SQL 语句完全没有投影项（例如解析器层面的特殊情况），默认选全部
            if col_indices.is_empty() && stmt.select_items.is_empty() {
                col_indices = (0..table.columns.len()).collect();
            }
    
            Ok(Box::new(ProjectOperator::new(plan, col_indices)))
        }
    }

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
                            return Err(format!("Operator '{}' expects Int, found {:?} and {:?}", op, lt, rt));
                        }
                        Ok(DataType::Int)
                    }
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