use crate::{
    operator::{ExecuteResult, Operator},
    parser::Expression,
    storage::{Table, Tuple, Value},
};

pub struct FilterOperator<'a> {
    source: Box<dyn Operator + 'a>,
    condition: PhysicalExpression,
    table_schema: &'a Table, // 用于 evaluate 时查找列索引
}

// 这是一个“物理”表达式，所有的列名都已经变成了索引
#[derive(Debug, Clone)]
pub enum PhysicalExpression {
    Literal(Value),
    BoundColumn(usize), // 预先找好的索引
    BinaryOp {
        left: Box<PhysicalExpression>,
        op: String,
        right: Box<PhysicalExpression>,
    },
}
impl<'a> FilterOperator<'a> {
    pub fn new(
        source: Box<dyn Operator + 'a>,
        condition: Expression,
        table_schema: &'a Table,
    ) -> Self {
        // 在这里进行递归绑定
        let physical_cond = Self::bind_expression(&condition, table_schema);

        Self {
            source,
            condition: physical_cond,
            table_schema,
        }
    }

    // 递归绑定函数：将“列名”映射为“索引”
    fn bind_expression(expr: &Expression, table: &Table) -> PhysicalExpression {
        match expr {
            Expression::Literal(v) => PhysicalExpression::Literal(v.clone()),
            
            Expression::Column(name) => {
                let idx = table.columns.iter()
                    .position(|c| c == name)
                    .expect(&format!("Column {} not found", name)); // 这里抛出错误
                PhysicalExpression::BoundColumn(idx)
            }

            Expression::BinaryOp { left, op, right } => {
                PhysicalExpression::BinaryOp {
                    left: Box::new(Self::bind_expression(left, table)),
                    op: op.clone(),
                    right: Box::new(Self::bind_expression(right, table)),
                }
            }
        }
    }

    pub fn evaluate(
        &self,
        expr: &PhysicalExpression,
        tuple: &Tuple,
    ) -> Result<Value, String> {
        match expr {
            // 1. 叶子节点：常量值
            PhysicalExpression::Literal(v) => Ok(v.clone()),

            // 2. 叶子节点：已绑定的列索引 (之前在 new 中找好的)
            PhysicalExpression::BoundColumn(idx) => {
                // 直接通过索引从 Tuple 中取值，避免了字符串查找
                Ok(tuple.0[*idx].clone())
            }

            // 3. 递归节点：二元运算
            PhysicalExpression::BinaryOp { left, op, right } => {
                // 递归计算左右子树的值
                let left_val = self.evaluate(left, tuple)?;
                let right_val = self.evaluate(right, tuple)?;

                // 执行具体的比较逻辑
                match op.as_str() {
                    "="  => Ok(Value::Bool(left_val == right_val)),
                    ">"  => Ok(Value::Bool(left_val > right_val)),
                    "<"  => Ok(Value::Bool(left_val < right_val)),
                    ">=" => Ok(Value::Bool(left_val >= right_val)),
                    "<=" => Ok(Value::Bool(left_val <= right_val)),
                    "!=" => Ok(Value::Bool(left_val != right_val)),
                    
                    // 预留算术运算（如果你以后解析 1 + 1）
                    "+" => {
                        if let (Value::Int(a), Value::Int(b)) = (left_val, right_val) {
                            Ok(Value::Int(a + b))
                        } else {
                            Err("Addition only supported for Integers".into())
                        }
                    }
                    _ => Err(format!("Unknown operator in execution: {}", op)),
                }
            }
        }
    }
}

impl<'a> Iterator for FilterOperator<'a> {
    type Item = ExecuteResult;

    fn next(&mut self) -> Option<Self::Item> {
        // 持续从下层算子（Source）拉取数据
        while let Some(item) = self.source.next() {
            match item {
                Ok(tuple) => {
                    // 调用递归的 evaluate 函数
                    // 注意：现在我们传入的是 PhysicalExpression
                    match self.evaluate(&self.condition, &tuple) {
                        Ok(Value::Bool(true)) => {
                            // 只有明确为 true 时才返回该行
                            return Some(Ok(tuple));
                        }
                        Ok(Value::Bool(false)) => {
                            // 不匹配，继续 while 循环拉取下一行
                            continue;
                        }
                        Ok(other_val) => {
                            // 语义错误：例如 WHERE age (结果是 Int 而不是 Bool)
                            return Some(Err(format!(
                                "Filter condition must evaluate to a boolean, found {:?}", 
                                other_val
                            )));
                        }
                        Err(e) => {
                            // 计算过程报错（如溢出、类型不匹配等）
                            return Some(Err(e));
                        }
                    }
                }
                // 下层算子（如 Scan）报错，直接向上透传
                Err(e) => return Some(Err(e)),
            }
        }
        // Source 耗尽，迭代结束
        None
    }
}
impl<'a> Operator for FilterOperator<'a> {}
