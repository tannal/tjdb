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
        condition: PhysicalExpression, // 👈 传入已经绑定好的物理表达式
        table_schema: &'a Table,
    ) -> Self {
        Self {
            source,
            condition,
            table_schema,
        }
    }

    /// 递归绑定函数：将带有“列名”的逻辑表达式 (Expression)
    /// 转换为带有“索引”的物理表达式 (PhysicalExpression)
    fn bind_expression(expr: &Expression, table: &Table) -> Result<PhysicalExpression, String> {
        match expr {
            // 1. 处理字面量：直接透传
            Expression::Literal(v) => Ok(PhysicalExpression::Literal(v.clone())),

            // 2. 处理列名：查找索引（Schema 绑定的核心）
            Expression::Column(name) => {
                let idx = table
                    .columns
                    .iter()
                    .position(|c| &c.name == name) // 比较 ColumnDefinition.name 和 String
                    .ok_or_else(|| {
                        format!("Column '{}' not found in table '{}'", name, table.name)
                    })?;

                Ok(PhysicalExpression::BoundColumn(idx))
            }

            // 3. 处理二元运算：递归绑定左右子树
            Expression::BinaryOp { left, op, right } => {
                let bound_left = Self::bind_expression(left, table)?;
                let bound_right = Self::bind_expression(right, table)?;

                Ok(PhysicalExpression::BinaryOp {
                    left: Box::new(bound_left),
                    op: op.clone(),
                    right: Box::new(bound_right),
                })
            }
        }
    }

    pub fn evaluate(&self, expr: &PhysicalExpression, tuple: &Tuple) -> Result<Value, String> {
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
                    "=" => Ok(Value::Bool(left_val == right_val)),
                    ">" => Ok(Value::Bool(left_val > right_val)),
                    "<" => Ok(Value::Bool(left_val < right_val)),
                    ">=" => Ok(Value::Bool(left_val >= right_val)),
                    "<=" => Ok(Value::Bool(left_val <= right_val)),
                    "!=" => Ok(Value::Bool(left_val != right_val)),

                    // 算术运算（如果你以后解析 1 + 1）
                    "+" => {
                        if let (Value::Int(a), Value::Int(b)) = (left_val, right_val) {
                            Ok(Value::Int(a + b))
                        } else {
                            Err("Type mismatch: '+' only supports Integers".into())
                        }
                    }
                    "*" => {
                        if let (Value::Int(a), Value::Int(b)) = (left_val, right_val) {
                            Ok(Value::Int(a * b))
                        } else {
                            Err("Type mismatch: '*' only supports Integers".into())
                        }
                    }
                    "-" => {
                        if let (Value::Int(a), Value::Int(b)) = (left_val, right_val) {
                            Ok(Value::Int(a - b))
                        } else {
                            Err("Type mismatch: '*' only supports Integers".into())
                        }
                    }
                    "/" => {
                        if let (Value::Int(a), Value::Int(b)) = (left_val, right_val) {
                            Ok(Value::Int(a / b))
                        } else {
                            Err("Type mismatch: '*' only supports Integers".into())
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
