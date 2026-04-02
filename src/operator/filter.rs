use crate::{
    operator::{ExecuteResult, Operator},
    parser::Expression,
    storage::{Table, Tuple, Value},
};

pub struct FilterOperator<'a> {
    source: Box<dyn Operator + 'a>,
    condition: Expression,
    table_schema: &'a Table, // 用于 evaluate 时查找列索引
    column_index: usize,     // 预先找好的索引
}

impl<'a> FilterOperator<'a> {
    pub fn new(
        source: Box<dyn Operator + 'a>,
        condition: Expression,
        table_schema: &'a Table,
    ) -> Self {
        let idx = match &condition {
            Expression::BinaryOp { left, .. } => {
                table_schema.columns.iter().position(|c| c == left).unwrap()
            }
        };
        Self {
            source,
            condition,
            table_schema,
            column_index: idx,
        }
    }
    pub fn evaluate_expression(
        &self,
        expr: &Expression,
        tuple: &Tuple,
        table: &Table,
    ) -> Result<bool, String> {
        match expr {
            Expression::BinaryOp { left, op, right } => {
                let left_val = &tuple.0[self.column_index];

                // 将 SQL 中的字面量（字符串）转换为对应的 Value 类型进行比较
                // 实际工程中，这个转换应该在 Planner 阶段完成，而不是执行阶段
                let right_val = Value::from_str_typed(right, left_val);

                match op.as_str() {
                    "=" => Ok(left_val == &right_val),
                    ">" => Ok(left_val > &right_val),
                    "<" => Ok(left_val < &right_val),
                    ">=" => Ok(left_val >= &right_val),
                    "<=" => Ok(left_val <= &right_val),
                    "!=" => Ok(left_val != &right_val),
                    _ => Err(format!("Unsupported operator: {}", op)),
                }
            }
        }
    }
}

impl<'a> Iterator for FilterOperator<'a> {
    type Item = ExecuteResult;
    fn next(&mut self) -> Option<Self::Item> {
        // 在 FilterOperator 的 next 方法中
        while let Some(item) = self.source.next() {
            match item {
                Ok(tuple) => {
                    // 调用 evaluate_expression 并处理其 Result
                    match self.evaluate_expression(&self.condition, &tuple, self.table_schema) {
                        Ok(true) => return Some(Ok(tuple)), // 条件匹配，返回这一行
                        Ok(false) => continue,              // 不匹配，跳过，继续找下一行
                        Err(e) => return Some(Err(e)), // 表达式计算报错（如列名写错），向上层抛出错误
                    }
                }
                Err(e) => return Some(Err(e)), // 下层算子报错，直接透传给上层
            }
        }
        None
    }
}
impl<'a> Operator for FilterOperator<'a> {}
