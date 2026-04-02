use crate::{
    operator::{ExecuteResult, Operator},
    parser::Expression,
    storage::{Table, Tuple},
};

pub struct FilterOperator<'a> {
    source: Box<dyn Operator + 'a>,
    condition: Expression,
    table_schema: &'a Table, // 用于 evaluate 时查找列索引
}

impl<'a> FilterOperator<'a> {
    pub fn new(
        source: Box<dyn Operator + 'a>,
        condition: Expression,
        table_schema: &'a Table,
    ) -> Self {
        Self {
            source,
            condition,
            table_schema,
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
                // 找到左侧列在原表中的索引
                let idx = table
                    .columns
                    .iter()
                    .position(|c| c == left)
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
