use crate::storage::{Table, Tuple, Value};
use crate::parser::{SelectItem, AggregateFunc};
use crate::operator::Operator;
use std::cmp::{min, max};

pub struct AggregateOperator<'a> {
    child: Box<dyn Operator + 'a>,
    select_items: Vec<SelectItem>,
    table_metadata: &'a Table,
    column_indices: Vec<Option<usize>>, // 预处理列索引
    done: bool,
}

impl<'a> AggregateOperator<'a> {
    pub fn new(
        child: Box<dyn Operator + 'a>,
        select_items: Vec<SelectItem>,
        table_metadata: &'a Table,
    ) -> Result<Self, String> {
        let mut column_indices = Vec::new();
        
        for item in &select_items {
            let idx = if let SelectItem::Aggregate(agg_func) = item {
                match agg_func {
                    // SUM, MIN, MAX 都需要指定列索引
                    AggregateFunc::Sum(col_name) | 
                    AggregateFunc::Min(col_name) | 
                    AggregateFunc::Max(col_name) => {
                        Some(table_metadata.columns.iter()
                            .position(|c| &c.name == col_name)
                            .ok_or_else(|| format!("Column '{}' not found", col_name))?)
                    },
                    AggregateFunc::CountWildcard => None,
                }
            } else {
                None
            };
            column_indices.push(idx);
        }

        Ok(Self {
            child,
            select_items,
            table_metadata,
            column_indices,
            done: false,
        })
    }
}

impl<'a> Iterator for AggregateOperator<'a> {
    type Item = Result<Tuple, String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        // 1. 初始化聚合状态
        let mut count_val = 0i32;
        let mut sums: Vec<i32> = vec![0; self.select_items.len()];
        let mut mins: Vec<i32> = vec![i32::MAX; self.select_items.len()];
        let mut maxs: Vec<i32> = vec![i32::MIN; self.select_items.len()];
        let mut has_data = false;

        // 2. 迭代并计算
        while let Some(result) = self.child.next() {
            match result {
                Ok(tuple) => {
                    has_data = true;
                    count_val += 1;
                    
                    for (i, &col_idx) in self.column_indices.iter().enumerate() {
                        if let Some(idx) = col_idx {
                            if let Value::Int(v) = tuple.0[idx] {
                                // 根据不同的聚合函数更新状态
                                sums[i] += v;
                                mins[i] = min(mins[i], v);
                                maxs[i] = max(maxs[i], v);
                            }
                        }
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }

        // 3. 构造结果行
        let mut result_values = Vec::new();
        for (i, item) in self.select_items.iter().enumerate() {
            match item {
                SelectItem::Aggregate(agg_func) => match agg_func {
                    AggregateFunc::CountWildcard => {
                        result_values.push(Value::Int(count_val));
                    }
                    AggregateFunc::Sum(_) => {
                        result_values.push(Value::Int(sums[i]));
                    }
                    AggregateFunc::Min(_) => {
                        // 如果没数据，MIN 理论上应为 NULL，这里暂存 MAX 初始值或 0
                        let val = if has_data { mins[i] } else { 0 };
                        result_values.push(Value::Int(val));
                    }
                    AggregateFunc::Max(_) => {
                        let val = if has_data { maxs[i] } else { 0 };
                        result_values.push(Value::Int(val));
                    }
                },
                _ => return Some(Err("AggregateOperator: Non-aggregate item found".into())),
            }
        }

        self.done = true;
        Some(Ok(Tuple(result_values)))
    }
}

impl<'a> Operator for AggregateOperator<'a> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer;
    use crate::storage::{Database, Table, ColumnDefinition, DataType, Value, Tuple};
    use crate::executor::Executor;
    use crate::parser::{self, AggregateFunc, SelectItem};

    // 辅助函数：创建一个包含测试数据的内存表
    fn setup_aggregate_db() -> Database {
        let mut db = Database::new();
        let schema = vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, is_nullable: false },
            ColumnDefinition { name: "age".to_string(), data_type: DataType::Int, is_nullable: false },
        ];
        db.tables.insert(
            "users".to_string(),
            Table {
                name: "users".to_string(),
                columns: schema,
                data: vec![
                    Tuple(vec![Value::Int(1), Value::Int(20)]),
                    Tuple(vec![Value::Int(2), Value::Int(30)]),
                    Tuple(vec![Value::Int(3), Value::Int(40)]),
                ],
            },
        );
        db
    }

    #[test]
    fn test_aggregate_functions() {
        let db = setup_aggregate_db();
        let executor = Executor::new();

        // 测试 SQL: SELECT COUNT(*), SUM(age), MIN(age), MAX(age) FROM users WHERE age > 25
        // 预期匹配行：age=30, age=40 (共2行)
        // 预期结果：COUNT=2, SUM=70, MIN=30, MAX=40
        let sql = "SELECT COUNT(*), SUM(age), MIN(age), MAX(age) FROM users WHERE age > 25";
        
        let mut parser = parser::Parser::new(lexer::Lexer::new(sql));
        let stmt = parser.parse_statement().expect("Parser failed");

        if let parser::Statement::Select(select_stmt) = stmt {
            let mut plan = executor.build_plan(select_stmt, &db).expect("Build plan failed");
            
            // 获取聚合结果（聚合查询只返回一行）
            let result = plan.next().expect("No result row").expect("Execution error");
            
            assert_eq!(result.0[0], Value::Int(2));  // COUNT
            assert_eq!(result.0[1], Value::Int(70)); // SUM
            assert_eq!(result.0[2], Value::Int(30)); // MIN
            assert_eq!(result.0[3], Value::Int(40)); // MAX
            
            // 确保没有第二行
            assert!(plan.next().is_none());
        } else {
            panic!("Expected Select statement");
        }
    }

    #[test]
    fn test_empty_set_aggregation() {
        let db = setup_aggregate_db();
        let executor = Executor::new();

        // 测试 SQL: 没有行匹配的情况
        let sql = "SELECT COUNT(*), SUM(age) FROM users WHERE age > 100";
        let mut parser = parser::Parser::new(lexer::Lexer::new(sql));
        let stmt = parser.parse_statement().unwrap();

        if let parser::Statement::Select(select_stmt) = stmt {
            let mut plan = executor.build_plan(select_stmt, &db).unwrap();
            let result = plan.next().unwrap().unwrap();
            
            assert_eq!(result.0[0], Value::Int(0)); // COUNT 应为 0
            assert_eq!(result.0[1], Value::Int(0)); // SUM 根据你的实现目前为 0
        }
    }
}