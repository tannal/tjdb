// src/main.rs

mod executor;
mod lexer;
mod operator;
mod parser;
mod storage;

use storage::{Database, Table, ColumnDefinition, DataType, Value, Tuple};
use executor::Executor;

fn main() {
    // 1. 初始化数据库
    let mut db = Database::new();
    db.tables.insert(
        "users".to_string(),
        Table {
            name: "users".to_string(),
            columns: vec![
                ColumnDefinition { name: "id".to_string(), data_type: DataType::Int },
                ColumnDefinition { name: "name".to_string(), data_type: DataType::Text },
                ColumnDefinition { name: "age".to_string(), data_type: DataType::Int },
            ],
            data: vec![
                Tuple(vec![Value::Int(1), Value::Text("Alice".into()), Value::Int(20)]),
                Tuple(vec![Value::Int(2), Value::Text("Bob".into()), Value::Int(25)]),
            ],
        },
    );

    let test_cases = vec![
        // --- 1. 初始查询 ---
        "SELECT id, name, age FROM users",

        // --- 2. 基础更新：把 Alice 的年龄改为 21 ---
        "UPDATE users SET age = 21 WHERE name = 'Alice'",
        "SELECT name, age FROM users WHERE name = 'Alice'",

        // --- 3. 计算更新：所有人都老 10 岁 ---
        "UPDATE users SET age = age + 10",
        "SELECT id, name, age FROM users",

        // --- 4. 复杂条件更新：给 30 岁以上的人改名 (虽然这有点怪) ---
        "UPDATE users SET name = 'Senior' WHERE age > 30",
        "SELECT * FROM users",

        // --- 5. 错误处理测试 ---
        // 错误：类型不匹配 (把字符串赋给 Int 列)
        "UPDATE users SET age = 'TooOld' WHERE id = 1",
        // 错误：列不存在
        "UPDATE users SET salary = 5000 WHERE id = 1",
        // 错误：WHERE 条件类型不对
        "UPDATE users SET age = 40 WHERE name + 1",

        // --- 6. 最终状态确认 ---
        "SELECT id, name, age FROM users",
    ];

    let executor = Executor::new();

    for sql in test_cases {
        println!("\n--- Executing: {} ---", sql);
        run_query(&mut db, &executor, sql);
    }
}

fn run_query(db: &mut Database, executor: &Executor, sql: &str) {
    let lexer = lexer::Lexer::new(sql);
    let mut parser = parser::Parser::new(lexer);

    match parser.parse_statement() {
        Ok(stmt) => {
            match stmt {
                parser::Statement::Select(select_stmt) => {
                    match executor.build_plan(select_stmt, db) {
                        Ok(plan) => {
                            println!("Query Results:");
                            for result in plan {
                                match result {
                                    Ok(tuple) => println!("  {:?}", tuple.0),
                                    Err(e) => println!("  Execution Error: {}", e),
                                }
                            }
                        }
                        Err(e) => println!("Plan Error: {}", e),
                    }
                }
                parser::Statement::Insert(insert_stmt) => {
                    match db.apply_insert(insert_stmt) {
                        Ok(_) => println!("Successfully inserted 1 row."),
                        Err(e) => println!("Insert Error: {}", e),
                    }
                }
                parser::Statement::Update(update_stmt) => {
                    // 注意：这里传入 &mut db
                    match executor.execute_update(update_stmt, db) {
                        Ok(count) => println!("Successfully updated {} rows.", count),
                        Err(e) => println!("Update Error: {}", e),
                    }
                }
            }
        }
        Err(e) => println!("Parser Error: {}", e),
    }
}