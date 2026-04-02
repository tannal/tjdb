// src/main.rs
mod executor;
mod lexer;
mod operator;
mod parser;
mod storage;

use storage::{Database, Table, Tuple, Value};

fn main() {
    // 1. 初始化数据库及测试数据
    // 注意：现在的数据存储使用的是 Value 枚举而不是 String
    let mut db = Database::new();
    db.tables.insert(
        "users".to_string(),
        Table {
            name: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
            data: vec![
                Tuple(vec![Value::Int(1), Value::Text("Alice".into()), Value::Int(20)]),
                Tuple(vec![Value::Int(2), Value::Text("Bob".into()), Value::Int(25)]),
                Tuple(vec![Value::Int(3), Value::Text("Charlie".into()), Value::Int(30)]),
                Tuple(vec![Value::Int(4), Value::Text("Meng".into()), Value::Int(20)]),
            ],
        },
    );

    let test_cases = vec![
        // A. 基础等值查询
        "SELECT name FROM users WHERE id = 1",
        
        // B. 整数大于比较 (验证 age > 21)
        "SELECT name, age FROM users WHERE age > 21",
        
        // C. 整数小于等于比较 (验证 age <= 20)
        "SELECT name FROM users WHERE age <= 20",
        
        // D. 字符串匹配 (假设你支持 WHERE name = 'Alice')
        "SELECT id FROM users WHERE name = 'Alice'",
        
        // E. 查无数据的情况
        "SELECT name FROM users WHERE age > 100",
        
        // F. 查询不存在的表 (应返回 Error)
        "SELECT name FROM non_existent_table",
        
        // G. 全表扫描 (无 WHERE)
        "SELECT id, name, age FROM users",
    ];

    for sql in test_cases {
        println!("\n--- Executing: {} ---", sql);
        run_query(&db, sql);
    }
    
}

fn run_query(db: &Database, sql: &str) {
    let lexer = lexer::Lexer::new(sql);
    let mut parser = parser::Parser::new(lexer);

    match parser.parse_statement() {
        Ok(ast) => {
            match ast {
                parser::Statement::Select(select_stmt) => {
                    println!("Debug AST: {:?}", select_stmt.where_clause);
                    // 只调用一次 build_plan
                    match executor::Executor::build_plan(select_stmt, db) {
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
                _ => println!("Unsupported statement type"),
            }
        }
        Err(e) => println!("Parser Error: {}", e),
    }
}