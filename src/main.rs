// src/main.rs
mod executor;
mod lexer;
mod operator;
mod parser;
mod storage;

use executor::Executor;
use lexer::Lexer;
use parser::Parser;
use storage::{Database, Table, Tuple};

fn main() {
    // 1. 初始化数据库及测试数据
    let mut db = Database::new();
    db.tables.insert(
        "users".to_string(),
        Table {
            name: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
            data: vec![
                Tuple(vec!["1".to_string(), "Alice".to_string(), "20".to_string()]),
                Tuple(vec!["2".to_string(), "Bob".to_string(), "25".to_string()]),
                Tuple(vec![
                    "3".to_string(),
                    "Charlie".to_string(),
                    "30".to_string(),
                ]),
                Tuple(vec!["4".to_string(), "Meng".to_string(), "20".to_string()]),
            ],
        },
    );

    // 2. 测试案例 A：带 WHERE 条件的查询
    let sql_where = "SELECT name, age FROM users WHERE id = 2";
    println!("--- Testing SQL: {} ---", sql_where);
    run_query(&db, sql_where);

    // 3. 测试案例 B：不带 WHERE 的全表查询 (验证向后兼容)
    let sql_simple = "SELECT id, name FROM users";
    println!("\n--- Testing SQL: {} ---", sql_simple);
    run_query(&db, sql_simple);
}

fn run_query(db: &Database, sql: &str) {
    let lexer = lexer::Lexer::new(sql);
    let mut parser = parser::Parser::new(lexer);

    if let Ok(ast) = parser.parse_statement() {
        // 使用 match 或 if let 解包 Statement
        match ast {
            parser::Statement::Select(select_stmt) => {
                // 现在 select_stmt 的类型是 SelectStatement，可以传给 build_plan 了
                let plan = executor::Executor::build_plan(select_stmt, &db);

                println!("Query Results:");
                for result in plan {
                    if let Ok(tuple) = result {
                        println!("{:?}", tuple.0);
                    }
                }
            }
            // 如果以后有 Statement::Insert(insert_stmt)，在这里处理
            _ => println!("Unsupported statement type"),
        }
    }
}
