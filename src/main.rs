// src/main.rs
mod lexer;
mod parser;
mod storage;
mod executor;

use storage::{Database, Table, Tuple};
use executor::Executor;
use lexer::Lexer;
use parser::Parser;

fn main() {
    // 1. 初始化数据库及测试数据
    let mut db = Database::new();
    db.tables.insert("users".to_string(), Table {
        name: "users".to_string(),
        columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
        data: vec![
            Tuple(vec!["1".to_string(), "Alice".to_string(), "20".to_string()]),
            Tuple(vec!["2".to_string(), "Bob".to_string(), "25".to_string()]),
            Tuple(vec!["3".to_string(), "Charlie".to_string(), "30".to_string()]),
            Tuple(vec!["4".to_string(), "Meng".to_string(), "20".to_string()]),
        ],
    });

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
    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer);

    match parser.parse_statement() {
        Ok(ast) => {
            println!("AST generated successfully.");
            // 如果你想看 AST 结构，可以取消注释下面这行
            // println!("{:#?}", ast);

            let exec = Executor::new(db);
            match exec.execute(ast) {
                Ok(results) => {
                    println!("Query Results ({} rows):", results.len());
                    for row in results {
                        println!("  {:?}", row.0);
                    }
                }
                Err(e) => println!("Execution Error: {}", e),
            }
        }
        Err(e) => println!("Parser Error: {}", e),
    }
}