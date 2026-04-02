// src/main.rs (部分)
mod lexer;
mod parser;
mod storage;
mod executor;

use storage::{Database, Table, Tuple};
use executor::Executor;

fn main() {
    // 1. 初始化模拟数据
    let mut db = Database::new();
    db.tables.insert("users".to_string(), Table {
        name: "users".to_string(),
        columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
        data: vec![
            Tuple(vec!["1".to_string(), "Alice".to_string(), "20".to_string()]),
            Tuple(vec!["2".to_string(), "Bob".to_string(), "25".to_string()]),
        ],
    });

    // 2. 解析 SQL
    let sql = "SELECT name, id FROM users"; // 换个顺序试试
    let mut parser = parser::Parser::new(lexer::Lexer::new(sql));
    let ast = parser.parse_statement().unwrap();

    // 3. 执行
    let exec = Executor::new(&db);
    let results = exec.execute(ast).unwrap();

    // 4. 打印结果
    println!("Query Results:");
    for row in results {
        println!("{:?}", row.0);
    }
}