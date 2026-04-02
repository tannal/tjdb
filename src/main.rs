// src/main.rs
mod executor;
mod lexer;
mod operator;
mod parser;
mod storage;

use storage::{Database, Table, Tuple, Value};

use crate::executor::Executor;

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
                Tuple(vec![Value::Int(20), Value::Text("id20".into()), Value::Int(20)]),
            ],
        },
    );

    let test_cases = vec![
        // 1. 基础比较
        "SELECT name FROM users WHERE id = 1",
        "SELECT name FROM users WHERE age <= 20",
        "SELECT id FROM users WHERE name = 'Alice'",

        // 2. 算术运算 (验证加、减、乘)
        "SELECT name FROM users WHERE (age + 1) > (id * 10)",
        "SELECT name FROM users WHERE (age - 5) < (id * 2)",
        
        // 3. 混合运算与优先级 (挑战你的 Parser 逻辑)
        // 预期: 20 + 1 * 2 = 22. 如果你的解析器是左结合，会算成 (20+1)*2 = 42
        "SELECT name FROM users WHERE age + 1 * 2 > 21",

        // 4. 括号嵌套 (深度递归测试)
        "SELECT name FROM users WHERE ((age - 1) * 2) > (id + 30)",

        // 5. 复杂表达式：列与列的计算
        "SELECT name FROM users WHERE (age - id) = 19",

        // 6. 边界与错误处理
        "SELECT name FROM users WHERE age > (10 + 20) * 0", // 常量计算
        "SELECT name FROM users WHERE age - 20",           // 语义错误：结果不是 Bool (Int(0))
        "SELECT name FROM non_existent_table",            // 计划错误：表不存在
        
        // 7. 无 WHERE 过滤的全表扫描
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
                    
                    // --- 核心变化点 ---
                    // 1. 实例化 Executor (它持有 db 的引用)
                    let executor = executor::Executor::new(db);
                    
                    // 2. 调用 build_plan (注意现在是 &self 调用)
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
                        Err(e) => println!("Plan Error: {}", e), // 这里会捕获到类型检查错误
                    }
                }
                _ => println!("Unsupported statement type"),
            }
        }
        Err(e) => println!("Parser Error: {}", e),
    }
}