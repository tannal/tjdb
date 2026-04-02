// src/main.rs
mod executor;
mod lexer;
mod operator;
mod parser;
mod storage;

use storage::{Database, Table, Tuple, Value};

use crate::{executor::Executor, storage::{ColumnDefinition, DataType}};

fn main() {
    // 1. 初始化数据库及测试数据
    // 注意：现在的数据存储使用的是 Value 枚举而不是 String
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
            data: vec![ /* ... 数据保持不变 ... */ ],
        },
    );

    let test_cases = vec![
        
        "INSERT INTO users VALUES (5, 'Gemini', 1)",
        // --- 1. 基础读取测试 ---
        "SELECT name FROM users WHERE age <= 20",

        // --- 2. 数据写入测试 (INSERT) ---
        "INSERT INTO users VALUES (6, 'Rustacean', 3)",
        
        // 验证写入是否成功：查询刚才插入的数据
        "SELECT id, name, age FROM users WHERE id >= 5",

        // --- 3. 优先级与复杂算术测试 ---
        // 验证优先级：1 + 2 * 3 = 7 (不是 9)
        "SELECT name FROM users WHERE age + 1 * 2 > 25",
        // 括号改变优先级
        "SELECT name FROM users WHERE (age + 1) * 2 > 50",

        // --- 4. 严谨的语义/类型检查测试 ---
        // 错误：比较类型不匹配 (Text = Int)
        "SELECT id FROM users WHERE name = 123",
        // 错误：WHERE 返回了 Int 而非 Bool
        "SELECT name FROM users WHERE age - 5",
        // 错误：算术运算应用在了字符串上
        "SELECT id FROM users WHERE name + 1 = 2",

        // --- 5. 写入错误处理 ---
        // 错误：插入不存在的表
        "INSERT INTO users VALUES (1, 'Boo', 99)",
        "INSERT INTO ghost_table VALUES (1, 'Boo', 99)",
        // 错误：列数不匹配 (表中定义了 3 列，这里只给了 2 列)
        "INSERT INTO users VALUES (7, 'Error')",

        // --- 6. 最终全表扫描 ---
        "SELECT id, name, age FROM users", // 如果你支持 * 的话，否则用 id, name, age
    ];

    for sql in test_cases {
        println!("\n--- Executing: {} ---", sql);
        run_query(&mut db, sql);
    }
    
}

fn run_query(db: &mut Database, sql: &str) {
    let lexer = lexer::Lexer::new(sql);
    let mut parser = parser::Parser::new(lexer);

    match parser.parse_statement() {
        Ok(ast) => {
            let executor = executor::Executor::new(db);
            match ast {
                parser::Statement::Select(select_stmt) => {
                    println!("Debug AST: {:?}", select_stmt.where_clause);
                    
                    // --- 核心变化点 ---
                    // 1. 实例化 Executor (它持有 db 的引用)
                    
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
                parser::Statement::Insert(insert_stmt) => {
                    match db.apply_insert(insert_stmt) {
                        Ok(_) => println!("Successfully inserted 1 row."),
                        Err(e) => println!("Insert Error: {}", e), // 此时 'Error' 那条用例会打印错误而不是存入脏数据
                    }
                }
                _ => println!("Unsupported statement type"),
            }
        }
        Err(e) => println!("Parser Error: {}", e),
    }
}