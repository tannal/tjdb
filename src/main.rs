mod executor;
mod lexer;
mod parser;
mod storage;
mod operator;

use storage::{Database, Table, ColumnDefinition, DataType, Value, Tuple};
use executor::Executor;
use std::io::{self, Write};

fn main() {
    // 1. 初始化 Schema
    let users_schema = vec![
        ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, is_nullable: false, },
        ColumnDefinition { name: "name".to_string(), data_type: DataType::Text, is_nullable: false,},
        ColumnDefinition { name: "age".to_string(), data_type: DataType::Int, is_nullable: false },
    ];

    // 2. 加载数据库
    let mut db = Database::new();
    println!("Welcome to MyDBMS v0.1");
    println!("Type 'exit' to quit.");

    match Table::load_from_csv("users", users_schema.clone()) {
        Ok(table) => {
            println!("- Loaded 'users' table from disk.");
            db.tables.insert("users".to_string(), table);
        }
        Err(_) => {
            println!("- No existing data. Created empty 'users' table.");
            db.tables.insert("users".to_string(), Table {
                name: "users".to_string(),
                columns: users_schema,
                data: vec![],
            });
        }
    }

    let executor = Executor::new();

    // --- REPL Loop 开始 ---
    loop {
        // 打印提示符
        print!("tjdb> ");
        io::stdout().flush().unwrap(); // 确保提示符立即显示

        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            println!("Error reading input.");
            continue;
        }

        let sql = input.trim();

        // 退出逻辑
        if sql.to_lowercase() == "exit" || sql.to_lowercase() == "quit" {
            break;
        }

        if sql.is_empty() {
            continue;
        }

        // 执行查询
        run_query(&mut db, &executor, sql);
    }

    // --- 退出前持久化 ---
    println!("\nShutting down...");
    db.shutdown();
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
                    // 注意：Database 需要实现 apply_insert
                    match db.apply_insert(insert_stmt) {
                        Ok(_) => println!("Successfully inserted 1 row."),
                        Err(e) => println!("Insert Error: {}", e),
                    }
                }
                parser::Statement::Update(update_stmt) => {
                    match executor.execute_update(update_stmt, db) {
                        Ok(count) => println!("Successfully updated {} rows.", count),
                        Err(e) => println!("Update Error: {}", e),
                    }
                }
                parser::Statement::Delete(update_stmt) => {
                    match executor.execute_delete(update_stmt, db) {
                        Ok(count) => println!("Successfully updated {} rows.", count),
                        Err(e) => println!("Update Error: {}", e),
                    }
                }
                // 如果你实现了 Delete，在这里添加处理逻辑
            }
        }
        Err(e) => println!("Parser Error: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Database, Table, ColumnDefinition, DataType, Value, Tuple};
    use crate::executor::Executor;

    // 辅助函数：快速创建一个带有初始数据的测试数据库
    fn setup_test_db() -> Database {
        let mut db = Database::new();
        let schema = vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, is_nullable:false },
            ColumnDefinition { name: "name".to_string(), data_type: DataType::Text, is_nullable: false },
            ColumnDefinition { name: "age".to_string(), data_type: DataType::Int, is_nullable: false },
        ];
        db.tables.insert(
            "users".to_string(),
            Table {
                name: "users".to_string(),
                columns: schema,
                data: vec![
                    Tuple(vec![Value::Int(1), Value::Text("Alice".into()), Value::Int(20)]),
                    Tuple(vec![Value::Int(2), Value::Text("Bob".into()), Value::Int(25)]),
                ],
            },
        );
        db
    }

    #[test]
    fn test_full_crud_cycle() {
        let mut db = setup_test_db();
        let executor = Executor::new();

        // 1. 测试 INSERT
        let insert_sql = "INSERT INTO users VALUES (3, 'Charlie', 30)";
        let stmt = parser::Parser::new(lexer::Lexer::new(insert_sql)).parse_statement().unwrap();
        if let parser::Statement::Insert(s) = stmt {
            db.apply_insert(s).unwrap();
        }
        assert_eq!(db.tables.get("users").unwrap().data.len(), 3);

        // 2. 测试多列 UPDATE (含算术运算)
        let update_sql = "UPDATE users SET age = age + 10, name = 'Senior' WHERE id = 1";
        let stmt = parser::Parser::new(lexer::Lexer::new(update_sql)).parse_statement().unwrap();
        if let parser::Statement::Update(s) = stmt {
            executor.execute_update(s, &mut db).unwrap();
        }
        
        // 验证更新结果
        let alice = &db.tables.get("users").unwrap().data[0];
        assert_eq!(alice.0[1], Value::Text("Senior".into()));
        assert_eq!(alice.0[2], Value::Int(30)); // 20 + 10

        // 3. 测试 DELETE
        let delete_sql = "DELETE FROM users WHERE age > 30";
        let stmt = parser::Parser::new(lexer::Lexer::new(delete_sql)).parse_statement().unwrap();
        if let parser::Statement::Delete(s) = stmt {
            let deleted = executor.execute_delete(s, &mut db).unwrap();
            assert_eq!(deleted, 0);
        }
    }

    #[test]
    fn test_type_mismatch_error() {
        let mut db = setup_test_db();
        let executor = Executor::new();
        
        // 故意尝试错误的类型赋值
        let error_sql = "UPDATE users SET age = 'NotInt' WHERE id = 1";
        let stmt = parser::Parser::new(lexer::Lexer::new(error_sql)).parse_statement().unwrap();
        if let parser::Statement::Update(s) = stmt {
            let result = executor.execute_update(s, &mut db);
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("Type mismatch"));
        }
    }
}