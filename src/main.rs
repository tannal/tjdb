mod executor;
mod lexer;
mod parser;
mod storage;
mod operator;

use storage::{Database, Table, ColumnDefinition, DataType, Value, Tuple};
use executor::Executor;

fn main() {
    // 1. 定义表结构 (Schema)
    // 在实际生产中，Schema 通常也存在特定的系统表里，这里我们手动定义
    let users_schema = vec![
        ColumnDefinition {name:"id".to_string(),data_type:DataType::Int, is_nullable: false },
        ColumnDefinition {name:"name".to_string(),data_type:DataType::Text, is_nullable: false },
        ColumnDefinition {name:"age".to_string(),data_type:DataType::Int, is_nullable: false },
    ];

    // 2. 初始化数据库并尝试从磁盘加载数据
    let mut db = Database::new();
    
    println!("--- Loading Data from Disk ---");
    match Table::load_from_csv("users", users_schema.clone()) {
        Ok(table) => {
            println!("Loaded users table with {} rows.", table.data.len());
            db.tables.insert("users".to_string(), table);
        }
        Err(e) => {
            println!("No existing data found or error loading: {}. Starting fresh.", e);
            // 如果加载失败（比如第一次运行），则创建一个空表
            db.tables.insert(
                "users".to_string(),
                Table {
                    name: "users".to_string(),
                    columns: users_schema,
                    data: vec![],
                },
            );
        }
    }

    let test_cases = vec![
        // 尝试插入数据（如果文件里已有 id=4，下次运行会再多一条）
        "INSERT INTO users VALUES (4, 'Dave', 40)",
        "SELECT * FROM users",
        
        // 多列更新
        "UPDATE users SET age = age + 1, name = 'Senior' WHERE age > 35",
        
        // 验证结果
        "SELECT * FROM users",
    ];

    let executor = Executor::new();

    for sql in test_cases {
        println!("\n--- Executing: {} ---", sql);
        run_query(&mut db, &executor, sql);
    }

    // 3. 关键：退出前持久化数据
    println!("\n--- Shutting Down: Saving to Disk ---");
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
                // 如果你实现了 Delete，在这里添加处理逻辑
            }
        }
        Err(e) => println!("Parser Error: {}", e),
    }
}