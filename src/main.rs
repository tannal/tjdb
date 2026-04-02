mod database;
mod executor;
mod lexer;
mod operator;
mod parser;
mod storage;
mod wal;
mod checkpoint;

use executor::Executor;
use std::io::{self, Write};
use crate::database::Database;
use crate::parser::Statement;

fn main() {
    // 1. 初始化数据库
    // Database::new 内部应当实现：
    //   a. 扫描 ./data 文件夹下的 *.schema.json
    //   b. 加载对应的 .csv 数据文件
    //   c. 进行 WAL 恢复 (Redo)
    let mut db = Database::new("./data/wal.log".into());
    
    println!("Welcome to MyDBMS v0.1");
    println!("Available commands: CREATE TABLE, SELECT, INSERT, UPDATE, DELETE");
    println!("Transaction: BEGIN, COMMIT, ROLLBACK | System: CHECKPOINT, DUMP, EXIT");

    // 2. 自动加载逻辑提示
    if db.tables.is_empty() {
        println!("- No tables found in ./data/. Use 'CREATE TABLE' to start.");
    } else {
        println!("- Loaded {} table(s) from disk: {:?}", db.tables.len(), db.tables.keys().collect::<Vec<_>>());
    }

    let executor = Executor::new();

    // --- REPL Loop ---
    loop {
        let prompt = if db.in_transaction { "tjdb (tx)> " } else { "tjdb> " };
        print!("{}", prompt);
        io::stdout().flush().unwrap();

        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            break;
        }

        let sql = input.trim();
        if sql.is_empty() { continue; }

        let sql_lower = sql.to_lowercase();
        if sql_lower == "exit" || sql_lower == "quit" { break; }

        // 系统调试指令
        match sql_lower.as_str() {
            "dump" => {
                let _ = db.wal.dump_log();
                continue;
            }
            "checkpoint" => {
                match db.create_checkpoint() {
                    Ok(_) => println!("Checkpoint persisted to disk."),
                    Err(e) => println!("Checkpoint Error: {}", e),
                }
                continue;
            }
            _ => {}
        }

        // 执行 SQL
        run_query(&mut db, &executor, sql);
    }

    println!("\nShutting down gracefully...");
    db.shutdown();
}

fn run_query(db: &mut Database, executor: &Executor, sql: &str) {
    let lexer = lexer::Lexer::new(sql);
    let mut parser = parser::Parser::new(lexer);

    match parser.parse_statement() {
        Ok(stmt) => match stmt {
            // --- DDL (数据定义语言) ---
            Statement::CreateTable(create_stmt) => {
                match db.apply_create_table(create_stmt) {
                    Ok(_) => println!("Table created successfully."),
                    Err(e) => println!("Create Table Error: {}", e),
                }
            }

            // --- 事务控制 ---
            Statement::Begin => {
                match db.begin_transaction() {
                    Ok(tid) => println!("Transaction {} started.", tid),
                    Err(e) => println!("Begin Error: {}", e),
                }
            }
            Statement::Commit => {
                match db.commit_transaction() {
                    Ok(_) => println!("Transaction committed."),
                    Err(e) => println!("Commit Error: {}", e),
                }
            }
            Statement::Rollback => {
                db.in_transaction = false;
                db.current_txn_id = None;
                db.active_transactions.clear();
                println!("Transaction rolled back.");
            }

            // --- DML (数据操作语言) ---
            Statement::Select(select_stmt) => {
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
            Statement::Insert(insert_stmt) => {
                match db.apply_insert(insert_stmt) {
                    Ok(_) => {
                        if db.in_transaction {
                            println!("1 row staged in transaction.");
                        } else {
                            println!("1 row inserted (autocommit).");
                        }
                    }
                    Err(e) => println!("Insert Error: {}", e),
                }
            }
            Statement::Update(update_stmt) => {
                match executor.execute_update(update_stmt, db) {
                    Ok(count) => println!("Successfully updated {} rows.", count),
                    Err(e) => println!("Update Error: {}", e),
                }
            }
            Statement::Delete(delete_stmt) => {
                match executor.execute_delete(delete_stmt, db) {
                    Ok(count) => println!("Successfully deleted {} rows.", count),
                    Err(e) => println!("Delete Error: {}", e),
                }
            }
        },
        Err(e) => println!("Parser Error: {}", e),
    }
}