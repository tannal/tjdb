use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use std::sync::{Arc, RwLock};
use crate::database::Database;
use crate::executor::Executor;
use crate::parser::{Parser, Statement};
use crate::lexer::Lexer;

pub struct TServer {
    db: Arc<RwLock<Database>>,
}

impl TServer {
    pub fn new(db: Database) -> Self {
        Self {
            db: Arc::new(RwLock::new(db)),
        }
    }

    pub async fn run(&self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(addr).await?;
        println!("[tjdb] RwLock Server initialized. Listening on {}", addr);

        loop {
            let (socket, _) = listener.accept().await?;
            let db_clone = Arc::clone(&self.db);
            
            tokio::spawn(async move {
                if let Err(e) = handle_client(socket, db_clone).await {
                    eprintln!("[tjdb] Client session error: {}", e);
                }
            });
        }
    }
}

async fn handle_client(mut socket: TcpStream, db: Arc<RwLock<Database>>) -> tokio::io::Result<()> {
    let (reader, mut writer) = socket.split();
    let mut lines = BufReader::new(reader).lines();
    let executor = Executor::new();
    
    writer.write_all(b"Welcome to tjdb Server (Concurrent Mode) v0.1\n> ").await?;

    while let Some(line) = lines.next_line().await? {
        let sql = line.trim();
        if sql.is_empty() { continue; }
        if sql.to_lowercase() == "exit" { break; }

        // 1. 预解析：判断是读操作还是写操作，以决定申请哪种锁
        let is_readonly = is_readonly_query(sql);

        // 2. 根据查询类型获取相应的锁
        let response = if is_readonly {
            // 读锁：允许多个客户端同时进入此代码块执行 SELECT
            let db_guard = db.read().unwrap();
            process_query_internal(&db_guard, &executor, sql)
        } else {
            // 写锁：排他性，确保数据修改时没有其他读写操作
            let mut db_guard = db.write().unwrap();
            process_query_mut_internal(&mut db_guard, &executor, sql)
        };

        writer.write_all(response.as_bytes()).await?;

        // 3. 动态 Prompt
        let prompt = {
            let db_guard = db.read().unwrap();
            if db_guard.in_transaction { "\ntjdb (tx)> " } else { "\ntjdb> " }
        };
        writer.write_all(prompt.as_bytes()).await?;
    }
    Ok(())
}

/// 辅助函数：快速判断是否为只读查询
fn is_readonly_query(sql: &str) -> bool {
    let sql_lower = sql.to_lowercase();
    // 只有 SELECT 且不在事务中，或者特定的系统查询可以认为是只读
    // 注意：如果处于事务中，为了简化状态管理，建议一律使用写锁
    sql_lower.starts_with("select") && !sql_lower.contains("into")
}

/// 针对只读操作的处理（使用不可变引用）
fn process_query_internal(db: &Database, executor: &Executor, sql: &str) -> String {
    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer);
    match parser.parse_statement() {
        Ok(Statement::Select(select_stmt)) => {
            match executor.build_plan(select_stmt, db) {
                Ok(plan) => {
                    let mut out = String::from("Query Results:\n");
                    for result in plan {
                        match result {
                            Ok(tuple) => out.push_str(&format!("  {:?}\n", tuple.0)),
                            Err(e) => out.push_str(&format!("  Error: {}\n", e)),
                        }
                    }
                    out
                }
                Err(e) => format!("Plan Error: {}", e),
            }
        }
        _ => "This operation requires a write lock (or invalid read syntax).".to_string(),
    }
}

/// 针对修改操作的处理（使用可变引用）
fn process_query_mut_internal(db: &mut Database, executor: &Executor, sql: &str) -> String {
    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer);
    let mut output = String::new();

    match parser.parse_statement() {
        Ok(stmt) => match stmt {
            Statement::CreateTable(cs) => match db.apply_create_table(cs) {
                Ok(_) => "Table created.".to_string(),
                Err(e) => format!("Error: {}", e),
            },
            Statement::Begin => match db.begin_transaction() {
                Ok(tid) => format!("Transaction {} started.", tid),
                Err(e) => format!("Error: {}", e),
            },
            Statement::Commit => match db.commit_transaction() {
                Ok(_) => "Committed.".to_string(),
                Err(e) => format!("Error: {}", e),
            },
            Statement::Rollback => {
                db.in_transaction = false;
                db.active_transactions.clear();
                "Rolled back.".to_string()
            }
            Statement::Insert(is) => match db.apply_insert(is) {
                Ok(_) => "Inserted.".to_string(),
                Err(e) => format!("Error: {}", e),
            },
            Statement::Update(us) => match executor.execute_update(us, db) {
                Ok(n) => format!("Updated {} rows.", n),
                Err(e) => format!("Error: {}", e),
            },
            Statement::Delete(ds) => match executor.execute_delete(ds, db) {
                Ok(n) => format!("Deleted {} rows.", n),
                Err(e) => format!("Error: {}", e),
            },
            // 如果在写锁里跑了 SELECT，也兼容处理
            Statement::Select(ss) => process_query_internal(db, executor, sql),
            _ => "Unsupported statement.".to_string(),
        },
        Err(e) => format!("Parser Error: {}", e),
    }
}