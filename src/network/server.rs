use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use std::sync::{Arc, Mutex};
use crate::database::Database;
use crate::executor::Executor;
use crate::parser::{Parser, Statement};
use crate::lexer::Lexer;

pub struct TredisServer {
    db: Arc<Mutex<Database>>,
}

impl TredisServer {
    /// 创建一个新的服务器实例
    pub fn new(db: Database) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
        }
    }

    /// 启动异步服务器循环
    pub async fn run(&self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(addr).await?;
        println!("[tjdb] Server protocol initialized. Listening on {}", addr);

        loop {
            let (socket, _) = listener.accept().await?;
            let db_clone = Arc::clone(&self.db);
            
            // 为每个 TCP 连接开启一个 Tokio Task (类似于轻量级线程)
            tokio::spawn(async move {
                if let Err(e) = handle_client(socket, db_clone).await {
                    eprintln!("[tjdb] Client session error: {}", e);
                }
            });
        }
    }
}

/// 处理单个客户端连接的会话
async fn handle_client(mut socket: TcpStream, db: Arc<Mutex<Database>>) -> tokio::io::Result<()> {
    let (reader, mut writer) = socket.split();
    let mut lines = BufReader::new(reader).lines();
    
    // 每个会话创建一个 Executor (如果 Executor 是无状态的)
    let executor = Executor::new();
    
    writer.write_all(b"Welcome to tjdb Server v0.1\nType 'EXIT' to disconnect.\n").await?;

    while let Some(line) = lines.next_line().await? {
        let sql = line.trim();
        if sql.is_empty() { continue; }
        if sql.to_lowercase() == "exit" || sql.to_lowercase() == "quit" {
            writer.write_all(b"Goodbye!\n").await?;
            break;
        }

        // --- 核心执行逻辑锁定区 ---
        // 我们在代码块内获取锁，确保执行完立即释放，不阻塞其他客户端
        let response = {
            let mut db_guard = db.lock().unwrap();
            process_query(&mut db_guard, &executor, sql)
        };

        // 发送结果和 Prompt
        writer.write_all(response.as_bytes()).await?;
        
        // 根据事务状态动态显示 Prompt
        let prompt = {
            let db_guard = db.lock().unwrap();
            if db_guard.in_transaction { "\ntjdb (tx)> " } else { "\ntjdb> " }
        };
        writer.write_all(prompt.as_bytes()).await?;
    }
    
    Ok(())
}

/// 将原来的 run_query 逻辑适配为返回 String，方便网络传输
fn process_query(db: &mut Database, executor: &Executor, sql: &str) -> String {
    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer);
    let mut output = String::new();

    match parser.parse_statement() {
        Ok(stmt) => match stmt {
            // --- DDL ---
            Statement::CreateTable(create_stmt) => {
                match db.apply_create_table(create_stmt) {
                    Ok(_) => output.push_str("Table created successfully."),
                    Err(e) => output.push_str(&format!("Create Table Error: {}", e)),
                }
            }

            // --- 事务控制 ---
            Statement::Begin => {
                match db.begin_transaction() {
                    Ok(tid) => output.push_str(&format!("Transaction {} started.", tid)),
                    Err(e) => output.push_str(&format!("Begin Error: {}", e)),
                }
            }
            Statement::Commit => {
                match db.commit_transaction() {
                    Ok(_) => output.push_str("Transaction committed."),
                    Err(e) => output.push_str(&format!("Commit Error: {}", e)),
                }
            }
            Statement::Rollback => {
                db.in_transaction = false;
                db.current_txn_id = None;
                db.active_transactions.clear();
                output.push_str("Transaction rolled back.");
            }

            // --- DML ---
            Statement::Select(select_stmt) => {
                match executor.build_plan(select_stmt, db) {
                    Ok(plan) => {
                        output.push_str("Query Results:\n");
                        for result in plan {
                            match result {
                                Ok(tuple) => output.push_str(&format!("  {:?}\n", tuple.0)),
                                Err(e) => output.push_str(&format!("  Execution Error: {}\n", e)),
                            }
                        }
                    }
                    Err(e) => output.push_str(&format!("Plan Error: {}", e)),
                }
            }
            Statement::Insert(insert_stmt) => {
                match db.apply_insert(insert_stmt) {
                    Ok(_) => {
                        if db.in_transaction {
                            output.push_str("1 row staged in transaction.");
                        } else {
                            output.push_str("1 row inserted (autocommit).");
                        }
                    }
                    Err(e) => output.push_str(&format!("Insert Error: {}", e)),
                }
            }
            Statement::Update(update_stmt) => {
                match executor.execute_update(update_stmt, db) {
                    Ok(count) => output.push_str(&format!("Successfully updated {} rows.", count)),
                    Err(e) => output.push_str(&format!("Update Error: {}", e)),
                }
            }
            Statement::Delete(delete_stmt) => {
                match executor.execute_delete(delete_stmt, db) {
                    Ok(count) => output.push_str(&format!("Successfully deleted {} rows.", count)),
                    Err(e) => output.push_str(&format!("Delete Error: {}", e)),
                }
            }
        },
        Err(e) => output.push_str(&format!("Parser Error: {}", e)),
    }
    output
}