mod database;
mod executor;
mod lexer;
mod operator;
mod parser;
mod storage;
mod wal;
mod checkpoint;
mod network;

use crate::network::server::TredisServer;
use crate::database::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 初始化数据库
    let db = Database::new("./data/wal.log".into());
    
    // 2. 创建并运行服务器
    let server = TredisServer::new(db);
    
    // 3. 启动（这会阻塞主线程直到程序关闭）
    server.run("127.0.0.1:12345").await?;

    Ok(())
}