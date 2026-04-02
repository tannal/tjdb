use serde::{Serialize, Deserialize};
use crate::storage::Value;

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Query(String),      // 执行 SQL
    Ping,               // 测试连接
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    ResultSet(Vec<Vec<Value>>), // 查询结果
    AffectedRows(usize),        // 增删改影响行数
    Error(String),              // 报错信息
}