// src/operator/mod.rs
pub mod scan;
pub mod filter;
pub mod project;
pub mod aggregate;

// 建议在这里统一定义 Trait，方便其他地方引用
use crate::storage::Tuple;
pub type ExecuteResult = Result<Tuple, String>;

pub trait Operator: Iterator<Item = ExecuteResult> {}