use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::checkpoint::Checkpoint;
use crate::parser::{CreateTableStatement, InsertStatement};
use crate::storage::{Table, Tuple, Value};
use crate::wal::{WalManager, WalOp};

pub struct Database {
    pub tables: HashMap<String, Table>,
    pub wal: WalManager,
    pub checkpoint_path: PathBuf,
    pub last_lsn: u64,

    // --- 工业级事务扩展 ---
    /// 活跃事务缓冲区: txn_id -> 该事务下未提交的 WalOp 列表
    pub active_transactions: HashMap<u64, Vec<WalOp>>,
    /// 下一个可用的事务 ID
    pub next_txn_id: u64,
    /// 标记当前会话是否处于显式事务中 (BEGIN...COMMIT)
    pub in_transaction: bool,
    /// 当前会话关联的事务 ID
    pub current_txn_id: Option<u64>,
}

impl Database {
    pub fn new(wal_path: PathBuf) -> Self {
        if let Some(parent) = wal_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).expect("Failed to create data directory");
            }
        }
        let mut checkpoint_path = wal_path.clone();
        checkpoint_path.set_extension("checkpoint.json");

        let mut db = Self {
            tables: HashMap::new(),
            wal: WalManager::new(wal_path),
            checkpoint_path,
            last_lsn: 0,
            active_transactions: HashMap::new(),
            next_txn_id: 1, // 事务 ID 从 1 开始
            in_transaction: false,
            current_txn_id: None,
        };

        // 1. 加载元数据/表结构
        db.load_all_tables();

        // 2. 执行两阶段恢复算法
        if let Err(e) = db.recover_from_wal() {
            eprintln!("WAL Recovery Error: {}", e);
        }

        db
    }

    fn load_all_tables(&mut self) {
        let data_dir = Path::new("./data");
        if !data_dir.exists() { return; }

        if let Ok(entries) = fs::read_dir(data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                // 寻找以 .schema.json 结尾的文件
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
                    if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                        if file_name.ends_with(".schema.json") {
                            // 提取表名，例如 "students.schema.json" -> "students"
                            let table_name = file_name.replace(".schema.json", "");
                            
                            // 调用你已有的 Table::load_from_disk
                            match Table::load_from_disk(&table_name) {
                                Ok(table) => {
                                    self.tables.insert(table_name.clone(), table);
                                    println!("- Discovered and loaded table: {}", table_name);
                                }
                                Err(e) => eprintln!("- Failed to load table {}: {}", table_name, e),
                            }
                        }
                    }
                }
            }
        }
    }

    /// 工业级恢复算法：两阶段扫描 (Analysis & Redo)
    pub fn recover_from_wal(&mut self) -> Result<(), String> {
        let cp = Checkpoint::load(&self.checkpoint_path);
        println!("Checkpoint status: Last Applied LSN = {}", cp.last_applied_lsn);
        self.last_lsn = cp.last_applied_lsn;

        let all_records = self.wal.recover_with_lsn()?;
        
        // --- 阶段 1: 分析阶段 (Analysis Pass) ---
        // 找出所有在崩溃前已经成功 Commit 的事务 ID
        let mut committed_txns = HashSet::new();
        for (_, op) in &all_records {
            if let WalOp::Commit { txn_id } = op {
                committed_txns.insert(*txn_id);
            }
        }

        // --- 阶段 2: 重放阶段 (Redo Pass) ---
        let mut recovered_count = 0;
        for (lsn, op) in all_records {
            // 只重放 Checkpoint 之后且已经提交的事务记录
            if lsn > cp.last_applied_lsn {
                let txn_id = match op {
                    WalOp::Insert { txn_id, .. } => Some(txn_id),
                    WalOp::Delete { txn_id, .. } => Some(txn_id),
                    _ => None,
                };

                if let Some(tid) = txn_id {
                    if committed_txns.contains(&tid) {
                        if self.apply_op_to_memory(&op) {
                            self.last_lsn = lsn;
                            recovered_count += 1;
                        }
                    }
                }
            }
        }

        if recovered_count > 0 {
            println!("Successfully REDO {} operations from WAL. Current LSN: {}", recovered_count, self.last_lsn);
        }
        Ok(())
    }

    /// 将具体的 WalOp 应用到内存中的 Table
    fn apply_op_to_memory(&mut self, op: &WalOp) -> bool {
        match op {
            WalOp::Insert { table: table_name, row, .. } => {
                if let Some(table) = self.tables.get_mut(table_name) {
                    table.data.push(Tuple(row.clone()));
                    return true;
                }
            }
            WalOp::Delete { table: table_name, row_id, .. } => {
                if let Some(table) = self.tables.get_mut(table_name) {
                    if *row_id < table.data.len() {
                        table.data.remove(*row_id);
                        return true;
                    }
                }
            }
            _ => {} // Begin/Commit/Abort 不需要直接修改 Table 内存
        }
        false
    }

    /// 开启事务逻辑
    pub fn begin_transaction(&mut self) -> Result<u64, String> {
        if self.in_transaction {
            return Err("Already in a transaction".into());
        }
        let tid = self.next_txn_id;
        self.next_txn_id += 1;
        
        self.wal.append(WalOp::Begin { txn_id: tid })?;
        self.in_transaction = true;
        self.current_txn_id = Some(tid);
        self.active_transactions.insert(tid, Vec::new());
        
        Ok(tid)
    }

    /// 提交事务逻辑
    pub fn commit_transaction(&mut self) -> Result<(), String> {
        let tid = self.current_txn_id.ok_or("No active transaction to commit")?;
        
        // 1. 写入 Commit 日志并强制落盘 (Industrial Flush)
        self.last_lsn = self.wal.append(WalOp::Commit { txn_id: tid })?;
        self.wal.flush()?;

        // 2. 将缓冲区内的所有操作真正应用到内存表
        if let Some(ops) = self.active_transactions.remove(&tid) {
            for op in ops {
                self.apply_op_to_memory(&op);
            }
        }

        self.in_transaction = false;
        self.current_txn_id = None;
        println!("Transaction {} committed.", tid);
        Ok(())
    }

    pub fn apply_create_table(&mut self, stmt: CreateTableStatement) -> Result<(), String> {
        if self.tables.contains_key(&stmt.table_name) {
            return Err(format!("Table '{}' already exists", stmt.table_name));
        }

        let table_name = stmt.table_name.clone();
        let new_table = Table::new(table_name.clone(), stmt.columns);

        // 1. 持久化 Schema 到磁盘 (假设数据目录在 ./data)
        // let schema_path = Path::new("./data");
        new_table.save_schema().map_err(|e| e.to_string())?;

        // 2. 更新内存映射
        self.tables.insert(table_name, new_table);

        println!("Table '{}' created successfully.", stmt.table_name);
        Ok(())
    }

    /// 数据插入接口：支持隐式/显式事务
    pub fn apply_insert(&mut self, stmt: InsertStatement) -> Result<usize, String> {
        let table_name = stmt.table_name;
        let row_values = stmt.values;

        if !self.tables.contains_key(&table_name) {
            return Err(format!("Table '{}' not found", table_name));
        }

        if self.in_transaction {
            // --- 场景 A: 显式事务 ---
            let tid = self.current_txn_id.unwrap();
            let op = WalOp::Insert {
                txn_id: tid,
                table: table_name,
                row: row_values,
            };
            // 写入 WAL，但仅存入内存 Buffer，暂不更新 self.tables
            self.last_lsn = self.wal.append(op.clone())?;
            self.active_transactions.get_mut(&tid).unwrap().push(op);
        } else {
            // --- 场景 B: 隐式事务 (Autocommit) ---
            let tid = self.next_txn_id;
            self.next_txn_id += 1;

            self.wal.append(WalOp::Begin { txn_id: tid })?;
            let op = WalOp::Insert {
                txn_id: tid,
                table: table_name,
                row: row_values,
            };
            self.wal.append(op.clone())?;
            self.last_lsn = self.wal.append(WalOp::Commit { txn_id: tid })?;
            self.wal.flush()?; // 确保安全落盘

            // 立即更新内存
            self.apply_op_to_memory(&op);
        }

        Ok(1)
    }

    pub fn create_checkpoint(&mut self) -> Result<(), String> {
        println!("Creating checkpoint at LSN {}...", self.last_lsn);
        for table in self.tables.values() {
            table.save_to_disk().map_err(|e| e.to_string())?;
        }
        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        let cp = Checkpoint { last_applied_lsn: self.last_lsn, timestamp: now_secs as i64 };
        cp.save(&self.checkpoint_path);
        self.wal.truncate()?;
        println!("Checkpoint completed.");
        Ok(())
    }

    pub fn shutdown(&mut self) {
        if self.in_transaction {
            println!("Aborting active transaction due to shutdown...");
            // 工业实践中关机通常会尝试回滚或强制写 Abort
        }
        let _ = self.create_checkpoint();
    }
}