use crate::storage::Value;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Cursor, Read, Write};
use std::path::PathBuf;

// 操作码定义
const OP_INSERT: u8 = 1;
const OP_DELETE: u8 = 2;
const OP_BEGIN: u8 = 3;
const OP_COMMIT: u8 = 4;
const OP_ABORT: u8 = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOp {
    /// 事务开始
    Begin { txn_id: u64 },
    
    /// 数据操作
    Insert { 
        txn_id: u64, 
        table: String, 
        row: Vec<Value> 
    },
    
    /// 删除操作
    Delete { 
        txn_id: u64, 
        table: String, 
        row_id: usize 
    },
    
    /// 事务提交
    Commit { txn_id: u64 },
    
    /// 事务回滚
    Abort { txn_id: u64 },
}

pub struct WalManager {
    pub path: PathBuf,
    writer: BufWriter<File>,
    current_lsn: u64,
}

impl WalManager {
    pub fn new(path: PathBuf) -> Self {
        let mut last_lsn = 0;
        if path.exists() {
            if let Ok(ops_with_lsn) = Self::internal_recover(&path) {
                if let Some((lsn, _)) = ops_with_lsn.last() {
                    last_lsn = *lsn;
                }
            }
        }

        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
            .expect("Failed to open WAL file");

        Self {
            path,
            writer: BufWriter::with_capacity(128 * 1024, file),
            current_lsn: last_lsn,
        }
    }

    pub fn append(&mut self, op: WalOp) -> Result<u64, String> {
        self.current_lsn += 1;
        let lsn = self.current_lsn;

        let payload = self.serialize_op(&op);

        let mut hasher = Hasher::new();
        hasher.update(&lsn.to_le_bytes());
        hasher.update(&payload);
        let checksum = hasher.finalize();

        let total_len = (8 + 4 + payload.len()) as u32;

        self.writer
            .write_all(&total_len.to_le_bytes())
            .map_err(|e| e.to_string())?;
        self.writer
            .write_all(&lsn.to_le_bytes())
            .map_err(|e| e.to_string())?;
        self.writer
            .write_all(&checksum.to_le_bytes())
            .map_err(|e| e.to_string())?;
        self.writer.write_all(&payload).map_err(|e| e.to_string())?;

        // 工业级事务通常在 append 时根据 op 类型决定是否 flush
        // 这里为了简化，我们保留每次 append 都 flush
        self.flush()?;
        Ok(lsn)
    }

    pub fn flush(&mut self) -> Result<(), String> {
        self.writer.flush().map_err(|e| e.to_string())?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn truncate(&mut self) -> Result<(), String> {
        self.writer.flush().map_err(|e| e.to_string())?;
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| e.to_string())?;

        self.writer = BufWriter::new(file);
        Ok(())
    }

    pub fn recover_with_lsn(&self) -> Result<Vec<(u64, WalOp)>, String> {
        Self::internal_recover(&self.path)
    }

    fn internal_recover(path: &PathBuf) -> Result<Vec<(u64, WalOp)>, String> {
        let mut file = File::open(path).map_err(|e| e.to_string())?;
        let mut all_data = Vec::new();
        if file.read_to_end(&mut all_data).is_err() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        let mut cursor = Cursor::new(all_data.as_slice());

        while cursor.position() < cursor.get_ref().len() as u64 {
            let mut len_buf = [0u8; 4];
            if cursor.read_exact(&mut len_buf).is_err() { break; }
            let total_len = u32::from_le_bytes(len_buf) as u64;

            let mut lsn_buf = [0u8; 8];
            cursor.read_exact(&mut lsn_buf).map_err(|e| e.to_string())?;
            let lsn = u64::from_le_bytes(lsn_buf);

            let mut ck_buf = [0u8; 4];
            cursor.read_exact(&mut ck_buf).map_err(|e| e.to_string())?;
            let stored_checksum = u32::from_le_bytes(ck_buf);

            let payload_len = total_len - 8 - 4;
            let mut payload = vec![0u8; payload_len as usize];
            cursor.read_exact(&mut payload).map_err(|e| e.to_string())?;

            let mut hasher = Hasher::new();
            hasher.update(&lsn.to_le_bytes());
            hasher.update(&payload);
            if hasher.finalize() != stored_checksum {
                return Err(format!("WAL Corruption at LSN {}", lsn));
            }

            let op = Self::static_deserialize_op(&payload)?;
            results.push((lsn, op));
        }
        Ok(results)
    }

    // --- 手动序列化逻辑 ---

    fn serialize_op(&self, op: &WalOp) -> Vec<u8> {
        let mut buf = Vec::new();
        match op {
            WalOp::Begin { txn_id } => {
                buf.push(OP_BEGIN);
                buf.write_all(&txn_id.to_le_bytes()).unwrap();
            }
            WalOp::Insert { txn_id, table, row } => {
                buf.push(OP_INSERT);
                buf.write_all(&txn_id.to_le_bytes()).unwrap();
                Self::static_write_string(&mut buf, table);
                buf.write_all(&(row.len() as u32).to_le_bytes()).unwrap();
                for val in row {
                    Self::static_write_value(&mut buf, val);
                }
            }
            WalOp::Delete { txn_id, table, row_id } => {
                buf.push(OP_DELETE);
                buf.write_all(&txn_id.to_le_bytes()).unwrap();
                Self::static_write_string(&mut buf, table);
                buf.write_all(&(*row_id as u64).to_le_bytes()).unwrap();
            }
            WalOp::Commit { txn_id } => {
                buf.push(OP_COMMIT);
                buf.write_all(&txn_id.to_le_bytes()).unwrap();
            }
            WalOp::Abort { txn_id } => {
                buf.push(OP_ABORT);
                buf.write_all(&txn_id.to_le_bytes()).unwrap();
            }
        }
        buf
    }

    fn static_deserialize_op(data: &[u8]) -> Result<WalOp, String> {
        let mut cursor = Cursor::new(data);
        let mut op_type = [0u8; 1];
        cursor.read_exact(&mut op_type).map_err(|e| e.to_string())?;

        match op_type[0] {
            OP_BEGIN => {
                let mut id_buf = [0u8; 8];
                cursor.read_exact(&mut id_buf).map_err(|e| e.to_string())?;
                Ok(WalOp::Begin { txn_id: u64::from_le_bytes(id_buf) })
            }
            OP_INSERT => {
                let mut id_buf = [0u8; 8];
                cursor.read_exact(&mut id_buf).map_err(|e| e.to_string())?;
                let txn_id = u64::from_le_bytes(id_buf);
                
                let table = Self::static_read_string(&mut cursor)?;
                let mut count_buf = [0u8; 4];
                cursor.read_exact(&mut count_buf).map_err(|e| e.to_string())?;
                let count = u32::from_le_bytes(count_buf);
                
                let mut row = Vec::new();
                for _ in 0..count {
                    row.push(Self::static_read_value(&mut cursor)?);
                }
                Ok(WalOp::Insert { txn_id, table, row })
            }
            OP_DELETE => {
                let mut id_buf = [0u8; 8];
                cursor.read_exact(&mut id_buf).map_err(|e| e.to_string())?;
                let txn_id = u64::from_le_bytes(id_buf);

                let table = Self::static_read_string(&mut cursor)?;
                let mut row_id_buf = [0u8; 8];
                cursor.read_exact(&mut row_id_buf).map_err(|e| e.to_string())?;
                let row_id = u64::from_le_bytes(row_id_buf) as usize;
                Ok(WalOp::Delete { txn_id, table, row_id })
            }
            OP_COMMIT => {
                let mut id_buf = [0u8; 8];
                cursor.read_exact(&mut id_buf).map_err(|e| e.to_string())?;
                Ok(WalOp::Commit { txn_id: u64::from_le_bytes(id_buf) })
            }
            OP_ABORT => {
                let mut id_buf = [0u8; 8];
                cursor.read_exact(&mut id_buf).map_err(|e| e.to_string())?;
                Ok(WalOp::Abort { txn_id: u64::from_le_bytes(id_buf) })
            }
            _ => Err(format!("Unknown WalOp Type Tag: {}", op_type[0])),
        }
    }

    // 辅助工具函数保持不变
    fn static_read_string(cursor: &mut Cursor<&[u8]>) -> Result<String, String> {
        let mut len_buf = [0u8; 4];
        cursor.read_exact(&mut len_buf).map_err(|e| e.to_string())?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut s_buf = vec![0u8; len];
        cursor.read_exact(&mut s_buf).map_err(|e| e.to_string())?;
        String::from_utf8(s_buf).map_err(|e| e.to_string())
    }

    fn static_read_value(cursor: &mut Cursor<&[u8]>) -> Result<Value, String> {
        let mut type_buf = [0u8; 1];
        cursor.read_exact(&mut type_buf).map_err(|e| e.to_string())?;
        match type_buf[0] {
            0 => {
                let mut n_buf = [0u8; 8];
                cursor.read_exact(&mut n_buf).map_err(|e| e.to_string())?;
                Ok(Value::Int(i64::from_le_bytes(n_buf) as i32))
            }
            1 => Ok(Value::Text(Self::static_read_string(cursor)?)),
            2 => Ok(Value::Null),
            3 => {
                let mut b_buf = [0u8; 1];
                cursor.read_exact(&mut b_buf).map_err(|e| e.to_string())?;
                Ok(Value::Bool(b_buf[0] != 0))
            }
            _ => Err("Invalid Value Type Tag".into()),
        }
    }

    fn static_write_string(buf: &mut Vec<u8>, s: &str) {
        buf.write_all(&(s.len() as u32).to_le_bytes()).unwrap();
        buf.write_all(s.as_bytes()).unwrap();
    }

    fn static_write_value(buf: &mut Vec<u8>, val: &Value) {
        match val {
            Value::Int(n) => {
                buf.push(0);
                buf.write_all(&(*n as i64).to_le_bytes()).unwrap();
            }
            Value::Text(s) => {
                buf.push(1);
                Self::static_write_string(buf, s);
            }
            Value::Null => buf.push(2),
            Value::Bool(b) => {
                buf.push(3);
                buf.push(if *b { 1 } else { 0 });
            }
        }
    }

    pub fn dump_log(&self) -> Result<(), String> {
        let data = Self::internal_recover(&self.path)?;
        println!("\n--- WAL DEBUG DUMP ---");
        for (lsn, op) in data {
            println!("LSN: {} | Op: {:?}", lsn, op);
        }
        println!("--- END OF WAL ---\n");
        Ok(())
    }
}