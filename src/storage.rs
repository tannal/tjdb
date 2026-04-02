use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::Path,
};

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Value {
    Int(i32),
    Text(String),
    Bool(bool),
    Null,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum DataType {
    Int,
    Text,
    Bool,
}

impl Value {
    pub fn to_csv_string(&self) -> String {
        match self {
            Value::Int(n) => n.to_string(),
            Value::Text(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
            Value::Null => "NULL".to_string(),
        }
    }

    pub fn from_csv_string(s: &str, data_type: &DataType) -> Result<Self, String> {
        match data_type {
            DataType::Int => s
                .parse::<i32>()
                .map(Value::Int)
                .map_err(|_| format!("Invalid int: {}", s)),
            DataType::Text => Ok(Value::Text(s.to_string())),
            DataType::Bool => s
                .parse::<bool>()
                .map(Value::Bool)
                .map_err(|_| format!("Invalid bool: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tuple(pub Vec<Value>);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub is_nullable: bool,
}

pub struct Table {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    pub data: Vec<Tuple>,
}

impl Table {
    pub fn new(name: String, columns: Vec<ColumnDefinition>) -> Self {
        Self {
            name,
            columns,
            data: Vec::new(),
        }
    }

    /// 仅保存表结构 (Schema) 到磁盘，用于 CREATE TABLE 语句
    pub fn save_schema(&self) -> std::io::Result<()> {
        let data_dir = Path::new("./data");

        // 确保数据目录存在
        if !data_dir.exists() {
            fs::create_dir_all(data_dir)?;
        }

        // 保存 Schema 为 JSON: ./data/{table_name}.schema.json
        let schema_path = data_dir.join(format!("{}.schema.json", self.name));

        // 使用 serde_json 将列定义序列化
        let schema_json = serde_json::to_string_pretty(&self.columns)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        fs::write(schema_path, schema_json)?;

        println!("Schema for table '{}' saved to disk.", self.name);
        Ok(())
    }

    // 你原有的 save_to_disk 可以调用 save_schema 来减少重复代码
    pub fn save_to_disk(&self) -> std::io::Result<()> {
        // 1. 调用 save_schema 保存结构
        self.save_schema()?;

        // 2. 保存数据为 CSV (原有逻辑)
        let data_dir = Path::new("./data");
        let csv_path = data_dir.join(format!("{}.csv", self.name));
        let mut file = File::create(csv_path)?;

        let header: Vec<String> = self.columns.iter().map(|c| c.name.clone()).collect();
        writeln!(file, "{}", header.join(","))?;

        for tuple in &self.data {
            let row: Vec<String> = tuple.0.iter().map(|v| v.to_csv_string()).collect();
            writeln!(file, "{}", row.join(","))?;
        }

        Ok(())
    }

    /// 核心修改：从 ./data 目录恢复表结构和数据
    pub fn load_from_disk(name: &str) -> Result<Self, String> {
        let data_dir = Path::new("./data");
        let schema_path = data_dir.join(format!("{}.schema.json", name));
        let csv_path = data_dir.join(format!("{}.csv", name));

        // 1. 加载 Schema
        if !schema_path.exists() {
            return Err(format!("Schema file for table '{}' not found", name));
        }
        let schema_content = fs::read_to_string(schema_path).map_err(|e| e.to_string())?;
        let columns: Vec<ColumnDefinition> =
            serde_json::from_str(&schema_content).map_err(|e| e.to_string())?;

        // 2. 加载 CSV 数据
        let mut data = Vec::new();
        if csv_path.exists() {
            let file = File::open(csv_path).map_err(|e| e.to_string())?;
            let reader = BufReader::new(file);
            let mut lines = reader.lines();

            lines.next(); // 跳过表头

            for line in lines {
                let line = line.map_err(|e| e.to_string())?;
                let values: Vec<&str> = line.split(',').collect();
                if values.len() != columns.len() {
                    continue;
                }

                let mut row = Vec::new();
                for (idx, val_str) in values.iter().enumerate() {
                    row.push(Value::from_csv_string(val_str, &columns[idx].data_type)?);
                }
                data.push(Tuple(row));
            }
        }

        Ok(Table {
            name: name.to_string(),
            columns,
            data,
        })
    }
}
