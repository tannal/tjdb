# tjdb - A Minimalist Relational Database Engine in Rust

A educational-purpose relational database management system (RDBMS) built from scratch in Rust. It features a SQL parser, a transactional Write-Ahead Log (WAL), a volcano-style execution engine, and persistent storage using JSON for metadata and CSV for data.

## 🚀 Features

- **SQL Support**: Implements DDL (`CREATE TABLE`) and DML (`SELECT`, `INSERT`, `UPDATE`, `DELETE`).
- **Transactional Integrity**: Supports `BEGIN`, `COMMIT`, and `ROLLBACK` with a dedicated WAL (Write-Ahead Logging) for crash recovery.
- **Volcano Execution Model**: Query execution via a tree of physical operators (Scan, Filter, Project, Aggregate).
- **Persistent Metadata**: Automatic schema discovery from disk using `.schema.json` files.
- **Graceful Shutdown**: Checkpointing system to flush memory state to CSV and truncate logs.

## 🛠️ Architecture

The engine is divided into several modular components:

- **Lexer & Parser**: Converts raw SQL strings into an Abstract Syntax Tree (AST).
- **Database Engine**: Manages the life-cycle of transactions and the mapping of in-memory tables.
- **Executor**: Binds logical AST nodes to physical operators and handles type checking.
- **Storage Layer**: Handles Row-based storage and CSV/JSON serialization.
- **WAL**: Ensures Atomicity and Durability by logging operations before applying them to memory.

## 📥 Installation

Ensure you have [Rust](https://www.rust-lang.org/tools/install) installed.

```bash
git clone [https://github.com/tannal/tjdb.git](https://github.com/tannal/tjdb.git)
cd tjdb
cargo build
```

## WAL

[4字节长度] + [8字节LSN] + [4字节Checksum] + [1字节Op类型] + [Payload]

https://gemini.google.com/app/b98c1c6e60ccdfaf