// src/parser.rs
#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
}

#[derive(Debug)]
pub struct SelectStatement {
    pub columns: Vec<String>, // ['id', 'name']
    pub table: String,        // 'users'
}

use crate::lexer::{Lexer, Token};

pub struct Parser {
    lexer: Lexer,
    curr_token: Token,
}

impl Parser {
    pub fn new(mut lexer: Lexer) -> Self {
        let curr_token = lexer.next_token();
        Self { lexer, curr_token }
    }

    // 辅助函数：移动到下一个 Token
    fn advance(&mut self) {
        self.curr_token = self.lexer.next_token();
    }

    // 核心解析入口
    pub fn parse_statement(&mut self) -> Result<Statement, String> {
        match self.curr_token {
            Token::Select => Ok(Statement::Select(self.parse_select()?)),
            _ => Err(format!("Unsupported statement: {:?}", self.curr_token)),
        }
    }

    // 解析 SELECT 语句: SELECT col1, col2 FROM table
    fn parse_select(&mut self) -> Result<SelectStatement, String> {
        self.advance(); // 跳过 SELECT

        let mut columns = Vec::new();
        
        // 1. 解析字段列表
        loop {
            if let Token::Identifier(name) = &self.curr_token {
                columns.push(name.clone());
                self.advance();
            } else {
                return Err("Expected column name".to_string());
            }

            if self.curr_token == Token::Comma {
                self.advance();
            } else {
                break; // 没有逗号了，字段列表结束
            }
        }

        // 2. 寻找 FROM
        if self.curr_token != Token::From {
            return Err(format!("Expected FROM, found {:?}", self.curr_token));
        }
        self.advance();

        // 3. 解析表名
        if let Token::Identifier(table_name) = &self.curr_token {
            let table = table_name.clone();
            self.advance();
            Ok(SelectStatement { columns, table })
        } else {
            Err("Expected table name".to_string())
        }
    }
}