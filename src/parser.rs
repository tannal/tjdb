// src/parser.rs
#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
}

#[derive(Debug)]
pub enum Expression {
    BinaryOp {
        left: String,  // 列名，如 "id"
        op: String,    // 运算符，如 "="
        right: String, // 值，如 "1"
    },
}

#[derive(Debug)]
pub struct SelectStatement {
    pub columns: Vec<String>,
    pub table: String,
    pub where_clause: Option<Expression>, // 变为可选
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
        let table = if let Token::Identifier(table_name) = &self.curr_token {
            let t = table_name.clone();
            self.advance();
            t
        } else {
            return Err("Expected table name".to_string());
        };
        
        // 4. 解析 WHERE (核心新增)
        let mut where_clause = None;
        if self.curr_token == Token::Where {
            self.advance(); // 跳过 WHERE
            where_clause = Some(self.parse_expression()?);
        }

        Ok(SelectStatement {
            columns,
            table,
            where_clause,
        })

    }

    // 极简版表达式解析：只能处理 <Ident> = <Number/String>
    fn parse_expression(&mut self) -> Result<Expression, String> {
        // 解析左侧：列名
        let left = if let Token::Identifier(name) = &self.curr_token {
            let n = name.clone();
            self.advance();
            n
        } else {
            return Err("Expected column name in WHERE".to_string());
        };

        // 解析运算符：目前只支持 =
        let op = if self.curr_token == Token::Equal {
            self.advance();
            "=".to_string()
        } else {
            return Err("Expected '=' in WHERE".to_string());
        };

        // 解析右侧：值 (可以是数字或标识符/字符串)
        let right = match &self.curr_token {
            Token::Number(val) | Token::Identifier(val) => {
                let v = val.clone();
                self.advance();
                v
            },
            _ => return Err("Expected value in WHERE".to_string()),
        };

        Ok(Expression::BinaryOp { left, op, right })
    }
}
