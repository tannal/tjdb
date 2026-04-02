// src/parser.rs
#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement), // 别忘了在 Statement 枚举中增加这一项
}

#[derive(Debug)]
pub enum Expression {
    Column(String),        // 基础列名：age
    Literal(Value),       // 常量值：21
    BinaryOp {
        left: Box<Expression>,  // 左边可以是 (age + 1)
        op: String,
        right: Box<Expression>, // 右边也可以是 (10 * 2)
    },
}

use crate::storage::Value;

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table_name: String,
    pub values: Vec<Value>, // 此时已经是转换后的 Value 枚举
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

    fn get_operator_string(&mut self) -> Result<String, String> {
        let op = match &self.curr_token {
            Token::Equal => "=".to_string(),
            Token::GreaterThan => ">".to_string(),
            Token::LessThan => "<".to_string(),
            Token::GreaterThanEqual => ">=".to_string(),
            Token::LessThanEqual => "<=".to_string(),
            // Token::NotEqual => "!=".to_string(),
            // 如果你以后想支持算术运算，可以在这里继续添加
            // Token::Plus => "+".to_string(),
            // Token::Minus => "-".to_string(),
            _ => return Err(format!("Expected operator, found {:?}", self.curr_token)),
        };
        Ok(op)
    }

    fn parse_expression(&mut self) -> Result<Expression, String> {
        let left = self.parse_primary()?;

        // ✅ 修改点：增加 Token::LessThanEqual 和 Token::GreaterThanEqual
        if matches!(
            self.curr_token,
            Token::Equal | 
            Token::GreaterThan | 
            Token::LessThan | 
            Token::GreaterThanEqual | 
            Token::LessThanEqual
        ) {
            let op = self.get_operator_string()?;
            self.advance(); // 移动到运算符之后的 Token (即右操作数)
            let right = self.parse_expression()?;
            Ok(Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    // 解析基础单元：数字、标识符（列名）
    fn parse_primary(&mut self) -> Result<Expression, String> {
        match self.curr_token.clone() {
            Token::StringLiteral(s) => {
                self.advance();
                // 关键点：字符串字面量在 AST 中被视为 Literal(Value::Text)
                Ok(Expression::Literal(Value::Text(s)))
            }
            Token::Number(n) => {
                self.advance();
                Ok(Expression::Literal(Value::Int(n)))
            }
            Token::Identifier(s) => {
                self.advance();
                Ok(Expression::Column(s))
            }
            _ => Err(format!("Unexpected token in expression: {:?}", self.curr_token)),
        }
    }
}
