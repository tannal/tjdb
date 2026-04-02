// src/parser.rs
#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement), // 别忘了在 Statement 枚举中增加这一项
}

#[derive(Debug)]
pub enum Expression {
    Column(String), // 基础列名：age
    Literal(Value), // 常量值：21
    BinaryOp {
        left: Box<Expression>, // 左边可以是 (age + 1)
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
            Token::Insert => Ok(Statement::Insert(self.parse_insert()?)),
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

    fn get_precedence(&self, op: &str) -> i32 {
        match op {
            "*" | "/" => 30,
            "+" | "-" => 20,
            ">" | "<" | ">=" | "<=" | "=" | "!=" => 10,
            _ => 0,
        }
    }

    fn is_binary_operator(token: &Token) -> bool {
        matches!(
            token,
            Token::Equal | Token::NotEqual | 
            Token::LessThan | Token::LessThanEqual | 
            Token::GreaterThan | Token::GreaterThanEqual |
            Token::Plus | Token::Minus | Token::Asterisk | Token::Divide // 顺便把除法加上
        )
    }

    fn get_operator_string(&mut self) -> Result<String, String> {
        let op = match &self.curr_token {
            Token::Equal => "=",
            Token::GreaterThan => ">",
            Token::LessThan => "<",
            Token::GreaterThanEqual => ">=",
            Token::LessThanEqual => "<=",
            Token::Plus => "+",
            Token::Minus => "-",   // ✅ 必须添加
            Token::Asterisk => "*",
            _ => return Err(format!("Unknown operator: {:?}", self.curr_token)),
        };
        Ok(op.to_string())
    }

    fn parse_insert(&mut self) -> Result<InsertStatement, String> {
        self.advance(); // 跳过 INSERT
        if self.curr_token != Token::Into { return Err("Expected INTO".into()); }
        self.advance();
    
        let table_name = if let Token::Identifier(name) = &self.curr_token {
            name.clone()
        } else {
            return Err("Expected table name".into());
        };
        self.advance();
    
        if self.curr_token != Token::Values { return Err("Expected VALUES".into()); }
        self.advance();
    
        if self.curr_token != Token::LeftParen { return Err("Expected '('".into()); }
        self.advance();
    
        let mut values = Vec::new();
        loop {
            match &self.curr_token {
                Token::Number(n) => values.push(Value::Int(*n)),
                Token::StringLiteral(s) => values.push(Value::Text(s.clone())),
                _ => return Err("Expected literal value".into()),
            }
            self.advance();
            if self.curr_token == Token::Comma { self.advance(); } 
            else { break; }
        }
    
        if self.curr_token != Token::RightParen { return Err("Expected ')'".into()); }
        self.advance();
    
        Ok(InsertStatement { table_name, values })
    }
    
    pub fn parse_expression(&mut self) -> Result<Expression, String> {
        self.parse_sub_expression(0) // 从优先级 0 开始解析
    }

    fn parse_sub_expression(&mut self, min_precedence: i32) -> Result<Expression, String> {
        let mut left = self.parse_primary()?;
    
        loop {
            // 1. 如果当前不是运算符，退出
            if !Self::is_binary_operator(&self.curr_token) {
                break;
            }
    
            let op = self.get_operator_string()?;
            let precedence = self.get_precedence(&op);
    
            // 2. 如果下一个运算符的优先级不够高，说明当前的 left 已经是一个完整的整体了
            if precedence < min_precedence {
                break;
            }
    
            // 3. 消费运算符
            self.advance();
    
            // 4. 递归解析右侧，传入当前优先级作为门槛
            // 对于左结合（如 + - * /），传入 precedence + 1
            let right = self.parse_sub_expression(precedence + 1)?;
    
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
    
        Ok(left)
    }

    // 解析基础单元：数字、标识符（列名）
    fn parse_primary(&mut self) -> Result<Expression, String> {
        match self.curr_token.clone() {
            Token::LeftParen => {
                self.advance(); // 跳过 (
                let expr = self.parse_expression()?; // 递归解析括号内的完整表达式
                if self.curr_token != Token::RightParen {
                    return Err(format!("Expected ')', found {:?}", self.curr_token));
                }
                self.advance(); // 跳过 )
                Ok(expr)
            }
            Token::StringLiteral(s) => {
                self.advance();
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
            _ => Err(format!(
                "Unexpected token in primary: {:?}",
                self.curr_token
            )),
        }
    }
}
