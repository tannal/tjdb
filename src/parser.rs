use crate::lexer::{Lexer, Token};
use crate::storage::{ColumnDefinition, DataType, Value};

// --- AST 定义 ---

#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    // --- 事务控制语句 ---
    Begin,
    Commit,
    Rollback,
    CreateTable(CreateTableStatement),
}

#[derive(Debug)]
pub enum Expression {
    Column(String),
    Literal(Value),
    BinaryOp {
        left: Box<Expression>,
        op: String,
        right: Box<Expression>,
    },
}

#[derive(Debug)]
pub struct UpdateStatement {
    pub table_name: String,
    pub assignments: Vec<(String, Expression)>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug)]
pub struct DeleteStatement {
    pub table_name: String,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table_name: String,
    pub values: Vec<Value>,
}

#[derive(Debug)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    Column(String),           // 普通列: name
    Wildcard,                 // 全选: *
    Aggregate(AggregateFunc), // 聚合函数: COUNT(*), SUM(age)
}

#[derive(Debug, Clone)]
pub enum AggregateFunc {
    CountWildcard, // COUNT(*)
    Sum(String),   // SUM(column_name)
    Min(String),   // MIN(column_name)
    Max(String),   // MAX(column_name)
}

#[derive(Debug)]
pub struct SelectStatement {
    pub select_items: Vec<SelectItem>,
    pub table_name: String,
    pub where_clause: Option<Expression>,
}

// --- Parser 实现 ---

pub struct Parser {
    lexer: Lexer,
    curr_token: Token,
}

impl Parser {
    pub fn new(mut lexer: Lexer) -> Self {
        let curr_token = lexer.next_token();
        Self { lexer, curr_token }
    }

    fn advance(&mut self) {
        self.curr_token = self.lexer.next_token();
    }

    fn consume(&mut self, expected: Token) -> Result<(), String> {
        if self.curr_token == expected {
            self.advance();
            Ok(())
        } else {
            Err(format!(
                "Expected {:?}, found {:?}",
                expected, self.curr_token
            ))
        }
    }

    fn parse_identifier(&mut self) -> Result<String, String> {
        if let Token::Identifier(s) = &self.curr_token {
            let name = s.clone();
            self.advance();
            Ok(name)
        } else {
            Err(format!("Expected identifier, found {:?}", self.curr_token))
        }
    }

    fn parse_select_item(&mut self) -> Result<SelectItem, String> {
        match &self.curr_token {
            Token::Asterisk => {
                self.advance();
                Ok(SelectItem::Wildcard)
            }
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();

                if self.curr_token == Token::LeftParen {
                    self.advance();
                    
                    let item = match name.to_uppercase().as_str() {
                        "COUNT" => {
                            self.consume(Token::Asterisk)?;
                            SelectItem::Aggregate(AggregateFunc::CountWildcard)
                        }
                        "SUM" => {
                            let col = self.parse_identifier()?;
                            SelectItem::Aggregate(AggregateFunc::Sum(col))
                        }
                        "MIN" => {
                            let col = self.parse_identifier()?;
                            SelectItem::Aggregate(AggregateFunc::Min(col))
                        }
                        "MAX" => {
                            let col = self.parse_identifier()?;
                            SelectItem::Aggregate(AggregateFunc::Max(col))
                        }
                        _ => return Err(format!("Unknown function: {}", name)),
                    };
                    
                    self.consume(Token::RightParen)?;
                    Ok(item)
                } else {
                    Ok(SelectItem::Column(name))
                }
            }
            _ => Err("Expected column name or function".into()),
        }
    }

    fn parse_create_table(&mut self) -> Result<CreateTableStatement, String> {
        self.consume(Token::Create)?;
        self.consume(Token::Table)?;
        let table_name = self.parse_identifier()?;
        self.consume(Token::LeftParen)?;

        let mut columns = Vec::new();
        loop {
            let col_name = self.parse_identifier()?;
            let data_type = match self.parse_identifier()?.to_uppercase().as_str() {
                "INT" | "INTEGER" => DataType::Int,
                "TEXT" | "VARCHAR" => DataType::Text,
                _ => return Err("Unsupported data type".into()),
            };
            
            columns.push(ColumnDefinition {
                name: col_name,
                data_type,
                is_nullable: true, // 简化版默认允许 null
            });

            if self.curr_token == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        self.consume(Token::RightParen)?;
        Ok(CreateTableStatement { table_name, columns })
    }

    pub fn parse_statement(&mut self) -> Result<Statement, String> {
        match self.curr_token {
            Token::Select => Ok(Statement::Select(self.parse_select()?)),
            Token::Insert => Ok(Statement::Insert(self.parse_insert()?)),
            Token::Update => Ok(Statement::Update(self.parse_update()?)),
            Token::Delete => Ok(Statement::Delete(self.parse_delete()?)),

            // 建表语句
            Token::Create => Ok(Statement::CreateTable(self.parse_create_table()?)),

            // 处理事务关键字
            Token::Begin => {
                self.advance();
                // 允许 BEGIN TRANSACTION 或 简写 BEGIN
                if let Token::Identifier(s) = &self.curr_token {
                    if s.to_uppercase() == "TRANSACTION" {
                        self.advance();
                    }
                }
                Ok(Statement::Begin)
            }
            Token::Commit => {
                self.advance();
                Ok(Statement::Commit)
            }
            Token::Rollback => {
                self.advance();
                Ok(Statement::Rollback)
            }
            _ => Err(format!("Unsupported statement: {:?}", self.curr_token)),
        }
    }

    fn parse_delete(&mut self) -> Result<DeleteStatement, String> {
        self.consume(Token::Delete)?;
        self.consume(Token::From)?;
        let table_name = self.parse_identifier()?;

        let mut where_clause = None;
        if self.curr_token == Token::Where {
            self.advance();
            where_clause = Some(self.parse_expression()?);
        }

        Ok(DeleteStatement { table_name, where_clause })
    }

    fn parse_update(&mut self) -> Result<UpdateStatement, String> {
        self.consume(Token::Update)?;
        let table_name = self.parse_identifier()?;
        self.consume(Token::Set)?;

        let mut assignments = Vec::new();
        loop {
            let column_name = self.parse_identifier()?;
            self.consume(Token::Equal)?;
            let new_value = self.parse_expression()?;
            assignments.push((column_name, new_value));

            if self.curr_token == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        let mut where_clause = None;
        if self.curr_token == Token::Where {
            self.advance();
            where_clause = Some(self.parse_expression()?);
        }

        Ok(UpdateStatement { table_name, assignments, where_clause })
    }

    fn parse_select(&mut self) -> Result<SelectStatement, String> {
        self.consume(Token::Select)?;

        let mut columns: Vec<SelectItem> = Vec::new();
        loop {
            columns.push(self.parse_select_item()?);

            if self.curr_token == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        self.consume(Token::From)?;
        let table = self.parse_identifier()?;

        let mut where_clause = None;
        if self.curr_token == Token::Where {
            self.advance();
            where_clause = Some(self.parse_expression()?);
        }

        Ok(SelectStatement {
            select_items: columns,
            table_name: table,
            where_clause,
        })
    }

    fn parse_insert(&mut self) -> Result<InsertStatement, String> {
        self.consume(Token::Insert)?;
        self.consume(Token::Into)?;
        let table_name = self.parse_identifier()?;
        self.consume(Token::Values)?;
        self.consume(Token::LeftParen)?;

        let mut values = Vec::new();
        loop {
            match &self.curr_token {
                Token::Number(n) => values.push(Value::Int(*n)),
                Token::StringLiteral(s) => values.push(Value::Text(s.clone())),
                _ => return Err(format!("Expected literal, found {:?}", self.curr_token)),
            }
            self.advance();
            if self.curr_token == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        self.consume(Token::RightParen)?;
        Ok(InsertStatement { table_name, values })
    }

    pub fn parse_expression(&mut self) -> Result<Expression, String> {
        self.parse_sub_expression(0)
    }

    fn parse_sub_expression(&mut self, min_precedence: i32) -> Result<Expression, String> {
        let mut left = self.parse_primary()?;

        loop {
            if !Self::is_binary_operator(&self.curr_token) {
                break;
            }

            let op = self.get_operator_string()?;
            let precedence = self.get_precedence(&op);

            if precedence < min_precedence {
                break;
            }

            self.advance();
            let right = self.parse_sub_expression(precedence + 1)?;

            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_primary(&mut self) -> Result<Expression, String> {
        match self.curr_token.clone() {
            Token::LeftParen => {
                self.advance();
                let expr = self.parse_expression()?;
                self.consume(Token::RightParen)?;
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
            _ => Err(format!("Unexpected token in primary: {:?}", self.curr_token)),
        }
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
            Token::Equal | Token::NotEqual | Token::LessThan | Token::LessThanEqual |
            Token::GreaterThan | Token::GreaterThanEqual | Token::Plus | Token::Minus |
            Token::Asterisk | Token::Divide
        )
    }

    fn get_operator_string(&self) -> Result<String, String> {
        let op = match &self.curr_token {
            Token::Equal => "=",
            Token::NotEqual => "!=",
            Token::GreaterThan => ">",
            Token::LessThan => "<",
            Token::GreaterThanEqual => ">=",
            Token::LessThanEqual => "<=",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Asterisk => "*",
            Token::Divide => "/",
            _ => return Err(format!("Unknown operator: {:?}", self.curr_token)),
        };
        Ok(op.to_string())
    }
}