use crate::lexer::{Lexer, Token};
use crate::storage::Value;

// --- AST 定义 ---

#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
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
    // 改为 Vec，存储多个 (列名, 新值表达式)
    pub assignments: Vec<(String, Expression)>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table_name: String,
    pub values: Vec<Value>,
}

#[derive(Debug)]
pub struct SelectStatement {
    pub columns: Vec<String>,
    pub table: String,
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

    // 辅助函数：消费当前 Token 并获取下一个
    fn advance(&mut self) {
        self.curr_token = self.lexer.next_token();
    }

    // 辅助函数：校验并消费
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

    // 解析标识符（表名、列名）
    fn parse_identifier(&mut self) -> Result<String, String> {
        if let Token::Identifier(s) = &self.curr_token {
            let name = s.clone();
            self.advance();
            Ok(name)
        } else {
            Err(format!("Expected identifier, found {:?}", self.curr_token))
        }
    }

    // 核心解析入口
    pub fn parse_statement(&mut self) -> Result<Statement, String> {
        match self.curr_token {
            Token::Select => Ok(Statement::Select(self.parse_select()?)),
            Token::Insert => Ok(Statement::Insert(self.parse_insert()?)),
            Token::Update => Ok(Statement::Update(self.parse_update()?)),
            _ => Err(format!("Unsupported statement: {:?}", self.curr_token)),
        }
    }

    /// 解析 UPDATE users SET age = age + 1 WHERE id = 1
    fn parse_update(&mut self) -> Result<UpdateStatement, String> {
        self.consume(Token::Update)?;

        // 解析表名
        let table_name = self.parse_identifier()?;

        self.consume(Token::Set)?;

        // 2. 解析赋值列表 (column = expr, column2 = expr2...)
        let mut assignments = Vec::new();
        loop {
            let column_name = self.parse_identifier()?;
            self.consume(Token::Equal)?;
            let new_value = self.parse_expression()?;

            assignments.push((column_name, new_value));

            // 如果看到逗号，继续解析下一对；否则跳出循环
            if self.curr_token == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        // 解析可选的 WHERE
        let mut where_clause = None;
        if self.curr_token == Token::Where {
            self.advance(); // 消耗 WHERE
            where_clause = Some(self.parse_expression()?);
        }

        Ok(UpdateStatement {
            table_name,
            assignments,
            where_clause,
        })
    }

    // 解析 SELECT 语句: SELECT col1, col2 FROM table
    fn parse_select(&mut self) -> Result<SelectStatement, String> {
        self.consume(Token::Select)?;

        let mut columns = Vec::new();

        // 检查是否是 SELECT *
        if self.curr_token == Token::Asterisk {
            self.advance(); // 消耗 *
        // columns 保持为空列表表示全选
        } else {
            loop {
                columns.push(self.parse_identifier()?);
                if self.curr_token == Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        self.consume(Token::From)?;
        let table = self.parse_identifier()?;

        let mut where_clause = None;
        if self.curr_token == Token::Where {
            self.advance(); // 消耗 WHERE
            where_clause = Some(self.parse_expression()?);
        }

        Ok(SelectStatement {
            columns,
            table,
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
                _ => {
                    return Err(format!(
                        "Expected literal value, found {:?}",
                        self.curr_token
                    ));
                }
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

    // --- 表达式解析 (Pratt Parsing) ---

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

            self.advance(); // 消费运算符

            // 左结合使用 precedence + 1
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
            _ => Err(format!(
                "Unexpected token in primary: {:?}",
                self.curr_token
            )),
        }
    }

    // --- 运算符辅助函数 ---

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
            Token::Equal
                | Token::NotEqual
                | Token::LessThan
                | Token::LessThanEqual
                | Token::GreaterThan
                | Token::GreaterThanEqual
                | Token::Plus
                | Token::Minus
                | Token::Asterisk
                | Token::Divide
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
