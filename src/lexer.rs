#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    // 关键字
    Select, From, Insert, Into, Values, Create, Table, Where,
    
    // 标点与符号
    Asterisk,   // *
    Comma,      // ,
    Semicolon,  // ;
    LeftParen,  // (
    RightParen, // )
    
    // 运算符 (支持单/双字符)
    Equal,            // =
    NotEqual,         // !=
    GreaterThan,      // >
    GreaterThanEqual, // >=
    LessThan,         // <
    LessThanEqual,    // <=

    Plus,
    Divide,
    Minus,

    // 字面量与标识符
    Identifier(String),
    Number(i32),
    StringLiteral(String),

    EOF,
}

pub struct Lexer {
    input: Vec<char>,
    pos: usize,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            pos: 0,
        }
    }

    // --- 核心入口 ---
    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();

        if self.is_eof() {
            return Token::EOF;
        }

        let ch = self.peek().unwrap();

        match ch {
            // 1. 处理操作符（可能包含双字符 <=, >=, !=）
            '=' | '!' | '<' | '>' => self.read_operator(),

            // 2. 处理单字符标点
            '*' => { self.advance(); Token::Asterisk }
            ',' => { self.advance(); Token::Comma }
            ';' => { self.advance(); Token::Semicolon }
            '(' => { self.advance(); Token::LeftParen }
            ')' => { self.advance(); Token::RightParen }

            '+' => {self.advance(); Token::Plus}
            '-' => {self.advance(); Token::Minus}
            '/' => {self.advance(); Token::Divide}

            // 3. 处理字符串字面量
            '\'' => self.read_string_literal(),

            // 4. 处理数字
            '0'..='9' => self.read_number(),

            // 5. 处理标识符与关键字 (支持下划线)
            'a'..='z' | 'A'..='Z' | '_' => self.read_identifier(),

            _ => panic!("Unexpected character: {} at position {}", ch, self.pos),
        }
    }

    // --- 私有处理函数 ---

    /// 工业级操作符处理：最长匹配原则
    fn read_operator(&mut self) -> Token {
        let curr = self.advance().unwrap();
        let next = self.peek();

        match (curr, next) {
            ('<', Some('=')) => { self.advance(); Token::LessThanEqual }
            ('>', Some('=')) => { self.advance(); Token::GreaterThanEqual }
            ('!', Some('=')) => { self.advance(); Token::NotEqual }
            ('<', _) => Token::LessThan,
            ('>', _) => Token::GreaterThan,
            ('=', _) => Token::Equal,
            ('!', _) => panic!("Unexpected '!' without '=' at position {}", self.pos),
            _ => unreachable!(),
        }
    }

    fn read_identifier(&mut self) -> Token {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_alphanumeric() || c == '_' {
                self.advance();
            } else {
                break;
            }
        }
        let text: String = self.input[start..self.pos].iter().collect();
        
        match text.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "FROM"   => Token::From,
            "INSERT" => Token::Insert,
            "INTO"   => Token::Into,
            "VALUES" => Token::Values,
            "CREATE" => Token::Create,
            "TABLE"  => Token::Table,
            "WHERE"  => Token::Where,
            _        => Token::Identifier(text),
        }
    }

    fn read_number(&mut self) -> Token {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                self.advance();
            } else {
                break;
            }
        }
        let text: String = self.input[start..self.pos].iter().collect();
        Token::Number(text.parse::<i32>().unwrap_or(0))
    }

    fn read_string_literal(&mut self) -> Token {
        self.advance(); // 跳过开头的 '
        let mut s = String::new();
        while let Some(c) = self.peek() {
            if c == '\'' {
                self.advance(); // 跳过结尾的 '
                return Token::StringLiteral(s);
            }
            s.push(c);
            self.advance();
        }
        panic!("Unterminated string literal at position {}", self.pos);
    }

    // --- 指针辅助工具 ---

    fn peek(&self) -> Option<char> {
        self.input.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let res = self.peek();
        if res.is_some() {
            self.pos += 1;
        }
        res
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.input.len()
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }
}