pub struct Lexer {
    input: Vec<char>,
    pos: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    // 关键字
    Select,
    From,
    Insert,
    Into,
    Values,
    Create,
    Table,

    Where,

    // 标点与符号
    Asterisk,   // *
    Comma,      // ,
    Semicolon,  // ;
    LeftParen,  // (
    RightParen, // )
    Equal,      // =
    GreaterThan,
    GreaterThanEqual,
    LessThan,
    LessThanEqual,

    // 字面量与标识符
    Identifier(String),
    Number(i32),
    StringLiteral(String),

    EOF,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            pos: 0,
        }
    }

    // 辅助函数：跳过空格
    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() && self.input[self.pos].is_whitespace() {
            self.pos += 1;
        }
    }

    // 核心函数：获取下一个 Token
    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();

        if self.pos >= self.input.len() {
            return Token::EOF;
        }

        let ch = self.input[self.pos];

        let token = match ch {
            '*' => Token::Asterisk,
            ',' => Token::Comma,
            ';' => Token::Semicolon,
            '(' => Token::LeftParen,
            ')' => Token::RightParen,
            '=' => Token::Equal,
            '>' => {
                self.pos += 1;
                if self.input[self.pos] == '=' {
                    self.pos += 1;
                    Token::GreaterThanEqual
                } else {
                    Token::GreaterThan
                }
            }
            '<' => {
                self.pos += 1;
                if self.input[self.pos] == '=' {
                    self.pos += 1;
                    Token::LessThanEqual
                } else {
                    Token::LessThan
                }
            }
            'a'..='z' | 'A'..='Z' => return self.read_identifier(),
            '0'..='9' => return self.read_number(),
            _ => panic!("Unexpected character: {}", ch),
        };

        self.pos += 1;
        token
    }
    // 处理标识符（字段名、表名或关键字）
    fn read_identifier(&mut self) -> Token {
        let start = self.pos;

        // 修改点：增加对 '_' 的支持
        while self.pos < self.input.len()
            && (self.input[self.pos].is_alphanumeric() || self.input[self.pos] == '_')
        {
            self.pos += 1;
        }

        let text: String = self.input[start..self.pos].iter().collect();

        // 匹配关键字
        match text.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "INSERT" => Token::Insert,
            "CREATE" => Token::Create,
            "TABLE" => Token::Table,
            "WHERE" => Token::Where,
            _ => Token::Identifier(text),
        }
    }

    fn read_number(&mut self) -> Token {
        let start = self.pos;
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        
        let text: String = self.input[start..self.pos].iter().collect();
        
        // 将字符串转换为 i32。如果解析失败（如数字太大），这里简单处理为 0
        let value = text.parse::<i32>().unwrap_or(0);
        
        Token::Number(value)
    }
}
