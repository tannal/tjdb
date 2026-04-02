use crate::lexer::{Lexer, Token};


mod lexer;

fn main() {
    let sql = "SELECT id, name FROM users WHERE id = 100;";
    let mut lexer = Lexer::new(sql);

    loop {
        let token = lexer.next_token();
        println!("{:?}", token);
        if token == Token::EOF {
            break;
        }
    }
}