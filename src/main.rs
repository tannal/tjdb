use crate::lexer::{Lexer, Token};


mod lexer;
mod parser;

use parser::Parser;

fn main() {
    let sql = "SELECT id, name FROM users";
    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer);

    match parser.parse_statement() {
        Ok(ast) => println!("AST: {:#?}", ast),
        Err(e) => println!("Error: {}", e),
    }
}