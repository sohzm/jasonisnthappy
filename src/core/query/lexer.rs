
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenType {
    Eof,
    Illegal,

    Ident,
    String,
    Number,
    True,
    False,
    Null,

    Gt,
    Gte,
    Lt,
    Lte,
    Is,
    IsNot,

    And,
    Or,
    Not,

    Exists,
    Has,
    Any,
    All,

    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Dot,
}

impl fmt::Display for TokenType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TokenType::Eof => write!(f, "EOF"),
            TokenType::Illegal => write!(f, "ILLEGAL"),
            TokenType::Ident => write!(f, "IDENT"),
            TokenType::String => write!(f, "STRING"),
            TokenType::Number => write!(f, "NUMBER"),
            TokenType::True => write!(f, "TRUE"),
            TokenType::False => write!(f, "FALSE"),
            TokenType::Null => write!(f, "NULL"),
            TokenType::Gt => write!(f, ">"),
            TokenType::Gte => write!(f, ">="),
            TokenType::Lt => write!(f, "<"),
            TokenType::Lte => write!(f, "<="),
            TokenType::Is => write!(f, "IS"),
            TokenType::IsNot => write!(f, "IS NOT"),
            TokenType::And => write!(f, "AND"),
            TokenType::Or => write!(f, "OR"),
            TokenType::Not => write!(f, "NOT"),
            TokenType::Exists => write!(f, "EXISTS"),
            TokenType::Has => write!(f, "HAS"),
            TokenType::Any => write!(f, "ANY"),
            TokenType::All => write!(f, "ALL"),
            TokenType::LParen => write!(f, "("),
            TokenType::RParen => write!(f, ")"),
            TokenType::LBracket => write!(f, "["),
            TokenType::RBracket => write!(f, "]"),
            TokenType::Comma => write!(f, ","),
            TokenType::Dot => write!(f, "."),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub token_type: TokenType,
    pub value: String,
    pub pos: usize,
}

impl Token {
    fn new(token_type: TokenType, value: String, pos: usize) -> Self {
        Self {
            token_type,
            value,
            pos,
        }
    }
}

pub struct Lexer {
    input: Vec<char>,
    pos: usize,
    ch: char,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        let chars: Vec<char> = input.chars().collect();
        let mut lexer = Self {
            input: chars,
            pos: 0,
            ch: '\0',
        };
        lexer.read_char();
        lexer
    }

    fn read_char(&mut self) {
        if self.pos >= self.input.len() {
            self.ch = '\0';
        } else {
            self.ch = self.input[self.pos];
        }
        self.pos += 1;
    }

    fn peek_char(&self) -> char {
        if self.pos >= self.input.len() {
            '\0'
        } else {
            self.input[self.pos]
        }
    }

    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();

        let token_pos = self.pos.saturating_sub(1);

        let token = match self.ch {
            '\0' => Token::new(TokenType::Eof, String::new(), token_pos),
            '(' => {
                let tok = Token::new(TokenType::LParen, self.ch.to_string(), token_pos);
                self.read_char();
                tok
            }
            ')' => {
                let tok = Token::new(TokenType::RParen, self.ch.to_string(), token_pos);
                self.read_char();
                tok
            }
            '[' => {
                let tok = Token::new(TokenType::LBracket, self.ch.to_string(), token_pos);
                self.read_char();
                tok
            }
            ']' => {
                let tok = Token::new(TokenType::RBracket, self.ch.to_string(), token_pos);
                self.read_char();
                tok
            }
            ',' => {
                let tok = Token::new(TokenType::Comma, self.ch.to_string(), token_pos);
                self.read_char();
                tok
            }
            '.' => {
                let tok = Token::new(TokenType::Dot, self.ch.to_string(), token_pos);
                self.read_char();
                tok
            }
            '>' => {
                if self.peek_char() == '=' {
                    self.read_char();
                    let tok = Token::new(TokenType::Gte, ">=".to_string(), token_pos);
                    self.read_char();
                    tok
                } else {
                    let tok = Token::new(TokenType::Gt, ">".to_string(), token_pos);
                    self.read_char();
                    tok
                }
            }
            '<' => {
                if self.peek_char() == '=' {
                    self.read_char();
                    let tok = Token::new(TokenType::Lte, "<=".to_string(), token_pos);
                    self.read_char();
                    tok
                } else {
                    let tok = Token::new(TokenType::Lt, "<".to_string(), token_pos);
                    self.read_char();
                    tok
                }
            }
            '"' | '\'' => {
                let value = self.read_string();
                Token::new(TokenType::String, value, token_pos)
            }
            _ => {
                if is_letter(self.ch) {
                    let value = self.read_identifier();
                    let token_type = lookup_keyword(&value);
                    return Token::new(token_type, value, token_pos);
                } else if is_digit(self.ch) {
                    let value = self.read_number();
                    return Token::new(TokenType::Number, value, token_pos);
                } else {
                    let tok = Token::new(TokenType::Illegal, self.ch.to_string(), token_pos);
                    self.read_char();
                    tok
                }
            }
        };

        token
    }

    fn skip_whitespace(&mut self) {
        while self.ch.is_whitespace() {
            self.read_char();
        }
    }

    fn read_identifier(&mut self) -> String {
        let start = self.pos - 1;
        while is_letter(self.ch) || is_digit(self.ch) || self.ch == '_' {
            self.read_char();
        }
        self.input[start..self.pos - 1].iter().collect()
    }

    fn read_number(&mut self) -> String {
        let start = self.pos - 1;
        while is_digit(self.ch) {
            self.read_char();
        }

        if self.ch == '.' && is_digit(self.peek_char()) {
            self.read_char();
            while is_digit(self.ch) {
                self.read_char();
            }
        }

        self.input[start..self.pos - 1].iter().collect()
    }

    fn read_string(&mut self) -> String {
        let quote = self.ch;
        self.read_char();
        let start = self.pos - 1;

        while self.ch != quote && self.ch != '\0' {
            self.read_char();
        }

        let value: String = self.input[start..self.pos - 1].iter().collect();
        self.read_char();
        value
    }
}

fn lookup_keyword(ident: &str) -> TokenType {
    match ident.to_lowercase().as_str() {
        "and" => TokenType::And,
        "or" => TokenType::Or,
        "not" => TokenType::Not,
        "is" => TokenType::Is,
        "exists" => TokenType::Exists,
        "has" => TokenType::Has,
        "any" => TokenType::Any,
        "all" => TokenType::All,
        "true" => TokenType::True,
        "false" => TokenType::False,
        "null" => TokenType::Null,
        _ => TokenType::Ident,
    }
}

fn is_letter(ch: char) -> bool {
    ch.is_alphabetic()
}

fn is_digit(ch: char) -> bool {
    ch.is_numeric()
}

pub fn tokenize(query: &str) -> Result<Vec<Token>, String> {
    let mut lexer = Lexer::new(query);
    let mut tokens = Vec::new();

    loop {
        let token = lexer.next_token();
        if token.token_type == TokenType::Illegal {
            return Err(format!(
                "illegal token at position {}: {}",
                token.pos, token.value
            ));
        }
        let is_eof = token.token_type == TokenType::Eof;
        tokens.push(token);
        if is_eof {
            break;
        }
    }

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_tokens() {
        let input = "age > 30";
        let tokens = tokenize(input).unwrap();

        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0].token_type, TokenType::Ident);
        assert_eq!(tokens[0].value, "age");
        assert_eq!(tokens[1].token_type, TokenType::Gt);
        assert_eq!(tokens[2].token_type, TokenType::Number);
        assert_eq!(tokens[2].value, "30");
        assert_eq!(tokens[3].token_type, TokenType::Eof);
    }

    #[test]
    fn test_comparison_operators() {
        let input = "> >= < <= is";
        let tokens = tokenize(input).unwrap();

        assert_eq!(tokens[0].token_type, TokenType::Gt);
        assert_eq!(tokens[1].token_type, TokenType::Gte);
        assert_eq!(tokens[2].token_type, TokenType::Lt);
        assert_eq!(tokens[3].token_type, TokenType::Lte);
        assert_eq!(tokens[4].token_type, TokenType::Is);
    }

    #[test]
    fn test_logical_operators() {
        let input = "and or not";
        let tokens = tokenize(input).unwrap();

        assert_eq!(tokens[0].token_type, TokenType::And);
        assert_eq!(tokens[1].token_type, TokenType::Or);
        assert_eq!(tokens[2].token_type, TokenType::Not);
    }

    #[test]
    fn test_string_literals() {
        let input = r#"name is "Alice""#;
        let tokens = tokenize(input).unwrap();

        assert_eq!(tokens[0].token_type, TokenType::Ident);
        assert_eq!(tokens[0].value, "name");
        assert_eq!(tokens[1].token_type, TokenType::Is);
        assert_eq!(tokens[2].token_type, TokenType::String);
        assert_eq!(tokens[2].value, "Alice");
    }

    #[test]
    fn test_boolean_and_null() {
        let input = "true false null";
        let tokens = tokenize(input).unwrap();

        assert_eq!(tokens[0].token_type, TokenType::True);
        assert_eq!(tokens[1].token_type, TokenType::False);
        assert_eq!(tokens[2].token_type, TokenType::Null);
    }

    #[test]
    fn test_delimiters() {
        let input = "()[],.";
        let tokens = tokenize(input).unwrap();

        assert_eq!(tokens[0].token_type, TokenType::LParen);
        assert_eq!(tokens[1].token_type, TokenType::RParen);
        assert_eq!(tokens[2].token_type, TokenType::LBracket);
        assert_eq!(tokens[3].token_type, TokenType::RBracket);
        assert_eq!(tokens[4].token_type, TokenType::Comma);
        assert_eq!(tokens[5].token_type, TokenType::Dot);
    }

    #[test]
    fn test_complex_query() {
        let input = r#"age > 30 and name is "Bob" or active is true"#;
        let tokens = tokenize(input).unwrap();

        assert_eq!(tokens.len(), 12);
        assert_eq!(tokens[0].value, "age");
        assert_eq!(tokens[1].token_type, TokenType::Gt);
        assert_eq!(tokens[2].value, "30");
        assert_eq!(tokens[3].token_type, TokenType::And);
        assert_eq!(tokens[4].value, "name");
        assert_eq!(tokens[5].token_type, TokenType::Is);
        assert_eq!(tokens[6].value, "Bob");
        assert_eq!(tokens[7].token_type, TokenType::Or);
        assert_eq!(tokens[8].value, "active");
        assert_eq!(tokens[9].token_type, TokenType::Is);
        assert_eq!(tokens[10].token_type, TokenType::True);
        assert_eq!(tokens[11].token_type, TokenType::Eof);
    }

    #[test]
    fn test_decimal_numbers() {
        let input = "price > 19.99";
        let tokens = tokenize(input).unwrap();

        assert_eq!(tokens[2].token_type, TokenType::Number);
        assert_eq!(tokens[2].value, "19.99");
    }

    #[test]
    fn test_dot_notation() {
        let input = "user.address.city";
        let tokens = tokenize(input).unwrap();

        assert_eq!(tokens[0].token_type, TokenType::Ident);
        assert_eq!(tokens[0].value, "user");
        assert_eq!(tokens[1].token_type, TokenType::Dot);
        assert_eq!(tokens[2].token_type, TokenType::Ident);
        assert_eq!(tokens[2].value, "address");
        assert_eq!(tokens[3].token_type, TokenType::Dot);
        assert_eq!(tokens[4].token_type, TokenType::Ident);
        assert_eq!(tokens[4].value, "city");
    }
}
