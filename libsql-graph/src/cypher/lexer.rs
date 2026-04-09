#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Match,
    Where,
    Return,
    Create,
    Delete,
    Detach,
    Set,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    And,
    Or,
    Not,
    True,
    False,
    Null,
    As,
    In,
    Distinct,
    Contains,
    StartsWith,
    EndsWith,
    Merge,
    OnCreate,
    OnMatch,
    With,
    Optional,
    Unwind,
    Case,
    When,
    Then,
    Else,
    End,
    Is,
    Skip,
    RegexOp,

    Ident(String),
    Integer(i64),
    Float(f64),
    StringLit(String),
    Parameter(String),

    LParen,
    RParen,
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Colon,
    Comma,
    Dot,
    Arrow,       // ->
    DashLBracket, // -[
    RBracketDash, // ]-
    LtDash,      // <-
    DashGt,      // ->  (same as Arrow but used in rel context)
    Dash,        // -
    Eq,
    Neq,         // <>
    Lt,
    Gt,
    Lte,         // <=
    Gte,         // >=
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    DotDot,      // ..

    Eof,
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

    pub fn tokenize(&mut self) -> Result<Vec<Token>, String> {
        let mut tokens = Vec::new();
        loop {
            let tok = self.next_token()?;
            if tok == Token::Eof {
                tokens.push(tok);
                break;
            }
            tokens.push(tok);
        }
        Ok(tokens)
    }

    fn peek(&self) -> Option<char> {
        self.input.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let ch = self.input.get(self.pos).copied();
        self.pos += 1;
        ch
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_whitespace() {
                self.advance();
            } else if ch == '/' && self.input.get(self.pos + 1) == Some(&'/') {
                while let Some(c) = self.peek() {
                    self.advance();
                    if c == '\n' {
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }

    fn next_token(&mut self) -> Result<Token, String> {
        self.skip_whitespace();

        let ch = match self.peek() {
            None => return Ok(Token::Eof),
            Some(c) => c,
        };

        match ch {
            '(' => { self.advance(); Ok(Token::LParen) }
            ')' => { self.advance(); Ok(Token::RParen) }
            '{' => { self.advance(); Ok(Token::LBrace) }
            '}' => { self.advance(); Ok(Token::RBrace) }
            ':' => { self.advance(); Ok(Token::Colon) }
            ',' => { self.advance(); Ok(Token::Comma) }
            '+' => { self.advance(); Ok(Token::Plus) }
            '*' => { self.advance(); Ok(Token::Star) }
            '%' => { self.advance(); Ok(Token::Percent) }
            '/' => { self.advance(); Ok(Token::Slash) }
            '[' => { self.advance(); Ok(Token::LBracket) }
            '.' => {
                self.advance();
                if self.peek() == Some('.') {
                    self.advance();
                    Ok(Token::DotDot)
                } else {
                    Ok(Token::Dot)
                }
            }
            '-' => {
                self.advance();
                if self.peek() == Some('[') {
                    self.advance();
                    Ok(Token::DashLBracket)
                } else if self.peek() == Some('>') {
                    self.advance();
                    Ok(Token::Arrow)
                } else {
                    Ok(Token::Dash)
                }
            }
            ']' => {
                self.advance();
                if self.peek() == Some('-') {
                    self.advance();
                    Ok(Token::RBracketDash)
                } else {
                    Ok(Token::RBracket)
                }
            }
            '<' => {
                self.advance();
                if self.peek() == Some('-') {
                    self.advance();
                    Ok(Token::LtDash)
                } else if self.peek() == Some('=') {
                    self.advance();
                    Ok(Token::Lte)
                } else if self.peek() == Some('>') {
                    self.advance();
                    Ok(Token::Neq)
                } else {
                    Ok(Token::Lt)
                }
            }
            '>' => {
                self.advance();
                if self.peek() == Some('=') {
                    self.advance();
                    Ok(Token::Gte)
                } else {
                    Ok(Token::Gt)
                }
            }
            '=' => {
                self.advance();
                if self.peek() == Some('~') {
                    self.advance();
                    Ok(Token::RegexOp)
                } else {
                    Ok(Token::Eq)
                }
            }
            '$' => {
                self.advance();
                let name = self.read_ident();
                Ok(Token::Parameter(name))
            }
            '\'' | '"' => self.read_string(),
            _ if ch.is_ascii_digit() => self.read_number(),
            _ if ch.is_ascii_alphabetic() || ch == '_' => {
                let ident = self.read_ident();
                Ok(keyword_or_ident(ident))
            }
            _ => {
                self.advance();
                Err(format!("unexpected character: '{}'", ch))
            }
        }
    }

    fn read_ident(&mut self) -> String {
        let mut s = String::new();
        while let Some(ch) = self.peek() {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                s.push(ch);
                self.advance();
            } else {
                break;
            }
        }
        s
    }

    fn read_number(&mut self) -> Result<Token, String> {
        let mut s = String::new();
        let mut is_float = false;

        while let Some(ch) = self.peek() {
            if ch.is_ascii_digit() {
                s.push(ch);
                self.advance();
            } else if ch == '.' && !is_float {
                if self.input.get(self.pos + 1).map_or(false, |c| c.is_ascii_digit()) {
                    is_float = true;
                    s.push(ch);
                    self.advance();
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        if is_float {
            s.parse::<f64>()
                .map(Token::Float)
                .map_err(|e| format!("invalid float: {e}"))
        } else {
            s.parse::<i64>()
                .map(Token::Integer)
                .map_err(|e| format!("invalid integer: {e}"))
        }
    }

    fn read_string(&mut self) -> Result<Token, String> {
        let quote = self.advance().unwrap();
        let mut s = String::new();

        loop {
            match self.advance() {
                None => return Err("unterminated string".into()),
                Some(ch) if ch == quote => break,
                Some('\\') => match self.advance() {
                    Some('n') => s.push('\n'),
                    Some('t') => s.push('\t'),
                    Some('\\') => s.push('\\'),
                    Some(c) if c == quote => s.push(c),
                    Some(c) => { s.push('\\'); s.push(c); }
                    None => return Err("unterminated escape".into()),
                },
                Some(ch) => s.push(ch),
            }
        }

        Ok(Token::StringLit(s))
    }
}

fn keyword_or_ident(s: String) -> Token {
    match s.to_uppercase().as_str() {
        "MATCH" => Token::Match,
        "WHERE" => Token::Where,
        "RETURN" => Token::Return,
        "CREATE" => Token::Create,
        "DELETE" => Token::Delete,
        "DETACH" => Token::Detach,
        "SET" => Token::Set,
        "ORDER" => Token::Order,
        "BY" => Token::By,
        "ASC" => Token::Asc,
        "DESC" => Token::Desc,
        "LIMIT" => Token::Limit,
        "AND" => Token::And,
        "OR" => Token::Or,
        "NOT" => Token::Not,
        "TRUE" => Token::True,
        "FALSE" => Token::False,
        "NULL" => Token::Null,
        "AS" => Token::As,
        "IN" => Token::In,
        "DISTINCT" => Token::Distinct,
        "CONTAINS" => Token::Contains,
        "STARTS" => Token::StartsWith,
        "ENDS" => Token::EndsWith,
        "MERGE" => Token::Merge,
        "ON" => Token::OnCreate,
        "WITH" => Token::With,
        "UNWIND" => Token::Unwind,
        "OPTIONAL" => Token::Optional,
        "IS" => Token::Is,
        "SKIP" => Token::Skip,
        "CASE" => Token::Case,
        "WHEN" => Token::When,
        "THEN" => Token::Then,
        "ELSE" => Token::Else,
        "END" => Token::End,
        _ => Token::Ident(s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(input: &str) -> Vec<Token> {
        Lexer::new(input).tokenize().unwrap()
    }

    #[test]
    fn test_simple_match() {
        let tokens = lex("MATCH (a:Person) RETURN a");
        assert_eq!(tokens, vec![
            Token::Match,
            Token::LParen,
            Token::Ident("a".into()),
            Token::Colon,
            Token::Ident("Person".into()),
            Token::RParen,
            Token::Return,
            Token::Ident("a".into()),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_relationship_pattern() {
        let tokens = lex("(a)-[:KNOWS]->(b)");
        assert_eq!(tokens, vec![
            Token::LParen,
            Token::Ident("a".into()),
            Token::RParen,
            Token::DashLBracket,
            Token::Colon,
            Token::Ident("KNOWS".into()),
            Token::RBracketDash,
            Token::Gt,
            Token::LParen,
            Token::Ident("b".into()),
            Token::RParen,
            Token::Eof,
        ]);
    }

    #[test]
    fn test_incoming_relationship() {
        let tokens = lex("(a)<-[:KNOWS]-(b)");
        assert_eq!(tokens, vec![
            Token::LParen,
            Token::Ident("a".into()),
            Token::RParen,
            Token::LtDash,
            Token::LBracket,  // standalone [
            Token::Colon,
            Token::Ident("KNOWS".into()),
            Token::RBracketDash,
            Token::LParen,
            Token::Ident("b".into()),
            Token::RParen,
            Token::Eof,
        ]);
    }

    #[test]
    fn test_properties() {
        let tokens = lex("{name: 'Alice', age: 28}");
        assert_eq!(tokens, vec![
            Token::LBrace,
            Token::Ident("name".into()),
            Token::Colon,
            Token::StringLit("Alice".into()),
            Token::Comma,
            Token::Ident("age".into()),
            Token::Colon,
            Token::Integer(28),
            Token::RBrace,
            Token::Eof,
        ]);
    }

    #[test]
    fn test_where_clause() {
        let tokens = lex("WHERE a.age > 25 AND a.name = 'Alice'");
        assert_eq!(tokens, vec![
            Token::Where,
            Token::Ident("a".into()),
            Token::Dot,
            Token::Ident("age".into()),
            Token::Gt,
            Token::Integer(25),
            Token::And,
            Token::Ident("a".into()),
            Token::Dot,
            Token::Ident("name".into()),
            Token::Eq,
            Token::StringLit("Alice".into()),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_variable_length_path() {
        let tokens = lex("-[:KNOWS*1..3]->");
        assert_eq!(tokens, vec![
            Token::DashLBracket,
            Token::Colon,
            Token::Ident("KNOWS".into()),
            Token::Star,
            Token::Integer(1),
            Token::DotDot,
            Token::Integer(3),
            Token::RBracketDash,
            Token::Gt,
            Token::Eof,
        ]);
    }

    #[test]
    fn test_float_literal() {
        let tokens = lex("3.14");
        assert_eq!(tokens, vec![Token::Float(3.14), Token::Eof]);
    }

    #[test]
    fn test_parameter() {
        let tokens = lex("$name");
        assert_eq!(tokens, vec![Token::Parameter("name".into()), Token::Eof]);
    }

    #[test]
    fn test_comparison_operators() {
        let tokens = lex("<= >= <>");
        assert_eq!(tokens, vec![
            Token::Lte,
            Token::Gte,
            Token::Neq,
            Token::Eof,
        ]);
    }

    #[test]
    fn test_case_insensitive_keywords() {
        let tokens = lex("match WHERE Return");
        assert_eq!(tokens, vec![
            Token::Match,
            Token::Where,
            Token::Return,
            Token::Eof,
        ]);
    }
}
