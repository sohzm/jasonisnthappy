
use super::lexer::{Token, TokenType};
use serde_json::Value;

pub trait Node: std::fmt::Debug {
    fn eval(&self, doc: &serde_json::Map<String, Value>) -> bool;
}

#[derive(Debug)]
pub struct BinaryOp {
    pub op: String,
    pub left: Box<dyn Node>,
    pub right: Box<dyn Node>,
}

impl Node for BinaryOp {
    fn eval(&self, doc: &serde_json::Map<String, Value>) -> bool {
        match self.op.as_str() {
            "and" => self.left.eval(doc) && self.right.eval(doc),
            "or" => self.left.eval(doc) || self.right.eval(doc),
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct UnaryOp {
    pub op: String,
    pub child: Box<dyn Node>,
}

impl Node for UnaryOp {
    fn eval(&self, doc: &serde_json::Map<String, Value>) -> bool {
        if self.op == "not" {
            !self.child.eval(doc)
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct CompareOp {
    pub field: String,
    pub op: String,
    pub value: Value,
}

impl Node for CompareOp {
    fn eval(&self, doc: &serde_json::Map<String, Value>) -> bool {
        let field_value = get_field(doc, &self.field);

        match self.op.as_str() {
            ">" => compare_greater(&field_value, &self.value),
            ">=" => compare_greater(&field_value, &self.value) || compare_equal(&field_value, &self.value),
            "<" => compare_less(&field_value, &self.value),
            "<=" => compare_less(&field_value, &self.value) || compare_equal(&field_value, &self.value),
            "is" => compare_equal(&field_value, &self.value),
            "is_not" => !compare_equal(&field_value, &self.value),
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct ExistsOp {
    pub field: String,
    pub not: bool,
}

impl Node for ExistsOp {
    fn eval(&self, doc: &serde_json::Map<String, Value>) -> bool {
        let exists = !get_field(doc, &self.field).is_null();
        if self.not {
            !exists
        } else {
            exists
        }
    }
}

#[derive(Debug)]
pub struct HasOp {
    pub field: String,
    pub op: String,
    pub values: Vec<Value>,
}

impl Node for HasOp {
    fn eval(&self, doc: &serde_json::Map<String, Value>) -> bool {
        let field_value = get_field(doc, &self.field);

        if let Some(arr) = field_value.as_array() {
            match self.op.as_str() {
                "has" => {
                    if let Some(val) = self.values.first() {
                        arr.iter().any(|v| compare_equal(v, val))
                    } else {
                        false
                    }
                }
                "has_any" => {
                    self.values.iter().any(|val| {
                        arr.iter().any(|v| compare_equal(v, val))
                    })
                }
                "has_all" => {
                    self.values.iter().all(|val| {
                        arr.iter().any(|v| compare_equal(v, val))
                    })
                }
                _ => false,
            }
        } else {
            false
        }
    }
}

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    pub fn parse(&mut self) -> Result<Box<dyn Node>, String> {
        if self.tokens.is_empty() || (self.tokens.len() == 1 && self.tokens[0].token_type == TokenType::Eof) {
            return Err("empty query".to_string());
        }
        self.or_expr()
    }

    fn or_expr(&mut self) -> Result<Box<dyn Node>, String> {
        let mut left = self.and_expr()?;

        while self.match_token(&[TokenType::Or]) {
            let right = self.and_expr()?;
            left = Box::new(BinaryOp {
                op: "or".to_string(),
                left,
                right,
            });
        }

        Ok(left)
    }

    fn and_expr(&mut self) -> Result<Box<dyn Node>, String> {
        let mut left = self.comparison()?;

        while self.match_token(&[TokenType::And]) {
            let right = self.comparison()?;
            left = Box::new(BinaryOp {
                op: "and".to_string(),
                left,
                right,
            });
        }

        Ok(left)
    }

    fn comparison(&mut self) -> Result<Box<dyn Node>, String> {
        if self.match_token(&[TokenType::LParen]) {
            let node = self.or_expr()?;
            if !self.match_token(&[TokenType::RParen]) {
                return Err(format!("expected ')' at position {}", self.current().pos));
            }
            return Ok(node);
        }

        if self.match_token(&[TokenType::Not]) {
            let child = self.comparison()?;
            return Ok(Box::new(UnaryOp {
                op: "not".to_string(),
                child,
            }));
        }

        if !self.check(TokenType::Ident) {
            return Err(format!(
                "expected field name at position {}, got {:?}",
                self.current().pos,
                self.current().token_type
            ));
        }
        let mut field = self.advance().value.clone();

        while self.match_token(&[TokenType::Dot]) {
            if !self.check(TokenType::Ident) {
                return Err(format!("expected field name after '.' at position {}", self.current().pos));
            }
            field.push('.');
            field.push_str(&self.advance().value);
        }

        if self.match_token(&[TokenType::Exists]) {
            return Ok(Box::new(ExistsOp {
                field,
                not: false,
            }));
        }

        if self.match_token(&[TokenType::Not]) {
            if !self.match_token(&[TokenType::Exists]) {
                return Err(format!("expected 'exists' after 'not' at position {}", self.current().pos));
            }
            return Ok(Box::new(ExistsOp {
                field,
                not: true,
            }));
        }

        if self.match_token(&[TokenType::Has]) {
            return self.parse_has(field);
        }

        let op = if self.match_token(&[TokenType::Gt]) {
            ">".to_string()
        } else if self.match_token(&[TokenType::Gte]) {
            ">=".to_string()
        } else if self.match_token(&[TokenType::Lt]) {
            "<".to_string()
        } else if self.match_token(&[TokenType::Lte]) {
            "<=".to_string()
        } else if self.match_token(&[TokenType::Is]) {
            if self.match_token(&[TokenType::Not]) {
                "is_not".to_string()
            } else {
                "is".to_string()
            }
        } else {
            if self.is_at_end() || self.check(TokenType::And) || self.check(TokenType::Or) || self.check(TokenType::RParen) {
                return Ok(Box::new(CompareOp {
                    field,
                    op: "is".to_string(),
                    value: Value::Bool(true),
                }));
            }
            return Err(format!("expected comparison operator at position {}", self.current().pos));
        };

        let value = self.parse_value()?;

        Ok(Box::new(CompareOp { field, op, value }))
    }

    fn parse_has(&mut self, field: String) -> Result<Box<dyn Node>, String> {
        let has_op = if self.match_token(&[TokenType::Any]) {
            "has_any".to_string()
        } else if self.match_token(&[TokenType::All]) {
            "has_all".to_string()
        } else {
            "has".to_string()
        };

        if has_op == "has" {
            let value = self.parse_value()?;
            return Ok(Box::new(HasOp {
                field,
                op: has_op,
                values: vec![value],
            }));
        } else {
            if !self.match_token(&[TokenType::LBracket]) {
                return Err(format!("expected '[' after 'has any/all' at position {}", self.current().pos));
            }

            let mut values = Vec::new();
            while !self.check(TokenType::RBracket) {
                let value = self.parse_value()?;
                values.push(value);

                if !self.match_token(&[TokenType::Comma]) {
                    break;
                }
            }

            if !self.match_token(&[TokenType::RBracket]) {
                return Err(format!("expected ']' at position {}", self.current().pos));
            }

            return Ok(Box::new(HasOp {
                field,
                op: has_op,
                values,
            }));
        }
    }

    fn parse_value(&mut self) -> Result<Value, String> {
        if self.match_token(&[TokenType::Number]) {
            let num_str = &self.previous().value;
            if let Ok(val) = num_str.parse::<f64>() {
                let number = serde_json::Number::from_f64(val)
                    .ok_or_else(|| format!("invalid number (NaN or Infinity not supported): {}", num_str))?;
                return Ok(Value::Number(number));
            }
            return Err(format!("invalid number: {}", num_str));
        }

        if self.match_token(&[TokenType::String]) {
            return Ok(Value::String(self.previous().value.clone()));
        }

        if self.match_token(&[TokenType::Ident]) {
            return Ok(Value::String(self.previous().value.clone()));
        }

        if self.match_token(&[TokenType::True]) {
            return Ok(Value::Bool(true));
        }

        if self.match_token(&[TokenType::False]) {
            return Ok(Value::Bool(false));
        }

        if self.match_token(&[TokenType::Null]) {
            return Ok(Value::Null);
        }

        Err(format!("expected value at position {}", self.current().pos))
    }


    fn current(&self) -> &Token {
        if self.pos >= self.tokens.len() {
            &self.tokens[self.tokens.len() - 1]
        } else {
            &self.tokens[self.pos]
        }
    }

    fn previous(&self) -> &Token {
        &self.tokens[self.pos - 1]
    }

    fn check(&self, token_type: TokenType) -> bool {
        if self.is_at_end() {
            false
        } else {
            self.current().token_type == token_type
        }
    }

    fn match_token(&mut self, types: &[TokenType]) -> bool {
        for &token_type in types {
            if self.check(token_type) {
                self.advance();
                return true;
            }
        }
        false
    }

    fn advance(&mut self) -> &Token {
        if !self.is_at_end() {
            self.pos += 1;
        }
        self.previous()
    }

    fn is_at_end(&self) -> bool {
        self.current().token_type == TokenType::Eof
    }
}

fn get_field(doc: &serde_json::Map<String, Value>, field: &str) -> Value {
    let parts: Vec<&str> = field.split('.').collect();
    let mut current = Value::Object(doc.clone());

    for part in parts {
        if let Some(obj) = current.as_object() {
            current = obj.get(part).cloned().unwrap_or(Value::Null);
        } else {
            return Value::Null;
        }
    }

    current
}

fn compare_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(na), Value::Number(nb)) => {
            na.as_f64().unwrap_or(0.0) == nb.as_f64().unwrap_or(0.0)
        }
        _ => a == b,
    }
}

fn compare_greater(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(na), Value::Number(nb)) => {
            na.as_f64().unwrap_or(0.0) > nb.as_f64().unwrap_or(0.0)
        }
        (Value::String(sa), Value::String(sb)) => sa > sb,
        _ => false,
    }
}

fn compare_less(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(na), Value::Number(nb)) => {
            na.as_f64().unwrap_or(0.0) < nb.as_f64().unwrap_or(0.0)
        }
        (Value::String(sa), Value::String(sb)) => sa < sb,
        _ => false,
    }
}

pub fn parse_query(query: &str) -> Result<Box<dyn Node>, String> {
    let tokens = super::lexer::tokenize(query)?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_eval(query: &str, doc: Value, expected: bool) {
        let ast = parse_query(query).unwrap();
        let doc_map = doc.as_object().unwrap();
        assert_eq!(ast.eval(doc_map), expected, "Query: {}", query);
    }

    #[test]
    fn test_simple_comparison() {
        test_eval("age > 30", json!({"age": 35}), true);
        test_eval("age > 30", json!({"age": 25}), false);
        test_eval("age >= 30", json!({"age": 30}), true);
        test_eval("age < 30", json!({"age": 25}), true);
        test_eval("age <= 30", json!({"age": 30}), true);

        test_eval("count > 10.5", json!({"count": 15.7}), true);
        test_eval("count >= 10.5", json!({"count": 10.5}), true);
    }

    #[test]
    fn test_equality() {
        test_eval("name is \"Alice\"", json!({"name": "Alice"}), true);
        test_eval("name is \"Alice\"", json!({"name": "Bob"}), false);
        test_eval("active is true", json!({"active": true}), true);
        test_eval("active is false", json!({"active": false}), true);
    }

    #[test]
    fn test_logical_operators() {
        test_eval(
            "age > 30 and name is \"Alice\"",
            json!({"age": 35, "name": "Alice"}),
            true,
        );
        test_eval(
            "age > 30 and name is \"Bob\"",
            json!({"age": 35, "name": "Alice"}),
            false,
        );
        test_eval(
            "age > 30 or name is \"Alice\"",
            json!({"age": 25, "name": "Alice"}),
            true,
        );
    }

    #[test]
    fn test_not_operator() {
        test_eval("not age > 30", json!({"age": 25}), true);
        test_eval("not age > 30", json!({"age": 35}), false);
    }

    #[test]
    fn test_exists_operator() {
        test_eval("email exists", json!({"email": "test@example.com"}), true);
        test_eval("email exists", json!({"name": "Alice"}), false);
        test_eval("email not exists", json!({"name": "Alice"}), true);
    }

    #[test]
    fn test_dot_notation() {
        test_eval(
            "user.name is \"Alice\"",
            json!({"user": {"name": "Alice"}}),
            true,
        );
        test_eval(
            "user.address.city is \"NYC\"",
            json!({"user": {"address": {"city": "NYC"}}}),
            true,
        );
    }

    #[test]
    fn test_parentheses() {
        test_eval(
            "(age > 30 or age < 20) and active is true",
            json!({"age": 35, "active": true}),
            true,
        );
        test_eval(
            "(age > 30 or age < 20) and active is true",
            json!({"age": 25, "active": true}),
            false,
        );
    }

    #[test]
    fn test_shorthand_boolean() {
        test_eval("active", json!({"active": true}), true);
        test_eval("active", json!({"active": false}), false);
    }
}
