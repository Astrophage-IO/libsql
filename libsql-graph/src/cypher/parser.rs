use crate::cypher::ast::*;
use crate::cypher::lexer::Token;

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        self.pos += 1;
        tok
    }

    fn expect(&mut self, expected: &Token) -> Result<Token, String> {
        let tok = self.advance();
        if std::mem::discriminant(&tok) == std::mem::discriminant(expected) {
            Ok(tok)
        } else {
            Err(format!("expected {:?}, got {:?}", expected, tok))
        }
    }

    fn expect_ident(&mut self) -> Result<String, String> {
        match self.advance() {
            Token::Ident(s) => Ok(s),
            tok => Err(format!("expected identifier, got {:?}", tok)),
        }
    }

    pub fn parse(&mut self) -> Result<Statement, String> {
        match self.peek() {
            Token::Match | Token::Optional => self.parse_match(),
            Token::Create => self.parse_create(),
            Token::Delete | Token::Detach => self.parse_delete_stmt(),
            Token::Merge => self.parse_merge(),
            Token::Unwind => self.parse_unwind(),
            _ => Err(format!(
                "expected MATCH, CREATE, DELETE, or MERGE, got {:?}",
                self.peek()
            )),
        }
    }

    fn parse_match(&mut self) -> Result<Statement, String> {
        let optional = if *self.peek() == Token::Optional {
            self.advance();
            true
        } else {
            false
        };
        self.expect(&Token::Match)?;
        let pattern = self.parse_pattern()?;

        let where_clause = if *self.peek() == Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let with_clause = if *self.peek() == Token::With {
            Some(self.parse_with()?)
        } else {
            None
        };

        let next_match = if *self.peek() == Token::Match || *self.peek() == Token::Optional {
            if let Statement::Match(m) = self.parse_match()? {
                Some(Box::new(m))
            } else {
                None
            }
        } else {
            None
        };

        let mut set_clauses = Vec::new();
        while *self.peek() == Token::Set {
            self.advance();
            set_clauses.push(self.parse_set_clause()?);
            while *self.peek() == Token::Comma {
                self.advance();
                set_clauses.push(self.parse_set_clause()?);
            }
        }

        let delete = if *self.peek() == Token::Detach || *self.peek() == Token::Delete {
            Some(self.parse_delete_clause()?)
        } else {
            None
        };

        let return_clause = if *self.peek() == Token::Return {
            Some(self.parse_return()?)
        } else {
            None
        };

        Ok(Statement::Match(MatchStatement {
            pattern,
            optional,
            where_clause,
            with_clause,
            next_match,
            set_clauses,
            delete,
            return_clause,
        }))
    }

    fn parse_with(&mut self) -> Result<WithClause, String> {
        self.expect(&Token::With)?;
        let mut items = Vec::new();
        items.push(self.parse_return_item()?);
        while *self.peek() == Token::Comma {
            self.advance();
            items.push(self.parse_return_item()?);
        }
        let where_clause = if *self.peek() == Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(WithClause {
            items,
            where_clause,
        })
    }

    fn parse_create(&mut self) -> Result<Statement, String> {
        self.expect(&Token::Create)?;
        let mut elements = Vec::new();

        elements.push(self.parse_create_element()?);
        while *self.peek() == Token::Comma {
            self.advance();
            elements.push(self.parse_create_element()?);
        }

        let return_clause = if *self.peek() == Token::Return {
            Some(self.parse_return()?)
        } else {
            None
        };

        Ok(Statement::Create(CreateStatement {
            elements,
            return_clause,
        }))
    }

    fn parse_create_element(&mut self) -> Result<CreateElement, String> {
        if *self.peek() == Token::LParen {
            let node = self.parse_node_pattern()?;

            if *self.peek() == Token::Dash || *self.peek() == Token::DashLBracket {
                let from_var = node
                    .variable
                    .ok_or("relationship source needs a variable")?;
                let rel_pat = self.parse_rel_pattern()?;
                self.expect(&Token::LParen)?;
                let to_var = self.expect_ident()?;
                self.expect(&Token::RParen)?;

                return Ok(CreateElement::Relationship {
                    from_var,
                    rel_type: rel_pat.rel_type.unwrap_or_default(),
                    to_var,
                    properties: rel_pat.properties,
                });
            }

            Ok(CreateElement::Node {
                variable: node.variable,
                label: node.label,
                properties: node.properties,
            })
        } else {
            Err(format!("expected '(' in CREATE, got {:?}", self.peek()))
        }
    }

    fn parse_delete_stmt(&mut self) -> Result<Statement, String> {
        let detach = if *self.peek() == Token::Detach {
            self.advance();
            true
        } else {
            false
        };
        self.expect(&Token::Delete)?;

        let mut variables = Vec::new();
        variables.push(self.expect_ident()?);
        while *self.peek() == Token::Comma {
            self.advance();
            variables.push(self.expect_ident()?);
        }

        Ok(Statement::Delete(DeleteStatement { detach, variables }))
    }

    fn parse_delete_clause(&mut self) -> Result<DeleteClause, String> {
        let detach = if *self.peek() == Token::Detach {
            self.advance();
            true
        } else {
            false
        };
        self.expect(&Token::Delete)?;

        let mut variables = Vec::new();
        variables.push(self.expect_ident()?);
        while *self.peek() == Token::Comma {
            self.advance();
            variables.push(self.expect_ident()?);
        }

        Ok(DeleteClause { detach, variables })
    }

    fn parse_set_clause(&mut self) -> Result<SetClause, String> {
        let variable = self.expect_ident()?;
        self.expect(&Token::Dot)?;
        let property = self.expect_ident()?;
        self.expect(&Token::Eq)?;
        let value = self.parse_expr()?;
        Ok(SetClause {
            variable,
            property,
            value,
        })
    }

    fn parse_return(&mut self) -> Result<ReturnClause, String> {
        self.expect(&Token::Return)?;

        let distinct = if *self.peek() == Token::Distinct {
            self.advance();
            true
        } else {
            false
        };

        let mut items = Vec::new();
        if *self.peek() == Token::Star {
            self.advance();
            items.push(ReturnItem {
                expr: Expr::Variable("*".to_string()),
                alias: None,
            });
        } else {
            items.push(self.parse_return_item()?);
            while *self.peek() == Token::Comma {
                self.advance();
                items.push(self.parse_return_item()?);
            }
        }

        let order_by = if *self.peek() == Token::Order {
            self.advance();
            self.expect(&Token::By)?;
            let mut orders = Vec::new();
            orders.push(self.parse_order_item()?);
            while *self.peek() == Token::Comma {
                self.advance();
                orders.push(self.parse_order_item()?);
            }
            Some(orders)
        } else {
            None
        };

        let skip = if *self.peek() == Token::Skip {
            self.advance();
            match self.advance() {
                Token::Integer(n) => Some(n as u64),
                tok => return Err(format!("expected integer after SKIP, got {:?}", tok)),
            }
        } else {
            None
        };

        let limit = if *self.peek() == Token::Limit {
            self.advance();
            match self.advance() {
                Token::Integer(n) => Some(n as u64),
                tok => return Err(format!("expected integer after LIMIT, got {:?}", tok)),
            }
        } else {
            None
        };

        Ok(ReturnClause {
            distinct,
            items,
            order_by,
            skip,
            limit,
        })
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem, String> {
        let expr = self.parse_expr()?;
        let alias = if *self.peek() == Token::As {
            self.advance();
            Some(self.expect_ident()?)
        } else {
            None
        };
        Ok(ReturnItem { expr, alias })
    }

    fn parse_order_item(&mut self) -> Result<OrderItem, String> {
        let expr = self.parse_expr()?;
        let descending = if *self.peek() == Token::Desc {
            self.advance();
            true
        } else {
            if *self.peek() == Token::Asc {
                self.advance();
            }
            false
        };
        Ok(OrderItem { expr, descending })
    }

    fn parse_pattern(&mut self) -> Result<Pattern, String> {
        let mut elements = Vec::new();

        let node = self.parse_node_pattern()?;
        elements.push(PatternElement::Node(node));

        while let Token::Dash | Token::DashLBracket | Token::LtDash = self.peek() {
            let rel = self.parse_rel_pattern()?;
            elements.push(PatternElement::Relationship(rel));
            let node = self.parse_node_pattern()?;
            elements.push(PatternElement::Node(node));
        }

        Ok(Pattern { elements })
    }

    fn parse_node_pattern(&mut self) -> Result<NodePattern, String> {
        self.expect(&Token::LParen)?;

        let mut variable = None;
        let mut label = None;
        let mut properties = Vec::new();

        if let Token::Ident(_) = self.peek() {
            variable = Some(self.expect_ident()?);
        }

        if *self.peek() == Token::Colon {
            self.advance();
            label = Some(self.expect_ident()?);
        }

        if *self.peek() == Token::LBrace {
            properties = self.parse_property_map()?;
        }

        self.expect(&Token::RParen)?;
        Ok(NodePattern {
            variable,
            label,
            properties,
        })
    }

    fn parse_rel_pattern(&mut self) -> Result<RelPattern, String> {
        let incoming = *self.peek() == Token::LtDash;
        if incoming {
            self.advance(); // <-
        }

        let has_bracket = if incoming {
            if *self.peek() == Token::LBracket {
                self.advance();
                true
            } else {
                false
            }
        } else if *self.peek() == Token::DashLBracket {
            self.advance();
            true
        } else if *self.peek() == Token::Dash {
            self.advance();
            false
        } else {
            return Err(format!(
                "expected - or -[ in relationship, got {:?}",
                self.peek()
            ));
        };

        let mut variable = None;
        let mut rel_type = None;
        let mut min_hops = None;
        let mut max_hops = None;
        let mut properties = Vec::new();

        if has_bracket {
            if let Token::Ident(_) = self.peek() {
                variable = Some(self.expect_ident()?);
            }

            if *self.peek() == Token::Colon {
                self.advance();
                rel_type = Some(self.expect_ident()?);
            }

            if *self.peek() == Token::Star {
                self.advance();
                if let Token::Integer(n) = self.peek() {
                    min_hops = Some(*n as u32);
                    self.advance();
                }
                if *self.peek() == Token::DotDot {
                    self.advance();
                    if let Token::Integer(n) = self.peek() {
                        max_hops = Some(*n as u32);
                        self.advance();
                    }
                } else if min_hops.is_some() && max_hops.is_none() {
                    max_hops = min_hops;
                }
            }

            if *self.peek() == Token::LBrace {
                properties = self.parse_property_map()?;
            }

            if *self.peek() == Token::RBracketDash {
                self.advance();
            } else {
                self.expect(&Token::RBracket)?;
                self.expect(&Token::Dash)?;
            }
        }

        let direction = if incoming {
            RelDirection::Incoming
        } else if *self.peek() == Token::Gt {
            self.advance();
            RelDirection::Outgoing
        } else {
            RelDirection::Both
        };

        Ok(RelPattern {
            variable,
            rel_type,
            direction,
            min_hops,
            max_hops,
            properties,
        })
    }

    fn parse_property_map(&mut self) -> Result<Vec<(String, Literal)>, String> {
        self.expect(&Token::LBrace)?;
        let mut props = Vec::new();

        if *self.peek() != Token::RBrace {
            let key = self.expect_ident()?;
            self.expect(&Token::Colon)?;
            let value = self.parse_literal()?;
            props.push((key, value));

            while *self.peek() == Token::Comma {
                self.advance();
                let key = self.expect_ident()?;
                self.expect(&Token::Colon)?;
                let value = self.parse_literal()?;
                props.push((key, value));
            }
        }

        self.expect(&Token::RBrace)?;
        Ok(props)
    }

    fn parse_literal(&mut self) -> Result<Literal, String> {
        match self.advance() {
            Token::Integer(n) => Ok(Literal::Integer(n)),
            Token::Float(f) => Ok(Literal::Float(f)),
            Token::StringLit(s) => Ok(Literal::String(s)),
            Token::True => Ok(Literal::Bool(true)),
            Token::False => Ok(Literal::Bool(false)),
            Token::Null => Ok(Literal::Null),
            Token::Dash => match self.advance() {
                Token::Integer(n) => Ok(Literal::Integer(-n)),
                Token::Float(f) => Ok(Literal::Float(-f)),
                tok => Err(format!("expected number after -, got {:?}", tok)),
            },
            tok => Err(format!("expected literal, got {:?}", tok)),
        }
    }

    fn parse_expr(&mut self) -> Result<Expr, String> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_and_expr()?;
        while *self.peek() == Token::Or {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::BinaryOp(Box::new(left), BinOp::Or, Box::new(right));
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_not_expr()?;
        while *self.peek() == Token::And {
            self.advance();
            let right = self.parse_not_expr()?;
            left = Expr::BinaryOp(Box::new(left), BinOp::And, Box::new(right));
        }
        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<Expr, String> {
        if *self.peek() == Token::Not {
            self.advance();
            let expr = self.parse_comparison()?;
            return Ok(Expr::UnaryOp(UnaryOp::Not, Box::new(expr)));
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr, String> {
        let left = self.parse_addition()?;
        let op = match self.peek() {
            Token::Eq => BinOp::Eq,
            Token::Neq => BinOp::Neq,
            Token::Lt => BinOp::Lt,
            Token::Gt => BinOp::Gt,
            Token::Lte => BinOp::Lte,
            Token::Gte => BinOp::Gte,
            Token::Contains => BinOp::Contains,
            Token::In => BinOp::In,
            Token::RegexOp => BinOp::RegexMatch,
            Token::Is => {
                self.advance();
                if *self.peek() == Token::Not {
                    self.advance();
                    self.expect(&Token::Null)?;
                    return Ok(Expr::UnaryOp(UnaryOp::IsNotNull, Box::new(left)));
                }
                self.expect(&Token::Null)?;
                return Ok(Expr::UnaryOp(UnaryOp::IsNull, Box::new(left)));
            }
            Token::StartsWith => {
                self.advance();
                self.expect(&Token::With)
                    .map_err(|_| "expected WITH after STARTS".to_string())?;
                let right = self.parse_addition()?;
                return Ok(Expr::BinaryOp(
                    Box::new(left),
                    BinOp::StartsWith,
                    Box::new(right),
                ));
            }
            Token::EndsWith => {
                self.advance();
                self.expect(&Token::With)
                    .map_err(|_| "expected WITH after ENDS".to_string())?;
                let right = self.parse_addition()?;
                return Ok(Expr::BinaryOp(
                    Box::new(left),
                    BinOp::EndsWith,
                    Box::new(right),
                ));
            }
            _ => return Ok(left),
        };
        self.advance();
        let right = self.parse_addition()?;
        Ok(Expr::BinaryOp(Box::new(left), op, Box::new(right)))
    }

    fn parse_addition(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_multiplication()?;
        loop {
            let op = match self.peek() {
                Token::Plus => BinOp::Add,
                Token::Dash => BinOp::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplication()?;
            left = Expr::BinaryOp(Box::new(left), op, Box::new(right));
        }
        Ok(left)
    }

    fn parse_multiplication(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Token::Star => BinOp::Mul,
                Token::Slash => BinOp::Div,
                Token::Percent => BinOp::Mod,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary()?;
            left = Expr::BinaryOp(Box::new(left), op, Box::new(right));
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, String> {
        if *self.peek() == Token::Dash {
            self.advance();
            let expr = self.parse_primary()?;
            return Ok(Expr::UnaryOp(UnaryOp::Neg, Box::new(expr)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr, String> {
        match self.peek().clone() {
            Token::Integer(n) => {
                self.advance();
                Ok(Expr::Literal(Literal::Integer(n)))
            }
            Token::Float(f) => {
                self.advance();
                Ok(Expr::Literal(Literal::Float(f)))
            }
            Token::StringLit(s) => {
                let s = s.clone();
                self.advance();
                Ok(Expr::Literal(Literal::String(s)))
            }
            Token::True => {
                self.advance();
                Ok(Expr::Literal(Literal::Bool(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Literal(Literal::Bool(false)))
            }
            Token::Null => {
                self.advance();
                Ok(Expr::Literal(Literal::Null))
            }
            Token::Parameter(name) => {
                let name = name.clone();
                self.advance();
                Ok(Expr::Parameter(name))
            }
            Token::Ident(name) => {
                let name = name.clone();
                self.advance();

                if *self.peek() == Token::Dot {
                    self.advance();
                    let prop = self.expect_ident()?;
                    Ok(Expr::Property(name, prop))
                } else if *self.peek() == Token::LParen {
                    self.advance();
                    let mut args = Vec::new();
                    if *self.peek() == Token::Star {
                        self.advance(); // count(*)
                    } else if *self.peek() != Token::RParen {
                        args.push(self.parse_expr()?);
                        while *self.peek() == Token::Comma {
                            self.advance();
                            args.push(self.parse_expr()?);
                        }
                    }
                    self.expect(&Token::RParen)?;
                    Ok(Expr::FunctionCall(name, args))
                } else {
                    Ok(Expr::Variable(name))
                }
            }
            Token::LParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Token::LBracket => {
                self.advance();
                let mut items = Vec::new();
                if *self.peek() != Token::RBracket {
                    items.push(self.parse_expr()?);
                    while *self.peek() == Token::Comma {
                        self.advance();
                        items.push(self.parse_expr()?);
                    }
                }
                // Handle ] or ]- (RBracketDash)
                if *self.peek() == Token::RBracketDash {
                    return Err("unexpected ]- in list literal".into());
                }
                self.expect(&Token::RBracket)?;
                Ok(Expr::Literal(Literal::List(
                    items
                        .into_iter()
                        .map(|e| match e {
                            Expr::Literal(lit) => lit,
                            _ => Literal::Null,
                        })
                        .collect(),
                )))
            }
            Token::Case => {
                self.advance();
                let operand = if *self.peek() != Token::When {
                    Some(Box::new(self.parse_expr()?))
                } else {
                    None
                };
                let mut when_clauses = Vec::new();
                while *self.peek() == Token::When {
                    self.advance();
                    let condition = self.parse_expr()?;
                    self.expect(&Token::Then)?;
                    let result = self.parse_expr()?;
                    when_clauses.push((condition, result));
                }
                let else_clause = if *self.peek() == Token::Else {
                    self.advance();
                    Some(Box::new(self.parse_expr()?))
                } else {
                    None
                };
                self.expect(&Token::End)?;
                Ok(Expr::Case {
                    operand,
                    when_clauses,
                    else_clause,
                })
            }
            tok => Err(format!("unexpected token in expression: {:?}", tok)),
        }
    }

    fn parse_unwind(&mut self) -> Result<Statement, String> {
        self.expect(&Token::Unwind)?;
        let expr = self.parse_expr()?;
        self.expect(&Token::As)?;
        let variable = self.expect_ident()?;
        let return_clause = if *self.peek() == Token::Return {
            Some(self.parse_return()?)
        } else {
            None
        };
        Ok(Statement::Unwind(UnwindStatement {
            expr,
            variable,
            return_clause,
        }))
    }

    fn parse_merge(&mut self) -> Result<Statement, String> {
        self.expect(&Token::Merge)?;
        let pattern = self.parse_node_pattern()?;

        let mut on_create_set = Vec::new();
        let mut on_match_set = Vec::new();

        while *self.peek() == Token::OnCreate {
            self.advance(); // ON
            match self.peek() {
                Token::Create => {
                    self.advance();
                    self.expect(&Token::Set)?;
                    on_create_set.push(self.parse_set_clause()?);
                    while *self.peek() == Token::Comma {
                        self.advance();
                        on_create_set.push(self.parse_set_clause()?);
                    }
                }
                Token::Match => {
                    self.advance();
                    self.expect(&Token::Set)?;
                    on_match_set.push(self.parse_set_clause()?);
                    while *self.peek() == Token::Comma {
                        self.advance();
                        on_match_set.push(self.parse_set_clause()?);
                    }
                }
                _ => {}
            }
        }

        let return_clause = if *self.peek() == Token::Return {
            Some(self.parse_return()?)
        } else {
            None
        };

        Ok(Statement::Merge(MergeStatement {
            pattern,
            on_create_set,
            on_match_set,
            return_clause,
        }))
    }
}

pub fn parse(input: &str) -> Result<Statement, String> {
    let mut lexer = crate::cypher::lexer::Lexer::new(input);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_match() {
        let stmt = parse("MATCH (a:Person) RETURN a").unwrap();
        match stmt {
            Statement::Match(m) => {
                assert_eq!(m.pattern.elements.len(), 1);
                if let PatternElement::Node(n) = &m.pattern.elements[0] {
                    assert_eq!(n.variable.as_deref(), Some("a"));
                    assert_eq!(n.label.as_deref(), Some("Person"));
                } else {
                    panic!("expected node pattern");
                }
                let ret = m.return_clause.unwrap();
                assert_eq!(ret.items.len(), 1);
            }
            _ => panic!("expected match statement"),
        }
    }

    #[test]
    fn test_parse_match_with_relationship() {
        let stmt = parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name").unwrap();
        match stmt {
            Statement::Match(m) => {
                assert_eq!(m.pattern.elements.len(), 3);
                if let PatternElement::Relationship(r) = &m.pattern.elements[1] {
                    assert_eq!(r.rel_type.as_deref(), Some("KNOWS"));
                    assert_eq!(r.direction, RelDirection::Outgoing);
                } else {
                    panic!("expected relationship");
                }
            }
            _ => panic!("expected match"),
        }
    }

    #[test]
    fn test_parse_match_with_where() {
        let stmt =
            parse("MATCH (a:Person) WHERE a.age > 25 AND a.name = 'Alice' RETURN a").unwrap();
        match stmt {
            Statement::Match(m) => {
                assert!(m.where_clause.is_some());
                if let Expr::BinaryOp(_, BinOp::And, _) = m.where_clause.unwrap() {
                } else {
                    panic!("expected AND expression");
                }
            }
            _ => panic!("expected match"),
        }
    }

    #[test]
    fn test_parse_match_with_order_limit() {
        let stmt = parse("MATCH (a:Person) RETURN a.name ORDER BY a.age DESC LIMIT 10").unwrap();
        match stmt {
            Statement::Match(m) => {
                let ret = m.return_clause.unwrap();
                let order = ret.order_by.unwrap();
                assert_eq!(order.len(), 1);
                assert!(order[0].descending);
                assert_eq!(ret.limit, Some(10));
            }
            _ => panic!("expected match"),
        }
    }

    #[test]
    fn test_parse_create_node() {
        let stmt = parse("CREATE (n:Person {name: 'Alice', age: 28})").unwrap();
        match stmt {
            Statement::Create(c) => {
                assert_eq!(c.elements.len(), 1);
                if let CreateElement::Node {
                    variable,
                    label,
                    properties,
                } = &c.elements[0]
                {
                    assert_eq!(variable.as_deref(), Some("n"));
                    assert_eq!(label.as_deref(), Some("Person"));
                    assert_eq!(properties.len(), 2);
                    assert_eq!(properties[0].0, "name");
                    assert_eq!(properties[0].1, Literal::String("Alice".into()));
                    assert_eq!(properties[1].0, "age");
                    assert_eq!(properties[1].1, Literal::Integer(28));
                } else {
                    panic!("expected node");
                }
            }
            _ => panic!("expected create"),
        }
    }

    #[test]
    fn test_parse_create_relationship() {
        let stmt = parse("CREATE (a)-[:KNOWS {since: 2020}]->(b)").unwrap();
        match stmt {
            Statement::Create(c) => {
                assert_eq!(c.elements.len(), 1);
                if let CreateElement::Relationship {
                    from_var,
                    rel_type,
                    to_var,
                    properties,
                } = &c.elements[0]
                {
                    assert_eq!(from_var, "a");
                    assert_eq!(rel_type, "KNOWS");
                    assert_eq!(to_var, "b");
                    assert_eq!(properties.len(), 1);
                } else {
                    panic!("expected relationship");
                }
            }
            _ => panic!("expected create"),
        }
    }

    #[test]
    fn test_parse_match_set() {
        let stmt = parse("MATCH (n:Person {name: 'Alice'}) SET n.age = 29 RETURN n").unwrap();
        match stmt {
            Statement::Match(m) => {
                assert_eq!(m.set_clauses.len(), 1);
                assert_eq!(m.set_clauses[0].variable, "n");
                assert_eq!(m.set_clauses[0].property, "age");
            }
            _ => panic!("expected match"),
        }
    }

    #[test]
    fn test_parse_match_detach_delete() {
        let stmt = parse("MATCH (n:Person {name: 'Alice'}) DETACH DELETE n").unwrap();
        match stmt {
            Statement::Match(m) => {
                let del = m.delete.unwrap();
                assert!(del.detach);
                assert_eq!(del.variables, vec!["n"]);
            }
            _ => panic!("expected match"),
        }
    }

    #[test]
    fn test_parse_delete_statement() {
        let stmt = parse("DELETE n").unwrap();
        match stmt {
            Statement::Delete(d) => {
                assert!(!d.detach);
                assert_eq!(d.variables, vec!["n"]);
            }
            _ => panic!("expected delete"),
        }
    }

    #[test]
    fn test_parse_incoming_relationship() {
        let stmt = parse("MATCH (a:Person)<-[:FOLLOWS]-(b) RETURN b").unwrap();
        match stmt {
            Statement::Match(m) => {
                if let PatternElement::Relationship(r) = &m.pattern.elements[1] {
                    assert_eq!(r.rel_type.as_deref(), Some("FOLLOWS"));
                    assert_eq!(r.direction, RelDirection::Incoming);
                } else {
                    panic!("expected relationship");
                }
            }
            _ => panic!("expected match"),
        }
    }

    #[test]
    fn test_parse_variable_length_path() {
        let stmt = parse("MATCH (a)-[:KNOWS*1..3]->(b) RETURN b").unwrap();
        match stmt {
            Statement::Match(m) => {
                if let PatternElement::Relationship(r) = &m.pattern.elements[1] {
                    assert_eq!(r.min_hops, Some(1));
                    assert_eq!(r.max_hops, Some(3));
                } else {
                    panic!("expected relationship");
                }
            }
            _ => panic!("expected match"),
        }
    }

    #[test]
    fn test_parse_function_call() {
        let stmt = parse("MATCH (a)-[:KNOWS]->(b) RETURN a.name, count(b) AS friends").unwrap();
        match stmt {
            Statement::Match(m) => {
                let ret = m.return_clause.unwrap();
                assert_eq!(ret.items.len(), 2);
                if let Expr::FunctionCall(name, args) = &ret.items[1].expr {
                    assert_eq!(name, "count");
                    assert_eq!(args.len(), 1);
                } else {
                    panic!("expected function call");
                }
                assert_eq!(ret.items[1].alias.as_deref(), Some("friends"));
            }
            _ => panic!("expected match"),
        }
    }

    #[test]
    fn test_parse_parameter() {
        let stmt = parse("MATCH (a:Person) WHERE a.name = $name RETURN a").unwrap();
        match stmt {
            Statement::Match(m) => {
                if let Expr::BinaryOp(_, BinOp::Eq, right) = m.where_clause.unwrap() {
                    assert_eq!(*right, Expr::Parameter("name".into()));
                } else {
                    panic!("expected eq expr");
                }
            }
            _ => panic!("expected match"),
        }
    }

    #[test]
    fn test_parse_undirected_relationship() {
        let stmt = parse("MATCH (a)-[:KNOWS]-(b) RETURN a, b").unwrap();
        match stmt {
            Statement::Match(m) => {
                if let PatternElement::Relationship(r) = &m.pattern.elements[1] {
                    assert_eq!(r.direction, RelDirection::Both);
                } else {
                    panic!("expected relationship");
                }
            }
            _ => panic!("expected match"),
        }
    }

    #[test]
    fn test_parse_complex_where() {
        let stmt =
            parse("MATCH (a:Person) WHERE (a.age >= 18 AND a.age < 65) OR a.vip = true RETURN a")
                .unwrap();
        match stmt {
            Statement::Match(m) => {
                assert!(m.where_clause.is_some());
                if let Expr::BinaryOp(_, BinOp::Or, _) = m.where_clause.unwrap() {
                } else {
                    panic!("expected OR at top level");
                }
            }
            _ => panic!("expected match"),
        }
    }
}
