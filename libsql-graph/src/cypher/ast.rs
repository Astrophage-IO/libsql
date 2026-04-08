#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Match(MatchStatement),
    Create(CreateStatement),
    Delete(DeleteStatement),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchStatement {
    pub pattern: Pattern,
    pub where_clause: Option<Expr>,
    pub set_clauses: Vec<SetClause>,
    pub delete: Option<DeleteClause>,
    pub return_clause: Option<ReturnClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateStatement {
    pub elements: Vec<CreateElement>,
    pub return_clause: Option<ReturnClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateElement {
    Node {
        variable: Option<String>,
        label: Option<String>,
        properties: Vec<(String, Literal)>,
    },
    Relationship {
        from_var: String,
        rel_type: String,
        to_var: String,
        properties: Vec<(String, Literal)>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub detach: bool,
    pub variables: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteClause {
    pub detach: bool,
    pub variables: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetClause {
    pub variable: String,
    pub property: String,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnClause {
    pub items: Vec<ReturnItem>,
    pub order_by: Option<Vec<OrderItem>>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pattern {
    pub elements: Vec<PatternElement>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PatternElement {
    Node(NodePattern),
    Relationship(RelPattern),
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub label: Option<String>,
    pub properties: Vec<(String, Literal)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelPattern {
    pub variable: Option<String>,
    pub rel_type: Option<String>,
    pub direction: RelDirection,
    pub min_hops: Option<u32>,
    pub max_hops: Option<u32>,
    pub properties: Vec<(String, Literal)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelDirection {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Literal),
    Variable(String),
    Property(String, String),
    FunctionCall(String, Vec<Expr>),
    BinaryOp(Box<Expr>, BinOp, Box<Expr>),
    UnaryOp(UnaryOp, Box<Expr>),
    Parameter(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}
