#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Match(MatchStatement),
    Create(CreateStatement),
    Delete(DeleteStatement),
    Merge(MergeStatement),
    Unwind(UnwindStatement),
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnwindStatement {
    pub expr: Expr,
    pub variable: String,
    pub return_clause: Option<ReturnClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchStatement {
    pub pattern: Pattern,
    pub optional: bool,
    pub where_clause: Option<Expr>,
    pub with_clause: Option<WithClause>,
    pub next_match: Option<Box<MatchStatement>>,
    pub set_clauses: Vec<SetClause>,
    pub delete: Option<DeleteClause>,
    pub return_clause: Option<ReturnClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub items: Vec<ReturnItem>,
    pub where_clause: Option<Expr>,
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
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
    pub order_by: Option<Vec<OrderItem>>,
    pub skip: Option<u64>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MergeStatement {
    pub pattern: NodePattern,
    pub on_create_set: Vec<SetClause>,
    pub on_match_set: Vec<SetClause>,
    pub return_clause: Option<ReturnClause>,
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
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
    List(Vec<Literal>),
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
    Contains,
    StartsWith,
    EndsWith,
    In,
    RegexMatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
    IsNull,
    IsNotNull,
}
