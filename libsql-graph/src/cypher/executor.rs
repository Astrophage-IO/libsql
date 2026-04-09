use std::collections::HashMap;

use crate::cypher::ast::*;
use crate::cypher::planner::{PlanStep, QueryPlan};
use crate::error::GraphError;
use crate::graph::{Direction, GraphEngine};
use crate::storage::property_store::PropertyValue;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Node(u64),
    List(Vec<Value>),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Bool(b) => write!(f, "{b}"),
            Self::Integer(n) => write!(f, "{n}"),
            Self::Float(n) => write!(f, "{n}"),
            Self::String(s) => write!(f, "{s}"),
            Self::Node(id) => write!(f, "Node({id})"),
            Self::List(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
        }
    }
}

impl Value {
    fn from_property(pv: &PropertyValue) -> Self {
        match pv {
            PropertyValue::Null => Self::Null,
            PropertyValue::Bool(b) => Self::Bool(*b),
            PropertyValue::Int32(n) => Self::Integer(*n as i64),
            PropertyValue::Int64(n) => Self::Integer(*n),
            PropertyValue::Float64(f) => Self::Float(*f),
            PropertyValue::ShortString(s) => Self::String(s.clone()),
            PropertyValue::Overflow(_) => Self::Null,
        }
    }

    fn to_property(&self) -> PropertyValue {
        match self {
            Self::Null => PropertyValue::Null,
            Self::Bool(b) => PropertyValue::Bool(*b),
            Self::Integer(n) => {
                if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                    PropertyValue::Int32(*n as i32)
                } else {
                    PropertyValue::Int64(*n)
                }
            }
            Self::Float(f) => PropertyValue::Float64(*f),
            Self::String(s) => PropertyValue::ShortString(s.clone()),
            Self::Node(_) | Self::List(_) => PropertyValue::Null,
        }
    }

    fn from_literal(lit: &Literal) -> Self {
        match lit {
            Literal::Integer(n) => Self::Integer(*n),
            Literal::Float(f) => Self::Float(*f),
            Literal::String(s) => Self::String(s.clone()),
            Literal::Bool(b) => Self::Bool(*b),
            Literal::Null => Self::Null,
        }
    }

    fn is_truthy(&self) -> bool {
        match self {
            Self::Null => false,
            Self::Bool(b) => *b,
            Self::Integer(n) => *n != 0,
            Self::Float(f) => *f != 0.0,
            Self::String(s) => !s.is_empty(),
            Self::Node(_) => true,
            Self::List(items) => !items.is_empty(),
        }
    }
}

type Bindings = HashMap<String, Value>;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub stats: QueryStats,
}

#[derive(Debug, Clone, Default)]
pub struct QueryStats {
    pub nodes_created: u64,
    pub relationships_created: u64,
    pub properties_set: u64,
    pub nodes_deleted: u64,
}

pub fn execute(
    engine: &mut GraphEngine,
    plan: &QueryPlan,
    params: &HashMap<String, Value>,
) -> Result<QueryResult, GraphError> {
    let mut binding_table: Vec<Bindings> = vec![HashMap::new()];
    let mut stats = QueryStats::default();
    let mut columns = Vec::new();
    let mut project_items: Option<&Vec<ReturnItem>> = None;
    #[allow(unused_assignments)]
    let mut expanded_star: Option<Vec<ReturnItem>> = None;
    let mut order_items: Option<&Vec<OrderItem>> = None;
    let mut limit: Option<u64> = None;

    for step in &plan.steps {
        match step {
            PlanStep::NodeScan {
                variable,
                label,
                properties,
            } => {
                binding_table =
                    exec_node_scan(engine, &binding_table, variable, label, properties)?;
            }
            PlanStep::Expand {
                from_var,
                rel_var: _,
                to_var,
                rel_type,
                direction,
                min_hops,
                max_hops,
            } => {
                if min_hops.is_some() || max_hops.is_some() {
                    binding_table = exec_var_length_expand(
                        engine,
                        &binding_table,
                        from_var,
                        to_var,
                        rel_type.as_deref(),
                        *direction,
                        min_hops.unwrap_or(1),
                        max_hops.unwrap_or(u32::MAX),
                    )?;
                } else {
                    binding_table = exec_expand(
                        engine,
                        &binding_table,
                        from_var,
                        to_var,
                        rel_type.as_deref(),
                        *direction,
                    )?;
                }
            }
            PlanStep::Filter { predicate } => {
                binding_table = exec_filter(engine, &binding_table, predicate, params)?;
            }
            PlanStep::CreateNode {
                variable,
                label,
                properties,
            } => {
                binding_table =
                    exec_create_node(engine, &binding_table, variable, label, properties, &mut stats)?;
            }
            PlanStep::CreateRelationship {
                from_var,
                rel_type,
                to_var,
                properties,
            } => {
                exec_create_rel(engine, &binding_table, from_var, rel_type, to_var, properties, &mut stats)?;
            }
            PlanStep::SetProperty {
                variable,
                property,
                value,
            } => {
                exec_set_property(engine, &binding_table, variable, property, value, params, &mut stats)?;
            }
            PlanStep::DeleteNode { variable, detach } => {
                exec_delete(engine, &binding_table, variable, *detach, &mut stats)?;
            }
            PlanStep::Project { items } => {
                if items.len() == 1 && items[0].expr == Expr::Variable("*".to_string()) {
                    let mut all_vars: Vec<String> = binding_table
                        .iter()
                        .flat_map(|b| b.keys().cloned())
                        .collect();
                    all_vars.sort();
                    all_vars.dedup();
                    let expanded: Vec<ReturnItem> = all_vars
                        .into_iter()
                        .map(|name| ReturnItem {
                            expr: Expr::Variable(name),
                            alias: None,
                        })
                        .collect();
                    expanded_star = Some(expanded);
                    project_items = expanded_star.as_ref();
                } else {
                    project_items = Some(items);
                }
            }
            PlanStep::OrderBy { items } => {
                order_items = Some(items);
            }
            PlanStep::Limit { count } => {
                limit = Some(*count);
            }
        }
    }

    let mut rows = Vec::new();
    if let Some(items) = project_items {
        columns = items
            .iter()
            .map(|item| {
                item.alias.clone().unwrap_or_else(|| expr_name(&item.expr))
            })
            .collect();

        let has_aggregates = items.iter().any(|item| is_aggregate_expr(&item.expr));

        if has_aggregates {
            rows = exec_aggregate(engine, &binding_table, items, params);
        } else {
            for bindings in &binding_table {
                let row: Vec<Value> = items
                    .iter()
                    .map(|item| eval_expr(engine, &item.expr, bindings, params))
                    .collect();
                rows.push(row);
            }
        }
    }

    if let Some(order) = order_items {
        let col_indices: Vec<usize> = if let Some(proj) = project_items {
            order.iter().map(|ord| {
                let ord_name = expr_name(&ord.expr);
                proj.iter().position(|item| {
                    item.alias.as_deref() == Some(&ord_name) || expr_name(&item.expr) == ord_name
                }).unwrap_or(0)
            }).collect()
        } else {
            (0..order.len()).collect()
        };

        rows.sort_by(|a, b| {
            for (i, ord) in order.iter().enumerate() {
                let col_idx = col_indices.get(i).copied().unwrap_or(0);
                let col_idx = col_idx.min(a.len().saturating_sub(1));
                let cmp = compare_values(&a[col_idx], &b[col_idx]);
                let cmp = if ord.descending { cmp.reverse() } else { cmp };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    if let Some(n) = limit {
        rows.truncate(n as usize);
    }

    Ok(QueryResult {
        columns,
        rows,
        stats,
    })
}

fn exec_node_scan(
    engine: &mut GraphEngine,
    _current: &[Bindings],
    variable: &str,
    label: &Option<String>,
    properties: &[(String, Literal)],
) -> Result<Vec<Bindings>, GraphError> {
    let mut results = Vec::new();
    let label_token = match label {
        Some(name) => {
            let next = engine.db().header().next_token_id;
            let store = crate::storage::token_store::TokenStore::new(
                engine.db().header().token_store_root,
                engine.db().page_size() as usize,
            );
            store.find_by_name(
                engine.db().pager(),
                name,
                crate::storage::token_store::TOKEN_KIND_LABEL,
                next,
            )?
        }
        None => None,
    };

    let max_id = engine.db().header().next_node_id;
    for id in 0..max_id {
        let node = engine.get_node(id)?;
        if !node.in_use() {
            continue;
        }
        if let Some(lt) = label_token {
            if node.label_token_id != lt {
                continue;
            }
        }

        let mut match_props = true;
        for (key, expected) in properties {
            let actual = engine.get_node_property(id, key)?;
            let expected_val = Value::from_literal(expected);
            match actual {
                Some(pv) if Value::from_property(&pv) == expected_val => {}
                _ => {
                    match_props = false;
                    break;
                }
            }
        }
        if !match_props {
            continue;
        }

        let mut bindings = HashMap::new();
        if !variable.is_empty() {
            bindings.insert(variable.to_string(), Value::Node(id));
        }
        results.push(bindings);
    }

    Ok(results)
}

fn exec_expand(
    engine: &mut GraphEngine,
    current: &[Bindings],
    from_var: &str,
    to_var: &str,
    rel_type: Option<&str>,
    direction: Direction,
) -> Result<Vec<Bindings>, GraphError> {
    let rel_type_token = match rel_type {
        Some(name) => {
            let next = engine.db().header().next_token_id;
            let store = crate::storage::token_store::TokenStore::new(
                engine.db().header().token_store_root,
                engine.db().page_size() as usize,
            );
            store.find_by_name(
                engine.db().pager(),
                name,
                crate::storage::token_store::TOKEN_KIND_REL_TYPE,
                next,
            )?
        }
        None => None,
    };

    let mut results = Vec::new();
    for bindings in current {
        let from_id = match bindings.get(from_var) {
            Some(Value::Node(id)) => *id,
            _ => continue,
        };

        let neighbors = engine.get_neighbors(from_id, direction)?;
        for (neighbor_id, rel_addr) in neighbors {
            if let Some(expected_type) = rel_type_token {
                let store_root = engine.db().header().rel_store_root;
                let ps = engine.db().page_size() as usize;
                let store = crate::storage::rel_store::RelStore::new(store_root, ps);
                let rel = store.read_rel_at(engine.db().pager(), rel_addr)?;
                if rel.type_token_id != expected_type {
                    continue;
                }
            }

            let mut new_bindings = bindings.clone();
            if !to_var.is_empty() {
                new_bindings.insert(to_var.to_string(), Value::Node(neighbor_id));
            }
            results.push(new_bindings);
        }
    }

    Ok(results)
}

fn exec_var_length_expand(
    engine: &mut GraphEngine,
    current: &[Bindings],
    from_var: &str,
    to_var: &str,
    rel_type: Option<&str>,
    direction: Direction,
    min_hops: u32,
    max_hops: u32,
) -> Result<Vec<Bindings>, GraphError> {
    let mut results = Vec::new();

    for bindings in current {
        let from_id = match bindings.get(from_var) {
            Some(Value::Node(id)) => *id,
            _ => continue,
        };

        let mut frontier: Vec<(u64, u32)> = vec![(from_id, 0)];
        let mut visited = std::collections::HashSet::new();
        visited.insert(from_id);

        while let Some((current_id, depth)) = frontier.pop() {
            if depth >= min_hops {
                let mut new_bindings = bindings.clone();
                if !to_var.is_empty() {
                    new_bindings.insert(to_var.to_string(), Value::Node(current_id));
                }
                results.push(new_bindings);
            }

            if depth >= max_hops {
                continue;
            }

            let neighbors = engine.get_neighbors(current_id, direction)?;
            for (neighbor_id, rel_addr) in neighbors {
                if let Some(rt) = rel_type {
                    let next = engine.db().header().next_token_id;
                    let store = crate::storage::token_store::TokenStore::new(
                        engine.db().header().token_store_root,
                        engine.db().page_size() as usize,
                    );
                    let type_token = store.find_by_name(
                        engine.db().pager(),
                        rt,
                        crate::storage::token_store::TOKEN_KIND_REL_TYPE,
                        next,
                    )?;
                    if let Some(tt) = type_token {
                        let store_root = engine.db().header().rel_store_root;
                        let ps = engine.db().page_size() as usize;
                        let store = crate::storage::rel_store::RelStore::new(store_root, ps);
                        let rel = store.read_rel_at(engine.db().pager(), rel_addr)?;
                        if rel.type_token_id != tt {
                            continue;
                        }
                    }
                }

                if visited.insert(neighbor_id) {
                    frontier.push((neighbor_id, depth + 1));
                }
            }
        }
    }

    Ok(results)
}

fn exec_filter(
    engine: &mut GraphEngine,
    current: &[Bindings],
    predicate: &Expr,
    params: &HashMap<String, Value>,
) -> Result<Vec<Bindings>, GraphError> {
    let mut results = Vec::new();
    for bindings in current {
        let val = eval_expr(engine, predicate, bindings, params);
        if val.is_truthy() {
            results.push(bindings.clone());
        }
    }
    Ok(results)
}

fn exec_create_node(
    engine: &mut GraphEngine,
    current: &[Bindings],
    variable: &Option<String>,
    label: &Option<String>,
    properties: &[(String, Literal)],
    stats: &mut QueryStats,
) -> Result<Vec<Bindings>, GraphError> {
    let label_name = label.as_deref().unwrap_or("_default");
    let node_id = engine.create_node(label_name)?;
    stats.nodes_created += 1;

    for (key, val) in properties {
        let pv = Value::from_literal(val).to_property();
        engine.set_node_property(node_id, key, pv)?;
        stats.properties_set += 1;
    }

    let mut results = Vec::new();
    if current.is_empty() {
        let mut bindings = HashMap::new();
        if let Some(var) = variable {
            bindings.insert(var.clone(), Value::Node(node_id));
        }
        results.push(bindings);
    } else {
        for bindings in current {
            let mut new_bindings = bindings.clone();
            if let Some(var) = variable {
                new_bindings.insert(var.clone(), Value::Node(node_id));
            }
            results.push(new_bindings);
        }
    }

    Ok(results)
}

fn exec_create_rel(
    engine: &mut GraphEngine,
    current: &[Bindings],
    from_var: &str,
    rel_type: &str,
    to_var: &str,
    properties: &[(String, Literal)],
    stats: &mut QueryStats,
) -> Result<(), GraphError> {
    for bindings in current {
        let from_id = match bindings.get(from_var) {
            Some(Value::Node(id)) => *id,
            _ => continue,
        };
        let to_id = match bindings.get(to_var) {
            Some(Value::Node(id)) => *id,
            _ => continue,
        };

        let _rel_id = engine.create_relationship(from_id, to_id, rel_type)?;
        stats.relationships_created += 1;
        let _ = properties;
    }
    Ok(())
}

fn exec_set_property(
    engine: &mut GraphEngine,
    current: &[Bindings],
    variable: &str,
    property: &str,
    value: &Expr,
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<(), GraphError> {
    for bindings in current {
        let node_id = match bindings.get(variable) {
            Some(Value::Node(id)) => *id,
            _ => continue,
        };
        let val = eval_expr(engine, value, bindings, params);
        engine.set_node_property(node_id, property, val.to_property())?;
        stats.properties_set += 1;
    }
    Ok(())
}

fn exec_delete(
    engine: &mut GraphEngine,
    current: &[Bindings],
    variable: &str,
    detach: bool,
    stats: &mut QueryStats,
) -> Result<(), GraphError> {
    let mut ids: Vec<u64> = current
        .iter()
        .filter_map(|b| match b.get(variable) {
            Some(Value::Node(id)) => Some(*id),
            _ => None,
        })
        .collect();
    ids.sort_unstable();
    ids.dedup();

    for id in ids {
        if detach {
            engine.detach_delete_node(id)?;
        } else {
            let node = engine.get_node(id)?;
            if node.rel_count > 0 {
                return Err(GraphError::PagerError(format!(
                    "cannot delete node {} with {} relationships without DETACH",
                    id, node.rel_count
                )));
            }
            engine.detach_delete_node(id)?;
        }
        stats.nodes_deleted += 1;
    }
    Ok(())
}

fn eval_expr(
    engine: &mut GraphEngine,
    expr: &Expr,
    bindings: &Bindings,
    params: &HashMap<String, Value>,
) -> Value {
    match expr {
        Expr::Literal(lit) => Value::from_literal(lit),
        Expr::Variable(name) => bindings.get(name).cloned().unwrap_or(Value::Null),
        Expr::Parameter(name) => params.get(name).cloned().unwrap_or(Value::Null),
        Expr::Property(var, prop) => {
            let node_id = match bindings.get(var) {
                Some(Value::Node(id)) => *id,
                _ => return Value::Null,
            };
            engine
                .get_node_property(node_id, prop)
                .ok()
                .flatten()
                .map(|pv| Value::from_property(&pv))
                .unwrap_or(Value::Null)
        }
        Expr::FunctionCall(name, args) => {
            match name.to_lowercase().as_str() {
                "tostring" | "tostr" => {
                    let val = eval_expr(engine, &args[0], bindings, params);
                    Value::String(val.to_string())
                }
                "tointeger" | "toint" => {
                    let val = eval_expr(engine, &args[0], bindings, params);
                    match val {
                        Value::Integer(_) => val,
                        Value::Float(f) => Value::Integer(f as i64),
                        Value::String(s) => s.parse::<i64>().map(Value::Integer).unwrap_or(Value::Null),
                        Value::Bool(b) => Value::Integer(b as i64),
                        _ => Value::Null,
                    }
                }
                "tofloat" => {
                    let val = eval_expr(engine, &args[0], bindings, params);
                    match val {
                        Value::Float(_) => val,
                        Value::Integer(n) => Value::Float(n as f64),
                        Value::String(s) => s.parse::<f64>().map(Value::Float).unwrap_or(Value::Null),
                        _ => Value::Null,
                    }
                }
                "type" => {
                    let val = eval_expr(engine, &args[0], bindings, params);
                    let type_name = match val {
                        Value::Null => "NULL",
                        Value::Bool(_) => "BOOLEAN",
                        Value::Integer(_) => "INTEGER",
                        Value::Float(_) => "FLOAT",
                        Value::String(_) => "STRING",
                        Value::Node(_) => "NODE",
                        Value::List(_) => "LIST",
                    };
                    Value::String(type_name.to_string())
                }
                "id" => {
                    let val = eval_expr(engine, &args[0], bindings, params);
                    match val {
                        Value::Node(id) => Value::Integer(id as i64),
                        _ => Value::Null,
                    }
                }
                "labels" => {
                    let val = eval_expr(engine, &args[0], bindings, params);
                    if let Value::Node(id) = val {
                        let label_name = engine.get_node(id).ok().map(|n| {
                            engine.get_label_name(n.label_token_id).unwrap_or_default()
                        }).unwrap_or_default();
                        Value::List(vec![Value::String(label_name)])
                    } else {
                        Value::Null
                    }
                }
                // Aggregates return placeholder when evaluated per-row;
                // real aggregation happens in exec_aggregate
                "count" | "sum" | "avg" | "min" | "max" | "collect" => {
                    if args.is_empty() {
                        Value::Integer(1) // count(*)
                    } else {
                        eval_expr(engine, &args[0], bindings, params)
                    }
                }
                _ => Value::Null,
            }
        }
        Expr::BinaryOp(left, op, right) => {
            let l = eval_expr(engine, left, bindings, params);
            let r = eval_expr(engine, right, bindings, params);
            eval_binop(&l, *op, &r)
        }
        Expr::UnaryOp(op, inner) => {
            let val = eval_expr(engine, inner, bindings, params);
            match op {
                UnaryOp::Not => Value::Bool(!val.is_truthy()),
                UnaryOp::Neg => match val {
                    Value::Integer(n) => Value::Integer(-n),
                    Value::Float(f) => Value::Float(-f),
                    _ => Value::Null,
                },
            }
        }
    }
}

fn eval_binop(left: &Value, op: BinOp, right: &Value) -> Value {
    match op {
        BinOp::And => Value::Bool(left.is_truthy() && right.is_truthy()),
        BinOp::Or => Value::Bool(left.is_truthy() || right.is_truthy()),
        BinOp::Eq => Value::Bool(left == right),
        BinOp::Neq => Value::Bool(left != right),
        BinOp::Lt => Value::Bool(compare_values(left, right) == std::cmp::Ordering::Less),
        BinOp::Gt => Value::Bool(compare_values(left, right) == std::cmp::Ordering::Greater),
        BinOp::Lte => Value::Bool(compare_values(left, right) != std::cmp::Ordering::Greater),
        BinOp::Gte => Value::Bool(compare_values(left, right) != std::cmp::Ordering::Less),
        BinOp::Add => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Integer(a + b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
            (Value::Integer(a), Value::Float(b)) => Value::Float(*a as f64 + b),
            (Value::Float(a), Value::Integer(b)) => Value::Float(a + *b as f64),
            (Value::String(a), Value::String(b)) => Value::String(format!("{a}{b}")),
            _ => Value::Null,
        },
        BinOp::Sub => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Integer(a - b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
            _ => Value::Null,
        },
        BinOp::Mul => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Integer(a * b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
            _ => Value::Null,
        },
        BinOp::Div => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) if *b != 0 => Value::Integer(a / b),
            (Value::Float(a), Value::Float(b)) if *b != 0.0 => Value::Float(a / b),
            _ => Value::Null,
        },
        BinOp::Mod => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) if *b != 0 => Value::Integer(a % b),
            _ => Value::Null,
        },
    }
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Integer(x), Value::Float(y)) => (*x as f64).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(x), Value::Integer(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}

fn expr_name(expr: &Expr) -> String {
    match expr {
        Expr::Variable(name) => name.clone(),
        Expr::Property(var, prop) => format!("{var}.{prop}"),
        Expr::FunctionCall(name, args) => {
            let inner = args.first().map(|a| expr_name(a)).unwrap_or("*".into());
            format!("{name}({inner})")
        }
        _ => "expr".to_string(),
    }
}

fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall(name, _) => matches!(
            name.to_lowercase().as_str(),
            "count" | "sum" | "avg" | "min" | "max" | "collect"
        ),
        _ => false,
    }
}

fn exec_aggregate(
    engine: &mut GraphEngine,
    binding_table: &[Bindings],
    items: &[ReturnItem],
    params: &HashMap<String, Value>,
) -> Vec<Vec<Value>> {
    let group_indices: Vec<usize> = items
        .iter()
        .enumerate()
        .filter(|(_, item)| !is_aggregate_expr(&item.expr))
        .map(|(i, _)| i)
        .collect();

    let agg_indices: Vec<usize> = items
        .iter()
        .enumerate()
        .filter(|(_, item)| is_aggregate_expr(&item.expr))
        .map(|(i, _)| i)
        .collect();

    if group_indices.is_empty() {
        let mut row = vec![Value::Null; items.len()];
        for &ai in &agg_indices {
            row[ai] = compute_aggregate(engine, binding_table, &items[ai].expr, params);
        }
        return vec![row];
    }

    let mut groups: Vec<(Vec<Value>, Vec<Vec<Value>>)> = Vec::new();

    for bindings in binding_table {
        let group_key: Vec<Value> = group_indices
            .iter()
            .map(|&i| eval_expr(engine, &items[i].expr, bindings, params))
            .collect();

        let all_vals: Vec<Value> = items
            .iter()
            .map(|item| eval_expr(engine, &item.expr, bindings, params))
            .collect();

        if let Some(group) = groups.iter_mut().find(|(key, _)| key == &group_key) {
            group.1.push(all_vals);
        } else {
            groups.push((group_key, vec![all_vals]));
        }
    }

    groups
        .into_iter()
        .map(|(_, group_rows)| {
            let mut result_row = vec![Value::Null; items.len()];

            for &gi in &group_indices {
                result_row[gi] = group_rows[0][gi].clone();
            }

            for &ai in &agg_indices {
                let vals: Vec<Value> = group_rows.iter().map(|row| row[ai].clone()).collect();
                result_row[ai] = aggregate_values(&items[ai].expr, &vals);
            }

            result_row
        })
        .collect()
}

fn compute_aggregate(
    engine: &mut GraphEngine,
    binding_table: &[Bindings],
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Value {
    if let Expr::FunctionCall(_name, args) = expr {
        let vals: Vec<Value> = binding_table
            .iter()
            .map(|b| {
                if args.is_empty() {
                    Value::Integer(1)
                } else {
                    eval_expr(engine, &args[0], b, params)
                }
            })
            .collect();
        aggregate_values(expr, &vals)
    } else {
        Value::Null
    }
}

fn aggregate_values(expr: &Expr, vals: &[Value]) -> Value {
    let func_name = match expr {
        Expr::FunctionCall(name, _) => name.to_lowercase(),
        _ => return Value::Null,
    };

    match func_name.as_str() {
        "count" => Value::Integer(vals.iter().filter(|v| **v != Value::Null).count() as i64),
        "sum" => {
            let mut total = 0i64;
            let mut is_float = false;
            let mut ftotal = 0.0f64;
            for v in vals {
                match v {
                    Value::Integer(n) => { total += n; ftotal += *n as f64; }
                    Value::Float(f) => { is_float = true; ftotal += f; }
                    _ => {}
                }
            }
            if is_float { Value::Float(ftotal) } else { Value::Integer(total) }
        }
        "avg" => {
            let mut sum = 0.0f64;
            let mut count = 0u64;
            for v in vals {
                match v {
                    Value::Integer(n) => { sum += *n as f64; count += 1; }
                    Value::Float(f) => { sum += f; count += 1; }
                    _ => {}
                }
            }
            if count == 0 { Value::Null } else { Value::Float(sum / count as f64) }
        }
        "min" => {
            vals.iter()
                .filter(|v| **v != Value::Null)
                .min_by(|a, b| compare_values(a, b))
                .cloned()
                .unwrap_or(Value::Null)
        }
        "max" => {
            vals.iter()
                .filter(|v| **v != Value::Null)
                .max_by(|a, b| compare_values(a, b))
                .cloned()
                .unwrap_or(Value::Null)
        }
        "collect" => Value::List(vals.to_vec()),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cypher::parser::parse;
    use crate::cypher::planner;
    use tempfile::NamedTempFile;

    fn temp_path() -> String {
        let f = NamedTempFile::new().unwrap();
        let p = f.path().to_str().unwrap().to_string();
        drop(f);
        p
    }

    fn setup_social_graph(path: &str) -> GraphEngine {
        let mut engine = GraphEngine::create(path, 4096).unwrap();
        let alice = engine.create_node("Person").unwrap();
        let bob = engine.create_node("Person").unwrap();
        let charlie = engine.create_node("Person").unwrap();

        engine.set_node_property(alice, "name", PropertyValue::ShortString("Alice".into())).unwrap();
        engine.set_node_property(alice, "age", PropertyValue::Int32(30)).unwrap();
        engine.set_node_property(bob, "name", PropertyValue::ShortString("Bob".into())).unwrap();
        engine.set_node_property(bob, "age", PropertyValue::Int32(25)).unwrap();
        engine.set_node_property(charlie, "name", PropertyValue::ShortString("Charlie".into())).unwrap();
        engine.set_node_property(charlie, "age", PropertyValue::Int32(35)).unwrap();

        engine.create_relationship(alice, bob, "KNOWS").unwrap();
        engine.create_relationship(alice, charlie, "KNOWS").unwrap();
        engine.create_relationship(bob, charlie, "FOLLOWS").unwrap();

        engine
    }

    fn run_query(
        engine: &mut GraphEngine,
        query: &str,
    ) -> QueryResult {
        let stmt = parse(query).unwrap();
        let plan = planner::plan(&stmt).unwrap();
        execute(engine, &plan, &HashMap::new()).unwrap()
    }

    #[test]
    fn test_match_all_persons() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(&mut engine, "MATCH (a:Person) RETURN a.name");
        assert_eq!(result.columns, vec!["a.name"]);
        assert_eq!(result.rows.len(), 3);
        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_match_with_where() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(
            &mut engine,
            "MATCH (a:Person) WHERE a.age > 28 RETURN a.name",
        );
        assert_eq!(result.rows.len(), 2);
        let names: Vec<&Value> = result.rows.iter().map(|r| &r[0]).collect();
        assert!(names.contains(&&Value::String("Alice".into())));
        assert!(names.contains(&&Value::String("Charlie".into())));
        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_match_relationship() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(
            &mut engine,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
        );
        assert_eq!(result.columns, vec!["a.name", "b.name"]);
        assert_eq!(result.rows.len(), 2);
        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_match_with_order_and_limit() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(
            &mut engine,
            "MATCH (a:Person) RETURN a.name, a.age ORDER BY a.age DESC LIMIT 2",
        );
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][1], Value::Integer(35));
        assert_eq!(result.rows[1][1], Value::Integer(30));
        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_create_node_via_cypher() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        let result = run_query(
            &mut engine,
            "CREATE (n:Person {name: 'Dave', age: 40})",
        );
        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.properties_set, 2);
        assert_eq!(engine.node_count(), 1);

        let name = engine.get_node_property(0, "name").unwrap();
        assert_eq!(name, Some(PropertyValue::ShortString("Dave".into())));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_match_set_property() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(
            &mut engine,
            "MATCH (n:Person) WHERE n.name = 'Alice' SET n.age = 31 RETURN n.age",
        );
        assert_eq!(result.stats.properties_set, 1);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(31));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_match_detach_delete() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        assert_eq!(engine.node_count(), 3);

        let stmt = parse("MATCH (n:Person {name: 'Alice'}) DETACH DELETE n").unwrap();
        let plan = planner::plan(&stmt).unwrap();
        let result = execute(&mut engine, &plan, &HashMap::new()).unwrap();

        assert_eq!(result.stats.nodes_deleted, 1);
        assert_eq!(engine.node_count(), 2);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_match_with_parameter() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);

        let stmt = parse("MATCH (a:Person) WHERE a.name = $name RETURN a.age").unwrap();
        let plan = planner::plan(&stmt).unwrap();
        let mut params = HashMap::new();
        params.insert("name".to_string(), Value::String("Bob".into()));

        let result = execute(&mut engine, &plan, &params).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(25));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_return_with_alias() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(
            &mut engine,
            "MATCH (a:Person) RETURN a.name AS person_name LIMIT 1",
        );
        assert_eq!(result.columns, vec!["person_name"]);
        assert_eq!(result.rows.len(), 1);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_arithmetic_in_return() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(
            &mut engine,
            "MATCH (a:Person) WHERE a.name = 'Alice' RETURN a.age + 1",
        );
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(31));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_no_results() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(
            &mut engine,
            "MATCH (a:Person) WHERE a.name = 'Nobody' RETURN a",
        );
        assert_eq!(result.rows.len(), 0);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_count_aggregate() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(
            &mut engine,
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name, count(b) AS friends",
        );
        assert_eq!(result.columns, vec!["a.name", "friends"]);
        assert_eq!(result.rows.len(), 1); // Alice is the only one with KNOWS outgoing
        assert_eq!(result.rows[0][0], Value::String("Alice".into()));
        assert_eq!(result.rows[0][1], Value::Integer(2));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_sum_aggregate() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(
            &mut engine,
            "MATCH (a:Person) RETURN sum(a.age)",
        );
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(90)); // 30 + 25 + 35

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_avg_aggregate() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(
            &mut engine,
            "MATCH (a:Person) RETURN avg(a.age)",
        );
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Float(30.0)); // (30+25+35)/3

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_min_max_aggregate() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(
            &mut engine,
            "MATCH (a:Person) RETURN min(a.age), max(a.age)",
        );
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(25));
        assert_eq!(result.rows[0][1], Value::Integer(35));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_collect_aggregate() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(
            &mut engine,
            "MATCH (a:Person) RETURN collect(a.name)",
        );
        assert_eq!(result.rows.len(), 1);
        if let Value::List(items) = &result.rows[0][0] {
            assert_eq!(items.len(), 3);
            assert!(items.contains(&Value::String("Alice".into())));
            assert!(items.contains(&Value::String("Bob".into())));
            assert!(items.contains(&Value::String("Charlie".into())));
        } else {
            panic!("expected list");
        }

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_query_convenience_method() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = engine.query("MATCH (a:Person) RETURN a.name LIMIT 2").unwrap();
        assert_eq!(result.rows.len(), 2);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_query_with_params() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let mut params = HashMap::new();
        params.insert("name".to_string(), Value::String("Bob".into()));
        let result = engine
            .query_with_params("MATCH (a:Person) WHERE a.name = $name RETURN a.age", params)
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(25));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_return_star() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(
            &mut engine,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN *",
        );
        assert!(result.columns.contains(&"a".to_string()));
        assert!(result.columns.contains(&"b".to_string()));
        assert_eq!(result.rows.len(), 2);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_id_function() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(
            &mut engine,
            "MATCH (a:Person) WHERE a.name = 'Alice' RETURN id(a)",
        );
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(0));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_labels_function() {
        let path = temp_path();
        let mut engine = setup_social_graph(&path);
        let result = run_query(
            &mut engine,
            "MATCH (a:Person) WHERE a.name = 'Alice' RETURN labels(a)",
        );
        assert_eq!(result.rows.len(), 1);
        if let Value::List(labels) = &result.rows[0][0] {
            assert_eq!(labels.len(), 1);
            assert_eq!(labels[0], Value::String("Person".into()));
        } else {
            panic!("expected list");
        }

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }
}
