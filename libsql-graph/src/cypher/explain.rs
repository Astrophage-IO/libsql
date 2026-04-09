use crate::cypher::planner::{PlanStep, QueryPlan};

pub fn explain(plan: &QueryPlan) -> String {
    let mut lines = Vec::new();
    lines.push("Query Plan:".to_string());

    for (i, step) in plan.steps.iter().enumerate() {
        let prefix = if i == plan.steps.len() - 1 {
            "└─"
        } else {
            "├─"
        };
        lines.push(format!("{} {}", prefix, format_step(step)));
    }

    lines.join("\n")
}

pub fn explain_verbose(plan: &QueryPlan) -> Vec<ExplainRow> {
    plan.steps
        .iter()
        .enumerate()
        .map(|(i, step)| ExplainRow {
            step: i,
            operation: operation_name(step),
            detail: format_step(step),
            estimated_rows: estimate_rows(step),
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct ExplainRow {
    pub step: usize,
    pub operation: String,
    pub detail: String,
    pub estimated_rows: String,
}

fn operation_name(step: &PlanStep) -> String {
    match step {
        PlanStep::NodeScan { .. } => "NodeScan".into(),
        PlanStep::Expand { .. } => "Expand".into(),
        PlanStep::Filter { .. } => "Filter".into(),
        PlanStep::CreateNode { .. } => "CreateNode".into(),
        PlanStep::CreateRelationship { .. } => "CreateRel".into(),
        PlanStep::SetProperty { .. } => "SetProperty".into(),
        PlanStep::DeleteNode { .. } => "DeleteNode".into(),
        PlanStep::Project { .. } => "Project".into(),
        PlanStep::OrderBy { .. } => "OrderBy".into(),
        PlanStep::Limit { .. } => "Limit".into(),
        PlanStep::Distinct => "Distinct".into(),
        PlanStep::With { .. } => "With".into(),
        PlanStep::Merge { .. } => "Merge".into(),
    }
}

fn format_step(step: &PlanStep) -> String {
    match step {
        PlanStep::NodeScan {
            variable,
            label,
            properties,
        } => {
            let label_str = label
                .as_deref()
                .map(|l| format!(":{l}"))
                .unwrap_or_default();
            let props_str = if properties.is_empty() {
                String::new()
            } else {
                let kv: Vec<String> = properties
                    .iter()
                    .map(|(k, _)| k.clone())
                    .collect();
                format!(" {{{}}}", kv.join(", "))
            };
            format!("NodeScan({variable}{label_str}{props_str})")
        }
        PlanStep::Expand {
            from_var,
            to_var,
            rel_type,
            direction,
            min_hops,
            max_hops,
            ..
        } => {
            let dir = match direction {
                crate::graph::Direction::Outgoing => "->",
                crate::graph::Direction::Incoming => "<-",
                crate::graph::Direction::Both => "--",
            };
            let type_str = rel_type
                .as_deref()
                .map(|t| format!(":{t}"))
                .unwrap_or_default();
            let hops = match (min_hops, max_hops) {
                (Some(min), Some(max)) => format!("*{min}..{max}"),
                (Some(min), None) => format!("*{min}.."),
                (None, Some(max)) => format!("*..{max}"),
                (None, None) => String::new(),
            };
            format!("Expand({from_var})-[{type_str}{hops}]{dir}({to_var})")
        }
        PlanStep::Filter { predicate } => {
            format!("Filter({})", format_expr(predicate))
        }
        PlanStep::CreateNode {
            variable,
            label,
            properties,
        } => {
            let var = variable.as_deref().unwrap_or("_");
            let label_str = label
                .as_deref()
                .map(|l| format!(":{l}"))
                .unwrap_or_default();
            format!(
                "CreateNode({var}{label_str} {{{}}})",
                properties.len()
            )
        }
        PlanStep::CreateRelationship {
            from_var,
            rel_type,
            to_var,
            ..
        } => {
            format!("CreateRel({from_var})-[:{rel_type}]->({to_var})")
        }
        PlanStep::SetProperty {
            variable,
            property,
            ..
        } => {
            format!("SetProperty({variable}.{property})")
        }
        PlanStep::DeleteNode { variable, detach } => {
            if *detach {
                format!("DetachDelete({variable})")
            } else {
                format!("Delete({variable})")
            }
        }
        PlanStep::Project { items } => {
            let cols: Vec<String> = items
                .iter()
                .map(|item| {
                    let name = format_expr(&item.expr);
                    match &item.alias {
                        Some(a) => format!("{name} AS {a}"),
                        None => name,
                    }
                })
                .collect();
            format!("Project({})", cols.join(", "))
        }
        PlanStep::OrderBy { items } => {
            let cols: Vec<String> = items
                .iter()
                .map(|item| {
                    let name = format_expr(&item.expr);
                    if item.descending {
                        format!("{name} DESC")
                    } else {
                        name
                    }
                })
                .collect();
            format!("OrderBy({})", cols.join(", "))
        }
        PlanStep::Limit { count } => format!("Limit({count})"),
        PlanStep::Distinct => "Distinct".into(),
        PlanStep::With { items, .. } => {
            let cols: Vec<String> = items.iter().map(|i| format_expr(&i.expr)).collect();
            format!("With({})", cols.join(", "))
        }
        PlanStep::Merge {
            variable,
            label,
            properties,
            ..
        } => {
            let var = variable.as_deref().unwrap_or("_");
            let label_str = label
                .as_deref()
                .map(|l| format!(":{l}"))
                .unwrap_or_default();
            format!(
                "Merge({var}{label_str} {{{}}})",
                properties.len()
            )
        }
    }
}

fn format_expr(expr: &crate::cypher::ast::Expr) -> String {
    use crate::cypher::ast::{BinOp, Expr, UnaryOp};
    match expr {
        Expr::Literal(lit) => format!("{:?}", lit),
        Expr::Variable(name) => name.clone(),
        Expr::Property(var, prop) => format!("{var}.{prop}"),
        Expr::FunctionCall(name, args) => {
            let arg_strs: Vec<String> = args.iter().map(format_expr).collect();
            format!("{name}({})", arg_strs.join(", "))
        }
        Expr::BinaryOp(l, op, r) => {
            let op_str = match op {
                BinOp::Eq => "=",
                BinOp::Neq => "<>",
                BinOp::Lt => "<",
                BinOp::Gt => ">",
                BinOp::Lte => "<=",
                BinOp::Gte => ">=",
                BinOp::And => "AND",
                BinOp::Or => "OR",
                BinOp::Add => "+",
                BinOp::Sub => "-",
                BinOp::Mul => "*",
                BinOp::Div => "/",
                BinOp::Mod => "%",
                BinOp::Contains => "CONTAINS",
                BinOp::StartsWith => "STARTS WITH",
                BinOp::EndsWith => "ENDS WITH",
                BinOp::In => "IN",
            };
            format!("{} {} {}", format_expr(l), op_str, format_expr(r))
        }
        Expr::UnaryOp(op, inner) => {
            let op_str = match op {
                UnaryOp::Not => "NOT",
                UnaryOp::Neg => "-",
            };
            format!("{op_str} {}", format_expr(inner))
        }
        Expr::Parameter(name) => format!("${name}"),
        Expr::Case { when_clauses, .. } => {
            format!("CASE[{}]", when_clauses.len())
        }
    }
}

fn estimate_rows(step: &PlanStep) -> String {
    match step {
        PlanStep::NodeScan { label: Some(_), .. } => "N/labels".into(),
        PlanStep::NodeScan { label: None, .. } => "N (all)".into(),
        PlanStep::Expand { .. } => "N*avg_degree".into(),
        PlanStep::Filter { .. } => "selectivity".into(),
        PlanStep::Limit { count } => format!("{count}"),
        PlanStep::Distinct => "<=input".into(),
        PlanStep::With { .. } => "<=input".into(),
        _ => "-".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cypher::{parser, planner};

    #[test]
    fn test_explain_simple_match() {
        let stmt = parser::parse("MATCH (a:Person) RETURN a.name").unwrap();
        let plan = planner::plan(&stmt).unwrap();
        let output = explain(&plan);
        assert!(output.contains("NodeScan(a:Person)"));
        assert!(output.contains("Project(a.name)"));
    }

    #[test]
    fn test_explain_match_with_rel() {
        let stmt = parser::parse(
            "MATCH (a:Person)-[:KNOWS]->(b) WHERE a.age > 25 RETURN b.name ORDER BY b.name DESC LIMIT 10"
        ).unwrap();
        let plan = planner::plan(&stmt).unwrap();
        let output = explain(&plan);
        assert!(output.contains("NodeScan"));
        assert!(output.contains("Expand"));
        assert!(output.contains(":KNOWS"));
        assert!(output.contains("Filter"));
        assert!(output.contains("OrderBy"));
        assert!(output.contains("Limit(10)"));
    }

    #[test]
    fn test_explain_create() {
        let stmt = parser::parse("CREATE (n:Person {name: 'Alice'})").unwrap();
        let plan = planner::plan(&stmt).unwrap();
        let output = explain(&plan);
        assert!(output.contains("CreateNode"));
    }

    #[test]
    fn test_explain_verbose() {
        let stmt = parser::parse("MATCH (a:Person) RETURN a.name LIMIT 5").unwrap();
        let plan = planner::plan(&stmt).unwrap();
        let rows = explain_verbose(&plan);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].operation, "NodeScan");
        assert_eq!(rows[1].operation, "Project");
        assert_eq!(rows[2].operation, "Limit");
    }

    #[test]
    fn test_explain_detach_delete() {
        let stmt = parser::parse("MATCH (n:Person) DETACH DELETE n").unwrap();
        let plan = planner::plan(&stmt).unwrap();
        let output = explain(&plan);
        assert!(output.contains("DetachDelete(n)"));
    }

    #[test]
    fn test_explain_merge() {
        let stmt = parser::parse("MERGE (n:Person {name: 'Alice'})").unwrap();
        let plan = planner::plan(&stmt).unwrap();
        let output = explain(&plan);
        assert!(output.contains("Merge(n:Person"));
    }

    #[test]
    fn test_explain_distinct() {
        let stmt = parser::parse("MATCH (a:Person) RETURN DISTINCT a.name").unwrap();
        let plan = planner::plan(&stmt).unwrap();
        let output = explain(&plan);
        assert!(output.contains("Distinct"));
    }
}
