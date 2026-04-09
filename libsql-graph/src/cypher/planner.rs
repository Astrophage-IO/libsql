use crate::cypher::ast::*;
use crate::graph::Direction;

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub steps: Vec<PlanStep>,
}

#[derive(Debug, Clone)]
pub enum PlanStep {
    NodeScan {
        variable: String,
        label: Option<String>,
        properties: Vec<(String, Literal)>,
        optional: bool,
    },
    Expand {
        from_var: String,
        rel_var: Option<String>,
        to_var: String,
        rel_type: Option<String>,
        direction: Direction,
        min_hops: Option<u32>,
        max_hops: Option<u32>,
    },
    Filter {
        predicate: Expr,
    },
    CreateNode {
        variable: Option<String>,
        label: Option<String>,
        properties: Vec<(String, Literal)>,
    },
    CreateRelationship {
        from_var: String,
        rel_type: String,
        to_var: String,
        properties: Vec<(String, Literal)>,
    },
    SetProperty {
        variable: String,
        property: String,
        value: Expr,
    },
    DeleteNode {
        variable: String,
        detach: bool,
    },
    Project {
        items: Vec<ReturnItem>,
    },
    OrderBy {
        items: Vec<OrderItem>,
    },
    Limit {
        count: u64,
    },
    Distinct,
    With {
        items: Vec<ReturnItem>,
        where_clause: Option<Expr>,
    },
    Unwind {
        expr: Expr,
        variable: String,
    },
    Merge {
        variable: Option<String>,
        label: Option<String>,
        properties: Vec<(String, Literal)>,
        on_create_set: Vec<SetClause>,
        on_match_set: Vec<SetClause>,
    },
}

pub fn plan(stmt: &Statement) -> Result<QueryPlan, String> {
    match stmt {
        Statement::Match(m) => plan_match(m),
        Statement::Create(c) => plan_create(c),
        Statement::Delete(d) => plan_delete(d),
        Statement::Merge(m) => plan_merge(m),
        Statement::Unwind(u) => plan_unwind(u),
    }
}

fn plan_match(m: &MatchStatement) -> Result<QueryPlan, String> {
    let mut steps = Vec::new();

    let mut iter = m.pattern.elements.iter();
    if let Some(PatternElement::Node(node)) = iter.next() {
        steps.push(PlanStep::NodeScan {
            variable: node.variable.clone().unwrap_or_default(),
            label: node.label.clone(),
            properties: node.properties.clone(),
            optional: m.optional,
        });
    } else {
        return Err("pattern must start with a node".into());
    }

    while let Some(elem) = iter.next() {
        if let PatternElement::Relationship(rel) = elem {
            let to_node = iter.next().ok_or("relationship must be followed by a node")?;
            if let PatternElement::Node(node) = to_node {
                let direction = match rel.direction {
                    RelDirection::Outgoing => Direction::Outgoing,
                    RelDirection::Incoming => Direction::Incoming,
                    RelDirection::Both => Direction::Both,
                };

                let from_var = steps.iter().rev().find_map(|s| match s {
                    PlanStep::NodeScan { variable, .. } => Some(variable.clone()),
                    PlanStep::Expand { to_var, .. } => Some(to_var.clone()),
                    _ => None,
                }).unwrap_or_default();

                steps.push(PlanStep::Expand {
                    from_var,
                    rel_var: rel.variable.clone(),
                    to_var: node.variable.clone().unwrap_or_default(),
                    rel_type: rel.rel_type.clone(),
                    direction,
                    min_hops: rel.min_hops,
                    max_hops: rel.max_hops,
                });
            } else {
                return Err("expected node after relationship".into());
            }
        }
    }

    if let Some(ref where_expr) = m.where_clause {
        steps.push(PlanStep::Filter {
            predicate: where_expr.clone(),
        });
    }

    if let Some(ref with) = m.with_clause {
        steps.push(PlanStep::With {
            items: with.items.clone(),
            where_clause: with.where_clause.clone(),
        });
    }

    if let Some(ref next) = m.next_match {
        let next_plan = plan_match(next)?;
        steps.extend(next_plan.steps);
    }

    for set_clause in &m.set_clauses {
        steps.push(PlanStep::SetProperty {
            variable: set_clause.variable.clone(),
            property: set_clause.property.clone(),
            value: set_clause.value.clone(),
        });
    }

    if let Some(ref delete) = m.delete {
        for var in &delete.variables {
            steps.push(PlanStep::DeleteNode {
                variable: var.clone(),
                detach: delete.detach,
            });
        }
    }

    if let Some(ref ret) = m.return_clause {
        steps.push(PlanStep::Project {
            items: ret.items.clone(),
        });
        if ret.distinct {
            steps.push(PlanStep::Distinct);
        }
        if let Some(ref order) = ret.order_by {
            steps.push(PlanStep::OrderBy {
                items: order.clone(),
            });
        }
        if let Some(limit) = ret.limit {
            steps.push(PlanStep::Limit { count: limit });
        }
    }

    Ok(QueryPlan { steps })
}

fn plan_create(c: &CreateStatement) -> Result<QueryPlan, String> {
    let mut steps = Vec::new();

    for elem in &c.elements {
        match elem {
            CreateElement::Node {
                variable,
                label,
                properties,
            } => {
                steps.push(PlanStep::CreateNode {
                    variable: variable.clone(),
                    label: label.clone(),
                    properties: properties.clone(),
                });
            }
            CreateElement::Relationship {
                from_var,
                rel_type,
                to_var,
                properties,
            } => {
                steps.push(PlanStep::CreateRelationship {
                    from_var: from_var.clone(),
                    rel_type: rel_type.clone(),
                    to_var: to_var.clone(),
                    properties: properties.clone(),
                });
            }
        }
    }

    if let Some(ref ret) = c.return_clause {
        steps.push(PlanStep::Project {
            items: ret.items.clone(),
        });
    }

    Ok(QueryPlan { steps })
}

fn plan_unwind(u: &UnwindStatement) -> Result<QueryPlan, String> {
    let mut steps = vec![PlanStep::Unwind {
        expr: u.expr.clone(),
        variable: u.variable.clone(),
    }];
    if let Some(ref ret) = u.return_clause {
        steps.push(PlanStep::Project {
            items: ret.items.clone(),
        });
    }
    Ok(QueryPlan { steps })
}

fn plan_merge(m: &MergeStatement) -> Result<QueryPlan, String> {
    let mut steps = Vec::new();
    steps.push(PlanStep::Merge {
        variable: m.pattern.variable.clone(),
        label: m.pattern.label.clone(),
        properties: m.pattern.properties.clone(),
        on_create_set: m.on_create_set.clone(),
        on_match_set: m.on_match_set.clone(),
    });

    if let Some(ref ret) = m.return_clause {
        steps.push(PlanStep::Project {
            items: ret.items.clone(),
        });
    }

    Ok(QueryPlan { steps })
}

fn plan_delete(d: &DeleteStatement) -> Result<QueryPlan, String> {
    let mut steps = Vec::new();
    for var in &d.variables {
        steps.push(PlanStep::DeleteNode {
            variable: var.clone(),
            detach: d.detach,
        });
    }
    Ok(QueryPlan { steps })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cypher::parser::parse;

    #[test]
    fn test_plan_simple_match() {
        let stmt = parse("MATCH (a:Person) RETURN a").unwrap();
        let plan = plan(&stmt).unwrap();
        assert_eq!(plan.steps.len(), 2);
        assert!(matches!(&plan.steps[0], PlanStep::NodeScan { label: Some(l), .. } if l == "Person"));
        assert!(matches!(&plan.steps[1], PlanStep::Project { .. }));
    }

    #[test]
    fn test_plan_match_with_relationship() {
        let stmt = parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name").unwrap();
        let plan = plan(&stmt).unwrap();
        assert_eq!(plan.steps.len(), 3);
        assert!(matches!(&plan.steps[0], PlanStep::NodeScan { .. }));
        assert!(matches!(&plan.steps[1], PlanStep::Expand { rel_type: Some(t), .. } if t == "KNOWS"));
        assert!(matches!(&plan.steps[2], PlanStep::Project { .. }));
    }

    #[test]
    fn test_plan_match_with_where() {
        let stmt = parse("MATCH (a:Person) WHERE a.age > 25 RETURN a").unwrap();
        let plan = plan(&stmt).unwrap();
        assert_eq!(plan.steps.len(), 3);
        assert!(matches!(&plan.steps[1], PlanStep::Filter { .. }));
    }

    #[test]
    fn test_plan_match_with_set() {
        let stmt = parse("MATCH (n:Person) SET n.age = 30 RETURN n").unwrap();
        let plan = plan(&stmt).unwrap();
        assert!(plan.steps.iter().any(|s| matches!(s, PlanStep::SetProperty { property, .. } if property == "age")));
    }

    #[test]
    fn test_plan_create_node() {
        let stmt = parse("CREATE (n:Person {name: 'Alice'})").unwrap();
        let plan = plan(&stmt).unwrap();
        assert_eq!(plan.steps.len(), 1);
        assert!(matches!(&plan.steps[0], PlanStep::CreateNode { label: Some(l), .. } if l == "Person"));
    }

    #[test]
    fn test_plan_detach_delete() {
        let stmt = parse("MATCH (n:Person) DETACH DELETE n").unwrap();
        let plan = plan(&stmt).unwrap();
        assert!(plan.steps.iter().any(|s| matches!(s, PlanStep::DeleteNode { detach: true, .. })));
    }

    #[test]
    fn test_plan_order_limit() {
        let stmt = parse("MATCH (a:Person) RETURN a.name ORDER BY a.age DESC LIMIT 10").unwrap();
        let plan = plan(&stmt).unwrap();
        assert!(plan.steps.iter().any(|s| matches!(s, PlanStep::OrderBy { .. })));
        assert!(plan.steps.iter().any(|s| matches!(s, PlanStep::Limit { count: 10 })));
    }

    #[test]
    fn test_plan_variable_length_expand() {
        let stmt = parse("MATCH (a)-[:KNOWS*1..3]->(b) RETURN b").unwrap();
        let plan = plan(&stmt).unwrap();
        if let PlanStep::Expand { min_hops, max_hops, .. } = &plan.steps[1] {
            assert_eq!(*min_hops, Some(1));
            assert_eq!(*max_hops, Some(3));
        } else {
            panic!("expected Expand step");
        }
    }
}
