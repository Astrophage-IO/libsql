use std::collections::HashMap;

use crate::cypher::ast::Expr;
use crate::cypher::planner::{PlanStep, QueryPlan};
use crate::storage::stats::GraphStats;

pub fn optimize(
    plan: QueryPlan,
    stats: &GraphStats,
    label_index_roots: &HashMap<u32, u32>,
    label_name_to_id: &HashMap<String, u32>,
) -> QueryPlan {
    let mut steps = plan.steps;
    steps = rewrite_label_scans(steps, label_index_roots, label_name_to_id);
    steps = push_filters_down(steps);
    steps = reorder_joins(steps, stats, label_name_to_id);
    QueryPlan { steps }
}

fn rewrite_label_scans(
    steps: Vec<PlanStep>,
    label_index_roots: &HashMap<u32, u32>,
    label_name_to_id: &HashMap<String, u32>,
) -> Vec<PlanStep> {
    steps
        .into_iter()
        .map(|step| match step {
            PlanStep::NodeScan {
                ref variable,
                label: Some(ref label),
                ref properties,
                optional,
            } => {
                if let Some(&token_id) = label_name_to_id.get(label.as_str()) {
                    if let Some(&root) = label_index_roots.get(&token_id) {
                        return PlanStep::IndexedNodeScan {
                            variable: variable.clone(),
                            label: label.clone(),
                            index_root: root,
                            properties: properties.clone(),
                            optional,
                        };
                    }
                }
                step
            }
            other => other,
        })
        .collect()
}

fn referenced_variables(expr: &Expr) -> Vec<String> {
    match expr {
        Expr::Variable(name) => vec![name.clone()],
        Expr::Property(var, _) => vec![var.clone()],
        Expr::BinaryOp(l, _, r) => {
            let mut vars = referenced_variables(l);
            vars.extend(referenced_variables(r));
            vars
        }
        Expr::UnaryOp(_, inner) => referenced_variables(inner),
        Expr::FunctionCall(_, args) => args.iter().flat_map(referenced_variables).collect(),
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            let mut vars = Vec::new();
            if let Some(op) = operand {
                vars.extend(referenced_variables(op));
            }
            for (cond, result) in when_clauses {
                vars.extend(referenced_variables(cond));
                vars.extend(referenced_variables(result));
            }
            if let Some(el) = else_clause {
                vars.extend(referenced_variables(el));
            }
            vars
        }
        Expr::Literal(_) | Expr::Parameter(_) => vec![],
    }
}

fn variables_bound_by(step: &PlanStep) -> Vec<String> {
    match step {
        PlanStep::NodeScan { variable, .. } | PlanStep::IndexedNodeScan { variable, .. } => {
            if variable.is_empty() {
                vec![]
            } else {
                vec![variable.clone()]
            }
        }
        PlanStep::Expand {
            to_var, rel_var, ..
        } => {
            let mut vars = Vec::new();
            if !to_var.is_empty() {
                vars.push(to_var.clone());
            }
            if let Some(rv) = rel_var {
                if !rv.is_empty() {
                    vars.push(rv.clone());
                }
            }
            vars
        }
        PlanStep::Unwind { variable, .. } => vec![variable.clone()],
        PlanStep::CreateNode {
            variable: Some(v), ..
        } => vec![v.clone()],
        PlanStep::Merge {
            variable: Some(v), ..
        } => vec![v.clone()],
        _ => vec![],
    }
}

fn push_filters_down(steps: Vec<PlanStep>) -> Vec<PlanStep> {
    let mut result = steps;
    let mut changed = true;
    while changed {
        changed = false;
        let mut i = 1;
        while i < result.len() {
            let is_filter = matches!(&result[i], PlanStep::Filter { .. });
            if !is_filter {
                i += 1;
                continue;
            }

            let filter_vars = if let PlanStep::Filter { predicate } = &result[i] {
                referenced_variables(predicate)
            } else {
                unreachable!()
            };

            let mut target = i;
            for j in (0..i).rev() {
                let bound = variables_bound_by(&result[j]);
                if filter_vars.iter().any(|v| bound.contains(v)) {
                    break;
                }
                let is_data_step = matches!(
                    &result[j],
                    PlanStep::Expand { .. }
                        | PlanStep::NodeScan { .. }
                        | PlanStep::IndexedNodeScan { .. }
                );
                if !is_data_step {
                    break;
                }
                target = j;
            }

            if target < i {
                let filter = result.remove(i);
                result.insert(target, filter);
                changed = true;
            }
            i += 1;
        }
    }
    result
}

fn is_scan_step(step: &PlanStep) -> bool {
    matches!(
        step,
        PlanStep::NodeScan { .. } | PlanStep::IndexedNodeScan { .. }
    )
}

fn scan_label_name(step: &PlanStep) -> Option<&str> {
    match step {
        PlanStep::NodeScan { label: Some(l), .. } => Some(l.as_str()),
        PlanStep::IndexedNodeScan { label, .. } => Some(label.as_str()),
        _ => None,
    }
}

fn scan_variable(step: &PlanStep) -> Option<&str> {
    match step {
        PlanStep::NodeScan { variable, .. } | PlanStep::IndexedNodeScan { variable, .. } => {
            Some(variable.as_str())
        }
        _ => None,
    }
}

struct ScanChain {
    scan_idx: usize,
    expand_indices: Vec<usize>,
    label_count: u64,
}

fn reorder_joins(
    steps: Vec<PlanStep>,
    stats: &GraphStats,
    label_name_to_id: &HashMap<String, u32>,
) -> Vec<PlanStep> {
    let mut chains: Vec<ScanChain> = Vec::new();
    let mut i = 0;
    while i < steps.len() {
        if is_scan_step(&steps[i]) {
            let label_count = scan_label_name(&steps[i])
                .and_then(|name| label_name_to_id.get(name))
                .map(|id| stats.label_count_for_id(*id))
                .unwrap_or(stats.node_count);
            let mut chain = ScanChain {
                scan_idx: i,
                expand_indices: Vec::new(),
                label_count,
            };
            let mut j = i + 1;
            while j < steps.len() {
                if matches!(&steps[j], PlanStep::Expand { .. }) {
                    chain.expand_indices.push(j);
                    j += 1;
                } else {
                    break;
                }
            }
            chains.push(chain);
            i = j;
        } else {
            i += 1;
        }
    }

    if chains.len() < 2 {
        return steps;
    }

    let connected = chains.len() == 2 && {
        let last_expand_of_first = chains[0].expand_indices.last().copied();
        if let Some(exp_idx) = last_expand_of_first {
            if let PlanStep::Expand { to_var, .. } = &steps[exp_idx] {
                scan_variable(&steps[chains[1].scan_idx]) == Some(to_var.as_str())
            } else {
                false
            }
        } else {
            false
        }
    };

    if !connected {
        return steps;
    }

    if chains[0].label_count <= chains[1].label_count {
        return steps;
    }

    let mut result = Vec::with_capacity(steps.len());
    let mut used = vec![false; steps.len()];

    let chain1_indices: Vec<usize> = std::iter::once(chains[1].scan_idx)
        .chain(chains[1].expand_indices.iter().copied())
        .collect();
    let chain0_indices: Vec<usize> = std::iter::once(chains[0].scan_idx)
        .chain(chains[0].expand_indices.iter().copied())
        .collect();

    for &idx in &chain1_indices {
        used[idx] = true;
        result.push(steps[idx].clone());
    }
    for &idx in &chain0_indices {
        used[idx] = true;
        result.push(steps[idx].clone());
    }

    for (idx, step) in steps.into_iter().enumerate() {
        if !used[idx] {
            result.push(step);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cypher::ast::*;
    use crate::graph::Direction;

    #[test]
    fn test_rewrite_label_scans_with_index() {
        let plan = QueryPlan {
            steps: vec![
                PlanStep::NodeScan {
                    variable: "a".into(),
                    label: Some("Person".into()),
                    properties: vec![],
                    optional: false,
                },
                PlanStep::Project {
                    items: vec![ReturnItem {
                        expr: Expr::Variable("a".into()),
                        alias: None,
                    }],
                },
            ],
        };

        let mut roots = HashMap::new();
        roots.insert(0u32, 10u32);
        let mut names = HashMap::new();
        names.insert("Person".to_string(), 0u32);

        let optimized = optimize(plan, &GraphStats::new(), &roots, &names);
        assert!(matches!(
            &optimized.steps[0],
            PlanStep::IndexedNodeScan {
                label, index_root, ..
            } if label == "Person" && *index_root == 10
        ));
    }

    #[test]
    fn test_no_index_fallback() {
        let plan = QueryPlan {
            steps: vec![
                PlanStep::NodeScan {
                    variable: "a".into(),
                    label: Some("Person".into()),
                    properties: vec![],
                    optional: false,
                },
                PlanStep::Project {
                    items: vec![ReturnItem {
                        expr: Expr::Variable("a".into()),
                        alias: None,
                    }],
                },
            ],
        };

        let roots = HashMap::new();
        let names = HashMap::new();

        let optimized = optimize(plan, &GraphStats::new(), &roots, &names);
        assert!(matches!(&optimized.steps[0], PlanStep::NodeScan { .. }));
    }

    #[test]
    fn test_push_filter_before_expand() {
        let plan = QueryPlan {
            steps: vec![
                PlanStep::NodeScan {
                    variable: "a".into(),
                    label: Some("Person".into()),
                    properties: vec![],
                    optional: false,
                },
                PlanStep::Expand {
                    from_var: "a".into(),
                    rel_var: None,
                    to_var: "b".into(),
                    rel_type: Some("KNOWS".into()),
                    direction: Direction::Outgoing,
                    min_hops: None,
                    max_hops: None,
                    optional: false,
                },
                PlanStep::Filter {
                    predicate: Expr::BinaryOp(
                        Box::new(Expr::Property("a".into(), "age".into())),
                        BinOp::Gt,
                        Box::new(Expr::Literal(Literal::Integer(30))),
                    ),
                },
                PlanStep::Project {
                    items: vec![ReturnItem {
                        expr: Expr::Variable("b".into()),
                        alias: None,
                    }],
                },
            ],
        };

        let optimized = optimize(plan, &GraphStats::new(), &HashMap::new(), &HashMap::new());
        assert!(matches!(&optimized.steps[0], PlanStep::NodeScan { .. }));
        assert!(matches!(&optimized.steps[1], PlanStep::Filter { .. }));
        assert!(matches!(&optimized.steps[2], PlanStep::Expand { .. }));
    }

    #[test]
    fn test_empty_stats_no_crash() {
        let plan = QueryPlan {
            steps: vec![
                PlanStep::NodeScan {
                    variable: "a".into(),
                    label: Some("Person".into()),
                    properties: vec![],
                    optional: false,
                },
                PlanStep::Project {
                    items: vec![ReturnItem {
                        expr: Expr::Variable("a".into()),
                        alias: None,
                    }],
                },
            ],
        };

        let stats = GraphStats::new();
        let optimized = optimize(plan, &stats, &HashMap::new(), &HashMap::new());
        assert_eq!(optimized.steps.len(), 2);
    }

    #[test]
    fn test_join_reorder_fewer_nodes_first() {
        let mut stats = GraphStats::new();
        stats.node_count = 1005;
        stats.label_counts.insert(0, 1000);
        stats.label_counts.insert(1, 5);

        let mut label_names = HashMap::new();
        label_names.insert("Person".to_string(), 0u32);
        label_names.insert("City".to_string(), 1u32);

        let plan = QueryPlan {
            steps: vec![
                PlanStep::NodeScan {
                    variable: "a".into(),
                    label: Some("Person".into()),
                    properties: vec![],
                    optional: false,
                },
                PlanStep::Expand {
                    from_var: "a".into(),
                    rel_var: None,
                    to_var: "c".into(),
                    rel_type: Some("LIVES_IN".into()),
                    direction: Direction::Outgoing,
                    min_hops: None,
                    max_hops: None,
                    optional: false,
                },
                PlanStep::NodeScan {
                    variable: "c".into(),
                    label: Some("City".into()),
                    properties: vec![],
                    optional: false,
                },
                PlanStep::Project {
                    items: vec![ReturnItem {
                        expr: Expr::Variable("a".into()),
                        alias: None,
                    }],
                },
            ],
        };

        let optimized = optimize(plan, &stats, &HashMap::new(), &label_names);
        assert!(matches!(
            &optimized.steps[0],
            PlanStep::NodeScan { label: Some(l), .. } if l == "City"
        ));
    }

    #[test]
    fn test_filter_not_pushed_past_binding_step() {
        let plan = QueryPlan {
            steps: vec![
                PlanStep::NodeScan {
                    variable: "a".into(),
                    label: Some("Person".into()),
                    properties: vec![],
                    optional: false,
                },
                PlanStep::Expand {
                    from_var: "a".into(),
                    rel_var: None,
                    to_var: "b".into(),
                    rel_type: Some("KNOWS".into()),
                    direction: Direction::Outgoing,
                    min_hops: None,
                    max_hops: None,
                    optional: false,
                },
                PlanStep::Filter {
                    predicate: Expr::BinaryOp(
                        Box::new(Expr::Property("b".into(), "name".into())),
                        BinOp::Eq,
                        Box::new(Expr::Literal(Literal::String("Bob".into()))),
                    ),
                },
                PlanStep::Project {
                    items: vec![ReturnItem {
                        expr: Expr::Variable("b".into()),
                        alias: None,
                    }],
                },
            ],
        };

        let optimized = optimize(plan, &GraphStats::new(), &HashMap::new(), &HashMap::new());
        assert!(matches!(&optimized.steps[0], PlanStep::NodeScan { .. }));
        assert!(matches!(&optimized.steps[1], PlanStep::Expand { .. }));
        assert!(matches!(&optimized.steps[2], PlanStep::Filter { .. }));
    }
}
