use crate::cypher::planner::PlanStep;
use crate::storage::stats::GraphStats;

#[derive(Debug, Clone)]
pub struct CostEstimate {
    pub estimated_rows: f64,
    pub estimated_cost: f64,
}

pub fn estimate_step_cost(step: &PlanStep, stats: &GraphStats) -> CostEstimate {
    match step {
        PlanStep::NodeScan { label: Some(_), .. } => {
            let est = (stats.node_count as f64 * 0.1).max(1.0);
            CostEstimate {
                estimated_rows: est,
                estimated_cost: est,
            }
        }
        PlanStep::NodeScan { label: None, .. } => CostEstimate {
            estimated_rows: stats.node_count as f64,
            estimated_cost: stats.node_count as f64,
        },
        PlanStep::Expand { .. } => {
            let avg_deg = stats.avg_degree();
            CostEstimate {
                estimated_rows: avg_deg,
                estimated_cost: avg_deg,
            }
        }
        PlanStep::IndexedNodeScan { .. } => {
            let est = (stats.node_count as f64 * 0.1).max(1.0);
            CostEstimate {
                estimated_rows: est,
                estimated_cost: est * 0.5,
            }
        }
        PlanStep::Filter { .. } => CostEstimate {
            estimated_rows: 0.0,
            estimated_cost: 0.1,
        },
        _ => CostEstimate {
            estimated_rows: 0.0,
            estimated_cost: 0.0,
        },
    }
}

pub fn estimate_step_cost_with_label_id(
    step: &PlanStep,
    stats: &GraphStats,
    label_id: Option<u32>,
) -> CostEstimate {
    match step {
        PlanStep::NodeScan { label: Some(_), .. } => {
            let count = label_id
                .map(|id| stats.label_count_for_id(id))
                .unwrap_or(0);
            CostEstimate {
                estimated_rows: count as f64,
                estimated_cost: count as f64,
            }
        }
        _ => estimate_step_cost(step, stats),
    }
}

pub fn estimate_plan_cost(steps: &[PlanStep], stats: &GraphStats) -> f64 {
    let mut total = 0.0;
    let mut cardinality = 1.0_f64;

    for step in steps {
        let est = estimate_step_cost(step, stats);
        let step_cost = est.estimated_cost * cardinality.max(1.0);
        total += step_cost;
        if est.estimated_rows > 0.0 {
            cardinality *= est.estimated_rows;
        }
    }

    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cypher::planner::PlanStep;
    use crate::graph::Direction;

    #[test]
    fn test_cost_node_scan_with_label() {
        let mut stats = GraphStats::new();
        stats.node_count = 200;
        stats.label_counts.insert(0, 100);

        let step = PlanStep::NodeScan {
            variable: "a".into(),
            label: Some("Person".into()),
            properties: vec![],
            optional: false,
        };

        let est = estimate_step_cost_with_label_id(&step, &stats, Some(0));
        assert!((est.estimated_cost - 100.0).abs() < f64::EPSILON);
        assert!((est.estimated_rows - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cost_node_scan_no_label() {
        let mut stats = GraphStats::new();
        stats.node_count = 200;

        let step = PlanStep::NodeScan {
            variable: "a".into(),
            label: None,
            properties: vec![],
            optional: false,
        };

        let est = estimate_step_cost(&step, &stats);
        assert!((est.estimated_cost - 200.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cost_expand() {
        let mut stats = GraphStats::new();
        stats.node_count = 100;
        stats.edge_count = 200;

        let step = PlanStep::Expand {
            from_var: "a".into(),
            rel_var: None,
            to_var: "b".into(),
            rel_type: None,
            direction: Direction::Outgoing,
            min_hops: None,
            max_hops: None,
            optional: false,
        };

        let est = estimate_step_cost(&step, &stats);
        assert!((est.estimated_cost - 2.0).abs() < f64::EPSILON);
        assert!((est.estimated_rows - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cost_filter() {
        let stats = GraphStats::new();
        let step = PlanStep::Filter {
            predicate: crate::cypher::ast::Expr::Literal(crate::cypher::ast::Literal::Bool(true)),
        };

        let est = estimate_step_cost(&step, &stats);
        assert!((est.estimated_cost - 0.1).abs() < f64::EPSILON);
    }
}
