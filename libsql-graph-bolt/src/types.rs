use std::collections::HashMap;

use libsql_graph::cypher::executor::{QueryStats, Value};
use libsql_graph::error::GraphError;

use crate::packstream::PackValue;

pub fn graph_value_to_pack(value: &Value) -> PackValue {
    match value {
        Value::Null => PackValue::Null,
        Value::Bool(b) => PackValue::Bool(*b),
        Value::Integer(n) => PackValue::Int(*n),
        Value::Float(f) => PackValue::Float(*f),
        Value::String(s) => PackValue::String(s.clone()),
        Value::List(items) => PackValue::List(items.iter().map(graph_value_to_pack).collect()),
        Value::Node(id) => PackValue::Int(*id as i64),
        Value::Rel(id) => PackValue::Int(*id as i64),
    }
}

pub fn pack_to_param_value(value: &PackValue) -> Value {
    match value {
        PackValue::Null => Value::Null,
        PackValue::Bool(b) => Value::Bool(*b),
        PackValue::Int(n) => Value::Integer(*n),
        PackValue::Float(f) => Value::Float(*f),
        PackValue::String(s) => Value::String(s.clone()),
        PackValue::List(items) => Value::List(items.iter().map(pack_to_param_value).collect()),
        PackValue::Map(_) | PackValue::Struct { .. } | PackValue::Bytes(_) => Value::Null,
    }
}

pub fn pack_params_to_hashmap(params: &HashMap<String, PackValue>) -> HashMap<String, Value> {
    params
        .iter()
        .map(|(k, v)| (k.clone(), pack_to_param_value(v)))
        .collect()
}

pub fn query_stats_to_map(stats: &QueryStats) -> HashMap<String, PackValue> {
    let mut map = HashMap::new();
    if stats.nodes_created > 0 {
        map.insert(
            "nodes-created".into(),
            PackValue::Int(stats.nodes_created as i64),
        );
    }
    if stats.relationships_created > 0 {
        map.insert(
            "relationships-created".into(),
            PackValue::Int(stats.relationships_created as i64),
        );
    }
    if stats.properties_set > 0 {
        map.insert(
            "properties-set".into(),
            PackValue::Int(stats.properties_set as i64),
        );
    }
    if stats.nodes_deleted > 0 {
        map.insert(
            "nodes-deleted".into(),
            PackValue::Int(stats.nodes_deleted as i64),
        );
    }
    map
}

pub fn graph_error_to_bolt(err: &GraphError) -> (String, String) {
    match err {
        GraphError::QueryParse(msg) => {
            ("Neo.ClientError.Statement.SyntaxError".into(), msg.clone())
        }
        GraphError::QueryPlan(msg) => ("Neo.ClientError.Statement.SyntaxError".into(), msg.clone()),
        GraphError::QueryExec(msg) => (
            "Neo.ClientError.Statement.ExecutionFailed".into(),
            msg.clone(),
        ),
        GraphError::NodeNotFound(id) => (
            "Neo.ClientError.Statement.EntityNotFound".into(),
            format!("Node not found: {}", id),
        ),
        GraphError::RelNotFound(id) => (
            "Neo.ClientError.Statement.EntityNotFound".into(),
            format!("Relationship not found: {}", id),
        ),
        GraphError::ConstraintViolation(msg) => (
            "Neo.ClientError.Schema.ConstraintValidationFailed".into(),
            msg.clone(),
        ),
        GraphError::NoTransaction => (
            "Neo.ClientError.Transaction.TransactionNotFound".into(),
            "No active transaction".into(),
        ),
        GraphError::TransactionActive => (
            "Neo.ClientError.Transaction.TransactionNotFound".into(),
            "Transaction already active".into(),
        ),
        other => (
            "Neo.DatabaseError.General.UnknownError".into(),
            format!("{}", other),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_null_to_pack() {
        assert_eq!(graph_value_to_pack(&Value::Null), PackValue::Null);
    }

    #[test]
    fn graph_bool_to_pack() {
        assert_eq!(
            graph_value_to_pack(&Value::Bool(true)),
            PackValue::Bool(true)
        );
        assert_eq!(
            graph_value_to_pack(&Value::Bool(false)),
            PackValue::Bool(false)
        );
    }

    #[test]
    fn graph_integer_to_pack() {
        assert_eq!(graph_value_to_pack(&Value::Integer(42)), PackValue::Int(42));
        assert_eq!(graph_value_to_pack(&Value::Integer(-1)), PackValue::Int(-1));
    }

    #[test]
    fn graph_float_to_pack() {
        assert_eq!(
            graph_value_to_pack(&Value::Float(3.14)),
            PackValue::Float(3.14)
        );
    }

    #[test]
    fn graph_string_to_pack() {
        assert_eq!(
            graph_value_to_pack(&Value::String("hello".into())),
            PackValue::String("hello".into())
        );
    }

    #[test]
    fn graph_list_to_pack() {
        let val = Value::List(vec![Value::Integer(1), Value::String("two".into())]);
        let expected = PackValue::List(vec![PackValue::Int(1), PackValue::String("two".into())]);
        assert_eq!(graph_value_to_pack(&val), expected);
    }

    #[test]
    fn graph_node_to_pack_int() {
        assert_eq!(graph_value_to_pack(&Value::Node(7)), PackValue::Int(7));
    }

    #[test]
    fn graph_rel_to_pack_int() {
        assert_eq!(graph_value_to_pack(&Value::Rel(99)), PackValue::Int(99));
    }

    #[test]
    fn pack_null_to_param() {
        assert!(matches!(pack_to_param_value(&PackValue::Null), Value::Null));
    }

    #[test]
    fn pack_bool_to_param() {
        assert!(matches!(
            pack_to_param_value(&PackValue::Bool(true)),
            Value::Bool(true)
        ));
    }

    #[test]
    fn pack_int_to_param() {
        assert!(matches!(
            pack_to_param_value(&PackValue::Int(42)),
            Value::Integer(42)
        ));
    }

    #[test]
    fn pack_float_to_param() {
        assert!(matches!(pack_to_param_value(&PackValue::Float(1.5)), Value::Float(f) if f == 1.5));
    }

    #[test]
    fn pack_string_to_param() {
        match pack_to_param_value(&PackValue::String("x".into())) {
            Value::String(s) => assert_eq!(s, "x"),
            _ => panic!("expected String"),
        }
    }

    #[test]
    fn pack_list_to_param() {
        let val = PackValue::List(vec![PackValue::Int(1)]);
        match pack_to_param_value(&val) {
            Value::List(items) => {
                assert_eq!(items.len(), 1);
                assert!(matches!(items[0], Value::Integer(1)));
            }
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn pack_map_to_param_returns_null() {
        assert!(matches!(
            pack_to_param_value(&PackValue::Map(vec![])),
            Value::Null
        ));
    }

    #[test]
    fn pack_bytes_to_param_returns_null() {
        assert!(matches!(
            pack_to_param_value(&PackValue::Bytes(vec![1])),
            Value::Null
        ));
    }

    #[test]
    fn pack_struct_to_param_returns_null() {
        let val = PackValue::Struct {
            tag: 0x4E,
            fields: vec![],
        };
        assert!(matches!(pack_to_param_value(&val), Value::Null));
    }

    #[test]
    fn params_hashmap_conversion() {
        let mut params = HashMap::new();
        params.insert("name".into(), PackValue::String("Alice".into()));
        params.insert("age".into(), PackValue::Int(30));
        let result = pack_params_to_hashmap(&params);
        assert_eq!(result.len(), 2);
        assert!(matches!(result.get("name"), Some(Value::String(s)) if s == "Alice"));
        assert!(matches!(result.get("age"), Some(Value::Integer(30))));
    }

    #[test]
    fn stats_map_omits_zeros() {
        let stats = QueryStats::default();
        let map = query_stats_to_map(&stats);
        assert!(map.is_empty());
    }

    #[test]
    fn stats_map_includes_nonzero() {
        let stats = QueryStats {
            nodes_created: 2,
            relationships_created: 0,
            properties_set: 3,
            nodes_deleted: 0,
        };
        let map = query_stats_to_map(&stats);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("nodes-created"), Some(&PackValue::Int(2)));
        assert_eq!(map.get("properties-set"), Some(&PackValue::Int(3)));
    }

    #[test]
    fn error_mapping_query_parse() {
        let (code, _msg) = graph_error_to_bolt(&GraphError::QueryParse("bad syntax".into()));
        assert_eq!(code, "Neo.ClientError.Statement.SyntaxError");
    }

    #[test]
    fn error_mapping_node_not_found() {
        let (code, msg) = graph_error_to_bolt(&GraphError::NodeNotFound(42));
        assert_eq!(code, "Neo.ClientError.Statement.EntityNotFound");
        assert!(msg.contains("42"));
    }

    #[test]
    fn error_mapping_constraint_violation() {
        let (code, _msg) = graph_error_to_bolt(&GraphError::ConstraintViolation("dup".into()));
        assert_eq!(code, "Neo.ClientError.Schema.ConstraintValidationFailed");
    }

    #[test]
    fn error_mapping_no_transaction() {
        let (code, _msg) = graph_error_to_bolt(&GraphError::NoTransaction);
        assert_eq!(code, "Neo.ClientError.Transaction.TransactionNotFound");
    }

    #[test]
    fn error_mapping_io() {
        let err = GraphError::IoError(std::io::Error::new(std::io::ErrorKind::Other, "disk fail"));
        let (code, _msg) = graph_error_to_bolt(&err);
        assert_eq!(code, "Neo.DatabaseError.General.UnknownError");
    }
}
