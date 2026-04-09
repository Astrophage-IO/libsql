use std::collections::HashMap;
use crate::error::BoltError;
use crate::packstream::PackValue;

#[derive(Debug, Clone, PartialEq)]
pub enum BoltRequest {
    Hello { extra: HashMap<String, PackValue> },
    Goodbye,
    Reset,
    Run { query: String, params: HashMap<String, PackValue>, extra: HashMap<String, PackValue> },
    Begin { extra: HashMap<String, PackValue> },
    Commit,
    Rollback,
    Discard { n: i64, qid: i64 },
    Pull { n: i64, qid: i64 },
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoltResponse {
    Success { metadata: HashMap<String, PackValue> },
    Record { data: Vec<PackValue> },
    Ignored,
    Failure { code: String, message: String },
}

fn pack_map_to_hashmap(value: PackValue) -> Result<HashMap<String, PackValue>, BoltError> {
    match value {
        PackValue::Map(pairs) => {
            let mut map = HashMap::with_capacity(pairs.len());
            for (k, v) in pairs {
                map.insert(k, v);
            }
            Ok(map)
        }
        _ => Err(BoltError::Protocol("expected Map value".into())),
    }
}

fn extract_n_qid(extra: &HashMap<String, PackValue>) -> (i64, i64) {
    let n = match extra.get("n") {
        Some(PackValue::Int(v)) => *v,
        _ => -1,
    };
    let qid = match extra.get("qid") {
        Some(PackValue::Int(v)) => *v,
        _ => -1,
    };
    (n, qid)
}

impl BoltRequest {
    pub fn parse(value: PackValue) -> Result<BoltRequest, BoltError> {
        match value {
            PackValue::Struct { tag, fields } => {
                match tag {
                    0x01 => {
                        let extra = match fields.into_iter().next() {
                            Some(v) => pack_map_to_hashmap(v)?,
                            None => HashMap::new(),
                        };
                        Ok(BoltRequest::Hello { extra })
                    }
                    0x02 => Ok(BoltRequest::Goodbye),
                    0x0F => Ok(BoltRequest::Reset),
                    0x10 => {
                        let mut iter = fields.into_iter();
                        let query = match iter.next() {
                            Some(PackValue::String(s)) => s,
                            _ => return Err(BoltError::Protocol("RUN: expected query string".into())),
                        };
                        let params = match iter.next() {
                            Some(v) => pack_map_to_hashmap(v)?,
                            None => HashMap::new(),
                        };
                        let extra = match iter.next() {
                            Some(v) => pack_map_to_hashmap(v)?,
                            None => HashMap::new(),
                        };
                        Ok(BoltRequest::Run { query, params, extra })
                    }
                    0x11 => {
                        let extra = match fields.into_iter().next() {
                            Some(v) => pack_map_to_hashmap(v)?,
                            None => HashMap::new(),
                        };
                        Ok(BoltRequest::Begin { extra })
                    }
                    0x12 => Ok(BoltRequest::Commit),
                    0x13 => Ok(BoltRequest::Rollback),
                    0x2F => {
                        let extra_map = match fields.into_iter().next() {
                            Some(v) => pack_map_to_hashmap(v)?,
                            None => HashMap::new(),
                        };
                        let (n, qid) = extract_n_qid(&extra_map);
                        Ok(BoltRequest::Discard { n, qid })
                    }
                    0x3F => {
                        let extra_map = match fields.into_iter().next() {
                            Some(v) => pack_map_to_hashmap(v)?,
                            None => HashMap::new(),
                        };
                        let (n, qid) = extract_n_qid(&extra_map);
                        Ok(BoltRequest::Pull { n, qid })
                    }
                    _ => Err(BoltError::Protocol(format!("unknown message tag: 0x{:02X}", tag))),
                }
            }
            _ => Err(BoltError::Protocol("expected Struct for Bolt message".into())),
        }
    }
}

fn hashmap_to_pack_map(map: &HashMap<String, PackValue>) -> PackValue {
    let pairs: Vec<(String, PackValue)> = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    PackValue::Map(pairs)
}

impl BoltResponse {
    pub fn to_pack_value(&self) -> PackValue {
        match self {
            BoltResponse::Success { metadata } => PackValue::Struct {
                tag: 0x70,
                fields: vec![hashmap_to_pack_map(metadata)],
            },
            BoltResponse::Record { data } => PackValue::Struct {
                tag: 0x71,
                fields: vec![PackValue::List(data.clone())],
            },
            BoltResponse::Ignored => PackValue::Struct {
                tag: 0x7E,
                fields: vec![],
            },
            BoltResponse::Failure { code, message } => PackValue::Struct {
                tag: 0x7F,
                fields: vec![PackValue::Map(vec![
                    ("code".into(), PackValue::String(code.clone())),
                    ("message".into(), PackValue::String(message.clone())),
                ])],
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_struct(tag: u8, fields: Vec<PackValue>) -> PackValue {
        PackValue::Struct { tag, fields }
    }

    fn make_map(pairs: Vec<(&str, PackValue)>) -> PackValue {
        PackValue::Map(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }

    #[test]
    fn parse_hello() {
        let val = make_struct(0x01, vec![
            make_map(vec![("user_agent", PackValue::String("test/1.0".into()))]),
        ]);
        let req = BoltRequest::parse(val).unwrap();
        match req {
            BoltRequest::Hello { extra } => {
                assert_eq!(extra.get("user_agent"), Some(&PackValue::String("test/1.0".into())));
            }
            _ => panic!("expected Hello"),
        }
    }

    #[test]
    fn parse_hello_empty_fields() {
        let val = make_struct(0x01, vec![]);
        let req = BoltRequest::parse(val).unwrap();
        match req {
            BoltRequest::Hello { extra } => assert!(extra.is_empty()),
            _ => panic!("expected Hello"),
        }
    }

    #[test]
    fn parse_goodbye() {
        let val = make_struct(0x02, vec![]);
        assert_eq!(BoltRequest::parse(val).unwrap(), BoltRequest::Goodbye);
    }

    #[test]
    fn parse_reset() {
        let val = make_struct(0x0F, vec![]);
        assert_eq!(BoltRequest::parse(val).unwrap(), BoltRequest::Reset);
    }

    #[test]
    fn parse_run() {
        let val = make_struct(0x10, vec![
            PackValue::String("MATCH (n) RETURN n".into()),
            make_map(vec![("x", PackValue::Int(42))]),
            make_map(vec![]),
        ]);
        let req = BoltRequest::parse(val).unwrap();
        match req {
            BoltRequest::Run { query, params, extra } => {
                assert_eq!(query, "MATCH (n) RETURN n");
                assert_eq!(params.get("x"), Some(&PackValue::Int(42)));
                assert!(extra.is_empty());
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn parse_run_missing_query() {
        let val = make_struct(0x10, vec![PackValue::Int(123)]);
        assert!(BoltRequest::parse(val).is_err());
    }

    #[test]
    fn parse_run_minimal_fields() {
        let val = make_struct(0x10, vec![
            PackValue::String("RETURN 1".into()),
        ]);
        let req = BoltRequest::parse(val).unwrap();
        match req {
            BoltRequest::Run { query, params, extra } => {
                assert_eq!(query, "RETURN 1");
                assert!(params.is_empty());
                assert!(extra.is_empty());
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn parse_begin() {
        let val = make_struct(0x11, vec![make_map(vec![("db", PackValue::String("neo4j".into()))])]);
        let req = BoltRequest::parse(val).unwrap();
        match req {
            BoltRequest::Begin { extra } => {
                assert_eq!(extra.get("db"), Some(&PackValue::String("neo4j".into())));
            }
            _ => panic!("expected Begin"),
        }
    }

    #[test]
    fn parse_commit() {
        let val = make_struct(0x12, vec![]);
        assert_eq!(BoltRequest::parse(val).unwrap(), BoltRequest::Commit);
    }

    #[test]
    fn parse_rollback() {
        let val = make_struct(0x13, vec![]);
        assert_eq!(BoltRequest::parse(val).unwrap(), BoltRequest::Rollback);
    }

    #[test]
    fn parse_discard() {
        let val = make_struct(0x2F, vec![
            make_map(vec![("n", PackValue::Int(100)), ("qid", PackValue::Int(0))]),
        ]);
        let req = BoltRequest::parse(val).unwrap();
        assert_eq!(req, BoltRequest::Discard { n: 100, qid: 0 });
    }

    #[test]
    fn parse_discard_defaults() {
        let val = make_struct(0x2F, vec![make_map(vec![])]);
        let req = BoltRequest::parse(val).unwrap();
        assert_eq!(req, BoltRequest::Discard { n: -1, qid: -1 });
    }

    #[test]
    fn parse_pull() {
        let val = make_struct(0x3F, vec![
            make_map(vec![("n", PackValue::Int(-1)), ("qid", PackValue::Int(3))]),
        ]);
        let req = BoltRequest::parse(val).unwrap();
        assert_eq!(req, BoltRequest::Pull { n: -1, qid: 3 });
    }

    #[test]
    fn parse_pull_defaults() {
        let val = make_struct(0x3F, vec![make_map(vec![])]);
        let req = BoltRequest::parse(val).unwrap();
        assert_eq!(req, BoltRequest::Pull { n: -1, qid: -1 });
    }

    #[test]
    fn parse_unknown_tag() {
        let val = make_struct(0xFF, vec![]);
        assert!(BoltRequest::parse(val).is_err());
    }

    #[test]
    fn parse_non_struct() {
        assert!(BoltRequest::parse(PackValue::Int(42)).is_err());
    }

    #[test]
    fn serialize_success() {
        let mut metadata = HashMap::new();
        metadata.insert("server".into(), PackValue::String("test".into()));
        let resp = BoltResponse::Success { metadata };
        let val = resp.to_pack_value();
        match val {
            PackValue::Struct { tag, fields } => {
                assert_eq!(tag, 0x70);
                assert_eq!(fields.len(), 1);
                match &fields[0] {
                    PackValue::Map(pairs) => {
                        let map: HashMap<&str, &PackValue> = pairs.iter().map(|(k, v)| (k.as_str(), v)).collect();
                        assert_eq!(map.get("server"), Some(&&PackValue::String("test".into())));
                    }
                    _ => panic!("expected Map"),
                }
            }
            _ => panic!("expected Struct"),
        }
    }

    #[test]
    fn serialize_record() {
        let resp = BoltResponse::Record { data: vec![PackValue::Int(1), PackValue::String("hello".into())] };
        let val = resp.to_pack_value();
        match val {
            PackValue::Struct { tag, fields } => {
                assert_eq!(tag, 0x71);
                assert_eq!(fields.len(), 1);
                match &fields[0] {
                    PackValue::List(items) => {
                        assert_eq!(items.len(), 2);
                        assert_eq!(items[0], PackValue::Int(1));
                        assert_eq!(items[1], PackValue::String("hello".into()));
                    }
                    _ => panic!("expected List"),
                }
            }
            _ => panic!("expected Struct"),
        }
    }

    #[test]
    fn serialize_ignored() {
        let resp = BoltResponse::Ignored;
        let val = resp.to_pack_value();
        match val {
            PackValue::Struct { tag, fields } => {
                assert_eq!(tag, 0x7E);
                assert!(fields.is_empty());
            }
            _ => panic!("expected Struct"),
        }
    }

    #[test]
    fn serialize_failure() {
        let resp = BoltResponse::Failure {
            code: "Neo.ClientError.Statement.SyntaxError".into(),
            message: "bad query".into(),
        };
        let val = resp.to_pack_value();
        match val {
            PackValue::Struct { tag, fields } => {
                assert_eq!(tag, 0x7F);
                assert_eq!(fields.len(), 1);
                match &fields[0] {
                    PackValue::Map(pairs) => {
                        let map: HashMap<&str, &PackValue> = pairs.iter().map(|(k, v)| (k.as_str(), v)).collect();
                        assert_eq!(map.get("code"), Some(&&PackValue::String("Neo.ClientError.Statement.SyntaxError".into())));
                        assert_eq!(map.get("message"), Some(&&PackValue::String("bad query".into())));
                    }
                    _ => panic!("expected Map"),
                }
            }
            _ => panic!("expected Struct"),
        }
    }

    #[test]
    fn roundtrip_success_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("fields".into(), PackValue::List(vec![PackValue::String("a".into())]));
        let resp = BoltResponse::Success { metadata: metadata.clone() };
        let val = resp.to_pack_value();
        match val {
            PackValue::Struct { tag, fields } => {
                assert_eq!(tag, 0x70);
                let recovered = pack_map_to_hashmap(fields.into_iter().next().unwrap()).unwrap();
                assert_eq!(recovered, metadata);
            }
            _ => panic!("expected Struct"),
        }
    }

    #[test]
    fn roundtrip_failure_fields() {
        let resp = BoltResponse::Failure {
            code: "err.code".into(),
            message: "err msg".into(),
        };
        let val = resp.to_pack_value();
        match val {
            PackValue::Struct { tag, fields } => {
                assert_eq!(tag, 0x7F);
                let map = pack_map_to_hashmap(fields.into_iter().next().unwrap()).unwrap();
                assert_eq!(map.get("code"), Some(&PackValue::String("err.code".into())));
                assert_eq!(map.get("message"), Some(&PackValue::String("err msg".into())));
            }
            _ => panic!("expected Struct"),
        }
    }

    #[test]
    fn pack_map_to_hashmap_rejects_non_map() {
        assert!(pack_map_to_hashmap(PackValue::Int(1)).is_err());
        assert!(pack_map_to_hashmap(PackValue::Null).is_err());
        assert!(pack_map_to_hashmap(PackValue::List(vec![])).is_err());
    }
}
