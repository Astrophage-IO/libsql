use bytes::{Buf, Bytes};

use super::value::PackValue;
use crate::error::BoltError;

const MAX_DEPTH: usize = 128;
const MAX_COLLECTION_SIZE: usize = 1_000_000;

pub fn decode(buf: &mut Bytes) -> Result<PackValue, BoltError> {
    decode_inner(buf, 0)
}

fn decode_inner(buf: &mut Bytes, depth: usize) -> Result<PackValue, BoltError> {
    if depth > MAX_DEPTH {
        return Err(BoltError::PackStream(format!(
            "recursion depth {} exceeds maximum of {}",
            depth, MAX_DEPTH
        )));
    }

    if !buf.has_remaining() {
        return Err(BoltError::PackStream("unexpected end of data".into()));
    }

    let marker = buf.get_u8();
    match marker {
        0xC0 => Ok(PackValue::Null),
        0xC2 => Ok(PackValue::Bool(false)),
        0xC3 => Ok(PackValue::Bool(true)),

        0xC1 => {
            ensure_remaining(buf, 8)?;
            Ok(PackValue::Float(buf.get_f64()))
        }

        0x00..=0x7F => Ok(PackValue::Int(marker as i64)),

        0xF0..=0xFF => Ok(PackValue::Int(marker as i8 as i64)),

        0xC8 => {
            ensure_remaining(buf, 1)?;
            Ok(PackValue::Int(buf.get_i8() as i64))
        }
        0xC9 => {
            ensure_remaining(buf, 2)?;
            Ok(PackValue::Int(buf.get_i16() as i64))
        }
        0xCA => {
            ensure_remaining(buf, 4)?;
            Ok(PackValue::Int(buf.get_i32() as i64))
        }
        0xCB => {
            ensure_remaining(buf, 8)?;
            Ok(PackValue::Int(buf.get_i64()))
        }

        0xCC => {
            ensure_remaining(buf, 1)?;
            let len = buf.get_u8() as usize;
            ensure_remaining(buf, len)?;
            let data = buf.copy_to_bytes(len).to_vec();
            Ok(PackValue::Bytes(data))
        }
        0xCD => {
            ensure_remaining(buf, 2)?;
            let len = buf.get_u16() as usize;
            ensure_remaining(buf, len)?;
            let data = buf.copy_to_bytes(len).to_vec();
            Ok(PackValue::Bytes(data))
        }
        0xCE => {
            ensure_remaining(buf, 4)?;
            let len = buf.get_u32() as usize;
            ensure_remaining(buf, len)?;
            let data = buf.copy_to_bytes(len).to_vec();
            Ok(PackValue::Bytes(data))
        }

        0x80..=0x8F => {
            let len = (marker & 0x0F) as usize;
            read_string(buf, len)
        }
        0xD0 => {
            ensure_remaining(buf, 1)?;
            let len = buf.get_u8() as usize;
            read_string(buf, len)
        }
        0xD1 => {
            ensure_remaining(buf, 2)?;
            let len = buf.get_u16() as usize;
            read_string(buf, len)
        }
        0xD2 => {
            ensure_remaining(buf, 4)?;
            let len = buf.get_u32() as usize;
            read_string(buf, len)
        }

        0x90..=0x9F => {
            let count = (marker & 0x0F) as usize;
            read_list(buf, count, depth)
        }
        0xD4 => {
            ensure_remaining(buf, 1)?;
            let count = buf.get_u8() as usize;
            read_list(buf, count, depth)
        }
        0xD5 => {
            ensure_remaining(buf, 2)?;
            let count = buf.get_u16() as usize;
            read_list(buf, count, depth)
        }
        0xD6 => {
            ensure_remaining(buf, 4)?;
            let count = buf.get_u32() as usize;
            read_list(buf, count, depth)
        }

        0xA0..=0xAF => {
            let count = (marker & 0x0F) as usize;
            read_map(buf, count, depth)
        }
        0xD8 => {
            ensure_remaining(buf, 1)?;
            let count = buf.get_u8() as usize;
            read_map(buf, count, depth)
        }
        0xD9 => {
            ensure_remaining(buf, 2)?;
            let count = buf.get_u16() as usize;
            read_map(buf, count, depth)
        }
        0xDA => {
            ensure_remaining(buf, 4)?;
            let count = buf.get_u32() as usize;
            read_map(buf, count, depth)
        }

        0xB0..=0xBF => {
            let field_count = (marker & 0x0F) as usize;
            ensure_remaining(buf, 1)?;
            let tag = buf.get_u8();
            validate_collection_size(field_count, 1, buf)?;
            let mut fields = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                fields.push(decode_inner(buf, depth + 1)?);
            }
            Ok(PackValue::Struct { tag, fields })
        }

        _ => Err(BoltError::PackStream(format!(
            "unknown marker byte: 0x{:02X}",
            marker
        ))),
    }
}

fn ensure_remaining(buf: &Bytes, needed: usize) -> Result<(), BoltError> {
    if buf.remaining() < needed {
        return Err(BoltError::PackStream(format!(
            "need {} bytes but only {} remaining",
            needed,
            buf.remaining()
        )));
    }
    Ok(())
}

fn read_string(buf: &mut Bytes, len: usize) -> Result<PackValue, BoltError> {
    ensure_remaining(buf, len)?;
    let raw = buf.copy_to_bytes(len);
    let s = String::from_utf8(raw.to_vec())
        .map_err(|e| BoltError::PackStream(format!("invalid utf-8: {}", e)))?;
    Ok(PackValue::String(s))
}

fn validate_collection_size(
    count: usize,
    bytes_per_item: usize,
    buf: &Bytes,
) -> Result<(), BoltError> {
    if count > MAX_COLLECTION_SIZE {
        return Err(BoltError::PackStream(format!(
            "collection size {} exceeds maximum of {}",
            count, MAX_COLLECTION_SIZE
        )));
    }
    let min_bytes = count.saturating_mul(bytes_per_item);
    if min_bytes > buf.remaining() {
        return Err(BoltError::PackStream(format!(
            "collection claims {} items but only {} bytes remain",
            count,
            buf.remaining()
        )));
    }
    Ok(())
}

fn read_list(buf: &mut Bytes, count: usize, depth: usize) -> Result<PackValue, BoltError> {
    validate_collection_size(count, 1, buf)?;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(decode_inner(buf, depth + 1)?);
    }
    Ok(PackValue::List(items))
}

fn read_map(buf: &mut Bytes, count: usize, depth: usize) -> Result<PackValue, BoltError> {
    validate_collection_size(count, 2, buf)?;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let key = match decode_inner(buf, depth + 1)? {
            PackValue::String(s) => s,
            other => {
                return Err(BoltError::PackStream(format!(
                    "map key must be string, got {:?}",
                    other
                )))
            }
        };
        let val = decode_inner(buf, depth + 1)?;
        entries.push((key, val));
    }
    Ok(PackValue::Map(entries))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packstream::encode;
    use bytes::BytesMut;

    fn decode_bytes(raw: &[u8]) -> PackValue {
        let mut buf = Bytes::from(raw.to_vec());
        decode(&mut buf).unwrap()
    }

    #[test]
    fn test_decode_null() {
        assert_eq!(decode_bytes(&[0xC0]), PackValue::Null);
    }

    #[test]
    fn test_decode_bool() {
        assert_eq!(decode_bytes(&[0xC2]), PackValue::Bool(false));
        assert_eq!(decode_bytes(&[0xC3]), PackValue::Bool(true));
    }

    #[test]
    fn test_decode_tiny_int_positive() {
        assert_eq!(decode_bytes(&[0x00]), PackValue::Int(0));
        assert_eq!(decode_bytes(&[0x01]), PackValue::Int(1));
        assert_eq!(decode_bytes(&[0x7F]), PackValue::Int(127));
    }

    #[test]
    fn test_decode_tiny_int_negative() {
        assert_eq!(decode_bytes(&[0xFF]), PackValue::Int(-1));
        assert_eq!(decode_bytes(&[0xF0]), PackValue::Int(-16));
    }

    #[test]
    fn test_decode_int8() {
        assert_eq!(decode_bytes(&[0xC8, 0xEF]), PackValue::Int(-17));
        assert_eq!(decode_bytes(&[0xC8, 0x80]), PackValue::Int(-128));
    }

    #[test]
    fn test_decode_int16() {
        let mut raw = vec![0xC9];
        raw.extend_from_slice(&(-129i16).to_be_bytes());
        assert_eq!(decode_bytes(&raw), PackValue::Int(-129));
    }

    #[test]
    fn test_decode_int32() {
        let mut raw = vec![0xCA];
        raw.extend_from_slice(&100_000i32.to_be_bytes());
        assert_eq!(decode_bytes(&raw), PackValue::Int(100_000));
    }

    #[test]
    fn test_decode_int64() {
        let mut raw = vec![0xCB];
        raw.extend_from_slice(&i64::MAX.to_be_bytes());
        assert_eq!(decode_bytes(&raw), PackValue::Int(i64::MAX));
    }

    #[test]
    fn test_decode_float() {
        let mut raw = vec![0xC1];
        raw.extend_from_slice(&3.14f64.to_be_bytes());
        assert_eq!(decode_bytes(&raw), PackValue::Float(3.14));
    }

    #[test]
    fn test_decode_empty_string() {
        assert_eq!(decode_bytes(&[0x80]), PackValue::String(String::new()));
    }

    #[test]
    fn test_decode_tiny_string() {
        let mut raw = vec![0x85];
        raw.extend_from_slice(b"hello");
        assert_eq!(decode_bytes(&raw), PackValue::String("hello".into()));
    }

    #[test]
    fn test_decode_empty_list() {
        assert_eq!(decode_bytes(&[0x90]), PackValue::List(vec![]));
    }

    #[test]
    fn test_decode_empty_map() {
        assert_eq!(decode_bytes(&[0xA0]), PackValue::Map(vec![]));
    }

    #[test]
    fn test_decode_struct() {
        let raw = vec![0xB1, 0x70, 0xA0];
        let result = decode_bytes(&raw);
        assert_eq!(
            result,
            PackValue::Struct {
                tag: 0x70,
                fields: vec![PackValue::Map(vec![])],
            }
        );
    }

    #[test]
    fn test_decode_empty_bytes() {
        assert_eq!(decode_bytes(&[0xCC, 0x00]), PackValue::Bytes(vec![]));
    }

    #[test]
    fn test_decode_truncated_data() {
        let mut buf = Bytes::from_static(&[0xC9]);
        assert!(decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_non_string_map_key() {
        let mut buf = Bytes::from(vec![0xA1, 0x01, 0x02]);
        assert!(decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_empty_buffer() {
        let mut buf = Bytes::new();
        assert!(decode(&mut buf).is_err());
    }

    #[test]
    fn test_round_trip_string_8() {
        let s = "a".repeat(20);
        let v = PackValue::String(s);
        let mut enc = BytesMut::new();
        encode::encode(&v, &mut enc);
        assert_eq!(enc[0], 0xD0);
        let mut dec = enc.freeze();
        assert_eq!(decode(&mut dec).unwrap(), v);
    }

    #[test]
    fn test_round_trip_string_16() {
        let s = "b".repeat(300);
        let v = PackValue::String(s);
        let mut enc = BytesMut::new();
        encode::encode(&v, &mut enc);
        assert_eq!(enc[0], 0xD1);
        let mut dec = enc.freeze();
        assert_eq!(decode(&mut dec).unwrap(), v);
    }

    #[test]
    fn test_round_trip_list_8() {
        let items: Vec<PackValue> = (0..20).map(|i| PackValue::Int(i)).collect();
        let v = PackValue::List(items);
        let mut enc = BytesMut::new();
        encode::encode(&v, &mut enc);
        assert_eq!(enc[0], 0xD4);
        let mut dec = enc.freeze();
        assert_eq!(decode(&mut dec).unwrap(), v);
    }

    #[test]
    fn test_round_trip_map_with_various_value_types() {
        let v = PackValue::Map(vec![
            ("null".into(), PackValue::Null),
            ("bool".into(), PackValue::Bool(true)),
            ("int".into(), PackValue::Int(-42)),
            ("float".into(), PackValue::Float(2.718)),
            ("string".into(), PackValue::String("value".into())),
            ("list".into(), PackValue::List(vec![PackValue::Int(1)])),
            ("bytes".into(), PackValue::Bytes(vec![0xDE, 0xAD])),
        ]);
        let mut enc = BytesMut::new();
        encode::encode(&v, &mut enc);
        let mut dec = enc.freeze();
        assert_eq!(decode(&mut dec).unwrap(), v);
    }

    #[test]
    fn test_struct_node_like() {
        let v = PackValue::Struct {
            tag: 0x4E,
            fields: vec![
                PackValue::Int(1),
                PackValue::List(vec![PackValue::String("Person".into())]),
                PackValue::Map(vec![
                    ("name".into(), PackValue::String("Alice".into())),
                    ("age".into(), PackValue::Int(30)),
                ]),
            ],
        };
        let mut enc = BytesMut::new();
        encode::encode(&v, &mut enc);
        let mut dec = enc.freeze();
        assert_eq!(decode(&mut dec).unwrap(), v);
    }

    #[test]
    fn test_struct_rel_like() {
        let v = PackValue::Struct {
            tag: 0x52,
            fields: vec![
                PackValue::Int(100),
                PackValue::Int(1),
                PackValue::Int(2),
                PackValue::String("KNOWS".into()),
                PackValue::Map(vec![("since".into(), PackValue::Int(2020))]),
            ],
        };
        let mut enc = BytesMut::new();
        encode::encode(&v, &mut enc);
        let mut dec = enc.freeze();
        assert_eq!(decode(&mut dec).unwrap(), v);
    }

    #[test]
    fn test_multiple_values_in_sequence() {
        let values = vec![
            PackValue::Int(1),
            PackValue::String("test".into()),
            PackValue::Null,
        ];
        let mut enc = BytesMut::new();
        for v in &values {
            encode::encode(v, &mut enc);
        }
        let mut dec = enc.freeze();
        for v in &values {
            assert_eq!(decode(&mut dec).unwrap(), *v);
        }
        assert!(!dec.has_remaining());
    }

    #[test]
    fn test_decode_list_claims_more_items_than_bytes() {
        let mut raw = vec![0xD4, 200];
        raw.push(0x01);
        let mut buf = Bytes::from(raw);
        let err = decode(&mut buf).unwrap_err();
        match err {
            BoltError::PackStream(msg) => assert!(msg.contains("claims"), "{}", msg),
            other => panic!("expected PackStream error, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_map_claims_more_items_than_bytes() {
        let mut raw = vec![0xD8, 100];
        raw.push(0x80);
        raw.push(0x01);
        let mut buf = Bytes::from(raw);
        let err = decode(&mut buf).unwrap_err();
        match err {
            BoltError::PackStream(msg) => assert!(msg.contains("claims"), "{}", msg),
            other => panic!("expected PackStream error, got {:?}", other),
        }
    }

    #[test]
    fn test_decode_deeply_nested_exceeds_max_depth() {
        let mut raw = Vec::new();
        for _ in 0..200 {
            raw.push(0x91);
        }
        raw.push(0xC0);
        let mut buf = Bytes::from(raw);
        let err = decode(&mut buf).unwrap_err();
        match err {
            BoltError::PackStream(msg) => assert!(msg.contains("recursion depth"), "{}", msg),
            other => panic!("expected PackStream error, got {:?}", other),
        }
    }
}
