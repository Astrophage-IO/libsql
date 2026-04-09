use bytes::BufMut;
use bytes::BytesMut;

use super::value::PackValue;

pub fn encode(value: &PackValue, buf: &mut BytesMut) {
    match value {
        PackValue::Null => buf.put_u8(0xC0),

        PackValue::Bool(false) => buf.put_u8(0xC2),
        PackValue::Bool(true) => buf.put_u8(0xC3),

        PackValue::Int(n) => encode_int(*n, buf),
        PackValue::Float(f) => {
            buf.put_u8(0xC1);
            buf.put_f64(*f);
        }

        PackValue::Bytes(data) => encode_bytes(data, buf),
        PackValue::String(s) => encode_string(s, buf),
        PackValue::List(items) => encode_list(items, buf),
        PackValue::Map(entries) => encode_map(entries, buf),

        PackValue::Struct { tag, fields } => {
            let count = fields.len();
            assert!(
                count <= 15,
                "PackStream structs support at most 15 fields, got {}",
                count
            );
            buf.put_u8(0xB0 | (count as u8));
            buf.put_u8(*tag);
            for field in fields {
                encode(field, buf);
            }
        }
    }
}

fn encode_int(n: i64, buf: &mut BytesMut) {
    if (-16..=127).contains(&n) {
        buf.put_u8(n as u8);
    } else if (-128..=-17).contains(&n) {
        buf.put_u8(0xC8);
        buf.put_i8(n as i8);
    } else if (-32_768..=32_767).contains(&n) {
        buf.put_u8(0xC9);
        buf.put_i16(n as i16);
    } else if (-2_147_483_648..=2_147_483_647).contains(&n) {
        buf.put_u8(0xCA);
        buf.put_i32(n as i32);
    } else {
        buf.put_u8(0xCB);
        buf.put_i64(n);
    }
}

fn encode_bytes(data: &[u8], buf: &mut BytesMut) {
    let len = data.len();
    if len <= 0xFF {
        buf.put_u8(0xCC);
        buf.put_u8(len as u8);
    } else if len <= 0xFFFF {
        buf.put_u8(0xCD);
        buf.put_u16(len as u16);
    } else {
        buf.put_u8(0xCE);
        buf.put_u32(len as u32);
    }
    buf.put_slice(data);
}

fn encode_string(s: &str, buf: &mut BytesMut) {
    let len = s.len();
    if len <= 0x0F {
        buf.put_u8(0x80 | (len as u8));
    } else if len <= 0xFF {
        buf.put_u8(0xD0);
        buf.put_u8(len as u8);
    } else if len <= 0xFFFF {
        buf.put_u8(0xD1);
        buf.put_u16(len as u16);
    } else {
        buf.put_u8(0xD2);
        buf.put_u32(len as u32);
    }
    buf.put_slice(s.as_bytes());
}

fn encode_list(items: &[PackValue], buf: &mut BytesMut) {
    let count = items.len();
    if count <= 0x0F {
        buf.put_u8(0x90 | (count as u8));
    } else if count <= 0xFF {
        buf.put_u8(0xD4);
        buf.put_u8(count as u8);
    } else if count <= 0xFFFF {
        buf.put_u8(0xD5);
        buf.put_u16(count as u16);
    } else {
        buf.put_u8(0xD6);
        buf.put_u32(count as u32);
    }
    for item in items {
        encode(item, buf);
    }
}

fn encode_map(entries: &[(String, PackValue)], buf: &mut BytesMut) {
    let count = entries.len();
    if count <= 0x0F {
        buf.put_u8(0xA0 | (count as u8));
    } else if count <= 0xFF {
        buf.put_u8(0xD8);
        buf.put_u8(count as u8);
    } else if count <= 0xFFFF {
        buf.put_u8(0xD9);
        buf.put_u16(count as u16);
    } else {
        buf.put_u8(0xDA);
        buf.put_u32(count as u32);
    }
    for (key, val) in entries {
        encode_string(key, buf);
        encode(val, buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packstream::decode;

    fn round_trip(value: &PackValue) -> PackValue {
        let mut buf = BytesMut::new();
        encode(value, &mut buf);
        let mut bytes = buf.freeze();
        decode::decode(&mut bytes).unwrap()
    }

    #[test]
    fn test_null() {
        let v = PackValue::Null;
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(&buf[..], &[0xC0]);
    }

    #[test]
    fn test_bool() {
        assert_eq!(round_trip(&PackValue::Bool(true)), PackValue::Bool(true));
        assert_eq!(round_trip(&PackValue::Bool(false)), PackValue::Bool(false));

        let mut buf = BytesMut::new();
        encode(&PackValue::Bool(false), &mut buf);
        assert_eq!(&buf[..], &[0xC2]);

        buf.clear();
        encode(&PackValue::Bool(true), &mut buf);
        assert_eq!(&buf[..], &[0xC3]);
    }

    #[test]
    fn test_int_tiny_positive() {
        for n in 0..=127i64 {
            let v = PackValue::Int(n);
            assert_eq!(round_trip(&v), v);
        }
        let mut buf = BytesMut::new();
        encode(&PackValue::Int(0), &mut buf);
        assert_eq!(&buf[..], &[0x00]);

        buf.clear();
        encode(&PackValue::Int(127), &mut buf);
        assert_eq!(&buf[..], &[0x7F]);
    }

    #[test]
    fn test_int_tiny_negative() {
        for n in -16..=-1i64 {
            let v = PackValue::Int(n);
            assert_eq!(round_trip(&v), v);
        }
        let mut buf = BytesMut::new();
        encode(&PackValue::Int(-1), &mut buf);
        assert_eq!(&buf[..], &[0xFF]);

        buf.clear();
        encode(&PackValue::Int(-16), &mut buf);
        assert_eq!(&buf[..], &[0xF0]);
    }

    #[test]
    fn test_int_8() {
        let v = PackValue::Int(-17);
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(buf[0], 0xC8);

        let v = PackValue::Int(-128);
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(buf[0], 0xC8);
    }

    #[test]
    fn test_int_16() {
        let v = PackValue::Int(-129);
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(buf[0], 0xC9);

        let v = PackValue::Int(128);
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(buf[0], 0xC9);
    }

    #[test]
    fn test_int_32() {
        let v = PackValue::Int(100_000);
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(buf[0], 0xCA);
    }

    #[test]
    fn test_int_64() {
        let v = PackValue::Int(i64::MAX);
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(buf[0], 0xCB);

        let v = PackValue::Int(i64::MIN);
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_float() {
        let v = PackValue::Float(3.14);
        assert_eq!(round_trip(&v), v);

        let v = PackValue::Float(0.0);
        assert_eq!(round_trip(&v), v);

        let v = PackValue::Float(-1.5e10);
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_bytes_small() {
        let v = PackValue::Bytes(vec![1, 2, 3]);
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_bytes_empty() {
        let v = PackValue::Bytes(vec![]);
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_bytes_256() {
        let v = PackValue::Bytes(vec![0xAB; 256]);
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(buf[0], 0xCD);
    }

    #[test]
    fn test_string_empty() {
        let v = PackValue::String(String::new());
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(&buf[..], &[0x80]);
    }

    #[test]
    fn test_string_tiny() {
        let v = PackValue::String("hello".into());
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(buf[0], 0x85);
    }

    #[test]
    fn test_string_15_bytes() {
        let v = PackValue::String("a]bcdefghijklmn".into());
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(buf[0], 0x8F);
    }

    #[test]
    fn test_string_16_bytes() {
        let v = PackValue::String("abcdefghijklmnop".into());
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(buf[0], 0xD0);
    }

    #[test]
    fn test_string_256_bytes() {
        let v = PackValue::String("x".repeat(256));
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(buf[0], 0xD1);
    }

    #[test]
    fn test_list_empty() {
        let v = PackValue::List(vec![]);
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(&buf[..], &[0x90]);
    }

    #[test]
    fn test_list_small() {
        let v = PackValue::List(vec![
            PackValue::Int(1),
            PackValue::Int(2),
            PackValue::Int(3),
        ]);
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_list_16_items() {
        let items: Vec<PackValue> = (0..16).map(|i| PackValue::Int(i)).collect();
        let v = PackValue::List(items);
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(buf[0], 0xD4);
    }

    #[test]
    fn test_map_empty() {
        let v = PackValue::Map(vec![]);
        assert_eq!(round_trip(&v), v);
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
        assert_eq!(&buf[..], &[0xA0]);
    }

    #[test]
    fn test_map_small() {
        let v = PackValue::Map(vec![
            ("name".into(), PackValue::String("Alice".into())),
            ("age".into(), PackValue::Int(30)),
        ]);
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_struct_basic() {
        let v = PackValue::Struct {
            tag: 0x70,
            fields: vec![PackValue::Map(vec![(
                "server".into(),
                PackValue::String("test".into()),
            )])],
        };
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_struct_tag_preserved() {
        for tag in [0x01u8, 0x10, 0x4E, 0x52, 0x70, 0x71, 0x7E, 0x7F] {
            let v = PackValue::Struct {
                tag,
                fields: vec![],
            };
            assert_eq!(round_trip(&v), v);
        }
    }

    #[test]
    fn test_nested_list_of_maps() {
        let v = PackValue::List(vec![
            PackValue::Map(vec![
                ("x".into(), PackValue::Int(1)),
                ("y".into(), PackValue::Int(2)),
            ]),
            PackValue::Map(vec![
                ("x".into(), PackValue::Int(3)),
                ("y".into(), PackValue::Int(4)),
            ]),
        ]);
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_nested_map_containing_lists() {
        let v = PackValue::Map(vec![
            (
                "items".into(),
                PackValue::List(vec![
                    PackValue::String("a".into()),
                    PackValue::String("b".into()),
                ]),
            ),
            (
                "counts".into(),
                PackValue::List(vec![PackValue::Int(10), PackValue::Int(20)]),
            ),
        ]);
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_deeply_nested() {
        let v = PackValue::List(vec![PackValue::Map(vec![(
            "nested".into(),
            PackValue::List(vec![PackValue::Struct {
                tag: 0x4E,
                fields: vec![
                    PackValue::Int(42),
                    PackValue::List(vec![PackValue::String("Label".into())]),
                    PackValue::Map(vec![("key".into(), PackValue::Bool(true))]),
                ],
            }]),
        )])]);
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_int_boundary_values() {
        for n in [
            -16,
            -17,
            -128,
            -129,
            127,
            128,
            32767,
            32768,
            -32768,
            -32769,
            i64::MAX,
            i64::MIN,
        ] {
            let v = PackValue::Int(n);
            assert_eq!(round_trip(&v), v, "failed for n={}", n);
        }
    }

    #[test]
    fn test_encode_raw_bytes_null() {
        let mut buf = BytesMut::new();
        encode(&PackValue::Null, &mut buf);
        assert_eq!(buf.to_vec(), vec![0xC0]);
    }

    #[test]
    fn test_encode_raw_bytes_float() {
        let mut buf = BytesMut::new();
        encode(&PackValue::Float(1.0), &mut buf);
        assert_eq!(buf.len(), 9);
        assert_eq!(buf[0], 0xC1);
        let val = f64::from_be_bytes(buf[1..9].try_into().unwrap());
        assert_eq!(val, 1.0);
    }

    #[test]
    fn test_all_variants_round_trip() {
        let values = vec![
            PackValue::Null,
            PackValue::Bool(true),
            PackValue::Bool(false),
            PackValue::Int(0),
            PackValue::Int(-1),
            PackValue::Int(127),
            PackValue::Int(-128),
            PackValue::Int(1000),
            PackValue::Int(i64::MAX),
            PackValue::Float(0.0),
            PackValue::Float(-273.15),
            PackValue::Bytes(vec![]),
            PackValue::Bytes(vec![0xFF; 100]),
            PackValue::String(String::new()),
            PackValue::String("hello world".into()),
            PackValue::List(vec![]),
            PackValue::List(vec![PackValue::Null, PackValue::Bool(true)]),
            PackValue::Map(vec![]),
            PackValue::Map(vec![("k".into(), PackValue::Int(1))]),
            PackValue::Struct {
                tag: 0x70,
                fields: vec![],
            },
            PackValue::Struct {
                tag: 0x71,
                fields: vec![PackValue::List(vec![])],
            },
        ];
        for v in &values {
            assert_eq!(round_trip(v), *v, "round-trip failed for {:?}", v);
        }
    }

    #[test]
    #[should_panic(expected = "PackStream structs support at most 15 fields")]
    fn test_encode_struct_16_fields_panics() {
        let fields: Vec<PackValue> = (0..16).map(|i| PackValue::Int(i)).collect();
        let v = PackValue::Struct { tag: 0x01, fields };
        let mut buf = BytesMut::new();
        encode(&v, &mut buf);
    }
}
