use crate::error::BoltError;

pub const BOLT_MAGIC: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];

#[derive(Debug, Clone, PartialEq)]
pub struct HandshakeResult {
    pub major: u8,
    pub minor: u8,
}

pub fn parse_handshake(data: &[u8; 20]) -> Result<HandshakeResult, BoltError> {
    if data[0..4] != BOLT_MAGIC {
        return Err(BoltError::Protocol("invalid Bolt magic preamble".into()));
    }

    for slot in 0..4 {
        let offset = 4 + slot * 4;
        let range = data[offset + 1];
        let minor = data[offset + 2];
        let major = data[offset + 3];

        if major == 0 && minor == 0 {
            continue;
        }

        if major == 4 {
            let min_minor = if range > 0 {
                minor.saturating_sub(range)
            } else {
                minor
            };
            if min_minor <= 4 && minor >= 4 {
                return Ok(HandshakeResult { major: 4, minor: 4 });
            }
        }
    }

    Err(BoltError::Protocol(
        "no compatible Bolt version (need v4.4)".into(),
    ))
}

pub fn handshake_response(result: &HandshakeResult) -> [u8; 4] {
    [0x00, 0x00, result.minor, result.major]
}

pub fn handshake_failure() -> [u8; 4] {
    [0x00, 0x00, 0x00, 0x00]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_handshake(slots: [[u8; 4]; 4]) -> [u8; 20] {
        let mut data = [0u8; 20];
        data[0..4].copy_from_slice(&BOLT_MAGIC);
        for (i, slot) in slots.iter().enumerate() {
            data[4 + i * 4..8 + i * 4].copy_from_slice(slot);
        }
        data
    }

    #[test]
    fn parse_exact_v4_4() {
        let data = build_handshake([
            [0x00, 0x00, 0x04, 0x04],
            [0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00],
        ]);
        let result = parse_handshake(&data).unwrap();
        assert_eq!(result, HandshakeResult { major: 4, minor: 4 });
    }

    #[test]
    fn parse_v4_4_in_second_slot() {
        let data = build_handshake([
            [0x00, 0x00, 0x01, 0x05],
            [0x00, 0x00, 0x04, 0x04],
            [0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00],
        ]);
        let result = parse_handshake(&data).unwrap();
        assert_eq!(result, HandshakeResult { major: 4, minor: 4 });
    }

    #[test]
    fn parse_range_covering_v4_4() {
        let data = build_handshake([
            [0x00, 0x03, 0x04, 0x04],
            [0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00],
        ]);
        let result = parse_handshake(&data).unwrap();
        assert_eq!(result, HandshakeResult { major: 4, minor: 4 });
    }

    #[test]
    fn parse_range_v4_3_to_v4_4() {
        let data = build_handshake([
            [0x00, 0x01, 0x04, 0x04],
            [0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00],
        ]);
        let result = parse_handshake(&data).unwrap();
        assert_eq!(result, HandshakeResult { major: 4, minor: 4 });
    }

    #[test]
    fn parse_range_not_covering_v4_4() {
        let data = build_handshake([
            [0x00, 0x01, 0x03, 0x04],
            [0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00],
        ]);
        assert!(parse_handshake(&data).is_err());
    }

    #[test]
    fn parse_no_compatible_version() {
        let data = build_handshake([
            [0x00, 0x00, 0x01, 0x05],
            [0x00, 0x00, 0x03, 0x04],
            [0x00, 0x00, 0x02, 0x03],
            [0x00, 0x00, 0x00, 0x00],
        ]);
        assert!(parse_handshake(&data).is_err());
    }

    #[test]
    fn parse_wrong_magic() {
        let mut data = [0u8; 20];
        data[0..4].copy_from_slice(&[0x00, 0x00, 0x00, 0x00]);
        assert!(parse_handshake(&data).is_err());
    }

    #[test]
    fn parse_all_empty_slots() {
        let data = build_handshake([
            [0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00],
        ]);
        assert!(parse_handshake(&data).is_err());
    }

    #[test]
    fn response_bytes() {
        let result = HandshakeResult { major: 4, minor: 4 };
        assert_eq!(handshake_response(&result), [0x00, 0x00, 0x04, 0x04]);
    }

    #[test]
    fn failure_bytes() {
        assert_eq!(handshake_failure(), [0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn neo4j_driver_typical_handshake() {
        let data = build_handshake([
            [0x00, 0x02, 0x04, 0x04],
            [0x00, 0x00, 0x01, 0x04],
            [0x00, 0x00, 0x00, 0x03],
            [0x00, 0x00, 0x00, 0x00],
        ]);
        let result = parse_handshake(&data).unwrap();
        assert_eq!(result, HandshakeResult { major: 4, minor: 4 });
    }

    #[test]
    fn v4_4_found_in_last_slot() {
        let data = build_handshake([
            [0x00, 0x00, 0x01, 0x05],
            [0x00, 0x00, 0x00, 0x05],
            [0x00, 0x00, 0x01, 0x03],
            [0x00, 0x00, 0x04, 0x04],
        ]);
        let result = parse_handshake(&data).unwrap();
        assert_eq!(result, HandshakeResult { major: 4, minor: 4 });
    }
}
