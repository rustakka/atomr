//! FIX message representation, wire encoding, and parsing.
//!
//! A FIX message is an ordered list of `tag=value` pairs separated by the SOH
//! control byte (`0x01`). Every message begins with the standard header fields
//! `BeginString(8)`, `BodyLength(9)`, `MsgType(35)` and ends with the
//! `CheckSum(10)` trailer.
//!
//! [`FixMessage`] keeps fields in insertion order in a flat `Vec<FixField>`.
//! The header ordering (8, 9, 35) and the trailer checksum are materialised
//! only at [`FixMessage::to_wire`] time, so callers can [`FixMessage::set`]
//! fields in any order.

use bytes::Bytes;
use thiserror::Error;

/// The SOH (Start Of Header) delimiter that separates FIX fields on the wire.
pub const SOH: u8 = 0x01;

// Standard tag numbers used by the session layer.
pub mod tags {
    pub const BEGIN_STRING: u32 = 8;
    pub const BODY_LENGTH: u32 = 9;
    pub const MSG_TYPE: u32 = 35;
    pub const SENDER_COMP_ID: u32 = 49;
    pub const TARGET_COMP_ID: u32 = 56;
    pub const MSG_SEQ_NUM: u32 = 34;
    pub const SENDING_TIME: u32 = 52;
    pub const CHECK_SUM: u32 = 10;

    // Session-message-specific tags.
    pub const HEART_BT_INT: u32 = 108;
    pub const TEST_REQ_ID: u32 = 112;
    pub const BEGIN_SEQ_NO: u32 = 7;
    pub const END_SEQ_NO: u32 = 16;
    pub const RESET_SEQ_NUM_FLAG: u32 = 141;
    pub const GAP_FILL_FLAG: u32 = 123;
    pub const NEW_SEQ_NO: u32 = 36;
    pub const POSS_DUP_FLAG: u32 = 43;
    pub const ENCRYPT_METHOD: u32 = 98;
    pub const TEXT: u32 = 58;
    pub const DEFAULT_APPL_VER_ID: u32 = 1137;

    // A couple of application tags so the NewOrderSingle / ExecutionReport
    // round-trip test has something realistic to carry.
    pub const CL_ORD_ID: u32 = 11;
    pub const SYMBOL: u32 = 55;
    pub const SIDE: u32 = 54;
    pub const ORDER_QTY: u32 = 38;
    pub const ORD_STATUS: u32 = 39;
    pub const EXEC_TYPE: u32 = 150;
}

/// A single `tag=value` field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixField {
    pub tag: u32,
    pub value: String,
}

impl FixField {
    pub fn new(tag: u32, value: impl Into<String>) -> Self {
        FixField { tag, value: value.into() }
    }
}

/// A FIX administrative or application message type (tag 35).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MsgType {
    Logon,
    Heartbeat,
    TestRequest,
    ResendRequest,
    SequenceReset,
    Logout,
    NewOrderSingle,
    ExecutionReport,
    Other(String),
}

impl MsgType {
    /// The FIX tag-35 code for this message type.
    pub fn code(&self) -> &str {
        match self {
            MsgType::Logon => "A",
            MsgType::Heartbeat => "0",
            MsgType::TestRequest => "1",
            MsgType::ResendRequest => "2",
            MsgType::SequenceReset => "4",
            MsgType::Logout => "5",
            MsgType::NewOrderSingle => "D",
            MsgType::ExecutionReport => "8",
            MsgType::Other(s) => s,
        }
    }

    /// Parse a FIX tag-35 code into a [`MsgType`].
    pub fn from_code(code: &str) -> MsgType {
        match code {
            "A" => MsgType::Logon,
            "0" => MsgType::Heartbeat,
            "1" => MsgType::TestRequest,
            "2" => MsgType::ResendRequest,
            "4" => MsgType::SequenceReset,
            "5" => MsgType::Logout,
            "D" => MsgType::NewOrderSingle,
            "8" => MsgType::ExecutionReport,
            other => MsgType::Other(other.to_string()),
        }
    }

    /// Whether this is a session-layer (administrative) message rather than an
    /// application message. Used by the FSM to decide what to surface to the
    /// application and what to replay during a resend.
    pub fn is_admin(&self) -> bool {
        matches!(
            self,
            MsgType::Logon
                | MsgType::Heartbeat
                | MsgType::TestRequest
                | MsgType::ResendRequest
                | MsgType::SequenceReset
                | MsgType::Logout
        )
    }
}

/// Errors produced while parsing a FIX frame.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum FixParseError {
    #[error("empty frame")]
    Empty,
    #[error("field {0:?} is not valid `tag=value`")]
    MalformedField(String),
    #[error("tag {0:?} is not a valid integer")]
    InvalidTag(String),
    #[error("field bytes are not valid UTF-8")]
    NotUtf8,
    #[error("missing required field, tag {0}")]
    MissingField(u32),
    #[error("checksum mismatch: computed {computed:03}, frame carried {found:03}")]
    ChecksumMismatch { computed: u8, found: u8 },
    #[error("CheckSum(10) value {0:?} is not a 3-digit number")]
    InvalidCheckSum(String),
}

/// An ordered collection of FIX fields.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FixMessage {
    fields: Vec<FixField>,
}

impl FixMessage {
    /// A new, empty message.
    pub fn new() -> Self {
        FixMessage { fields: Vec::new() }
    }

    /// Build a message of a given type (tag 35). The `MsgType` field is the
    /// first body field; header framing fields (8/9) are added at encode time.
    pub fn of_type(msg_type: MsgType) -> Self {
        let mut m = FixMessage::new();
        m.set(tags::MSG_TYPE, msg_type.code());
        m
    }

    /// First value for `tag`, if present.
    pub fn get(&self, tag: u32) -> Option<&str> {
        self.fields.iter().find(|f| f.tag == tag).map(|f| f.value.as_str())
    }

    /// Convenience: parse a field value as a `u64`.
    pub fn get_u64(&self, tag: u32) -> Option<u64> {
        self.get(tag).and_then(|v| v.parse().ok())
    }

    /// Set `tag` to `value`, replacing the first existing occurrence or
    /// appending if absent. Returns `&mut self` for chaining.
    pub fn set(&mut self, tag: u32, value: impl Into<String>) -> &mut Self {
        let value = value.into();
        if let Some(f) = self.fields.iter_mut().find(|f| f.tag == tag) {
            f.value = value;
        } else {
            self.fields.push(FixField { tag, value });
        }
        self
    }

    /// All fields in insertion order (header framing fields excluded — they are
    /// only materialised by [`to_wire`](Self::to_wire)).
    pub fn fields(&self) -> &[FixField] {
        &self.fields
    }

    /// The message type from tag 35, if present.
    pub fn msg_type(&self) -> Option<MsgType> {
        self.get(tags::MSG_TYPE).map(MsgType::from_code)
    }

    /// The `MsgSeqNum` (tag 34), if present and numeric.
    pub fn seq_num(&self) -> Option<u64> {
        self.get_u64(tags::MSG_SEQ_NUM)
    }

    /// Encode the message to wire bytes.
    ///
    /// Ordering: `8=BeginString | 9=BodyLength | 35=MsgType | <body…> | 10=CheckSum`.
    /// `BodyLength(9)` counts every byte after the SOH that terminates field 9
    /// up to and including the SOH that terminates the last field before
    /// `CheckSum(10)`. `CheckSum(10)` is the sum of all bytes up to and
    /// including the SOH before the `10=` field, taken mod 256 and rendered as
    /// three decimal digits.
    pub fn to_wire(&self) -> Bytes {
        let begin_string = self.get(tags::BEGIN_STRING).unwrap_or("FIX.4.4").to_string();
        let msg_type = self.get(tags::MSG_TYPE).unwrap_or("").to_string();

        // Body = MsgType(35) first, then every other field except the framing
        // fields (8/9) and the trailer (10).
        let mut body = Vec::new();
        push_field(&mut body, tags::MSG_TYPE, &msg_type);
        for f in &self.fields {
            if f.tag == tags::BEGIN_STRING
                || f.tag == tags::BODY_LENGTH
                || f.tag == tags::MSG_TYPE
                || f.tag == tags::CHECK_SUM
            {
                continue;
            }
            push_field(&mut body, f.tag, &f.value);
        }

        let mut out = Vec::with_capacity(body.len() + 32);
        push_field(&mut out, tags::BEGIN_STRING, &begin_string);
        push_field(&mut out, tags::BODY_LENGTH, &body.len().to_string());
        out.extend_from_slice(&body);

        // Checksum covers everything emitted so far (8, 9, and the body, each
        // including its terminating SOH).
        let sum: u32 = out.iter().map(|b| *b as u32).sum();
        let checksum = (sum % 256) as u8;
        push_field(&mut out, tags::CHECK_SUM, &format!("{checksum:03}"));

        Bytes::from(out)
    }

    /// Parse a single SOH-delimited FIX frame.
    ///
    /// The frame may or may not include the trailing SOH after `10=NNN`; both
    /// are accepted. When a `CheckSum(10)` field is present it is verified
    /// against the recomputed checksum.
    pub fn parse(input: &[u8]) -> Result<FixMessage, FixParseError> {
        if input.is_empty() {
            return Err(FixParseError::Empty);
        }

        // Split into fields on SOH, ignoring a trailing empty segment caused by
        // a terminal SOH.
        let mut fields = Vec::new();
        let mut checksum_boundary: Option<usize> = None; // byte offset where 10= field begins
        let mut found_checksum: Option<u8> = None;

        let mut offset = 0usize;
        for raw in input.split(|b| *b == SOH) {
            if raw.is_empty() {
                // trailing SOH or doubled SOH — skip, but still advance offset.
                offset += 1;
                continue;
            }
            let s = std::str::from_utf8(raw).map_err(|_| FixParseError::NotUtf8)?;
            let eq = s.find('=').ok_or_else(|| FixParseError::MalformedField(s.to_string()))?;
            let (tag_str, val_str) = s.split_at(eq);
            let value = &val_str[1..]; // skip '='
            let tag: u32 = tag_str.parse().map_err(|_| FixParseError::InvalidTag(tag_str.to_string()))?;

            if tag == tags::CHECK_SUM {
                checksum_boundary = Some(offset);
                if value.len() != 3 || !value.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(FixParseError::InvalidCheckSum(value.to_string()));
                }
                // Validated above as exactly 3 ASCII digits, so the parse is
                // infallible; map the error anyway to keep the path panic-free.
                let parsed =
                    value.parse::<u32>().map_err(|_| FixParseError::InvalidCheckSum(value.to_string()))?;
                found_checksum = Some(parsed as u8);
            }

            fields.push(FixField { tag, value: value.to_string() });
            offset += raw.len() + 1; // +1 for the SOH
        }

        if fields.is_empty() {
            return Err(FixParseError::Empty);
        }

        // Verify checksum if present.
        if let (Some(boundary), Some(found)) = (checksum_boundary, found_checksum) {
            let sum: u32 = input[..boundary].iter().map(|b| *b as u32).sum();
            let computed = (sum % 256) as u8;
            if computed != found {
                return Err(FixParseError::ChecksumMismatch { computed, found });
            }
        }

        Ok(FixMessage { fields })
    }
}

fn push_field(buf: &mut Vec<u8>, tag: u32, value: &str) {
    buf.extend_from_slice(tag.to_string().as_bytes());
    buf.push(b'=');
    buf.extend_from_slice(value.as_bytes());
    buf.push(SOH);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msg_type_codes_round_trip() {
        for mt in [
            MsgType::Logon,
            MsgType::Heartbeat,
            MsgType::TestRequest,
            MsgType::ResendRequest,
            MsgType::SequenceReset,
            MsgType::Logout,
            MsgType::NewOrderSingle,
            MsgType::ExecutionReport,
        ] {
            assert_eq!(MsgType::from_code(mt.code()), mt);
        }
        assert_eq!(MsgType::from_code("XY"), MsgType::Other("XY".to_string()));
    }

    #[test]
    fn to_wire_then_parse_round_trips() {
        let mut m = FixMessage::of_type(MsgType::Heartbeat);
        m.set(tags::BEGIN_STRING, "FIX.4.4");
        m.set(tags::SENDER_COMP_ID, "CLIENT");
        m.set(tags::TARGET_COMP_ID, "SERVER");
        m.set(tags::MSG_SEQ_NUM, "1");
        m.set(tags::TEST_REQ_ID, "ABC");

        let wire = m.to_wire();
        let parsed = FixMessage::parse(&wire).expect("parse");

        assert_eq!(parsed.msg_type(), Some(MsgType::Heartbeat));
        assert_eq!(parsed.get(tags::SENDER_COMP_ID), Some("CLIENT"));
        assert_eq!(parsed.get(tags::TARGET_COMP_ID), Some("SERVER"));
        assert_eq!(parsed.get(tags::TEST_REQ_ID), Some("ABC"));
        assert_eq!(parsed.seq_num(), Some(1));
        // BeginString / BodyLength / CheckSum materialised on the wire.
        assert_eq!(parsed.get(tags::BEGIN_STRING), Some("FIX.4.4"));
        assert!(parsed.get(tags::BODY_LENGTH).is_some());
        assert!(parsed.get(tags::CHECK_SUM).is_some());
    }

    #[test]
    fn checksum_matches_known_vector() {
        // A well-known QuickFIX logon example:
        // 8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|108=30|
        // has CheckSum 062. Build the same body and verify our encoder agrees.
        let mut m = FixMessage::of_type(MsgType::Logon);
        m.set(tags::BEGIN_STRING, "FIX.4.2");
        m.set(tags::SENDER_COMP_ID, "SERVER");
        m.set(tags::TARGET_COMP_ID, "CLIENT");
        m.set(tags::MSG_SEQ_NUM, "177");
        m.set(tags::SENDING_TIME, "20090107-18:15:16");
        m.set(tags::ENCRYPT_METHOD, "0");
        m.set(tags::HEART_BT_INT, "30");

        let wire = m.to_wire();
        let s = String::from_utf8(wire.to_vec()).unwrap().replace(SOH as char, "|");
        assert_eq!(
            s,
            "8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|108=30|10=062|"
        );

        // And the recomputed checksum verifies on parse.
        let parsed = FixMessage::parse(&wire).expect("parse");
        assert_eq!(parsed.get(tags::CHECK_SUM), Some("062"));
    }

    #[test]
    fn parse_rejects_bad_checksum() {
        // Tamper with a valid frame's checksum.
        let mut m = FixMessage::of_type(MsgType::Heartbeat);
        m.set(tags::BEGIN_STRING, "FIX.4.4");
        m.set(tags::MSG_SEQ_NUM, "1");
        let wire = m.to_wire();
        let mut tampered = wire.to_vec();
        // The checksum is the last "10=NNN\x01"; flip a digit.
        let pos = tampered.len() - 2; // last digit before trailing SOH
        tampered[pos] = if tampered[pos] == b'0' { b'1' } else { b'0' };
        let err = FixMessage::parse(&tampered).unwrap_err();
        assert!(matches!(err, FixParseError::ChecksumMismatch { .. }));
    }

    #[test]
    fn parse_rejects_malformed_field() {
        let bad = b"8=FIX.4.4\x01nonsense\x0135=0\x01";
        let err = FixMessage::parse(bad).unwrap_err();
        assert!(matches!(err, FixParseError::MalformedField(_)));
    }
}
