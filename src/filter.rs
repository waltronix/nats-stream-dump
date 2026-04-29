use serde_json_path::JsonPath;
use serde_json_path::functions::ValueType;

use crate::stream_reader::RawMessage;

/// A compiled JSONPath filter that evaluates per-element filter expressions
/// against the full `RawMessage` structure (subject, payload, timestamp, sequence).
///
/// To evaluate a per-element filter (e.g., `$[?@.payload.severity == 'critical']`)
/// against a single message, the message is wrapped in a one-element JSON array,
/// the filter is applied, and the result is checked for non-emptiness.
///
/// This allows filtering on any message field:
/// - Payload fields: `$[?@.payload.severity == 'critical']`
/// - Subject: `$[?@.subject == 'OrderCreated.v1']`
/// - Timestamp: `$[?datetime(@.timestamp) >= datetime('2026-03-01T00:00:00Z')]`
/// - Sequence: `$[?@.sequence > 100]`
#[derive(Debug)]
pub struct MessageFilter {
    path: JsonPath,
}

impl MessageFilter {
    /// Parses a JSONPath filter expression and returns a compiled `MessageFilter`.
    ///
    /// # Errors
    ///
    /// Returns an error if the expression is not a valid JSONPath query.
    pub fn new(expression: &str) -> Result<Self, serde_json_path::ParseError> {
        let path = JsonPath::parse(expression)?;
        Ok(Self { path })
    }

    /// Evaluates the filter against the full message (subject, payload, timestamp, sequence).
    ///
    /// Returns `true` if the message matches the filter expression.
    pub fn matches(&self, message: &RawMessage) -> bool {
        let wrapped = serde_json::json!([message]);
        let result = self.path.query(&wrapped);
        !result.all().is_empty()
    }
}

/// Custom JSONPath function that converts an RFC 3339 timestamp string to
/// nanosecond-precision Unix timestamp (i64).
///
/// Enables temporal range queries like:
/// ```text
/// $[?datetime(@.timestamp) >= datetime('2026-03-01T00:00:00Z')]
/// ```
///
/// Returns `ValueType::Nothing` if the input is not a valid RFC 3339 string
/// or if the timestamp overflows i64 nanoseconds (dates beyond ~year 2262).
#[serde_json_path::function]
fn datetime(value: ValueType<'_>) -> ValueType<'_> {
    let v = match value {
        ValueType::Value(v) => v,
        ValueType::Node(v) => v.clone(),
        ValueType::Nothing => return ValueType::Nothing,
    };
    let Some(s) = v.as_str() else {
        return ValueType::Nothing;
    };
    let Ok(dt) = time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
    else {
        return ValueType::Nothing;
    };
    let nanos = dt.unix_timestamp_nanos();
    let Ok(nanos_i64) = i64::try_from(nanos) else {
        tracing::error!("datetime overflow: timestamp '{s}' exceeds i64 nanosecond range");
        return ValueType::Nothing;
    };
    ValueType::Value(serde_json::json!(nanos_i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_message(payload: serde_json::Value) -> RawMessage {
        RawMessage {
            subject: "test.subject".to_string(),
            payload,
            timestamp: Some("2026-03-30T12:00:00Z".to_string()),
            sequence: 1,
        }
    }

    #[test]
    fn message_filter_should_match_by_exact_payload_field() {
        let filter = MessageFilter::new("$[?@.payload.severity == 'critical']").unwrap();

        let critical = make_message(serde_json::json!({"severity": "critical"}));
        let info = make_message(serde_json::json!({"severity": "info"}));

        assert!(filter.matches(&critical));
        assert!(!filter.matches(&info));
    }

    #[test]
    fn message_filter_should_match_by_numeric_comparison() {
        let filter = MessageFilter::new("$[?@.payload.value > 50]").unwrap();

        let high = make_message(serde_json::json!({"value": 75}));
        let low = make_message(serde_json::json!({"value": 25}));

        assert!(filter.matches(&high));
        assert!(!filter.matches(&low));
    }

    #[test]
    fn message_filter_should_match_by_subject() {
        let filter = MessageFilter::new("$[?@.subject == 'test.subject']").unwrap();

        let msg = make_message(serde_json::json!({"any": "data"}));
        assert!(filter.matches(&msg));
    }

    #[test]
    fn message_filter_should_match_by_timestamp() {
        let filter =
            MessageFilter::new("$[?datetime(@.timestamp) >= datetime('2026-03-15T00:00:00Z')]")
                .unwrap();

        let recent = make_message(serde_json::json!({"any": "data"}));
        // make_message sets timestamp to "2026-03-30T12:00:00Z" which is after the threshold
        assert!(filter.matches(&recent));
    }

    #[test]
    fn message_filter_should_return_error_for_invalid_expression() {
        let result = MessageFilter::new("$[?@.invalid ===");
        assert!(result.is_err());
    }

    #[test]
    fn datetime_should_convert_valid_rfc3339_to_nanoseconds() {
        let path = JsonPath::parse("$[?datetime(@.ts) == datetime('2026-03-30T12:00:00Z')]")
            .expect("valid path");
        let data = serde_json::json!([{"ts": "2026-03-30T12:00:00Z"}]);
        let result = path.query(&data);
        assert_eq!(result.all().len(), 1);
    }

    #[test]
    fn datetime_should_return_nothing_for_invalid_string() {
        let path = JsonPath::parse("$[?datetime(@.ts) > 0]").expect("valid path");
        let data = serde_json::json!([{"ts": "not-a-date"}]);
        let result = path.query(&data);
        assert!(result.all().is_empty());
    }

    #[test]
    fn datetime_should_return_nothing_for_non_string_value() {
        let path = JsonPath::parse("$[?datetime(@.ts) > 0]").expect("valid path");
        let data = serde_json::json!([{"ts": 12345}]);
        let result = path.query(&data);
        assert!(result.all().is_empty());
    }

    #[test]
    fn datetime_should_enable_temporal_range_comparison() {
        let path = JsonPath::parse("$[?datetime(@.ts) >= datetime('2026-03-15T00:00:00Z')]")
            .expect("valid path");

        let data = serde_json::json!([
            {"ts": "2026-03-10T00:00:00Z", "id": 1},
            {"ts": "2026-03-15T00:00:00Z", "id": 2},
            {"ts": "2026-03-20T00:00:00Z", "id": 3},
        ]);

        let result = path.query(&data);
        let matches: Vec<&serde_json::Value> = result.all().into_iter().collect();
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0]["id"], 2);
        assert_eq!(matches[1]["id"], 3);
    }
}
