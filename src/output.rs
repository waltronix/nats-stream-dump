use std::io::Write;

use crate::stream_reader::RawMessage;

/// Writes `RawMessage` values incrementally as a valid JSON array to a writer.
///
/// Handles the opening `[`, commas between entries, and closing `]` so that
/// each message is serialized and flushed to disk as it arrives.
/// Memory usage is constant regardless of message count.
#[derive(Debug)]
pub struct JsonArrayWriter<W: Write> {
    writer: W,
    count: u64,
    pretty: bool,
    output_path: String,
}

impl<W: Write> JsonArrayWriter<W> {
    /// Creates a new `JsonArrayWriter` that writes to the given writer.
    ///
    /// The `output_path` is used only for the summary message emitted via `tracing::info!`.
    pub fn new(mut writer: W, pretty: bool, output_path: &str) -> std::io::Result<Self> {
        if pretty {
            writeln!(writer, "[")?;
        } else {
            write!(writer, "[")?;
        }
        writer.flush()?;
        Ok(Self {
            writer,
            count: 0,
            pretty,
            output_path: output_path.to_string(),
        })
    }

    /// Writes a single message to the JSON array.
    pub fn write(&mut self, message: &RawMessage) -> std::io::Result<()> {
        if self.count > 0 {
            if self.pretty {
                writeln!(self.writer, ",")?;
            } else {
                write!(self.writer, ",")?;
            }
        }

        if self.pretty {
            let json = serde_json::to_string_pretty(message)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            // Indent each line of the pretty-printed JSON by 2 spaces
            for line in json.lines() {
                writeln!(self.writer, "  {line}")?;
            }
        } else {
            serde_json::to_writer(&mut self.writer, message)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        }

        self.writer.flush()?;
        self.count += 1;
        Ok(())
    }

    /// Closes the JSON array and emits a summary via `tracing::info!`.
    pub fn finish(mut self) -> std::io::Result<()> {
        if self.pretty {
            writeln!(self.writer, "]")?;
        } else {
            write!(self.writer, "]")?;
        }
        self.writer.flush()?;

        tracing::info!("Wrote {} messages to {}", self.count, self.output_path);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_message(seq: u64) -> RawMessage {
        RawMessage {
            subject: "test.subject".to_string(),
            payload: serde_json::json!({"count": seq}),
            timestamp: Some("2026-03-30T12:00:00Z".to_string()),
            sequence: seq,
        }
    }

    #[test]
    fn should_write_empty_array_when_no_messages() {
        let mut buf = Vec::new();
        let writer = JsonArrayWriter::new(&mut buf, false, "test.json").unwrap();
        writer.finish().unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert_eq!(output, "[]");
    }

    #[test]
    fn should_write_single_message_as_json_array() {
        let mut buf = Vec::new();
        let mut writer = JsonArrayWriter::new(&mut buf, false, "test.json").unwrap();
        writer.write(&make_message(1)).unwrap();
        writer.finish().unwrap();

        let output = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let array = parsed.as_array().unwrap();
        assert_eq!(array.len(), 1);
    }

    #[test]
    fn should_write_multiple_messages_as_valid_json_array() {
        let mut buf = Vec::new();
        let mut writer = JsonArrayWriter::new(&mut buf, false, "test.json").unwrap();
        writer.write(&make_message(1)).unwrap();
        writer.write(&make_message(2)).unwrap();
        writer.write(&make_message(3)).unwrap();
        writer.finish().unwrap();

        let output = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let array = parsed.as_array().unwrap();
        assert_eq!(array.len(), 3);
    }

    #[test]
    fn should_produce_indented_output_in_pretty_mode() {
        let mut buf = Vec::new();
        let mut writer = JsonArrayWriter::new(&mut buf, true, "test.json").unwrap();
        writer.write(&make_message(1)).unwrap();
        writer.finish().unwrap();

        let output = String::from_utf8(buf).unwrap();
        // Pretty mode should have indentation
        assert!(output.contains("  "));
        // Should still be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert!(parsed.is_array());
    }

    #[tracing_test::traced_test]
    #[test]
    fn should_emit_summary_via_tracing_on_finish() {
        let mut buf = Vec::new();
        let mut writer = JsonArrayWriter::new(&mut buf, false, "test.json").unwrap();
        writer.write(&make_message(1)).unwrap();
        writer.write(&make_message(2)).unwrap();
        writer.finish().unwrap();

        assert!(logs_contain("Wrote 2 messages to test.json"));
    }

    #[test]
    fn should_preserve_message_content_in_output() {
        let mut buf = Vec::new();
        let mut writer = JsonArrayWriter::new(&mut buf, false, "test.json").unwrap();

        let msg = RawMessage {
            subject: "my.subject".to_string(),
            payload: serde_json::json!({"key": "value", "nested": {"a": 1}}),
            timestamp: Some("2026-03-30T12:00:00Z".to_string()),
            sequence: 42,
        };
        writer.write(&msg).unwrap();
        writer.finish().unwrap();

        let output = String::from_utf8(buf).unwrap();
        let parsed: Vec<RawMessage> = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed[0], msg);
    }
}
