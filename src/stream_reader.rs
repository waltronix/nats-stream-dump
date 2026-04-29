use std::pin::Pin;
use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::jetstream::consumer::push::OrderedConfig;
use futures::Stream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

/// A raw message fetched from a NATS JetStream stream.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RawMessage {
    pub subject: String,
    pub payload: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    pub sequence: u64,
}

/// Errors that can occur when reading from a stream.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to connect to NATS at '{url}': {reason}")]
    ConnectionFailed { url: String, reason: String },

    #[error("Stream '{name}' not found")]
    StreamNotFound { name: String },

    #[error("Failed to access stream '{name}': {reason}")]
    StreamAccess { name: String, reason: String },

    #[error("Failed to create consumer: {0}")]
    Consumer(String),

    #[error("Failed to read message: {0}")]
    Message(String),
}

/// Connects to NATS and returns the client.
pub async fn connect(url: &str) -> Result<async_nats::Client, Error> {
    async_nats::connect(url)
        .await
        .map_err(|e| Error::ConnectionFailed {
            url: url.to_string(),
            reason: e.to_string(),
        })
}

/// A message stream that wraps an async stream of messages along with
/// the resolved total message count.
///
/// The `total` represents either the user-provided `--count` value or the
/// stream's actual message count (queried from NATS stream info). This is
/// useful for progress reporting (e.g., progress bars).
pub struct MessageStream {
    inner: Pin<Box<dyn Stream<Item = Result<RawMessage, Error>> + Send>>,
    total: u64,
}

impl std::fmt::Debug for MessageStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageStream")
            .field("total", &self.total)
            .finish_non_exhaustive()
    }
}

impl MessageStream {
    /// Creates a new `MessageStream` wrapping the given stream and total count.
    pub fn new(
        inner: Pin<Box<dyn Stream<Item = Result<RawMessage, Error>> + Send>>,
        total: u64,
    ) -> Self {
        Self { inner, total }
    }

    /// Returns the resolved total message count for this stream.
    ///
    /// This is either the user-provided count or the stream's actual message
    /// count at the time of creation.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Consumes this wrapper and returns the inner async stream.
    pub fn into_inner(self) -> Pin<Box<dyn Stream<Item = Result<RawMessage, Error>> + Send>> {
        self.inner
    }
}

impl Stream for MessageStream {
    type Item = Result<RawMessage, Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// Reads raw messages from a NATS JetStream stream as an async stream.
///
/// Returns a [`MessageStream`] that yields messages one at a time and exposes
/// the resolved total message count. When `count` is `Some(n)`, terminates
/// after n messages. When `count` is `None`, queries stream info for the total
/// message count and terminates after that many.
pub async fn read_messages(
    client: &async_nats::Client,
    stream_name: &str,
    count: Option<u64>,
) -> Result<MessageStream, Error> {
    let jetstream = jetstream::new(client.clone());

    let mut stream = jetstream.get_stream(stream_name).await.map_err(|e| {
        if let jetstream::context::GetStreamErrorKind::JetStream(ref js_err) = e.kind()
            && js_err.error_code() == jetstream::ErrorCode::STREAM_NOT_FOUND
        {
            return Error::StreamNotFound {
                name: stream_name.to_string(),
            };
        }
        Error::StreamAccess {
            name: stream_name.to_string(),
            reason: e.to_string(),
        }
    })?;

    let total = match count {
        Some(n) => n,
        None => {
            let info = stream.info().await.map_err(|e| Error::StreamAccess {
                name: stream_name.to_string(),
                reason: e.to_string(),
            })?;
            info.state.messages
        }
    };

    if total == 0 {
        return Ok(MessageStream::new(Box::pin(futures::stream::empty()), 0));
    }

    let deliver_subject = format!("_INBOX.dump.{}", nuid::next());
    let ordered_config = OrderedConfig {
        deliver_policy: DeliverPolicy::All,
        deliver_subject,
        ..Default::default()
    };

    let consumer = stream
        .create_consumer(ordered_config)
        .await
        .map_err(|e| Error::Consumer(e.to_string()))?;

    let ordered = consumer
        .messages()
        .await
        .map_err(|e| Error::Consumer(e.to_string()))?;

    let message_stream = ordered.take(total as usize).map(|result| match result {
        Ok(msg) => {
            let subject = msg.message.subject.to_string();
            let payload = parse_payload(&msg.message.payload);
            let (timestamp, sequence) = extract_metadata(&msg);
            Ok(RawMessage {
                subject,
                payload,
                timestamp,
                sequence,
            })
        }
        Err(e) => Err(Error::Message(e.to_string())),
    });

    // Add an idle timeout so we don't hang forever if stream has fewer messages than expected.
    // We spawn the upstream into a task and forward through a channel, so we can apply
    // a per-item timeout on the receiving end.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut stream = std::pin::pin!(message_stream);
        while let Some(item) = stream.next().await {
            if tx.send(item).is_err() {
                break;
            }
        }
    });

    let timeout_stream = tokio_stream::StreamExt::timeout(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
        Duration::from_secs(5),
    )
    .take_while(|item| {
        let cont = item.is_ok();
        async move { cont }
    })
    .map(|item| item.expect("take_while ensures only Ok items pass through"));

    Ok(MessageStream::new(Box::pin(timeout_stream), total))
}

/// Attempt to parse payload as JSON, falling back to base64-encoded string.
fn parse_payload(payload: &[u8]) -> serde_json::Value {
    serde_json::from_slice(payload).unwrap_or_else(|_| {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(payload);
        serde_json::Value::String(encoded)
    })
}

/// Extract timestamp and sequence number from a JetStream message.
fn extract_metadata(msg: &jetstream::Message) -> (Option<String>, u64) {
    match msg.info() {
        Ok(info) => {
            let timestamp = info
                .published
                .format(&time::format_description::well_known::Rfc3339)
                .ok();
            (timestamp, info.stream_sequence)
        }
        Err(_) => (None, 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_json_payload() {
        let json_bytes = br#"{"key": "value"}"#;
        let result = parse_payload(json_bytes);
        assert_eq!(result, serde_json::json!({"key": "value"}));
    }

    #[test]
    fn should_fallback_to_base64_for_non_json_payload() {
        let binary_data = &[0x00, 0x01, 0x02, 0xFF];
        let result = parse_payload(binary_data);
        use base64::Engine;
        let expected = base64::engine::general_purpose::STANDARD.encode(binary_data);
        assert_eq!(result, serde_json::Value::String(expected));
    }

    #[test]
    fn message_stream_should_expose_total_count() {
        let inner = futures::stream::empty();
        let message_stream = MessageStream::new(Box::pin(inner), 42);

        assert_eq!(message_stream.total(), 42);
    }

    #[test]
    fn message_stream_should_expose_zero_total_for_empty_stream() {
        let inner = futures::stream::empty();
        let message_stream = MessageStream::new(Box::pin(inner), 0);

        assert_eq!(message_stream.total(), 0);
    }
}
