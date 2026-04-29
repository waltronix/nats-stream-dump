// Integration tests for nats-stream-dump.
// These tests require Docker to run testcontainers with NATS.
//
// Run with: cargo test --test nats_stream_dump

use std::future::Future;
use std::io::BufWriter;

use nats_stream_dump::filter::MessageFilter;
use nats_stream_dump::output::JsonArrayWriter;
use nats_stream_dump::stream_reader::{self, RawMessage};
use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy};
use futures::StreamExt;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};

trait ContainerExt {
    fn url(&self) -> impl Future<Output = String>;
}

impl ContainerExt for ContainerAsync<testcontainers_modules::nats::Nats> {
    async fn url(&self) -> String {
        let port = self
            .ports()
            .await
            .unwrap()
            .map_to_host_port_ipv4(4_222)
            .expect("port 4222 is available");
        let host = self.get_host().await.unwrap();
        format!("nats://{host}:{port}")
    }
}

async fn start_nats() -> ContainerAsync<testcontainers_modules::nats::Nats> {
    testcontainers_modules::nats::Nats::default()
        .with_cmd(&testcontainers_modules::nats::NatsServerCmd::default().with_jetstream())
        .start()
        .await
        .unwrap()
}

/// Starts a NATS container and returns both the container (to keep it alive) and a connected client.
async fn start_nats_with_client() -> (
    ContainerAsync<testcontainers_modules::nats::Nats>,
    async_nats::Client,
) {
    let nats_server = start_nats().await;
    let url = nats_server.url().await;
    let client = async_nats::connect(&url).await.unwrap();
    (nats_server, client)
}

/// Generic event payload used by all tests.
///
/// Every test publishes messages with this exact shape so filter expressions
/// remain consistent across the suite. Tests vary the field values to cover
/// different filter scenarios (severity match, numeric comparison, timestamp
/// range, combined predicates).
fn event(id: u64, severity: &str, value: i64, ts: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "severity": severity,
        "value": value,
        "ts": ts,
    })
}

/// Helper to create a stream and publish messages to it.
async fn setup_stream_with_messages(
    client: &async_nats::Client,
    stream_name: &str,
    subject: &str,
    messages: &[serde_json::Value],
) {
    let jetstream = async_nats::jetstream::new(client.clone());
    let subject_owned = subject.to_string();

    jetstream
        .get_or_create_stream(StreamConfig {
            name: stream_name.to_string(),
            subjects: vec![subject_owned.clone()],
            retention: RetentionPolicy::Limits,
            ..Default::default()
        })
        .await
        .expect("should create stream");

    for msg in messages {
        let payload = serde_json::to_vec(msg).unwrap();
        jetstream
            .publish(subject_owned.clone(), payload.into())
            .await
            .expect("should publish")
            .await
            .expect("should ack publish");
    }
}

#[tokio::test]
async fn should_dump_all_messages_from_stream() {
    let nats_server = start_nats().await;
    let url = nats_server.url().await;
    let client = async_nats::connect(&url).await.unwrap();

    let messages: Vec<serde_json::Value> = (0u64..5)
        .map(|i| event(i, "info", 0, "2026-03-15T00:00:00Z"))
        .collect();

    setup_stream_with_messages(&client, "TEST_DUMP_ALL", "test.dump.all", &messages).await;

    let mut msg_stream = stream_reader::read_messages(&client, "TEST_DUMP_ALL", None)
        .await
        .expect("should create message stream");

    let mut received = Vec::new();
    while let Some(result) = msg_stream.next().await {
        received.push(result.expect("should read message"));
    }

    assert_eq!(received.len(), 5);
    for (i, msg) in received.iter().enumerate() {
        assert_eq!(msg.subject, "test.dump.all");
        assert_eq!(msg.payload["id"], i);
        assert!(msg.timestamp.is_some());
        assert_eq!(msg.sequence, (i as u64) + 1);
    }
}

#[tokio::test]
async fn should_dump_specific_count_of_messages() {
    let nats_server = start_nats().await;
    let url = nats_server.url().await;
    let client = async_nats::connect(&url).await.unwrap();

    let messages: Vec<serde_json::Value> = (0u64..10)
        .map(|i| event(i, "info", 0, "2026-03-15T00:00:00Z"))
        .collect();

    setup_stream_with_messages(&client, "TEST_DUMP_COUNT", "test.dump.count", &messages).await;

    let mut msg_stream = stream_reader::read_messages(&client, "TEST_DUMP_COUNT", Some(3))
        .await
        .expect("should create message stream");

    let mut received = Vec::new();
    while let Some(result) = msg_stream.next().await {
        received.push(result.expect("should read message"));
    }

    assert_eq!(received.len(), 3);
}

#[tokio::test]
async fn should_return_error_for_nonexistent_stream() {
    let nats_server = start_nats().await;
    let url = nats_server.url().await;
    let client = async_nats::connect(&url).await.unwrap();

    let result = stream_reader::read_messages(&client, "NONEXISTENT_STREAM", None).await;

    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("Expected error for nonexistent stream"),
    };
    assert!(
        matches!(err, stream_reader::Error::StreamNotFound { .. }),
        "Expected StreamNotFound, got: {err:?}"
    );
}

#[tokio::test]
async fn should_return_empty_stream_for_empty_stream() {
    let nats_server = start_nats().await;
    let url = nats_server.url().await;
    let client = async_nats::connect(&url).await.unwrap();

    // Create an empty stream (no messages published)
    setup_stream_with_messages(&client, "TEST_DUMP_EMPTY", "test.dump.empty", &[]).await;

    let mut msg_stream = stream_reader::read_messages(&client, "TEST_DUMP_EMPTY", None)
        .await
        .expect("should create message stream");

    let mut received = Vec::new();
    while let Some(result) = msg_stream.next().await {
        received.push(result.expect("should read message"));
    }

    assert!(received.is_empty());
}

#[tokio::test]
async fn should_write_dumped_messages_to_json_file() {
    let nats_server = start_nats().await;
    let url = nats_server.url().await;
    let client = async_nats::connect(&url).await.unwrap();

    let messages: Vec<serde_json::Value> = (0u64..3)
        .map(|i| event(i, "info", 0, "2026-03-15T00:00:00Z"))
        .collect();

    setup_stream_with_messages(&client, "TEST_DUMP_FILE", "test.dump.file", &messages).await;

    let mut msg_stream = stream_reader::read_messages(&client, "TEST_DUMP_FILE", None)
        .await
        .expect("should create message stream");

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let file = std::fs::File::create(tmp.path()).unwrap();
    let buf_writer = BufWriter::new(file);

    let mut writer = JsonArrayWriter::new(buf_writer, false, &path).unwrap();

    while let Some(result) = msg_stream.next().await {
        let msg = result.expect("should read message");
        writer.write(&msg).unwrap();
    }
    writer.finish().unwrap();

    // Read back and verify
    let content = std::fs::read_to_string(tmp.path()).unwrap();
    let parsed: Vec<RawMessage> = serde_json::from_str(&content).unwrap();
    assert_eq!(parsed.len(), 3);
    for (i, msg) in parsed.iter().enumerate() {
        assert_eq!(msg.payload["id"], i);
    }
}

/// Helper: read all messages from a stream, apply a filter, and collect matching messages.
async fn read_and_filter(
    client: &async_nats::Client,
    stream_name: &str,
    filter: &MessageFilter,
) -> Vec<RawMessage> {
    let mut msg_stream = stream_reader::read_messages(client, stream_name, None)
        .await
        .expect("should create message stream");

    let mut matched = Vec::new();
    while let Some(result) = msg_stream.next().await {
        let msg = result.expect("should read message");
        if filter.matches(&msg) {
            matched.push(msg);
        }
    }
    matched
}

#[tokio::test]
async fn should_filter_by_exact_field_match() {
    let (_nats_server, client) = start_nats_with_client().await;

    let messages = vec![
        event(1, "critical", 0, "2026-03-15T00:00:00Z"),
        event(2, "info", 0, "2026-03-15T00:00:00Z"),
        event(3, "critical", 0, "2026-03-15T00:00:00Z"),
        event(4, "warning", 0, "2026-03-15T00:00:00Z"),
    ];

    setup_stream_with_messages(&client, "TEST_FILTER_EXACT", "test.filter.exact", &messages).await;

    let filter = MessageFilter::new("$[?@.payload.severity == 'critical']").unwrap();
    let matched = read_and_filter(&client, "TEST_FILTER_EXACT", &filter).await;

    assert_eq!(matched.len(), 2);
    assert_eq!(matched[0].payload["id"], 1);
    assert_eq!(matched[1].payload["id"], 3);
}

#[tokio::test]
async fn should_filter_by_numeric_comparison() {
    let (_nats_server, client) = start_nats_with_client().await;

    let messages = vec![
        event(1, "info", 25, "2026-03-15T00:00:00Z"),
        event(2, "info", 75, "2026-03-15T00:00:00Z"),
        event(3, "info", 50, "2026-03-15T00:00:00Z"),
        event(4, "info", 100, "2026-03-15T00:00:00Z"),
    ];

    setup_stream_with_messages(
        &client,
        "TEST_FILTER_NUMERIC",
        "test.filter.numeric",
        &messages,
    )
    .await;

    let filter = MessageFilter::new("$[?@.payload.value > 50]").unwrap();
    let matched = read_and_filter(&client, "TEST_FILTER_NUMERIC", &filter).await;

    assert_eq!(matched.len(), 2);
    assert_eq!(matched[0].payload["id"], 2);
    assert_eq!(matched[1].payload["id"], 4);
}

#[tokio::test]
async fn should_filter_with_datetime_function() {
    let (_nats_server, client) = start_nats_with_client().await;

    let messages = vec![
        event(1, "info", 0, "2026-03-01T00:00:00Z"),
        event(2, "info", 0, "2026-03-15T00:00:00Z"),
        event(3, "info", 0, "2026-03-20T00:00:00Z"),
        event(4, "info", 0, "2026-03-10T00:00:00Z"),
    ];

    setup_stream_with_messages(
        &client,
        "TEST_FILTER_DATETIME",
        "test.filter.datetime",
        &messages,
    )
    .await;

    let filter =
        MessageFilter::new("$[?datetime(@.payload.ts) >= datetime('2026-03-15T00:00:00Z')]")
            .unwrap();
    let matched = read_and_filter(&client, "TEST_FILTER_DATETIME", &filter).await;

    assert_eq!(matched.len(), 2);
    assert_eq!(matched[0].payload["id"], 2);
    assert_eq!(matched[1].payload["id"], 3);
}

#[tokio::test]
async fn should_filter_with_combined_field_match_and_datetime() {
    let (_nats_server, client) = start_nats_with_client().await;

    let messages = vec![
        event(1, "critical", 0, "2026-03-10T00:00:00Z"),
        event(2, "critical", 0, "2026-03-20T00:00:00Z"),
        event(3, "warning", 0, "2026-03-20T00:00:00Z"),
        event(4, "critical", 0, "2026-03-25T00:00:00Z"),
    ];

    setup_stream_with_messages(
        &client,
        "TEST_FILTER_COMBINED",
        "test.filter.combined",
        &messages,
    )
    .await;

    let filter = MessageFilter::new(
        "$[?@.payload.severity == 'critical' && datetime(@.payload.ts) >= datetime('2026-03-15T00:00:00Z')]",
    )
    .unwrap();
    let matched = read_and_filter(&client, "TEST_FILTER_COMBINED", &filter).await;

    assert_eq!(matched.len(), 2);
    assert_eq!(matched[0].payload["id"], 2);
    assert_eq!(matched[1].payload["id"], 4);
}

#[tokio::test]
async fn should_return_empty_when_filter_matches_nothing() {
    let (_nats_server, client) = start_nats_with_client().await;

    let messages = vec![
        event(1, "info", 0, "2026-03-15T00:00:00Z"),
        event(2, "warning", 0, "2026-03-15T00:00:00Z"),
    ];

    setup_stream_with_messages(
        &client,
        "TEST_FILTER_NO_MATCH",
        "test.filter.nomatch",
        &messages,
    )
    .await;

    let filter = MessageFilter::new("$[?@.payload.severity == 'critical']").unwrap();
    let matched = read_and_filter(&client, "TEST_FILTER_NO_MATCH", &filter).await;

    assert!(matched.is_empty());
}

#[test]
fn should_reject_invalid_filter_expression() {
    let result = MessageFilter::new("$[?@.invalid ===");
    assert!(result.is_err());
}
