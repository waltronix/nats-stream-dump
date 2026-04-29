# nats-stream-dump

A diagnostic CLI for investigating NATS JetStream streams. Dumps raw messages
to a JSON file or stdout with optional JSONPath filtering.

## Features

- Streaming output with constant memory usage — messages are written one at a time
- JSONPath filtering (RFC 9535) on the full message structure (subject, payload, timestamp, sequence)
- Custom `datetime()` function for temporal range queries on RFC 3339 timestamps
- Progress bar when writing to a file, hidden when piping to stdout
- Pretty-print with `--pretty`, limit messages with `-n`
- Falls back to base64 encoding for non-JSON payloads

## Installation

```bash
cargo install --path .
```

Or build a release binary:

```bash
cargo build --release
# binary at ./target/release/nats-stream-dump
```

## Usage

```bash
# Dump all messages from a stream to a file
nats-stream-dump dump MY_STREAM -o dump.json

# Pipe to jq
nats-stream-dump dump MY_STREAM | jq '.[].payload'

# Limit to first 100 messages with pretty-printed output
nats-stream-dump dump MY_STREAM -n 100 --pretty -o dump.json

# Filter by payload field
nats-stream-dump dump MY_STREAM -f "$[?@.payload.severity == 'critical']" -o critical.json

# Filter by timestamp range using the custom datetime() function
nats-stream-dump dump MY_STREAM \
  -f "$[?datetime(@.timestamp) >= datetime('2026-03-01T00:00:00Z')]"

# Connect to a remote NATS server
nats-stream-dump --url nats://nats.example.com:4222 dump MY_STREAM -o dump.json
```

Run `nats-stream-dump dump --help` for the full reference with filter syntax and examples.

## Filter Expressions

Filters use JSONPath syntax (RFC 9535). The filter operates on the full message
structure: `subject`, `payload`, `timestamp`, `sequence`.

| Selector            | Example                                                                   |
| ------------------- | ------------------------------------------------------------------------- |
| Payload field       | `$[?@.payload.severity == 'critical']`                                    |
| Subject             | `$[?@.subject == 'OrderCreated.v1']`                                      |
| Sequence            | `$[?@.sequence > 100]`                                                    |
| Nested payload      | `$[?@.payload.event.severity == 'critical']`                              |
| Numeric             | `$[?@.payload.value > 50]`                                                |
| Timestamp range     | `$[?datetime(@.timestamp) >= datetime('2026-03-01T00:00:00Z')]`           |
| Combined            | `$[?@.payload.severity == 'critical' && datetime(@.timestamp) >= datetime('2026-03-15T00:00:00Z')]` |

The custom `datetime()` function converts RFC 3339 strings to nanosecond
timestamps, enabling temporal comparisons with `>`, `>=`, `<`, `<=`, `==`.

## Removing Messages from a Stream

Once you have identified a broken message in the dump, note its `sequence` field.
You can then delete it from the stream using the official [`nats` CLI](https://github.com/nats-io/natscli):

```bash
nats stream rmm "STREAM_NAME" "SEQUENCE_ID"
```

This is useful for unblocking consumers that are stuck on a poison-pill message
that cannot be deserialized or processed.

## Library Use

The crate also exposes the underlying primitives as a library:

- [`nats_stream_dump::stream_reader`] — connect to NATS and stream `RawMessage` values
- [`nats_stream_dump::filter`] — compile and apply JSONPath filters with the `datetime()` function
- [`nats_stream_dump::output`] — incrementally write `RawMessage` values as a JSON array

## Logging

Output is emitted via `tracing`. Control the log level with `RUST_LOG`:

```bash
RUST_LOG=info nats-stream-dump dump MY_STREAM -o dump.json
RUST_LOG=error nats-stream-dump dump MY_STREAM -o dump.json   # suppress progress
```
