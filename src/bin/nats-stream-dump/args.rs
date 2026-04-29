use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// CLI tool for investigating NATS JetStream streams.
#[derive(Debug, Parser)]
#[command(
    name = "nats-stream-dump",
    about = "NATS JetStream stream investigation tool",
    after_help = "Use `nats-stream-dump <COMMAND> --help` for more information about a specific command."
)]
pub struct Cli {
    /// NATS server URL
    #[arg(long, default_value = "nats://localhost:4222", global = true)]
    pub url: String,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Dump raw messages from a NATS JetStream stream to a JSON file or stdout
    Dump(DumpArgs),
}

#[derive(Debug, clap::Args)]
#[command(after_help = "\
FILTER EXPRESSIONS:
  Filters use JSONPath syntax (RFC 9535) to select messages. The filter
  operates on the full message structure: subject, payload, timestamp, sequence.

  Payload fields:    $[?@.payload.severity == 'critical']
  Subject:           $[?@.subject == 'OrderCreated.v1']
  Sequence:          $[?@.sequence > 100]
  Nested fields:     $[?@.payload.event.severity == 'critical']
  Numeric:           $[?@.payload.value > 50]
  Timestamp range:   $[?datetime(@.timestamp) >= datetime('2026-03-01T00:00:00Z')]
  Combined:          $[?@.payload.severity == 'critical' && datetime(@.timestamp) >= datetime('2026-03-15T00:00:00Z')]

  The custom datetime() function converts RFC 3339 strings to nanosecond
  timestamps, enabling temporal comparisons with >, >=, <, <=, ==.

EXAMPLES:
  Dump all messages to a file:
    nats-stream-dump dump MY_STREAM -o dump.json

  Dump to stdout and pipe to jq:
    nats-stream-dump dump MY_STREAM | jq '.[].payload'

  Dump first 100 messages with pretty-print:
    nats-stream-dump dump MY_STREAM -n 100 --pretty -o dump.json

  Filter by payload field:
    nats-stream-dump dump MY_STREAM -f \"$[?@.payload.severity == 'critical']\" -o critical.json

  Filter by timestamp range:
    nats-stream-dump dump MY_STREAM -f \"$[?datetime(@.timestamp) >= datetime('2026-03-01T00:00:00Z')]\"

  Connect to a remote NATS server:
    nats-stream-dump --url nats://nats.example.com:4222 dump MY_STREAM -o dump.json")]
pub struct DumpArgs {
    /// Name of the NATS JetStream stream to dump
    pub stream: String,

    /// Output file path. When omitted, writes JSON to stdout, which is
    /// useful for piping to tools like jq. A progress bar is shown on
    /// stderr when writing to a file.
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Maximum number of messages to read from the stream.
    /// When omitted, all messages currently in the stream are dumped.
    /// This limits how many messages are fetched, not how many pass the filter.
    #[arg(short = 'n', long)]
    pub count: Option<u64>,

    /// Pretty-print the JSON output with indentation
    #[arg(long)]
    pub pretty: bool,

    /// JSONPath filter expression (RFC 9535). Only messages matching the
    /// expression are included in the output. The filter operates on the
    /// full message (subject, payload, timestamp, sequence). Supports a
    /// custom datetime() function for temporal range queries.
    #[arg(short = 'f', long, value_name = "JSONPATH")]
    pub filter: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("should parse valid args")
    }

    fn parse_dump(args: &[&str]) -> DumpArgs {
        let cli = parse(args);
        let Command::Dump(dump_args) = cli.command;
        dump_args
    }

    #[test]
    fn should_parse_dump_subcommand_with_all_args() {
        let args = parse_dump(&["nats-stream-dump", "dump", "my-stream", "-o", "output.json"]);

        assert_eq!(args.stream, "my-stream");
        assert_eq!(args.output, Some(PathBuf::from("output.json")));
        assert!(args.count.is_none());
        assert!(!args.pretty);
    }

    #[test]
    fn should_use_default_nats_url() {
        let cli = parse(&["nats-stream-dump", "dump", "my-stream", "-o", "output.json"]);
        assert_eq!(cli.url, "nats://localhost:4222");
    }

    #[test]
    fn should_parse_custom_nats_url() {
        let cli = parse(&[
            "nats-stream-dump",
            "--url",
            "nats://custom:4222",
            "dump",
            "my-stream",
            "-o",
            "output.json",
        ]);
        assert_eq!(cli.url, "nats://custom:4222");
    }

    #[test]
    fn should_parse_count_option() {
        let args = parse_dump(&[
            "nats-stream-dump",
            "dump",
            "my-stream",
            "-o",
            "output.json",
            "-n",
            "100",
        ]);
        assert_eq!(args.count, Some(100));
    }

    #[test]
    fn should_parse_pretty_flag() {
        let args = parse_dump(&[
            "nats-stream-dump",
            "dump",
            "my-stream",
            "-o",
            "output.json",
            "--pretty",
        ]);
        assert!(args.pretty);
    }

    #[test]
    fn should_reject_missing_stream_name() {
        let result = Cli::try_parse_from(["nats-stream-dump", "dump", "-o", "output.json"]);
        assert!(result.is_err());
    }

    #[test]
    fn should_accept_missing_output_flag_and_default_to_none() {
        let args = parse_dump(&["nats-stream-dump", "dump", "my-stream"]);
        assert!(args.output.is_none());
    }

    #[test]
    fn should_parse_output_flag_as_some() {
        let args = parse_dump(&["nats-stream-dump", "dump", "my-stream", "-o", "output.json"]);
        assert_eq!(args.output, Some(PathBuf::from("output.json")));
    }

    #[test]
    fn should_parse_filter_with_long_flag() {
        let args = parse_dump(&[
            "nats-stream-dump",
            "dump",
            "my-stream",
            "-o",
            "output.json",
            "--filter",
            "$[?@.severity == 'critical']",
        ]);
        assert_eq!(
            args.filter,
            Some("$[?@.severity == 'critical']".to_string())
        );
    }

    #[test]
    fn should_parse_filter_with_short_flag() {
        let args = parse_dump(&[
            "nats-stream-dump",
            "dump",
            "my-stream",
            "-o",
            "output.json",
            "-f",
            "$[?@.value > 50]",
        ]);
        assert_eq!(args.filter, Some("$[?@.value > 50]".to_string()));
    }

    #[test]
    fn should_default_filter_to_none_when_omitted() {
        let args = parse_dump(&["nats-stream-dump", "dump", "my-stream", "-o", "output.json"]);
        assert!(args.filter.is_none());
    }
}
