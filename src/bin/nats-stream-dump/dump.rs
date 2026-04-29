use std::fs::File;
use std::io::{BufWriter, Write};

use nats_stream_dump::filter::MessageFilter;
use nats_stream_dump::output::JsonArrayWriter;
use nats_stream_dump::stream_reader::{self, Error};
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};

use crate::args::DumpArgs;

/// Creates the appropriate writer and output label based on the `--output` argument.
///
/// When an output path is provided, creates a `BufWriter<File>`.
/// When omitted, returns a locked stdout handle for piping workflows.
fn create_writer(args: &DumpArgs) -> Result<(Box<dyn Write>, String), Error> {
    match &args.output {
        Some(path) => {
            let file = File::create(path).map_err(|e| {
                Error::Message(format!(
                    "Failed to create output file '{}': {e}",
                    path.display()
                ))
            })?;
            let label = path.display().to_string();
            Ok((Box::new(BufWriter::new(file)), label))
        }
        None => Ok((Box::new(std::io::stdout().lock()), "stdout".to_string())),
    }
}

/// Creates a progress bar for file output, or a hidden one for stdout.
///
/// When writing to a file, displays a determinate progress bar on stderr
/// showing message count and throughput. When writing to stdout, returns
/// a hidden progress bar that does nothing (avoids corrupting JSON output).
///
/// When a filter is active, the template includes a `{msg}` placeholder for
/// showing the matched count (e.g. "15 matched").
fn create_progress_bar(writing_to_file: bool, total: u64, has_filter: bool) -> ProgressBar {
    if !writing_to_file {
        return ProgressBar::hidden();
    }

    let pb = ProgressBar::new(total);

    let template = if has_filter {
        "[{bar:40}] {pos}/{len} messages ({msg}) ({per_sec})"
    } else {
        "[{bar:40}] {pos}/{len} messages ({per_sec})"
    };

    pb.set_style(
        ProgressStyle::with_template(template)
            .expect("valid progress bar template")
            .with_key(
                "per_sec",
                |state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| {
                    let _ = write!(w, "{:.0}/s", state.per_sec());
                },
            )
            .progress_chars("=> "),
    );

    if has_filter {
        pb.set_message("0 matched");
    }

    pb
}

/// Parses the optional `--filter` argument into a `MessageFilter`.
///
/// Returns `Ok(None)` when no filter is provided, or `Ok(Some(filter))` when
/// the expression is valid. Exits with an error message if parsing fails.
fn parse_filter(args: &DumpArgs) -> Result<Option<MessageFilter>, Error> {
    args.filter
        .as_deref()
        .map(MessageFilter::new)
        .transpose()
        .map_err(|e| Error::Message(format!("Invalid filter expression: {e}")))
}

/// Executes the dump subcommand: connects to NATS, reads messages, writes to JSON file.
pub async fn run(url: &str, args: &DumpArgs) -> Result<(), Error> {
    tracing::info!("Connecting to {url}...");
    let client = stream_reader::connect(url).await?;

    let stream_name = &args.stream;
    let filter = parse_filter(args)?;

    tracing::info!("Fetching messages from stream '{stream_name}'...");

    let message_stream = stream_reader::read_messages(&client, stream_name, args.count).await?;
    let total = message_stream.total();
    let writing_to_file = args.output.is_some();

    let (writer, output_label) = create_writer(args)?;

    let mut json_writer = JsonArrayWriter::new(writer, args.pretty, &output_label)
        .map_err(|e| Error::Message(format!("Failed to initialize JSON writer: {e}")))?;

    let pb = create_progress_bar(writing_to_file, total, filter.is_some());
    let mut message_stream = std::pin::pin!(message_stream);
    let mut matched_count: u64 = 0;

    while let Some(result) = message_stream.next().await {
        let msg = result?;
        pb.inc(1);

        let should_write = match &filter {
            Some(f) => f.matches(&msg),
            None => true,
        };

        if should_write {
            json_writer
                .write(&msg)
                .map_err(|e| Error::Message(format!("Failed to write message: {e}")))?;

            if filter.is_some() {
                matched_count += 1;
                pb.set_message(format!("{matched_count} matched"));
            }
        }
    }

    pb.finish_and_clear();

    json_writer
        .finish()
        .map_err(|e| Error::Message(format!("Failed to finalize output: {e}")))?;

    Ok(())
}
