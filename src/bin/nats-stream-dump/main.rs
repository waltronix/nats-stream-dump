mod args;
mod dump;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use args::{Cli, Command};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(false)
        .without_time()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let result = match &cli.command {
        Command::Dump(args) => dump::run(&cli.url, args).await,
    };

    if let Err(e) = result {
        tracing::error!("Error: {e}");
        std::process::exit(1);
    }
}
