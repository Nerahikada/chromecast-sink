//! chromecast-sink — use a Chromecast / Google Nest device as a Linux

use clap::Parser;

use chromecast_sink::pipeline;

#[derive(Parser)]
#[command(
    name = "chromecast-sink",
    version,
    about = "Use a Google Chromecast / Nest device as a Linux speaker output. \
             Creates a PipeWire virtual sink that streams audio to the Chromecast."
)]
struct Cli {
    /// Connect to a specific device by name.
    #[arg(short, long, value_name = "NAME")]
    device: Option<String>,

    /// Enable verbose (debug) logging.
    #[arg(short, long)]
    verbose: bool,
}

fn main() {
    let cli = Cli::parse();

    let level = if cli.verbose { "debug" } else { "warn" };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(level))
        .format_timestamp_millis()
        .init();

    if let Err(e) = pipeline::run(cli.device.as_deref()) {
        eprintln!("Error: {e:#}");
        std::process::exit(1);
    }
}
