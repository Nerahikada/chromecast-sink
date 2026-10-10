use clap::Parser;

use chromecast_sink::pipeline;

#[derive(Parser)]
#[command(
    name = "chromecast-sink",
    version,
    about = "Use a Google Chromecast / Nest device or speaker group as a speaker output. Linux: creates a PipeWire virtual sink. Windows: captures a playback device (default VB-Audio \"CABLE Input\") via WASAPI loopback."
)]
struct Cli {
    /// Connect to a specific device by name.
    #[arg(short, long, value_name = "NAME")]
    device: Option<String>,

    /// Windows: playback device to capture via WASAPI loopback (case-insensitive substring of its name).
    /// Select that device as output in Windows; whatever plays there is cast.
    #[cfg(windows)]
    #[arg(long, value_name = "NAME", default_value = chromecast_sink::virtual_sink::DEFAULT_CAPTURE_DEVICE)]
    capture_device: String,

    /// Windows: list playback devices usable with --capture-device and exit.
    #[cfg(windows)]
    #[arg(long)]
    list_devices: bool,

    /// Enable verbose (debug) logging.
    #[arg(short, long)]
    verbose: bool,
}

fn main() {
    let cli = Cli::parse();

    let level = if cli.verbose { "debug" } else { "warn" };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(level)).format_timestamp_millis().init();

    #[cfg(windows)]
    {
        if cli.list_devices {
            match chromecast_sink::virtual_sink::list_capture_devices() {
                Ok(names) => names.iter().for_each(|n| println!("{n}")),
                Err(e) => {
                    eprintln!("Error: {e:#}");
                    std::process::exit(1);
                }
            }
            return;
        }
        chromecast_sink::virtual_sink::set_capture_device(&cli.capture_device);
        chromecast_sink::virtual_sink::spawn_stop_event_listener();
    }

    if let Err(e) = pipeline::run(cli.device.as_deref()) {
        eprintln!("Error: {e:#}");
        std::process::exit(1);
    }
}
