use clap::Parser;

use chromecast_sink::pipeline;

#[derive(Parser)]
#[command(name = "chromecast-sink", version)]
#[cfg_attr(
    not(windows),
    command(
        about = "Use a Google Chromecast / Nest device as a Linux speaker output. Creates a PipeWire virtual sink that streams audio to the Chromecast."
    )
)]
#[cfg_attr(
    windows,
    command(
        about = "Use a Google Chromecast / Nest device as a Windows speaker output. Captures a playback device (default: the default playback device) via WASAPI loopback and streams it to the Chromecast."
    )
)]
struct Cli {
    /// Connect to a specific device by name.
    #[arg(short, long, value_name = "NAME")]
    device: Option<String>,

    /// Skip mDNS discovery and connect to this IPv4 address or hostname directly. Audio-only receivers only.
    #[arg(long, value_name = "HOST", conflicts_with = "device")]
    host: Option<String>,

    /// Cast v2 TCP port for --host. Single devices listen on 8009. A speaker group listens on its leader at a different port.
    #[arg(
        long,
        value_name = "PORT",
        default_value_t = 8009,
        requires = "host",
        conflicts_with = "device",
        value_parser = clap::value_parser!(u16).range(1..)
    )]
    port: u16,

    /// Windows: playback device to capture via WASAPI loopback (case-insensitive substring of its name).
    /// Defaults to the default playback device. Whatever plays there is cast.
    #[cfg(windows)]
    #[arg(long, value_name = "NAME")]
    capture_device: Option<String>,

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
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(level))
        .format_timestamp_millis()
        .init();

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
    }

    let result = match cli.host {
        Some(host) => pipeline::run_with_host(
            &host,
            cli.port,
            #[cfg(windows)]
            cli.capture_device.as_deref(),
        ),
        None => pipeline::run(
            cli.device.as_deref(),
            #[cfg(windows)]
            cli.capture_device.as_deref(),
        ),
    };
    if let Err(e) = result {
        eprintln!("Error: {e:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_and_port_constraints() {
        let parse = |args: &[&str]| {
            Cli::try_parse_from(std::iter::once("chromecast-sink").chain(args.iter().copied()))
        };
        assert!(parse(&[]).is_ok());
        assert!(parse(&["--device", "X"]).is_ok());
        assert_eq!(
            parse(&["--host", "192.168.1.20"]).ok().map(|c| c.port),
            Some(8009)
        );
        assert!(parse(&["--port", "32012"]).is_err());
        assert!(parse(&["--device", "X", "--host", "192.168.1.20"]).is_err());
        assert!(parse(&["--device", "X", "--port", "32012"]).is_err());
        assert!(parse(&["--host", "192.168.1.20", "--port", "0"]).is_err());
    }
}
