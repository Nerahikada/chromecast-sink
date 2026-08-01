//! Run the full pipeline against a fixed IP (discovery bypassed).
//! Useful in environments where multicast/mDNS is blocked.
//!
//! Usage: cargo run --example run_device --release -- 192.168.238.100

use chromecast_sink::{discovery::Device, pipeline};

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let host = std::env::args().nth(1).unwrap_or_else(|| "192.168.238.100".into());
    let device = Device {
        friendly_name: "Test Nest".into(),
        model: Some("Google Nest Mini".into()),
        host,
        is_audio_only: true,
    };
    if let Err(e) = pipeline::run_with_device(device) {
        eprintln!("Error: {e:#}");
        std::process::exit(1);
    }
}
