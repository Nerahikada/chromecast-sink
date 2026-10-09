//! Usage: cargo run --example run_device --release -- 192.168.238.100 [--port=32012] [--video]
//!
//! `--port=` targets a speaker group on its leader's IP; `--video` marks the device as having a screen.

use chromecast_sink::{discovery::Device, pipeline};

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).format_timestamp_millis().init();

    let args: Vec<String> = std::env::args().collect();
    let host = args.iter().skip(1).find(|a| !a.starts_with("--")).cloned().unwrap_or_else(|| "192.168.238.100".into());
    let port: u16 = args.iter().find_map(|a| a.strip_prefix("--port=")).map(|p| p.parse().expect("--port must be 1..=65535")).unwrap_or(8009);
    let is_audio_only = !args.iter().any(|a| a == "--video");
    let device = Device { friendly_name: "Test Nest".into(), model: Some("Google Nest Mini".into()), host, port, is_audio_only };
    if let Err(e) = pipeline::run_with_device(device) {
        eprintln!("Error: {e:#}");
        std::process::exit(1);
    }
}
