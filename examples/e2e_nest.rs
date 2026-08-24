//! Usage:
//!   cargo run --example e2e_nest --release -- 192.168.238.100
//!   cargo run --example e2e_nest --release -- 192.168.238.100 --no-rtcp
//!
//! `--no-rtcp` disables Sender Reports to verify the receiver kills the

use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::Result;
use chromecast_sink::{
    capture,
    cast_channel,
    cast_rtp::{self, CastRtpSender, StatsHandle},
    mirroring::{self, StreamMode, StreamOffer, OPUS_BITRATE, RTP_PAYLOAD_TYPE},
    virtual_sink::VirtualSink,
};

const TIMEOUT: Duration = Duration::from_secs(10);

fn snapshot(stats: &StatsHandle, label: &str) -> f64 {
    let (f0, o0) = stats.snapshot();
    thread::sleep(Duration::from_secs(2));
    let (f1, o1) = stats.snapshot();
    let df = f1.wrapping_sub(f0);
    let d_o = o1.wrapping_sub(o0);
    let bpf = if df > 0 { d_o as f64 / df as f64 } else { 0.0 };
    println!("[{label}] {df} frames / 2s, {bpf:.1} B/frame");
    bpf
}

fn make_tone_wav(path: &str) -> Result<()> {
    let sr: u32 = 48_000;
    let seconds: u32 = 6;
    let n = sr * seconds;
    let mut data: Vec<u8> = Vec::with_capacity(n as usize * 4);
    let amp = (32767.0f64 * 0.12) as i16;
    for i in 0..n {
        let v = (amp as f64 * (2.0 * std::f64::consts::PI * 440.0 * i as f64 / sr as f64).sin()) as i16;
        data.extend_from_slice(&v.to_le_bytes());
        data.extend_from_slice(&v.to_le_bytes());
    }
    let subchunk2_size = data.len() as u32;
    let chunk_size = 36 + subchunk2_size;
    let byte_rate = sr * 2 * 2;
    let block_align: u16 = 2 * 2;
    let mut wav = Vec::with_capacity(44 + data.len());
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&chunk_size.to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes()); // PCM
    wav.extend_from_slice(&2u16.to_le_bytes()); // stereo
    wav.extend_from_slice(&sr.to_le_bytes());
    wav.extend_from_slice(&byte_rate.to_le_bytes());
    wav.extend_from_slice(&block_align.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&subchunk2_size.to_le_bytes());
    wav.extend_from_slice(&data);
    std::fs::write(path, wav)?;
    Ok(())
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let args: Vec<String> = std::env::args().collect();
    let no_rtcp = args.iter().any(|a| a == "--no-rtcp");
    let host = args
        .iter()
        .skip(1)
        .find(|a| !a.starts_with("--"))
        .cloned()
        .unwrap_or_else(|| "192.168.238.100".into());

    let tone_path = std::env::temp_dir().join("chromecast_sink_e2e_tone.wav");
    let tone = tone_path.to_str().expect("temp path is valid UTF-8");
    make_tone_wav(tone)?;

    println!("Creating virtual sink...");
    let sink = VirtualSink::new("Test Nest")?;
    println!("  monitor: {}", sink.monitor_source);
    println!("  sink   : {}", sink.sink_name);

    println!("Connecting to {host}...");
    let (channel, incoming) = cast_channel::connect(&host)?;
    let session = mirroring::launch_mirroring(&channel, &incoming, StreamMode::AudioOnly, TIMEOUT)?;
    channel.connect_transport(&session.transport_id)?;

    let offer = StreamOffer::default();
    let answer = mirroring::send_offer(&channel, &incoming, &session.transport_id, &offer, TIMEOUT)?;
    assert!(answer.send_indexes.contains(&0), "audio stream not accepted");
    println!("ANSWER: udp_port={} send_indexes={:?}", answer.udp_port, answer.send_indexes);

    let mut sender = CastRtpSender::new(cast_rtp::Config {
        chromecast_host: host.clone(),
        udp_port: answer.udp_port,
        ssrc: offer.ssrc,
        payload_type: RTP_PAYLOAD_TYPE,
        aes_key: offer.aes_key,
        aes_iv_mask: offer.aes_iv_mask,
    })?;
    let stats = sender.stats_handle();
    if no_rtcp {
        println!("*** RTCP DISABLED ***");
    } else {
        sender.start();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let cap_stop = Arc::clone(&stop);
    let monitor = sink.monitor_source.clone();
    let cap_thread = thread::spawn(move || {
        if let Err(e) = capture::run(&monitor, &mut sender, cap_stop, OPUS_BITRATE) {
            eprintln!("capture error: {e:#}");
        }
        sender.stop();
    });

    thread::sleep(Duration::from_secs(2));
    let silence = snapshot(&stats, "silence");

    println!("injecting tone via paplay...");
    let mut pa = Command::new("paplay")
        .arg(format!("--device={}", sink.sink_name))
        .arg(tone)
        .spawn()?;
    thread::sleep(Duration::from_secs(1));
    let tone_bpf = snapshot(&stats, "tone");
    let _ = pa.wait();
    let after = snapshot(&stats, "after");

    if no_rtcp {
        println!("no-RTCP endurance: 60s...");
        for i in 0..6 {
            thread::sleep(Duration::from_secs(10));
            let (f, _) = stats.snapshot();
            println!("  t+{}s frames={f}", (i + 1) * 10);
        }
    }

    stop.store(true, Ordering::Relaxed);
    let _ = cap_thread.join();

    let ok = silence < 20.0 && (100.0..200.0).contains(&tone_bpf);
    println!(
        "\nRESULT: silence={silence:.1} B/f, tone={tone_bpf:.1} B/f, after={after:.1} B/f -> {}",
        if ok { "PASS" } else { "CHECK MANUALLY" }
    );
    Ok(())
}
