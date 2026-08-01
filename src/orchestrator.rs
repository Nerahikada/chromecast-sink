//! Wires all components together in the same order as the Python orchestrator.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::capture;
use crate::cast_rtp::{self, CastRtpSender};
use crate::castv2;
use crate::discovery::{self, Device};
use crate::virtual_sink::VirtualSink;
use crate::webrtc::{self, StreamOffer};

const TIMEOUT: Duration = Duration::from_secs(10);
const OPUS_BITRATE: i32 = 128_000;

pub fn run(device_name: Option<&str>) -> Result<()> {
    // Phase 1: discover
    println!("Discovering Chromecast devices...");
    let devices = discovery::discover(device_name, TIMEOUT)?;
    if devices.is_empty() {
        bail!(
            "No Chromecast devices found.\n\
             Check that your device is powered on, on the same network, and\n\
             that multicast UDP 5353 is not blocked by a firewall."
        );
    }

    // Phase 2: pick one
    let device = pick_device(devices)?;
    run_with_device(device)
}

/// Run the pipeline with a pre-selected device (bypasses mDNS discovery).
/// Public so integration tests can drive it without multicast.
pub fn run_with_device(device: Device) -> Result<()> {
    println!("Selected: {}", device.friendly_name);
    log::info!("Chromecast: {} ({:?})", device.host, device.model);

    // Phase 3: virtual sink (in-process pipewire node)
    println!("Creating virtual sink \"Chromecast - {}\"...", device.friendly_name);
    let sink = VirtualSink::create(&device.friendly_name)?;

    // Phase 4: Cast v2 channel
    println!("Connecting to Chromecast...");
    let (channel, incoming) = castv2::connect(&device.host)?;

    // Phase 5: launch mirroring receiver
    println!("Launching mirroring receiver...");
    let (_app_id, transport) =
        webrtc::launch_mirroring(&channel, &incoming, device.is_audio_only, TIMEOUT)?;
    channel.connect_transport(&transport)?;

    // Phase 6: OFFER / ANSWER
    let offer = StreamOffer::default();
    println!(
        "Negotiating stream (Opus {}kbps, target delay {}ms)...",
        offer.bit_rate / 1000,
        offer.target_delay_ms
    );
    let answer = webrtc::send_offer(&channel, &incoming, &transport, &offer, TIMEOUT)?;
    if !answer.send_indexes.contains(&0) {
        bail!(
            "Chromecast did not accept the audio stream (sendIndexes={:?})",
            answer.send_indexes
        );
    }
    log::info!("Stream negotiated: UDP port {}", answer.udp_port);

    // Phase 7: RTP sender
    let mut sender = CastRtpSender::new(cast_rtp::Config {
        chromecast_host: device.host.clone(),
        udp_port: answer.udp_port,
        ssrc: offer.ssrc,
        payload_type: offer.rtp_payload_type,
        aes_key: offer.aes_key,
        aes_iv_mask: offer.aes_iv_mask,
    })
    .context("bind UDP socket")?;
    sender.start();

    // Phase 8: shutdown coordination
    let stop = Arc::new(AtomicBool::new(false));
    install_signal_handler(Arc::clone(&stop));

    // Phase 9: capture in this thread (the RTCP thread runs in the background)
    println!(
        "\nStreaming to \"{}\" via Cast Streaming (UDP).\n\
         Select \"Chromecast - {}\" as your audio output to start casting.\n\
         Press Ctrl+C to stop.",
        device.friendly_name, device.friendly_name,
    );

    let capture_result = capture::run(
        &sink.monitor_source,
        &mut sender,
        Arc::clone(&stop),
        OPUS_BITRATE,
    );

    // Cleanup in reverse order; close/drop both join their worker threads.
    sender.stop();
    channel.close();
    drop(sink); // pipewire node destroyed here
    capture_result
}

fn pick_device(mut devices: Vec<Device>) -> Result<Device> {
    if devices.len() == 1 {
        return Ok(devices.pop().unwrap());
    }
    eprintln!("Multiple devices found:");
    for d in &devices {
        let m = d.model.as_deref().map(|s| format!(" ({s})")).unwrap_or_default();
        eprintln!("  - {}{m}", d.friendly_name);
    }
    eprintln!("Specify one with --device \"NAME\".");
    bail!("device selection required")
}

/// SIGINT/SIGTERM handler that flips the passed AtomicBool. Called at most
/// once per process (OnceLock only takes the first `stop`).
fn install_signal_handler(stop: Arc<AtomicBool>) {
    static STOP_PTR: std::sync::OnceLock<Arc<AtomicBool>> = std::sync::OnceLock::new();
    let _ = STOP_PTR.set(stop);

    extern "C" fn handler(_: libc::c_int) {
        if let Some(s) = STOP_PTR.get() {
            s.store(true, Ordering::Relaxed);
        }
    }

    // SAFETY: the handler is async-signal-safe (a single relaxed atomic store).
    unsafe {
        libc::signal(libc::SIGINT, handler as *const () as libc::sighandler_t);
        libc::signal(libc::SIGTERM, handler as *const () as libc::sighandler_t);
    }
}
