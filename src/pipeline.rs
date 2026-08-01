//! End-to-end pipeline: discover → sink → cast channel → OFFER/ANSWER → capture.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::capture;
use crate::cast_channel::{self, CastMessage, NS_CONNECTION};
use crate::cast_rtp::{self, CastRtpSender};
use crate::discovery::{self, Device};
use crate::mirroring::{self, StreamMode, StreamOffer, OPUS_BITRATE, RTP_PAYLOAD_TYPE, TARGET_DELAY_MS};
use crate::virtual_sink::VirtualSink;

const TIMEOUT: Duration = Duration::from_secs(10);

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
    let sink = VirtualSink::new(&device.friendly_name)?;

    // Phase 4: Cast v2 channel
    println!("Connecting to Chromecast...");
    let (channel, incoming) = cast_channel::connect(&device.host)?;

    // Phase 5: launch mirroring receiver
    println!("Launching mirroring receiver...");
    let mode = if device.is_audio_only { StreamMode::AudioOnly } else { StreamMode::AudioVideo };
    let transport =
        mirroring::launch_mirroring(&channel, &incoming, mode, TIMEOUT)?;
    channel.connect_transport(&transport)?;

    // Phase 6: OFFER / ANSWER
    let offer = StreamOffer::default();
    println!(
        "Negotiating stream (Opus {}kbps, target delay {}ms)...",
        OPUS_BITRATE / 1000,
        TARGET_DELAY_MS,
    );
    let answer = mirroring::send_offer(&channel, &incoming, &transport, &offer, TIMEOUT)?;
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
        payload_type: RTP_PAYLOAD_TYPE,
        aes_key: offer.aes_key,
        aes_iv_mask: offer.aes_iv_mask,
    })
    .context("bind UDP socket")?;
    sender.start();

    // Phase 8: shutdown coordination
    let stop = Arc::new(AtomicBool::new(false));
    install_signal_handler(&stop)?;

    // Watch the Cast channel: stop when the receiver CLOSEs our session
    // (e.g. someone else starts casting) or the TLS connection dies. Without
    // this, the pipeline would keep sending UDP into the void forever.
    let monitor = spawn_session_monitor(incoming, transport.clone(), Arc::clone(&stop));

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

    // Cleanup in reverse order. Dropping the channel joins the dispatcher and
    // drops its incoming sender, which unblocks the session monitor. This
    // must happen before `monitor.join()` or the join deadlocks.
    sender.stop();
    drop(channel);
    let _ = monitor.join();
    drop(sink); // pipewire node destroyed here
    capture_result
}

/// Session-death watchdog: sets `stop` when the receiver closes our virtual
/// connection to the mirroring app or the Cast socket dies.
fn spawn_session_monitor(
    incoming: Receiver<CastMessage>,
    transport: String,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("session-monitor".into())
        .spawn(move || {
            // Default reason applies when `recv()` errors (dispatcher gone:
            // either our own shutdown, or the TLS socket died).
            let mut reason = "Connection to Chromecast lost";
            while let Ok(msg) = incoming.recv() {
                if msg.namespace == NS_CONNECTION
                    && msg.source == transport
                    && msg.payload.contains(r#""CLOSE""#)
                {
                    reason = "Receiver closed the mirroring session";
                    break;
                }
                log::debug!("cast rx [{}] {}", msg.namespace, msg.payload);
            }
            if !stop.swap(true, Ordering::Relaxed) {
                eprintln!("\n{reason}; shutting down.");
            }
        })
        .expect("spawn session monitor")
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

/// SIGINT/SIGTERM handler that flips the passed AtomicBool. `ctrlc` installs
/// a dedicated handler thread (using `sigaction` on Unix, not the BSD-quirky
/// `signal(2)`), and its `termination` feature adds SIGTERM alongside SIGINT.
/// Only callable once per process — a second `run()` would return an error.
fn install_signal_handler(stop: &Arc<AtomicBool>) -> Result<()> {
    let flag = Arc::clone(stop);
    ctrlc::set_handler(move || flag.store(true, Ordering::Relaxed))
        .context("install SIGINT/SIGTERM handler")
}
