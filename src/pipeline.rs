use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::capture;
use crate::cast_channel::{self, payload_type_is, CastMessage, NS_CONNECTION};
use crate::cast_rtp::{self, CastRtpSender};
use crate::discovery::{self, Device};
use crate::mirroring::{self, StreamMode, StreamOffer, OPUS_BITRATE, RTP_PAYLOAD_TYPE, TARGET_DELAY_MS};
use crate::virtual_sink::VirtualSink;

const TIMEOUT: Duration = Duration::from_secs(10);

pub fn run(device_name: Option<&str>) -> Result<()> {
    println!("Discovering Chromecast devices...");
    let devices = discovery::discover(device_name, TIMEOUT)?;
    if devices.is_empty() {
        bail!("No Chromecast devices found.\nCheck that your device is powered on, on the same network, and that multicast UDP 5353 is not blocked by a firewall.");
    }

    let device = pick_device(devices)?;
    run_with_device(device)
}

/// Bypasses mDNS discovery; used by integration tests that can't rely on multicast.
pub fn run_with_device(device: Device) -> Result<()> {
    println!("Selected: {}", device.friendly_name);
    log::info!("Chromecast: {} ({:?})", device.host, device.model);

    println!("Creating virtual sink \"Chromecast - {}\"...", device.friendly_name);
    let mut sink = VirtualSink::new(&device.friendly_name)?;

    println!("Connecting to Chromecast...");
    let (channel, incoming) = cast_channel::connect(&device.host)?;

    println!("Launching mirroring receiver...");
    let mode = if device.is_audio_only { StreamMode::AudioOnly } else { StreamMode::AudioVideo };
    let session = mirroring::launch_mirroring(&channel, &incoming, mode, TIMEOUT)?;
    channel.connect_transport(&session.transport_id)?;

    let offer = StreamOffer::default();
    println!("Negotiating stream (Opus {}kbps, target delay {}ms)...", OPUS_BITRATE / 1000, TARGET_DELAY_MS);
    let answer = mirroring::send_offer(&channel, &incoming, &session.transport_id, &offer, TIMEOUT)?;
    if !answer.send_indexes.contains(&0) {
        bail!("Chromecast did not accept the audio stream (sendIndexes={:?})", answer.send_indexes);
    }
    log::info!("Stream negotiated: UDP port {}", answer.udp_port);

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

    let stop = Arc::new(AtomicBool::new(false));
    install_signal_handler(&stop)?;

    let monitor = spawn_session_monitor(incoming, session.transport_id.clone(), Arc::clone(&stop));

    println!(
        "\nStreaming to \"{}\" via Cast Streaming (UDP).\nSelect \"Chromecast - {}\" as your audio output to start casting.\nPress Ctrl+C to stop.",
        device.friendly_name, device.friendly_name,
    );

    let mut ring = sink.take_consumer().expect("ring consumer is taken exactly once");
    let capture_result = capture::run(&mut ring, &mut sender, Arc::clone(&stop), OPUS_BITRATE);
    // Claim the shutdown so the session monitor doesn't also report a cause.
    if capture_result.is_err() {
        stop.store(true, Ordering::Relaxed);
    }

    sender.stop();
    drop(session);
    // drop(channel) must precede monitor.join(): it releases the incoming Sender the monitor is blocked on, otherwise the join deadlocks.
    drop(channel);
    let _ = monitor.join();
    drop(sink);
    capture_result
}

fn spawn_session_monitor(
    incoming: Receiver<CastMessage>,
    transport: String,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("session-monitor".into())
        .spawn(move || {
            // Default reason covers the recv-error path (dispatcher gone: our shutdown, or the TLS socket died).
            // Overwritten if we see CLOSE.
            let mut reason = "Connection to Chromecast lost";
            while let Ok(msg) = incoming.recv() {
                if msg.namespace == NS_CONNECTION
                    && msg.source == transport
                    && payload_type_is(&msg.payload, "CLOSE")
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

/// `ctrlc::set_handler` is only callable once per process.
fn install_signal_handler(stop: &Arc<AtomicBool>) -> Result<()> {
    let flag = Arc::clone(stop);
    ctrlc::set_handler(move || flag.store(true, Ordering::Relaxed)).context("install SIGINT/SIGTERM handler")
}
