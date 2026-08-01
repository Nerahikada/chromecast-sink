//! Monitor capture via libpulse-simple, Opus encode, hand off to Cast RTP.
//!
//! Reads one Opus-frame worth per pa_simple_read; the sink's node.force-quantum
//! dominates capture latency, so there's nothing to gain from smaller reads.
//! When we fall behind, we explicitly drain (see `_drain` in the Python side):
//! PulseAudio does not shed backlog on its own.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use libpulse_binding::sample::{Format, Spec};
use libpulse_binding::stream::Direction;
use libpulse_binding::def::BufferAttr;
use libpulse_simple_binding::Simple;

use crate::cast_rtp::{CastRtpSender, OPUS_SAMPLES_PER_FRAME};
use crate::opus_enc::OpusEncoder;

pub const SAMPLE_RATE: u32 = 48_000;
pub const CHANNELS: u8 = 2;
const BYTES_PER_SAMPLE: usize = 2;
const FRAME_BYTES: usize = OPUS_SAMPLES_PER_FRAME as usize * CHANNELS as usize * BYTES_PER_SAMPLE;

const DRAIN_THRESHOLD_MS: f64 = 30.0;
const DRAIN_TARGET_MS: f64 = 10.0;
const DRAIN_MAX_FRAMES: usize = 2000;

fn latency_ms(s: &Simple) -> f64 {
    // libpulse_binding::time::MicroSeconds is a newtype over u64 µs.
    s.get_latency().map(|d| d.0 as f64 / 1000.0).unwrap_or(0.0)
}

fn drain(s: &Simple, buf: &mut [u8]) -> usize {
    let mut dropped = 0;
    while dropped < DRAIN_MAX_FRAMES && latency_ms(s) > DRAIN_TARGET_MS {
        if s.read(buf).is_err() {
            break;
        }
        dropped += 1;
    }
    dropped
}

/// Capture, encode, and send audio until `stop` is set.
pub fn run(
    monitor_source: &str,
    sender: &mut CastRtpSender,
    stop: Arc<AtomicBool>,
    bit_rate: i32,
) -> Result<()> {
    let spec = Spec { format: Format::S16le, channels: CHANNELS, rate: SAMPLE_RATE };
    assert!(spec.is_valid());
    let attr = BufferAttr {
        maxlength: u32::MAX,
        tlength: u32::MAX,
        prebuf: u32::MAX,
        minreq: u32::MAX,
        fragsize: FRAME_BYTES as u32,
    };
    let simple = Simple::new(
        None,
        "chromecast-sink",
        Direction::Record,
        Some(monitor_source),
        "capture",
        &spec,
        None,
        Some(&attr),
    )
    .with_context(|| format!("open pulse record on {monitor_source}"))?;

    let mut encoder = OpusEncoder::new(SAMPLE_RATE, CHANNELS, bit_rate)?;
    log::info!(
        "Capture started: {monitor_source}, Opus {} kbps, {} ms frames, encoder lookahead {:.1} ms",
        bit_rate / 1000,
        OPUS_SAMPLES_PER_FRAME * 1000 / SAMPLE_RATE,
        encoder.lookahead_samples() as f64 * 1000.0 / SAMPLE_RATE as f64,
    );

    let mut pcm_bytes = vec![0u8; FRAME_BYTES];
    // Interleaved i16 view — libpulse-simple only speaks &[u8]
    let mut pcm_i16 = vec![0i16; OPUS_SAMPLES_PER_FRAME as usize * CHANNELS as usize];

    let start = Instant::now();
    let mut first_frame: Option<Instant> = None;
    let mut frames: u64 = 0;
    let mut dropped_total: usize = 0;
    let mut last_stats = start;

    while !stop.load(Ordering::Relaxed) {
        simple.read(&mut pcm_bytes).context("pulse read")?;

        if latency_ms(&simple) > DRAIN_THRESHOLD_MS {
            let d = drain(&simple, &mut pcm_bytes) + 1; // the frame just read is stale too
            dropped_total += d;
            log::warn!(
                "Fell behind; dropped {d} frames ({} ms) to restore latency",
                d as u32 * OPUS_SAMPLES_PER_FRAME * 1000 / SAMPLE_RATE,
            );
            continue;
        }

        let now = Instant::now();
        if first_frame.is_none() {
            first_frame = Some(now);
            log::info!(
                "First audio frame captured ({} ms after start)",
                (now - start).as_millis()
            );
        }

        // little-endian bytes -> i16
        for (i, chunk) in pcm_bytes.chunks_exact(2).enumerate() {
            pcm_i16[i] = i16::from_le_bytes([chunk[0], chunk[1]]);
        }
        let opus = encoder.encode(&pcm_i16)?;
        sender.send_frame(opus).context("send RTP frame")?;
        frames += 1;

        if now.duration_since(last_stats).as_secs_f64() >= 5.0 {
            let elapsed = now.duration_since(first_frame.unwrap()).as_secs_f64();
            let fps = if elapsed > 0.0 { frames as f64 / elapsed } else { 0.0 };
            log::info!(
                "Capture stats: {frames} frames in {elapsed:.1}s ({fps:.1} fps, expected ~100), \
                 {dropped_total} dropped, latency {:.1} ms",
                latency_ms(&simple),
            );
            last_stats = now;
        }
    }

    log::info!("Capture stopped ({frames} frames sent, {dropped_total} dropped)");
    Ok(())
}
