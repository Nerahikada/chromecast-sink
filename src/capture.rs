use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};

use crate::audio_ring::RingConsumer;
use crate::cast_rtp::{CastRtpSender, OPUS_SAMPLES_PER_FRAME};
use crate::opus_enc::OpusEncoder;

pub const SAMPLE_RATE: u32 = 48_000;
pub const CHANNELS: u8 = 2;

const DRAIN_THRESHOLD_MS: usize = 30;
const DRAIN_TARGET_MS: usize = 10;

/// Headroom past the drain point, so a producer batch cannot land on the
/// region being copied.
const MAX_EXPECTED_QUANTUM: usize = 2048;

/// Send failures self-heal (a Wi-Fi blip returns `ENETUNREACH`, then the same
/// socket works again). Past this point RTCP has been down just as long, so the
/// receiver has dropped the app anyway.
const OUTAGE_FATAL: Duration = Duration::from_secs(5);

struct Outage {
    since: Instant,
    frames: u64,
}

fn frames_for_ms(ms: usize) -> usize {
    ms * SAMPLE_RATE as usize / 1000
}

pub fn run(
    ring: &mut RingConsumer,
    sender: &mut CastRtpSender,
    stop: Arc<AtomicBool>,
    bit_rate: i32,
) -> Result<()> {
    let frame_frames = OPUS_SAMPLES_PER_FRAME as usize;
    let threshold = frames_for_ms(DRAIN_THRESHOLD_MS);
    let target = frames_for_ms(DRAIN_TARGET_MS);
    assert!(threshold + frame_frames + MAX_EXPECTED_QUANTUM <= ring.capacity_frames());

    let mut encoder = OpusEncoder::new(SAMPLE_RATE, CHANNELS, bit_rate)?;
    log::info!(
        "Capture started: Opus {} kbps, {} ms frames, encoder lookahead {:.1} ms",
        bit_rate / 1000,
        OPUS_SAMPLES_PER_FRAME * 1000 / SAMPLE_RATE,
        encoder.lookahead_samples() as f64 * 1000.0 / SAMPLE_RATE as f64,
    );

    let mut pcm = vec![0i16; frame_frames * CHANNELS as usize];

    // The sink filled the ring all through session negotiation; not a stall.
    ring.skip_frames(ring.available_frames());

    let start = Instant::now();
    let mut first_frame: Option<Instant> = None;
    let mut frames: u64 = 0;
    let mut dropped_total: usize = 0;
    let mut send_failures: u64 = 0;
    let mut outage: Option<Outage> = None;
    let mut last_stats = start;

    while !stop.load(Ordering::Relaxed) {
        if ring.is_closed() {
            bail!("pipewire sink stopped delivering audio");
        }
        let avail = ring.available_frames();

        if avail > threshold {
            let dropped = avail - target;
            ring.skip_frames(dropped);
            dropped_total += dropped;
            log::warn!(
                "Fell behind; dropped {dropped} frames ({} ms) to restore latency",
                dropped * 1000 / SAMPLE_RATE as usize,
            );
            continue;
        }

        if avail < frame_frames {
            let missing = (frame_frames - avail) as u64;
            let us = (missing * 1_000_000 / SAMPLE_RATE as u64).max(200);
            std::thread::sleep(Duration::from_micros(us));
            continue;
        }

        if !ring.read_frames(&mut pcm) {
            continue;
        }

        let now = Instant::now();
        if first_frame.is_none() {
            first_frame = Some(now);
            log::info!(
                "First audio frame encoded ({} ms after start)",
                (now - start).as_millis()
            );
        }

        let opus = encoder.encode(&pcm)?;
        match sender.send_frame(opus) {
            Ok(()) => {
                if let Some(o) = outage.take() {
                    log::warn!(
                        "UDP send recovered; discarded {} frames over {} ms",
                        o.frames,
                        now.duration_since(o.since).as_millis(),
                    );
                }
                frames += 1;
            }
            Err(e) => {
                send_failures += 1;
                let o = match &mut outage {
                    Some(o) => o,
                    None => {
                        log::warn!("UDP send failed ({e}); discarding frames until it recovers");
                        outage.insert(Outage { since: now, frames: 0 })
                    }
                };
                o.frames += 1;
                let down = now.duration_since(o.since);
                if down >= OUTAGE_FATAL {
                    bail!("UDP send failing for {:.1}s: {e}", down.as_secs_f64());
                }
            }
        }

        if now.duration_since(last_stats).as_secs_f64() >= 5.0 {
            let elapsed = now.duration_since(first_frame.unwrap()).as_secs_f64();
            let fps = if elapsed > 0.0 { frames as f64 / elapsed } else { 0.0 };
            log::info!(
                "Capture stats: {frames} frames in {elapsed:.1}s ({fps:.1} fps, expected ~100), \
                 {dropped_total} dropped, {send_failures} send failures, backlog {:.1} ms",
                ring.available_frames() as f64 * 1000.0 / SAMPLE_RATE as f64,
            );
            last_stats = now;
        }
    }

    log::info!("Capture stopped ({frames} frames sent, {dropped_total} dropped)");
    Ok(())
}
