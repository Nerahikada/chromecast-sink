//! Cast mirroring control: LAUNCH the receiver app, then OFFER/ANSWER
//! stream negotiation over the (misleadingly-named) `webrtc` namespace —
//! the payloads are Cast-proprietary JSON, not SDP/WebRTC.
//!
//! The receiver namespace flow gives us the `transportId` of the launched
//! app, which is the destination for the OFFER.

use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use rand::RngCore;
use serde_json::{json, Value};

use crate::cast_channel::{
    CastChannel, CastMessage, NS_RECEIVER, NS_WEBRTC, PLATFORM_RECEIVER_ID,
};

/// Cast Streaming receiver apps (openscreen: `cast_streaming_app_ids.h`).
pub const APP_MIRRORING_AUDIO_VIDEO: &str = "0F5096E8";
pub const APP_MIRRORING_AUDIO_ONLY: &str = "85CDB22F";

// Wire-fixed offer parameters. These are constants because no caller has any
// business overriding them: the encoder (`opus_enc`) and the RTP layer
// choice (LowDelay + 10ms Opus, `targetDelay=0`, PT=127).
pub const OPUS_CODEC: &str = "opus";
pub const OPUS_SAMPLE_RATE: u32 = 48_000;
pub const OPUS_CHANNELS: u32 = 2;
pub const OPUS_BITRATE: i32 = 128_000;
pub const RTP_PAYLOAD_TYPE: u8 = 127;
/// Target playout delay in ms. `0` is intentional (not "device default" —
pub const TARGET_DELAY_MS: u32 = 0;

/// Which mirroring app to launch. Audio-only speakers reject the video
#[derive(Debug, Clone, Copy)]
pub enum StreamMode {
    AudioOnly,
    AudioVideo,
}

impl StreamMode {
    fn app_id(self) -> &'static str {
        match self {
            StreamMode::AudioOnly => APP_MIRRORING_AUDIO_ONLY,
            StreamMode::AudioVideo => APP_MIRRORING_AUDIO_VIDEO,
        }
    }
}

/// Per-session state offered to the receiver. Everything else about the
/// audio stream (codec/rate/channels/bitrate/PT/target-delay) is fixed —
/// see the `OPUS_*` / `RTP_*` / `TARGET_DELAY_MS` constants above.
pub struct StreamOffer {
    pub ssrc: u32,
    pub aes_key: [u8; 16],
    pub aes_iv_mask: [u8; 16],
}

impl Default for StreamOffer {
    fn default() -> Self {
        let mut rng = rand::rng();
        let mut aes_key = [0u8; 16];
        let mut aes_iv_mask = [0u8; 16];
        rng.fill_bytes(&mut aes_key);
        rng.fill_bytes(&mut aes_iv_mask);
        Self {
            ssrc: rng.next_u32() & 0x7FFF_FFFF | 1,
            aes_key,
            aes_iv_mask,
        }
    }
}

pub struct StreamAnswer {
    pub udp_port: u16,
    pub send_indexes: Vec<u64>,
}

/// Launch the mirroring receiver app and return its `transport_id`.
pub fn launch_mirroring(
    channel: &CastChannel,
    incoming: &Receiver<CastMessage>,
    mode: StreamMode,
    timeout: Duration,
) -> Result<String> {
    let app_id = mode.app_id();
    let request_id = new_request_id();

    log::info!("Launching mirroring app {app_id} ({mode:?})");
    channel.send_json(
        PLATFORM_RECEIVER_ID,
        NS_RECEIVER,
        &json!({
            "type": "LAUNCH",
            "requestId": request_id,
            "appId": app_id,
        }),
    )?;

    wait_for_json_message(incoming, NS_RECEIVER, Instant::now() + timeout, |v| {
        // RECEIVER_STATUS: {applications: [{appId, transportId, sessionId, ...}]}
        match v.get("type").and_then(|t| t.as_str()) {
            Some("RECEIVER_STATUS") => v
                .pointer("/status/applications")
                .and_then(|a| a.as_array())
                .and_then(|apps| {
                    apps.iter().find(|app| {
                        app.get("appId").and_then(|s| s.as_str()) == Some(app_id)
                    })
                })
                .map(|app| {
                    let transport = app
                        .get("transportId")
                        .and_then(|s| s.as_str())
                        .ok_or_else(|| anyhow!("no transportId"))?
                        .to_string();
                    log::info!("Mirroring app {app_id} ready (transport {transport})");
                    Ok(transport)
                }),
            Some("LAUNCH_ERROR") => Some(Err(anyhow!("Receiver rejected LAUNCH: {v}"))),
            _ => None,
        }
    })
    .context("Timeout launching mirroring app")
}

/// Send OFFER on the webrtc namespace to `transport_id` and wait for ANSWER.
pub fn send_offer(
    channel: &CastChannel,
    incoming: &Receiver<CastMessage>,
    transport_id: &str,
    offer: &StreamOffer,
    timeout: Duration,
) -> Result<StreamAnswer> {
    let seq_num = new_request_id() as i64;
    let payload = json!({
        "type": "OFFER",
        "seqNum": seq_num,
        "offer": {
            "castMode": "mirroring",
            "supportedStreams": [{
                "index": 0,
                "type": "audio_source",
                "codecName": OPUS_CODEC,
                "rtpProfile": "cast",
                "rtpPayloadType": RTP_PAYLOAD_TYPE,
                "ssrc": offer.ssrc,
                "targetDelay": TARGET_DELAY_MS,
                "aesKey": hex::encode(offer.aes_key),
                "aesIvMask": hex::encode(offer.aes_iv_mask),
                "timeBase": format!("1/{}", OPUS_SAMPLE_RATE),
                "bitRate": OPUS_BITRATE,
                "sampleRate": OPUS_SAMPLE_RATE,
                "channels": OPUS_CHANNELS,
            }],
        },
    });
    log::info!(
        "Sending OFFER (seqNum={seq_num}, ssrc={}, targetDelay={TARGET_DELAY_MS}ms)",
        offer.ssrc
    );
    channel.send_json(transport_id, NS_WEBRTC, &payload)?;

    wait_for_json_message(incoming, NS_WEBRTC, Instant::now() + timeout, |v| {
        if v.get("type").and_then(|t| t.as_str()) != Some("ANSWER") {
            return None;
        }
        Some(parse_answer(v))
    })
    .context("Timeout waiting for ANSWER from Chromecast")
}

fn parse_answer(v: &Value) -> Result<StreamAnswer> {
    if v.get("result").and_then(|r| r.as_str()) != Some("ok") {
        bail!("OFFER rejected: {v}");
    }
    let ans = v.get("answer").ok_or_else(|| anyhow!("no `answer` field"))?;
    let udp_port = ans
        .get("udpPort")
        .and_then(|p| p.as_u64())
        .ok_or_else(|| anyhow!("no udpPort"))? as u16;
    let send_indexes = ans
        .get("sendIndexes")
        .and_then(|s| s.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_u64()).collect::<Vec<_>>())
        .unwrap_or_default();
    log::info!("ANSWER received: udpPort={udp_port}, sendIndexes={send_indexes:?}");
    Ok(StreamAnswer { udp_port, send_indexes })
}

/// Wait for a JSON message on `namespace` matching `predicate`, up to `deadline`.
/// The predicate returns:
///   - `None` — not the message we want, keep waiting.
///   - `Some(Ok(t))` — success, return `t`.
///   - `Some(Err(e))` — terminal error observed in the reply, bail with `e`.
///
/// Non-JSON payloads on the target namespace are silently dropped (the receiver
/// occasionally interleaves other messages we don't care about).
fn wait_for_json_message<T>(
    rx: &Receiver<CastMessage>,
    namespace: &str,
    deadline: Instant,
    mut predicate: impl FnMut(&Value) -> Option<Result<T>>,
) -> Result<T> {
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let msg = match rx.recv_timeout(remaining.min(Duration::from_millis(500))) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if msg.namespace != namespace {
            continue;
        }
        let v: Value = match serde_json::from_str(&msg.payload) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Some(result) = predicate(&v) {
            return result;
        }
    }
    bail!("deadline reached")
}

fn new_request_id() -> u64 {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}
