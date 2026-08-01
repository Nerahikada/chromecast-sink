//! Cast Streaming OFFER/ANSWER over the webrtc namespace, plus receiver
//! LAUNCH/GET_STATUS helpers on the receiver namespace.
//!
//! The receiver namespace flow gives us the `transportId` of the launched
//! app, which is the destination for the webrtc OFFER.

use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use rand::RngCore;
use serde_json::{json, Value};

use crate::castv2::{
    CastChannel, CastMessage, NS_RECEIVER, NS_WEBRTC, PLATFORM,
};

/// Cast Streaming receiver apps (openscreen: `cast_streaming_app_ids.h`).
pub const APP_MIRRORING_AUDIO_VIDEO: &str = "0F5096E8";
pub const APP_MIRRORING_AUDIO_ONLY: &str = "85CDB22F";

/// Offered audio stream parameters.
pub struct StreamOffer {
    pub ssrc: u32,
    pub aes_key: [u8; 16],
    pub aes_iv_mask: [u8; 16],
    pub codec: &'static str,
    pub sample_rate: u32,
    pub channels: u32,
    pub bit_rate: i32,
    pub rtp_payload_type: u8,
    pub target_delay_ms: u32,
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
            codec: "opus",
            sample_rate: 48_000,
            channels: 2,
            bit_rate: 128_000,
            rtp_payload_type: 127,
            target_delay_ms: 0,
        }
    }
}

pub struct StreamAnswer {
    pub udp_port: u16,
    pub send_indexes: Vec<u64>,
}

/// Launch the mirroring receiver app and return `(app_id, transport_id)`.
pub fn launch_mirroring(
    channel: &CastChannel,
    incoming: &Receiver<CastMessage>,
    is_audio_only: bool,
    timeout: Duration,
) -> Result<(String, String)> {
    let app_id = if is_audio_only {
        APP_MIRRORING_AUDIO_ONLY
    } else {
        APP_MIRRORING_AUDIO_VIDEO
    };
    let request_id = new_request_id();

    log::info!(
        "Launching mirroring app {app_id} (audio-only: {is_audio_only})"
    );
    channel.send_json(
        PLATFORM,
        NS_RECEIVER,
        &json!({
            "type": "LAUNCH",
            "requestId": request_id,
            "appId": app_id,
        }),
    )?;

    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let msg = match incoming.recv_timeout(remaining.min(Duration::from_millis(500))) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if msg.namespace != NS_RECEIVER {
            continue;
        }
        let v: Value = match serde_json::from_str(&msg.payload) {
            Ok(v) => v,
            Err(_) => continue,
        };
        // RECEIVER_STATUS: {applications: [{appId, transportId, sessionId, ...}]}
        if v.get("type").and_then(|t| t.as_str()) == Some("RECEIVER_STATUS") {
            if let Some(apps) = v.pointer("/status/applications").and_then(|a| a.as_array()) {
                for app in apps {
                    if app.get("appId").and_then(|s| s.as_str()) == Some(app_id) {
                        let transport = app
                            .get("transportId")
                            .and_then(|s| s.as_str())
                            .ok_or_else(|| anyhow!("no transportId"))?
                            .to_string();
                        log::info!("Mirroring app ready (transport {transport})");
                        // Give it a moment to fully initialize
                        std::thread::sleep(Duration::from_millis(500));
                        return Ok((app_id.to_string(), transport));
                    }
                }
            }
        }
        if v.get("type").and_then(|t| t.as_str()) == Some("LAUNCH_ERROR") {
            bail!("Receiver rejected LAUNCH: {}", msg.payload);
        }
    }
    bail!("Timeout launching mirroring app")
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
                "codecName": offer.codec,
                "rtpProfile": "cast",
                "rtpPayloadType": offer.rtp_payload_type,
                "ssrc": offer.ssrc,
                "targetDelay": offer.target_delay_ms,
                "aesKey": hex::encode(offer.aes_key),
                "aesIvMask": hex::encode(offer.aes_iv_mask),
                "timeBase": format!("1/{}", offer.sample_rate),
                "bitRate": offer.bit_rate,
                "sampleRate": offer.sample_rate,
                "channels": offer.channels,
            }],
        },
    });
    log::info!(
        "Sending OFFER (seqNum={seq_num}, ssrc={}, targetDelay={}ms)",
        offer.ssrc, offer.target_delay_ms
    );
    channel.send_json(transport_id, NS_WEBRTC, &payload)?;

    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let msg = match incoming.recv_timeout(remaining.min(Duration::from_millis(500))) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if msg.namespace != NS_WEBRTC {
            continue;
        }
        let v: Value = serde_json::from_str(&msg.payload).context("decode webrtc payload")?;
        if v.get("type").and_then(|t| t.as_str()) != Some("ANSWER") {
            continue;
        }
        if v.get("result").and_then(|r| r.as_str()) != Some("ok") {
            bail!("OFFER rejected: {}", msg.payload);
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
        return Ok(StreamAnswer { udp_port, send_indexes });
    }
    bail!("Timeout waiting for ANSWER from Chromecast")
}

fn new_request_id() -> u64 {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}
