//! Mirroring control: LAUNCH the app, then OFFER/ANSWER on the `webrtc` namespace (Cast-proprietary JSON, not SDP/WebRTC).

use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use rand::RngCore;
use serde_json::{json, Value};

use crate::cast_channel::{
    CastChannel, CastMessage, NS_RECEIVER, NS_WEBRTC, PLATFORM_RECEIVER_ID,
};

/// openscreen `cast_streaming_app_ids.h`.
pub const APP_MIRRORING_AUDIO_VIDEO: &str = "0F5096E8";
pub const APP_MIRRORING_AUDIO_ONLY: &str = "85CDB22F";

pub const OPUS_CODEC: &str = "opus";
pub const OPUS_SAMPLE_RATE: u32 = 48_000;
pub const OPUS_CHANNELS: u32 = 2;
pub const OPUS_BITRATE: i32 = 128_000;
pub const RTP_PAYLOAD_TYPE: u8 = 127;
/// `0` means 0ms, not "device default" — the default (400ms) only applies when the field is absent.
pub const TARGET_DELAY_MS: u32 = 0;

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

/// Owns the receiver-side app: dropping it tears the session down, so early returns between LAUNCH and shutdown still unload it.
pub struct MirroringSession<'a> {
    channel: &'a CastChannel,
    pub transport_id: String,
    pub session_id: String,
}

/// STOP unloads the app; the two CLOSEs are ours — Chromium's `TerminateSession` sends STOP alone and lets both VCs die with the socket, which it keeps open.
impl Drop for MirroringSession<'_> {
    fn drop(&mut self) {
        if let Err(e) = send_stop(self.channel, &self.session_id) {
            log::debug!("STOP not sent for session {}: {e}", self.session_id);
        }
        let _ = self.channel.close_transport(&self.transport_id);
        let _ = self.channel.close_transport(PLATFORM_RECEIVER_ID);
    }
}

pub fn launch_mirroring<'a>(
    channel: &'a CastChannel,
    incoming: &Receiver<CastMessage>,
    mode: StreamMode,
    timeout: Duration,
) -> Result<MirroringSession<'a>> {
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
            "language": "en-US",
            "supportedAppTypes": ["WEB"],
        }),
    )?;

    wait_for_json_message(incoming, NS_RECEIVER, Instant::now() + timeout, |v| {
        // Filter by requestId, not app_id: a RECEIVER_STATUS broadcast may describe a leftover session from a crashed prior run (same app_id, dead transport/sessionId).
        if v.get("requestId").and_then(|r| r.as_u64()) != Some(request_id) {
            return None;
        }
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
                    let transport_id = app
                        .get("transportId")
                        .and_then(|s| s.as_str())
                        .ok_or_else(|| anyhow!("no transportId"))?
                        .to_string();
                    let session_id = app
                        .get("sessionId")
                        .and_then(|s| s.as_str())
                        .ok_or_else(|| anyhow!("no sessionId"))?
                        .to_string();
                    log::info!(
                        "Mirroring app {app_id} ready (transport {transport_id}, session {session_id})"
                    );
                    Ok(MirroringSession { channel, transport_id, session_id })
                }),
            Some("LAUNCH_ERROR") => Some(Err(anyhow!("Receiver rejected LAUNCH: {v}"))),
            _ => None,
        }
    })
    .context("Launching mirroring app")
}

fn send_stop(channel: &CastChannel, session_id: &str) -> Result<()> {
    channel.send_json(
        PLATFORM_RECEIVER_ID,
        NS_RECEIVER,
        &json!({
            "type": "STOP",
            "requestId": new_request_id(),
            "sessionId": session_id,
        }),
    )
}

pub fn send_offer(
    channel: &CastChannel,
    incoming: &Receiver<CastMessage>,
    transport_id: &str,
    offer: &StreamOffer,
    timeout: Duration,
) -> Result<StreamAnswer> {
    let seq_num = new_request_id() as i64;
    // openscreen `AudioStream::ToJson`: sample rate rides in `timeBase`, not a separate `sampleRate` key.
    let payload = json!({
        "type": "OFFER",
        "seqNum": seq_num,
        "offer": {
            "castMode": "mirroring",
            "supportedStreams": [{
                "index": 0,
                "type": "audio_source",
                "codecName": OPUS_CODEC,
                "codecParameter": "",
                "rtpProfile": "cast",
                "rtpPayloadType": RTP_PAYLOAD_TYPE,
                "ssrc": offer.ssrc,
                "targetDelay": TARGET_DELAY_MS,
                "aesKey": hex::encode(offer.aes_key),
                "aesIvMask": hex::encode(offer.aes_iv_mask),
                "timeBase": format!("1/{}", OPUS_SAMPLE_RATE),
                "receiverRtcpEventLog": false,
                "bitRate": OPUS_BITRATE,
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
    .context("Waiting for ANSWER from Chromecast")
}

fn parse_answer(v: &Value) -> Result<StreamAnswer> {
    if v.get("result").and_then(|r| r.as_str()) != Some("ok") {
        bail!("OFFER rejected: {v}");
    }
    let ans = v.get("answer").ok_or_else(|| anyhow!("no `answer` field"))?;
    let udp_port_raw = ans.get("udpPort").and_then(|p| p.as_u64()).ok_or_else(|| anyhow!("no udpPort"))?;
    let udp_port = match udp_port_raw {
        1..=65535 => udp_port_raw as u16,
        n => bail!("udpPort {n} out of range (expected 1..=65535)"),
    };
    let send_indexes = ans
        .get("sendIndexes")
        .and_then(|s| s.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_u64()).collect::<Vec<_>>())
        .unwrap_or_default();
    log::info!("ANSWER received: udpPort={udp_port}, sendIndexes={send_indexes:?}");
    Ok(StreamAnswer { udp_port, send_indexes })
}

/// Predicate returns `None` to keep waiting, `Some(Ok(_))` for success, `Some(Err(_))` to bail.
/// Non-JSON payloads on `namespace` are dropped.
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
            Err(RecvTimeoutError::Timeout) => continue,
            // Returns instantly once the dispatcher is gone; `continue` spins.
            Err(RecvTimeoutError::Disconnected) => bail!("Cast connection lost"),
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
    bail!("timed out waiting for a reply")
}

fn new_request_id() -> u64 {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn cast_msg(namespace: &str, payload: &str) -> CastMessage {
        CastMessage {
            source: PLATFORM_RECEIVER_ID.into(),
            destination: "sender-0".into(),
            namespace: namespace.into(),
            payload: payload.into(),
        }
    }

    #[test]
    fn disconnect_bails_instead_of_spinning_to_the_deadline() {
        let (tx, rx) = mpsc::channel::<CastMessage>();
        drop(tx);
        let started = Instant::now();
        let r = wait_for_json_message(&rx, NS_RECEIVER, started + Duration::from_secs(10), |_| {
            Some(Ok(()))
        });
        let elapsed = started.elapsed();
        assert!(r.is_err());
        assert!(elapsed < Duration::from_millis(200), "spun for {elapsed:?}");
        assert!(r.unwrap_err().to_string().contains("connection lost"));
    }

    #[test]
    fn live_sender_still_waits_for_the_deadline() {
        let (_tx, rx) = mpsc::channel::<CastMessage>();
        let started = Instant::now();
        let r = wait_for_json_message::<()>(
            &rx,
            NS_RECEIVER,
            started + Duration::from_millis(300),
            |_| None,
        );
        assert!(started.elapsed() >= Duration::from_millis(300));
        assert!(r.unwrap_err().to_string().contains("timed out"));
    }

    #[test]
    fn other_namespaces_and_non_json_never_reach_the_predicate() {
        let (tx, rx) = mpsc::channel::<CastMessage>();
        tx.send(cast_msg(NS_WEBRTC, r#"{"type":"ANSWER"}"#)).unwrap();
        tx.send(cast_msg(NS_RECEIVER, "not json")).unwrap();
        tx.send(cast_msg(NS_RECEIVER, r#"{"requestId":7}"#)).unwrap();
        let got = wait_for_json_message(&rx, NS_RECEIVER, Instant::now() + Duration::from_secs(5), |v| {
            Some(Ok(v.get("requestId").and_then(|r| r.as_u64())))
        });
        assert_eq!(got.unwrap(), Some(7));
    }

    #[test]
    fn parse_answer_extracts_port_and_indexes() {
        let v = serde_json::json!({
            "result": "ok",
            "answer": {"udpPort": 44321, "sendIndexes": [0], "ssrcs": [1]},
        });
        let a = parse_answer(&v).unwrap();
        assert_eq!(a.udp_port, 44321);
        assert_eq!(a.send_indexes, vec![0]);
    }

    #[test]
    fn parse_answer_rejects_errors_and_missing_port() {
        assert!(parse_answer(&serde_json::json!({"result": "error"})).is_err());
        assert!(parse_answer(&serde_json::json!({"result": "ok"})).is_err());
        assert!(parse_answer(&serde_json::json!({
            "result": "ok",
            "answer": {"sendIndexes": [0]},
        }))
        .is_err());
    }

    #[test]
    fn parse_answer_rejects_port_above_u16() {
        let v = serde_json::json!({
            "result": "ok",
            "answer": {"udpPort": 70000, "sendIndexes": [0], "ssrcs": [1]},
        });
        let err = match parse_answer(&v) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected error for udpPort 70000"),
        };
        assert!(err.contains("70000"), "error should name the offending value: {err}");
        assert!(err.contains("udpPort"), "error should mention the field: {err}");
    }

    #[test]
    fn parse_answer_rejects_port_zero() {
        let v = serde_json::json!({
            "result": "ok",
            "answer": {"udpPort": 0, "sendIndexes": [0], "ssrcs": [1]},
        });
        assert!(parse_answer(&v).is_err());
    }

    #[test]
    fn parse_answer_accepts_max_port() {
        let v = serde_json::json!({
            "result": "ok",
            "answer": {"udpPort": 65535, "sendIndexes": [0], "ssrcs": [1]},
        });
        assert_eq!(parse_answer(&v).unwrap().udp_port, 65535);
    }

    #[test]
    fn parse_answer_accepts_typical_port() {
        let v = serde_json::json!({
            "result": "ok",
            "answer": {"udpPort": 5004, "sendIndexes": [0], "ssrcs": [1]},
        });
        assert_eq!(parse_answer(&v).unwrap().udp_port, 5004);
    }
}
