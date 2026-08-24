//! Cast v2 channel: TLS to port 8009, length-prefixed CastMessage protobuf.
//!
//! Protocol namespaces we speak:
//! - `urn:x-cast:com.google.cast.tp.connection` — CONNECT / CLOSE
//! - `urn:x-cast:com.google.cast.tp.heartbeat` — PING / PONG
//! - `urn:x-cast:com.google.cast.receiver`     — LAUNCH / GET_STATUS / STOP
//! - `urn:x-cast:com.google.cast.webrtc`       — OFFER / ANSWER (mirroring)
//!
//! Design: one dispatcher thread owns the TLS stream, does non-blocking reads
//! interleaved with outbound writes and periodic heartbeat PINGs. PING from
//! the receiver is answered with PONG inline; everything else is forwarded on
//! a channel to the caller.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use native_tls::TlsConnector;
use rand::Rng;
use serde_json::{json, Value};

pub const NS_CONNECTION: &str = "urn:x-cast:com.google.cast.tp.connection";
pub const NS_HEARTBEAT: &str = "urn:x-cast:com.google.cast.tp.heartbeat";
pub const NS_RECEIVER: &str = "urn:x-cast:com.google.cast.receiver";
pub const NS_WEBRTC: &str = "urn:x-cast:com.google.cast.webrtc";

pub const PLATFORM_RECEIVER_ID: &str = "receiver-0";

/// Virtual-connection close reason. Mirrors Chromium's `kVirtualConnectionClosedByPeer`
/// enum value in `cast_message_util.h`. Sent in CLOSE payloads so the receiver
/// classifies our shutdown as an intentional peer close rather than an abrupt
/// network drop (`CreateVirtualConnectionClose` in `cast_message_util.cc`).
const CLOSE_REASON_CLOSED_BY_PEER: u32 = 5;

/// Per-process sender id, matching Chromium's `CastMessageHandler` convention
/// (`sender-<rand 0..1M>`). A fresh identity per run means the receiver treats
/// our virtual connections as brand-new, so a leftover virtual connection from
/// a crashed previous run (which the receiver may still be holding open) does
/// not collide with ours. Generated lazily so tests that construct
/// `CastMessage`s by hand still see a deterministic literal.
pub fn local_sender_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| format!("sender-{}", rand::rng().random_range(0..1_000_000)))
}

/// Build the CONNECT payload in the shape Chromium's `CreateVirtualConnectionRequest`
/// emits (`components/media_router/common/providers/cast/channel/cast_message_util.cc`).
/// A bare `{"type":"CONNECT"}` is legal enough to open a virtual connection,
/// but Chromium always sends the full set below and the receiver may classify
/// the connection (e.g. strong-vs-invisible) using these fields.
///
/// Enum values are the ones from `cast_message_util.h`:
/// - `connType: 0` = `VirtualConnectionType::kStrong` (regular sender; kInvisible=2)
/// - `senderInfo.sdkType: 2` = `kVirtualConnectSdkType`
/// - `senderInfo.platform: 6` = Linux (3=Win, 4=Apple, 5=CrOS, 0=other)
/// - `senderInfo.connectionType: 1` = `kVirtualConnectTypeLocal` (LAN)
///
/// `userAgent` / `systemVersion` values are just labels the receiver logs;
/// the exact strings don't affect wire framing — we advertise as this crate.
fn build_connect_payload() -> Value {
    const USER_AGENT: &str = concat!(
        "chromecast-sink/",
        env!("CARGO_PKG_VERSION"),
        " (Linux; Rust)"
    );
    json!({
        "type": "CONNECT",
        "userAgent": USER_AGENT,
        "connType": 0,
        "origin": {},
        "senderInfo": {
            "sdkType": 2,
            "version": env!("CARGO_PKG_VERSION"),
            "browserVersion": env!("CARGO_PKG_VERSION"),
            "platform": 6,
            "connectionType": 1,
            "systemVersion": "Linux",
        },
    })
}

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// Read poll interval for the dispatcher loop.
const READ_TIMEOUT: Duration = Duration::from_millis(50);
/// Long timeout for the TLS handshake itself — must not be as tight as the
/// dispatcher's read poll, or a slow handshake looks like an aborted read.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// A parsed CastMessage.
#[derive(Debug, Clone)]
pub struct CastMessage {
    pub source: String,
    pub destination: String,
    pub namespace: String,
    pub payload: String, // we only use STRING payloads
}

/// The client side of the channel: send messages via `send`, receive on `events`.
pub struct CastChannel {
    outbound: Sender<CastMessage>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl CastChannel {
    pub fn send(&self, msg: CastMessage) -> Result<()> {
        self.outbound.send(msg).context("dispatcher gone")
    }

    /// Convenience: send a JSON payload.
    pub fn send_json(&self, destination: &str, namespace: &str, payload: &Value) -> Result<()> {
        self.send(CastMessage {
            source: local_sender_id().into(),
            destination: destination.into(),
            namespace: namespace.into(),
            payload: payload.to_string(),
        })
    }

    /// Send CONNECT to a destination (must be done before any app message).
    pub fn connect_transport(&self, destination: &str) -> Result<()> {
        self.send_json(destination, NS_CONNECTION, &build_connect_payload())
    }

    /// Send CLOSE on the connection namespace — signals the receiver that we
    /// are tearing down the virtual connection to `destination`. Used at
    /// shutdown for both the mirroring transport and `receiver-0`.
    ///
    /// Payload matches Chromium's `CreateVirtualConnectionClose`
    /// (`cast_message_util.cc`): `{"type":"CLOSE","reasonCode":5}` — the
    /// reasonCode is what tells the receiver this is an intentional peer close
    /// rather than an abrupt network teardown. No `requestId` (CLOSE is
    /// unidirectional).
    pub fn close_transport(&self, destination: &str) -> Result<()> {
        self.send_json(
            destination,
            NS_CONNECTION,
            &json!({"type": "CLOSE", "reasonCode": CLOSE_REASON_CLOSED_BY_PEER}),
        )
    }
}

impl Drop for CastChannel {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Open a TLS connection to a Chromecast at (host, 8009) and start the dispatcher.
/// Returns (channel, incoming-events).
pub fn connect(host: &str) -> Result<(CastChannel, Receiver<CastMessage>)> {
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()?;
    let tcp = TcpStream::connect((host, 8009))
        .with_context(|| format!("TCP connect to {host}:8009"))?;
    // Handshake needs headroom for multiple TLS round-trips.
    tcp.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
    tcp.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut tls = connector.connect(host, tcp).context("TLS handshake")?;
    // After the handshake, drop to a short poll interval so the dispatcher
    // loop wakes quickly to check `stop` and drain the outbound queue.
    tls.get_ref().set_read_timeout(Some(READ_TIMEOUT))?;

    let (outbound_tx, outbound_rx) = mpsc::channel::<CastMessage>();
    let (incoming_tx, incoming_rx) = mpsc::channel::<CastMessage>();
    let stop = Arc::new(AtomicBool::new(false));

    // CONNECT to the platform receiver up front.
    write_message(
        &mut tls,
        &CastMessage {
            source: local_sender_id().into(),
            destination: PLATFORM_RECEIVER_ID.into(),
            namespace: NS_CONNECTION.into(),
            payload: build_connect_payload().to_string(),
        },
    )?;

    let stop2 = Arc::clone(&stop);
    let thread = std::thread::Builder::new()
        .name("cast-dispatcher".into())
        .spawn(move || dispatcher(tls, outbound_rx, incoming_tx, stop2))
        .expect("spawn dispatcher");

    Ok((
        CastChannel { outbound: outbound_tx, stop, thread: Some(thread) },
        incoming_rx,
    ))
}

fn dispatcher(
    mut tls: native_tls::TlsStream<TcpStream>,
    outbound: Receiver<CastMessage>,
    incoming: Sender<CastMessage>,
    stop: Arc<AtomicBool>,
) {
    let mut buf = vec![0u8; 8192];
    let mut acc: Vec<u8> = Vec::new();
    let mut last_ping = Instant::now();

    while !stop.load(Ordering::Relaxed) {
        // 1. Try reading
        match tls.read(&mut buf) {
            Ok(0) => {
                log::debug!("Cast socket closed by peer");
                break;
            }
            Ok(n) => acc.extend(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
                   || e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(e) => {
                log::warn!("Cast socket read error: {e}");
                break;
            }
        }

        // 2. Parse complete messages
        while acc.len() >= 4 {
            let len = u32::from_be_bytes([acc[0], acc[1], acc[2], acc[3]]) as usize;
            if acc.len() < 4 + len {
                break;
            }
            let _ = acc.drain(..4);
            let body: Vec<u8> = acc.drain(..len).collect();
            match decode_cast_message(&body) {
                Ok(msg) => {
                    if msg.namespace == NS_HEARTBEAT {
                        // Auto-PONG to PINGs. Never forward heartbeat traffic
                        // to the caller — it is dispatcher-internal noise.
                        if payload_type_is(&msg.payload, "PING") {
                            let pong = CastMessage {
                                source: local_sender_id().into(),
                                destination: msg.source.clone(),
                                namespace: NS_HEARTBEAT.into(),
                                payload: r#"{"type":"PONG"}"#.into(),
                            };
                            let _ = write_message(&mut tls, &pong);
                        }
                    } else {
                        let _ = incoming.send(msg);
                    }
                }
                Err(e) => log::warn!("CastMessage decode failed: {e}"),
            }
        }

        // 3. Drain outbound queue. A write error means the socket is dead or
        // (after a partial write) the message framing is corrupt — either way
        // the connection is unusable, so shut the dispatcher down.
        loop {
            match outbound.try_recv() {
                Ok(msg) => {
                    if let Err(e) = write_message(&mut tls, &msg) {
                        log::warn!("Cast socket write error: {e}");
                        stop.store(true, Ordering::Relaxed);
                        break;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    stop.store(true, Ordering::Relaxed);
                    break;
                }
            }
        }

        // 4. Periodic heartbeat PING to keep the socket alive
        if last_ping.elapsed() >= HEARTBEAT_INTERVAL {
            let ping = CastMessage {
                source: local_sender_id().into(),
                destination: PLATFORM_RECEIVER_ID.into(),
                namespace: NS_HEARTBEAT.into(),
                payload: r#"{"type":"PING"}"#.into(),
            };
            let _ = write_message(&mut tls, &ping);
            last_ping = Instant::now();
        }
    }

    // Final drain: `CastChannel::drop` sets `stop` after the pipeline has
    // enqueued shutdown messages (STOP + two CLOSEs), so any messages queued
    // between the last loop iteration and the `stop` flip would otherwise be
    // dropped on the floor. Flush them synchronously before releasing the TLS
    // stream (whose Drop sends close_notify) so the receiver sees a clean
    // handshake.
    while let Ok(msg) = outbound.try_recv() {
        if let Err(e) = write_message(&mut tls, &msg) {
            log::debug!("Cast socket write error during shutdown drain: {e}");
            break;
        }
    }
    log::debug!("Cast dispatcher exiting");
}

fn write_message(tls: &mut native_tls::TlsStream<TcpStream>, msg: &CastMessage) -> std::io::Result<()> {
    let body = encode_cast_message(msg);
    let len = (body.len() as u32).to_be_bytes();
    tls.write_all(&len)?;
    tls.write_all(&body)?;
    tls.flush()?;
    log::debug!("cast tx [{}]: {}", msg.namespace, msg.payload);
    Ok(())
}

// -------- CastMessage protobuf (hand-encoded — this is all we need) --------
//
// message CastMessage {
//   required ProtocolVersion protocol_version = 1;   // enum, 0 = CASTV2_1_0
//   required string source_id = 2;
//   required string destination_id = 3;
//   required string namespace = 4;
//   required PayloadType payload_type = 5;           // 0 = STRING
//   optional string payload_utf8 = 6;
//   optional bytes  payload_binary = 7;
// }

fn write_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push(((v & 0x7F) as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

fn write_string(buf: &mut Vec<u8>, tag: u32, s: &str) {
    let bytes = s.as_bytes();
    write_varint(buf, ((tag as u64) << 3) | 2);
    write_varint(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

fn write_varint_field(buf: &mut Vec<u8>, tag: u32, v: u64) {
    // wire_type 0 (varint) — the `| 0` is elided
    write_varint(buf, (tag as u64) << 3);
    write_varint(buf, v);
}

/// Returns true iff `payload` parses as a JSON object with `{"type": expected}`.
/// Used to gate hot-path behavior (heartbeat auto-PONG, session-close detection)
/// on a real JSON parse rather than a fragile substring match.
pub fn payload_type_is(payload: &str, expected: &str) -> bool {
    let Ok(v) = serde_json::from_str::<Value>(payload) else { return false };
    v.get("type").and_then(|t| t.as_str()) == Some(expected)
}

pub fn encode_cast_message(msg: &CastMessage) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64 + msg.payload.len());
    write_varint_field(&mut buf, 1, 0); // protocol_version = CASTV2_1_0
    write_string(&mut buf, 2, &msg.source);
    write_string(&mut buf, 3, &msg.destination);
    write_string(&mut buf, 4, &msg.namespace);
    write_varint_field(&mut buf, 5, 0); // payload_type = STRING
    write_string(&mut buf, 6, &msg.payload);
    buf
}

fn read_varint(data: &[u8], pos: &mut usize) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0;
    loop {
        if *pos >= data.len() {
            bail!("varint overruns buffer");
        }
        let b = data[*pos];
        *pos += 1;
        result |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            bail!("varint too long");
        }
    }
}

pub fn decode_cast_message(data: &[u8]) -> Result<CastMessage> {
    let mut pos = 0usize;
    let mut source = String::new();
    let mut destination = String::new();
    let mut namespace = String::new();
    let mut payload = String::new();

    while pos < data.len() {
        let key = read_varint(data, &mut pos)?;
        let tag = (key >> 3) as u32;
        let wire = (key & 0x7) as u8;
        match wire {
            0 => {
                let _ = read_varint(data, &mut pos)?; // discard varint fields we don't use
            }
            2 => {
                let len = read_varint(data, &mut pos)? as usize;
                if pos + len > data.len() {
                    bail!("length-delimited field overruns buffer");
                }
                let slice = &data[pos..pos + len];
                pos += len;
                match tag {
                    2 => source = String::from_utf8_lossy(slice).into_owned(),
                    3 => destination = String::from_utf8_lossy(slice).into_owned(),
                    4 => namespace = String::from_utf8_lossy(slice).into_owned(),
                    6 => payload = String::from_utf8_lossy(slice).into_owned(),
                    _ => {} // ignore payload_binary (7) and unknown fields
                }
            }
            _ => bail!("unsupported wire type {wire} at tag {tag}"),
        }
    }

    Ok(CastMessage { source, destination, namespace, payload })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Byte-identical to the official protobuf encoder: expected hex was
    /// generated with pychromecast's cast_channel_pb2 (protoc output).
    #[test]
    fn encoding_matches_official_protobuf() {
        let ping = CastMessage {
            source: "sender-0".into(),
            destination: "receiver-0".into(),
            namespace: NS_HEARTBEAT.into(),
            payload: r#"{"type":"PING"}"#.into(),
        };
        assert_eq!(
            hex::encode(encode_cast_message(&ping)),
            "0800120873656e6465722d301a0a72656365697665722d30222775726e3a782d\
             636173743a636f6d2e676f6f676c652e636173742e74702e6865617274626561\
             742800320f7b2274797065223a2250494e47227d",
        );
        let offer = CastMessage {
            source: "sender-0".into(),
            destination: "173cb36a-a488-4fc3-963a-651334ad51c1".into(),
            namespace: NS_WEBRTC.into(),
            payload: r#"{"type":"OFFER","seqNum":1}"#.into(),
        };
        assert_eq!(
            hex::encode(encode_cast_message(&offer)),
            "0800120873656e6465722d301a2431373363623336612d613438382d34666333\
             2d393633612d363531333334616435316331222175726e3a782d636173743a63\
             6f6d2e676f6f676c652e636173742e7765627274632800321b7b227479706522\
             3a224f46464552222c227365714e756d223a317d",
        );
    }

    #[test]
    fn roundtrip_cast_message() {
        let m = CastMessage {
            source: "sender-0".into(),
            destination: "receiver-0".into(),
            namespace: "urn:x-cast:foo".into(),
            payload: r#"{"type":"PING"}"#.into(),
        };
        let encoded = encode_cast_message(&m);
        let decoded = decode_cast_message(&encoded).unwrap();
        assert_eq!(decoded.source, m.source);
        assert_eq!(decoded.destination, m.destination);
        assert_eq!(decoded.namespace, m.namespace);
        assert_eq!(decoded.payload, m.payload);
    }
}
