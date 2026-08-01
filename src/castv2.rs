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

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use native_tls::TlsConnector;
use serde_json::{json, Value};

pub const NS_CONNECTION: &str = "urn:x-cast:com.google.cast.tp.connection";
pub const NS_HEARTBEAT: &str = "urn:x-cast:com.google.cast.tp.heartbeat";
pub const NS_RECEIVER: &str = "urn:x-cast:com.google.cast.receiver";
pub const NS_WEBRTC: &str = "urn:x-cast:com.google.cast.webrtc";

pub const PLATFORM: &str = "receiver-0";
pub const SENDER_LOCAL: &str = "sender-0";

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
            source: SENDER_LOCAL.into(),
            destination: destination.into(),
            namespace: namespace.into(),
            payload: payload.to_string(),
        })
    }

    /// Send CONNECT to a destination (must be done before any app message).
    pub fn connect_transport(&self, destination: &str) -> Result<()> {
        self.send_json(destination, NS_CONNECTION, &json!({"type": "CONNECT"}))
    }

    pub fn close(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
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
            source: SENDER_LOCAL.into(),
            destination: PLATFORM.into(),
            namespace: NS_CONNECTION.into(),
            payload: json!({"type": "CONNECT"}).to_string(),
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
    let mut acc: VecDeque<u8> = VecDeque::new();
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
                        if msg.payload.contains(r#""type":"PING""#) {
                            let pong = CastMessage {
                                source: SENDER_LOCAL.into(),
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
                source: SENDER_LOCAL.into(),
                destination: PLATFORM.into(),
                namespace: NS_HEARTBEAT.into(),
                payload: r#"{"type":"PING"}"#.into(),
            };
            let _ = write_message(&mut tls, &ping);
            last_ping = Instant::now();
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
