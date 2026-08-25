//! Cast v2 channel: TLS to port 8009, length-prefixed CastMessage protobuf.
//! One dispatcher thread owns the TLS stream; PINGs are auto-PONGed inline, everything else is forwarded to the caller.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, ClientConnection, DigitallySignedStruct, SignatureScheme, StreamOwned};
use rand::Rng;
use serde_json::{json, Value};

pub const NS_CONNECTION: &str = "urn:x-cast:com.google.cast.tp.connection";
pub const NS_HEARTBEAT: &str = "urn:x-cast:com.google.cast.tp.heartbeat";
pub const NS_RECEIVER: &str = "urn:x-cast:com.google.cast.receiver";
pub const NS_WEBRTC: &str = "urn:x-cast:com.google.cast.webrtc";

pub const PLATFORM_RECEIVER_ID: &str = "receiver-0";

/// Chromium `kVirtualConnectionClosedByPeer` (`cast_message_util.h`).
const CLOSE_REASON_CLOSED_BY_PEER: u32 = 5;

/// Random per-process sender id (Chromium `CastMessageHandler` convention).
/// A fresh identity per run keeps a leftover virtual connection on the receiver from a prior run from colliding with ours.
pub fn local_sender_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| format!("sender-{}", rand::rng().random_range(0..1_000_000)))
}

/// Matches Chromium `CreateVirtualConnectionRequest` (`cast_message_util.cc`).
/// Enum values from `cast_message_util.h`: `connType=0` (kStrong), `sdkType=2`, `platform=6` (Linux), `connectionType=1` (LAN).
fn build_connect_payload() -> Value {
    const USER_AGENT: &str = concat!("chromecast-sink/", env!("CARGO_PKG_VERSION"), " (Linux; Rust)");
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
const READ_TIMEOUT: Duration = Duration::from_millis(50);
/// Handshake needs headroom for multiple TLS round-trips; a 50ms poll here would spuriously fail on a slow network.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// openscreen `kMaxBodySize` (`cast/common/channel/message_framer.cc`).
const MAX_BODY_SIZE: usize = 65536;
/// Chromium `OpenParams::liveness_timeout_in_seconds` (`cast_media_sink_service_impl.h`).
/// Two HEARTBEAT_INTERVALs, so a single dropped PONG is not fatal.
const LIVENESS_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct CastMessage {
    pub source: String,
    pub destination: String,
    pub namespace: String,
    pub payload: String, // STRING payloads only
}

pub struct CastChannel {
    outbound: Sender<CastMessage>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl CastChannel {
    pub fn send(&self, msg: CastMessage) -> Result<()> {
        self.outbound.send(msg).context("dispatcher gone")
    }

    pub fn send_json(&self, destination: &str, namespace: &str, payload: &Value) -> Result<()> {
        self.send(CastMessage {
            source: local_sender_id().into(),
            destination: destination.into(),
            namespace: namespace.into(),
            payload: payload.to_string(),
        })
    }

    /// Required before sending any app message to `destination`.
    pub fn connect_transport(&self, destination: &str) -> Result<()> {
        self.send_json(destination, NS_CONNECTION, &build_connect_payload())
    }

    /// Close a virtual connection.
    /// `reasonCode` signals an intentional peer close rather than an abrupt drop (Chromium `CreateVirtualConnectionClose`).
    pub fn close_transport(&self, destination: &str) -> Result<()> {
        self.send_json(destination, NS_CONNECTION, &json!({"type": "CLOSE", "reasonCode": CLOSE_REASON_CLOSED_BY_PEER}))
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

pub fn connect(host: &str) -> Result<(CastChannel, Receiver<CastMessage>)> {
    // ring provider is process-global; Err on subsequent calls is fine.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut tcp = TcpStream::connect((host, 8009)).with_context(|| format!("TCP connect to {host}:8009"))?;
    tcp.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
    tcp.set_write_timeout(Some(Duration::from_secs(5)))?;

    let provider = CryptoProvider::get_default()
        .expect("rustls default CryptoProvider not installed")
        .clone();
    let cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify(provider)))
        .with_no_client_auth();
    let name = ServerName::try_from(host).map(|n| n.to_owned()).map_err(|e| anyhow::anyhow!("invalid server name {host}: {e}"))?;
    let mut conn = ClientConnection::new(Arc::new(cfg), name).context("TLS client init")?;
    conn.complete_io(&mut tcp).context("TLS handshake")?;
    let mut tls = StreamOwned::new(conn, tcp);
    // Post-handshake: short poll so the dispatcher can wake to check `stop` and drain outbound.
    tls.sock.set_read_timeout(Some(READ_TIMEOUT))?;

    let (outbound_tx, outbound_rx) = mpsc::channel::<CastMessage>();
    let (incoming_tx, incoming_rx) = mpsc::channel::<CastMessage>();
    let stop = Arc::new(AtomicBool::new(false));

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

    Ok((CastChannel { outbound: outbound_tx, stop, thread: Some(thread) }, incoming_rx))
}

fn dispatcher(
    mut tls: StreamOwned<ClientConnection, TcpStream>,
    outbound: Receiver<CastMessage>,
    incoming: Sender<CastMessage>,
    stop: Arc<AtomicBool>,
) {
    let mut buf = vec![0u8; 8192];
    let mut acc: Vec<u8> = Vec::new();
    let mut last_ping = Instant::now();
    let mut last_rx = Instant::now();

    'session: while !stop.load(Ordering::Relaxed) {
        match tls.read(&mut buf) {
            Ok(0) => {
                log::debug!("Cast socket closed by peer");
                break;
            }
            Ok(n) => {
                acc.extend(&buf[..n]);
                last_rx = Instant::now();
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {}
            // rustls surfaces peer TCP drop without close_notify as UnexpectedEof; Chromecast never sends close_notify.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                log::debug!("Cast socket closed by peer");
                break;
            }
            Err(e) => {
                log::warn!("Cast socket read error: {e}");
                break;
            }
        }

        loop {
            let body = match take_frame(&mut acc) {
                Ok(Some(body)) => body,
                Ok(None) => break,
                Err(e) => {
                    log::warn!("Cast framing error: {e}");
                    break 'session;
                }
            };
            match decode_cast_message(&body) {
                Ok(msg) => {
                    if msg.namespace == NS_HEARTBEAT {
                        // Never forward heartbeat to the caller — dispatcher-internal.
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

        // Any write error kills the dispatcher — a partial write corrupts the length-prefix framing for everything after it.
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

        // A dead receiver leaves TCP silently open for minutes; without this the PINGs above go unanswered forever and nothing ever reports the loss.
        if last_rx.elapsed() >= LIVENESS_TIMEOUT {
            log::warn!("No data from Chromecast in {LIVENESS_TIMEOUT:?}; channel is dead");
            break;
        }
    }

    // Flush any messages enqueued between the last loop tick and the `stop` flip in CastChannel::drop, so a graceful shutdown handshake still makes it out before TLS close_notify.
    while let Ok(msg) = outbound.try_recv() {
        if let Err(e) = write_message(&mut tls, &msg) {
            log::debug!("Cast socket write error during shutdown drain: {e}");
            break;
        }
    }
    log::debug!("Cast dispatcher exiting");
}

/// `Ok(None)` means "need more bytes"; `Err` means the framing is unrecoverable.
fn take_frame(acc: &mut Vec<u8>) -> Result<Option<Vec<u8>>> {
    if acc.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([acc[0], acc[1], acc[2], acc[3]]) as usize;
    // Checked before buffering the body, or a peer can grow `acc` without bound.
    if len > MAX_BODY_SIZE {
        bail!("frame declares {len} bytes, over the {MAX_BODY_SIZE}-byte maximum");
    }
    if acc.len() < 4 + len {
        return Ok(None);
    }
    let _ = acc.drain(..4);
    Ok(Some(acc.drain(..len).collect()))
}

fn write_message(tls: &mut StreamOwned<ClientConnection, TcpStream>, msg: &CastMessage) -> std::io::Result<()> {
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
/// Used to gate hot-path behavior (heartbeat auto-PONG, session-close detection) on a real JSON parse rather than a fragile substring match.
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
                let len = read_varint(data, &mut pos)?;
                // Peer-supplied and free to claim u64::MAX; compared against the bytes left rather than `pos + len`, which would wrap.
                if len > (data.len() - pos) as u64 {
                    bail!("length-delimited field overruns buffer");
                }
                let len = len as usize;
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

#[derive(Debug)]
struct NoVerify(Arc<CryptoProvider>);

impl ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Expected hex is protoc output for openscreen's `cast_channel.proto`.
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

    fn sample() -> CastMessage {
        CastMessage {
            source: "sender-0".into(),
            destination: "receiver-0".into(),
            namespace: NS_HEARTBEAT.into(),
            payload: r#"{"type":"PING"}"#.into(),
        }
    }

    #[test]
    fn decode_rejects_oversized_length_delimited_field() {
        for len in [u64::MAX, u64::MAX - 1, 1 << 63, 1 << 62, u32::MAX as u64, 17] {
            let mut data = vec![0x12]; // tag 2 (source_id), wire type 2
            write_varint(&mut data, len);
            data.extend_from_slice(b"AAAAAAAAAAAAAAAA");
            assert!(decode_cast_message(&data).is_err(), "len={len}");
        }
    }

    #[test]
    fn decode_rejects_malformed_varints() {
        assert!(decode_cast_message(&[0x80]).is_err()); // continuation with no successor
        assert!(decode_cast_message(&[0xFF; 16]).is_err()); // varint past 10 bytes
        assert!(decode_cast_message(&[0x08]).is_err()); // wire type 0, value missing
        assert!(decode_cast_message(&[0x09, 0, 0, 0, 0, 0, 0, 0, 0]).is_err()); // wire type 1
        assert!(decode_cast_message(&[0x0D, 0, 0, 0, 0]).is_err()); // wire type 5
    }

    #[test]
    fn decode_never_panics_on_corrupt_input() {
        let full = encode_cast_message(&sample());
        for n in 0..full.len() {
            let _ = decode_cast_message(&full[..n]);
        }
        for i in 0..full.len() {
            for bit in 0..8 {
                let mut m = full.clone();
                m[i] ^= 1 << bit;
                let _ = decode_cast_message(&m);
            }
        }
    }

    #[test]
    fn take_frame_rejects_oversized_declared_length() {
        for len in [u32::MAX, 300 * 1024 * 1024, MAX_BODY_SIZE as u32 + 1] {
            let mut acc = len.to_be_bytes().to_vec();
            assert!(take_frame(&mut acc).is_err(), "len={len}");
            assert_eq!(acc.len(), 4);
        }
    }

    #[test]
    fn take_frame_accepts_max_body_size() {
        let body = vec![0u8; MAX_BODY_SIZE];
        let mut acc = (MAX_BODY_SIZE as u32).to_be_bytes().to_vec();
        acc.extend_from_slice(&body);
        assert_eq!(take_frame(&mut acc).unwrap(), Some(body));
        assert!(acc.is_empty());
    }

    #[test]
    fn take_frame_waits_for_complete_frames() {
        let body = encode_cast_message(&sample());
        let mut one = (body.len() as u32).to_be_bytes().to_vec();
        one.extend_from_slice(&body);
        let mut wire = one.clone();
        wire.extend_from_slice(&one);

        let mut acc = Vec::new();
        let mut frames = 0;
        for b in &wire {
            acc.push(*b);
            while let Some(f) = take_frame(&mut acc).unwrap() {
                assert_eq!(f, body);
                frames += 1;
            }
        }
        assert_eq!(frames, 2);
        assert!(acc.is_empty());
    }
}
