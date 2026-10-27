//! Cast RTP: custom "cast" profile (NOT SRTP).
//! AES-128-CTR per frame, 12 B RTP header + 7 B Cast extension, RTCP SR every 500 ms.
//!
//! SRs are *required*: without them the receiver kills the mirroring app within seconds (verified on Nest Mini).

use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aes::cipher::{KeyIvInit, StreamCipher};
use aes::Aes128;
use ctr::Ctr128BE;

use crate::audio_format::SAMPLE_RATE;

/// 10 ms Opus frames.
pub const OPUS_SAMPLES_PER_FRAME: u32 = SAMPLE_RATE / 100;

const NTP_EPOCH_OFFSET_SECS: u64 = 2_208_988_800;

/// Openscreen `kMaxUnackedFrames`. The receiver expands a frame ID within 128 of its newest, so a resend of anything older would decrypt under the wrong ID.
const HISTORY_FRAMES: usize = 120;

/// Openscreen skips a retransmit sent within one RTT. We don't measure RTT.
const MIN_RESEND_INTERVAL: Duration = Duration::from_millis(20);

const NACK_POLL_INTERVAL: Duration = Duration::from_millis(200);

const RTCP_PT_PAYLOAD_SPECIFIC: u8 = 206;
const RTCP_FMT_CAST_FEEDBACK: u8 = 15;
const CAST_FEEDBACK_MAGIC: u32 = 0x4341_5354;
const ALL_PACKETS_LOST: u16 = 0xFFFF;

type Aes128Ctr = Ctr128BE<Aes128>;

pub struct Config {
    pub chromecast_host: String,
    pub udp_port: u16,
    pub ssrc: u32,
    pub payload_type: u8,
    pub aes_key: [u8; 16],
    pub aes_iv_mask: [u8; 16],
}

/// One Opus frame is always one packet (~161 B). `frame_id` doubles as the RTP sequence number and RTCP packet count, and the RTP timestamp is `frame_id * OPUS_SAMPLES_PER_FRAME`.
pub struct CastRtpSender {
    config: Config,
    socket: Arc<UdpSocket>,
    dest: (String, u16),
    frame_id: Arc<AtomicU32>,
    octet_count: Arc<AtomicU64>,
    stop_tx: Option<mpsc::Sender<()>>,
    rtcp_thread: Option<JoinHandle<()>>,
    history: Arc<Mutex<History>>,
    nack_stop: Arc<AtomicBool>,
    nack_thread: Option<JoinHandle<()>>,
}

struct SentPacket {
    frame_id: u32,
    packet: Vec<u8>,
    sent_at: Instant,
}

struct History {
    slots: [Option<SentPacket>; HISTORY_FRAMES],
    last_sent: Option<u32>,
}

impl History {
    fn new() -> Self {
        Self {
            slots: [const { None }; HISTORY_FRAMES],
            last_sent: None,
        }
    }

    fn get_mut(&mut self, frame_id: u32) -> Option<&mut SentPacket> {
        self.slots[frame_id as usize % HISTORY_FRAMES]
            .as_mut()
            .filter(|sent| sent.frame_id == frame_id)
    }

    fn record(&mut self, frame_id: u32, packet: Vec<u8>) {
        self.slots[frame_id as usize % HISTORY_FRAMES] = Some(SentPacket {
            frame_id,
            packet,
            sent_at: Instant::now(),
        });
        self.last_sent = Some(frame_id);
    }
}

impl CastRtpSender {
    pub fn new(config: Config) -> std::io::Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.set_read_timeout(Some(NACK_POLL_INTERVAL))?;
        let dest = (config.chromecast_host.clone(), config.udp_port);
        Ok(Self {
            config,
            socket: Arc::new(socket),
            dest,
            frame_id: Arc::new(AtomicU32::new(0)),
            octet_count: Arc::new(AtomicU64::new(0)),
            stop_tx: None,
            rtcp_thread: None,
            history: Arc::new(Mutex::new(History::new())),
            nack_stop: Arc::new(AtomicBool::new(false)),
            nack_thread: None,
        })
    }

    /// Nonce = 16 zero bytes with `frame_id` BE u32 at offset 8, XOR `aes_iv_mask`.
    fn nonce(&self, frame_id: u32) -> [u8; 16] {
        let mut nonce = [0u8; 16];
        nonce[8..12].copy_from_slice(&frame_id.to_be_bytes());
        for (n, m) in nonce.iter_mut().zip(self.config.aes_iv_mask.iter()) {
            *n ^= m;
        }
        nonce
    }

    fn encrypt(&self, data: &[u8], frame_id: u32) -> Vec<u8> {
        let nonce = self.nonce(frame_id);
        let mut buf = data.to_vec();
        let mut cipher = Aes128Ctr::new(&self.config.aes_key.into(), &nonce.into());
        cipher.apply_keystream(&mut buf);
        buf
    }

    /// 12 B RTP header + 7 B Cast extension.
    /// Openscreen `RtpPacketizer` sets `kRtpHasReferenceFrameIdBitMask=0x40` unconditionally. For independently-decodable frames `referenced_frame_id` (byte 18) equals `frame_id` per `encoded_frame.h`.
    fn build_packet(&self, encrypted: &[u8], frame_id: u32) -> Vec<u8> {
        const CAST_EXT_HEADER_LEN: usize = 7;
        let mut pkt = Vec::with_capacity(12 + CAST_EXT_HEADER_LEN + encrypted.len());
        let v_p_x_cc: u8 = 0x80;
        let m_pt: u8 = 0x80 | (self.config.payload_type & 0x7F);
        let seq = (frame_id & 0xFFFF) as u16;
        let ts = frame_id.wrapping_mul(OPUS_SAMPLES_PER_FRAME);

        pkt.push(v_p_x_cc);
        pkt.push(m_pt);
        pkt.extend_from_slice(&seq.to_be_bytes());
        pkt.extend_from_slice(&ts.to_be_bytes());
        pkt.extend_from_slice(&self.config.ssrc.to_be_bytes());

        pkt.push(0x80 | 0x40);
        let frame_id_u8 = (frame_id & 0xFF) as u8;
        pkt.push(frame_id_u8);
        pkt.extend_from_slice(&0u16.to_be_bytes());
        pkt.extend_from_slice(&0u16.to_be_bytes());
        pkt.push(frame_id_u8);

        pkt.extend_from_slice(encrypted);
        pkt
    }

    pub fn send_frame(&mut self, opus_frame: &[u8]) -> std::io::Result<()> {
        let fid = self.frame_id.load(Ordering::Relaxed);
        let enc = self.encrypt(opus_frame, fid);
        let pkt = self.build_packet(&enc, fid);
        // Never advance frame_id on failure: a 128+ gap desyncs the receiver's AES-CTR keystream.
        self.socket.send_to(&pkt, &self.dest)?;
        self.history.lock().unwrap().record(fid, pkt);

        self.frame_id.store(fid.wrapping_add(1), Ordering::Relaxed);
        self.octet_count
            .fetch_add(opus_frame.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    #[cfg(test)]
    pub fn stats(&self) -> (u32, u64) {
        (
            self.frame_id.load(Ordering::Relaxed),
            self.octet_count.load(Ordering::Relaxed),
        )
    }

    pub fn stats_handle(&self) -> StatsHandle {
        StatsHandle {
            frame_id: Arc::clone(&self.frame_id),
            octet_count: Arc::clone(&self.octet_count),
        }
    }

    /// Spawn the 500 ms RTCP SR thread.
    /// Required. Without SRs the receiver kills the mirroring app within seconds.
    pub fn start(&mut self) {
        let (tx, rx) = mpsc::channel::<()>();
        let socket = Arc::clone(&self.socket);
        let dest = self.dest.clone();
        let ssrc = self.config.ssrc;
        let frame_id = Arc::clone(&self.frame_id);
        let octet_count = Arc::clone(&self.octet_count);

        self.stop_tx = Some(tx);
        self.rtcp_thread = Some(
            std::thread::Builder::new()
                .name("rtcp-sender".into())
                .spawn(move || loop {
                    let fid = frame_id.load(Ordering::Relaxed);
                    let octets = octet_count.load(Ordering::Relaxed);
                    let sr = build_rtcp_sr(ssrc, fid, octets);
                    if let Err(e) = socket.send_to(&sr, &dest) {
                        log::debug!("RTCP send error: {e}");
                    }
                    match rx.recv_timeout(Duration::from_millis(500)) {
                        Err(RecvTimeoutError::Timeout) => continue,
                        _ => break,
                    }
                })
                .expect("spawn rtcp thread"),
        );

        let socket = Arc::clone(&self.socket);
        let ssrc = self.config.ssrc;
        let history = Arc::clone(&self.history);
        let stop = Arc::clone(&self.nack_stop);
        match self.dest.0.parse::<IpAddr>() {
            Err(_) => log::warn!(
                "Receiver host {:?} is not an IP address; retransmission disabled",
                self.dest.0
            ),
            Ok(ip) => {
                let peer = SocketAddr::new(ip, self.dest.1);
                self.nack_thread = Some(
                    std::thread::Builder::new()
                        .name("rtcp-nack".into())
                        .spawn(move || serve_nacks(&socket, peer, ssrc, &history, &stop))
                        .expect("spawn rtcp nack thread"),
                );
            }
        }
        log::info!("Cast RTP started (RTCP interval=500ms)");
    }

    pub fn stop(&mut self) {
        self.stop_tx = None;
        if let Some(t) = self.rtcp_thread.take() {
            let _ = t.join();
        }
        self.nack_stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.nack_thread.take() {
            let _ = t.join();
        }
        log::info!(
            "Cast RTP sender stopped (sent {} packets, {} bytes)",
            self.frame_id.load(Ordering::Relaxed),
            self.octet_count.load(Ordering::Relaxed)
        );
    }
}

impl Drop for CastRtpSender {
    fn drop(&mut self) {
        if self.rtcp_thread.is_some() {
            self.stop();
        }
    }
}

#[derive(Clone)]
pub struct StatsHandle {
    frame_id: Arc<AtomicU32>,
    octet_count: Arc<AtomicU64>,
}

impl StatsHandle {
    pub fn snapshot(&self) -> (u32, u64) {
        (
            self.frame_id.load(Ordering::Relaxed),
            self.octet_count.load(Ordering::Relaxed),
        )
    }
}

fn serve_nacks(
    socket: &UdpSocket,
    peer: SocketAddr,
    ssrc: u32,
    history: &Mutex<History>,
    stop: &AtomicBool,
) {
    let mut buf = [0u8; 1500];
    while !stop.load(Ordering::Relaxed) {
        // Openscreen `SenderPacketRouter` likewise drops packets from anywhere but the receiver.
        let Ok((n, src)) = socket.recv_from(&mut buf) else {
            continue;
        };
        if src.ip() != peer.ip() {
            continue;
        }
        let mut history = history.lock().unwrap();
        let Some(last_sent) = history.last_sent else {
            continue;
        };
        let now = Instant::now();
        for frame_id in parse_nacked_frames(&buf[..n], ssrc, last_sent) {
            let Some(sent) = history.get_mut(frame_id) else {
                continue;
            };
            if now.duration_since(sent.sent_at) < MIN_RESEND_INTERVAL {
                continue;
            }
            match socket.send_to(&sent.packet, peer) {
                Ok(_) => {
                    sent.sent_at = now;
                    log::debug!("Retransmitted frame {frame_id}");
                }
                Err(e) => log::debug!("Retransmit of frame {frame_id} failed: {e}"),
            }
        }
    }
}

/// Greatest ID `<= max` whose low 8 bits are `truncated` (openscreen `ExpandLessThanOrEqual`).
fn expand_le(max: u32, truncated: u8) -> u32 {
    max.wrapping_sub((max as u8).wrapping_sub(truncated) as u32)
}

/// Frame IDs whose packet 0 the last Cast feedback in a compound RTCP packet NACKs, following openscreen `CompoundRtcpParser::ParseFeedback`.
///
/// The peer is unauthenticated: a malformed compound yields nothing, as in openscreen, and feedback for another SSRC is skipped.
fn parse_nacked_frames(mut buf: &[u8], ssrc: u32, last_sent: u32) -> Vec<u32> {
    let mut checkpoint: Option<u32> = None;
    let mut nacked = Vec::new();
    while !buf.is_empty() {
        if buf.len() < 4 || buf[0] >> 5 != 0b100 {
            return Vec::new();
        }
        let fmt = buf[0] & 0x1F;
        let pt = buf[1];
        let payload_len = u16::from_be_bytes([buf[2], buf[3]]) as usize * 4;
        let Some(payload) = buf.get(4..4 + payload_len) else {
            return Vec::new();
        };
        buf = &buf[4 + payload_len..];
        if pt != RTCP_PT_PAYLOAD_SPECIFIC || fmt != RTCP_FMT_CAST_FEEDBACK {
            continue;
        }
        if payload.len() < 16 {
            return Vec::new();
        }
        if u32::from_be_bytes(payload[4..8].try_into().unwrap()) != ssrc {
            continue;
        }
        if u32::from_be_bytes(payload[8..12].try_into().unwrap()) != CAST_FEEDBACK_MAGIC {
            return Vec::new();
        }
        let ck = expand_le(last_sent, payload[12]);
        let loss_fields = payload[13] as usize;
        if checkpoint.is_some_and(|prev| (ck.wrapping_sub(prev) as i32) < 0) {
            continue;
        }
        checkpoint = Some(ck);
        nacked.clear();
        let Some(fields) = payload.get(16..16 + loss_fields * 4) else {
            return Vec::new();
        };
        for field in fields.chunks_exact(4) {
            let frame_id = expand_le(ck.wrapping_add(256), field[0]);
            let packet_id = u16::from_be_bytes([field[1], field[2]]);
            if packet_id == 0 || packet_id == ALL_PACKETS_LOST {
                nacked.push(frame_id);
            }
        }
    }
    nacked
}

fn build_rtcp_sr(ssrc: u32, frame_id: u32, octet_count: u64) -> [u8; 28] {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    build_rtcp_sr_at(ssrc, frame_id, octet_count, now)
}

fn build_rtcp_sr_at(ssrc: u32, frame_id: u32, octet_count: u64, now: Duration) -> [u8; 28] {
    let ntp_sec = (now.as_secs() + NTP_EPOCH_OFFSET_SECS) as u32;
    let ntp_frac =
        ((now.subsec_nanos() as u64 * (1u64 << 32) / 1_000_000_000) & 0xFFFF_FFFF) as u32;
    let rtp_ts = frame_id.wrapping_mul(OPUS_SAMPLES_PER_FRAME);

    let mut buf = [0u8; 28];
    buf[0] = 0x80; // V=2, P=0, RC=0
    buf[1] = 200; // Sender Report
    buf[2..4].copy_from_slice(&6u16.to_be_bytes()); // length = 6 (32-bit words - 1)
    buf[4..8].copy_from_slice(&ssrc.to_be_bytes());
    buf[8..12].copy_from_slice(&ntp_sec.to_be_bytes());
    buf[12..16].copy_from_slice(&ntp_frac.to_be_bytes());
    buf[16..20].copy_from_slice(&rtp_ts.to_be_bytes());
    buf[20..24].copy_from_slice(&frame_id.to_be_bytes());
    buf[24..28].copy_from_slice(&(octet_count as u32).to_be_bytes());
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    fn cfg() -> Config {
        Config {
            chromecast_host: "127.0.0.1".into(),
            udp_port: 9999,
            ssrc: 0xDEAD_BEEF,
            payload_type: 127,
            aes_key: hex!("00000000000000000000000000000000"),
            aes_iv_mask: hex!("11111111111111111111111111111111"),
        }
    }

    #[test]
    fn failed_send_does_not_advance_frame_id() {
        let mut s = CastRtpSender::new(cfg()).unwrap();
        // Broadcast without SO_BROADCAST: EACCES on Linux, WSAEACCES on Windows. Nothing leaves the host.
        // Port 0 won't do here: Linux rejects it with EINVAL, but Windows sends the datagram anyway.
        s.dest = ("255.255.255.255".into(), 9);

        for _ in 0..3 {
            assert!(s.send_frame(b"opus").is_err());
            assert_eq!(s.stats(), (0, 0));
        }

        let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
        rx.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s.dest = ("127.0.0.1".into(), rx.local_addr().unwrap().port());
        s.send_frame(b"opus").unwrap();

        let mut buf = [0u8; 64];
        let n = rx.recv(&mut buf).unwrap();
        assert_eq!(n, 19 + 4);
        assert_eq!(buf[13], 0, "resumes at the frame_id the failed sends held");
        assert_eq!(s.stats(), (1, 4));
    }

    fn diff_cfg() -> Config {
        let mut key = [0u8; 16];
        let mut mask = [0u8; 16];
        for i in 0..16 {
            key[i] = i as u8;
            mask[i] = 0xF0 + i as u8;
        }
        Config {
            chromecast_host: "127.0.0.1".into(),
            udp_port: 1,
            ssrc: 0xDEAD_BEEF,
            payload_type: 127,
            aes_key: key,
            aes_iv_mask: mask,
        }
    }

    #[test]
    fn encryption_matches_reference_vectors() {
        let s = CastRtpSender::new(diff_cfg()).unwrap();
        let payload = b"0123456789abcdef";
        for (fid, expected) in [
            (0u32, "5696f5db0067077faf68bf655072c8cb"),
            (5, "32f6787cc44c4201f466acc715997af9"),
            (256, "da4c3a84b5565c4161b5d95ebcffa2d4"),
            (0x1234_5678, "747b8dae4bc012851ec33fba1e6b8ea3"),
        ] {
            assert_eq!(
                hex::encode(s.encrypt(payload, fid)),
                expected,
                "frame_id={fid}"
            );
        }
    }

    #[test]
    fn packet_matches_openscreen_layout() {
        let s = CastRtpSender::new(diff_cfg()).unwrap();
        assert_eq!(
            hex::encode(s.build_packet(b"payload", 5)),
            "80ff000500000960deadbeefc00500000000057061796c6f6164"
        );
        // frame_id > 255: both offset 13 and offset 18 truncate to low 8 bits.
        assert_eq!(
            hex::encode(s.build_packet(b"xy", 300)),
            "80ff012c00023280deadbeefc02c000000002c7879"
        );
    }

    #[test]
    fn rtcp_sr_matches_reference_vector() {
        let sr = build_rtcp_sr_at(
            0xDEAD_BEEF,
            12345,
            987_654,
            Duration::new(1_234_567_890, 500_000_000),
        );
        assert_eq!(
            hex::encode(sr),
            "80c80006deadbeefcd40815280000000005a6ae000003039000f1206"
        );
        // The wall-clock wrapper may only differ in the NTP timestamp (bytes 8..16).
        let live = build_rtcp_sr(0xDEAD_BEEF, 12345, 987_654);
        assert_eq!(live[..8], sr[..8]);
        assert_eq!(live[16..], sr[16..]);
    }

    /// Cast feedback (PT 206, FMT 15) from receiver SSRC 1 to `ssrc`, preceded by an XR block as the Nest Mini sends it.
    fn feedback(ssrc: u32, checkpoint: u8, losses: &[(u8, u16, u8)]) -> Vec<u8> {
        let mut pkt = hex!("80cf0004 00000001 04000002 00000000 00000000").to_vec();
        pkt.extend_from_slice(&[0x8F, 206]);
        pkt.extend_from_slice(&(4 + losses.len() as u16).to_be_bytes());
        pkt.extend_from_slice(&1u32.to_be_bytes());
        pkt.extend_from_slice(&ssrc.to_be_bytes());
        pkt.extend_from_slice(b"CAST");
        pkt.extend_from_slice(&[checkpoint, losses.len() as u8, 0, 0]);
        for &(frame, packet, bits) in losses {
            pkt.push(frame);
            pkt.extend_from_slice(&packet.to_be_bytes());
            pkt.push(bits);
        }
        pkt
    }

    #[test]
    fn nack_frame_ids_expand_past_the_checkpoint() {
        // Checkpoint 195 is frame 707. NACKs for 196 and 2 wrap to 708 and 770.
        let fb = feedback(7, 195, &[(196, ALL_PACKETS_LOST, 0), (2, 0, 0)]);
        assert_eq!(parse_nacked_frames(&fb, 7, 780), [708, 770]);
        assert!(parse_nacked_frames(&fb, 8, 780).is_empty(), "other SSRC");
    }

    #[test]
    fn nacks_for_packets_we_never_send_are_ignored() {
        let fb = feedback(7, 10, &[(11, 1, 0b11), (12, 0, 0b1)]);
        assert_eq!(parse_nacked_frames(&fb, 7, 20), [12]);
    }

    #[test]
    fn malformed_rtcp_yields_no_nacks() {
        let fb = feedback(7, 10, &[(11, 0, 0)]);
        let mut truncated = fb.clone();
        truncated.pop();
        let mut bad_magic = fb.clone();
        bad_magic[32] ^= 1;
        let mut overstated_losses = fb.clone();
        overstated_losses[37] = 2;
        for bad in [&truncated[..], &bad_magic, &overstated_losses, &[0x80]] {
            assert!(parse_nacked_frames(bad, 7, 20).is_empty());
        }
    }

    #[test]
    fn history_keeps_only_the_last_window_of_frames() {
        let mut h = History::new();
        for fid in 0..=200 {
            h.record(fid, vec![]);
        }
        assert!(
            h.get_mut(80).is_none(),
            "200 - 80 = 120 is out of the window"
        );
        assert_eq!(h.get_mut(81).map(|s| s.frame_id), Some(81));
        assert!(h.get_mut(201).is_none(), "not sent yet");
    }

    #[test]
    fn nacked_frame_is_retransmitted_byte_for_byte() {
        let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
        rx.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let mut c = cfg();
        c.udp_port = rx.local_addr().unwrap().port();
        let mut s = CastRtpSender::new(c).unwrap();
        let sender_port = s.socket.local_addr().unwrap().port();
        s.start();

        // The deadline spans calls: the 500 ms SRs would otherwise keep resetting a per-recv timeout.
        let deadline = Instant::now() + Duration::from_secs(5);
        let next_rtp = || {
            let mut buf = [0u8; 64];
            while Instant::now() < deadline {
                if let Ok(n) = rx.recv(&mut buf) {
                    if buf[1] != 200 {
                        return buf[..n].to_vec();
                    }
                }
            }
            panic!("no RTP packet before the deadline");
        };

        let mut sent = Vec::new();
        for payload in [b"aaaa", b"bbbb", b"cccc"] {
            s.send_frame(payload).unwrap();
            sent.push(next_rtp());
        }

        std::thread::sleep(MIN_RESEND_INTERVAL);
        let nack = feedback(s.config.ssrc, 0, &[(1, ALL_PACKETS_LOST, 0)]);
        rx.send_to(&nack, ("127.0.0.1", sender_port)).unwrap();
        assert_eq!(next_rtp(), sent[1]);
        s.stop();
    }
}
