//! Cast RTP: custom "cast" profile (NOT SRTP).
//! AES-128-CTR per frame, 12 B RTP header + 7 B Cast extension, RTCP SR every 500 ms.
//!
//! SRs are *required*: without them the receiver kills the mirroring app within seconds (verified on Nest Mini).

use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aes::Aes128;
use aes::cipher::{KeyIvInit, StreamCipher};
use ctr::Ctr128BE;

/// Opus at 48 kHz with 10 ms frames = 480 samples/frame.
pub const OPUS_SAMPLES_PER_FRAME: u32 = 480;

const NTP_EPOCH_OFFSET_SECS: u64 = 2_208_988_800;

type Aes128Ctr = Ctr128BE<Aes128>;

pub struct Config {
    pub chromecast_host: String,
    pub udp_port: u16,
    pub ssrc: u32,
    pub payload_type: u8,
    pub aes_key: [u8; 16],
    pub aes_iv_mask: [u8; 16],
}

/// One Opus frame is always one packet (~161 B), so `frame_id` doubles as the RTP sequence number and RTCP packet count, and the RTP timestamp is `frame_id * OPUS_SAMPLES_PER_FRAME`.
pub struct CastRtpSender {
    config: Config,
    socket: Arc<UdpSocket>,
    dest: (String, u16),
    frame_id: Arc<AtomicU32>,
    octet_count: Arc<AtomicU64>,
    stop_tx: Option<mpsc::Sender<()>>,
    rtcp_thread: Option<JoinHandle<()>>,
}

impl CastRtpSender {
    pub fn new(config: Config) -> std::io::Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        let dest = (config.chromecast_host.clone(), config.udp_port);
        Ok(Self {
            config,
            socket: Arc::new(socket),
            dest,
            frame_id: Arc::new(AtomicU32::new(0)),
            octet_count: Arc::new(AtomicU64::new(0)),
            stop_tx: None,
            rtcp_thread: None,
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
    /// Openscreen `RtpPacketizer` sets `kRtpHasReferenceFrameIdBitMask=0x40` unconditionally, so byte 18 (`referenced_frame_id`) is always present; for independently-decodable frames it equals `frame_id` per `encoded_frame.h`.
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

        self.frame_id.store(fid.wrapping_add(1), Ordering::Relaxed);
        self.octet_count.fetch_add(opus_frame.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    #[cfg(test)]
    pub fn stats(&self) -> (u32, u64) {
        (self.frame_id.load(Ordering::Relaxed), self.octet_count.load(Ordering::Relaxed))
    }

    pub fn stats_handle(&self) -> StatsHandle {
        StatsHandle { frame_id: Arc::clone(&self.frame_id), octet_count: Arc::clone(&self.octet_count) }
    }

    /// Spawn the 500 ms RTCP SR thread.
    /// Required — without SRs the receiver kills the mirroring app within seconds.
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
                .spawn(move || {
                    loop {
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
                    }
                })
                .expect("spawn rtcp thread"),
        );
        log::info!("Cast RTP started (RTCP interval=500ms)");
    }

    pub fn stop(&mut self) {
        self.stop_tx = None;
        if let Some(t) = self.rtcp_thread.take() {
            let _ = t.join();
        }
        log::info!(
            "Cast RTP sender stopped (sent {} packets, {} bytes)",
            self.frame_id.load(Ordering::Relaxed),
            self.octet_count.load(Ordering::Relaxed),
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
        (self.frame_id.load(Ordering::Relaxed), self.octet_count.load(Ordering::Relaxed))
    }
}

fn build_rtcp_sr(ssrc: u32, frame_id: u32, octet_count: u64) -> [u8; 28] {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    build_rtcp_sr_at(ssrc, frame_id, octet_count, now)
}

fn build_rtcp_sr_at(ssrc: u32, frame_id: u32, octet_count: u64, now: Duration) -> [u8; 28] {
    let ntp_sec = (now.as_secs() + NTP_EPOCH_OFFSET_SECS) as u32;
    let ntp_frac = ((now.subsec_nanos() as u64 * (1u64 << 32) / 1_000_000_000) & 0xFFFF_FFFF) as u32;
    let rtp_ts = frame_id.wrapping_mul(OPUS_SAMPLES_PER_FRAME);

    let mut buf = [0u8; 28];
    buf[0] = 0x80;                                    // V=2, P=0, RC=0
    buf[1] = 200;                                     // Sender Report
    buf[2..4].copy_from_slice(&6u16.to_be_bytes());   // length = 6 (32-bit words - 1)
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
    fn packet_layout_matches_openscreen() {
        let s = CastRtpSender::new(cfg()).unwrap();
        let pkt = s.build_packet(b"payload", 5);
        assert_eq!(pkt.len(), 19 + 7);
        assert_eq!(pkt[0], 0x80);
        assert_eq!(pkt[1], 0xFF);
        assert_eq!(u16::from_be_bytes([pkt[2], pkt[3]]), 5);
        assert_eq!(u32::from_be_bytes([pkt[4], pkt[5], pkt[6], pkt[7]]), 5 * OPUS_SAMPLES_PER_FRAME);
        assert_eq!(u32::from_be_bytes([pkt[8], pkt[9], pkt[10], pkt[11]]), 0xDEAD_BEEF);
        assert_eq!(pkt[12], 0xC0);
        assert_eq!(pkt[13], 5);
        assert_eq!(u16::from_be_bytes([pkt[14], pkt[15]]), 0);
        assert_eq!(u16::from_be_bytes([pkt[16], pkt[17]]), 0);
        assert_eq!(pkt[18], 5);
        assert_eq!(&pkt[19..], b"payload");
    }

    #[test]
    fn failed_send_does_not_advance_frame_id() {
        let mut c = cfg();
        c.udp_port = 0; // EINVAL from the kernel; nothing leaves the host
        let mut s = CastRtpSender::new(c).unwrap();

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

    #[test]
    fn nonce_construction() {
        let s = CastRtpSender::new(cfg()).unwrap();
        let n = s.nonce(1);
        let mut expected = [0u8; 16];
        expected[11] = 1;
        for b in &mut expected {
            *b ^= 0x11;
        }
        assert_eq!(n, expected);
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
            assert_eq!(hex::encode(s.encrypt(payload, fid)), expected, "frame_id={fid}");
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
        let sr = build_rtcp_sr_at(0xDEAD_BEEF, 12345, 987_654, Duration::new(1_234_567_890, 500_000_000));
        assert_eq!(
            hex::encode(sr),
            "80c80006deadbeefcd40815280000000005a6ae000003039000f1206"
        );
    }

    #[test]
    fn rtcp_sr_size_and_header() {
        let sr = build_rtcp_sr(0xDEAD_BEEF, 100, 12345);
        assert_eq!(sr.len(), 28);
        assert_eq!(sr[0], 0x80);
        assert_eq!(sr[1], 200);
        assert_eq!(u16::from_be_bytes([sr[2], sr[3]]), 6);
        assert_eq!(u32::from_be_bytes([sr[4], sr[5], sr[6], sr[7]]), 0xDEAD_BEEF);
        assert_eq!(u32::from_be_bytes([sr[16], sr[17], sr[18], sr[19]]), 100 * OPUS_SAMPLES_PER_FRAME);
        assert_eq!(u32::from_be_bytes([sr[20], sr[21], sr[22], sr[23]]), 100);
    }
}
