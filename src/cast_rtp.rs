//! Cast RTP: AES-128-CTR frame encryption, packet construction, UDP sending.
//!
//! Cast Streaming uses a custom "cast" RTP profile (NOT SRTP):
//! - AES-128-CTR per frame, nonce = (16 zero bytes with frame_id BE u32 at
//!   offset 8) XOR aesIvMask
//! - 12-byte standard RTP header + 6-byte Cast extension header
//! - We send RTCP Sender Reports (type 200) every 500 ms; receiver feedback
//!   is not parsed. SRs are *required*: without them the receiver kills the
//!   mirroring app within seconds (verified on Nest Mini).
//!
//! Reference: <https://chromium.googlesource.com/openscreen/+/main/cast/streaming/>

use std::net::UdpSocket;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aes::Aes128;
use aes::cipher::{KeyIvInit, StreamCipher};
use ctr::Ctr128BE;

/// Opus at 48 kHz with 10 ms frames = 480 samples/frame.
pub const OPUS_SAMPLES_PER_FRAME: u32 = 480;

/// Seconds between 1900-01-01 (NTP epoch) and 1970-01-01 (UNIX epoch).
const NTP_EPOCH_OFFSET: u64 = 2_208_988_800;

type Aes128Ctr = Ctr128BE<Aes128>;

pub struct Config {
    pub chromecast_host: String,
    pub udp_port: u16,
    pub ssrc: u32,
    pub payload_type: u8,
    pub aes_key: [u8; 16],
    pub aes_iv_mask: [u8; 16],
}

/// Sends encrypted audio frames via Cast RTP.
///
/// One Opus frame is always one packet (~161 B), so `frame_id` doubles as the
/// RTP sequence number and the RTCP packet count, and the RTP timestamp is
/// `frame_id * OPUS_SAMPLES_PER_FRAME`. No fragmentation.
pub struct CastRtpSender {
    config: Config,
    socket: Arc<UdpSocket>,
    dest: (String, u16),
    frame_id: u32,
    octet_count: u64,
    /// (bool, Condvar) → RTCP thread waits with wait_timeout on the condvar;
    /// stop() flips the bool and notifies. Mirrors the Python `threading.Event`.
    stop: Arc<(Mutex<bool>, Condvar)>,
    // Shared with the RTCP thread; updated on every send.
    shared_frame_id: Arc<AtomicU32>,
    shared_octets: Arc<AtomicU64>,
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
            frame_id: 0,
            octet_count: 0,
            stop: Arc::new((Mutex::new(false), Condvar::new())),
            shared_frame_id: Arc::new(AtomicU32::new(0)),
            shared_octets: Arc::new(AtomicU64::new(0)),
            rtcp_thread: None,
        })
    }

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

    /// Build one Cast RTP packet (marker bit set, keyframe flag set, no extensions,
    /// packet_id/max_packet_id = 0).
    fn build_packet(&self, encrypted: &[u8], frame_id: u32) -> Vec<u8> {
        let mut pkt = Vec::with_capacity(18 + encrypted.len());
        let v_p_x_cc: u8 = 0x80;
        let m_pt: u8 = 0x80 | (self.config.payload_type & 0x7F);
        let seq = (frame_id & 0xFFFF) as u16;
        let ts = frame_id.wrapping_mul(OPUS_SAMPLES_PER_FRAME);

        pkt.push(v_p_x_cc);
        pkt.push(m_pt);
        pkt.extend_from_slice(&seq.to_be_bytes());
        pkt.extend_from_slice(&ts.to_be_bytes());
        pkt.extend_from_slice(&self.config.ssrc.to_be_bytes());

        // Cast extension: keyframe|ext_count=0, frame_id u8, packet_id=0, max_packet_id=0
        pkt.push(0x80);
        pkt.push((frame_id & 0xFF) as u8);
        pkt.extend_from_slice(&0u16.to_be_bytes());
        pkt.extend_from_slice(&0u16.to_be_bytes());

        pkt.extend_from_slice(encrypted);
        pkt
    }

    pub fn send_frame(&mut self, opus_frame: &[u8]) -> std::io::Result<()> {
        let enc = self.encrypt(opus_frame, self.frame_id);
        let pkt = self.build_packet(&enc, self.frame_id);
        self.socket.send_to(&pkt, &self.dest)?;

        self.frame_id = self.frame_id.wrapping_add(1);
        self.octet_count = self.octet_count.wrapping_add(opus_frame.len() as u64);
        self.shared_frame_id.store(self.frame_id, Ordering::Relaxed);
        self.shared_octets.store(self.octet_count, Ordering::Relaxed);
        Ok(())
    }

    /// Snapshot of frames-sent and total payload bytes (useful for diagnostics).
    #[cfg(test)]
    pub fn stats(&self) -> (u32, u64) {
        (self.frame_id, self.octet_count)
    }

    /// Handle for reading live counters from another thread. The counters
    /// are updated on every `send_frame` and by the RTCP thread.
    pub fn stats_handle(&self) -> StatsHandle {
        StatsHandle {
            frame_id: Arc::clone(&self.shared_frame_id),
            octet_count: Arc::clone(&self.shared_octets),
        }
    }

    /// Spawn the 500 ms RTCP Sender Report thread. Required for the receiver
    /// to keep the mirroring app alive.
    pub fn start(&mut self) {
        let stop = Arc::clone(&self.stop);
        let socket = Arc::clone(&self.socket);
        let dest = self.dest.clone();
        let ssrc = self.config.ssrc;
        let shared_frame_id = Arc::clone(&self.shared_frame_id);
        let shared_octets = Arc::clone(&self.shared_octets);

        self.rtcp_thread = Some(std::thread::Builder::new()
            .name("rtcp-sender".into())
            .spawn(move || {
                let (lock, cvar) = &*stop;
                loop {
                    let fid = shared_frame_id.load(Ordering::Relaxed);
                    let octets = shared_octets.load(Ordering::Relaxed);
                    let sr = build_rtcp_sr(ssrc, fid, octets);
                    if let Err(e) = socket.send_to(&sr, &dest) {
                        log::debug!("RTCP send error: {e}");
                    }
                    let guard = lock.lock().expect("stop mutex");
                    if *guard {
                        break;
                    }
                    let (guard, _) = cvar
                        .wait_timeout(guard, Duration::from_millis(500))
                        .expect("cvar wait");
                    if *guard {
                        break;
                    }
                }
            })
            .expect("spawn rtcp thread"));
        log::info!("Cast RTP started (RTCP interval=500ms)");
    }

    pub fn stop(&mut self) {
        {
            let (lock, cvar) = &*self.stop;
            let mut stopped = lock.lock().expect("stop mutex");
            *stopped = true;
            cvar.notify_all();
        }
        if let Some(t) = self.rtcp_thread.take() {
            let _ = t.join();
        }
        log::info!(
            "Cast RTP sender stopped (sent {} packets, {} bytes)",
            self.frame_id, self.octet_count
        );
    }

    fn is_stopped(&self) -> bool {
        *self.stop.0.lock().expect("stop mutex")
    }
}

impl Drop for CastRtpSender {
    fn drop(&mut self) {
        if !self.is_stopped() {
            self.stop();
        }
    }
}

/// Cheap cross-thread view of the sender's counters.
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

fn build_rtcp_sr(ssrc: u32, frame_id: u32, octet_count: u64) -> [u8; 28] {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let ntp_sec = (now.as_secs() + NTP_EPOCH_OFFSET) as u32;
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
    fn packet_layout_matches_python() {
        let s = CastRtpSender::new(cfg()).unwrap();
        let pkt = s.build_packet(b"payload", 5);
        assert_eq!(pkt.len(), 18 + 7);
        assert_eq!(pkt[0], 0x80);
        assert_eq!(pkt[1], 0xFF); // marker + payload_type 127
        assert_eq!(u16::from_be_bytes([pkt[2], pkt[3]]), 5);
        assert_eq!(
            u32::from_be_bytes([pkt[4], pkt[5], pkt[6], pkt[7]]),
            5 * OPUS_SAMPLES_PER_FRAME
        );
        assert_eq!(u32::from_be_bytes([pkt[8], pkt[9], pkt[10], pkt[11]]), 0xDEAD_BEEF);
        assert_eq!(pkt[12], 0x80);
        assert_eq!(pkt[13], 5);
        assert_eq!(u16::from_be_bytes([pkt[14], pkt[15]]), 0);
        assert_eq!(u16::from_be_bytes([pkt[16], pkt[17]]), 0);
        assert_eq!(&pkt[18..], b"payload");
    }

    #[test]
    fn nonce_construction() {
        let s = CastRtpSender::new(cfg()).unwrap();
        // frame_id=1: 16 zero bytes with 0x00000001 at offset 8, XOR mask 0x11...
        let n = s.nonce(1);
        let mut expected = [0u8; 16];
        expected[11] = 1;
        for b in &mut expected {
            *b ^= 0x11;
        }
        assert_eq!(n, expected);
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
