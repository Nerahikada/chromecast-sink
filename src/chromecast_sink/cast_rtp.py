"""Cast RTP: AES-128-CTR frame encryption, packet construction, and UDP sending.

The Cast Streaming protocol uses a custom RTP profile ("cast") with:
- AES-128-CTR encryption per frame (NOT standard SRTP)
- Custom 6-7 byte extension header after the standard 12-byte RTP header
- Custom RTCP: Sender Reports (type 200), PSFB with "CAST"/"CST2" magic (type 206, FMT=15)

Reference: https://chromium.googlesource.com/openscreen/+/main/cast/streaming/
"""

from __future__ import annotations

import logging
import socket
import struct
import threading
import time
from dataclasses import dataclass

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

logger = logging.getLogger(__name__)

# Opus at 48kHz with 10ms frame_duration = 480 samples per frame
OPUS_SAMPLES_PER_FRAME = 480

# NTP epoch offset: seconds between 1900-01-01 and 1970-01-01
_NTP_EPOCH_OFFSET = 2208988800

# Cast RTP extension types (from openscreen rtp_defines.h)
_EXT_ADAPTIVE_LATENCY = 0x01


@dataclass
class CastRTPConfig:
    """Configuration for Cast RTP streaming."""

    chromecast_host: str
    udp_port: int
    ssrc: int
    payload_type: int
    aes_key: bytes
    aes_iv_mask: bytes
    target_playout_delay_ms: int = 0


class CastRTPSender:
    """Sends encrypted audio frames via Cast RTP to Chromecast.

    Each Opus frame is:
    1. Encrypted with AES-128-CTR (key from OFFER, nonce from ivMask XOR frame_id)
    2. Wrapped in a Cast RTP packet (standard header + Cast extension)
    3. Sent via UDP to the Chromecast's negotiated port
    """

    def __init__(self, config: CastRTPConfig):
        self.config = config
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._dest = (config.chromecast_host, config.udp_port)
        self._seq_num = 0
        self._frame_id = 0
        self._rtp_timestamp = 0
        self._packet_count = 0
        self._octet_count = 0
        self._running = False
        self._rtcp_thread: threading.Thread | None = None
        self._rtcp_recv_thread: threading.Thread | None = None

    def _encrypt_frame(self, data: bytes, frame_id: int) -> bytes:
        """Encrypt frame data using AES-128-CTR with Cast nonce construction.

        Nonce: 16 zero bytes with frame_id as big-endian uint32 at offset 8,
        then XOR'd with aesIvMask.
        """
        nonce = bytearray(16)
        struct.pack_into(">I", nonce, 8, frame_id & 0xFFFFFFFF)
        for i in range(16):
            nonce[i] ^= self.config.aes_iv_mask[i]

        cipher = Cipher(algorithms.AES(self.config.aes_key), modes.CTR(bytes(nonce)))
        enc = cipher.encryptor()
        return enc.update(data) + enc.finalize()

    def _build_packet(
        self,
        encrypted_payload: bytes,
        frame_id: int,
        is_keyframe: bool = True,
        packet_id: int = 0,
        max_packet_id: int = 0,
    ) -> bytes:
        """Build a Cast RTP packet.

        Layout:
        - [0-11]  Standard RTP header (12 bytes)
        - [12]    Flags: keyframe | ref_frame_present | 6-bit extension count
        - [13]    Frame ID (uint8)
        - [14-15] Packet ID (uint16)
        - [16-17] Max Packet ID (uint16)
        - [18+]   Encrypted payload
        """
        # Standard RTP header
        v_p_x_cc = 0x80  # V=2, P=0, X=0, CC=0
        marker = 1 if packet_id == max_packet_id else 0
        m_pt = (marker << 7) | (self.config.payload_type & 0x7F)

        rtp_header = struct.pack(
            ">BBHII",
            v_p_x_cc,
            m_pt,
            self._seq_num & 0xFFFF,
            self._rtp_timestamp & 0xFFFFFFFF,
            self.config.ssrc & 0xFFFFFFFF,
        )

        # Cast extension header (6 bytes, no extensions)
        cast_byte0 = 0x00  # extension_count = 0
        if is_keyframe:
            cast_byte0 |= 0x80  # Key-frame bit

        cast_ext = struct.pack(
            ">BBHH",
            cast_byte0,
            frame_id & 0xFF,
            packet_id,
            max_packet_id,
        )

        return rtp_header + cast_ext + encrypted_payload

    def send_frame(self, opus_frame: bytes) -> None:
        """Encrypt and send one Opus frame as a Cast RTP packet."""
        encrypted = self._encrypt_frame(opus_frame, self._frame_id)
        packet = self._build_packet(encrypted, self._frame_id)

        self._sock.sendto(packet, self._dest)

        self._seq_num += 1
        self._frame_id += 1
        self._rtp_timestamp += OPUS_SAMPLES_PER_FRAME
        self._packet_count += 1
        self._octet_count += len(opus_frame)

    def _build_rtcp_sr(self) -> bytes:
        """Build an RTCP Sender Report packet (RFC 3550)."""
        now = time.time()
        ntp_sec = int(now) + _NTP_EPOCH_OFFSET
        ntp_frac = int((now % 1) * (2**32))

        return struct.pack(
            ">BBHIIIIII",
            0x80,  # V=2, P=0, RC=0
            200,   # PT = Sender Report
            6,     # Length in 32-bit words minus 1
            self.config.ssrc & 0xFFFFFFFF,
            ntp_sec & 0xFFFFFFFF,
            ntp_frac & 0xFFFFFFFF,
            self._rtp_timestamp & 0xFFFFFFFF,
            self._packet_count & 0xFFFFFFFF,
            self._octet_count & 0xFFFFFFFF,
        )

    def _rtcp_recv_loop(self) -> None:
        """Receive RTCP packets from Chromecast and extract diagnostics."""
        self._sock.settimeout(1.0)
        last_log_time = 0.0
        rtcp_count = 0

        while self._running:
            try:
                data, addr = self._sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break

            rtcp_count += 1
            now = time.monotonic()

            # Parse compound RTCP: extract Cast feedback playout delay
            self._parse_rtcp_compound(data)

            # Rate-limit logging: summary every 5 seconds
            if now - last_log_time >= 5.0:
                last_log_time = now
                logger.debug(
                    "RTCP recv: %d packets total from %s", rtcp_count, addr
                )

    def _parse_rtcp_compound(self, data: bytes) -> None:
        """Parse compound RTCP packet and log Cast-specific feedback."""
        offset = 0
        while offset + 4 <= len(data):
            byte0 = data[offset]
            pt = data[offset + 1]
            length = struct.unpack(">H", data[offset + 2 : offset + 4])[0]
            pkt_len = (length + 1) * 4

            if pt == 206 and (byte0 & 0x1F) == 15:
                # Cast-specific PSFB (Payload-Specific Feedback, FMT=15):
                # Contains "CAST" (0x43415354) or "CST2" magic, frame ACK + feedback.
                # After 12-byte header, the FCI contains Cast feedback data.
                if offset + pkt_len <= len(data) and pkt_len > 12:
                    fci = data[offset + 12 : offset + pkt_len]
                    if len(fci) >= 4:
                        logger.debug(
                            "Cast PSFB FCI (%d bytes): %s",
                            len(fci), fci[:16].hex(),
                        )

            offset += pkt_len

    def start(self) -> None:
        """Start the RTCP sender and receiver threads."""
        self._running = True

        def rtcp_send_loop():
            while self._running:
                try:
                    sr = self._build_rtcp_sr()
                    self._sock.sendto(sr, self._dest)
                except Exception as e:
                    logger.debug("RTCP send error: %s", e)
                for _ in range(10):
                    if not self._running:
                        break
                    time.sleep(0.05)

        self._rtcp_thread = threading.Thread(
            target=rtcp_send_loop, daemon=True, name="rtcp-sender"
        )
        self._rtcp_thread.start()

        self._rtcp_recv_thread = threading.Thread(
            target=self._rtcp_recv_loop, daemon=True, name="rtcp-receiver"
        )
        self._rtcp_recv_thread.start()

        logger.info(
            "Cast RTP started (RTCP interval=500ms, playout_delay=%dms)",
            self.config.target_playout_delay_ms,
        )

    def stop(self) -> None:
        """Stop the RTP/RTCP sender and close the socket."""
        self._running = False
        if self._rtcp_thread:
            self._rtcp_thread.join(timeout=2)
        if self._rtcp_recv_thread:
            self._rtcp_recv_thread.join(timeout=2)
        try:
            self._sock.close()
        except Exception:
            pass
        logger.info(
            "Cast RTP sender stopped (sent %d packets, %d bytes)",
            self._packet_count,
            self._octet_count,
        )


def parse_rtp_payload(data: bytes) -> bytes | None:
    """Extract the payload from a standard RTP packet.

    Handles CSRC, extensions, and padding.
    Returns the payload bytes, or None if the packet is malformed.
    """
    if len(data) < 12:
        return None

    cc = data[0] & 0x0F
    has_extension = bool(data[0] & 0x10)
    has_padding = bool(data[0] & 0x20)

    offset = 12 + cc * 4

    if has_extension:
        if len(data) < offset + 4:
            return None
        ext_len = struct.unpack(">H", data[offset + 2 : offset + 4])[0]
        offset += 4 + ext_len * 4

    end = len(data)
    if has_padding:
        pad_len = data[-1]
        end -= pad_len

    if offset >= end:
        return None

    return data[offset:end]


def run_rtp_relay(
    recv_sock: socket.socket,
    cast_sender: CastRTPSender,
    stop_event: threading.Event,
) -> None:
    """Read RTP packets from FFmpeg and relay as Cast RTP to Chromecast.

    Runs in a loop until stop_event is set. Each received RTP packet
    is parsed to extract the Opus payload, which is then encrypted and
    sent as a Cast RTP packet.

    Args:
        recv_sock: UDP socket bound to the port FFmpeg sends RTP to.
        cast_sender: Configured CastRTPSender instance.
        stop_event: Set this to stop the relay loop.
    """
    recv_sock.settimeout(0.5)

    relay_start = time.monotonic()
    first_frame_time: float | None = None
    frame_count = 0
    last_stats_time = relay_start

    logger.info("RTP relay started (waiting for first frame from FFmpeg...)")

    while not stop_event.is_set():
        try:
            data, _ = recv_sock.recvfrom(4096)
        except socket.timeout:
            continue
        except OSError:
            break

        payload = parse_rtp_payload(data)
        if payload:
            now = time.monotonic()

            if first_frame_time is None:
                first_frame_time = now
                logger.info(
                    "First audio frame received from FFmpeg "
                    "(%.0fms after relay start, %d bytes payload)",
                    (now - relay_start) * 1000,
                    len(payload),
                )

            cast_sender.send_frame(payload)
            frame_count += 1

            # Log stats every 5 seconds
            if now - last_stats_time >= 5.0:
                elapsed = now - first_frame_time
                fps = frame_count / elapsed if elapsed > 0 else 0
                logger.info(
                    "Relay stats: %d frames in %.1fs (%.1f fps, expected ~100)",
                    frame_count, elapsed, fps,
                )
                last_stats_time = now

    logger.info("RTP relay stopped (sent %d frames total)", frame_count)
