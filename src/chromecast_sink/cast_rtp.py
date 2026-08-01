"""Cast RTP: AES-128-CTR frame encryption, packet construction, and UDP sending.

The Cast Streaming protocol uses a custom RTP profile ("cast") with:
- AES-128-CTR encryption per frame (NOT standard SRTP)
- Custom 6-byte extension header after the standard 12-byte RTP header
- Custom RTCP: we send Sender Reports (type 200); receiver feedback is not parsed

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


@dataclass
class CastRTPConfig:
    """Configuration for Cast RTP streaming."""

    chromecast_host: str
    udp_port: int
    ssrc: int
    payload_type: int
    aes_key: bytes
    aes_iv_mask: bytes


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
        # One frame is always one packet, so the frame ID doubles as the RTP
        # sequence number and packet count; the RTP timestamp is derived.
        self._frame_id = 0
        self._octet_count = 0
        self._stop = threading.Event()
        self._rtcp_thread: threading.Thread | None = None

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

    def _build_packet(self, encrypted_payload: bytes, frame_id: int) -> bytes:
        """Build a Cast RTP packet for one Opus frame.

        Each Opus frame is self-contained (keyframe) and always fits in a single
        packet (~161 B), so packet_id and max_packet_id are always 0 and the
        marker bit is always set — no fragmentation to handle.

        Layout:
        - [0-11]  Standard RTP header (12 bytes)
        - [12]    Flags: keyframe (0x80) | 6-bit extension count (0)
        - [13]    Frame ID (uint8)
        - [14-15] Packet ID (uint16) = 0
        - [16-17] Max Packet ID (uint16) = 0
        - [18+]   Encrypted payload
        """
        # Standard RTP header. Marker bit set: this packet ends the frame.
        v_p_x_cc = 0x80  # V=2, P=0, X=0, CC=0
        m_pt = 0x80 | (self.config.payload_type & 0x7F)

        rtp_header = struct.pack(
            ">BBHII",
            v_p_x_cc,
            m_pt,
            frame_id & 0xFFFF,
            (frame_id * OPUS_SAMPLES_PER_FRAME) & 0xFFFFFFFF,
            self.config.ssrc & 0xFFFFFFFF,
        )

        # Cast extension header (6 bytes): keyframe bit, no extensions.
        cast_ext = struct.pack(
            ">BBHH",
            0x80,            # keyframe | extension_count=0
            frame_id & 0xFF,
            0,               # packet_id
            0,               # max_packet_id
        )

        return rtp_header + cast_ext + encrypted_payload

    def send_frame(self, opus_frame: bytes) -> None:
        """Encrypt and send one Opus frame as a Cast RTP packet."""
        encrypted = self._encrypt_frame(opus_frame, self._frame_id)
        packet = self._build_packet(encrypted, self._frame_id)

        self._sock.sendto(packet, self._dest)

        self._frame_id += 1
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
            (self._frame_id * OPUS_SAMPLES_PER_FRAME) & 0xFFFFFFFF,
            self._frame_id & 0xFFFFFFFF,
            self._octet_count & 0xFFFFFFFF,
        )

    def start(self) -> None:
        """Start the RTCP sender thread."""

        def rtcp_send_loop():
            while not self._stop.is_set():
                try:
                    self._sock.sendto(self._build_rtcp_sr(), self._dest)
                except Exception as e:
                    logger.debug("RTCP send error: %s", e)
                self._stop.wait(0.5)

        self._rtcp_thread = threading.Thread(
            target=rtcp_send_loop, daemon=True, name="rtcp-sender"
        )
        self._rtcp_thread.start()

        logger.info("Cast RTP started (RTCP interval=500ms)")

    def stop(self) -> None:
        """Stop the RTP/RTCP sender and close the socket."""
        self._stop.set()
        if self._rtcp_thread:
            self._rtcp_thread.join(timeout=2)
        try:
            self._sock.close()
        except Exception:
            pass
        logger.info(
            "Cast RTP sender stopped (sent %d packets, %d bytes)",
            self._frame_id,
            self._octet_count,
        )
