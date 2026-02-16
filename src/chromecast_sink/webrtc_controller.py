"""Cast Streaming WebRTC signaling controller.

Handles the OFFER/ANSWER exchange on urn:x-cast:com.google.cast.webrtc
to negotiate audio streaming parameters with the Chromecast mirroring app.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import threading
import time
from dataclasses import dataclass, field

import pychromecast
from pychromecast.controllers import BaseController

logger = logging.getLogger(__name__)

WEBRTC_NAMESPACE = "urn:x-cast:com.google.cast.webrtc"
MIRRORING_APP_ID = "0F5096E8"


@dataclass
class StreamOffer:
    """Audio stream parameters for the Cast Streaming OFFER."""

    ssrc: int = field(default_factory=lambda: secrets.randbelow(2**31) + 1)
    aes_key: bytes = field(default_factory=lambda: os.urandom(16))
    aes_iv_mask: bytes = field(default_factory=lambda: os.urandom(16))
    codec: str = "opus"
    sample_rate: int = 48000
    channels: int = 2
    bit_rate: int = 128000
    rtp_payload_type: int = 127
    target_delay: int = 0


@dataclass
class StreamAnswer:
    """Parsed ANSWER from Chromecast."""

    udp_port: int
    receiver_ssrc: int
    send_indexes: list[int]


class WebRTCController(BaseController):
    """pychromecast controller for Cast Streaming signaling.

    Handles OFFER/ANSWER exchange on urn:x-cast:com.google.cast.webrtc.
    """

    def __init__(self) -> None:
        super().__init__(WEBRTC_NAMESPACE)
        self._answer_event = threading.Event()
        self._answer: StreamAnswer | None = None
        self._error: str | None = None
        self._seq_num = secrets.randbelow(2**30)

    def receive_message(self, _message, data: dict) -> bool:
        """Handle incoming messages on the WebRTC namespace."""
        msg_type = data.get("type")
        logger.debug("WebRTC recv: %s", json.dumps(data))

        if msg_type == "ANSWER":
            self._handle_answer(data)

        return True

    def _handle_answer(self, data: dict) -> None:
        result = data.get("result")
        if result != "ok":
            self._error = f"OFFER rejected: {data.get('error', data)}"
            logger.error(self._error)
            self._answer_event.set()
            return

        answer = data.get("answer", {})
        self._answer = StreamAnswer(
            udp_port=answer["udpPort"],
            receiver_ssrc=answer.get("ssrcs", [0])[0],
            send_indexes=answer.get("sendIndexes", []),
        )
        logger.info(
            "ANSWER received: udpPort=%d, sendIndexes=%s",
            self._answer.udp_port,
            self._answer.send_indexes,
        )

        # Log constraints if present (shows min/max delay the receiver supports)
        constraints = answer.get("constraints")
        if constraints:
            logger.info("Receiver constraints: %s", json.dumps(constraints))
            audio_c = constraints.get("audio")
            if audio_c:
                min_delay = audio_c.get("minDelay")
                max_delay = audio_c.get("maxDelay")
                if min_delay is not None or max_delay is not None:
                    logger.warning(
                        "Receiver delay range: min=%s ms, max=%s ms",
                        min_delay, max_delay,
                    )

        self._answer_event.set()

    def send_offer(self, offer: StreamOffer, timeout: float = 10) -> StreamAnswer:
        """Send OFFER and wait for ANSWER.

        Args:
            offer: Audio stream parameters.
            timeout: Seconds to wait for the ANSWER.

        Returns:
            Parsed StreamAnswer with UDP port and accepted stream indexes.

        Raises:
            RuntimeError: If the OFFER is rejected or times out.
        """
        self._seq_num += 1
        self._answer = None
        self._error = None
        self._answer_event.clear()

        msg = {
            "type": "OFFER",
            "seqNum": self._seq_num,
            "offer": {
                "castMode": "mirroring",
                "receiverGetStatus": True,
                "supportedStreams": [
                    {
                        "index": 0,
                        "type": "audio_source",
                        "codecName": offer.codec,
                        "rtpProfile": "cast",
                        "rtpPayloadType": offer.rtp_payload_type,
                        "ssrc": offer.ssrc,
                        "targetDelay": offer.target_delay,
                        "aesKey": offer.aes_key.hex(),
                        "aesIvMask": offer.aes_iv_mask.hex(),
                        "timeBase": f"1/{offer.sample_rate}",
                        "bitRate": offer.bit_rate,
                        "sampleRate": offer.sample_rate,
                        "channels": offer.channels,
                        "receiverRtcpEventLog": True,
                    }
                ],
            },
        }

        logger.info(
            "Sending OFFER (seqNum=%d, ssrc=%d, targetDelay=%dms)",
            self._seq_num,
            offer.ssrc,
            offer.target_delay,
        )
        logger.debug("OFFER payload: %s", json.dumps(msg, indent=2))

        self.send_message(msg)

        if not self._answer_event.wait(timeout=timeout):
            raise RuntimeError("Timeout waiting for ANSWER from Chromecast")

        if self._error:
            raise RuntimeError(self._error)

        assert self._answer is not None
        return self._answer


def launch_mirroring_app(
    cast: pychromecast.Chromecast,
    timeout: float = 10,
) -> None:
    """Launch the Cast Mirroring receiver app and wait for it to start.

    Args:
        cast: Connected Chromecast instance.
        timeout: Seconds to wait for the app to launch.

    Raises:
        RuntimeError: If the app fails to launch within the timeout.
    """
    # Already running?
    if cast.app_id == MIRRORING_APP_ID:
        logger.info("Mirroring app already running")
        return

    app_ready = threading.Event()

    class _Listener:
        def new_cast_status(self, status):
            if status.app_id == MIRRORING_APP_ID:
                app_ready.set()

    listener = _Listener()
    cast.register_status_listener(listener)

    logger.info("Launching mirroring app %s", MIRRORING_APP_ID)
    cast.start_app(MIRRORING_APP_ID)

    if not app_ready.wait(timeout=timeout):
        raise RuntimeError(
            f"Timeout launching mirroring app (current app: {cast.app_id})"
        )

    # Give the app a moment to initialize
    time.sleep(0.5)
    logger.info("Mirroring app ready")
