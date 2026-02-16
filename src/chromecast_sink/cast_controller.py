"""Chromecast connection and playback control via pychromecast."""

from __future__ import annotations

import logging
import threading
from typing import Callable

import pychromecast

logger = logging.getLogger(__name__)


class _ConnectionListener:
    """Monitors Chromecast connection state changes."""

    def __init__(self, on_disconnect: Callable[[], None]):
        self._on_disconnect = on_disconnect
        self.connected = threading.Event()

    def new_connection_status(self, status) -> None:
        status_str = status.status
        logger.debug("Connection status: %s", status_str)
        if status_str == "CONNECTED":
            self.connected.set()
        elif status_str in ("DISCONNECTED", "LOST"):
            self.connected.clear()
            logger.warning("Chromecast connection: %s", status_str)
            self._on_disconnect()


class _MediaListener:
    """Monitors media playback state on the Chromecast."""

    def __init__(self) -> None:
        self.player_state: str = "UNKNOWN"

    def new_media_status(self, status) -> None:
        self.player_state = status.player_state or "UNKNOWN"
        logger.debug("Media status: %s", self.player_state)


class CastController:
    """Manages Chromecast connection, playback, and status monitoring."""

    def __init__(
        self,
        cast: pychromecast.Chromecast,
        on_disconnect: Callable[[], None],
    ):
        self.cast = cast
        self._conn_listener = _ConnectionListener(on_disconnect)
        self._media_listener = _MediaListener()

    def connect_and_play(
        self,
        stream_url: str,
        content_type: str = "audio/wav",
        timeout: float = 30,
    ) -> None:
        """Connect to the Chromecast and start playing the audio stream.

        Args:
            stream_url: HTTP URL of the audio stream.
            content_type: MIME type of the audio stream.
            timeout: Seconds to wait for the media to become active.

        Raises:
            RuntimeError: If connection or playback initiation fails.
        """
        device_name = self.cast.cast_info.friendly_name
        logger.info("Connecting to %s...", device_name)

        self.cast.register_connection_listener(self._conn_listener)
        self.cast.wait()

        mc = self.cast.media_controller
        mc.register_status_listener(self._media_listener)

        logger.info("Starting playback: %s (%s)", stream_url, content_type)
        mc.play_media(stream_url, content_type, stream_type="LIVE")
        mc.block_until_active(timeout=timeout)

        logger.info("Playback active on %s", device_name)

    def stop(self) -> None:
        """Stop Chromecast playback and disconnect."""
        try:
            self.cast.media_controller.stop()
        except Exception as e:
            logger.debug("Error stopping media: %s", e)

        try:
            self.cast.quit_app()
        except Exception as e:
            logger.debug("Error quitting app: %s", e)

        try:
            self.cast.disconnect(timeout=5)
        except Exception as e:
            logger.debug("Error disconnecting: %s", e)

        logger.info("Cast controller stopped")
