"""HTTP streaming server for serving audio to Chromecast."""

from __future__ import annotations

import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

logger = logging.getLogger(__name__)

# For WAV 32-bit (~345 KB/s), use larger chunks to reduce syscall overhead.
# 16384 bytes ≈ 47ms of audio — still low-latency but fewer write calls.
CHUNK_SIZE = 16384

# File extension used in the stream URL.
# Chromecast may use this to detect format, so we make it match.
_STREAM_EXTENSIONS = {
    "audio/wav": "wav",
    "audio/mpeg": "mp3",
    "audio/flac": "flac",
}


class _AudioStreamHandler(BaseHTTPRequestHandler):
    """Serves a continuous audio stream from FFmpeg stdout."""

    # Set by AudioStreamServer before starting
    ffmpeg_stdout = None
    content_type: str = "audio/wav"

    def do_GET(self) -> None:
        if self.path != "/stream":
            self.send_error(404)
            return

        self.send_response(200)
        self.send_header("Content-Type", self.content_type)
        self.send_header("Cache-Control", "no-cache, no-store")
        self.send_header("Connection", "close")
        self.send_header("icy-name", "Chromecast Sink")
        # Omit Content-Length for infinite live stream
        self.end_headers()

        logger.debug("Chromecast connected to stream")

        try:
            while True:
                chunk = self.ffmpeg_stdout.read(CHUNK_SIZE)
                if not chunk:
                    break  # FFmpeg process ended
                self.wfile.write(chunk)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass  # Chromecast disconnected — normal during shutdown

        logger.debug("Stream connection closed")

    def log_message(self, format: str, *args: object) -> None:
        """Suppress default HTTP request logging unless debug is enabled."""
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(format, *args)


class AudioStreamServer:
    """HTTP server that serves the FFmpeg audio stream on a given host/port."""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self._httpd: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self, ffmpeg_stdout, content_type: str = "audio/wav") -> str:
        """Start the HTTP server in a daemon thread.

        Args:
            ffmpeg_stdout: The stdout file object of the FFmpeg subprocess.
            content_type: MIME type for the audio stream.

        Returns:
            The stream URL (e.g. "http://192.168.1.100:8080/stream").
        """
        _AudioStreamHandler.ffmpeg_stdout = ffmpeg_stdout
        _AudioStreamHandler.content_type = content_type

        self._httpd = HTTPServer((self.host, self.port), _AudioStreamHandler)
        # Resolve actual port if 0 was used (auto-select)
        self.port = self._httpd.server_address[1]

        self._thread = threading.Thread(
            target=self._httpd.serve_forever,
            daemon=True,
            name="stream-server",
        )
        self._thread.start()

        url = f"http://{self.host}:{self.port}/stream"
        logger.info("Stream server started: %s (content_type=%s)", url, content_type)
        return url

    def stop(self) -> None:
        """Shut down the HTTP server."""
        if self._httpd is not None:
            logger.debug("Shutting down stream server")
            self._httpd.shutdown()
            self._httpd = None
