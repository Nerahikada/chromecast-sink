"""Main lifecycle orchestrator — wires all components together."""

from __future__ import annotations

import atexit
import json
import logging
import signal
import socket
import subprocess
import sys
import threading
from dataclasses import dataclass

from chromecast_sink.audio_capture import (
    FORMATS,
    start_ffmpeg,
    start_ffmpeg_opus_rtp,
    stop_ffmpeg,
    verify_ffmpeg_running,
)
from chromecast_sink.cast_controller import CastController
from chromecast_sink.cast_rtp import CastRTPConfig, CastRTPSender, run_rtp_relay
from chromecast_sink.discovery import DiscoveryResult, discover_devices
from chromecast_sink.network import get_local_ip
from chromecast_sink.stream_server import AudioStreamServer
from chromecast_sink.virtual_sink import (
    SinkInfo,
    create_virtual_sink,
    destroy_virtual_sink,
)
from chromecast_sink.webrtc_controller import (
    StreamOffer,
    WebRTCController,
    launch_mirroring_app,
)

logger = logging.getLogger(__name__)


@dataclass
class Config:
    """Runtime configuration from CLI arguments."""

    mode: str = "stream"  # "stream" (Cast Streaming/UDP) or "http" (legacy HTTP)
    audio_format: str = "wav"
    bitrate: str = "320k"
    sample_rate: int = 44100
    port: int = 0
    device_name: str | None = None
    timeout: float = 10
    target_delay: int = 0
    opus_bitrate: int = 128000


class Orchestrator:
    """Manages the full lifecycle of the Chromecast audio bridge."""

    def __init__(self, config: Config):
        self.config = config
        self._sink_info: SinkInfo | None = None
        self._ffmpeg_proc = None
        self._stream_server: AudioStreamServer | None = None
        self._cast_controller: CastController | None = None
        self._cast_rtp_sender: CastRTPSender | None = None
        self._rtp_recv_sock: socket.socket | None = None
        self._rtp_relay_thread: threading.Thread | None = None
        self._discovery_result: DiscoveryResult | None = None
        self._shutdown_event = threading.Event()
        self._cleanup_done = False

    def run(self) -> int:
        """Execute the full pipeline. Returns exit code."""
        atexit.register(self._cleanup)
        self._setup_signals()

        try:
            return self._run_inner()
        except KeyboardInterrupt:
            print("\nShutting down...")
            return 0
        except Exception as e:
            logger.error("Fatal error: %s", e, exc_info=True)
            print(f"Error: {e}", file=sys.stderr)
            return 1
        finally:
            self._cleanup()

    def _run_inner(self) -> int:
        # Phase 1: Discover devices
        print("Discovering Chromecast devices...")
        self._discovery_result = discover_devices(
            timeout=self.config.timeout,
            device_name=self.config.device_name,
        )

        chromecasts = self._discovery_result.chromecasts
        if not chromecasts:
            print(
                "No Chromecast devices found.\n"
                "Check that your device is:\n"
                "  - Powered on and connected to the same network\n"
                "  - Not in guest mode\n"
                "  - Multicast UDP port 5353 is not blocked by firewall",
                file=sys.stderr,
            )
            return 1

        # Phase 2: Select device
        if len(chromecasts) == 1:
            cast = chromecasts[0]
        else:
            cast = self._interactive_select(chromecasts)

        device_name = cast.cast_info.friendly_name
        cast_host = str(cast.cast_info.host)
        print(f"Selected: {device_name}")

        # Phase 3: Determine local IP
        local_ip = get_local_ip(cast_host)
        logger.info("Local IP: %s, Chromecast: %s", local_ip, cast_host)

        # Phase 4: Create virtual sink
        print(f'Creating virtual sink "Chromecast - {device_name}"...')
        self._sink_info = create_virtual_sink(device_name)

        # Log PipeWire quantum for diagnostics
        self._log_pipewire_quantum()

        # Branch based on mode
        if self.config.mode == "stream":
            return self._run_streaming(cast, cast_host)
        else:
            return self._run_http(cast, local_ip, device_name)

    def _run_streaming(self, cast, cast_host: str) -> int:
        """Cast Streaming pipeline (WebRTC/UDP)."""
        assert self._sink_info is not None

        # Phase 5: Connect to Chromecast and set up signaling
        print("Connecting to Chromecast...")
        cast.wait()

        webrtc = WebRTCController()
        cast.register_handler(webrtc)

        # Phase 6: Launch mirroring app
        print("Launching mirroring receiver...")
        launch_mirroring_app(cast, timeout=self.config.timeout)

        # Phase 7: Send OFFER and wait for ANSWER
        offer = StreamOffer(
            bit_rate=self.config.opus_bitrate,
            target_delay=self.config.target_delay,
        )
        print(
            f"Negotiating stream (Opus {offer.bit_rate // 1000}kbps, "
            f"target delay {offer.target_delay}ms)..."
        )
        answer = webrtc.send_offer(offer, timeout=self.config.timeout)

        if 0 not in answer.send_indexes:
            raise RuntimeError(
                "Chromecast did not accept the audio stream. "
                "This device may require video as well."
            )

        logger.info(
            "Stream negotiated: UDP port %d, receiver SSRC %d",
            answer.udp_port,
            answer.receiver_ssrc,
        )

        # Phase 8: Create local UDP socket for FFmpeg RTP output
        self._rtp_recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._rtp_recv_sock.bind(("127.0.0.1", 0))
        local_rtp_port = self._rtp_recv_sock.getsockname()[1]
        logger.info("FFmpeg RTP -> localhost:%d", local_rtp_port)

        # Phase 9: Start FFmpeg (Opus/RTP to localhost)
        print("Starting audio capture (Opus/RTP)...")
        self._ffmpeg_proc = start_ffmpeg_opus_rtp(
            self._sink_info.monitor_source,
            local_rtp_port,
            bit_rate=self.config.opus_bitrate,
        )
        verify_ffmpeg_running(self._ffmpeg_proc)

        # Phase 10: Create and start Cast RTP sender
        rtp_config = CastRTPConfig(
            chromecast_host=cast_host,
            udp_port=answer.udp_port,
            ssrc=offer.ssrc,
            payload_type=offer.rtp_payload_type,
            aes_key=offer.aes_key,
            aes_iv_mask=offer.aes_iv_mask,
            target_playout_delay_ms=self.config.target_delay,
        )
        self._cast_rtp_sender = CastRTPSender(rtp_config)
        self._cast_rtp_sender.start()

        # Phase 11: Start RTP relay (FFmpeg -> Cast RTP -> Chromecast)
        self._rtp_relay_thread = threading.Thread(
            target=run_rtp_relay,
            args=(self._rtp_recv_sock, self._cast_rtp_sender, self._shutdown_event),
            daemon=True,
            name="rtp-relay",
        )
        self._rtp_relay_thread.start()

        # Success
        print(
            f'\nStreaming to "{cast.cast_info.friendly_name}" '
            f"via Cast Streaming (UDP).\n"
            f'Select "Chromecast - {cast.cast_info.friendly_name}" '
            f"as your audio output to start casting.\n"
            f"Press Ctrl+C to stop."
        )

        # Phase 12: Wait for shutdown signal
        self._shutdown_event.wait()
        return 0

    def _run_http(self, cast, local_ip: str, device_name: str) -> int:
        """Legacy HTTP streaming pipeline."""
        assert self._sink_info is not None

        # Phase 5: Start FFmpeg
        fmt = FORMATS[self.config.audio_format]
        print(f"Audio format: {self.config.audio_format} ({fmt.content_type})")
        self._ffmpeg_proc = start_ffmpeg(
            self._sink_info.monitor_source,
            audio_format=self.config.audio_format,
            bitrate=self.config.bitrate,
            sample_rate=self.config.sample_rate,
        )
        verify_ffmpeg_running(self._ffmpeg_proc)

        # Phase 6: Start HTTP server
        self._stream_server = AudioStreamServer(local_ip, self.config.port)
        stream_url = self._stream_server.start(
            self._ffmpeg_proc.stdout,
            content_type=fmt.content_type,
        )
        logger.info("Stream URL: %s", stream_url)

        # Phase 7: Connect to Chromecast and start playback
        print("Connecting to Chromecast...")
        self._cast_controller = CastController(
            cast,
            on_disconnect=self._handle_disconnect,
        )
        self._cast_controller.connect_and_play(stream_url, fmt.content_type)

        # Success
        print(
            f'\nConnected! Ready to cast to "{device_name}".\n'
            f'Select "Chromecast - {device_name}" as your audio output\n'
            f"in Settings > Sound to start casting.\n"
            f"Press Ctrl+C to stop."
        )

        # Phase 8: Wait for shutdown signal
        self._shutdown_event.wait()
        return 0

    def _interactive_select(self, chromecasts) -> object:
        """Present an interactive device selection menu."""
        print(f"\nFound {len(chromecasts)} device(s):")
        for i, cc in enumerate(chromecasts, 1):
            info = cc.cast_info
            model = f" ({info.model_name})" if info.model_name else ""
            print(f"  {i}. {info.friendly_name}{model}")

        while True:
            try:
                choice = input(f"\nSelect device [1-{len(chromecasts)}]: ").strip()
                idx = int(choice) - 1
                if 0 <= idx < len(chromecasts):
                    return chromecasts[idx]
            except (ValueError, EOFError):
                pass
            print(f"Please enter a number between 1 and {len(chromecasts)}.")

    def _log_pipewire_quantum(self) -> None:
        """Log the PipeWire quantum setting for the virtual sink."""
        if not self._sink_info:
            return
        try:
            result = subprocess.run(
                ["pw-dump"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                for obj in json.loads(result.stdout):
                    if obj.get("type") != "PipeWire:Interface:Node":
                        continue
                    props = obj.get("info", {}).get("props", {})
                    if props.get("node.name") == self._sink_info.sink_name:
                        quantum = props.get("node.force-quantum", "not set")
                        latency = props.get("node.latency", "not set")
                        logger.info(
                            "PipeWire node %s: force-quantum=%s, latency=%s",
                            self._sink_info.sink_name, quantum, latency,
                        )
                        # Also check params for actual quantum
                        params = obj.get("info", {}).get("params", {})
                        if "Format" in params:
                            for fmt in params["Format"]:
                                if "size" in str(fmt):
                                    logger.info("PipeWire Format params: %s", fmt)
                        return
                logger.warning("PipeWire node %s not found in pw-dump", self._sink_info.sink_name)
        except Exception as e:
            logger.debug("Could not query PipeWire quantum: %s", e)

    def _handle_disconnect(self) -> None:
        """Called by CastConnectionListener on disconnection."""
        print("\nChromecast disconnected.")
        self._shutdown_event.set()

    def _setup_signals(self) -> None:
        """Register signal handlers for graceful shutdown."""
        def handler(signum, frame):
            self._shutdown_event.set()

        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)

    def _cleanup(self) -> None:
        """Tear down all resources in reverse creation order."""
        if self._cleanup_done:
            return
        self._cleanup_done = True

        logger.debug("Running cleanup...")

        # 1. Stop Cast RTP sender
        if self._cast_rtp_sender:
            try:
                self._cast_rtp_sender.stop()
            except Exception as e:
                logger.debug("Cast RTP cleanup error: %s", e)

        # 2. Close RTP receive socket (stops the relay thread)
        if self._rtp_recv_sock:
            try:
                self._rtp_recv_sock.close()
            except Exception as e:
                logger.debug("RTP socket cleanup error: %s", e)

        # 3. Stop Chromecast (HTTP mode)
        if self._cast_controller:
            try:
                self._cast_controller.stop()
            except Exception as e:
                logger.debug("Cast cleanup error: %s", e)

        # 4. Stop HTTP server (HTTP mode)
        if self._stream_server:
            try:
                self._stream_server.stop()
            except Exception as e:
                logger.debug("Server cleanup error: %s", e)

        # 5. Stop FFmpeg
        if self._ffmpeg_proc:
            try:
                stop_ffmpeg(self._ffmpeg_proc)
            except Exception as e:
                logger.debug("FFmpeg cleanup error: %s", e)

        # 6. Remove virtual sink
        if self._sink_info:
            try:
                destroy_virtual_sink(self._sink_info)
            except Exception as e:
                logger.debug("Sink cleanup error: %s", e)

        # 7. Stop discovery
        if self._discovery_result:
            try:
                self._discovery_result.stop()
            except Exception as e:
                logger.debug("Discovery cleanup error: %s", e)

        # 8. Quit mirroring app (best-effort, after discovery cleanup)
        # Note: cast.quit_app() may already be handled by pychromecast disconnect

        logger.debug("Cleanup complete")
