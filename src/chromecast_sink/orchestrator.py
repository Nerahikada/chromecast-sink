"""Main lifecycle orchestrator — wires all components together."""

from __future__ import annotations

import logging
import signal
import sys
import threading

from chromecast_sink.audio_capture import run_capture
from chromecast_sink.cast_rtp import CastRTPConfig, CastRTPSender
from chromecast_sink.discovery import DiscoveryResult, discover_devices
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

# Fixed and empirically chosen; deliberately not CLI surface. The Opus bitrate
TIMEOUT = 10.0


class Orchestrator:
    """Manages the full lifecycle of the Chromecast audio bridge."""

    def __init__(self, device_name: str | None = None):
        self.device_name = device_name
        self._sink_info: SinkInfo | None = None
        self._cast_rtp_sender: CastRTPSender | None = None
        self._capture_thread: threading.Thread | None = None
        self._discovery_result: DiscoveryResult | None = None
        self._shutdown_event = threading.Event()

    def run(self) -> int:
        """Execute the full pipeline. Returns exit code."""
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
            timeout=TIMEOUT,
            device_name=self.device_name,
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
        if len(chromecasts) > 1:
            print("Multiple devices found:", file=sys.stderr)
            for cc in chromecasts:
                info = cc.cast_info
                model = f" ({info.model_name})" if info.model_name else ""
                print(f"  - {info.friendly_name}{model}", file=sys.stderr)
            print('Specify one with --device "NAME".', file=sys.stderr)
            return 1
        cast = chromecasts[0]

        device_name = cast.cast_info.friendly_name
        cast_host = str(cast.cast_info.host)
        print(f"Selected: {device_name}")
        logger.info("Chromecast: %s", cast_host)

        # Phase 3: Create virtual sink
        print(f'Creating virtual sink "Chromecast - {device_name}"...')
        self._sink_info = create_virtual_sink(device_name)

        # Phase 4: Connect to Chromecast and set up signaling
        print("Connecting to Chromecast...")
        cast.wait()

        webrtc = WebRTCController()
        cast.register_handler(webrtc)

        # Phase 5: Launch mirroring app
        print("Launching mirroring receiver...")
        launch_mirroring_app(cast, timeout=TIMEOUT)

        # Phase 6: Send OFFER and wait for ANSWER
        offer = StreamOffer()
        print(
            f"Negotiating stream (Opus {offer.bit_rate // 1000}kbps, "
            f"target delay {offer.target_delay}ms)..."
        )
        answer = webrtc.send_offer(offer, timeout=TIMEOUT)

        if 0 not in answer.send_indexes:
            raise RuntimeError(
                "Chromecast did not accept the audio stream. "
                "This device may require video as well."
            )

        logger.info("Stream negotiated: UDP port %d", answer.udp_port)

        # Phase 7: Create and start Cast RTP sender
        rtp_config = CastRTPConfig(
            chromecast_host=cast_host,
            udp_port=answer.udp_port,
            ssrc=offer.ssrc,
            payload_type=offer.rtp_payload_type,
            aes_key=offer.aes_key,
            aes_iv_mask=offer.aes_iv_mask,
        )
        self._cast_rtp_sender = CastRTPSender(rtp_config)
        self._cast_rtp_sender.start()

        # Phase 8: Start capture (monitor -> Opus -> Cast RTP -> Chromecast)
        print("Starting audio capture...")
        self._capture_thread = threading.Thread(
            target=self._run_capture,
            args=(offer.bit_rate,),
            daemon=True,
            name="capture",
        )
        self._capture_thread.start()

        # Success
        print(
            f'\nStreaming to "{device_name}" via Cast Streaming (UDP).\n'
            f'Select "Chromecast - {device_name}" '
            f"as your audio output to start casting.\n"
            f"Press Ctrl+C to stop."
        )

        # Phase 9: Wait for shutdown signal
        self._shutdown_event.wait()
        return 0

    def _run_capture(self, bit_rate: int) -> None:
        """Capture loop body; a failure here must bring the process down."""
        assert self._sink_info is not None
        assert self._cast_rtp_sender is not None
        try:
            run_capture(
                self._sink_info.monitor_source,
                self._cast_rtp_sender,
                self._shutdown_event,
                bit_rate=bit_rate,
            )
        except Exception as e:
            logger.error("Capture failed: %s", e, exc_info=True)
            print(f"\nCapture failed: {e}", file=sys.stderr)
            self._shutdown_event.set()

    def _setup_signals(self) -> None:
        """Register signal handlers for graceful shutdown."""
        def handler(signum, frame):
            self._shutdown_event.set()

        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)

    def _cleanup(self) -> None:
        """Tear down all resources in reverse creation order."""
        logger.debug("Running cleanup...")

        # 1. Stop the capture loop and wait for it to release the source
        self._shutdown_event.set()
        if self._capture_thread:
            self._capture_thread.join(timeout=3)

        # 2. Stop Cast RTP sender
        if self._cast_rtp_sender:
            try:
                self._cast_rtp_sender.stop()
            except Exception as e:
                logger.debug("Cast RTP cleanup error: %s", e)

        # 3. Remove virtual sink
        if self._sink_info:
            try:
                destroy_virtual_sink(self._sink_info)
            except Exception as e:
                logger.debug("Sink cleanup error: %s", e)

        # 4. Stop discovery
        if self._discovery_result:
            try:
                self._discovery_result.stop()
            except Exception as e:
                logger.debug("Discovery cleanup error: %s", e)

        logger.debug("Cleanup complete")
