"""Audio capture and Opus encoding for the Cast Streaming pipeline.

Reads PCM from the virtual sink's monitor source and encodes it to Opus
in-process, feeding frames straight to the Cast RTP sender.
"""

from __future__ import annotations

import logging
import threading
import time

from chromecast_sink._opus import OpusEncoder
from chromecast_sink._pulse import MonitorRecorder
from chromecast_sink.cast_rtp import OPUS_SAMPLES_PER_FRAME, CastRTPSender

logger = logging.getLogger(__name__)

SAMPLE_RATE = 48000
CHANNELS = 2
_BYTES_PER_SAMPLE = 2

# One fragment per Opus frame, so a read yields exactly one frame to encode.
# The sink's quantum (node.force-quantum) dominates capture latency, so there
# is nothing to gain from reading in smaller pieces.
FRAGMENT_BYTES = OPUS_SAMPLES_PER_FRAME * CHANNELS * _BYTES_PER_SAMPLE

# If the process stalls (GC, scheduler), audio piles up server-side and the
# backlog never clears on its own: reading proceeds at real time, so it can
# never outpace the source. Discarding the backlog is the only way back to low
# latency, and it costs one audible glitch instead of permanent delay.
_DRAIN_THRESHOLD_MS = 30.0
_DRAIN_TARGET_MS = 10.0
_DRAIN_MAX_FRAMES = 2000  # 20s; a runaway guard, never reached in practice


def _drain(recorder: MonitorRecorder) -> int:
    """Discard buffered audio until capture latency is back to target.

    Returns the number of frames dropped.
    """
    dropped = 0
    while dropped < _DRAIN_MAX_FRAMES and recorder.latency_ms() > _DRAIN_TARGET_MS:
        recorder.read()
        dropped += 1
    return dropped


def run_capture(
    monitor_source: str,
    cast_sender: CastRTPSender,
    stop_event: threading.Event,
    bit_rate: int = 128000,
) -> None:
    """Capture, encode, and send audio until stop_event is set.

    Each read blocks for one 10ms fragment, which paces the loop at real time.

    Args:
        monitor_source: PulseAudio monitor source name of the virtual sink.
        cast_sender: Configured CastRTPSender to receive the Opus frames.
        stop_event: Set this to stop the loop.
        bit_rate: Opus bitrate in bps.
    """
    recorder = MonitorRecorder(
        monitor_source,
        sample_rate=SAMPLE_RATE,
        channels=CHANNELS,
        fragment_bytes=FRAGMENT_BYTES,
    )
    encoder = OpusEncoder(
        sample_rate=SAMPLE_RATE, channels=CHANNELS, bit_rate=bit_rate
    )
    logger.info(
        "Capture started: %s, Opus %d kbps, %d ms frames, encoder lookahead %.1f ms",
        monitor_source,
        bit_rate // 1000,
        OPUS_SAMPLES_PER_FRAME * 1000 // SAMPLE_RATE,
        encoder.lookahead_samples * 1000 / SAMPLE_RATE,
    )

    start = time.monotonic()
    first_frame: float | None = None
    frames = 0
    dropped_total = 0
    last_stats = start

    try:
        while not stop_event.is_set():
            pcm = recorder.read()

            if recorder.latency_ms() > _DRAIN_THRESHOLD_MS:
                dropped = _drain(recorder) + 1  # the frame just read is stale too
                dropped_total += dropped
                logger.warning(
                    "Fell behind; dropped %d frames (%d ms) to restore latency",
                    dropped, dropped * OPUS_SAMPLES_PER_FRAME * 1000 // SAMPLE_RATE,
                )
                continue

            now = time.monotonic()
            if first_frame is None:
                first_frame = now
                logger.info(
                    "First audio frame captured (%.0f ms after start)",
                    (now - start) * 1000,
                )

            cast_sender.send_frame(encoder.encode(pcm, OPUS_SAMPLES_PER_FRAME))
            frames += 1

            if now - last_stats >= 5.0:
                elapsed = now - first_frame
                logger.info(
                    "Capture stats: %d frames in %.1fs (%.1f fps, expected ~100), "
                    "%d dropped, latency %.1f ms",
                    frames, elapsed, frames / elapsed if elapsed > 0 else 0,
                    dropped_total, recorder.latency_ms(),
                )
                last_stats = now
    finally:
        recorder.close()
        logger.info(
            "Capture stopped (%d frames sent, %d dropped)", frames, dropped_total
        )
