"""FFmpeg subprocess management for audio capture and encoding."""

from __future__ import annotations

import logging
import os
import subprocess
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Audio format definitions.
# Chromecast Default Media Receiver buffers ~512KB before playback starts.
# Higher bitrate = buffer fills faster = lower latency.
#
# Format     | Bitrate      | 512KB fill time | Typical latency
# -----------+--------------+-----------------+----------------
# wav32      | ~345 KB/s    | ~1.5s           | ~2s
# wav16      | ~172 KB/s    | ~3s             | ~3-5s
# flac       | ~100 KB/s    | ~5s             | ~5-7s
# mp3 320k   | ~40 KB/s     | ~13s            | ~10s


@dataclass
class AudioFormat:
    """Audio format configuration for FFmpeg and HTTP server."""

    name: str
    ffmpeg_args: list[str]
    content_type: str


FORMATS: dict[str, AudioFormat] = {
    "wav": AudioFormat(
        name="wav",
        # WAV 32-bit LE: highest bitrate → lowest latency (~2s)
        ffmpeg_args=["-acodec", "pcm_s32le", "-f", "wav"],
        content_type="audio/wav",
    ),
    "wav16": AudioFormat(
        name="wav16",
        # WAV 16-bit LE: moderate bitrate → moderate latency (~3-5s)
        ffmpeg_args=["-acodec", "pcm_s16le", "-f", "wav"],
        content_type="audio/wav",
    ),
    "flac": AudioFormat(
        name="flac",
        ffmpeg_args=["-acodec", "flac", "-f", "flac"],
        content_type="audio/flac",
    ),
    "mp3": AudioFormat(
        name="mp3",
        # MP3 at specified bitrate (set dynamically)
        ffmpeg_args=["-acodec", "libmp3lame", "-f", "mp3"],
        content_type="audio/mpeg",
    ),
}


def start_ffmpeg(
    monitor_source: str,
    audio_format: str = "wav",
    bitrate: str = "320k",
    sample_rate: int = 44100,
) -> subprocess.Popen:
    """Launch FFmpeg to capture audio from a PulseAudio monitor source.

    Args:
        monitor_source: PulseAudio source name (e.g. "chromecast_sink_xxx.monitor").
        audio_format: Output format key (wav, wav16, flac, mp3).
        bitrate: MP3 bitrate (only used when audio_format="mp3").
        sample_rate: Audio sample rate in Hz.

    Returns:
        The running FFmpeg subprocess with stdout=PIPE.
    """
    fmt = FORMATS[audio_format]

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        # Input: PulseAudio monitor source (works via PipeWire compatibility)
        "-f", "pulse",
        "-fragment_size", "1024",
        "-i", monitor_source,
        # Output settings
        "-ac", "2",
        "-ar", str(sample_rate),
        *fmt.ffmpeg_args,
        "-fflags", "+nobuffer",
        "-flush_packets", "1",
    ]

    # Add bitrate for MP3
    if audio_format == "mp3":
        cmd.extend(["-ab", bitrate])

    cmd.append("pipe:1")

    logger.debug("Starting FFmpeg: %s", " ".join(cmd))

    env = {**os.environ, "PULSE_LATENCY_MSEC": "20"}

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )

    return proc


def verify_ffmpeg_running(proc: subprocess.Popen, wait_seconds: float = 0.3) -> None:
    """Check that FFmpeg started successfully.

    Polls the process after a short delay. If it exited, reads stderr
    for the error message.

    Raises:
        RuntimeError: If FFmpeg exited early.
    """
    time.sleep(wait_seconds)
    retcode = proc.poll()
    if retcode is not None:
        stderr = ""
        if proc.stderr:
            stderr = proc.stderr.read().decode(errors="replace")
        raise RuntimeError(
            f"FFmpeg exited with code {retcode}.\n"
            f"stderr: {stderr}"
        )
    logger.debug("FFmpeg is running (pid=%d)", proc.pid)


def start_ffmpeg_opus_rtp(
    monitor_source: str,
    local_rtp_port: int,
    bit_rate: int = 128000,
) -> subprocess.Popen:
    """Launch FFmpeg to encode audio as Opus and output via RTP to localhost.

    Used by the Cast Streaming pipeline. FFmpeg encodes audio as Opus
    and sends standard RTP packets to a local UDP port, where the
    Python relay thread picks them up for Cast RTP re-packetization.

    Args:
        monitor_source: PulseAudio source name.
        local_rtp_port: Local UDP port for RTP output.
        bit_rate: Opus bitrate in bps (default 128000).

    Returns:
        The running FFmpeg subprocess.
    """
    input_args = [
        "-analyzeduration", "0",
        "-probesize", "32",
        "-f", "pulse",
        "-fragment_size", "1024",
        "-i", monitor_source,
    ]

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        *input_args,
        # Output: Opus, 48kHz stereo
        "-c:a", "libopus",
        "-ar", "48000",
        "-ac", "2",
        "-b:a", str(bit_rate),
        # Low-latency Opus settings:
        # - frame_duration 10ms (vs default 20ms)
        # - application=lowdelay reduces algorithmic delay from ~26.5ms to ~5ms
        "-frame_duration", "10",
        "-application", "lowdelay",
        "-fflags", "+nobuffer",
        "-flush_packets", "1",
        # RTP output: max_delay=0 disables packet batching
        "-max_delay", "0",
        "-f", "rtp",
        f"rtp://127.0.0.1:{local_rtp_port}",
    ]

    logger.debug("Starting FFmpeg (Opus/RTP): %s", " ".join(cmd))

    # PULSE_LATENCY_MSEC overrides PipeWire's pulse.default.frag (default 2s!)
    env = {**os.environ, "PULSE_LATENCY_MSEC": "20"}

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        env=env,
    )
    return proc


def stop_ffmpeg(proc: subprocess.Popen) -> None:
    """Terminate the FFmpeg process gracefully."""
    if proc.poll() is not None:
        return  # Already exited

    logger.debug("Stopping FFmpeg (pid=%d)", proc.pid)

    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        logger.debug("FFmpeg did not exit after SIGTERM, sending SIGKILL")
        proc.kill()
        proc.wait(timeout=2)

    # Close file descriptors
    if proc.stdout:
        proc.stdout.close()
    if proc.stderr:
        proc.stderr.close()
