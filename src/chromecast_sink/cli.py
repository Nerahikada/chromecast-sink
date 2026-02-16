"""CLI entry point for chromecast-sink."""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys

from chromecast_sink import __version__
from chromecast_sink.audio_capture import FORMATS
from chromecast_sink.orchestrator import Config, Orchestrator


def _parse_bitrate(value: str) -> int:
    """Parse bitrate string like '128k' into integer bps."""
    value = value.strip().lower()
    if value.endswith("k"):
        return int(float(value[:-1]) * 1000)
    if value.endswith("m"):
        return int(float(value[:-1]) * 1000000)
    return int(value)


def _check_dependencies(mode: str) -> list[str]:
    """Check that required system tools are available."""
    errors = []

    if not shutil.which("ffmpeg"):
        errors.append("ffmpeg not found. Install with: sudo apt install ffmpeg")

    if not shutil.which("pactl"):
        errors.append(
            "pactl not found. Install with: sudo apt install pulseaudio-utils"
        )

    if not shutil.which("pw-cli"):
        errors.append("pw-cli not found. Install with: sudo apt install pipewire")

    if not shutil.which("pw-dump"):
        errors.append("pw-dump not found. Install with: sudo apt install pipewire")

    # Check PipeWire / PulseAudio is running
    result = subprocess.run(
        ["pactl", "info"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        errors.append(
            "PulseAudio/PipeWire is not running. Check:\n"
            "  systemctl --user status pipewire pipewire-pulse"
        )

    # Check for libopus support in FFmpeg (needed for stream mode)
    if mode == "stream":
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            capture_output=True,
            text=True,
        )
        if "libopus" not in result.stdout:
            errors.append(
                "FFmpeg was built without libopus support.\n"
                "  Install with: sudo apt install libopus-dev\n"
                "  Or use --mode http for legacy HTTP streaming."
            )

    return errors


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="chromecast-sink",
        description=(
            "Use a Google Chromecast / Nest device as a Linux speaker output. "
            "Creates a PipeWire virtual sink that streams audio to the Chromecast."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "-d", "--device",
        metavar="NAME",
        help="Connect to a specific device by name (skip interactive selection)",
    )
    parser.add_argument(
        "-m", "--mode",
        choices=["stream", "http"],
        default="stream",
        help=(
            "Streaming mode (default: stream). "
            "stream = Cast Streaming via UDP, "
            "http = legacy HTTP streaming."
        ),
    )
    parser.add_argument(
        "--target-delay",
        type=int,
        default=0,
        metavar="MS",
        help=(
            "Target playout delay in milliseconds (default: 0). "
            "0 = minimum latency, higher = more buffer against dropouts. "
            "Valid range: 0-5000. Only used with --mode stream."
        ),
    )
    parser.add_argument(
        "--opus-bitrate",
        default="128k",
        metavar="RATE",
        help=(
            "Opus bitrate for stream mode (default: 128k). "
            "Only used with --mode stream."
        ),
    )

    # HTTP mode options
    http_group = parser.add_argument_group(
        "HTTP mode options",
        "These options only apply when using --mode http.",
    )
    http_group.add_argument(
        "-f", "--format",
        choices=list(FORMATS.keys()),
        default="wav",
        help=(
            "Audio format (default: wav). "
            "wav = WAV 32-bit, "
            "wav16 = WAV 16-bit, "
            "flac = FLAC, "
            "mp3 = MP3."
        ),
    )
    http_group.add_argument(
        "-b", "--bitrate",
        default="320k",
        metavar="RATE",
        help="MP3 bitrate (default: 320k). Only used with --format mp3.",
    )
    http_group.add_argument(
        "--sample-rate",
        type=int,
        default=44100,
        metavar="HZ",
        help="Audio sample rate in Hz (default: 44100)",
    )
    http_group.add_argument(
        "-p", "--port",
        type=int,
        default=0,
        metavar="PORT",
        help="HTTP server port (default: auto-select)",
    )

    # Common options
    parser.add_argument(
        "-t", "--timeout",
        type=float,
        default=10,
        metavar="SECS",
        help="Device discovery timeout in seconds (default: 10)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose/debug logging",
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    args = _parse_args(argv)

    # Configure logging
    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Check dependencies
    errors = _check_dependencies(args.mode)
    if errors:
        print("Missing dependencies:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1

    # Build config and run
    config = Config(
        mode=args.mode,
        audio_format=args.format,
        bitrate=args.bitrate,
        sample_rate=args.sample_rate,
        port=args.port,
        device_name=args.device,
        timeout=args.timeout,
        target_delay=args.target_delay,
        opus_bitrate=_parse_bitrate(args.opus_bitrate),
    )

    orchestrator = Orchestrator(config)
    return orchestrator.run()


if __name__ == "__main__":
    sys.exit(main())
