"""CLI entry point for chromecast-sink."""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys

from chromecast_sink import __version__, _opus, _pulse
from chromecast_sink.orchestrator import Config, Orchestrator


def _parse_bitrate(value: str) -> int:
    """Parse bitrate string like '128k' into integer bps."""
    value = value.strip().lower()
    if value.endswith("k"):
        return int(float(value[:-1]) * 1000)
    if value.endswith("m"):
        return int(float(value[:-1]) * 1000000)
    return int(value)


def _check_dependencies() -> list[str]:
    """Check that required system libraries and tools are available."""
    errors = []

    for check in (_opus.check_available, _pulse.check_available):
        error = check()
        if error:
            errors.append(error)

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
        "--target-delay",
        type=int,
        default=0,
        metavar="MS",
        help=(
            "Target playout delay in milliseconds (default: 0). "
            "0 = minimum latency, higher = more buffer against dropouts. "
            "Valid range: 0-5000."
        ),
    )
    parser.add_argument(
        "--opus-bitrate",
        default="128k",
        metavar="RATE",
        help="Opus bitrate (default: 128k)",
    )
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
    errors = _check_dependencies()
    if errors:
        print("Missing dependencies:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1

    # Build config and run
    config = Config(
        device_name=args.device,
        timeout=args.timeout,
        target_delay=args.target_delay,
        opus_bitrate=_parse_bitrate(args.opus_bitrate),
    )

    orchestrator = Orchestrator(config)
    return orchestrator.run()


if __name__ == "__main__":
    sys.exit(main())
