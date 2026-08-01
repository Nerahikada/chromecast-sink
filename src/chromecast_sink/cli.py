"""CLI entry point for chromecast-sink."""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys

from chromecast_sink import __version__, _opus, _pulse
from chromecast_sink.orchestrator import Orchestrator


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
    else:
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

    if not shutil.which("pw-cli"):
        errors.append("pw-cli not found. Install with: sudo apt install pipewire")

    if not shutil.which("pw-dump"):
        errors.append("pw-dump not found. Install with: sudo apt install pipewire")

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
        help="Connect to a specific device by name",
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

    return Orchestrator(device_name=args.device).run()


if __name__ == "__main__":
    sys.exit(main())
