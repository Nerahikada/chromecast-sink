"""PipeWire virtual sink management.

Creates a native PipeWire null-audio-sink (not a PulseAudio compat module)
with the correct properties to appear in GNOME Sound Settings:
  - node.virtual=false  — GNOME filters out virtual=true nodes
  - device.class=sound  — WirePlumber exposes it as a real audio device
  - media.class=Audio/Sink
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SinkInfo:
    """Information about a created virtual sink."""

    node_id: int
    sink_name: str
    monitor_source: str


def _sanitize_sink_name(device_name: str) -> str:
    """Convert a device friendly name to a valid sink name."""
    name = device_name.lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = name.strip("_")
    return f"chromecast_sink_{name}" if name else "chromecast_sink"


def _find_pa_sink_name(node_name: str) -> str | None:
    """Find the PulseAudio sink name for a PipeWire node by node.name."""
    result = subprocess.run(
        ["pactl", "-f", "json", "list", "sinks"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        return None

    try:
        sinks = json.loads(result.stdout)
    except (json.JSONDecodeError, ValueError):
        return None

    for sink in sinks:
        name = sink.get("name", "")
        props = sink.get("properties", {})
        nn = props.get("node.name", "")
        if name == node_name or nn == node_name:
            return name

    return None


def _find_pw_node_id(node_name: str) -> int | None:
    """Find a PipeWire node ID by its node.name using pw-dump."""
    result = subprocess.run(
        ["pw-dump"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        return None

    try:
        objects = json.loads(result.stdout)
    except (json.JSONDecodeError, ValueError):
        return None

    for obj in objects:
        if obj.get("type") != "PipeWire:Interface:Node":
            continue
        props = obj.get("info", {}).get("props", {})
        if props.get("node.name") == node_name:
            return obj.get("id")

    return None


def _parse_pw_cli_node_id(output: str) -> int | None:
    """Parse node ID from pw-cli create-node output.

    Output format varies by PipeWire version:
      "id: 42, type: PipeWire:Interface:Node/3"
    or just:
      "42"
    """
    match = re.search(r"id:\s*(\d+)", output)
    if match:
        return int(match.group(1))
    stripped = output.strip()
    if stripped.isdigit():
        return int(stripped)
    return None


def cleanup_stale_sinks() -> None:
    """Remove any lingering chromecast_sink nodes from previous runs.

    Because we create nodes with object.linger=true, they persist even
    after the process exits. Old nodes (especially those without
    node.force-quantum) can interfere with PipeWire's quantum negotiation.
    """
    result = subprocess.run(
        ["pw-dump"], capture_output=True, text=True,
    )
    if result.returncode != 0:
        return

    try:
        objects = json.loads(result.stdout)
    except (json.JSONDecodeError, ValueError):
        return

    stale_ids = []
    for obj in objects:
        if obj.get("type") != "PipeWire:Interface:Node":
            continue
        props = obj.get("info", {}).get("props", {})
        node_name = props.get("node.name", "")
        if node_name.startswith("chromecast_sink"):
            stale_ids.append(obj["id"])

    for node_id in stale_ids:
        logger.info("Removing stale PipeWire node: id=%d", node_id)
        subprocess.run(
            ["pw-cli", "destroy", str(node_id)],
            capture_output=True, text=True,
        )

    if stale_ids:
        logger.info("Cleaned up %d stale node(s)", len(stale_ids))


def create_virtual_sink(device_name: str) -> SinkInfo:
    """Create a PipeWire null-audio-sink that appears in GNOME Sound Settings.

    Uses pw-cli to create a native PipeWire node with the properties that
    EasyEffects and similar apps use to make sinks visible in GNOME:
      - node.virtual=false  — GNOME filters out virtual=true nodes
      - device.class=sound  — WirePlumber exposes it as a real audio device

    The user selects this sink from Sound Settings when they want to cast.

    Args:
        device_name: Chromecast friendly name.

    Returns:
        SinkInfo with node_id, sink_name, and monitor_source.
    """
    # Remove any leftover nodes from previous runs first
    cleanup_stale_sinks()

    sink_name = _sanitize_sink_name(device_name)
    description = f"Chromecast - {device_name}"

    # Create a native PipeWire null-audio-sink via pw-cli.
    # Key properties for GNOME visibility:
    #   node.virtual=false  — GNOME filters out virtual=true
    #   device.class=sound  — WirePlumber treats it as a real audio device
    props = (
        "{ "
        f"factory.name=support.null-audio-sink "
        f'node.name="{sink_name}" '
        f'node.description="{description}" '
        f'device.description="{description}" '
        f"device.class=sound "
        f"node.virtual=false "
        f"media.class=Audio/Sink "
        f"audio.position=[FL FR] "
        f"object.linger=true "
        f"monitor.channel-volumes=true "
        # Force a small quantum to minimize PipeWire capture latency.
        # 256 samples @ 48kHz = ~5.3ms (vs default 2048 = ~42ms).
        f"node.force-quantum=256 "
        "}"
    )

    logger.debug("Creating PipeWire node: pw-cli create-node adapter '%s'", props)

    result = subprocess.run(
        ["pw-cli", "create-node", "adapter", props],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to create PipeWire node: {result.stderr.strip()}\n"
            "Ensure PipeWire is running:\n"
            "  systemctl --user status pipewire"
        )

    # Try parsing node ID from stdout, then stderr (varies by PipeWire version)
    node_id = _parse_pw_cli_node_id(result.stdout)
    if node_id is None:
        node_id = _parse_pw_cli_node_id(result.stderr)

    # If pw-cli didn't print the ID, wait for registration and look it up
    if node_id is None:
        logger.debug(
            "pw-cli returned no node ID (stdout=%r, stderr=%r), "
            "searching by node.name...",
            result.stdout.strip(),
            result.stderr.strip(),
        )
        time.sleep(1.0)
        node_id = _find_pw_node_id(sink_name)

    if node_id is None:
        raise RuntimeError(
            "Virtual sink node was not found after creation.\n"
            f"pw-cli stdout: {result.stdout!r}\n"
            f"pw-cli stderr: {result.stderr!r}\n"
            "Try running manually:\n"
            f"  pw-cli create-node adapter '{props}'"
        )

    logger.info("Created PipeWire node: id=%d, name=%s", node_id, sink_name)

    # Wait for PipeWire to register the node and PulseAudio layer to pick it up
    time.sleep(1.0)

    # Find the actual PulseAudio sink name (may differ from node.name)
    pa_sink_name = _find_pa_sink_name(sink_name)
    if pa_sink_name:
        logger.info("PulseAudio sink name: %s", pa_sink_name)
    else:
        pa_sink_name = sink_name
        logger.warning(
            "Could not find PulseAudio sink name, using node name: %s",
            pa_sink_name,
        )

    monitor_source = f"{pa_sink_name}.monitor"

    return SinkInfo(
        node_id=node_id,
        sink_name=pa_sink_name,
        monitor_source=monitor_source,
    )


def destroy_virtual_sink(sink_info: SinkInfo) -> None:
    """Remove the virtual sink (PipeWire node)."""
    result = subprocess.run(
        ["pw-cli", "destroy", str(sink_info.node_id)],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        logger.info("Destroyed PipeWire node (id=%d)", sink_info.node_id)
    else:
        logger.warning(
            "Failed to destroy PipeWire node (id=%d): %s",
            sink_info.node_id,
            result.stderr.strip(),
        )
