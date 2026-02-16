"""Local IP address detection for Chromecast communication."""

from __future__ import annotations

import socket
import logging

logger = logging.getLogger(__name__)


def get_local_ip(chromecast_host: str) -> str:
    """Determine the local IP address on the same network as the Chromecast.

    Uses the UDP socket connect trick: creates a UDP socket, "connects" to the
    Chromecast IP (no actual data is sent), then reads back the local address
    the OS routing table selected. This works correctly on multi-homed machines
    (VPN, Docker bridge, multiple NICs).
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect((chromecast_host, 80))
            local_ip = s.getsockname()[0]
        finally:
            s.close()
        logger.debug("Local IP for reaching %s: %s", chromecast_host, local_ip)
        return local_ip
    except OSError:
        logger.warning(
            "Failed to detect local IP via UDP connect to %s, falling back",
            chromecast_host,
        )
        return _fallback_local_ip()


def _fallback_local_ip() -> str:
    """Fallback: find a non-loopback IPv4 address."""
    try:
        hostname = socket.gethostname()
        addrs = socket.getaddrinfo(hostname, None, socket.AF_INET)
        for addr in addrs:
            ip = addr[4][0]
            if not ip.startswith("127."):
                return ip
    except OSError:
        pass
    raise RuntimeError(
        "Could not determine local IP address. "
        "Please check your network configuration."
    )
