"""Chromecast device discovery via mDNS/zeroconf."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pychromecast

if TYPE_CHECKING:
    from pychromecast import Chromecast
    from zeroconf import ServiceBrowser

logger = logging.getLogger(__name__)


class DiscoveryResult:
    """Holds discovered Chromecasts and the browser for cleanup."""

    def __init__(
        self,
        chromecasts: list[Chromecast],
        browser: ServiceBrowser | None,
    ):
        self.chromecasts = chromecasts
        self.browser = browser

    def stop(self) -> None:
        """Stop the mDNS discovery browser."""
        if self.browser is not None:
            try:
                pychromecast.discovery.stop_discovery(self.browser)
            except Exception as e:
                logger.debug("Error stopping discovery: %s", e)
            self.browser = None


def discover_devices(
    timeout: float = 10,
    device_name: str | None = None,
) -> DiscoveryResult:
    """Discover Chromecast / Google Nest devices on the local network.

    Args:
        timeout: Seconds to wait for device discovery.
        device_name: If set, search for this specific device name (faster).

    Returns:
        DiscoveryResult containing found devices and the browser handle.
    """
    if device_name:
        logger.debug("Searching for device: %s", device_name)
        chromecasts, browser = pychromecast.get_listed_chromecasts(
            friendly_names=[device_name],
            timeout=timeout,
        )
    else:
        logger.debug("Discovering all Chromecast devices (timeout=%ss)", timeout)
        chromecasts, browser = pychromecast.get_chromecasts(timeout=timeout)

    logger.info("Found %d device(s)", len(chromecasts))
    for cc in chromecasts:
        logger.debug(
            "  %s (%s:%s, model=%s)",
            cc.cast_info.friendly_name,
            cc.cast_info.host,
            cc.cast_info.port,
            cc.cast_info.model_name,
        )

    return DiscoveryResult(chromecasts=chromecasts, browser=browser)
