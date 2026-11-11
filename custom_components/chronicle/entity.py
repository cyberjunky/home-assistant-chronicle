"""Shared device for the Chronicle entities."""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_HOST
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo

from .const import CONF_DATABASE, DOMAIN


def device_info(entry: ConfigEntry) -> DeviceInfo:
    """One service device per database."""
    return DeviceInfo(
        identifiers={(DOMAIN, entry.entry_id)},
        name=entry.title,
        manufacturer="cyberjunky",
        model=f"{entry.data[CONF_DATABASE]} on {entry.data[CONF_HOST]}",
        entry_type=DeviceEntryType.SERVICE,
    )
