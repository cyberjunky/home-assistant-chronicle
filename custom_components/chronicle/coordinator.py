"""Polls the database sizes and counts for the diagnostic sensors."""

from __future__ import annotations

import logging
from datetime import timedelta

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import DOMAIN, STATS_INTERVAL
from .db import ChronicleDatabase, ChronicleDatabaseError, DatabaseStats

_LOGGER = logging.getLogger(__name__)


class ChronicleStatsCoordinator(DataUpdateCoordinator[DatabaseStats]):
    """Database statistics every few minutes (catalog queries, no table scans)."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry, db: ChronicleDatabase) -> None:
        """Initialize."""
        self._db = db
        super().__init__(
            hass,
            _LOGGER,
            config_entry=entry,
            name=DOMAIN,
            update_interval=timedelta(seconds=STATS_INTERVAL),
        )

    async def _async_update_data(self) -> DatabaseStats:
        try:
            return await self._db.async_stats()
        except ChronicleDatabaseError as err:
            raise UpdateFailed(str(err)) from err
