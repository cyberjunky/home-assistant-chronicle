"""Connection state of the writer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .const import SIGNAL_WRITER_UPDATE
from .entity import device_info

if TYPE_CHECKING:
    from . import ChronicleConfigEntry


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ChronicleConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Add the connection sensor."""
    async_add_entities([ConnectedSensor(entry)])


class ConnectedSensor(BinarySensorEntity):
    """On while the last write succeeded; the last error is an attribute."""

    _attr_has_entity_name = True
    _attr_translation_key = "connected"
    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_should_poll = False

    def __init__(self, entry: ChronicleConfigEntry) -> None:
        """Initialize."""
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_connected"
        self._attr_device_info = device_info(entry)

    async def async_added_to_hass(self) -> None:
        """Follow the writer."""
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass,
                SIGNAL_WRITER_UPDATE.format(self._entry.entry_id),
                self._handle_update,
            )
        )

    @callback
    def _handle_update(self) -> None:
        self.async_write_ha_state()

    @property
    def is_on(self) -> bool:
        """Whether the database accepted the last write."""
        return self._entry.runtime_data.writer.stats.connected

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """The last error, if any."""
        return {"last_error": self._entry.runtime_data.writer.stats.last_error}
