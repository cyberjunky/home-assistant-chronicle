"""Diagnostic sensors: what Chronicle writes and how big the history is."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import EntityCategory, UnitOfInformation, UnitOfTime
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import SIGNAL_WRITER_UPDATE
from .coordinator import ChronicleStatsCoordinator
from .db import DatabaseStats
from .entity import device_info
from .writer import WriterStats

if TYPE_CHECKING:
    from . import ChronicleConfigEntry


@dataclass(frozen=True, kw_only=True)
class DatabaseSensorDescription(SensorEntityDescription):
    """Sensor fed by the statistics coordinator."""

    value_fn: Callable[[DatabaseStats], Any]


@dataclass(frozen=True, kw_only=True)
class WriterSensorDescription(SensorEntityDescription):
    """Sensor fed by the writer."""

    value_fn: Callable[[WriterStats], Any]


def _size(key: str, value_fn: Callable[[DatabaseStats], Any]) -> DatabaseSensorDescription:
    return DatabaseSensorDescription(
        key=key,
        translation_key=key,
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.BYTES,
        suggested_unit_of_measurement=UnitOfInformation.MEBIBYTES,
        suggested_display_precision=1,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=value_fn,
    )


DATABASE_SENSORS: tuple[DatabaseSensorDescription, ...] = (
    _size("database_size", lambda s: s.database_size),
    _size("history_size", lambda s: s.states_size),
    DatabaseSensorDescription(
        key="compression_ratio",
        translation_key="compression_ratio",
        native_unit_of_measurement="x",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda s: s.compression_ratio,
    ),
    DatabaseSensorDescription(
        key="rows",
        translation_key="rows",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda s: s.rows,
    ),
    DatabaseSensorDescription(
        key="entities",
        translation_key="entities",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda s: s.entities,
    ),
    DatabaseSensorDescription(
        key="attribute_sets",
        translation_key="attribute_sets",
        state_class=SensorStateClass.MEASUREMENT,
        entity_registry_enabled_default=False,
        value_fn=lambda s: s.attribute_sets,
    ),
    DatabaseSensorDescription(
        key="oldest",
        translation_key="oldest",
        device_class=SensorDeviceClass.TIMESTAMP,
        value_fn=lambda s: s.oldest,
    ),
)

WRITER_SENSORS: tuple[WriterSensorDescription, ...] = (
    WriterSensorDescription(
        key="queue",
        translation_key="queue",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda w: w.queue,
    ),
    WriterSensorDescription(
        key="rows_written",
        translation_key="rows_written",
        state_class=SensorStateClass.TOTAL_INCREASING,
        value_fn=lambda w: w.rows_written,
    ),
    WriterSensorDescription(
        key="dropped",
        translation_key="dropped",
        state_class=SensorStateClass.TOTAL_INCREASING,
        value_fn=lambda w: w.dropped,
    ),
    WriterSensorDescription(
        key="last_write",
        translation_key="last_write",
        device_class=SensorDeviceClass.TIMESTAMP,
        value_fn=lambda w: w.last_write,
    ),
    WriterSensorDescription(
        key="write_duration",
        translation_key="write_duration",
        device_class=SensorDeviceClass.DURATION,
        native_unit_of_measurement=UnitOfTime.MILLISECONDS,
        state_class=SensorStateClass.MEASUREMENT,
        entity_registry_enabled_default=False,
        value_fn=lambda w: w.last_duration_ms,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ChronicleConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Add the sensors."""
    data = entry.runtime_data
    entities: list[SensorEntity] = [
        DatabaseSensor(data.coordinator, entry, description) for description in DATABASE_SENSORS
    ]
    entities += [WriterSensor(entry, description) for description in WRITER_SENSORS]
    async_add_entities(entities)


class DatabaseSensor(CoordinatorEntity[ChronicleStatsCoordinator], SensorEntity):
    """A database figure."""

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    entity_description: DatabaseSensorDescription

    def __init__(
        self,
        coordinator: ChronicleStatsCoordinator,
        entry: ChronicleConfigEntry,
        description: DatabaseSensorDescription,
    ) -> None:
        """Initialize."""
        super().__init__(coordinator)
        self.entity_description = description
        self._attr_unique_id = f"{entry.entry_id}_{description.key}"
        self._attr_device_info = device_info(entry)

    @property
    def native_value(self) -> Any:
        """The figure from the last poll."""
        return self.entity_description.value_fn(self.coordinator.data)


class WriterSensor(SensorEntity):
    """A writer figure, updated by the writer (push)."""

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_should_poll = False
    entity_description: WriterSensorDescription

    def __init__(self, entry: ChronicleConfigEntry, description: WriterSensorDescription) -> None:
        """Initialize."""
        self.entity_description = description
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_{description.key}"
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
    def native_value(self) -> Any:
        """The writer's current figure."""
        return self.entity_description.value_fn(self._entry.runtime_data.writer.stats)
