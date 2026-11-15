"""Chronicle: long-term history of every state in TimescaleDB."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import probatio as vol
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    ATTR_ENTITY_ID,
    CONF_HOST,
    CONF_PASSWORD,
    CONF_PORT,
    CONF_USERNAME,
    Platform,
)
from homeassistant.core import HomeAssistant, ServiceCall, ServiceResponse, SupportsResponse
from homeassistant.exceptions import ConfigEntryNotReady, HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.entityfilter import EntityFilter, convert_filter
from homeassistant.helpers.typing import ConfigType

from .const import (
    ATTR_DOMAIN,
    ATTR_KEEP_DAYS,
    ATTR_PURGE,
    CONF_AGGREGATES,
    CONF_CHUNK_DAYS,
    CONF_COMMIT_INTERVAL,
    CONF_COMPRESS_AFTER_DAYS,
    CONF_DATABASE,
    CONF_EXCLUDE_DOMAINS,
    CONF_EXCLUDE_ENTITIES,
    CONF_EXCLUDE_GLOBS,
    CONF_INCLUDE_DOMAINS,
    CONF_INCLUDE_ENTITIES,
    CONF_INCLUDE_GLOBS,
    CONF_MAX_QUEUE,
    CONF_RETENTION_DAYS,
    CONF_SKIP_ATTRIBUTE_ONLY,
    CONF_SSL,
    CONF_STORE_ATTRIBUTES,
    DEFAULT_AGGREGATES,
    DEFAULT_CHUNK_DAYS,
    DEFAULT_COMMIT_INTERVAL,
    DEFAULT_COMPRESS_AFTER_DAYS,
    DEFAULT_MAX_QUEUE,
    DEFAULT_RETENTION_DAYS,
    DEFAULT_SKIP_ATTRIBUTE_ONLY,
    DEFAULT_STORE_ATTRIBUTES,
    DOMAIN,
    SERVICE_EXCLUDE_DOMAINS,
    SERVICE_EXCLUDE_ENTITIES,
    SERVICE_FLUSH,
    SERVICE_PURGE_ENTITIES,
)
from .coordinator import ChronicleStatsCoordinator
from .db import ChronicleDatabase, ChronicleDatabaseError, Settings
from .writer import ChronicleWriter

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.BINARY_SENSOR, Platform.SENSOR]

CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

type ChronicleConfigEntry = ConfigEntry[ChronicleData]


@dataclass(slots=True)
class ChronicleData:
    """Everything a config entry runs."""

    db: ChronicleDatabase
    writer: ChronicleWriter
    coordinator: ChronicleStatsCoordinator


def option(entry: ConfigEntry, key: str, default: Any) -> Any:
    """An option with its default."""
    return entry.options.get(key, default)


def build_filter(options: dict[str, Any]) -> EntityFilter:
    """Entity filter from the options; with no include rules everything is included."""
    return convert_filter(
        {
            "include_domains": options.get(CONF_INCLUDE_DOMAINS, []),
            "include_entities": options.get(CONF_INCLUDE_ENTITIES, []),
            "include_entity_globs": options.get(CONF_INCLUDE_GLOBS, []),
            "exclude_domains": options.get(CONF_EXCLUDE_DOMAINS, []),
            "exclude_entities": options.get(CONF_EXCLUDE_ENTITIES, []),
            "exclude_entity_globs": options.get(CONF_EXCLUDE_GLOBS, []),
        }
    )


async def async_setup(hass: HomeAssistant, _config: ConfigType) -> bool:
    """Register the services (once, for all entries)."""
    _register_services(hass)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ChronicleConfigEntry) -> bool:
    """Connect, create or upgrade the tables, then start recording."""
    db = ChronicleDatabase(
        entry.data[CONF_HOST],
        entry.data[CONF_PORT],
        entry.data[CONF_USERNAME],
        entry.data[CONF_PASSWORD],
        entry.data[CONF_DATABASE],
        entry.data.get(CONF_SSL, False),
    )
    settings = Settings(
        chunk_days=option(entry, CONF_CHUNK_DAYS, DEFAULT_CHUNK_DAYS),
        compress_after_days=option(entry, CONF_COMPRESS_AFTER_DAYS, DEFAULT_COMPRESS_AFTER_DAYS),
        retention_days=option(entry, CONF_RETENTION_DAYS, DEFAULT_RETENTION_DAYS),
        aggregates=option(entry, CONF_AGGREGATES, DEFAULT_AGGREGATES),
        time_zone=hass.config.time_zone,
    )
    try:
        await db.async_connect()
        await db.async_setup_schema(settings)
    except ChronicleDatabaseError as err:
        await db.async_close()
        raise ConfigEntryNotReady(str(err)) from err

    writer = ChronicleWriter(
        hass,
        entry.entry_id,
        db,
        build_filter(dict(entry.options)),
        store_attributes=option(entry, CONF_STORE_ATTRIBUTES, DEFAULT_STORE_ATTRIBUTES),
        skip_attribute_only=option(entry, CONF_SKIP_ATTRIBUTE_ONLY, DEFAULT_SKIP_ATTRIBUTE_ONLY),
        commit_interval=option(entry, CONF_COMMIT_INTERVAL, DEFAULT_COMMIT_INTERVAL),
        max_queue=option(entry, CONF_MAX_QUEUE, DEFAULT_MAX_QUEUE),
    )
    coordinator = ChronicleStatsCoordinator(hass, entry, db)
    entry.runtime_data = ChronicleData(db, writer, coordinator)

    await writer.async_start()
    await coordinator.async_refresh()
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ChronicleConfigEntry) -> bool:
    """Write what is queued, then disconnect."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        await entry.runtime_data.writer.async_stop()
        await entry.runtime_data.db.async_close()
    return unload_ok


# ---------------------------------------------------------------------- services


def _loaded_entries(hass: HomeAssistant) -> list[ChronicleConfigEntry]:
    entries = hass.config_entries.async_loaded_entries(DOMAIN)
    if not entries:
        raise HomeAssistantError("Chronicle is not connected to a database")
    return entries


def _register_services(hass: HomeAssistant) -> None:

    async def flush(_call: ServiceCall) -> None:
        for entry in _loaded_entries(hass):
            await entry.runtime_data.writer.async_flush()

    async def purge_entities(call: ServiceCall) -> ServiceResponse:
        entity_ids: list[str] = call.data[ATTR_ENTITY_ID]
        keep_days: int | None = call.data.get(ATTR_KEEP_DAYS)
        deleted = 0
        for entry in _loaded_entries(hass):
            data = entry.runtime_data
            await data.writer.async_flush()
            try:
                deleted += await data.db.async_purge(entity_ids, keep_days)
            except ChronicleDatabaseError as err:
                raise HomeAssistantError(f"Purge failed: {err}") from err
            data.writer.reset_caches()
            await data.coordinator.async_request_refresh()
        return {"deleted_rows": deleted}

    async def exclude(option_key: str, items: list[str], purge: bool) -> int:
        """Puts entities or domains on an exclude list in the options and restarts the
        recording with it; with purge, their recorded history is deleted too."""
        deleted = 0
        for entry in _loaded_entries(hass):
            data = entry.runtime_data
            excluded = list(entry.options.get(option_key, []))
            excluded += [i for i in items if i not in excluded]
            if purge:
                await data.writer.async_flush()
                try:
                    entity_ids = (
                        items
                        if option_key == CONF_EXCLUDE_ENTITIES
                        else await data.db.async_entity_ids(items)
                    )
                    if entity_ids:
                        deleted += await data.db.async_purge(entity_ids, None)
                except ChronicleDatabaseError as err:
                    raise HomeAssistantError(f"Purge failed: {err}") from err
            hass.config_entries.async_update_entry(
                entry, options={**entry.options, option_key: excluded}
            )
            hass.config_entries.async_schedule_reload(entry.entry_id)
        return deleted

    async def exclude_entities(call: ServiceCall) -> ServiceResponse:
        entity_ids: list[str] = call.data[ATTR_ENTITY_ID]
        deleted = await exclude(CONF_EXCLUDE_ENTITIES, entity_ids, call.data[ATTR_PURGE])
        # A new list: the response is typed as JSON values, which list[str] is not to mypy
        return {"excluded": [*entity_ids], "deleted_rows": deleted}

    async def exclude_domains(call: ServiceCall) -> ServiceResponse:
        domains: list[str] = call.data[ATTR_DOMAIN]
        deleted = await exclude(CONF_EXCLUDE_DOMAINS, domains, call.data[ATTR_PURGE])
        return {"excluded": [*domains], "deleted_rows": deleted}

    hass.services.async_register(DOMAIN, SERVICE_FLUSH, flush)
    hass.services.async_register(
        DOMAIN,
        SERVICE_PURGE_ENTITIES,
        purge_entities,
        schema=vol.Schema(
            {
                vol.Required(ATTR_ENTITY_ID): cv.entity_ids,
                vol.Optional(ATTR_KEEP_DAYS): vol.All(vol.Coerce(int), vol.Range(min=1)),
            }
        ),
        supports_response=SupportsResponse.OPTIONAL,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_EXCLUDE_ENTITIES,
        exclude_entities,
        schema=vol.Schema(
            {
                vol.Required(ATTR_ENTITY_ID): cv.entity_ids,
                vol.Optional(ATTR_PURGE, default=False): cv.boolean,
            }
        ),
        supports_response=SupportsResponse.OPTIONAL,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_EXCLUDE_DOMAINS,
        exclude_domains,
        schema=vol.Schema(
            {
                vol.Required(ATTR_DOMAIN): vol.All(cv.ensure_list, [cv.string]),
                vol.Optional(ATTR_PURGE, default=False): cv.boolean,
            }
        ),
        supports_response=SupportsResponse.OPTIONAL,
    )
