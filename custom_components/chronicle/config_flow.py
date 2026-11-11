"""Config flow for Chronicle."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlowWithReload,
)
from homeassistant.const import CONF_HOST, CONF_PASSWORD, CONF_PORT, CONF_USERNAME
from homeassistant.core import callback
from homeassistant.data_entry_flow import section
from homeassistant.helpers.selector import (
    BooleanSelector,
    EntitySelector,
    EntitySelectorConfig,
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    SelectSelector,
    SelectSelectorConfig,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .const import (
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
    DEFAULT_DATABASE,
    DEFAULT_HOST,
    DEFAULT_MAX_QUEUE,
    DEFAULT_PORT,
    DEFAULT_RETENTION_DAYS,
    DEFAULT_SKIP_ATTRIBUTE_ONLY,
    DEFAULT_STORE_ATTRIBUTES,
    DEFAULT_USERNAME,
    DOMAIN,
)
from .db import (
    ChronicleDatabase,
    ChronicleDatabaseError,
    PermissionMissingError,
    TimescaleMissingError,
)

_LOGGER = logging.getLogger(__name__)

# The aggregates refresh the last 10 days; raw rows must outlive that window
MIN_RETENTION_DAYS = 14

CONNECTION_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HOST, default=DEFAULT_HOST): str,
        vol.Required(CONF_PORT, default=DEFAULT_PORT): vol.All(
            vol.Coerce(int), vol.Range(min=1, max=65535)
        ),
        vol.Required(CONF_USERNAME, default=DEFAULT_USERNAME): str,
        vol.Required(CONF_PASSWORD): TextSelector(
            TextSelectorConfig(type=TextSelectorType.PASSWORD)
        ),
        vol.Required(CONF_DATABASE, default=DEFAULT_DATABASE): str,
        vol.Optional(CONF_SSL, default=False): bool,
    }
)


async def validate_connection(data: dict[str, Any]) -> str:
    """Connect, make sure TimescaleDB is there; returns the server version."""
    db = ChronicleDatabase(
        data[CONF_HOST],
        data[CONF_PORT],
        data[CONF_USERNAME],
        data[CONF_PASSWORD],
        data[CONF_DATABASE],
        data.get(CONF_SSL, False),
    )
    try:
        return await db.async_check()
    finally:
        await db.async_close()


async def _try_connection(data: dict[str, Any]) -> dict[str, str]:
    errors: dict[str, str] = {}
    try:
        await validate_connection(data)
    except TimescaleMissingError:
        errors["base"] = "timescaledb_missing"
    except PermissionMissingError:
        errors["base"] = "permission"
    except ChronicleDatabaseError as err:
        _LOGGER.debug("Connection test failed: %s", err)
        errors["base"] = "cannot_connect"
    except Exception:
        _LOGGER.exception("Unexpected exception")
        errors["base"] = "unknown"
    return errors


def _unique_id(data: dict[str, Any]) -> str:
    return f"{data[CONF_HOST]}:{data[CONF_PORT]}/{data[CONF_DATABASE]}"


class ChronicleConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Chronicle."""

    VERSION = 1

    @staticmethod
    @callback
    def async_get_options_flow(_entry: ConfigEntry) -> ChronicleOptionsFlow:
        """Options: which entities, how long and how compact."""
        return ChronicleOptionsFlow()

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Ask for the database."""
        errors: dict[str, str] = {}
        if user_input is not None:
            await self.async_set_unique_id(_unique_id(user_input))
            self._abort_if_unique_id_configured()
            errors = await _try_connection(user_input)
            if not errors:
                return self.async_create_entry(
                    # The database is only added to tell several entries apart
                    title=(
                        f"Chronicle ({user_input[CONF_DATABASE]})"
                        if self._async_current_entries()
                        else "Chronicle"
                    ),
                    data=user_input,
                )
        return self.async_show_form(
            step_id="user",
            data_schema=self.add_suggested_values_to_schema(CONNECTION_SCHEMA, user_input),
            errors=errors,
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Move to another server or database, or change the password."""
        entry = self._get_reconfigure_entry()
        errors: dict[str, str] = {}
        if user_input is not None:
            errors = await _try_connection(user_input)
            if not errors:
                await self.async_set_unique_id(_unique_id(user_input))
                if entry.unique_id != _unique_id(user_input):
                    self._abort_if_unique_id_configured()
                return self.async_update_reload_and_abort(
                    entry, unique_id=_unique_id(user_input), data_updates=user_input
                )
        return self.async_show_form(
            step_id="reconfigure",
            data_schema=self.add_suggested_values_to_schema(
                CONNECTION_SCHEMA, user_input or entry.data
            ),
            errors=errors,
        )


def _days(minimum: int, maximum: int) -> NumberSelector:
    return NumberSelector(
        NumberSelectorConfig(
            min=minimum,
            max=maximum,
            step=1,
            mode=NumberSelectorMode.BOX,
            unit_of_measurement="days",
        )
    )


class ChronicleOptionsFlow(OptionsFlowWithReload):
    """Filter and storage settings; saving reloads the integration."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """One form in two sections."""
        errors: dict[str, str] = {}
        if user_input is not None:
            flat = {**user_input["entities"], **user_input["storage"]}
            retention = int(flat[CONF_RETENTION_DAYS])
            if 0 < retention < MIN_RETENTION_DAYS:
                errors["base"] = "retention_too_short"
            else:
                for key in (
                    CONF_COMMIT_INTERVAL,
                    CONF_MAX_QUEUE,
                    CONF_CHUNK_DAYS,
                    CONF_COMPRESS_AFTER_DAYS,
                    CONF_RETENTION_DAYS,
                ):
                    flat[key] = int(flat[key])
                return self.async_create_entry(data=flat)

        options = self.config_entry.options
        domains = sorted({state.domain for state in self.hass.states.async_all()})
        domain_selector = SelectSelector(
            SelectSelectorConfig(options=domains, multiple=True, custom_value=True, sort=True)
        )
        glob_selector = TextSelector(TextSelectorConfig(multiple=True))
        entity_selector = EntitySelector(EntitySelectorConfig(multiple=True))

        def default(key: str, fallback: Any) -> dict[str, Any]:
            return {"default": options.get(key, fallback)}

        schema = vol.Schema(
            {
                vol.Required("entities"): section(
                    vol.Schema(
                        {
                            vol.Optional(
                                CONF_INCLUDE_DOMAINS, **default(CONF_INCLUDE_DOMAINS, [])
                            ): domain_selector,
                            vol.Optional(
                                CONF_INCLUDE_ENTITIES, **default(CONF_INCLUDE_ENTITIES, [])
                            ): entity_selector,
                            vol.Optional(
                                CONF_INCLUDE_GLOBS, **default(CONF_INCLUDE_GLOBS, [])
                            ): glob_selector,
                            vol.Optional(
                                CONF_EXCLUDE_DOMAINS, **default(CONF_EXCLUDE_DOMAINS, [])
                            ): domain_selector,
                            vol.Optional(
                                CONF_EXCLUDE_ENTITIES, **default(CONF_EXCLUDE_ENTITIES, [])
                            ): entity_selector,
                            vol.Optional(
                                CONF_EXCLUDE_GLOBS, **default(CONF_EXCLUDE_GLOBS, [])
                            ): glob_selector,
                        }
                    ),
                    {"collapsed": False},
                ),
                vol.Required("storage"): section(
                    vol.Schema(
                        {
                            vol.Required(
                                CONF_STORE_ATTRIBUTES,
                                **default(CONF_STORE_ATTRIBUTES, DEFAULT_STORE_ATTRIBUTES),
                            ): BooleanSelector(),
                            vol.Required(
                                CONF_SKIP_ATTRIBUTE_ONLY,
                                **default(CONF_SKIP_ATTRIBUTE_ONLY, DEFAULT_SKIP_ATTRIBUTE_ONLY),
                            ): BooleanSelector(),
                            vol.Required(
                                CONF_AGGREGATES, **default(CONF_AGGREGATES, DEFAULT_AGGREGATES)
                            ): BooleanSelector(),
                            vol.Required(
                                CONF_RETENTION_DAYS,
                                **default(CONF_RETENTION_DAYS, DEFAULT_RETENTION_DAYS),
                            ): _days(0, 36500),
                            vol.Required(
                                CONF_COMPRESS_AFTER_DAYS,
                                **default(CONF_COMPRESS_AFTER_DAYS, DEFAULT_COMPRESS_AFTER_DAYS),
                            ): _days(1, 365),
                            vol.Required(
                                CONF_CHUNK_DAYS, **default(CONF_CHUNK_DAYS, DEFAULT_CHUNK_DAYS)
                            ): _days(1, 90),
                            vol.Required(
                                CONF_COMMIT_INTERVAL,
                                **default(CONF_COMMIT_INTERVAL, DEFAULT_COMMIT_INTERVAL),
                            ): NumberSelector(
                                NumberSelectorConfig(
                                    min=1,
                                    max=300,
                                    step=1,
                                    mode=NumberSelectorMode.BOX,
                                    unit_of_measurement="s",
                                )
                            ),
                            vol.Required(
                                CONF_MAX_QUEUE, **default(CONF_MAX_QUEUE, DEFAULT_MAX_QUEUE)
                            ): NumberSelector(
                                NumberSelectorConfig(
                                    min=1000, max=2_000_000, step=1000, mode=NumberSelectorMode.BOX
                                )
                            ),
                        }
                    ),
                    {"collapsed": True},
                ),
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema, errors=errors)
