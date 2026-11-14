"""Config flow."""

from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers.service_info.hassio import HassioServiceInfo
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.chronicle.const import DOMAIN


async def test_user_flow(hass: HomeAssistant, database) -> None:
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    assert result["type"] is FlowResultType.FORM
    result = await hass.config_entries.flow.async_configure(result["flow_id"], database)
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"] == database
    assert result["title"] == "Chronicle"


async def test_wrong_password(hass: HomeAssistant, database) -> None:
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {**database, "password": "wrong"}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "cannot_connect"}


async def test_options_flow(hass: HomeAssistant, database) -> None:
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    result = await hass.config_entries.flow.async_configure(result["flow_id"], database)
    entry = result["result"]
    await hass.async_block_till_done()

    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert result["type"] is FlowResultType.FORM
    storage = {
        "store_attributes": True,
        "aggregates": True,
        "retention_days": 5,
        "compress_after_days": 7,
        "chunk_days": 7,
        "commit_interval": 5,
        "max_queue": 100000,
    }
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {"entities": {}, "storage": storage}
    )
    assert result["errors"] == {"base": "retention_too_short"}
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {
            "entities": {"exclude_domains": ["automation"]},
            "storage": {**storage, "retention_days": 90},
        },
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options["retention_days"] == 90
    assert entry.options["exclude_domains"] == ["automation"]
    await hass.async_block_till_done()


def _discovery(config: dict) -> HassioServiceInfo:
    return HassioServiceInfo(
        config=config, name="Chronicle", slug="1b41fb55_app-chronicle", uuid="1234"
    )


async def test_hassio_discovery(hass: HomeAssistant, database) -> None:
    """The Chronicle app announces its database: confirm, and the entry is created."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_HASSIO}, data=_discovery(database)
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "hassio_confirm"
    assert result["description_placeholders"]["database"] == database["database"]
    result = await hass.config_entries.flow.async_configure(result["flow_id"], {})
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"] == {**database, "ssl": False}
    assert result["title"] == "Chronicle"


async def test_hassio_discovery_wrong_password(hass: HomeAssistant, database) -> None:
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_HASSIO},
        data=_discovery({**database, "password": "wrong"}),
    )
    result = await hass.config_entries.flow.async_configure(result["flow_id"], {})
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "cannot_connect"}


async def test_hassio_discovery_updates_the_entry(hass: HomeAssistant, database) -> None:
    """Announced again for a database Chronicle already records into: the entry takes the
    new connection (a new password after a restore) and no second entry is offered."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={**database, "password": "old", "ssl": False},
        unique_id=f"{database['host']}:{database['port']}/{database['database']}",
    )
    entry.add_to_hass(hass)
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_HASSIO}, data=_discovery(database)
    )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"
    assert entry.data["password"] == database["password"]


async def test_hassio_discovery_incomplete(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_HASSIO}, data=_discovery({"host": "x"})
    )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "invalid_discovery"
