"""Config flow."""

from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType

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
