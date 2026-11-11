"""End to end: Home Assistant states arrive in TimescaleDB."""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.chronicle.const import DOMAIN


async def _setup(
    hass: HomeAssistant, database: dict, options: dict | None = None
) -> MockConfigEntry:
    entry = MockConfigEntry(domain=DOMAIN, data=database, options=options or {}, title="Chronicle")
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.LOADED
    return entry


async def _flush(hass: HomeAssistant) -> None:
    await hass.services.async_call(DOMAIN, "flush", blocking=True)
    # The writer runs as a background task: give it time to finish the batch
    for _ in range(50):
        await hass.async_block_till_done()
        writer = hass.config_entries.async_entries(DOMAIN)[0].runtime_data.writer
        if not writer._queue:
            break
        import asyncio

        await asyncio.sleep(0.05)
    import asyncio

    await asyncio.sleep(0.3)


async def test_records_states(hass: HomeAssistant, database, sql) -> None:
    entry = await _setup(hass, database)

    hass.states.async_set(
        "sensor.power", "123.4", {"unit_of_measurement": "W", "friendly_name": "Power"}
    )
    hass.states.async_set(
        "sensor.power", "125", {"unit_of_measurement": "W", "friendly_name": "Power"}
    )
    hass.states.async_set("light.desk", "on", {"brightness": 200})
    hass.states.async_set("sensor.mode", "eco")
    await _flush(hass)

    rows = await sql.fetch(
        "SELECT entity_id, state, value, attributes FROM chronicle.history ORDER BY time"
    )
    recorded = [(r["entity_id"], r["state"], r["value"]) for r in rows]
    assert ("sensor.power", "123.4", 123.4) in recorded
    assert ("sensor.power", "125", 125.0) in recorded
    assert ("light.desk", "on", 1.0) in recorded
    assert ("sensor.mode", "eco", None) in recorded

    # The two power states share one attribute set; entity metadata is in its own table
    assert (
        await sql.fetchval(
            "SELECT count(DISTINCT attributes) FROM chronicle.states s JOIN chronicle.entities e"
            " ON e.id = s.entity WHERE e.entity_id = 'sensor.power'"
        )
        == 1
    )
    meta = await sql.fetchrow(
        "SELECT name, unit, domain FROM chronicle.entities WHERE entity_id = 'sensor.power'"
    )
    assert tuple(meta) == ("Power", "W", "sensor")

    # Chronicle's own sensors are not recorded
    assert not await sql.fetchval(
        "SELECT count(*) FROM chronicle.entities WHERE entity_id LIKE '%chronicle%'"
    )
    assert entry.runtime_data.writer.stats.rows_written >= 4

    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_filter_and_no_attributes(hass: HomeAssistant, database, sql) -> None:
    await _setup(
        hass,
        database,
        {
            "include_domains": ["sensor"],
            "exclude_entity_globs": ["sensor.*_rssi"],
            "store_attributes": False,
        },
    )
    hass.states.async_set("sensor.temp", "20", {"unit_of_measurement": "°C"})
    hass.states.async_set("sensor.wifi_rssi", "-60")
    hass.states.async_set("switch.pump", "on")
    await _flush(hass)

    entities = {r["entity_id"] for r in await sql.fetch("SELECT entity_id FROM chronicle.entities")}
    assert entities == {"sensor.temp"}
    assert await sql.fetchval("SELECT count(*) FROM chronicle.attributes") == 0


async def test_survives_database_outage(hass: HomeAssistant, database, sql) -> None:
    entry = await _setup(hass, database)
    writer = entry.runtime_data.writer
    db = entry.runtime_data.db

    # Simulate the database going away: the pool is closed under the writer
    await db.async_close()
    hass.states.async_set("sensor.during_outage", "1")
    await hass.services.async_call(DOMAIN, "flush", blocking=True)
    import asyncio

    for _ in range(40):
        await asyncio.sleep(0.1)
        if writer.stats.rows_written:
            break
    assert (
        await sql.fetchval(
            "SELECT count(*) FROM chronicle.history WHERE entity_id = 'sensor.during_outage'"
        )
        == 1
    )


async def test_purge(hass: HomeAssistant, database, sql) -> None:
    await _setup(hass, database)
    hass.states.async_set("sensor.gone", "1")
    hass.states.async_set("sensor.kept", "2")
    await _flush(hass)

    response = await hass.services.async_call(
        DOMAIN,
        "purge_entities",
        {"entity_id": ["sensor.gone"]},
        blocking=True,
        return_response=True,
    )
    assert response == {"deleted_rows": 1}
    assert {
        r["entity_id"] for r in await sql.fetch("SELECT entity_id FROM chronicle.entities")
    } == {"sensor.kept"}

    # The entity comes back cleanly after a purge (caches were reset)
    hass.states.async_set("sensor.gone", "3")
    await _flush(hass)
    assert (
        await sql.fetchval("SELECT count(*) FROM chronicle.history WHERE entity_id = 'sensor.gone'")
        == 1
    )


async def test_exclude_entities(hass: HomeAssistant, database, sql) -> None:
    entry = await _setup(hass, database, {"exclude_entities": ["sensor.other"]})
    hass.states.async_set("sensor.noisy", "1")
    hass.states.async_set("sensor.kept", "2")
    await _flush(hass)

    response = await hass.services.async_call(
        DOMAIN,
        "exclude_entities",
        {"entity_id": ["sensor.noisy"], "purge": True},
        blocking=True,
        return_response=True,
    )
    await hass.async_block_till_done()
    assert response == {"excluded": ["sensor.noisy"], "deleted_rows": 1}
    assert entry.options["exclude_entities"] == ["sensor.other", "sensor.noisy"]
    assert entry.state is ConfigEntryState.LOADED

    # No longer recorded after the restart; the others still are
    hass.states.async_set("sensor.noisy", "3")
    hass.states.async_set("sensor.kept", "4")
    await _flush(hass)
    assert {r["entity_id"] for r in await sql.fetch("SELECT entity_id FROM chronicle.history")} == {
        "sensor.kept"
    }


async def test_exclude_domains(hass: HomeAssistant, database, sql) -> None:
    entry = await _setup(hass, database)
    hass.states.async_set("automation.a", "on")
    hass.states.async_set("automation.b", "off")
    hass.states.async_set("sensor.kept", "2")
    await _flush(hass)

    response = await hass.services.async_call(
        DOMAIN,
        "exclude_domains",
        {"domain": "automation", "purge": True},
        blocking=True,
        return_response=True,
    )
    await hass.async_block_till_done()
    assert response == {"excluded": ["automation"], "deleted_rows": 2}
    assert entry.options["exclude_domains"] == ["automation"]

    hass.states.async_set("automation.a", "off")
    hass.states.async_set("sensor.kept", "3")
    await _flush(hass)
    assert {r["entity_id"] for r in await sql.fetch("SELECT entity_id FROM chronicle.history")} == {
        "sensor.kept"
    }


async def test_sensors(hass: HomeAssistant, database) -> None:
    await _setup(hass, database)
    state = hass.states.get("binary_sensor.chronicle_connected")
    assert state is not None
    assert hass.states.get("sensor.chronicle_database_size") is not None


async def test_skip_attribute_only(hass: HomeAssistant, database, sql) -> None:
    await _setup(hass, database, {"skip_attribute_only": True})
    hass.states.async_set("automation.a", "on", {"last_triggered": "1"})
    hass.states.async_set("automation.a", "on", {"last_triggered": "2"})
    hass.states.async_set("automation.a", "off", {"last_triggered": "2"})
    await _flush(hass)

    states = [
        r["state"] for r in await sql.fetch("SELECT state FROM chronicle.history ORDER BY time")
    ]
    assert states == ["on", "off"]
