"""Fixtures: a fresh database on a local TimescaleDB for every test.

Start one with:
    docker run -d --name chronicle-test-db -p 55432:5432 -e POSTGRES_PASSWORD=test \
        timescale/timescaledb:latest-pg18
Tests that need it are skipped when it isn't running.
"""

from __future__ import annotations

import os
import uuid

import asyncpg
import pytest

TEST_HOST = os.environ.get("CHRONICLE_TEST_HOST", "127.0.0.1")
TEST_PORT = int(os.environ.get("CHRONICLE_TEST_PORT", "55432"))
TEST_PASSWORD = os.environ.get("CHRONICLE_TEST_PASSWORD", "test")


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations):
    """Load custom_components/chronicle."""
    return


@pytest.fixture
async def database(request, socket_enabled):
    """Create an empty database and drop it afterwards; yields its connection data."""
    name = f"chronicle_{uuid.uuid4().hex[:8]}"
    try:
        admin = await asyncpg.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            user="postgres",
            password=TEST_PASSWORD,
            database="postgres",
            timeout=3,
        )
    except (OSError, asyncpg.PostgresError) as err:
        pytest.skip(f"No test database: {err}")
    await admin.execute(f'CREATE DATABASE "{name}"')
    try:
        yield {
            "host": TEST_HOST,
            "port": TEST_PORT,
            "username": "postgres",
            "password": TEST_PASSWORD,
            "database": name,
            "ssl": False,
        }
    finally:
        # Unload Chronicle first, so its pool doesn't reconnect to a dropped database
        if "hass" in request.fixturenames:
            hass = request.getfixturevalue("hass")
            for entry in hass.config_entries.async_entries("chronicle"):
                if entry.state.recoverable:
                    await hass.config_entries.async_unload(entry.entry_id)
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await admin.close()


@pytest.fixture
async def sql(database):
    """A plain connection to the test database for checks."""
    conn = await asyncpg.connect(
        host=database["host"],
        port=database["port"],
        user="postgres",
        password=TEST_PASSWORD,
        database=database["database"],
    )
    yield conn
    await conn.close()
