"""Database access for Chronicle (asyncpg, TimescaleDB)."""

from __future__ import annotations

import asyncio
import logging
import ssl as ssl_module
from dataclasses import dataclass
from typing import Any

import asyncpg

from .const import SCHEMA, SCHEMA_VERSION

_LOGGER = logging.getLogger(__name__)

# Schema changes take this advisory lock, so two Home Assistant instances (or a restart
# that overlaps the old one) never change the layout at the same time
_LOCK_KEY = 0x6368726F6E  # "chron"

# A state is stored as a number only when it is a plain decimal number (used by the writer).
# Not numbers: "nan", "inf", "1_000", "0x10", "", "unknown", ...
NUMBER_PATTERN = r"^[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$"
# States that also get a value (1/0) next to their text, for duty-cycle aggregates
BOOLEAN_VALUES = {"on": 1.0, "off": 0.0}


class ChronicleDatabaseError(Exception):
    """The database could not be reached or refused the request."""


class TimescaleMissingError(ChronicleDatabaseError):
    """The TimescaleDB extension is not available in the database."""


class PermissionMissingError(ChronicleDatabaseError):
    """The user may not create the schema or the extension."""


@dataclass(slots=True)
class Settings:
    """Storage settings that are applied to the database on every start."""

    chunk_days: int
    compress_after_days: int
    retention_days: int
    aggregates: bool
    time_zone: str


@dataclass(slots=True)
class DatabaseStats:
    """Figures for the diagnostic sensors."""

    database_size: int
    states_size: int
    compressed_before: int | None
    compressed_after: int | None
    rows: int
    entities: int
    attribute_sets: int
    oldest: Any  # datetime | None

    @property
    def compression_ratio(self) -> float | None:
        """How many times smaller the compressed chunks are, None before the first one."""
        if not self.compressed_before or not self.compressed_after:
            return None
        return round(self.compressed_before / self.compressed_after, 1)


# Version 1 of the layout. Statements run in order inside one transaction.
_SCHEMA_V1 = [
    f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.meta (
        key text PRIMARY KEY,
        value text NOT NULL
    )
    """,
    # One row per entity: the states refer to it by a 4-byte id instead of repeating the
    # entity_id text in every row
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.entities (
        id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        entity_id text NOT NULL UNIQUE,
        domain text GENERATED ALWAYS AS (split_part(entity_id, '.', 1)) STORED,
        name text,
        unit text,
        device_class text,
        state_class text,
        first_seen timestamptz NOT NULL DEFAULT now(),
        updated timestamptz NOT NULL DEFAULT now()
    )
    """,
    # Every distinct set of attributes is stored once; hash is the first 64 bits of the
    # BLAKE2b hash of the canonical JSON
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.attributes (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        hash bigint NOT NULL UNIQUE,
        data jsonb NOT NULL
    )
    """,
    # state: the state text, NULL when the state is a number (then it is in value only)
    # value: the number for numeric states, 1/0 for on/off states, else NULL
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.states (
        time timestamptz NOT NULL,
        entity integer NOT NULL,
        state text,
        value double precision,
        attributes bigint
    )
    """,
    f"SELECT create_hypertable('{SCHEMA}.states', by_range('time', INTERVAL '7 days'), if_not_exists => true)",
    f"CREATE INDEX IF NOT EXISTS states_entity_time ON {SCHEMA}.states (entity, time DESC)",
    f"""
    ALTER TABLE {SCHEMA}.states SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'entity',
        timescaledb.compress_orderby = 'time DESC'
    )
    """,
    # Readable view for Grafana and plain SQL
    f"""
    CREATE OR REPLACE VIEW {SCHEMA}.history AS
    SELECT s.time,
           e.entity_id,
           COALESCE(s.state, s.value::text) AS state,
           s.value,
           a.data AS attributes
    FROM {SCHEMA}.states s
    JOIN {SCHEMA}.entities e ON e.id = s.entity
    LEFT JOIN {SCHEMA}.attributes a ON a.id = s.attributes
    """,
]

_MIGRATIONS: dict[int, list[str]] = {1: _SCHEMA_V1}


def _ssl_context(use_ssl: bool) -> ssl_module.SSLContext | bool:
    """SSL without certificate checks: the database is normally on the local network."""
    if not use_ssl:
        return False
    context = ssl_module.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl_module.CERT_NONE
    return context


class ChronicleDatabase:
    """Connection pool and every query Chronicle runs."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        use_ssl: bool = False,
    ) -> None:
        """Store the connection details; nothing connects until async_connect."""
        self._dsn = {
            "host": host,
            "port": port,
            "user": username,
            "password": password,
            "database": database,
        }
        self._ssl = use_ssl
        self._pool: asyncpg.Pool | None = None

    @property
    def connected(self) -> bool:
        """Whether a pool exists (it reconnects single connections by itself)."""
        return self._pool is not None

    async def async_connect(self) -> None:
        """Open the pool; raises ChronicleDatabaseError when the database is unreachable."""
        if self._pool is not None:
            return
        try:
            self._pool = await asyncpg.create_pool(
                **self._dsn,
                ssl=_ssl_context(self._ssl),
                min_size=1,
                max_size=3,
                command_timeout=120,
                timeout=15,
                server_settings={"application_name": "chronicle"},
            )
        except (OSError, TimeoutError, asyncpg.PostgresError, asyncpg.InterfaceError) as err:
            raise _translate(err) from err

    async def async_close(self) -> None:
        """Close the pool, waiting at most 10 seconds for running queries."""
        pool, self._pool = self._pool, None
        if pool is not None:
            try:
                await asyncio.wait_for(pool.close(), 10)
            except Exception:
                pool.terminate()

    def _require_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise ChronicleDatabaseError("Not connected")
        return self._pool

    # ------------------------------------------------------------------ schema

    async def async_check(self) -> str:
        """Validate the connection for the config flow; returns the server version text."""
        await self.async_connect()
        try:
            async with self._require_pool().acquire() as conn:
                await self._ensure_timescaledb(conn)
                return str(await conn.fetchval("SHOW server_version"))
        except (OSError, TimeoutError, asyncpg.PostgresError, asyncpg.InterfaceError) as err:
            raise _translate(err) from err

    async def _ensure_timescaledb(self, conn: asyncpg.Connection) -> None:
        installed = await conn.fetchval(
            "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
        )
        if installed:
            return
        available = await conn.fetchval(
            "SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb'"
        )
        if not available:
            raise TimescaleMissingError("The TimescaleDB extension is not installed on the server")
        await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")

    async def async_setup_schema(self, settings: Settings) -> None:
        """Create or upgrade the layout and apply the storage settings."""
        try:
            async with self._require_pool().acquire() as conn:
                await self._ensure_timescaledb(conn)
                async with conn.transaction():
                    await conn.execute("SELECT pg_advisory_xact_lock($1)", _LOCK_KEY)
                    await self._migrate(conn)
                await self._apply_settings(conn, settings)
        except (OSError, TimeoutError, asyncpg.PostgresError, asyncpg.InterfaceError) as err:
            raise _translate(err) from err

    async def _migrate(self, conn: asyncpg.Connection) -> None:
        exists = await conn.fetchval("SELECT to_regclass($1)", f"{SCHEMA}.meta")
        current = 0
        if exists:
            row = await conn.fetchval(
                f"SELECT value FROM {SCHEMA}.meta WHERE key = 'schema_version'"
            )
            current = int(row) if row else 0
        if current > SCHEMA_VERSION:
            raise ChronicleDatabaseError(
                f"The database layout is version {current}, newer than this Chronicle "
                f"version supports ({SCHEMA_VERSION}). Update the integration."
            )
        for version in range(current + 1, SCHEMA_VERSION + 1):
            _LOGGER.info("Creating/upgrading the Chronicle tables to version %s", version)
            for statement in _MIGRATIONS[version]:
                await conn.execute(statement)
            await conn.execute(
                f"""INSERT INTO {SCHEMA}.meta (key, value) VALUES ('schema_version', $1)
                    ON CONFLICT (key) DO UPDATE SET value = excluded.value""",
                str(version),
            )

    async def _apply_settings(self, conn: asyncpg.Connection, settings: Settings) -> None:
        """Chunk size, compression, retention and the aggregates; safe to repeat."""
        table = f"{SCHEMA}.states"
        await conn.execute(
            "SELECT set_chunk_time_interval($1::regclass, make_interval(days => $2))",
            table,
            settings.chunk_days,
        )
        await conn.execute(
            "SELECT remove_compression_policy($1::regclass, if_exists => true)", table
        )
        await conn.execute(
            "SELECT add_compression_policy($1::regclass, make_interval(days => $2))",
            table,
            settings.compress_after_days,
        )
        await conn.execute("SELECT remove_retention_policy($1::regclass, if_exists => true)", table)
        if settings.retention_days > 0:
            await conn.execute(
                "SELECT add_retention_policy($1::regclass, make_interval(days => $2))",
                table,
                settings.retention_days,
            )
        if settings.aggregates:
            await self._create_aggregates(conn, settings.time_zone)
        else:
            await self._drop_aggregates(conn)

    async def _create_aggregates(self, conn: asyncpg.Connection, time_zone: str) -> None:
        """Hourly and daily averages per numeric entity, kept independently of the raw rows.

        The daily view buckets on local days; when the time zone changed since it was
        created, it is created again.
        """
        stored_zone = await conn.fetchval(
            f"SELECT value FROM {SCHEMA}.meta WHERE key = 'aggregate_time_zone'"
        )
        daily_exists = await conn.fetchval("SELECT to_regclass($1)", f"{SCHEMA}.states_daily")
        if daily_exists and stored_zone != time_zone:
            _LOGGER.info("Time zone changed to %s, rebuilding the daily aggregate", time_zone)
            await conn.execute(f"DROP MATERIALIZED VIEW {SCHEMA}.states_daily")

        await conn.execute(
            f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {SCHEMA}.states_hourly
            WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
            SELECT time_bucket(INTERVAL '1 hour', time) AS bucket,
                   entity,
                   avg(value) AS mean,
                   min(value) AS min,
                   max(value) AS max,
                   last(value, time) AS last,
                   count(*) AS samples
            FROM {SCHEMA}.states
            WHERE value IS NOT NULL
            GROUP BY bucket, entity
            WITH NO DATA
            """
        )
        await conn.execute(
            f"""SELECT add_continuous_aggregate_policy('{SCHEMA}.states_hourly',
                    start_offset => INTERVAL '3 days',
                    end_offset => INTERVAL '1 hour',
                    schedule_interval => INTERVAL '30 minutes',
                    if_not_exists => true)"""
        )
        # Built on the hourly view (hierarchical aggregate): the mean is weighted by samples
        zone = time_zone.replace("'", "''")
        await conn.execute(
            f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {SCHEMA}.states_daily
            WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
            SELECT time_bucket(INTERVAL '1 day', bucket, '{zone}') AS bucket,
                   entity,
                   sum(mean * samples) / sum(samples) AS mean,
                   min(min) AS min,
                   max(max) AS max,
                   last(last, bucket) AS last,
                   sum(samples)::bigint AS samples
            FROM {SCHEMA}.states_hourly
            GROUP BY 1, entity
            WITH NO DATA
            """
        )
        await conn.execute(
            f"""SELECT add_continuous_aggregate_policy('{SCHEMA}.states_daily',
                    start_offset => INTERVAL '10 days',
                    end_offset => INTERVAL '1 hour',
                    schedule_interval => INTERVAL '1 hour',
                    if_not_exists => true)"""
        )
        await conn.execute(
            f"""INSERT INTO {SCHEMA}.meta (key, value) VALUES ('aggregate_time_zone', $1)
                ON CONFLICT (key) DO UPDATE SET value = excluded.value""",
            time_zone,
        )

    async def _drop_aggregates(self, conn: asyncpg.Connection) -> None:
        await conn.execute(f"DROP MATERIALIZED VIEW IF EXISTS {SCHEMA}.states_daily")
        await conn.execute(f"DROP MATERIALIZED VIEW IF EXISTS {SCHEMA}.states_hourly")

    # ------------------------------------------------------------------ writing

    async def async_write(
        self,
        entities: list[tuple[str | None, ...]],
        attributes: list[tuple[int, str]],
        rows_factory: Any,
    ) -> tuple[dict[str, int], dict[int, int], int]:
        """Write one batch in a single transaction.

        entities: (entity_id, name, unit, device_class, state_class) that are new or whose
            metadata changed; upserted, their ids returned.
        attributes: (hash, json) of attribute sets not yet known; their ids returned.
        rows_factory: called with (entity ids, attribute ids) once those are known, returns
            the state rows (time, entity, state, value, attributes) for COPY.
        Returns (entity ids, attribute ids, rows written).
        """
        pool = self._require_pool()
        try:
            async with pool.acquire() as conn, conn.transaction():
                entity_ids: dict[str, int] = {}
                if entities:
                    columns = list(zip(*entities, strict=True))
                    records = await conn.fetch(
                        f"""
                        INSERT INTO {SCHEMA}.entities
                            (entity_id, name, unit, device_class, state_class)
                        SELECT * FROM unnest($1::text[], $2::text[], $3::text[],
                                             $4::text[], $5::text[])
                        ON CONFLICT (entity_id) DO UPDATE SET
                            name = excluded.name,
                            unit = excluded.unit,
                            device_class = excluded.device_class,
                            state_class = excluded.state_class,
                            updated = now()
                        RETURNING id, entity_id
                        """,
                        *columns,
                    )
                    entity_ids = {r["entity_id"]: r["id"] for r in records}

                attribute_ids: dict[int, int] = {}
                if attributes:
                    hashes, documents = zip(*attributes, strict=True)
                    records = await conn.fetch(
                        f"""
                        INSERT INTO {SCHEMA}.attributes (hash, data)
                        SELECT * FROM unnest($1::bigint[], $2::jsonb[])
                        ON CONFLICT (hash) DO UPDATE SET hash = excluded.hash
                        RETURNING id, hash
                        """,
                        list(hashes),
                        list(documents),
                    )
                    attribute_ids = {r["hash"]: r["id"] for r in records}

                rows = rows_factory(entity_ids, attribute_ids)
                if rows:
                    await conn.copy_records_to_table(
                        "states",
                        schema_name=SCHEMA,
                        columns=["time", "entity", "state", "value", "attributes"],
                        records=rows,
                    )
                return entity_ids, attribute_ids, len(rows)
        except (OSError, TimeoutError, asyncpg.PostgresError, asyncpg.InterfaceError) as err:
            raise _translate(err) from err

    # ------------------------------------------------------------------ maintenance

    async def async_entity_ids(self, domains: list[str]) -> list[str]:
        """The recorded entities of these domains."""
        pool = self._require_pool()
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    f"SELECT entity_id FROM {SCHEMA}.entities WHERE domain = ANY($1::text[])",
                    domains,
                )
        except (OSError, asyncpg.PostgresError) as err:
            raise ChronicleDatabaseError(str(err)) from err
        return [r["entity_id"] for r in rows]

    async def async_purge(self, entity_ids: list[str], keep_days: int | None) -> int:
        """Delete the history of entities; with keep_days only rows older than that.

        Without keep_days the entities disappear completely, aggregates included.
        Returns the number of deleted state rows.
        """
        pool = self._require_pool()
        try:
            async with pool.acquire() as conn, conn.transaction():
                ids = [
                    r["id"]
                    for r in await conn.fetch(
                        f"SELECT id FROM {SCHEMA}.entities WHERE entity_id = ANY($1::text[])",
                        entity_ids,
                    )
                ]
                if not ids:
                    return 0
                if keep_days:
                    status = await conn.execute(
                        f"""DELETE FROM {SCHEMA}.states WHERE entity = ANY($1::int[])
                            AND time < now() - make_interval(days => $2)""",
                        ids,
                        keep_days,
                    )
                    return int(status.split()[-1])
                status = await conn.execute(
                    f"DELETE FROM {SCHEMA}.states WHERE entity = ANY($1::int[])", ids
                )
                # The materialized rows of the aggregates live in internal hypertables
                for record in await conn.fetch(
                    """SELECT format('%I.%I', materialization_hypertable_schema,
                                     materialization_hypertable_name) AS name
                       FROM timescaledb_information.continuous_aggregates
                       WHERE view_schema = $1""",
                    SCHEMA,
                ):
                    await conn.execute(
                        f"DELETE FROM {record['name']} WHERE entity = ANY($1::int[])", ids
                    )
                await conn.execute(f"DELETE FROM {SCHEMA}.entities WHERE id = ANY($1::int[])", ids)
                return int(status.split()[-1])
        except (OSError, TimeoutError, asyncpg.PostgresError, asyncpg.InterfaceError) as err:
            raise _translate(err) from err

    async def async_stats(self) -> DatabaseStats:
        """Sizes and counts for the diagnostic sensors (cheap catalog queries only)."""
        pool = self._require_pool()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"""
                    SELECT pg_database_size(current_database()) AS database_size,
                           hypertable_size('{SCHEMA}.states') AS states_size,
                           approximate_row_count('{SCHEMA}.states') AS rows,
                           (SELECT count(*) FROM {SCHEMA}.entities) AS entities,
                           (SELECT reltuples::bigint FROM pg_class
                             WHERE oid = '{SCHEMA}.attributes'::regclass) AS attribute_sets,
                           (SELECT min(range_start) FROM timescaledb_information.chunks
                             WHERE hypertable_schema = '{SCHEMA}'
                               AND hypertable_name = 'states') AS oldest
                    """
                )
                compression = await conn.fetchrow(
                    f"""SELECT sum(before_compression_total_bytes) AS before,
                               sum(after_compression_total_bytes) AS after
                        FROM hypertable_compression_stats('{SCHEMA}.states')"""
                )
        except (OSError, TimeoutError, asyncpg.PostgresError, asyncpg.InterfaceError) as err:
            raise _translate(err) from err
        return DatabaseStats(
            database_size=row["database_size"],
            states_size=row["states_size"] or 0,
            compressed_before=compression["before"],
            compressed_after=compression["after"],
            rows=max(row["rows"] or 0, 0),
            entities=row["entities"],
            attribute_sets=max(row["attribute_sets"] or 0, 0),
            oldest=row["oldest"],
        )


def _translate(err: BaseException) -> ChronicleDatabaseError:
    """Turn driver errors into the integration's own, with a readable message."""
    if isinstance(err, ChronicleDatabaseError):
        return err
    if isinstance(err, asyncpg.InsufficientPrivilegeError):
        return PermissionMissingError(str(err))
    if isinstance(err, asyncpg.InvalidPasswordError):
        return ChronicleDatabaseError(f"Login failed: {err}")
    if isinstance(err, asyncpg.InvalidCatalogNameError):
        return ChronicleDatabaseError(f"Database does not exist: {err}")
    return ChronicleDatabaseError(f"{type(err).__name__}: {err}")
