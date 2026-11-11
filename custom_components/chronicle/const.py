"""Constants for the Chronicle integration."""

from __future__ import annotations

from typing import Final

DOMAIN: Final = "chronicle"

# Connection (config entry data)
CONF_DATABASE: Final = "database"
CONF_SSL: Final = "ssl"

DEFAULT_HOST: Final = "1b41fb55-timescaledb"
DEFAULT_PORT: Final = 5432
DEFAULT_USERNAME: Final = "postgres"
DEFAULT_DATABASE: Final = "homeassistant"

# Filter (config entry options)
CONF_INCLUDE_DOMAINS: Final = "include_domains"
CONF_INCLUDE_ENTITIES: Final = "include_entities"
CONF_INCLUDE_GLOBS: Final = "include_entity_globs"
CONF_EXCLUDE_DOMAINS: Final = "exclude_domains"
CONF_EXCLUDE_ENTITIES: Final = "exclude_entities"
CONF_EXCLUDE_GLOBS: Final = "exclude_entity_globs"

# Storage and tuning (config entry options)
CONF_STORE_ATTRIBUTES: Final = "store_attributes"
CONF_SKIP_ATTRIBUTE_ONLY: Final = "skip_attribute_only"
CONF_COMMIT_INTERVAL: Final = "commit_interval"
CONF_MAX_QUEUE: Final = "max_queue"
CONF_CHUNK_DAYS: Final = "chunk_days"
CONF_COMPRESS_AFTER_DAYS: Final = "compress_after_days"
CONF_RETENTION_DAYS: Final = "retention_days"
CONF_AGGREGATES: Final = "aggregates"

DEFAULT_STORE_ATTRIBUTES: Final = True
DEFAULT_SKIP_ATTRIBUTE_ONLY: Final = False
DEFAULT_COMMIT_INTERVAL: Final = 5  # seconds between writes
DEFAULT_MAX_QUEUE: Final = 100_000  # states kept in memory while the database is away
DEFAULT_CHUNK_DAYS: Final = 7
DEFAULT_COMPRESS_AFTER_DAYS: Final = 7
DEFAULT_RETENTION_DAYS: Final = 0  # 0 = keep forever
DEFAULT_AGGREGATES: Final = True

# A batch is written as soon as this many states are waiting, without waiting for the
# commit interval
BATCH_SIZE: Final = 2_000

# Reconnect back-off (seconds)
RECONNECT_MIN: Final = 2
RECONNECT_MAX: Final = 60

# Database statistics for the diagnostic sensors
STATS_INTERVAL: Final = 300  # seconds

# Database layout; every object lives in its own schema
SCHEMA: Final = "chronicle"
SCHEMA_VERSION: Final = 1

# Dispatcher signal sent after every write attempt (writer statistics changed)
SIGNAL_WRITER_UPDATE: Final = f"{DOMAIN}_writer_update_{{}}"

# Services
SERVICE_FLUSH: Final = "flush"
SERVICE_PURGE_ENTITIES: Final = "purge_entities"
SERVICE_IMPORT_LTSS: Final = "import_ltss"
ATTR_KEEP_DAYS: Final = "keep_days"
