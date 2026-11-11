"""Collects state changes and writes them to the database in batches."""

from __future__ import annotations

import asyncio
import logging
import math
import re
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from datetime import datetime
from hashlib import blake2b
from typing import Any

from homeassistant.const import (
    ATTR_DEVICE_CLASS,
    ATTR_FRIENDLY_NAME,
    ATTR_UNIT_OF_MEASUREMENT,
    EVENT_HOMEASSISTANT_FINAL_WRITE,
    EVENT_STATE_CHANGED,
)
from homeassistant.core import CALLBACK_TYPE, Event, EventStateChangedData, HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.entityfilter import EntityFilter
from homeassistant.helpers.json import json_bytes_sorted
from homeassistant.util import dt as dt_util

from .const import (
    BATCH_SIZE,
    DOMAIN,
    RECONNECT_MAX,
    RECONNECT_MIN,
    SIGNAL_WRITER_UPDATE,
)
from .db import BOOLEAN_VALUES, NUMBER_PATTERN, ChronicleDatabase, ChronicleDatabaseError

_LOGGER = logging.getLogger(__name__)

_NUMBER = re.compile(NUMBER_PATTERN)
_ATTRIBUTE_CACHE_SIZE = 20_000
_FINAL_FLUSH_TIMEOUT = 20
# The diagnostic sensors follow the writer at most this often (seconds), unless the
# connection state changes: they are recorded by Home Assistant's own recorder too
_SENSOR_UPDATE_INTERVAL = 60

# (entity_id, time, state, attributes)
type _Pending = tuple[str, datetime, str, Any]
# Metadata kept per entity in its own table: name, unit, device_class, state_class
type _Meta = tuple[str | None, str | None, str | None, str | None]


def parse_value(state: str) -> tuple[str | None, float | None]:
    """Split a state into (text, number) as stored.

    Plain numbers are stored as number only, on/off as text and 1/0, the rest as text.
    """
    if _NUMBER.match(state):
        value = float(state)
        if math.isfinite(value):
            return None, value
    return state, BOOLEAN_VALUES.get(state)


def attributes_hash(document: bytes) -> int:
    """Signed 64-bit hash of the canonical attribute JSON."""
    return int.from_bytes(blake2b(document, digest_size=8).digest(), "big", signed=True)


@dataclass(slots=True)
class WriterStats:
    """Live figures for the diagnostic sensors."""

    connected: bool = False
    rows_written: int = 0
    batches: int = 0
    dropped: int = 0
    last_write: datetime | None = None
    last_duration_ms: float | None = None
    last_error: str | None = None
    queue: int = 0


class ChronicleWriter:
    """Listens to state changes and writes them in batches; survives database outages.

    The event callback only filters and queues (no I/O, no JSON), so it never slows Home
    Assistant down. A single task writes the queue every commit interval, or sooner when
    BATCH_SIZE states are waiting. While the database is unreachable the queue keeps up
    to max_queue states (the oldest are dropped beyond that) and the task reconnects with
    an increasing delay.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        entry_id: str,
        db: ChronicleDatabase,
        entity_filter: EntityFilter,
        *,
        store_attributes: bool,
        skip_attribute_only: bool = False,
        commit_interval: float,
        max_queue: int,
    ) -> None:
        """Set up the writer; async_start begins listening."""
        self.hass = hass
        self._entry_id = entry_id
        self._db = db
        self._filter = entity_filter
        self._store_attributes = store_attributes
        self._skip_attribute_only = skip_attribute_only
        self._commit_interval = commit_interval
        self._queue: deque[_Pending] = deque(maxlen=max_queue)
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._unsubs: list[CALLBACK_TYPE] = []
        self._stopping = False
        self._last_dispatch = 0.0

        # entity_id -> include? (filter result, reset when the registry changes)
        self._included: dict[str, bool] = {}
        # entity_id -> database id, and the metadata last written for it
        self._entity_ids: dict[str, int] = {}
        self._entity_meta: dict[str, _Meta] = {}
        # attribute hash -> database id (least recently used dropped first)
        self._attribute_ids: OrderedDict[int, int] = OrderedDict()
        self.stats = WriterStats()

    # ------------------------------------------------------------------ lifecycle

    async def async_start(self) -> None:
        """Start listening and writing."""
        self._unsubs.append(
            self.hass.bus.async_listen(EVENT_STATE_CHANGED, self._handle_state_changed)
        )
        self._unsubs.append(
            self.hass.bus.async_listen(
                er.EVENT_ENTITY_REGISTRY_UPDATED, self._handle_registry_updated
            )
        )
        self._unsubs.append(
            self.hass.bus.async_listen_once(
                EVENT_HOMEASSISTANT_FINAL_WRITE, self._handle_final_write
            )
        )
        self._task = self.hass.async_create_background_task(
            self._run(), f"{DOMAIN} writer", eager_start=False
        )

    async def async_stop(self) -> None:
        """Stop listening, write what is queued (bounded) and end the task."""
        for unsub in self._unsubs:
            unsub()
        self._unsubs.clear()
        if self._task is None:
            return
        self._stopping = True
        self._wake.set()
        try:
            await asyncio.wait_for(asyncio.shield(self._task), _FINAL_FLUSH_TIMEOUT)
        except TimeoutError:
            _LOGGER.warning("Could not write %s queued states before stopping", len(self._queue))
            self._task.cancel()
        self._task = None

    async def _handle_final_write(self, _event: Event) -> None:
        await self.async_stop()

    async def async_flush(self) -> None:
        """Write the queue now (service call)."""
        self._wake.set()
        await asyncio.sleep(0)

    # ------------------------------------------------------------------ events

    @callback
    def _handle_registry_updated(self, _event: Event[er.EventEntityRegistryUpdatedData]) -> None:
        # A renamed or newly hidden entity can change the filter result
        self._included.clear()

    def _include(self, entity_id: str) -> bool:
        included = self._included.get(entity_id)
        if included is None:
            entry = er.async_get(self.hass).async_get(entity_id)
            # Chronicle's own sensors change on every write: recording them would feed a loop
            included = (entry is None or entry.platform != DOMAIN) and self._filter(entity_id)
            self._included[entity_id] = included
        return included

    @callback
    def _handle_state_changed(self, event: Event[EventStateChangedData]) -> None:
        new_state = event.data["new_state"]
        if new_state is None or not self._include(new_state.entity_id):
            return
        if self._skip_attribute_only:
            old_state = event.data["old_state"]
            if old_state is not None and old_state.state == new_state.state:
                return
        if len(self._queue) == self._queue.maxlen:
            self.stats.dropped += 1
        self._queue.append(
            (new_state.entity_id, new_state.last_updated, new_state.state, new_state.attributes)
        )
        if len(self._queue) >= BATCH_SIZE:
            self._wake.set()

    # ------------------------------------------------------------------ writing

    async def _run(self) -> None:
        delay = RECONNECT_MIN
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), self._commit_interval)
            except TimeoutError:
                pass
            self._wake.clear()
            try:
                if not self._db.connected:
                    await self._db.async_connect()
                while self._queue:
                    await self._write_batch()
                    if not self._stopping and len(self._queue) < BATCH_SIZE:
                        break
                delay = RECONNECT_MIN
                self._set_connected(True, None)
            except ChronicleDatabaseError as err:
                self._set_connected(False, str(err))
                if self._stopping:
                    _LOGGER.warning(
                        "Stopping without writing %s queued states: %s", len(self._queue), err
                    )
                    return
                _LOGGER.warning(
                    "Writing to the database failed (%s); %s states queued, retrying in %ss",
                    err,
                    len(self._queue),
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX)
                continue
            if self._stopping:
                return

    def _set_connected(self, connected: bool, error: str | None) -> None:
        changed = connected != self.stats.connected or error != self.stats.last_error
        if connected and not self.stats.connected:
            _LOGGER.info("Connected to the database")
        self.stats.connected = connected
        self.stats.last_error = error
        self.stats.queue = len(self._queue)
        now = time.monotonic()
        if changed or now - self._last_dispatch >= _SENSOR_UPDATE_INTERVAL:
            self._last_dispatch = now
            async_dispatcher_send(self.hass, SIGNAL_WRITER_UPDATE.format(self._entry_id))

    async def _write_batch(self) -> None:
        """Write up to BATCH_SIZE states; on failure they go back to the queue."""
        batch = [self._queue.popleft() for _ in range(min(BATCH_SIZE, len(self._queue)))]
        started = time.monotonic()

        new_entities: dict[str, tuple[str | None, ...]] = {}
        new_attributes: dict[int, str] = {}
        prepared: list[tuple[str, datetime, str | None, float | None, int | None]] = []
        for entity_id, when, state, attributes in batch:
            meta: _Meta = (
                attributes.get(ATTR_FRIENDLY_NAME),
                attributes.get(ATTR_UNIT_OF_MEASUREMENT),
                attributes.get(ATTR_DEVICE_CLASS),
                attributes.get("state_class"),
            )
            if self._entity_meta.get(entity_id) != meta:
                new_entities[entity_id] = (entity_id, *(_text(v) for v in meta))
                self._entity_meta[entity_id] = meta
            attribute_hash: int | None = None
            if self._store_attributes and attributes:
                document = json_bytes_sorted(attributes)
                attribute_hash = attributes_hash(document)
                if attribute_hash in self._attribute_ids:
                    self._attribute_ids.move_to_end(attribute_hash)
                else:
                    new_attributes[attribute_hash] = document.decode()
            text, value = parse_value(state)
            prepared.append((entity_id, when, text, value, attribute_hash))

        def rows(entity_ids: dict[str, int], attribute_ids: dict[int, int]) -> list[tuple]:
            self._entity_ids.update(entity_ids)
            for key, ident in attribute_ids.items():
                self._attribute_ids[key] = ident
            while len(self._attribute_ids) > _ATTRIBUTE_CACHE_SIZE:
                self._attribute_ids.popitem(last=False)
            return [
                (
                    when,
                    self._entity_ids[entity_id],
                    text,
                    value,
                    None if attribute_hash is None else self._attribute_ids[attribute_hash],
                )
                for entity_id, when, text, value, attribute_hash in prepared
            ]

        try:
            _, _, written = await self._db.async_write(
                list(new_entities.values()), list(new_attributes.items()), rows
            )
        except ChronicleDatabaseError:
            # Nothing was committed: forget what this batch would have cached, requeue it
            for entity_id in new_entities:
                self._entity_meta.pop(entity_id, None)
            self._queue.extendleft(reversed(batch))
            raise
        except KeyError as err:
            # An id was missing from the caches (e.g. purged meanwhile): rebuild them
            _LOGGER.debug("Cache miss for %s, refreshing the caches", err)
            self.reset_caches()
            self._queue.extendleft(reversed(batch))
            return

        self.stats.rows_written += written
        self.stats.batches += 1
        self.stats.last_write = dt_util.utcnow()
        self.stats.last_duration_ms = round((time.monotonic() - started) * 1000, 1)
        self.stats.queue = len(self._queue)

    def reset_caches(self) -> None:
        """Forget every cached id; the next batch upserts entities again."""
        self._entity_ids.clear()
        self._entity_meta.clear()
        self._attribute_ids.clear()


def _text(value: Any) -> str | None:
    return None if value is None else str(value)
