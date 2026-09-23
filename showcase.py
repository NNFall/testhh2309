"""In-memory quote showcase with bounded catalog reads and atomic imports."""

from __future__ import annotations

import asyncio
import ctypes
import os
import re
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass
from urllib.parse import quote, urlsplit

import aiohttp
import orjson
from aiohttp import web

from packed_store import PackedStore


FIRST_LINE = re.compile(rb'^\s*\{\s*"quotes"\s*:\s*\[\s*$')
LAST_LINE = re.compile(rb'^\s*\]\s*\}\s*$')
MAX_IMPORT_BYTES = 115 * 1024 * 1024
OVERLAY_LIMIT = 12 * 1024 * 1024
BACKGROUND_AFTER = 15.0
REQUIRE_FRESH_AFTER = 55.0
MAX_AGE = 60.0
CATALOG_TIMEOUT = aiohttp.ClientTimeout(total=2.4, connect=0.4, sock_connect=0.4)


def json_reply(data: object, status: int = 200, headers: dict | None = None) -> web.Response:
    return web.Response(
        body=orjson.dumps(data),
        status=status,
        headers=headers,
        content_type="application/json",
    )


def valid_quote(record: object) -> bool:
    return (
        isinstance(record, dict)
        and isinstance(record.get("id"), str)
        and bool(record["id"])
        and isinstance(record.get("author"), str)
        and isinstance(record.get("text"), str)
    )


def release_unused_memory() -> None:
    # The container may receive many full snapshots without restarting. On
    # glibc, return freed compressed generations and temporary index buffers.
    if sys.platform.startswith("linux"):
        try:
            ctypes.CDLL(None).malloc_trim(0)
        except (AttributeError, OSError):
            pass


async def read_bounded(request: web.Request, limit: int) -> bytes:
    try:
        return await request.content.readexactly(limit + 1)
    except asyncio.IncompleteReadError as exc:
        return exc.partial


@dataclass(slots=True)
class CachedQuote:
    raw: bytes
    updated_at: float
    version: int
    direct: bool


class Showcase:
    def __init__(self) -> None:
        self.store: PackedStore | None = None
        self.snapshot_at = 0.0
        self.source: str | None = None
        self.source_epoch = 0
        self.session: aiohttp.ClientSession | None = None
        self.import_lock = asyncio.Lock()
        self.catalog_limit = asyncio.Semaphore(4)
        self.inflight: dict[str, asyncio.Task] = {}
        self.overlay: OrderedDict[str, CachedQuote] = OrderedDict()
        self.overlay_bytes = 0
        self.tombstones: dict[str, int] = {}
        self.invalidated: dict[str, int] = {}
        self.write_versions: dict[str, int] = {}
        self.version = 0
        self.breaker_until = 0.0
        self.failures = 0
        self.refresh_after: OrderedDict[str, float] = OrderedDict()
        self.retry_after: OrderedDict[str, float] = OrderedDict()
        self.served_local = 0
        self.served_from_catalog = 0
        self.catalog_reads = 0
        self.evictions = 0

    def _next_version(self) -> int:
        self.version += 1
        return self.version

    def _remove_overlay(self, quote_id: str) -> None:
        entry = self.overlay.pop(quote_id, None)
        if entry is not None:
            self.overlay_bytes -= len(entry.raw) + len(quote_id.encode("utf-8"))

    def _put_overlay(self, quote_id: str, raw: bytes, direct: bool) -> None:
        self._remove_overlay(quote_id)
        self.overlay[quote_id] = CachedQuote(raw, time.monotonic(), self._next_version(), direct)
        self.overlay_bytes += len(raw) + len(quote_id.encode("utf-8"))
        self.tombstones.pop(quote_id, None)
        self.invalidated.pop(quote_id, None)
        self.refresh_after.pop(quote_id, None)
        self._evict_overlays()

    def _evict_overlays(self) -> None:
        if self.overlay_bytes <= OVERLAY_LIMIT:
            return
        for quote_id, entry in list(self.overlay.items()):
            if self.overlay_bytes <= OVERLAY_LIMIT:
                break
            if entry.direct:
                continue
            self._remove_overlay(quote_id)
            self.evictions += 1

    def _local_raw(self, quote_id: str) -> tuple[bytes | None, float]:
        if quote_id in self.tombstones or quote_id in self.invalidated:
            return None, 0.0
        cached = self.overlay.get(quote_id)
        if cached is not None:
            self.overlay.move_to_end(quote_id)
            return cached.raw, cached.updated_at
        if self.store is None:
            return None, 0.0
        raw = self.store.get_raw(quote_id)
        return raw, self.snapshot_at if raw is not None else 0.0

    def _available_locally(self) -> int:
        known = len(self.store) if self.store is not None else 0
        if self.store is not None:
            known -= sum(1 for key in self.tombstones if self.store.contains(key))
            known -= sum(1 for key in self.invalidated if self.store.contains(key) and key not in self.tombstones)
            known += sum(1 for key in self.overlay if not self.store.contains(key))
        else:
            known += len(self.overlay)
        return known

    def stats(self) -> dict:
        local = self._available_locally()
        return {
            "served_local": self.served_local,
            "served_from_catalog": self.served_from_catalog,
            "catalog_reads": self.catalog_reads,
            "evictions": self.evictions,
            "local": local,
            "bytes": (self.store.bytes_used if self.store is not None else 0) + self.overlay_bytes,
            "catalog": local,
        }

    def set_source(self, url: str) -> None:
        if url != self.source:
            self.source_epoch += 1
            self.source = url
            self.breaker_until = 0.0
            self.failures = 0
            self.refresh_after.clear()
            self.retry_after.clear()
            self.snapshot_at = 0.0
            for quote_id, entry in list(self.overlay.items()):
                if not entry.direct:
                    self._remove_overlay(quote_id)

    def _catalog_url(self, quote_id: str) -> str:
        return self.source.rstrip("/") + "/quote/" + quote(quote_id, safe="")

    def _backoff(self, seconds: float | None = None) -> None:
        self.failures += 1
        if seconds is None:
            seconds = min(30.0, 0.5 * (2 ** min(self.failures - 1, 6)))
        self.breaker_until = max(self.breaker_until, time.monotonic() + max(0.0, seconds))

    def _note_refresh_failure(self, quote_id: str) -> None:
        self.refresh_after[quote_id] = max(self.breaker_until, time.monotonic() + 1.0)
        self.refresh_after.move_to_end(quote_id)
        if len(self.refresh_after) > 8192:
            self.refresh_after.popitem(last=False)

    def _note_busy(self, quote_id: str, seconds: float) -> None:
        now = time.monotonic()
        self.retry_after[quote_id] = now + seconds
        self.retry_after.move_to_end(quote_id)
        if len(self.retry_after) > 8192:
            self.retry_after.popitem(last=False)
        # A short global pause protects a struggling catalog without treating
        # one busy quote as proof that every other quote is unavailable.
        self.breaker_until = max(self.breaker_until, now + 0.25)

    async def _fetch_from_catalog(self, quote_id: str) -> tuple[int, bytes | None]:
        if self.source is None or self.session is None:
            return 503, None
        source_epoch = self.source_epoch
        mutation_version = self.write_versions.get(quote_id, 0)

        async with self.catalog_limit:
            if source_epoch != self.source_epoch:
                return 503, None
            cooldown = self.breaker_until - time.monotonic()
            if cooldown > 1.0:
                return 503, None
            if cooldown > 0:
                await asyncio.sleep(cooldown)
            if source_epoch != self.source_epoch:
                return 503, None
            self.catalog_reads += 1
            try:
                async with self.session.get(self._catalog_url(quote_id), timeout=CATALOG_TIMEOUT) as response:
                    if response.status == 503:
                        try:
                            retry = min(3600.0, max(0.0, float(response.headers.get("Retry-After", "1"))))
                        except ValueError:
                            retry = 1.0
                        self._note_busy(quote_id, retry)
                        self._note_refresh_failure(quote_id)
                        return 503, None
                    if response.status == 404:
                        self.failures = 0
                        if mutation_version != self.write_versions.get(quote_id, 0) or source_epoch != self.source_epoch:
                            return 409, None
                        self._remove_overlay(quote_id)
                        if self.store is not None and self.store.contains(quote_id):
                            self.invalidated[quote_id] = self._next_version()
                        return 404, None
                    if response.status != 200:
                        self._backoff()
                        self._note_refresh_failure(quote_id)
                        return 503, None

                    raw_body = await response.read()
                    try:
                        record = orjson.loads(raw_body)
                    except orjson.JSONDecodeError:
                        record = None
                    if not valid_quote(record) or record["id"] != quote_id:
                        self._backoff()
                        self._note_refresh_failure(quote_id)
                        return 503, None
                    if mutation_version != self.write_versions.get(quote_id, 0) or source_epoch != self.source_epoch:
                        return 409, None
                    raw = orjson.dumps(record)
                    self._put_overlay(quote_id, raw, direct=False)
                    self.failures = 0
                    return 200, raw
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
                self._backoff()
                self._note_refresh_failure(quote_id)
                return 503, None

    def _lookup(self, quote_id: str, background: bool = False) -> asyncio.Task | None:
        task = self.inflight.get(quote_id)
        if task is not None:
            return task
        now = time.monotonic()
        if self.source is None or self.breaker_until - now > 1.0 or len(self.inflight) >= 96:
            return None
        if now < self.retry_after.get(quote_id, 0.0):
            return None
        if background and now < self.refresh_after.get(quote_id, 0.0):
            return None
        task = asyncio.create_task(self._fetch_from_catalog(quote_id))
        self.inflight[quote_id] = task

        def finished(done: asyncio.Task) -> None:
            if self.inflight.get(quote_id) is done:
                del self.inflight[quote_id]
            if not done.cancelled():
                done.exception()

        task.add_done_callback(finished)
        return task

    async def reader(self, quote_id: str) -> web.Response:
        if quote_id in self.tombstones:
            self.served_local += 1
            return json_reply({"detail": "not found"}, 404)

        direct = self.overlay.get(quote_id)
        if direct is not None and direct.direct:
            self.overlay.move_to_end(quote_id)
            self.served_local += 1
            return web.Response(body=direct.raw, headers={"X-Source": "LOCAL"}, content_type="application/json")

        raw, updated_at = self._local_raw(quote_id)
        age = time.monotonic() - updated_at if raw is not None else float("inf")
        if raw is not None and age < REQUIRE_FRESH_AFTER:
            if age >= BACKGROUND_AFTER:
                self._lookup(quote_id, background=True)
            self.served_local += 1
            return web.Response(body=raw, headers={"X-Source": "LOCAL"}, content_type="application/json")

        task = self._lookup(quote_id)
        if task is None:
            return self._fallback(quote_id, raw, updated_at)

        try:
            status, fetched = await asyncio.wait_for(asyncio.shield(task), timeout=2.6)
        except asyncio.TimeoutError:
            status, fetched = 503, None

        if status == 200 and fetched is not None:
            self.served_from_catalog += 1
            return web.Response(body=fetched, headers={"X-Source": "CATALOG"}, content_type="application/json")
        if status == 404:
            self.served_from_catalog += 1
            return json_reply({"detail": "not found"}, 404)
        if status == 409:
            current, changed_at = self._local_raw(quote_id)
            if current is not None and time.monotonic() - changed_at < MAX_AGE:
                self.served_local += 1
                return web.Response(body=current, headers={"X-Source": "LOCAL"}, content_type="application/json")
        return self._fallback(quote_id, raw, updated_at)

    def _fallback(self, quote_id: str, raw: bytes | None, updated_at: float) -> web.Response:
        if quote_id in self.tombstones:
            self.served_local += 1
            return json_reply({"detail": "not found"}, 404)
        if raw is not None and time.monotonic() - updated_at < MAX_AGE:
            self.served_local += 1
            return web.Response(body=raw, headers={"X-Source": "LOCAL"}, content_type="application/json")
        self.served_from_catalog += 1
        return json_reply({"detail": "catalog unavailable"}, 503)


async def parse_import(request: web.Request, store: PackedStore) -> int:
    """Validate the newline-framed snapshot while building a new generation."""
    stream = request.content
    first = await stream.readline()
    consumed = len(first)
    if not first or consumed > MAX_IMPORT_BYTES:
        raise ValueError("invalid import body")

    # Small clients often send compact JSON on one line. Large snapshots use
    # the guaranteed line format and are never accumulated in memory.
    if not FIRST_LINE.fullmatch(first.strip()):
        if len(first) > 1024 * 1024:
            raise ValueError("single-line import is too large")
        try:
            document = orjson.loads(first)
        except orjson.JSONDecodeError as exc:
            raise ValueError("invalid JSON") from exc
        if not isinstance(document, dict) or not isinstance(document.get("quotes"), list):
            raise ValueError("quotes must be an array")
        for record in document["quotes"]:
            if not valid_quote(record):
                raise ValueError("invalid quote")
            store.add(record)
        await check_trailing_whitespace(stream)
        store.seal()
        return len(document["quotes"])

    imported = 0
    comma_after_previous = True
    while True:
        line = await stream.readline()
        consumed += len(line)
        if not line or consumed > MAX_IMPORT_BYTES:
            raise ValueError("incomplete or oversized import")
        stripped = line.strip()
        if LAST_LINE.fullmatch(stripped):
            if imported and comma_after_previous:
                raise ValueError("trailing comma")
            break
        if imported and not comma_after_previous:
            raise ValueError("missing comma")
        comma_after_previous = stripped.endswith(b",")
        payload = stripped[:-1].rstrip() if comma_after_previous else stripped
        if not payload:
            raise ValueError("empty quote")
        try:
            record = orjson.loads(payload)
        except orjson.JSONDecodeError as exc:
            raise ValueError("invalid quote JSON") from exc
        if not valid_quote(record):
            raise ValueError("invalid quote")
        store.add(record)
        imported += 1
        if imported % 256 == 0:
            await asyncio.sleep(0)

    await check_trailing_whitespace(stream)
    store.seal()
    return imported


async def check_trailing_whitespace(stream) -> None:
    while chunk := await stream.read(65536):
        if chunk.strip():
            raise ValueError("unexpected data after import")


async def health(request: web.Request) -> web.Response:
    return json_reply({"status": "healthy"})


async def set_source(request: web.Request) -> web.Response:
    try:
        body = await read_bounded(request, 8192)
        if len(body) > 8192:
            raise ValueError("source body is too large")
        document = orjson.loads(body)
        url = document.get("url") if isinstance(document, dict) else None
        if not isinstance(url, str):
            raise ValueError("url is required")
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password:
            raise ValueError("invalid catalog URL")
    except (ValueError, orjson.JSONDecodeError):
        return json_reply({"detail": "invalid source"}, 400)
    state: Showcase = request.app["showcase"]
    normalized = url.rstrip("/")
    state.set_source(normalized)
    return json_reply({"source": normalized})


async def import_snapshot(request: web.Request) -> web.Response:
    state: Showcase = request.app["showcase"]
    async with state.import_lock:
        started_at = time.monotonic()
        start_version = state.version
        candidate = PackedStore()
        try:
            imported = await parse_import(request, candidate)
        except (ValueError, UnicodeError, OverflowError, orjson.JSONDecodeError):
            return json_reply({"detail": "invalid import body"}, 400)

        old_store = state.store
        dropped = 0
        if old_store is not None:
            for position, quote_id in enumerate(old_store.ids()):
                if not candidate.contains(quote_id):
                    dropped += 1
                if position % 4096 == 0:
                    await asyncio.sleep(0)

        state.store = candidate
        state.snapshot_at = started_at
        for quote_id, entry in list(state.overlay.items()):
            if entry.version <= start_version:
                state._remove_overlay(quote_id)
        state.tombstones = {key: version for key, version in state.tombstones.items() if version > start_version}
        state.invalidated = {key: version for key, version in state.invalidated.items() if version > start_version}
        state.refresh_after.clear()
        del old_store
        release_unused_memory()
        return json_reply({"imported": imported, "dropped": dropped})


async def put_quote(request: web.Request) -> web.Response:
    quote_id = request.match_info["quote_id"]
    try:
        body = await read_bounded(request, 262144)
        if len(body) > 262144:
            raise ValueError("too large")
        document = orjson.loads(body)
    except (ValueError, orjson.JSONDecodeError):
        return json_reply({"detail": "invalid body"}, 400)
    author = document.get("author") if isinstance(document, dict) else None
    text = document.get("text") if isinstance(document, dict) else None
    if not isinstance(author, str) or not isinstance(text, str) or not (1 <= len(author) <= 200) or not (1 <= len(text) <= 16384):
        return json_reply({"detail": "invalid quote"}, 422)

    record = {"id": quote_id, "author": author, "text": text}
    state: Showcase = request.app["showcase"]
    state.write_versions[quote_id] = state._next_version()
    state._put_overlay(quote_id, orjson.dumps(record), direct=True)
    return json_reply(record)


async def delete_quote(request: web.Request) -> web.Response:
    quote_id = request.match_info["quote_id"]
    state: Showcase = request.app["showcase"]
    raw, _ = state._local_raw(quote_id)
    if raw is None:
        if quote_id in state.tombstones:
            return json_reply({"detail": "not found"}, 404)
        task = state._lookup(quote_id)
        if task is None:
            return json_reply({"detail": "catalog unavailable"}, 503)
        try:
            status, _ = await asyncio.wait_for(asyncio.shield(task), timeout=2.6)
        except asyncio.TimeoutError:
            status = 503
        if status == 404:
            return json_reply({"detail": "not found"}, 404)
        if status == 409:
            raw, _ = state._local_raw(quote_id)
        elif status != 200:
            return json_reply({"detail": "catalog unavailable"}, 503)
        if status == 409 and raw is None:
            return json_reply({"detail": "not found"}, 404)
    state._remove_overlay(quote_id)
    state.invalidated.pop(quote_id, None)
    version = state._next_version()
    state.write_versions[quote_id] = version
    state.tombstones[quote_id] = version
    return json_reply({"deleted": True})


async def get_quote(request: web.Request) -> web.Response:
    state: Showcase = request.app["showcase"]
    return await state.reader(request.match_info["quote_id"])


async def get_stats(request: web.Request) -> web.Response:
    state: Showcase = request.app["showcase"]
    return json_reply(state.stats())


async def session_lifecycle(app: web.Application):
    state: Showcase = app["showcase"]
    connector = aiohttp.TCPConnector(limit=4, limit_per_host=4, ttl_dns_cache=10)
    state.session = aiohttp.ClientSession(connector=connector)
    yield
    for task in state.inflight.values():
        task.cancel()
    if state.session is not None:
        await state.session.close()


def create_app() -> web.Application:
    app = web.Application(client_max_size=MAX_IMPORT_BYTES)
    app["showcase"] = Showcase()
    app.cleanup_ctx.append(session_lifecycle)
    app.router.add_get("/health", health)
    app.router.add_post("/catalog/source", set_source)
    app.router.add_post("/import", import_snapshot)
    app.router.add_put("/catalog/{quote_id}", put_quote)
    app.router.add_delete("/catalog/{quote_id}", delete_quote)
    app.router.add_get("/quotes/{quote_id}", get_quote)
    app.router.add_get("/stats", get_stats)
    return app


if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), access_log=None)
