"""Black-box integration checks for the quote showcase HTTP contract."""

import asyncio
import http.client
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from collections import Counter
from pathlib import Path

import aiohttp
from aiohttp import web
import pytest
import pytest_asyncio


ROOT = Path(__file__).resolve().parents[1]
HOST = "127.0.0.1"


def _id() -> str:
    return f"it-{uuid.uuid4().hex}"


def _quote(quote_id: str, text: str = "A quote") -> dict:
    return {"id": quote_id, "author": "Test Author", "text": text}


def _snapshot(quotes: list[dict]) -> bytes:
    rows = [
        json.dumps(quote, ensure_ascii=False, separators=(",", ":"))
        for quote in quotes
    ]
    lines = "".join(
        row + ("," if index < len(rows) - 1 else "") + "\n"
        for index, row in enumerate(rows)
    )
    return ('{"quotes":[\n' + lines + ']}').encode("utf-8")


def _process_log(log) -> str:
    log.flush()
    log.seek(0)
    return log.read().decode("utf-8", errors="replace")[-4000:]


@pytest.fixture(scope="session")
def service():
    script = ROOT / "showcase.py"
    if not script.is_file():
        pytest.fail(f"Service entry point is missing: {script}")

    port = int(os.environ.get("SHOWCASE_TEST_PORT", "8000"))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        try:
            probe.bind((HOST, port))
        except OSError as exc:
            pytest.fail(f"Test port {port} is already in use: {exc}")

    env = os.environ.copy()
    env["PORT"] = str(port)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    with tempfile.TemporaryFile(mode="w+b") as log:
        process = subprocess.Popen(
            [sys.executable, "-u", str(script)],
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 15
            health_before_source = None
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    pytest.fail(
                        f"showcase.py exited with {process.returncode}:\n"
                        f"{_process_log(log)}"
                    )
                connection = http.client.HTTPConnection(HOST, port, timeout=0.5)
                try:
                    connection.request("GET", "/health")
                    response = connection.getresponse()
                    health_before_source = (response.status, response.read())
                    break
                except (OSError, http.client.HTTPException):
                    time.sleep(0.05)
                finally:
                    connection.close()

            if health_before_source is None:
                pytest.fail(f"showcase.py did not start:\n{_process_log(log)}")
            yield {
                "url": f"http://{HOST}:{port}",
                "health_before_source": health_before_source,
            }
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


@pytest_asyncio.fixture
async def client(service):
    timeout = aiohttp.ClientTimeout(total=3.2)
    async with aiohttp.ClientSession(base_url=service["url"], timeout=timeout) as session:
        yield session


class FakeCatalog:
    def __init__(self):
        self.quotes = {}
        self.busy = set()
        self.retry_after = "1"
        self.delays = {}
        self.hits = []
        self.url = None
        self._runner = None

    async def __aenter__(self):
        app = web.Application()
        app.router.add_get("/quote/{quote_id}", self._get_quote)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, HOST, 0)
        await site.start()
        self.url = f"http://{HOST}:{self._runner.addresses[0][1]}"
        return self

    async def __aexit__(self, *_):
        await self._runner.cleanup()

    async def _get_quote(self, request):
        quote_id = request.match_info["quote_id"]
        self.hits.append((quote_id, time.monotonic()))
        if quote_id in self.delays:
            await asyncio.sleep(self.delays[quote_id])
        if quote_id in self.busy:
            return web.json_response(
                {"detail": "busy"},
                status=503,
                headers={"Retry-After": self.retry_after},
            )
        quote = self.quotes.get(quote_id)
        if quote is None:
            return web.json_response({"detail": "not found"}, status=404)
        return web.json_response(quote)

    def reads_for(self, quote_id: str) -> int:
        return Counter(hit_id for hit_id, _ in self.hits)[quote_id]


async def _request(client, method: str, path: str, **kwargs):
    async with client.request(method, path, **kwargs) as response:
        return response.status, await response.json(content_type=None), response.headers


async def _source(client, catalog: FakeCatalog):
    status, body, _ = await _request(
        client, "POST", "/catalog/source", json={"url": catalog.url}
    )
    assert status == 200
    assert body == {"source": catalog.url}


async def _import(client, quotes: list[dict]):
    return await _request(
        client,
        "POST",
        "/import",
        data=_snapshot(quotes),
        headers={"Content-Type": "application/json"},
    )


async def _stats(client) -> dict:
    status, body, _ = await _request(client, "GET", "/stats")
    assert status == 200
    return body


def test_health_before_source(service):
    status, raw_body = service["health_before_source"]
    assert status == 200
    assert json.loads(raw_body) == {"status": "healthy"}


@pytest.mark.asyncio
async def test_last_catalog_source_wins(client):
    quote_id = _id()
    async with FakeCatalog() as first, FakeCatalog() as second:
        second.quotes[quote_id] = _quote(quote_id, "From the second source")
        await _source(client, first)
        status, _, _ = await _request(client, "GET", f"/quotes/{quote_id}")
        assert status == 404
        assert first.reads_for(quote_id) == 1

        await _source(client, second)
        status, body, headers = await _request(client, "GET", f"/quotes/{quote_id}")
        assert status == 200
        assert body == second.quotes[quote_id]
        assert headers["X-Source"] == "CATALOG"
        assert second.reads_for(quote_id) == 1
        assert first.reads_for(quote_id) == 1


@pytest.mark.asyncio
async def test_import_is_atomic_and_reports_dropped(client):
    kept_id, removed_id, added_id = _id(), _id(), _id()
    old_kept = _quote(kept_id, "Old text")
    new_kept = _quote(kept_id, "New text")
    added = _quote(added_id, "Added")
    async with FakeCatalog() as catalog:
        await _source(client, catalog)
        await _import(client, [])
        status, body, _ = await _import(client, [old_kept, _quote(removed_id)])
        assert (status, body) == (200, {"imported": 2, "dropped": 0})
        before = await _stats(client)
        assert before["catalog"] == 2

        for malformed in (
            b'{"quotes":[\n{"id":"broken",\n]}',
            _snapshot([{"id": _id(), "author": "Missing text"}]),
        ):
            status, _, _ = await _request(
                client,
                "POST",
                "/import",
                data=malformed,
                headers={"Content-Type": "application/json"},
            )
            assert status == 400
        status, body, _ = await _request(client, "GET", f"/quotes/{kept_id}")
        assert status == 200
        assert body == old_kept
        status, body, _ = await _request(client, "GET", f"/quotes/{removed_id}")
        assert status == 200

        status, body, _ = await _import(client, [new_kept, added])
        assert (status, body) == (200, {"imported": 2, "dropped": 1})
        after = await _stats(client)
        assert after["catalog"] == 2
        assert after["evictions"] == before["evictions"]
        status, body, _ = await _request(client, "GET", f"/quotes/{kept_id}")
        assert (status, body) == (200, new_kept)
        status, body, _ = await _request(client, "GET", f"/quotes/{added_id}")
        assert (status, body) == (200, added)
        status, _, _ = await _request(client, "GET", f"/quotes/{removed_id}")
        assert status == 404


@pytest.mark.asyncio
async def test_put_delete_and_validation(client):
    quote_id = _id()
    async with FakeCatalog() as catalog:
        catalog.quotes[quote_id] = _quote(quote_id, "Still in the catalog")
        await _source(client, catalog)
        for invalid in (
            {"author": "", "text": "ok"},
            {"author": "a" * 201, "text": "ok"},
            {"author": "ok", "text": ""},
            {"author": "ok", "text": "x" * 16385},
        ):
            status, _, _ = await _request(
                client, "PUT", f"/catalog/{quote_id}", json=invalid
            )
            assert status == 422

        status, body, _ = await _request(
            client,
            "PUT",
            f"/catalog/{quote_id}",
            json={"author": "Editor", "text": "Published"},
        )
        expected = {"id": quote_id, "author": "Editor", "text": "Published"}
        assert (status, body) == (200, expected)
        status, body, headers = await _request(client, "GET", f"/quotes/{quote_id}")
        assert (status, body) == (200, expected)
        assert headers["X-Source"] == "LOCAL"
        assert catalog.reads_for(quote_id) == 0

        status, body, _ = await _request(client, "DELETE", f"/catalog/{quote_id}")
        assert (status, body) == (200, {"deleted": True})
        status, _, _ = await _request(client, "GET", f"/quotes/{quote_id}")
        assert status == 404
        assert catalog.reads_for(quote_id) == 0
        status, _, _ = await _request(client, "DELETE", f"/catalog/{quote_id}")
        assert status == 404

        await _import(client, [catalog.quotes[quote_id]])
        status, body, _ = await _request(client, "GET", f"/quotes/{quote_id}")
        assert (status, body) == (200, catalog.quotes[quote_id])


@pytest.mark.asyncio
async def test_delete_quote_known_only_to_catalog(client):
    quote_id = _id()
    async with FakeCatalog() as catalog:
        catalog.quotes[quote_id] = _quote(quote_id)
        await _source(client, catalog)

        status, body, _ = await _request(client, "DELETE", f"/catalog/{quote_id}")
        assert (status, body) == (200, {"deleted": True})
        assert catalog.reads_for(quote_id) == 1

        status, _, _ = await _request(client, "GET", f"/quotes/{quote_id}")
        assert status == 404
        assert catalog.reads_for(quote_id) == 1

        status, body, _ = await _import(client, [catalog.quotes[quote_id]])
        assert status == 200
        assert body["imported"] == 1
        status, body, headers = await _request(client, "GET", f"/quotes/{quote_id}")
        assert (status, body) == (200, catalog.quotes[quote_id])
        assert headers["X-Source"] == "LOCAL"


@pytest.mark.asyncio
async def test_get_preserves_full_fields_and_source_header(client):
    local_id, catalog_id = _id(), _id()
    local = {
        **_quote(local_id, "From snapshot"),
        "tags": ["history", "wisdom"],
        "year": 170,
        "lang": "ru",
        "rights": {"region": "global"},
    }
    remote = {
        **_quote(catalog_id, "From catalog"),
        "tags": ["new"],
        "year": None,
        "lang": "en",
    }
    async with FakeCatalog() as catalog:
        catalog.quotes[catalog_id] = remote
        await _source(client, catalog)
        await _import(client, [local])
        status, body, headers = await _request(client, "GET", f"/quotes/{local_id}")
        assert (status, body) == (200, local)
        assert headers["X-Source"] == "LOCAL"
        assert catalog.reads_for(local_id) == 0

        status, body, headers = await _request(client, "GET", f"/quotes/{catalog_id}")
        assert (status, body) == (200, remote)
        assert headers["X-Source"] == "CATALOG"
        assert catalog.reads_for(catalog_id) == 1


@pytest.mark.asyncio
async def test_missing_quote_is_not_negative_cached(client):
    quote_id = _id()
    async with FakeCatalog() as catalog:
        await _source(client, catalog)
        status, body, _ = await _request(client, "GET", f"/quotes/{quote_id}")
        assert (status, body) == (404, {"detail": "not found"})
        assert catalog.reads_for(quote_id) == 1

        catalog.quotes[quote_id] = _quote(quote_id, "Created after the miss")
        status, body, headers = await _request(client, "GET", f"/quotes/{quote_id}")
        assert (status, body) == (200, catalog.quotes[quote_id])
        assert headers["X-Source"] == "CATALOG"
        assert catalog.reads_for(quote_id) == 2


@pytest.mark.asyncio
async def test_catalog_503_respects_retry_after(client):
    quote_id = _id()
    async with FakeCatalog() as catalog:
        catalog.busy.add(quote_id)
        await _source(client, catalog)
        for _ in range(3):
            started = time.monotonic()
            status, _, _ = await _request(client, "GET", f"/quotes/{quote_id}")
            assert status in (404, 503)
            assert time.monotonic() - started < 3.2

        hits = [at for hit_id, at in catalog.hits if hit_id == quote_id]
        assert hits, "The showcase never consulted the catalog"
        assert all(later - earlier >= 0.85 for earlier, later in zip(hits, hits[1:]))

        catalog.busy.remove(quote_id)
        catalog.quotes[quote_id] = _quote(quote_id, "Available again")
        await asyncio.sleep(max(0, 1.1 - (time.monotonic() - hits[-1])))
        for _ in range(4):
            status, body, headers = await _request(client, "GET", f"/quotes/{quote_id}")
            if status == 200:
                break
            await asyncio.sleep(0.5)
        assert (status, body) == (200, catalog.quotes[quote_id])
        assert headers["X-Source"] == "CATALOG"


@pytest.mark.asyncio
async def test_busy_quote_does_not_block_other_ids_or_lose_retry_after(client):
    busy_id, good_id = _id(), _id()
    async with FakeCatalog() as catalog:
        catalog.busy.add(busy_id)
        catalog.retry_after = "120"
        catalog.quotes[good_id] = _quote(good_id)
        catalog.delays[good_id] = 0.1
        await _source(client, catalog)

        busy = asyncio.create_task(_request(client, "GET", f"/quotes/{busy_id}"))
        good = asyncio.create_task(_request(client, "GET", f"/quotes/{good_id}"))
        (busy_status, _, _), (good_status, good_body, _) = await asyncio.gather(busy, good)
        assert busy_status == 503
        assert (good_status, good_body) == (200, catalog.quotes[good_id])

        status, _, _ = await _request(client, "GET", f"/quotes/{busy_id}")
        assert status == 503
        assert catalog.reads_for(busy_id) == 1

        fresh_id = _id()
        catalog.quotes[fresh_id] = _quote(fresh_id)
        status, body, _ = await _request(client, "GET", f"/quotes/{fresh_id}")
        assert (status, body) == (200, catalog.quotes[fresh_id])


@pytest.mark.asyncio
async def test_concurrent_catalog_reads_do_not_drop_ready_quotes(client):
    quote_ids = [_id() for _ in range(25)]
    async with FakeCatalog() as catalog:
        for quote_id in quote_ids:
            catalog.quotes[quote_id] = _quote(quote_id)
            catalog.delays[quote_id] = 0.1
        await _source(client, catalog)
        results = await asyncio.gather(*(
            _request(client, "GET", f"/quotes/{quote_id}") for quote_id in quote_ids
        ))
        assert all(status == 200 for status, _, _ in results)
        assert all(catalog.reads_for(quote_id) == 1 for quote_id in quote_ids)


@pytest.mark.asyncio
async def test_full_length_unicode_put_and_large_snapshot_field(client):
    put_id, imported_id = _id(), _id()
    unicode_text = "\U0001f600" * 16384
    status, body, _ = await _request(
        client, "PUT", f"/catalog/{put_id}", json={"author": "\u0420\u0435\u0434\u0430\u043a\u0442\u043e\u0440", "text": unicode_text}
    )
    assert (status, body["text"]) == (200, unicode_text)

    imported = {**_quote(imported_id), "metadata": "m" * 150000}
    status, body, _ = await _import(client, [imported])
    assert (status, body["imported"]) == (200, 1)
    status, body, headers = await _request(client, "GET", f"/quotes/{imported_id}")
    assert (status, body) == (200, imported)
    assert headers["X-Source"] == "LOCAL"


@pytest.mark.asyncio
async def test_stats_report_request_deltas(client):
    local_id, catalog_id = _id(), _id()
    async with FakeCatalog() as catalog:
        catalog.quotes[catalog_id] = _quote(catalog_id)
        await _source(client, catalog)
        status, _, _ = await _request(
            client,
            "PUT",
            f"/catalog/{local_id}",
            json={"author": "Editor", "text": "Local"},
        )
        assert status == 200
        before = await _stats(client)
        for _ in range(2):
            status, _, headers = await _request(client, "GET", f"/quotes/{local_id}")
            assert status == 200
            assert headers["X-Source"] == "LOCAL"
        status, _, headers = await _request(client, "GET", f"/quotes/{catalog_id}")
        assert status == 200
        assert headers["X-Source"] == "CATALOG"
        after = await _stats(client)

        for field in (
            "served_local",
            "served_from_catalog",
            "catalog_reads",
            "evictions",
            "local",
            "bytes",
            "catalog",
        ):
            assert isinstance(after[field], int) and after[field] >= 0
        assert after["served_local"] - before["served_local"] == 2
        assert after["served_from_catalog"] - before["served_from_catalog"] == 1
        assert after["catalog_reads"] - before["catalog_reads"] == 1
        assert after["evictions"] >= before["evictions"]
        assert catalog.reads_for(catalog_id) == 1


@pytest.mark.asyncio
async def test_health_and_old_snapshot_remain_available_during_large_import(client):
    marker_id = _id()
    marker = _quote(marker_id, "Before import")
    async with FakeCatalog() as catalog:
        await _source(client, catalog)
        await _import(client, [])
        status, _, _ = await _import(client, [marker])
        assert status == 200

        prefix = uuid.uuid4().hex
        quotes = [
            _quote(f"bulk-{prefix}-{number}", "x" * 128)
            for number in range(30000)
        ]
        payload = _snapshot(quotes)
        split_at = payload.find(b"\n", len(payload) // 2) + 1
        paused = asyncio.Event()
        release = asyncio.Event()

        async def upload():
            yield payload[:split_at]
            paused.set()
            await release.wait()
            yield payload[split_at:]

        request_task = asyncio.create_task(
            client.post(
                "/import",
                data=upload(),
                headers={"Content-Type": "application/json"},
            )
        )
        response = None
        try:
            await asyncio.wait_for(paused.wait(), timeout=5)
            started = time.monotonic()
            status, body, _ = await _request(client, "GET", "/health")
            assert (status, body) == (200, {"status": "healthy"})
            assert time.monotonic() - started < 1.0
            status, body, headers = await _request(client, "GET", f"/quotes/{marker_id}")
            assert (status, body) == (200, marker)
            assert headers["X-Source"] == "LOCAL"

            release.set()
            started = time.monotonic()
            status, body, _ = await _request(client, "GET", "/health")
            assert (status, body) == (200, {"status": "healthy"})
            assert time.monotonic() - started < 1.0
            response = await asyncio.wait_for(request_task, timeout=30)
            assert response.status == 200
            assert await response.json() == {"imported": 30000, "dropped": 1}
        finally:
            release.set()
            if response is not None:
                response.release()
            if not request_task.done():
                request_task.cancel()
                await asyncio.gather(request_task, return_exceptions=True)

        status, _, _ = await _request(client, "GET", f"/quotes/{marker_id}")
        assert status == 404
        status, body, _ = await _request(client, "GET", f"/quotes/{quotes[-1]['id']}")
        assert (status, body) == (200, quotes[-1])
