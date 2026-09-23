"""Freshness behavior when a catalog read cannot complete."""

import time

import orjson
import pytest

from packed_store import PackedStore
from showcase import Showcase


@pytest.mark.asyncio
async def test_local_snapshot_is_not_served_after_sixty_seconds():
    state = Showcase()
    store = PackedStore()
    store.add({"id": "known", "author": "Author", "text": "Text"})
    store.seal()
    state.store = store
    state.source = "http://catalog.example"
    state.breaker_until = time.monotonic() + 100

    state.snapshot_at = time.monotonic() - 59
    response = await state.reader("known")
    assert response.status == 200
    assert response.headers["X-Source"] == "LOCAL"

    state.snapshot_at = time.monotonic() - 61
    response = await state.reader("known")
    assert response.status == 503
    assert orjson.loads(response.body) == {"detail": "catalog unavailable"}


@pytest.mark.asyncio
async def test_missing_quote_is_not_turned_into_local_not_found_during_outage():
    state = Showcase()
    state.source = "http://catalog.example"
    state.breaker_until = time.monotonic() + 100

    response = await state.reader("new")
    assert response.status == 503
    assert "new" not in state.invalidated
    assert "new" not in state.tombstones


@pytest.mark.asyncio
async def test_direct_editorial_put_remains_authoritative_without_catalog_read():
    state = Showcase()
    state.source = "http://catalog.example"
    state.breaker_until = time.monotonic() + 100
    state._put_overlay("editorial", b'{"id":"editorial","author":"A","text":"Current"}', direct=True)
    state.overlay["editorial"].updated_at = time.monotonic() - 3600

    response = await state.reader("editorial")
    assert response.status == 200
    assert response.headers["X-Source"] == "LOCAL"
    assert state.catalog_reads == 0
