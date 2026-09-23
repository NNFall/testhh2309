"""Focused checks for the compact snapshot representation."""

import pytest

from packed_store import PackedStore
import orjson


def test_snapshot_keeps_all_fields_across_compressed_blocks():
    store = PackedStore()
    records = [
        {
            "id": f"q-{number:05d}",
            "author": "Author",
            "text": "line" * (number % 300 + 1),
            "tags": ["t", number],
            "year": None,
        }
        for number in range(1000)
    ]
    for record in records:
        store.add(record)
    store.seal()

    assert len(store) == len(records)
    assert list(store.ids()) == [record["id"] for record in records]
    assert store.get_raw("missing") is None
    for record in (records[0], records[499], records[-1]):
        assert orjson.loads(store.get_raw(record["id"])) == record
    assert store.bytes_used > 0


def test_duplicate_id_rejected_without_changing_count():
    store = PackedStore()
    record = {"id": "same", "author": "A", "text": "first"}
    store.add(record)
    with pytest.raises(ValueError, match="duplicate"):
        store.add({"id": "same", "author": "A", "text": "second"})
    store.seal()

    assert len(store) == 1
    assert orjson.loads(store.get_raw("same")) == record
