"""Compact, read-only snapshot storage with random access by quote ID."""

from __future__ import annotations

import struct
from collections import OrderedDict
from typing import Iterator

import orjson
import zstandard as zstd


BLOCK_SIZE = 128 * 1024
SLOT = struct.Struct("<QIII")
ID_LENGTH = struct.Struct("<H")
MAX_LOAD = 0.68


class PackedStore:
    """Store a snapshot as compressed blocks and a compact open-addressed index.

    The index holds only hash, block number, offset and length. IDs are checked
    against the JSON record on hash matches, so hash collisions stay correct.
    """

    __slots__ = (
        "_blocks",
        "_cache",
        "_compressor",
        "_decompressor",
        "_id_log",
        "_index",
        "_open_block",
        "_open_size",
        "_payload_bytes",
        "_size",
    )

    def __init__(self) -> None:
        self._blocks: list[bytes] = []
        self._cache: OrderedDict[int, bytes] = OrderedDict()
        self._compressor = zstd.ZstdCompressor(level=1)
        self._decompressor = zstd.ZstdDecompressor()
        self._id_log = bytearray()
        self._index = bytearray(SLOT.size * 1024)
        self._open_block = bytearray(BLOCK_SIZE)
        self._open_size = 0
        self._payload_bytes = 0
        self._size = 0

    def __len__(self) -> int:
        return self._size

    @property
    def bytes_used(self) -> int:
        if self._size == 0:
            return 0
        return self._payload_bytes + len(self._index) + len(self._id_log)

    @staticmethod
    def _fingerprint(quote_id: str) -> int:
        return (hash(quote_id) & 0xFFFFFFFFFFFFFFFF) or 1

    def _find_slot(self, quote_id: str, fingerprint: int) -> tuple[int, bool]:
        capacity = len(self._index) // SLOT.size
        slot = fingerprint & (capacity - 1)
        while True:
            position = slot * SLOT.size
            known_hash, block_no, offset, size = SLOT.unpack_from(self._index, position)
            if known_hash == 0:
                return position, False
            if known_hash == fingerprint:
                record = self._read(block_no, offset, size)
                if orjson.loads(record)["id"] == quote_id:
                    return position, True
            slot = (slot + 1) & (capacity - 1)

    def _grow(self) -> None:
        previous = self._index
        self._index = bytearray(len(previous) * 2)
        capacity = len(self._index) // SLOT.size
        for position in range(0, len(previous), SLOT.size):
            fingerprint, block_no, offset, size = SLOT.unpack_from(previous, position)
            if fingerprint == 0:
                continue
            slot = fingerprint & (capacity - 1)
            while SLOT.unpack_from(self._index, slot * SLOT.size)[0] != 0:
                slot = (slot + 1) & (capacity - 1)
            SLOT.pack_into(self._index, slot * SLOT.size, fingerprint, block_no, offset, size)

    def add(self, record: dict) -> None:
        quote_id = record["id"]
        encoded_id = quote_id.encode("utf-8")
        if len(encoded_id) > 65535:
            raise ValueError("quote ID is too long")

        fingerprint = self._fingerprint(quote_id)
        position, exists = self._find_slot(quote_id, fingerprint)
        if exists:
            raise ValueError("duplicate quote ID")

        raw = orjson.dumps(record)
        if len(raw) > BLOCK_SIZE:
            raise ValueError("quote is too large")
        if self._open_size + len(raw) > BLOCK_SIZE:
            self._seal_block()

        block_no = len(self._blocks)
        offset = self._open_size
        self._open_block[offset : offset + len(raw)] = raw
        self._open_size += len(raw)
        SLOT.pack_into(self._index, position, fingerprint, block_no, offset, len(raw))
        self._id_log.extend(ID_LENGTH.pack(len(encoded_id)))
        self._id_log.extend(encoded_id)
        self._size += 1

        if self._size > (len(self._index) // SLOT.size) * MAX_LOAD:
            self._grow()

    def _seal_block(self) -> None:
        if self._open_size == 0:
            return
        raw = bytes(memoryview(self._open_block)[: self._open_size])
        compressed = self._compressor.compress(raw)
        self._blocks.append(compressed)
        self._payload_bytes += len(compressed)
        self._open_size = 0

    def seal(self) -> None:
        self._seal_block()
        self._open_block = bytearray()

    def _read(self, block_no: int, offset: int, size: int) -> bytes:
        if block_no == len(self._blocks):
            return bytes(memoryview(self._open_block)[offset : offset + size])
        block = self._cache.get(block_no)
        if block is None:
            block = self._decompressor.decompress(self._blocks[block_no])
            self._cache[block_no] = block
            if len(self._cache) > 2:
                self._cache.popitem(last=False)
        else:
            self._cache.move_to_end(block_no)
        return block[offset : offset + size]

    def get_raw(self, quote_id: str) -> bytes | None:
        fingerprint = self._fingerprint(quote_id)
        position, exists = self._find_slot(quote_id, fingerprint)
        if not exists:
            return None
        _, block_no, offset, size = SLOT.unpack_from(self._index, position)
        return self._read(block_no, offset, size)

    def contains(self, quote_id: str) -> bool:
        _, exists = self._find_slot(quote_id, self._fingerprint(quote_id))
        return exists

    def ids(self) -> Iterator[str]:
        cursor = 0
        while cursor < len(self._id_log):
            size = ID_LENGTH.unpack_from(self._id_log, cursor)[0]
            cursor += ID_LENGTH.size
            yield self._id_log[cursor : cursor + size].decode("utf-8")
            cursor += size
