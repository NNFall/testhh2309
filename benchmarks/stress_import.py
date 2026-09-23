"""Stream two large snapshots without buffering the payload on the client."""

from __future__ import annotations

import argparse
import asyncio
import base64
import os
import time

import aiohttp
import orjson


async def snapshot(record_count: int, random_bytes: int, id_random_bytes: int, generation: int, last_id: list[str]):
    yield b'{"quotes":[\n'
    buffer = bytearray()
    for number in range(record_count):
        quote_id = f"stress-{number:07d}"
        if id_random_bytes:
            quote_id += "-" + base64.urlsafe_b64encode(os.urandom(id_random_bytes)).decode("ascii")
        record = {
            "id": quote_id,
            "author": "Performance test",
            "text": base64.b64encode(os.urandom(random_bytes)).decode("ascii"),
            "generation": generation,
        }
        if number + 1 == record_count:
            last_id[:] = [quote_id]
        buffer.extend(orjson.dumps(record))
        buffer.extend(b",\n" if number + 1 < record_count else b"\n")
        if len(buffer) >= 64 * 1024:
            yield bytes(buffer)
            buffer.clear()
            await asyncio.sleep(0)
    if buffer:
        yield bytes(buffer)
    yield b"]}\n"


async def health_probe(session: aiohttp.ClientSession, task: asyncio.Task):
    slowest = 0.0
    failures = 0
    while not task.done():
        began = time.monotonic()
        try:
            async with session.get("/health", timeout=aiohttp.ClientTimeout(total=3)) as response:
                if response.status != 200:
                    failures += 1
                await response.read()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            failures += 1
        slowest = max(slowest, time.monotonic() - began)
        await asyncio.sleep(0.2)
    return slowest, failures


async def main(url: str, count: int, random_bytes: int, id_random_bytes: int, repeats: int):
    async with aiohttp.ClientSession(base_url=url, timeout=aiohttp.ClientTimeout(total=None)) as session:
        last_id = []
        for generation in range(repeats):
            began = time.monotonic()
            upload = asyncio.create_task(session.post(
                "/import",
                data=snapshot(count, random_bytes, id_random_bytes, generation, last_id),
                headers={"Content-Type": "application/json"},
            ))
            probe = asyncio.create_task(health_probe(session, upload))
            async with await upload as response:
                result = await response.json()
                status = response.status
            slowest, failures = await probe
            print({
                "generation": generation,
                "http": status,
                "result": result,
                "seconds": round(time.monotonic() - began, 2),
                "slowest_health_seconds": round(slowest, 3),
                "health_failures": failures,
            })
            if status != 200 or result.get("imported") != count or failures:
                raise SystemExit("large import check failed")

        async with session.get(f"/quotes/{last_id[0]}") as response:
            record = await response.json()
            print({"last_record_http": response.status, "generation": record.get("generation")})
            if response.status != 200 or record.get("generation") != repeats - 1:
                raise SystemExit("last record is not current")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--count", type=int, default=340000)
    parser.add_argument("--random-bytes", type=int, default=180)
    parser.add_argument("--id-random-bytes", type=int, default=0)
    parser.add_argument("--repeats", type=int, default=2)
    args = parser.parse_args()
    asyncio.run(main(args.url, args.count, args.random_bytes, args.id_random_bytes, args.repeats))
