"""Concurrency test for the availability change feed.

Runs a real uvicorn server and interleaves uploads (incl. same-number
re-uploads), leases, reclaims, and concurrent cursor polling. Afterwards it
asserts that:

* the feed's seq values are a gap-free 1..N sequence with no duplicates,
  regardless of the page size used while polling;
* every generation's lifecycle is one of the legal transition shapes;
* interleaved uploads/reclaims are reflected exactly once and in order;
* no request fails with a 5xx.
"""

from __future__ import annotations

import hashlib
import os
import random
import threading
import time

import httpx
import uvicorn

from app.config import Config
from app.main import create_app

PORT = 8124
BASE = f"http://127.0.0.1:{PORT}"


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _wait_up(client: httpx.Client, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if client.get("/health").status_code == 200:
                return
        except httpx.TransportError:
            time.sleep(0.05)
    raise RuntimeError("server did not start")


def test_feed_consistency_under_concurrent_traffic(tmp_path):
    cfg = Config(
        db_path=str(tmp_path / "cache.db"),
        storage_dir=str(tmp_path / "storage"),
        token_ttl=2.0,
        orphan_grace=0.0,
        sweep_interval=0.0,
    )
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(cfg), host="127.0.0.1", port=PORT, log_level="error"
        )
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    errors: list[str] = []
    stop = threading.Event()
    polled: list[dict] = []

    try:
        with httpx.Client(base_url=BASE, timeout=15) as client:
            _wait_up(client)

        def worker(wid: int) -> None:
            rng = random.Random(1000 + wid)
            with httpx.Client(base_url=BASE, timeout=15) as client:
                for _ in range(50):
                    number = f"n{rng.randint(1, 6)}"
                    op = rng.choice(
                        ["upload", "reupload", "lease", "reclaim", "reclaim"]
                    )
                    try:
                        if op in ("upload", "reupload"):
                            data = os.urandom(rng.randint(1, 1024))
                            r = client.put(
                                f"/artifacts/{number}",
                                content=data,
                                params={"sha256": _sha(data)},
                            )
                        elif op == "lease":
                            r = client.post(f"/artifacts/{number}/tokens")
                            if r.status_code == 201:
                                token = r.json()["token"]
                                client.post(f"/tokens/{token}/release")
                        else:
                            r = client.post(
                                "/reclaim",
                                json={"target_bytes": rng.randint(0, 4096)},
                            )
                        if r.status_code >= 500:
                            errors.append(
                                f"worker {wid} {op}: {r.status_code} {r.text[:150]}"
                            )
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"worker {wid} {op}: {exc!r}")

        def poller() -> None:
            """Continuously resume from the last cursor with varying pages."""
            cursor = 0
            rng = random.Random(42)
            with httpx.Client(base_url=BASE, timeout=15) as client:
                while not stop.is_set():
                    limit = rng.choice([1, 2, 3, 7])
                    r = client.get(
                        "/availability/changes",
                        params={"cursor": cursor, "limit": limit},
                    )
                    if r.status_code != 200:
                        errors.append(f"poller: {r.status_code} {r.text[:150]}")
                        return
                    body = r.json()
                    for ch in body["changes"]:
                        polled.append(ch)
                    cursor = body["next_cursor"]
                    if not body["has_more"]:
                        time.sleep(0.005)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
        poll_thread = threading.Thread(target=poller, daemon=True)
        poll_thread.start()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        stop.set()
        poll_thread.join(timeout=10)

        assert not errors, "\n".join(errors[:20])

        # Drain whatever the poller had not caught yet.
        with httpx.Client(base_url=BASE, timeout=15) as client:
            cursor = polled[-1]["seq"] if polled else 0
            while True:
                body = client.get(
                    "/availability/changes",
                    params={"cursor": cursor, "limit": 5},
                ).json()
                polled.extend(body["changes"])
                cursor = body["next_cursor"]
                if not body["has_more"]:
                    break
            # The server's own view of the total event count.
            total = client.get(
                "/availability/changes", params={"cursor": 0, "limit": 1000}
            ).json()

        # 1) gap-free, ordered, duplicate-free cursor sequence.
        seqs = [e["seq"] for e in polled]
        assert seqs == list(range(1, len(seqs) + 1)), "feed has gaps or dupes"
        assert len(polled) == len(total["changes"])

        # 2) per-generation lifecycle obeys the transition rules: exactly
        #    one 'available' and it is first; zero or more 'invalidated'
        #    (reclaim selection and/or superseding upload); at most one
        #    'released', and if present it is the terminal transition.
        lifecycles: dict[tuple, list[str]] = {}
        for e in polled:
            lifecycles.setdefault((e["number"], e["generation"]), []).append(
                e["status"]
            )
        for key, shape in lifecycles.items():
            assert shape[0] == "available", f"{key}: {shape}"
            assert shape.count("available") == 1, f"{key}: {shape}"
            assert shape.count("released") <= 1, f"{key}: {shape}"
            if "released" in shape:
                assert shape[-1] == "released", f"{key}: {shape}"
            for status in shape[1:]:
                assert status in ("invalidated", "released"), f"{key}: {shape}"

        # 3) every currently-ready artifact's newest generation is available
        #    and not later invalidated in the feed.
        with httpx.Client(base_url=BASE, timeout=15) as client:
            for meta in client.get("/artifacts").json():
                key = (meta["number"], meta["generation"])
                assert lifecycles[key][-1] == "available"
    finally:
        stop.set()
        server.should_exit = True
        thread.join(timeout=15)
