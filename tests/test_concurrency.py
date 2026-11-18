"""Concurrency / interleaving tests against a real uvicorn server.

Invariants checked under concurrent acquire / renew / release / upload /
reclaim traffic:

* no request ever fails with a 5xx;
* every successful download's body matches its X-Sha256 header (the server
  also verifies integrity before serving);
* an artifact with a continuously-held token is never reclaimed;
* after the storm, every artifact listed as downloadable really downloads.
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

PORT = 8123
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


def test_concurrent_interleaving(tmp_path):
    cfg = Config(
        db_path=str(tmp_path / "cache.db"),
        storage_dir=str(tmp_path / "storage"),
        token_ttl=2.0,
        orphan_grace=0.0,
        sweep_interval=0.0,
    )
    server = uvicorn.Server(
        uvicorn.Config(create_app(cfg), host="127.0.0.1", port=PORT, log_level="error")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    errors: list[str] = []
    stop = threading.Event()

    try:
        with httpx.Client(base_url=BASE, timeout=15) as client:
            _wait_up(client)
            # Pinned artifact: a token is kept alive for the whole storm, so
            # this artifact must survive every reclaim.
            pinned = os.urandom(64)
            r = client.put("/artifacts/pinned", content=pinned, params={"sha256": _sha(pinned)})
            assert r.status_code == 200
            pinned_token = client.post("/artifacts/pinned/tokens").json()["token"]

        def keepalive() -> None:
            with httpx.Client(base_url=BASE, timeout=15) as client:
                while not stop.is_set():
                    r = client.post(f"/tokens/{pinned_token}/renew")
                    if r.status_code != 200:
                        errors.append(f"pinned renew failed: {r.status_code} {r.text}")
                        return
                    stop.wait(0.5)

        keeper = threading.Thread(target=keepalive, daemon=True)
        keeper.start()

        def worker(wid: int) -> None:
            rng = random.Random(wid)
            with httpx.Client(base_url=BASE, timeout=15) as client:
                for i in range(60):
                    number = f"n{rng.randint(1, 5)}"
                    op = rng.choice(["upload", "download", "lease", "reclaim"])
                    try:
                        if op == "upload":
                            data = os.urandom(rng.randint(1, 2048))
                            r = client.put(
                                f"/artifacts/{number}",
                                content=data,
                                params={"sha256": _sha(data)},
                            )
                        elif op == "download":
                            r = client.post(f"/artifacts/{number}/tokens")
                            if r.status_code == 201:
                                token = r.json()["token"]
                                r = client.get(
                                    f"/artifacts/{number}/content",
                                    params={"token": token},
                                )
                                if r.status_code == 200:
                                    assert _sha(r.content) == r.headers["x-sha256"], (
                                        "download body/hash mismatch"
                                    )
                        elif op == "lease":
                            r = client.post(f"/artifacts/{number}/tokens")
                            if r.status_code == 201:
                                token = r.json()["token"]
                                client.post(f"/tokens/{token}/renew")
                                r = client.post(f"/tokens/{token}/release")
                        else:
                            r = client.post(
                                "/reclaim",
                                json={"target_bytes": rng.randint(0, 4096)},
                            )
                        if r.status_code >= 500:
                            errors.append(
                                f"worker {wid} op {op}: {r.status_code} {r.text[:200]}"
                            )
                    except Exception as exc:  # noqa: BLE001 - reported below
                        errors.append(f"worker {wid} op {op}: {exc!r}")

        workers = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for w in workers:
            w.start()
        for w in workers:
            w.join()
        stop.set()
        keeper.join()

        assert not errors, "\n".join(errors[:20])

        with httpx.Client(base_url=BASE, timeout=15) as client:
            # The pinned, continuously-occupied artifact survived everything.
            r = client.get("/artifacts/pinned/content", params={"token": pinned_token})
            assert r.status_code == 200 and r.content == pinned

            # Final consistency: whatever the server lists as downloadable
            # must actually download with a fresh token.
            for meta in client.get("/artifacts").json():
                assert meta["downloadable"] is True or meta["state"] != "ready"
                if meta["downloadable"]:
                    token = client.post(f"/artifacts/{meta['number']}/tokens").json()["token"]
                    r = client.get(
                        f"/artifacts/{meta['number']}/content", params={"token": token}
                    )
                    assert r.status_code == 200
                    assert _sha(r.content) == meta["sha256"]
    finally:
        stop.set()
        server.should_exit = True
        thread.join(timeout=15)
