"""Functional tests: upload/verify, leases, LRU reclaim, and the required
interleaving / failure scenarios."""

from __future__ import annotations

import hashlib
import os
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Config
from app.main import create_app


def make_config(tmp_path, **overrides) -> Config:
    kw = dict(
        db_path=str(tmp_path / "cache.db"),
        storage_dir=str(tmp_path / "storage"),
        token_ttl=300.0,
        orphan_grace=0.0,
        sweep_interval=0.0,
    )
    kw.update(overrides)
    return Config(**kw)


def make_client(tmp_path, **overrides) -> TestClient:
    return TestClient(create_app(make_config(tmp_path, **overrides)))


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def put(client: TestClient, number: str, data: bytes):
    return client.put(f"/artifacts/{number}", content=data, params={"sha256": sha(data)})


def acquire(client: TestClient, number: str) -> str:
    r = client.post(f"/artifacts/{number}/tokens")
    assert r.status_code == 201, r.text
    return r.json()["token"]


def download(client: TestClient, number: str, token: str):
    return client.get(f"/artifacts/{number}/content", params={"token": token})


# --------------------------------------------------------------------- upload


def test_upload_download_roundtrip(tmp_path):
    with make_client(tmp_path) as c:
        data = b"hello cache" * 500
        r = put(c, "art-1", data)
        assert r.status_code == 200, r.text
        meta = r.json()
        assert meta["generation"] == 1
        assert meta["size"] == len(data)
        assert meta["downloadable"] is True

        token = acquire(c, "art-1")
        r = download(c, "art-1", token)
        assert r.status_code == 200
        assert r.content == data
        assert r.headers["x-sha256"] == sha(data)


def test_upload_rejects_bad_or_missing_sha256(tmp_path):
    with make_client(tmp_path) as c:
        assert c.put("/artifacts/a", content=b"x", params={"sha256": "0" * 64}).status_code == 400
        assert c.put("/artifacts/a", content=b"x").status_code == 400
        assert c.put("/artifacts/a", content=b"x", params={"sha256": "not-hex"}).status_code == 400
        assert c.get("/artifacts/a").status_code == 404  # nothing stored


def test_upload_size_limit(tmp_path):
    with make_client(tmp_path) as c:
        assert put(c, "max", b"x" * (1024 * 1024)).status_code == 200
        assert put(c, "over", b"x" * (1024 * 1024 + 1)).status_code == 413
        assert c.get("/artifacts/over").status_code == 404


def test_upload_validates_number(tmp_path):
    with make_client(tmp_path) as c:
        assert put(c, "bad name", b"x").status_code == 400
        assert put(c, "x" * 200, b"x").status_code == 400
        assert put(c, "ok_name-1.2", b"x").status_code == 200


def test_validate_number_rejects_dot_traversal():
    from app.main import _validate_number

    for bad in ("..", ".", "...", "a/b", "a\\b", ""):
        with pytest.raises(Exception):
            _validate_number(bad)
    _validate_number("build-123_linux.tar")  # no raise


# --------------------------------------------------------------------- tokens


def test_download_requires_valid_token(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"data")
        assert c.get("/artifacts/a/content").status_code == 401
        assert download(c, "a", "bogus-token").status_code == 403


def test_token_renew_extends_life_and_expiry_blocks(tmp_path):
    with make_client(tmp_path, token_ttl=1.0) as c:
        put(c, "a", b"data-a")
        token = acquire(c, "a")
        time.sleep(0.6)
        r = c.post(f"/tokens/{token}/renew")
        assert r.status_code == 200  # renewed: now valid until ~t+1.6
        time.sleep(0.7)              # t=1.3 > original 1.0 expiry
        assert download(c, "a", token).status_code == 200
        time.sleep(0.5)              # t=1.8 > renewed 1.6 expiry
        assert download(c, "a", token).status_code == 403
        assert c.post(f"/tokens/{token}/renew").status_code == 409


def test_release_semantics(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"data")
        token = acquire(c, "a")
        assert c.post(f"/tokens/{token}/release").status_code == 200
        assert download(c, "a", token).status_code == 403
        assert c.post(f"/tokens/{token}/renew").status_code == 409
        assert c.post(f"/tokens/{token}/release").status_code == 200  # idempotent
        assert c.post("/tokens/unknown/release").status_code == 404


# --------------------------------------------------------------------- reclaim


def test_reclaim_lru_order_and_actual_freed(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"a" * 10)
        time.sleep(0.02)
        put(c, "b", b"b" * 20)
        time.sleep(0.02)
        put(c, "c", b"c" * 30)

        r = c.post("/reclaim", json={"target_bytes": 10})
        assert r.status_code == 200
        assert r.json()["freed_bytes"] == 10
        assert c.get("/artifacts/a").status_code == 404
        assert c.get("/artifacts/b").status_code == 200

        # Touch c so b becomes the LRU entry.
        download(c, "c", acquire(c, "c"))
        time.sleep(0.02)
        r = c.post("/reclaim", json={"target_bytes": 20})
        assert r.json()["freed_bytes"] == 20
        assert c.get("/artifacts/b").status_code == 404
        assert c.get("/artifacts/c").status_code == 200


def test_reclaim_never_deletes_occupied(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"a" * 10)
        put(c, "b", b"b" * 20)
        put(c, "c", b"c" * 30)
        token = acquire(c, "a")  # occupies the LRU artifact

        r = c.post("/reclaim", json={"target_bytes": 10_000})
        body = r.json()
        assert body["freed_bytes"] == 50  # b + c only
        assert c.get("/artifacts/a").status_code == 200
        assert download(c, "a", token).status_code == 200

        c.post(f"/tokens/{token}/release")
        r = c.post("/reclaim", json={"target_bytes": 10_000})
        assert r.json()["freed_bytes"] == 10
        assert c.get("/artifacts/a").status_code == 404


def test_reclaim_zero_target_frees_nothing(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"a" * 10)
        r = c.post("/reclaim", json={"target_bytes": 0})
        assert r.json()["freed_bytes"] == 0
        assert c.get("/artifacts/a").status_code == 200


def test_expired_token_does_not_block_reclaim(tmp_path):
    with make_client(tmp_path, token_ttl=0.3) as c:
        put(c, "a", b"a" * 10)
        acquire(c, "a")
        time.sleep(0.4)
        r = c.post("/reclaim", json={"target_bytes": 10})
        assert r.json()["freed_bytes"] == 10
        assert c.get("/artifacts/a").status_code == 404


# -------------------------------------------------- interleaving: tokens vs reclaim


def test_stale_token_cannot_read_reuploaded_artifact(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"version-1")
        old_token = acquire(c, "a")  # still valid (ttl 300)
        put(c, "a", b"version-2")    # re-upload bumps generation

        r = download(c, "a", old_token)
        assert r.status_code == 409  # pinned to the old generation

        new_token = acquire(c, "a")
        r = download(c, "a", new_token)
        assert r.status_code == 200 and r.content == b"version-2"


def test_expired_token_cannot_read_reuploaded_artifact(tmp_path):
    with make_client(tmp_path, token_ttl=0.3) as c:
        put(c, "a", b"version-1")
        token = acquire(c, "a")
        time.sleep(0.4)
        put(c, "a", b"version-2")
        assert download(c, "a", token).status_code == 403


def test_reclaim_selected_artifact_issues_no_tokens_and_reupload_is_safe(tmp_path):
    """Mark an artifact 'reclaiming' (as a crashed reclaim would leave it):
    no new tokens may be issued; a re-upload must survive the pending
    deletion untouched."""
    with make_client(tmp_path) as c:
        put(c, "a", b"old-content")
        store = c.app.state.store
        with store.lock, store._txn() as db:  # simulate crash mid-reclaim
            db.execute("UPDATE artifacts SET state='reclaiming' WHERE number='a'")

        assert c.post("/artifacts/a/tokens").status_code == 409

        r = put(c, "a", b"new-content")  # re-upload while reclaim pending
        assert r.status_code == 200 and r.json()["generation"] == 2

        store.sweep()  # the pending reclaim must not eat the re-upload

        meta = c.get("/artifacts/a").json()
        assert meta["generation"] == 2 and meta["downloadable"] is True
        token = acquire(c, "a")
        assert download(c, "a", token).content == b"new-content"
        # Only the generation-2 file remains on disk.
        files = os.listdir(os.path.join(store.storage_dir, "a"))
        assert len(files) == 1 and files[0].startswith("00000002-")


def test_renew_rejected_when_artifact_pending_reclaim(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"data")
        token = acquire(c, "a")
        store = c.app.state.store
        with store.lock, store._txn() as db:
            db.execute("UPDATE artifacts SET state='reclaiming' WHERE number='a'")
        assert c.post(f"/tokens/{token}/renew").status_code == 409


# ------------------------------------------------------- failure / inconsistency


def test_missing_file_is_not_claimed_downloadable(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"data-a")
        token = acquire(c, "a")
        store = c.app.state.store
        a_dir = os.path.join(store.storage_dir, "a")
        victim = os.path.join(a_dir, os.listdir(a_dir)[0])
        os.remove(victim)

        assert c.get("/artifacts/a").json()["downloadable"] is False
        assert download(c, "a", token).status_code == 409

        # Reclaim still drops the record but honestly reports 0 freed bytes
        # (the file was already gone).
        c.post(f"/tokens/{token}/release")
        r = c.post("/reclaim", json={"target_bytes": 100})
        body = r.json()
        assert body["freed_bytes"] == 0
        assert body["reclaimed"][0]["file_deleted"] is False
        assert c.get("/artifacts/a").status_code == 404


def test_delete_failure_continues_cleanup_and_recovers(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"a" * 10)
        put(c, "b", b"b" * 20)
        store = c.app.state.store
        a_dir = os.path.join(store.storage_dir, "a")
        a_file = os.path.join(a_dir, os.listdir(a_dir)[0])
        os.remove(a_file)
        os.mkdir(a_file)  # os.remove() on a directory fails -> deletion error

        r = c.post("/reclaim", json={"target_bytes": 30})
        body = r.json()
        assert body["freed_bytes"] == 20          # b reclaimed, a failed
        assert len(body["errors"]) == 1
        assert body["errors"][0]["number"] == "a"
        assert c.get("/artifacts/b").status_code == 404
        assert c.get("/artifacts/a").json()["state"] == "reclaiming"  # kept for retry

        # Repair the obstacle; the next reclaim retries the pending deletion.
        os.rmdir(a_file)
        with open(a_file, "wb") as fh:
            fh.write(b"a" * 10)
        r = c.post("/reclaim", json={"target_bytes": 30})
        assert r.json()["freed_bytes"] == 10
        assert c.get("/artifacts/a").status_code == 404


def test_restart_completes_interrupted_reclaim(tmp_path):
    cfg = make_config(tmp_path)
    with TestClient(create_app(cfg)) as c:
        put(c, "a", b"data-a")
        put(c, "b", b"data-b")
        store = c.app.state.store
        with store.lock, store._txn() as db:  # crash after marking, before delete
            db.execute("UPDATE artifacts SET state='reclaiming' WHERE number='a'")

    # "Restart": a fresh app over the same db + storage runs the startup sweep.
    with TestClient(create_app(cfg)) as c2:
        assert c2.get("/artifacts/a").status_code == 404
        assert c2.get("/artifacts/b").status_code == 200
        token = acquire(c2, "b")
        assert download(c2, "b", token).status_code == 200
    assert not os.path.exists(os.path.join(cfg.storage_dir, "a"))


def test_orphan_files_swept_with_grace(tmp_path):
    with make_client(tmp_path, orphan_grace=0.2) as c:
        put(c, "a", b"data")
        store = c.app.state.store
        orphan = os.path.join(store.storage_dir, "orphan.bin")
        with open(orphan, "wb") as fh:
            fh.write(b"orphan")

        store.sweep()  # too recent -> kept (may belong to an in-flight upload)
        assert os.path.exists(orphan)

        old = time.time() - 10
        os.utime(orphan, (old, old))
        store.sweep()
        assert not os.path.exists(orphan)
        # The real artifact file was never at risk.
        token = acquire(c, "a")
        assert download(c, "a", token).status_code == 200


# ----------------------------------------------------------------------- docs


def test_docs_and_openapi_available(tmp_path):
    with make_client(tmp_path) as c:
        assert c.get("/docs").status_code == 200
        spec = c.get("/openapi.json")
        assert spec.status_code == 200
        paths = spec.json()["paths"]
        for expected in (
            "/artifacts/{number}",
            "/artifacts/{number}/content",
            "/artifacts/{number}/tokens",
            "/tokens/{token}/renew",
            "/tokens/{token}/release",
            "/reclaim",
        ):
            assert expected in paths
