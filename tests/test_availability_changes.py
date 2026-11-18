"""Tests for the cursor-resumable availability change feed.

Covers the required semantics:

* successful upload records ``available``;
* same-number re-upload records the old generation ``invalidated`` and the
  new one ``available`` in the same commit;
* reclaim selection records ``invalidated``; ``released`` is recorded only
  after the file is actually removed, never on a deletion failure;
* restart/sweep recovery neither misses nor duplicates events;
* a late sweep of an old generation never overwrites the new generation's
  events;
* paging across interleaved uploads / leases / reclaims follows commit
  order with no gaps and no duplicates;
* existing download and lease behavior is unchanged.
"""

from __future__ import annotations

import hashlib
import os

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
    return client.put(
        f"/artifacts/{number}", content=data, params={"sha256": sha(data)}
    )


def drain(client: TestClient, page_size: int = 2) -> list[dict]:
    """Read the whole feed using small pages, exercising every cursor hop."""
    out: list[dict] = []
    cursor = 0
    seen: set[int] = set()
    while True:
        r = client.get(
            "/availability/changes", params={"cursor": cursor, "limit": page_size}
        )
        assert r.status_code == 200, r.text
        body = r.json()
        for ch in body["changes"]:
            assert ch["seq"] not in seen, f"duplicate seq {ch['seq']}"
            seen.add(ch["seq"])
            out.append(ch)
        if not body["has_more"]:
            assert body["next_cursor"] == (out[-1]["seq"] if out else cursor)
            return out
        assert body["changes"], "has_more requires a non-empty page"
        cursor = body["next_cursor"]


def events_for(events: list[dict], number: str, generation: int) -> list[str]:
    return [
        e["status"]
        for e in events
        if e["number"] == number and e["generation"] == generation
    ]


# --------------------------------------------------------------- basic writes


def test_first_upload_records_available(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"data-a")
        events = drain(c)
        assert len(events) == 1
        ev = events[0]
        assert ev["seq"] == 1
        assert ev["number"] == "a"
        assert ev["generation"] == 1
        assert ev["status"] == "available"
        assert isinstance(ev["changed_at"], float)


def test_reupload_records_invalidated_and_available_in_one_commit(tmp_path):
    with make_client(tmp_path) as c:
        store = c.app.state.store
        put(c, "a", b"version-1")
        put(c, "a", b"version-2")  # same number, new generation

        events = drain(c)
        assert [(e["generation"], e["status"]) for e in events] == [
            (1, "available"),
            (1, "invalidated"),
            (2, "available"),
        ]
        # Both rows are committed together by the upload transaction; no
        # other writer can interleave between them (global store lock), so
        # their seqs are consecutive and share one timestamp.
        gen1_inv = next(
            e for e in events
            if e["generation"] == 1 and e["status"] == "invalidated"
        )
        gen2 = next(e for e in events if e["generation"] == 2)
        assert gen2["seq"] == gen1_inv["seq"] + 1
        assert gen2["changed_at"] == gen1_inv["changed_at"]
        with store.lock:
            persisted = store.conn.execute(
                "SELECT seq, generation, status FROM availability_changes "
                "ORDER BY seq"
            ).fetchall()
        assert [tuple(r) for r in persisted] == [
            (1, 1, "available"),
            (2, 1, "invalidated"),
            (3, 2, "available"),
        ]


def test_reupload_supersedes_reclaiming_generation_with_events(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"old-content")
        store = c.app.state.store
        # Simulate a crash after reclaim selection: gen 1 already invalidated.
        with store.lock, store._txn() as db:
            db.execute(
                "UPDATE artifacts SET state='reclaiming' WHERE number='a'"
            )
            db.execute(
                "INSERT INTO availability_changes(number, generation, status, "
                "changed_at) VALUES ('a', 1, 'invalidated', 1.0)"
            )

        r = put(c, "a", b"new-content")
        assert r.status_code == 200 and r.json()["generation"] == 2

        events = drain(c)
        # Original available, reclaim invalidation, then the superseding
        # upload's own invalidated(1) + available(2).
        assert [(e["generation"], e["status"]) for e in events] == [
            (1, "available"),
            (1, "invalidated"),
            (1, "invalidated"),
            (2, "available"),
        ]
        assert events_for(events, "a", 2) == ["available"]


# --------------------------------------------------------------------- reclaim


def test_reclaim_records_invalidated_then_released(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"a" * 10)
        r = c.post("/reclaim", json={"target_bytes": 10})
        assert r.status_code == 200 and r.json()["freed_bytes"] == 10

        events = drain(c)
        assert events_for(events, "a", 1) == [
            "available",
            "invalidated",
            "released",
        ]
        # Ordering: invalidation is selected first, release after unlink.
        statuses = [e["status"] for e in events]
        assert statuses.index("invalidated") < statuses.index("released")


def test_failed_deletion_does_not_record_released_and_recovery_completes(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"a" * 10)
        store = c.app.state.store
        a_dir = os.path.join(store.storage_dir, "a")
        a_file = os.path.join(a_dir, os.listdir(a_dir)[0])
        os.remove(a_file)
        os.mkdir(a_file)  # os.remove on a directory raises -> deletion error

        r = c.post("/reclaim", json={"target_bytes": 100})
        assert r.json()["errors"][0]["number"] == "a"

        events = drain(c)
        # Selected (invalidated) but the failed unlink must not look released.
        assert events_for(events, "a", 1) == ["available", "invalidated"]
        assert c.get("/artifacts/a").json()["state"] == "reclaiming"

        # Repair and let the sweeper finish the interrupted deletion.
        os.rmdir(a_file)
        with open(a_file, "wb") as fh:
            fh.write(b"a" * 10)
        sweep = c.post("/admin/sweep")
        assert sweep.status_code == 200

        events = drain(c)
        assert events_for(events, "a", 1) == [
            "available",
            "invalidated",
            "released",
        ]  # exactly one release, no gap
        assert c.get("/artifacts/a").status_code == 404


def test_repeated_sweep_emits_no_duplicate_release(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"data-a")
        c.post("/reclaim", json={"target_bytes": 100})
        c.post("/admin/sweep")
        c.post("/admin/sweep")  # nothing pending; must not re-emit anything

        events = drain(c)
        assert events_for(events, "a", 1).count("released") == 1
        assert events_for(events, "a", 1) == [
            "available", "invalidated", "released",
        ]


def test_missing_file_still_released_when_record_dropped(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"data-a")
        store = c.app.state.store
        a_dir = os.path.join(store.storage_dir, "a")
        victim = os.path.join(a_dir, os.listdir(a_dir)[0])
        os.remove(victim)  # file already gone before reclaim runs

        r = c.post("/reclaim", json={"target_bytes": 100})
        assert r.status_code == 200
        item = r.json()["reclaimed"][0]
        assert item["file_deleted"] is False  # honest: nothing was unlinked

        events = drain(c)
        # The generation's storage lifecycle nevertheless ends exactly once.
        assert events_for(events, "a", 1) == [
            "available",
            "invalidated",
            "released",
        ]


# ------------------------------------------------------------- restart / late


def test_restart_sweep_completes_events_without_loss_or_dupes(tmp_path):
    cfg = make_config(tmp_path)
    with TestClient(create_app(cfg)) as c:
        put(c, "a", b"data-a")
        store = c.app.state.store
        with store.lock, store._txn() as db:  # crash after selection
            db.execute(
                "UPDATE artifacts SET state='reclaiming' WHERE number='a'"
            )
            db.execute(
                "INSERT INTO availability_changes(number, generation, status, "
                "changed_at) VALUES ('a', 1, 'invalidated', 1.0)"
            )

    with TestClient(create_app(cfg)) as c2:  # startup sweep finishes it
        assert c2.get("/artifacts/a").status_code == 404
        events = drain(c2)
        assert [(e["generation"], e["status"]) for e in events] == [
            (1, "available"),
            (1, "invalidated"),
            (1, "released"),
        ]

    # A second restart over the same db must not add or duplicate events.
    with TestClient(create_app(cfg)) as c3:
        again = drain(c3)
        assert again == events


def test_late_old_generation_sweep_does_not_touch_new_generation(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"old-content")
        store = c.app.state.store
        with store.lock, store._txn() as db:  # crashed mid-reclaim of gen 1
            db.execute(
                "UPDATE artifacts SET state='reclaiming' WHERE number='a'"
            )
            db.execute(
                "INSERT INTO availability_changes(number, generation, status, "
                "changed_at) VALUES ('a', 1, 'invalidated', 1.0)"
            )

        put(c, "a", b"new-content")  # gen 2 supersedes the stale reclaim
        c.post("/admin/sweep")       # late sweep of the old generation

        events = drain(c)
        assert events_for(events, "a", 2) == ["available"]  # untouched
        meta = c.get("/artifacts/a").json()
        assert meta["generation"] == 2 and meta["downloadable"] is True
        # No release is attributed to gen 1 either: its row was superseded,
        # not deleted as a reclaiming match, and gen 2 owns the number now.
        assert "released" not in events_for(events, "a", 1)


# ------------------------------------------------------------- cursor / paging


def test_cursor_paging_is_gap_and_duplicate_free_under_interleaving(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "n1", b"v1")
        c.post("/artifacts/n1/tokens")  # leases create no availability events
        put(c, "n2", b"v2")
        put(c, "n1", b"v1-new")
        c.post("/reclaim", json={"target_bytes": 1})  # reclaims LRU n2
        put(c, "n3", b"v3")
        c.post("/artifacts/n3/tokens")

        events = drain(c, page_size=1)  # one row per page: every hop exercised
        seqs = [e["seq"] for e in events]
        assert seqs == list(range(1, len(seqs) + 1))  # gap-free, in order

        expected = [
            ("n1", 1, "available"),
            ("n2", 1, "available"),
            ("n1", 1, "invalidated"),
            ("n1", 2, "available"),
            ("n2", 1, "invalidated"),
            ("n2", 1, "released"),
            ("n3", 1, "available"),
        ]
        assert [(e["number"], e["generation"], e["status"]) for e in events] == expected


def test_polling_empty_then_resuming_delivers_only_new_events(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"x")
        first = c.get("/availability/changes", params={"cursor": 0}).json()
        assert len(first["changes"]) == 1 and first["has_more"] is False
        cursor = first["next_cursor"]

        idle = c.get(
            "/availability/changes", params={"cursor": cursor}
        ).json()
        assert idle["changes"] == []
        assert idle["next_cursor"] == cursor  # stable when nothing new
        assert idle["has_more"] is False

        put(c, "b", b"y")
        resumed = c.get(
            "/availability/changes", params={"cursor": cursor}
        ).json()
        assert len(resumed["changes"]) == 1
        assert resumed["changes"][0]["number"] == "b"
        assert resumed["changes"][0]["status"] == "available"
        # Re-using the old cursor would re-read; advancing does not.
        again = c.get(
            "/availability/changes",
            params={"cursor": resumed["next_cursor"]},
        ).json()
        assert again["changes"] == []


def test_has_more_and_page_limit_boundaries(tmp_path):
    with make_client(tmp_path) as c:
        for i in range(5):
            put(c, f"n{i}", b"x")

        page = c.get(
            "/availability/changes", params={"cursor": 0, "limit": 2}
        ).json()
        assert len(page["changes"]) == 2 and page["has_more"] is True
        assert [e["seq"] for e in page["changes"]] == [1, 2]
        assert page["next_cursor"] == 2

        last = c.get(
            "/availability/changes", params={"cursor": 4, "limit": 10}
        ).json()
        assert [e["seq"] for e in last["changes"]] == [5]
        assert last["has_more"] is False and last["next_cursor"] == 5

        # Validation errors are 422, never 5xx.
        assert c.get(
            "/availability/changes", params={"cursor": -1}
        ).status_code == 422
        assert c.get(
            "/availability/changes", params={"limit": 0}
        ).status_code == 422
        assert c.get(
            "/availability/changes", params={"limit": 1001}
        ).status_code == 422


def test_feed_is_append_only_and_survives_reupload_reclaim_mix(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"a1")
        put(c, "b", b"b1")
        put(c, "a", b"a2")                 # invalidate a/1, available a/2
        c.post("/reclaim", json={"target_bytes": 10_000})  # LRU b then a/2
        put(c, "a", b"a3")                 # a/3 available after a/2 reclaimed

        events = drain(c)
        # a/1 was superseded by a re-upload, so it was never itself reclaimed.
        assert events_for(events, "a", 1) == ["available", "invalidated"]
        assert events_for(events, "b", 1) == [
            "available", "invalidated", "released",
        ]
        assert events_for(events, "a", 2) == [
            "available", "invalidated", "released",
        ]
        assert events_for(events, "a", 3) == ["available"]
        # Nothing ever mutates an already-published event row.
        store = c.app.state.store
        with store.lock:
            count = store.conn.execute(
                "SELECT COUNT(*) AS c FROM availability_changes"
            ).fetchone()["c"]
        assert count == len(events)


# ------------------------------------------------------- existing behavior


def test_leases_and_downloads_still_work_alongside_feed(tmp_path):
    with make_client(tmp_path) as c:
        data = b"lease-and-feed"
        put(c, "a", data)
        token = c.post("/artifacts/nothere/tokens")
        assert token.status_code == 404
        token = c.post("/artifacts/a/tokens").json()["token"]
        r = c.get("/artifacts/a/content", params={"token": token})
        assert r.status_code == 200 and r.content == data

        old = token
        put(c, "a", b"new-bytes")
        assert c.get(
            "/artifacts/a/content", params={"token": old}
        ).status_code == 409  # stale generation token still rejected
        token2 = c.post("/artifacts/a/tokens").json()["token"]
        r = c.get("/artifacts/a/content", params={"token": token2})
        assert r.status_code == 200 and r.content == b"new-bytes"


def test_generation_is_never_reused_after_full_reclaim(tmp_path):
    """A number whose every generation was reclaimed (row deleted) must still
    allocate fresh, monotonic generations on a later re-upload, so two
    different physical generations never share one (number, generation)."""
    with make_client(tmp_path) as c:
        put(c, "a", b"first")
        assert c.get("/artifacts/a").json()["generation"] == 1
        c.post("/reclaim", json={"target_bytes": 10_000})  # gen 1 fully gone
        assert c.get("/artifacts/a").status_code == 404

        r = put(c, "a", b"second")
        assert r.status_code == 200
        assert r.json()["generation"] == 2  # not reused as 1

        c.post("/reclaim", json={"target_bytes": 10_000})
        assert put(c, "a", b"third").json()["generation"] == 3

        events = drain(c)
        # Each generation has its own distinct lifecycle in the feed.
        assert events_for(events, "a", 1) == [
            "available", "invalidated", "released",
        ]
        assert events_for(events, "a", 2) == [
            "available", "invalidated", "released",
        ]
        assert events_for(events, "a", 3) == ["available"]


def test_feed_path_published_in_openapi(tmp_path):
    with make_client(tmp_path) as c:
        spec = c.get("/openapi.json").json()
        assert "/availability/changes" in spec["paths"]
        schema = spec["components"]["schemas"]
        assert "AvailabilityChangeOut" in schema
        assert "AvailabilityChangesOut" in schema
