"""Storage engine for the build artifact cache.

Design notes
------------
* Single-process, single SQLite connection guarded by one threading lock.
  Every public method is fully serialized, so acquire / renew / reclaim
  interleavings cannot race inside the process. SQLite is the source of
  truth for crash recovery.
* Every upload gets a fresh *generation* and a unique file path
  (``<number>/<generation>-<uuid>.bin``). Tokens pin ``(number,
  generation)`` so a token issued for an older upload can never read the
  re-uploaded content, and a pending reclaim of generation N can never
  delete the file of generation N+1.
* Reclaim is two-phase: a conditional ``UPDATE ... SET state='reclaiming'``
  (which atomically re-checks "no active tokens" and blocks any new token
  issuance), then file deletion, then a conditional row delete. If file
  deletion fails, the row stays in ``reclaiming`` and is retried by the
  next reclaim call, the background sweeper, or the startup sweep after a
  process restart.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
import time
import uuid

from .config import Config

SCHEMA = """
CREATE TABLE IF NOT EXISTS artifacts (
    number       TEXT PRIMARY KEY,
    generation   INTEGER NOT NULL,
    sha256       TEXT NOT NULL,
    size         INTEGER NOT NULL,
    file_path    TEXT NOT NULL,
    state        TEXT NOT NULL DEFAULT 'ready'
                 CHECK (state IN ('ready', 'reclaiming')),
    created_at   REAL NOT NULL,
    last_used_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS tokens (
    token      TEXT PRIMARY KEY,
    number     TEXT NOT NULL,
    generation INTEGER NOT NULL,
    expires_at REAL NOT NULL,
    released   INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tokens_active
    ON tokens(number, generation, released, expires_at);
"""

# SQL fragment: an active (unreleased, unexpired) token pinning this exact
# artifact generation exists. Takes one parameter: the current timestamp.
_ACTIVE_TOKEN = """
    EXISTS(SELECT 1 FROM tokens t
           WHERE t.number = artifacts.number
             AND t.generation = artifacts.generation
             AND t.released = 0
             AND t.expires_at > ?)
"""


class StoreError(Exception):
    """Domain error carrying an HTTP status code and a client-safe detail."""

    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class _Txn:
    """Explicit BEGIN IMMEDIATE transaction on the autocommit connection."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def __enter__(self) -> sqlite3.Connection:
        self.conn.execute("BEGIN IMMEDIATE")
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.conn.execute("ROLLBACK" if exc_type else "COMMIT")
        return False


class Store:
    def __init__(self, config: Config):
        self.config = config
        self.storage_dir = os.path.abspath(config.storage_dir)
        os.makedirs(self.storage_dir, exist_ok=True)
        if config.db_path != ":memory:":
            parent = os.path.dirname(os.path.abspath(config.db_path))
            os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(config.db_path, check_same_thread=False)
        self.conn.isolation_level = None  # autocommit; explicit txns via _Txn
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(SCHEMA)
        self.lock = threading.Lock()

    # ------------------------------------------------------------------ util

    def _txn(self) -> _Txn:
        return _Txn(self.conn)

    def _abs(self, rel_path: str) -> str:
        return os.path.join(self.storage_dir, rel_path)

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    @staticmethod
    def _now() -> float:
        return time.time()

    # -------------------------------------------------------------- artifacts

    def upload(self, number: str, data: bytes, sha256: str) -> dict:
        """Store a new generation of ``number`` and return its metadata.

        The file is written to a unique per-upload path before the row is
        committed, so a crash leaves at worst an orphan file (swept later),
        never a row pointing at missing content created by *this* upload.
        """
        now = self._now()
        with self.lock:
            with self._txn() as db:
                row = db.execute(
                    "SELECT generation, file_path FROM artifacts WHERE number=?",
                    (number,),
                ).fetchone()
                generation = (row["generation"] + 1) if row else 1
                old_path = row["file_path"] if row else None

                rel = f"{number}/{generation:08d}-{uuid.uuid4().hex}.bin"
                dest = self._abs(rel)
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                tmp = f"{dest}.tmp-{uuid.uuid4().hex}"
                with open(tmp, "wb") as fh:
                    fh.write(data)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp, dest)
                try:  # best-effort directory fsync for rename durability
                    dfd = os.open(os.path.dirname(dest), os.O_RDONLY)
                    try:
                        os.fsync(dfd)
                    finally:
                        os.close(dfd)
                except OSError:
                    pass

                db.execute(
                    """
                    INSERT INTO artifacts(number, generation, sha256, size,
                                          file_path, state, created_at, last_used_at)
                    VALUES (?,?,?,?,?, 'ready', ?, ?)
                    ON CONFLICT(number) DO UPDATE SET
                        generation   = excluded.generation,
                        sha256       = excluded.sha256,
                        size         = excluded.size,
                        file_path    = excluded.file_path,
                        state        = 'ready',
                        last_used_at = excluded.last_used_at
                    """,
                    (number, generation, sha256, len(data), rel, now, now),
                )
                # NB: tokens pinned to older generations are left in place;
                # they can never match the new generation (download -> 409,
                # renew -> 409) and are cleaned up once they expire.

            # Best-effort removal of the replaced file. If it fails (or the
            # old generation was mid-reclaim), the sweeper finishes the job;
            # the unique path of the new upload is never touched.
            if old_path and old_path != rel:
                try:
                    os.remove(self._abs(old_path))
                except OSError:
                    pass

            return self._artifact_out(number, now)

    def get_artifact(self, number: str) -> dict:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM artifacts WHERE number=?", (number,)
            ).fetchone()
            if row is None:
                raise StoreError(404, f"artifact {number!r} not found")
            return self._artifact_out(number, self._now())

    def list_artifacts(self) -> list[dict]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT number FROM artifacts ORDER BY number"
            ).fetchall()
            now = self._now()
            return [self._artifact_out(r["number"], now) for r in rows]

    def _artifact_out(self, number: str, now: float) -> dict:
        row = self.conn.execute(
            "SELECT * FROM artifacts WHERE number=?", (number,)
        ).fetchone()
        if row is None:
            raise StoreError(404, f"artifact {number!r} not found")
        active = self.conn.execute(
            "SELECT COUNT(*) AS c FROM tokens "
            "WHERE number=? AND generation=? AND released=0 AND expires_at>?",
            (number, row["generation"], now),
        ).fetchone()["c"]
        # Never claim downloadable unless the record is ready *and* the file
        # is actually present on disk.
        downloadable = row["state"] == "ready" and os.path.isfile(
            self._abs(row["file_path"])
        )
        return {
            "number": row["number"],
            "generation": row["generation"],
            "sha256": row["sha256"],
            "size": row["size"],
            "state": row["state"],
            "created_at": row["created_at"],
            "last_used_at": row["last_used_at"],
            "active_tokens": active,
            "downloadable": downloadable,
        }

    # ----------------------------------------------------------------- tokens

    def acquire(self, number: str) -> dict:
        now = self._now()
        with self.lock, self._txn() as db:
            row = db.execute(
                "SELECT generation, state FROM artifacts WHERE number=?", (number,)
            ).fetchone()
            if row is None:
                raise StoreError(404, f"artifact {number!r} not found")
            if row["state"] != "ready":
                raise StoreError(
                    409,
                    f"artifact {number!r} is pending reclamation; "
                    "no new tokens are issued",
                )
            token = uuid.uuid4().hex
            expires_at = now + self.config.token_ttl
            db.execute(
                "INSERT INTO tokens(token, number, generation, expires_at, "
                "released, created_at) VALUES (?,?,?,?,0,?)",
                (token, number, row["generation"], expires_at, now),
            )
            db.execute(
                "UPDATE artifacts SET last_used_at=? WHERE number=?", (now, number)
            )
        return {
            "token": token,
            "number": number,
            "generation": row["generation"],
            "expires_at": expires_at,
            "expires_in": self.config.token_ttl,
        }

    def _load_valid_token(self, token: str, number: str | None) -> sqlite3.Row:
        row = self.conn.execute(
            "SELECT * FROM tokens WHERE token=?", (token,)
        ).fetchone()
        if row is None or (number is not None and row["number"] != number):
            raise StoreError(403, "invalid token")
        if row["released"]:
            raise StoreError(403, "token has been released")
        if row["expires_at"] <= self._now():
            raise StoreError(403, "token has expired")
        return row

    def download(self, number: str, token: str) -> tuple[bytes, dict]:
        with self.lock:
            tok = self._load_valid_token(token, number)
            art = self.conn.execute(
                "SELECT * FROM artifacts WHERE number=?", (number,)
            ).fetchone()
            if art is None:
                raise StoreError(404, f"artifact {number!r} not found")
            if art["generation"] != tok["generation"]:
                raise StoreError(
                    409,
                    "token was issued for generation "
                    f"{tok['generation']}, but the artifact has been "
                    f"re-uploaded as generation {art['generation']}",
                )
            path = self._abs(art["file_path"])
            try:
                with open(path, "rb") as fh:
                    data = fh.read()
            except FileNotFoundError:
                raise StoreError(
                    409,
                    "artifact record exists but the file is missing on disk "
                    "(inconsistent state); refusing to serve",
                )
            except OSError as exc:
                raise StoreError(409, f"artifact file unreadable: {exc}")
            digest = hashlib.sha256(data).hexdigest()
            if digest != art["sha256"]:
                raise StoreError(
                    409,
                    "artifact file failed integrity check "
                    "(record/file mismatch); refusing to serve",
                )
            now = self._now()
            with self._txn() as db:
                db.execute(
                    "UPDATE artifacts SET last_used_at=? WHERE number=?",
                    (now, number),
                )
            meta = {
                "number": number,
                "generation": art["generation"],
                "sha256": art["sha256"],
                "size": art["size"],
            }
            return data, meta

    def renew(self, token: str) -> dict:
        now = self._now()
        with self.lock, self._txn() as db:
            row = db.execute(
                "SELECT * FROM tokens WHERE token=?", (token,)
            ).fetchone()
            if row is None:
                raise StoreError(404, "unknown token")
            if row["released"]:
                raise StoreError(409, "token has been released")
            if row["expires_at"] <= now:
                raise StoreError(409, "token has expired and cannot be renewed")
            art = db.execute(
                "SELECT generation, state FROM artifacts WHERE number=?",
                (row["number"],),
            ).fetchone()
            if (
                art is None
                or art["generation"] != row["generation"]
                or art["state"] != "ready"
            ):
                raise StoreError(
                    409,
                    "artifact is no longer available for renewal "
                    "(reclaimed, being reclaimed, or re-uploaded)",
                )
            expires_at = now + self.config.token_ttl
            db.execute(
                "UPDATE tokens SET expires_at=? WHERE token=?", (expires_at, token)
            )
        return {
            "token": token,
            "number": row["number"],
            "generation": row["generation"],
            "expires_at": expires_at,
            "expires_in": self.config.token_ttl,
        }

    def release(self, token: str) -> dict:
        with self.lock, self._txn() as db:
            cur = db.execute(
                "UPDATE tokens SET released=1 WHERE token=?", (token,)
            )
            if cur.rowcount == 0:
                raise StoreError(404, "unknown token")
        return {"token": token, "released": True}

    def get_token(self, token: str) -> dict:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM tokens WHERE token=?", (token,)
            ).fetchone()
            if row is None:
                raise StoreError(404, "unknown token")
            now = self._now()
            return {
                "token": row["token"],
                "number": row["number"],
                "generation": row["generation"],
                "expires_at": row["expires_at"],
                "released": bool(row["released"]),
                "active": not row["released"] and row["expires_at"] > now,
            }

    # ---------------------------------------------------------------- reclaim

    def reclaim(self, target_bytes: int) -> dict:
        """Free at least ``target_bytes``, LRU-first, never touching occupied
        or already-selected artifacts. Returns what was actually freed."""
        if target_bytes < 0:
            raise StoreError(422, "target_bytes must be >= 0")
        now = self._now()
        freed = 0
        reclaimed: list[dict] = []
        errors: list[dict] = []
        with self.lock:
            # First finish any deletions interrupted earlier (crash or I/O
            # failure); bytes freed now count toward this call's target.
            f, rec, err = self._reclaim_pending_locked()
            freed += f
            reclaimed += rec
            errors += err

            while freed < target_bytes:
                rows = self.conn.execute(
                    f"""
                    SELECT number, generation, size, file_path FROM artifacts
                    WHERE state='ready' AND NOT {_ACTIVE_TOKEN}
                    ORDER BY last_used_at ASC, number ASC
                    """,
                    (now,),
                ).fetchall()
                if not rows:
                    break
                progress = False
                for row in rows:
                    if freed >= target_bytes:
                        break
                    status, value = self._mark_and_delete_locked(row, now)
                    if status == "freed":
                        freed += value
                        reclaimed.append(
                            {
                                "number": row["number"],
                                "generation": row["generation"],
                                "size": row["size"],
                                "file_deleted": True,
                            }
                        )
                        progress = True
                    elif status == "removed_empty":
                        reclaimed.append(
                            {
                                "number": row["number"],
                                "generation": row["generation"],
                                "size": row["size"],
                                "file_deleted": False,
                            }
                        )
                        progress = True
                    elif status == "failed":
                        errors.append(
                            {
                                "number": row["number"],
                                "generation": row["generation"],
                                "error": value,
                            }
                        )
                    # "skipped" -> lost the conditional update; not counted
                    # as progress so the loop can re-evaluate.
                if not progress:
                    break
        return {
            "target_bytes": target_bytes,
            "freed_bytes": freed,
            "reclaimed": reclaimed,
            "errors": errors,
        }

    def _reclaim_pending_locked(self) -> tuple[int, list[dict], list[dict]]:
        """Retry rows already marked 'reclaiming' (crash / delete failure)."""
        rows = self.conn.execute(
            "SELECT number, generation, size, file_path FROM artifacts "
            "WHERE state='reclaiming'"
        ).fetchall()
        freed = 0
        reclaimed: list[dict] = []
        errors: list[dict] = []
        for row in rows:
            status, value = self._delete_marked_locked(
                row["number"], row["generation"], row["size"], row["file_path"]
            )
            if status == "freed":
                freed += value
                reclaimed.append(
                    {
                        "number": row["number"],
                        "generation": row["generation"],
                        "size": row["size"],
                        "file_deleted": True,
                    }
                )
            elif status == "removed_empty":
                reclaimed.append(
                    {
                        "number": row["number"],
                        "generation": row["generation"],
                        "size": row["size"],
                        "file_deleted": False,
                    }
                )
            elif status == "failed":
                errors.append(
                    {
                        "number": row["number"],
                        "generation": row["generation"],
                        "error": value,
                    }
                )
        return freed, reclaimed, errors

    def _mark_and_delete_locked(self, row: sqlite3.Row, now: float):
        """Atomically select an artifact for reclaim, then delete it.

        The conditional UPDATE re-checks occupancy and readiness, so an
        artifact that gained a token since the candidate query is skipped.
        Once marked 'reclaiming', no new tokens can be issued for it.
        """
        cur = self.conn.execute(
            f"""
            UPDATE artifacts SET state='reclaiming'
            WHERE number=? AND generation=? AND state='ready'
              AND NOT {_ACTIVE_TOKEN}
            """,
            (row["number"], row["generation"], now),
        )
        if cur.rowcount == 0:
            return "skipped", 0
        return self._delete_marked_locked(
            row["number"], row["generation"], row["size"], row["file_path"]
        )

    def _delete_marked_locked(
        self, number: str, generation: int, size: int, file_path: str
    ):
        """Delete the file of a 'reclaiming' row, then the row itself.

        On file-deletion failure the row stays in 'reclaiming' so the
        sweeper / next reclaim / startup recovery retries it. The row delete
        is conditional on (number, generation, reclaiming): if the artifact
        was re-uploaded meanwhile, the new row and its new file are left
        untouched.
        """
        try:
            os.remove(self._abs(file_path))
            deleted = True
        except FileNotFoundError:
            deleted = False  # already gone; still drop the stale record
        except OSError as exc:
            return "failed", f"{type(exc).__name__}: {exc}"
        with self._txn() as db:
            db.execute(
                "DELETE FROM artifacts WHERE number=? AND generation=? "
                "AND state='reclaiming'",
                (number, generation),
            )
            db.execute(
                "DELETE FROM tokens WHERE number=? AND generation=?",
                (number, generation),
            )
        return ("freed", size) if deleted else ("removed_empty", 0)

    # ------------------------------------------------------------------ sweep

    def sweep(self) -> dict:
        """Recovery pass: finish interrupted reclaims, drop dead tokens and
        orphan files. Runs at startup, periodically, and on demand."""
        now = self._now()
        with self.lock:
            freed, reclaimed, errors = self._reclaim_pending_locked()
            with self._txn() as db:
                db.execute(
                    "DELETE FROM tokens WHERE released=1 OR expires_at<?", (now,)
                )
            known = {
                r["file_path"]
                for r in self.conn.execute("SELECT file_path FROM artifacts")
            }
            orphans_removed: list[str] = []
            for root, _dirs, files in os.walk(self.storage_dir):
                for name in files:
                    full = os.path.join(root, name)
                    rel = os.path.relpath(full, self.storage_dir)
                    if rel in known:
                        continue
                    try:
                        st = os.stat(full)
                    except OSError:
                        continue
                    # Grace window: a just-written file may belong to an
                    # in-flight upload whose row is not committed yet.
                    if now - st.st_mtime < self.config.orphan_grace:
                        continue
                    try:
                        os.remove(full)
                        orphans_removed.append(rel)
                    except OSError:
                        pass
            # Prune empty per-artifact directories (bottom-up).
            for root, _dirs, _files in os.walk(self.storage_dir, topdown=False):
                if root == self.storage_dir:
                    continue
                try:
                    os.rmdir(root)
                except OSError:
                    pass
            return {
                "freed_bytes": freed,
                "pending_reclaimed": reclaimed,
                "orphans_removed": orphans_removed,
                "errors": errors,
            }

    # ------------------------------------------------------------------ stats

    def stats(self) -> dict:
        now = self._now()
        with self.lock:
            art = self.conn.execute(
                "SELECT COUNT(*) AS c, COALESCE(SUM(size),0) AS b, "
                "COALESCE(SUM(state='reclaiming'),0) AS p FROM artifacts"
            ).fetchone()
            tok = self.conn.execute(
                "SELECT COUNT(*) AS c FROM tokens "
                "WHERE released=0 AND expires_at>?",
                (now,),
            ).fetchone()
            return {
                "artifacts": art["c"],
                "bytes": art["b"],
                "pending_reclaim": art["p"],
                "active_tokens": tok["c"],
            }
