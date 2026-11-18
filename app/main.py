"""FastAPI surface for the build artifact cache."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from contextlib import asynccontextmanager, suppress

from fastapi import Body, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from pydantic import BaseModel, Field

from .config import Config
from .store import Store, StoreError

log = logging.getLogger("artifact-cache")

_NUMBER_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


# --------------------------------------------------------------------- models


class ArtifactOut(BaseModel):
    number: str
    generation: int
    sha256: str
    size: int
    state: str
    created_at: float
    last_used_at: float
    active_tokens: int
    downloadable: bool


class TokenOut(BaseModel):
    token: str
    number: str
    generation: int
    expires_at: float
    expires_in: float


class TokenStatusOut(BaseModel):
    token: str
    number: str
    generation: int
    expires_at: float
    released: bool
    active: bool


class ReclaimIn(BaseModel):
    target_bytes: int = Field(ge=0, description="Number of bytes to free")


class ReclaimedItem(BaseModel):
    number: str
    generation: int
    size: int
    file_deleted: bool


class ReclaimOut(BaseModel):
    target_bytes: int
    freed_bytes: int
    reclaimed: list[ReclaimedItem]
    errors: list[dict]


class ReleaseOut(BaseModel):
    token: str
    released: bool


# ------------------------------------------------------------------------- app


def _validate_number(number: str) -> None:
    if not _NUMBER_RE.fullmatch(number) or not number.strip("."):
        raise HTTPException(
            400,
            "invalid artifact number: 1-128 chars of [A-Za-z0-9._-], "
            "not consisting solely of dots",
        )


def create_app(config: Config | None = None) -> FastAPI:
    cfg = config or Config.from_env()
    store = Store(cfg)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup recovery: finish reclaims interrupted by a previous crash
        # or failed deletion, and drop orphan files.
        await asyncio.to_thread(store.sweep)
        sweeper_task = None
        if cfg.sweep_interval > 0:

            async def _sweeper() -> None:
                while True:
                    await asyncio.sleep(cfg.sweep_interval)
                    try:
                        await asyncio.to_thread(store.sweep)
                    except Exception:  # pragma: no cover - defensive
                        log.exception("background sweep failed")

            sweeper_task = asyncio.create_task(_sweeper())
        try:
            yield
        finally:
            if sweeper_task is not None:
                sweeper_task.cancel()
                with suppress(asyncio.CancelledError):
                    await sweeper_task
            store.close()

    app = FastAPI(
        title="Build Artifact Cache",
        version="1.0.0",
        description=(
            "Content-addressed cache for CI build artifacts. Uploads are "
            "SHA-256 verified and capped at 1 MiB. Downloads require a "
            "time-limited lease token. Reclamation frees LRU, unoccupied "
            "artifacts first."
        ),
        lifespan=lifespan,
    )
    app.state.store = store
    app.state.config = cfg

    @app.exception_handler(StoreError)
    async def _store_error_handler(_req: Request, exc: StoreError):
        return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)

    # ------------------------------------------------------------- artifacts

    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse("/docs")

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.put(
        "/artifacts/{number}",
        response_model=ArtifactOut,
        summary="Upload (or re-upload) an artifact, SHA-256 verified, max 1 MiB",
    )
    def put_artifact(
        number: str,
        request: Request,
        body: bytes = Body(default=b"", media_type="application/octet-stream"),
        sha256: str | None = Query(
            default=None, description="Expected SHA-256 hex digest of the body"
        ),
        x_sha256: str | None = Header(
            default=None, description="Alternative to the sha256 query param"
        ),
    ):
        _validate_number(number)
        provided = (sha256 or x_sha256 or "").strip().lower()
        if not _SHA256_RE.fullmatch(provided):
            raise HTTPException(
                400,
                "missing or malformed sha256: pass ?sha256=<hex> or the "
                "X-Sha256 header (64 hex chars)",
            )
        content_length = request.headers.get("content-length")
        if (
            content_length is not None
            and content_length.isdigit()
            and int(content_length) > cfg.max_upload
        ):
            raise HTTPException(
                413, f"artifact exceeds the {cfg.max_upload}-byte limit"
            )
        if len(body) > cfg.max_upload:
            raise HTTPException(
                413, f"artifact exceeds the {cfg.max_upload}-byte limit"
            )
        digest = hashlib.sha256(body).hexdigest()
        if digest != provided:
            raise HTTPException(
                400, f"sha256 mismatch: declared {provided}, computed {digest}"
            )
        return store.upload(number, body, digest)

    @app.get(
        "/artifacts",
        response_model=list[ArtifactOut],
        summary="List all artifacts",
    )
    def list_artifacts():
        return store.list_artifacts()

    @app.get(
        "/artifacts/{number}",
        response_model=ArtifactOut,
        summary="Artifact metadata (downloadable only if record and file agree)",
    )
    def get_artifact(number: str):
        _validate_number(number)
        return store.get_artifact(number)

    @app.get(
        "/artifacts/{number}/content",
        summary="Download an artifact with a valid lease token",
        responses={200: {"content": {"application/octet-stream": {}}}},
    )
    def download_artifact(
        number: str,
        token: str | None = Query(default=None),
        x_cache_token: str | None = Header(default=None),
    ):
        _validate_number(number)
        tok = token or x_cache_token
        if not tok:
            raise HTTPException(
                401, "token required: pass ?token= or the X-Cache-Token header"
            )
        data, meta = store.download(number, tok)
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={
                "X-Sha256": meta["sha256"],
                "X-Artifact-Generation": str(meta["generation"]),
                "ETag": f'"{meta["sha256"]}"',
            },
        )

    # ---------------------------------------------------------------- tokens

    @app.post(
        "/artifacts/{number}/tokens",
        response_model=TokenOut,
        status_code=201,
        summary="Acquire a time-limited occupancy token (lease)",
    )
    def acquire_token(number: str):
        _validate_number(number)
        return store.acquire(number)

    @app.post(
        "/tokens/{token}/renew",
        response_model=TokenOut,
        summary="Renew a live token for another TTL",
    )
    def renew_token(token: str):
        return store.renew(token)

    @app.post(
        "/tokens/{token}/release",
        response_model=ReleaseOut,
        summary="Release a token (idempotent)",
    )
    def release_token(token: str):
        return store.release(token)

    @app.get(
        "/tokens/{token}",
        response_model=TokenStatusOut,
        summary="Inspect a token",
    )
    def token_status(token: str):
        return store.get_token(token)

    # --------------------------------------------------------------- reclaim

    @app.post(
        "/reclaim",
        response_model=ReclaimOut,
        summary="Free space: reclaim least-recently-used, unoccupied artifacts",
    )
    def reclaim(body: ReclaimIn):
        return store.reclaim(body.target_bytes)

    @app.post(
        "/admin/sweep",
        summary="Run recovery sweep now (interrupted reclaims, orphan files)",
    )
    def admin_sweep():
        return store.sweep()

    @app.get("/stats", summary="Cache statistics")
    def stats():
        return store.stats()

    return app


app = create_app()
