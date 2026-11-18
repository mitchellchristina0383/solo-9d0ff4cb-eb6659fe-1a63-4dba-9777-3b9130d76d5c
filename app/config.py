"""Runtime configuration for the artifact cache service."""

from __future__ import annotations

import os
from dataclasses import dataclass

MIB = 1024 * 1024


@dataclass
class Config:
    db_path: str
    storage_dir: str
    token_ttl: float = 300.0          # seconds a lease token stays valid
    max_upload: int = MIB             # hard upload limit (1 MiB by spec)
    orphan_grace: float = 60.0        # seconds before an unreferenced file is swept
    sweep_interval: float = 30.0      # background sweep period; 0 disables it

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            db_path=os.environ.get("CACHE_DB_PATH", "./data/cache.db"),
            storage_dir=os.environ.get("CACHE_STORAGE_DIR", "./data/storage"),
            token_ttl=float(os.environ.get("TOKEN_TTL_SECONDS", "300")),
            max_upload=int(os.environ.get("MAX_UPLOAD_BYTES", str(MIB))),
            orphan_grace=float(os.environ.get("ORPHAN_GRACE_SECONDS", "60")),
            sweep_interval=float(os.environ.get("SWEEP_INTERVAL_SECONDS", "30")),
        )
