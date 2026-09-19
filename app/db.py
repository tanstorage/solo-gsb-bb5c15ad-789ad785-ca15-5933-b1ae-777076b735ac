"""SQLite 存储层：单连接 + 进程内写锁，启动时自动建库建表。"""
from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path

DB_PATH = os.environ.get("UPLOAD_DB_PATH", "./data/coordinator.db")

# 进程内写锁：SQLite 单写者，串行化所有状态变更，保证租约/完成/提交的原子性。
DB_LOCK = threading.RLock()

_conn: sqlite3.Connection | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id                TEXT PRIMARY KEY,
    total_chunks      INTEGER NOT NULL,
    file_digest       TEXT NOT NULL,
    digest_algorithm  TEXT NOT NULL DEFAULT 'sha256',
    status            TEXT NOT NULL DEFAULT 'active',   -- active | finalized
    created_at        TEXT NOT NULL,
    finalized_at      TEXT
);

CREATE TABLE IF NOT EXISTS chunks (
    session_id        TEXT NOT NULL REFERENCES sessions(id),
    chunk_index       INTEGER NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending',  -- pending | leased | completed
    lease_token       TEXT,
    lease_expires_at  TEXT,
    chunk_digest      TEXT,
    completed_at      TEXT,
    PRIMARY KEY (session_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_chunks_session ON chunks(session_id);

-- 分片依赖图：chunk_index 领取租约前，depends_on 必须已完成
CREATE TABLE IF NOT EXISTS chunk_dependencies (
    session_id        TEXT NOT NULL,
    chunk_index       INTEGER NOT NULL,
    depends_on        INTEGER NOT NULL,
    PRIMARY KEY (session_id, chunk_index, depends_on),
    FOREIGN KEY (session_id, chunk_index) REFERENCES chunks(session_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_chunk_deps_session ON chunk_dependencies(session_id);
"""


def get_conn() -> sqlite3.Connection:
    """返回进程级共享连接（惰性创建，首次调用时建库建表）。"""
    global _conn
    if _conn is None:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA foreign_keys=ON")
        _conn.execute("PRAGMA busy_timeout=5000")
        with DB_LOCK:
            _conn.executescript(SCHEMA)
            _conn.commit()
    return _conn


def init_db() -> None:
    """启动时显式初始化数据库。"""
    get_conn()
