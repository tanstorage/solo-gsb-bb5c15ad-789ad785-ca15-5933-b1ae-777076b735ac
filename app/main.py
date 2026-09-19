"""断点分片上传协调 API。

仅协调元数据：不接收、不保存任何文件内容。
- 创建会话：记录总分片数与期望文件摘要
- 领取租约：同一分片同一时刻仅一个有效租约，过期后可重新领取
- 上报完成：需携带匹配租约令牌与分片摘要，重复上报幂等
- 提交会话：仅当全部分片完成，返回按序分片清单，否则列出缺失分片
- 状态查询：会话与逐分片状态
"""
from __future__ import annotations

import os
import secrets
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import JSONResponse

from .db import DB_LOCK, get_conn, init_db
from .schemas import (
    ChunkStatus,
    CompleteRequest,
    CompleteResponse,
    CreateSessionRequest,
    FinalizeResponse,
    LeaseRequest,
    LeaseResponse,
    ManifestEntry,
    SessionResponse,
    SessionStatusResponse,
)

DEFAULT_LEASE_TTL_SECONDS = int(os.environ.get("DEFAULT_LEASE_TTL_SECONDS", "300"))
MAX_LEASE_TTL_SECONDS = int(os.environ.get("MAX_LEASE_TTL_SECONDS", "3600"))


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()  # 启动时建库建表
    yield


app = FastAPI(
    title="Chunked Upload Coordinator",
    description="断点分片上传协调服务：只管理元数据（会话 / 租约 / 分片摘要），不接收文件内容。",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------- 工具函数

def get_session_or_404(session_id: str):
    row = get_conn().execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="session not found")
    return row


def get_chunk_or_404(session_id: str, chunk_index: int):
    row = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? AND chunk_index = ?",
        (session_id, chunk_index),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="chunk not found")
    return row


def lease_valid(chunk, now: datetime) -> bool:
    return (
        chunk["lease_expires_at"] is not None
        and parse_ts(chunk["lease_expires_at"]) > now
    )


def conflict(message: str, **extra) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": message, **extra})


# ---------------------------------------------------------------- 会话

@app.post(
    "/sessions",
    status_code=201,
    response_model=SessionResponse,
    tags=["sessions"],
    summary="创建上传会话",
)
def create_session(req: CreateSessionRequest):
    now = utcnow()
    session_id = uuid.uuid4().hex
    with DB_LOCK:
        conn = get_conn()
        conn.execute(
            "INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm, status, created_at)"
            " VALUES (?, ?, ?, ?, 'active', ?)",
            (session_id, req.total_chunks, req.file_digest, req.digest_algorithm, now.isoformat()),
        )
        conn.executemany(
            "INSERT INTO chunks (session_id, chunk_index) VALUES (?, ?)",
            [(session_id, i) for i in range(req.total_chunks)],
        )
        conn.commit()
    return SessionResponse(
        id=session_id,
        total_chunks=req.total_chunks,
        file_digest=req.file_digest,
        digest_algorithm=req.digest_algorithm,
        status="active",
        created_at=now,
    )


@app.get(
    "/sessions/{session_id}",
    response_model=SessionStatusResponse,
    tags=["sessions"],
    summary="查询会话状态",
)
def get_session_status(session_id: str):
    session = get_session_or_404(session_id)
    rows = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
        (session_id,),
    ).fetchall()
    now = utcnow()

    chunks: list[ChunkStatus] = []
    missing: list[int] = []
    for row in rows:
        status = row["status"]
        lease_expires_at = parse_ts(row["lease_expires_at"]) if row["lease_expires_at"] else None
        if status == "leased" and (lease_expires_at is None or lease_expires_at <= now):
            status = "pending"  # 租约已过期，读时按可领取展示
            lease_expires_at = None
        if row["status"] != "completed":
            missing.append(row["chunk_index"])
        chunks.append(
            ChunkStatus(
                chunk_index=row["chunk_index"],
                status=status,
                lease_expires_at=lease_expires_at,
                chunk_digest=row["chunk_digest"],
                completed_at=parse_ts(row["completed_at"]) if row["completed_at"] else None,
            )
        )

    return SessionStatusResponse(
        id=session["id"],
        status=session["status"],
        total_chunks=session["total_chunks"],
        completed_chunks=session["total_chunks"] - len(missing),
        missing_chunks=missing,
        file_digest=session["file_digest"],
        digest_algorithm=session["digest_algorithm"],
        created_at=parse_ts(session["created_at"]),
        finalized_at=parse_ts(session["finalized_at"]) if session["finalized_at"] else None,
        chunks=chunks,
    )


# ---------------------------------------------------------------- 租约

@app.post(
    "/sessions/{session_id}/chunks/{chunk_index}/lease",
    response_model=LeaseResponse,
    tags=["leases"],
    summary="领取分片租约（限时，独占）",
)
def claim_lease(
    session_id: str,
    chunk_index: int,
    req: Optional[LeaseRequest] = Body(default=None),
):
    ttl = DEFAULT_LEASE_TTL_SECONDS
    if req is not None and req.lease_ttl_seconds is not None:
        ttl = min(req.lease_ttl_seconds, MAX_LEASE_TTL_SECONDS)

    now = utcnow()
    with DB_LOCK:
        session = get_session_or_404(session_id)
        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")
        chunk = get_chunk_or_404(session_id, chunk_index)

        if chunk["status"] == "completed":
            raise HTTPException(status_code=409, detail="chunk already completed")
        if lease_valid(chunk, now):
            return conflict(
                "chunk lease already held",
                lease_expires_at=chunk["lease_expires_at"],
            )

        token = secrets.token_urlsafe(32)
        expires_at = now + timedelta(seconds=ttl)
        get_conn().execute(
            "UPDATE chunks SET status = 'leased', lease_token = ?, lease_expires_at = ?"
            " WHERE session_id = ? AND chunk_index = ?",
            (token, expires_at.isoformat(), session_id, chunk_index),
        )
        get_conn().commit()

    return LeaseResponse(
        session_id=session_id,
        chunk_index=chunk_index,
        lease_token=token,
        expires_at=expires_at,
    )


# ---------------------------------------------------------------- 完成上报

@app.post(
    "/sessions/{session_id}/chunks/{chunk_index}/complete",
    response_model=CompleteResponse,
    tags=["chunks"],
    summary="上报分片完成（幂等）",
)
def complete_chunk(session_id: str, chunk_index: int, req: CompleteRequest):
    now = utcnow()
    with DB_LOCK:
        session = get_session_or_404(session_id)
        chunk = get_chunk_or_404(session_id, chunk_index)

        if chunk["status"] == "completed":
            # 幂等重放：同一租约令牌 + 同一摘要 => 返回首次完成结果
            if chunk["lease_token"] == req.lease_token and chunk["chunk_digest"] == req.chunk_digest:
                return CompleteResponse(
                    session_id=session_id,
                    chunk_index=chunk_index,
                    status="completed",
                    chunk_digest=chunk["chunk_digest"],
                    completed_at=parse_ts(chunk["completed_at"]),
                    idempotent_replay=True,
                )
            raise HTTPException(
                status_code=409,
                detail="chunk already completed with different lease token or digest",
            )

        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")
        if chunk["lease_token"] is None or chunk["lease_token"] != req.lease_token:
            raise HTTPException(status_code=409, detail="lease token mismatch")
        if not lease_valid(chunk, now):
            raise HTTPException(status_code=409, detail="lease expired")

        get_conn().execute(
            "UPDATE chunks SET status = 'completed', chunk_digest = ?, completed_at = ?"
            " WHERE session_id = ? AND chunk_index = ?",
            (req.chunk_digest, now.isoformat(), session_id, chunk_index),
        )
        get_conn().commit()

    return CompleteResponse(
        session_id=session_id,
        chunk_index=chunk_index,
        status="completed",
        chunk_digest=req.chunk_digest,
        completed_at=now,
        idempotent_replay=False,
    )


# ---------------------------------------------------------------- 提交

@app.post(
    "/sessions/{session_id}/finalize",
    response_model=FinalizeResponse,
    tags=["sessions"],
    summary="提交会话（全部分片完成后）",
    responses={409: {"description": "存在缺失分片，响应体包含 missing_chunks"}},
)
def finalize_session(session_id: str):
    with DB_LOCK:
        session = get_session_or_404(session_id)
        rows = get_conn().execute(
            "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
            (session_id,),
        ).fetchall()

        missing = [r["chunk_index"] for r in rows if r["status"] != "completed"]
        if missing:
            return conflict("upload incomplete", missing_chunks=missing)

        now = utcnow()
        finalized_at = parse_ts(session["finalized_at"]) if session["finalized_at"] else None
        if session["status"] != "finalized":
            finalized_at = now
            get_conn().execute(
                "UPDATE sessions SET status = 'finalized', finalized_at = ? WHERE id = ?",
                (finalized_at.isoformat(), session_id),
            )
            get_conn().commit()
        # 已 finalized 时重复提交：幂等返回同一份清单

    return FinalizeResponse(
        session_id=session_id,
        status="finalized",
        file_digest=session["file_digest"],
        digest_algorithm=session["digest_algorithm"],
        total_chunks=session["total_chunks"],
        manifest=[
            ManifestEntry(
                chunk_index=r["chunk_index"],
                chunk_digest=r["chunk_digest"],
                completed_at=parse_ts(r["completed_at"]),
            )
            for r in rows
        ],
        finalized_at=finalized_at,
    )


# ---------------------------------------------------------------- 其他

@app.get("/healthz", tags=["meta"], summary="健康检查")
def healthz():
    get_conn().execute("SELECT 1")
    return {"status": "ok"}


@app.get("/", tags=["meta"], summary="服务信息")
def root():
    return {
        "service": "chunked-upload-coordinator",
        "docs": "/docs",
        "openapi": "/openapi.json",
        "default_lease_ttl_seconds": DEFAULT_LEASE_TTL_SECONDS,
        "max_lease_ttl_seconds": MAX_LEASE_TTL_SECONDS,
    }
