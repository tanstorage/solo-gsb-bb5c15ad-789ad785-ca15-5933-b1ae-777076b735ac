"""Pydantic 请求/响应模型。"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class CreateSessionRequest(BaseModel):
    total_chunks: int = Field(ge=1, le=100_000, description="总分片数")
    file_digest: str = Field(min_length=1, max_length=256, description="期望的完整文件摘要")
    digest_algorithm: str = Field(default="sha256", min_length=1, max_length=32,
                                  description="摘要算法标识，仅作记录")


class SessionResponse(BaseModel):
    id: str
    total_chunks: int
    file_digest: str
    digest_algorithm: str
    status: str
    created_at: datetime
    finalized_at: Optional[datetime] = None


class LeaseRequest(BaseModel):
    lease_ttl_seconds: Optional[int] = Field(
        default=None, ge=1,
        description="租约有效期（秒），缺省取服务端默认值，超过服务端上限会被截断")


class LeaseResponse(BaseModel):
    session_id: str
    chunk_index: int
    lease_token: str
    expires_at: datetime


class CompleteRequest(BaseModel):
    lease_token: str = Field(min_length=1, max_length=128, description="当前有效租约令牌")
    chunk_digest: str = Field(min_length=1, max_length=256, description="该分片的摘要")


class CompleteResponse(BaseModel):
    session_id: str
    chunk_index: int
    status: str
    chunk_digest: str
    completed_at: datetime
    idempotent_replay: bool = Field(default=False, description="是否为幂等重放")


class ManifestEntry(BaseModel):
    chunk_index: int
    chunk_digest: str
    completed_at: datetime


class FinalizeResponse(BaseModel):
    session_id: str
    status: str
    file_digest: str
    digest_algorithm: str
    total_chunks: int
    manifest: list[ManifestEntry]
    finalized_at: datetime


class ChunkStatus(BaseModel):
    chunk_index: int
    status: str  # pending | leased | completed（过期租约在读时按 pending 展示）
    lease_expires_at: Optional[datetime] = None
    chunk_digest: Optional[str] = None
    completed_at: Optional[datetime] = None


class SessionStatusResponse(BaseModel):
    id: str
    status: str
    total_chunks: int
    completed_chunks: int
    missing_chunks: list[int]
    file_digest: str
    digest_algorithm: str
    created_at: datetime
    finalized_at: Optional[datetime] = None
    chunks: list[ChunkStatus]
