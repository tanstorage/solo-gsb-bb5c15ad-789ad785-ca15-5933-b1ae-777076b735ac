"""API 行为测试：租约独占/过期、幂等上报、提交与状态查询。"""
import os
import tempfile

# 必须在导入 app 之前指定测试数据库
os.environ["UPLOAD_DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test.db")
os.environ["DEFAULT_LEASE_TTL_SECONDS"] = "300"

from fastapi.testclient import TestClient  # noqa: E402

from app.db import get_conn  # noqa: E402
from app.main import app  # noqa: E402

client = TestClient(app)


def make_session(total_chunks=3, file_digest="f" * 64):
    resp = client.post("/sessions", json={
        "total_chunks": total_chunks,
        "file_digest": file_digest,
        "digest_algorithm": "sha256",
    })
    assert resp.status_code == 201, resp.text
    return resp.json()


def lease(session_id, index, ttl=None):
    body = {} if ttl is None else {"lease_ttl_seconds": ttl}
    return client.post(f"/sessions/{session_id}/chunks/{index}/lease", json=body)


def complete(session_id, index, token, digest):
    return client.post(
        f"/sessions/{session_id}/chunks/{index}/complete",
        json={"lease_token": token, "chunk_digest": digest},
    )


def test_create_session_and_status():
    s = make_session(total_chunks=4)
    assert s["status"] == "active"
    assert s["total_chunks"] == 4

    st = client.get(f"/sessions/{s['id']}").json()
    assert st["completed_chunks"] == 0
    assert st["missing_chunks"] == [0, 1, 2, 3]
    assert [c["status"] for c in st["chunks"]] == ["pending"] * 4


def test_lease_is_exclusive_until_expiry():
    s = make_session()
    r1 = lease(s["id"], 0)
    assert r1.status_code == 200
    token1 = r1.json()["lease_token"]
    assert token1

    # 同一分片在租约有效期内不可再次领取
    r2 = lease(s["id"], 0)
    assert r2.status_code == 409

    # 手动让租约过期后，可被重新领取
    get_conn().execute(
        "UPDATE chunks SET lease_expires_at = '2000-01-01T00:00:00+00:00'"
        " WHERE session_id = ? AND chunk_index = 0",
        (s["id"],),
    )
    get_conn().commit()
    r3 = lease(s["id"], 0)
    assert r3.status_code == 200
    assert r3.json()["lease_token"] != token1


def test_complete_requires_valid_lease_and_token():
    s = make_session()
    # 未领取租约直接上报
    assert complete(s["id"], 0, "no-such-token", "a" * 64).status_code == 409

    token = lease(s["id"], 0).json()["lease_token"]
    # 令牌不匹配
    assert complete(s["id"], 0, "wrong-token", "a" * 64).status_code == 409
    # 正确令牌
    assert complete(s["id"], 0, token, "a" * 64).status_code == 200


def test_expired_lease_cannot_complete():
    s = make_session()
    token = lease(s["id"], 0).json()["lease_token"]
    get_conn().execute(
        "UPDATE chunks SET lease_expires_at = '2000-01-01T00:00:00+00:00'"
        " WHERE session_id = ? AND chunk_index = 0",
        (s["id"],),
    )
    get_conn().commit()
    assert complete(s["id"], 0, token, "a" * 64).status_code == 409


def test_complete_is_idempotent():
    s = make_session()
    token = lease(s["id"], 1).json()["lease_token"]
    digest = "b" * 64

    r1 = complete(s["id"], 1, token, digest)
    assert r1.status_code == 200
    assert r1.json()["idempotent_replay"] is False

    # 相同令牌 + 相同摘要：幂等成功，completed_at 不变
    r2 = complete(s["id"], 1, token, digest)
    assert r2.status_code == 200
    assert r2.json()["idempotent_replay"] is True
    assert r2.json()["completed_at"] == r1.json()["completed_at"]

    # 相同令牌但摘要不同：冲突
    assert complete(s["id"], 1, token, "c" * 64).status_code == 409
    # 已完成分片不可再领取租约
    assert lease(s["id"], 1).status_code == 409


def test_finalize_blocked_until_all_chunks_done():
    s = make_session(total_chunks=3)
    token = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token, "0" * 64).status_code == 200

    r = client.post(f"/sessions/{s['id']}/finalize")
    assert r.status_code == 409
    assert r.json()["missing_chunks"] == [1, 2]

    for i in (1, 2):
        t = lease(s["id"], i).json()["lease_token"]
        assert complete(s["id"], i, t, str(i) * 64).status_code == 200

    ok = client.post(f"/sessions/{s['id']}/finalize")
    assert ok.status_code == 200
    body = ok.json()
    assert body["status"] == "finalized"
    assert body["file_digest"] == "f" * 64
    # 清单按分片序号升序汇总
    assert [m["chunk_index"] for m in body["manifest"]] == [0, 1, 2]
    assert [m["chunk_digest"] for m in body["manifest"]] == [str(i) * 64 for i in range(3)]

    # 重复提交幂等
    again = client.post(f"/sessions/{s['id']}/finalize")
    assert again.status_code == 200
    assert again.json()["manifest"] == body["manifest"]

    # 已提交会话不可再领取租约
    assert lease(s["id"], 0).status_code == 409

    st = client.get(f"/sessions/{s['id']}").json()
    assert st["status"] == "finalized"
    assert st["missing_chunks"] == []
    assert st["completed_chunks"] == 3


def test_unknown_session_and_chunk():
    assert client.get("/sessions/nope").status_code == 404
    s = make_session()
    assert lease(s["id"], 99).status_code == 404
    assert complete(s["id"], 99, "t", "d").status_code == 404


def test_validation():
    assert client.post("/sessions", json={"total_chunks": 0, "file_digest": "x"}).status_code == 422
    assert client.post("/sessions", json={"total_chunks": 1}).status_code == 422


# ---------------------------------------------------------------- 依赖调度

def make_session_with_deps(total_chunks, dependencies):
    resp = client.post("/sessions", json={
        "total_chunks": total_chunks,
        "file_digest": "f" * 64,
        "digest_algorithm": "sha256",
        "dependencies": {str(k): v for k, v in dependencies.items()},
    })
    assert resp.status_code == 201, resp.text
    return resp.json()


def ready(session_id):
    resp = client.get(f"/sessions/{session_id}/ready")
    assert resp.status_code == 200, resp.text
    return resp.json()


def expire_lease(session_id, index):
    get_conn().execute(
        "UPDATE chunks SET lease_expires_at = '2000-01-01T00:00:00+00:00'"
        " WHERE session_id = ? AND chunk_index = ?",
        (session_id, index),
    )
    get_conn().commit()


def create_session_raw(payload):
    return client.post("/sessions", json={"file_digest": "f" * 64, **payload})


def test_dependencies_rejected_when_invalid():
    # 键越界
    assert create_session_raw({"total_chunks": 3, "dependencies": {"3": [0]}}).status_code == 422
    assert create_session_raw({"total_chunks": 3, "dependencies": {"-1": [0]}}).status_code == 422
    # 值越界
    assert create_session_raw({"total_chunks": 3, "dependencies": {"1": [3]}}).status_code == 422
    assert create_session_raw({"total_chunks": 3, "dependencies": {"1": [-2]}}).status_code == 422
    # 自依赖
    assert create_session_raw({"total_chunks": 3, "dependencies": {"1": [1]}}).status_code == 422
    # 环：二元环与三元环
    assert create_session_raw(
        {"total_chunks": 3, "dependencies": {"0": [1], "1": [0]}}).status_code == 422
    assert create_session_raw(
        {"total_chunks": 3, "dependencies": {"0": [1], "1": [2], "2": [0]}}).status_code == 422
    # 非数值键
    assert create_session_raw({"total_chunks": 3, "dependencies": {"x": [0]}}).status_code == 422
    # 合法依赖（含重复值，服务端去重）可创建
    assert create_session_raw(
        {"total_chunks": 3, "dependencies": {"2": [0, 1, 1]}}).status_code == 201


def test_lease_blocked_until_dependencies_complete():
    s = make_session_with_deps(3, {2: [0, 1]})

    # 前置全部未完成：409 + 按序缺失前置
    r = lease(s["id"], 2)
    assert r.status_code == 409
    assert r.json()["missing_dependencies"] == [0, 1]

    # 完成一个前置后仍被阻塞，缺失列表随之缩小
    t0 = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, t0, "0" * 64).status_code == 200
    r = lease(s["id"], 2)
    assert r.status_code == 409
    assert r.json()["missing_dependencies"] == [1]

    # 前置全部完成后无需额外操作即可领取
    t1 = lease(s["id"], 1).json()["lease_token"]
    assert complete(s["id"], 1, t1, "1" * 64).status_code == 200
    assert lease(s["id"], 2).status_code == 200

    # 无依赖的分片不受影响
    s2 = make_session()
    assert lease(s2["id"], 2).status_code == 200


def test_status_exposes_dependencies():
    s = make_session_with_deps(3, {2: [0, 1]})
    st = client.get(f"/sessions/{s['id']}").json()
    assert [c["dependencies"] for c in st["chunks"]] == [[], [], [0, 1]]

    # 未提供依赖的会话：每个分片 dependencies 为空
    st2 = client.get(f"/sessions/{make_session()['id']}").json()
    assert [c["dependencies"] for c in st2["chunks"]] == [[], [], []]


def test_ready_endpoint_lists_ready_and_blocked():
    s = make_session_with_deps(4, {2: [0], 3: [1, 2]})

    # 初始：0、1 可领取；2 等 0，3 等 1 和 2
    r = ready(s["id"])
    assert r["ready_chunks"] == [0, 1]
    assert r["blocked_chunks"] == [
        {"chunk_index": 2, "missing_dependencies": [0]},
        {"chunk_index": 3, "missing_dependencies": [1, 2]},
    ]

    # 持有有效租约的分片不再出现在 ready 中
    t0 = lease(s["id"], 0).json()["lease_token"]
    assert ready(s["id"])["ready_chunks"] == [1]

    # 完成 0 后 2 解锁
    assert complete(s["id"], 0, t0, "0" * 64).status_code == 200
    r = ready(s["id"])
    assert r["ready_chunks"] == [1, 2]
    assert r["blocked_chunks"] == [{"chunk_index": 3, "missing_dependencies": [1, 2]}]

    # 链式推进：完成 1、2 后 3 解锁
    t1 = lease(s["id"], 1).json()["lease_token"]
    assert complete(s["id"], 1, t1, "1" * 64).status_code == 200
    t2 = lease(s["id"], 2).json()["lease_token"]
    assert complete(s["id"], 2, t2, "2" * 64).status_code == 200
    r = ready(s["id"])
    assert r["ready_chunks"] == [3]
    assert r["blocked_chunks"] == []


def test_ready_endpoint_treats_expired_lease_as_claimable():
    s = make_session_with_deps(2, {1: [0]})

    lease(s["id"], 0)
    assert ready(s["id"])["ready_chunks"] == []

    # 租约过期后恢复为可领取
    expire_lease(s["id"], 0)
    assert ready(s["id"])["ready_chunks"] == [0]

    # 无依赖会话：全部 pending 分片均可领取，无阻塞
    s2 = make_session()
    r = ready(s2["id"])
    assert r["ready_chunks"] == [0, 1, 2]
    assert r["blocked_chunks"] == []


def test_ready_unknown_session():
    assert client.get("/sessions/nope/ready").status_code == 404


def test_finalize_with_dependencies():
    s = make_session_with_deps(2, {1: [0]})
    # 被依赖阻塞时无法完成后续分片，提交自然列出缺失
    r = client.post(f"/sessions/{s['id']}/finalize")
    assert r.status_code == 409
    assert r.json()["missing_chunks"] == [0, 1]

    for i in (0, 1):
        t = lease(s["id"], i).json()["lease_token"]
        assert complete(s["id"], i, t, str(i) * 64).status_code == 200
    assert client.post(f"/sessions/{s['id']}/finalize").status_code == 200
