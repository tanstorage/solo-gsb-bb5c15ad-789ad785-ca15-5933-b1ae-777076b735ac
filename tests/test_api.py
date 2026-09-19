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


def make_session(total_chunks=3, file_digest="f" * 64, dependencies=None):
    body = {
        "total_chunks": total_chunks,
        "file_digest": file_digest,
        "digest_algorithm": "sha256",
    }
    if dependencies is not None:
        body["dependencies"] = dependencies
    resp = client.post("/sessions", json=body)
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


# ---------------------------------------------------------------- 依赖感知调度

def test_status_exposes_dependencies():
    # 1 -> 0, 2 -> 0；重复项与顺序在服务端归一化
    s = make_session(total_chunks=3, dependencies={"1": [0], "2": [0, 0]})
    st = client.get(f"/sessions/{s['id']}").json()
    deps = {c["chunk_index"]: c["dependencies"] for c in st["chunks"]}
    assert deps == {0: [], 1: [0], 2: [0]}


def test_lease_blocked_by_dependencies_and_unblocks_after_completion():
    s = make_session(total_chunks=3, dependencies={"1": [0], "2": [1, 0]})

    # 初始：0 可领取；1、2 被阻塞
    ready = client.get(f"/sessions/{s['id']}/ready").json()
    assert ready["ready"] == [0]
    assert ready["blocked"] == [
        {"chunk_index": 1, "missing_dependencies": [0]},
        {"chunk_index": 2, "missing_dependencies": [0, 1]},
    ]

    # 直接领取未就绪的分片 => 409，返回按序缺失前置
    r = lease(s["id"], 1)
    assert r.status_code == 409
    assert r.json()["missing_dependencies"] == [0]
    r = lease(s["id"], 2)
    assert r.status_code == 409
    assert r.json()["missing_dependencies"] == [0, 1]

    # 完成前置分片 0
    token0 = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token0, "0" * 64).status_code == 200

    # 1 自动就绪，无需额外操作；2 仍缺 1
    ready = client.get(f"/sessions/{s['id']}/ready").json()
    assert ready["ready"] == [1]
    assert ready["blocked"] == [
        {"chunk_index": 2, "missing_dependencies": [1]},
    ]
    token1 = lease(s["id"], 1).json()["lease_token"]
    assert complete(s["id"], 1, token1, "1" * 64).status_code == 200

    # 全部前置完成后 2 可领取
    ready = client.get(f"/sessions/{s['id']}/ready").json()
    assert ready["ready"] == [2]
    assert ready["blocked"] == []
    token2 = lease(s["id"], 2).json()["lease_token"]
    assert complete(s["id"], 2, token2, "2" * 64).status_code == 200

    # 全完成后 ready/blocked 均空，提交成功
    ready = client.get(f"/sessions/{s['id']}/ready").json()
    assert ready == {"session_id": s["id"], "ready": [], "blocked": []}
    assert client.post(f"/sessions/{s['id']}/finalize").status_code == 200


def test_ready_excludes_actively_leased_and_restores_expired():
    s = make_session(total_chunks=3, dependencies={"1": [0]})
    lease(s["id"], 0)  # 持有 0 的有效租约

    ready = client.get(f"/sessions/{s['id']}/ready").json()
    # 0 依赖已就绪但有效租约在身，不出现在 ready；1 被依赖阻塞；2 可领取
    assert ready["ready"] == [2]
    assert ready["blocked"] == [{"chunk_index": 1, "missing_dependencies": [0]}]

    # 租约过期后 0 恢复为可领取
    get_conn().execute(
        "UPDATE chunks SET lease_expires_at = '2000-01-01T00:00:00+00:00'"
        " WHERE session_id = ? AND chunk_index = 0",
        (s["id"],),
    )
    get_conn().commit()
    ready = client.get(f"/sessions/{s['id']}/ready").json()
    assert ready["ready"] == [0, 2]
    # 过期租约可被直接重新领取
    assert lease(s["id"], 0).status_code == 200


def test_dependency_validation_rejects_bad_graphs():
    def create(total, deps):
        return client.post(
            "/sessions",
            json={"total_chunks": total, "file_digest": "f" * 64, "dependencies": deps},
        )

    # 键越界
    assert create(3, {"3": [0]}).status_code == 422
    # 值越界
    assert create(3, {"0": [3]}).status_code == 422
    assert create(3, {"0": [-1]}).status_code == 422
    # 自依赖
    assert create(3, {"1": [1]}).status_code == 422
    # 直接环
    assert create(3, {"0": [1], "1": [0]}).status_code == 422
    # 间接环（0 -> 1 -> 2 -> 0）
    assert create(3, {"0": [2], "1": [0], "2": [1]}).status_code == 422
    # 依赖值非列表
    assert create(3, {"0": 1}).status_code == 422

    # 被拒绝的会话不落库：服务仍可创建无依赖会话
    assert make_session(total_chunks=1)["status"] == "active"


def test_chain_dependencies_finalize_end_to_end():
    # 链式 0 -> 无，1 -> 0，2 -> 1，配合就绪查询依次完成
    s = make_session(total_chunks=3, dependencies={"1": [0], "2": [1]})
    for i in range(3):
        ready = client.get(f"/sessions/{s['id']}/ready").json()
        assert ready["ready"] == [i]
        token = lease(s["id"], i).json()["lease_token"]
        assert complete(s["id"], i, token, str(i) * 64).status_code == 200

        # 中途提交：列出尚未完成的分片
        mid = client.post(f"/sessions/{s['id']}/finalize")
        if i < 2:
            assert mid.status_code == 409
            assert mid.json()["missing_chunks"] == list(range(i + 1, 3))

    # 全部完成后提交：清单按序汇总
    r = client.post(f"/sessions/{s['id']}/finalize")
    assert r.status_code == 200
    assert [m["chunk_index"] for m in r.json()["manifest"]] == [0, 1, 2]


def test_no_dependencies_keeps_original_behaviour():
    # 显式空依赖与省略 dependencies 等价
    for deps in (None, {}, {"2": []}):
        s = make_session(total_chunks=3, dependencies=deps)
        ready = client.get(f"/sessions/{s['id']}/ready").json()
        assert ready["ready"] == [0, 1, 2]
        assert ready["blocked"] == []
        st = client.get(f"/sessions/{s['id']}").json()
        assert all(c["dependencies"] == [] for c in st["chunks"])
