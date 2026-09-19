# 断点分片上传协调 API（Chunked Upload Coordinator）

只协调**元数据**的上传调度服务：创建会话、分片租约、完成上报、提交汇总与状态查询。
**不接收、不保存任何文件内容**，文件字节流由客户端自行点对点传输或写入对象存储。

技术栈：Python 3.12 · FastAPI · SQLite（WAL）

## 快速启动（单条命令）

```bash
docker compose up --build
```

启动后访问：

| 地址 | 说明 |
|---|---|
| `http://localhost:8000` | 服务信息 |
| `http://localhost:8000/docs` | Swagger 交互式 API 文档 |
| `http://localhost:8000/openapi.json` | OpenAPI 描述 |
| `http://localhost:8000/healthz` | 健康检查 |

依赖安装已纳入镜像构建，SQLite 建库建表在应用启动时自动完成，无需额外初始化步骤。
数据通过命名卷 `coordinator-data` 持久化，删除卷即清空全部状态。

## 配置（环境变量）

均可在 `docker-compose.yml` 的 `environment` 中调整：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `UPLOAD_DB_PATH` | `/data/coordinator.db` | SQLite 数据库文件路径 |
| `DEFAULT_LEASE_TTL_SECONDS` | `300` | 租约默认有效期（秒） |
| `MAX_LEASE_TTL_SECONDS` | `3600` | 租约有效期上限（秒），超出会被截断 |

端口映射默认为宿主机 `8000` → 容器 `8000`，可在 `docker-compose.yml` 的 `ports` 中修改。

## 核心语义

- **会话**：创建时登记 `total_chunks`（总分片数）与 `file_digest`（期望的完整文件摘要），分片序号从 `0` 开始。
- **依赖图（可选）**：创建会话时可提交 `dependencies`，键和值均为分片序号；例如 `{"2": [0, 1]}` 表示分片 2 领取前，分片 0 与 1 必须全部完成。服务端拒绝越界（序号超出 `[0, total_chunks-1]`）、自依赖和有环关系（返回 `422`，不产生会话），并将依赖图持久化。缺省、`null`、`{}` 或空列表表示无依赖，各分片立即可领取，行为与旧版本完全一致。
- **租约**：同一分片同一时刻仅允许一个有效租约；租约过期后可被重新领取（令牌轮换）。前置分片未完成时领取返回 `409`，并在 `missing_dependencies` 中按序列出尚未完成的前置分片；前置全部完成后无需任何额外操作即可领取。
- **就绪查询**：`GET /sessions/{id}/ready` 返回当前可领取（无未完成前置且无有效租约）的分片 `ready`，以及被依赖阻塞的分片 `blocked`（含各自未完成的前置项）；租约过期后分片自动恢复为可领取。
- **完成上报**：必须携带该分片**当前有效租约的令牌**与分片摘要；以相同令牌 + 相同摘要重复上报为幂等成功（返回 `idempotent_replay: true`），摘要不同则返回 `409`。
- **提交**：仅当全部分片完成才允许提交，返回按分片序号升序汇总的清单；否则返回 `409` 及 `missing_chunks` 缺失分片列表。重复提交幂等返回同一清单。
- 已完成的分片不可再领取租约；已提交的会话进入只读状态。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/sessions` | 创建上传会话（可选提交 `dependencies` 依赖图） |
| `GET` | `/sessions/{session_id}` | 查询会话与逐分片状态（含每分片 `dependencies`） |
| `GET` | `/sessions/{session_id}/ready` | 查询可领取分片与被依赖阻塞的分片 |
| `POST` | `/sessions/{session_id}/chunks/{index}/lease` | 领取分片租约（前置未完成返回 `409 + missing_dependencies`） |
| `POST` | `/sessions/{session_id}/chunks/{index}/complete` | 上报分片完成（幂等） |
| `POST` | `/sessions/{session_id}/finalize` | 提交会话，返回按序分片清单 |
| `GET` | `/healthz` | 健康检查 |

## 调用示例（curl）

```bash
# 1. 创建会话：4 个分片，登记期望的文件 SHA-256
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 4, "file_digest": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "digest_algorithm": "sha256"}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 2. 领取分片 0 的租约（有效期 120 秒）
LEASE=$(curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/lease \
  -H 'Content-Type: application/json' -d '{"lease_ttl_seconds": 120}')
TOKEN=$(echo "$LEASE" | python3 -c 'import sys, json; print(json.load(sys.stdin)["lease_token"])')

# 3. 客户端自行传输该分片后，上报完成（携带租约令牌与分片摘要）
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/complete \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$TOKEN\", \"chunk_digest\": \"<分片0的SHA256>\"}"

# 4. 对其余分片重复 2-3 步，然后提交会话（缺分片时返回 409 + missing_chunks）
curl -s -X POST http://localhost:8000/sessions/$SID/finalize

# 5. 随时查询状态（每个分片都带有 dependencies 字段）
curl -s http://localhost:8000/sessions/$SID
```

### 依赖感知调度示例

要求分片 1 必须在分片 0 完成后领取，分片 2 必须在 0、1 都完成后领取：

```bash
# 1. 创建会话时提交依赖图（键与值均为分片序号；省略 dependencies 则无依赖）
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 3, "file_digest": "<文件SHA256>",
       "dependencies": {"1": [0], "2": [0, 1]}}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 2. 查询就绪情况：0 可领取，1/2 被阻塞并给出未完成前置
curl -s http://localhost:8000/sessions/$SID/ready
# {"session_id":"...","ready":[0],"blocked":[
#   {"chunk_index":1,"missing_dependencies":[0]},
#   {"chunk_index":2,"missing_dependencies":[0,1]}]}

# 3. 直接领取未就绪的分片会得到 409，响应体按序列出缺失前置
curl -s -i -X POST http://localhost:8000/sessions/$SID/chunks/2/lease \
  -H 'Content-Type: application/json' -d '{}'
# HTTP/1.1 409 Conflict
# {"detail":"chunk dependencies not satisfied","missing_dependencies":[0,1]}

# 4. 依次领取并完成 0；完成后 1 自动出现在 ready 中，无需任何额外操作
#    （租约若过期，对应分片也会自动恢复为 ready，可重新领取）
curl -s http://localhost:8000/sessions/$SID/ready
# {"session_id":"...","ready":[1],"blocked":[
#   {"chunk_index":2,"missing_dependencies":[1]}]}
```

非法依赖图（越界、自依赖、环）在创建时即被拒绝，返回 `422` 且不产生会话，例如 `{"1": [1]}`（自依赖）或 `{"0": [1], "1": [0]}`（环）。

## 本地开发（不用 Docker）

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

运行测试（需额外安装 `pytest`、`httpx`）：

```bash
python3 -m pytest tests/ -q
```

## 项目结构

```
app/
  main.py      # FastAPI 应用与全部端点
  db.py        # SQLite 连接与建库建表（启动时自动执行）
  schemas.py   # Pydantic 请求/响应模型
tests/
  test_api.py  # 租约独占/过期、幂等、提交、依赖图校验/阻塞/就绪等端到端行为测试
Dockerfile
docker-compose.yml
requirements.txt
```
