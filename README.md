# Dependency-Aware Task Lease API

基于 **Python + FastAPI + SQLite** 的本地任务调度服务：提交由任务与依赖边构成的
DAG，工作器原子领取「所有前置均已成功」的任务，凭借唯一租约令牌回报结果。

## 核心语义

- **DAG 校验**：提交时拒绝缺失依赖（`422 missing_dependency`）与环
  （`422 cycle_detected`，返回环上的节点序列），重复任务键同样被拒。
- **依赖门控的原子领取**：只有全部前置任务为 `succeeded` 的任务可被领取；
  领取在单事务 `BEGIN IMMEDIATE` 下完成，并发工作器中只有一个能拿到同一任务。
- **递增 attempt 与唯一租约令牌**：每次领取生成新的 `attempt`（从 1 起递增）
  和新的随机 `lease_token`，并写入 attempts 历史。
- **租约过期与旧令牌失效**：租约到期后任务可被再次领取（新尝试、新令牌）；
  旧令牌的续租（heartbeat）或结果回报一律返回 **409**，不可能覆盖新尝试。
- **重试**：任务可配置 `max_retries`（额外重试次数）。只有工作器**显式回报失败**
  （`success=false`）才原子递增独立的 `failure_count` 并消耗重试预算；租约过期后
  被重新领取只增加 `attempt`，不影响失败计数。失败后回到 `pending` 等待下一次领取；
  `failure_count` 达到上限后置为 `failed`，并递归阻塞所有直接/间接后继
  （状态 `blocked`，带 `blocked_reason`），后继永远无法被领取。
- **幂等回报**：回报接口必须携带客户端生成的 `completion_id`。相同
  `task_id + completion_id` 且 `lease_token`、`success`、`result`、`error`
  等价的请求只生效一次——即使任务此后已被重新领取，重放也逐字节返回首次响应
  （同样的 HTTP 状态码与响应体），不会再次递增 `failure_count` 或重复阻塞后继；
  相同键但内容不同返回 `409 idempotency_conflict`；不同键仍执行当前租约校验。
  JSON 对象键序不同不影响等价判定。幂等记录持久化在 SQLite 中，服务重启后仍有效。
- **剩余额度**：状态响应中的 `retries_remaining = max(0, max_retries - failure_count)`，
  与显式失败计数一致，不因租约过期重领而减少。
- **状态接口**：DAG 概览给出各状态计数；任务详情列出每次尝试的状态、租约令牌、
  过期时间、心跳时间、错误信息，以及当前租约是否已过期和阻塞原因。

## 一条命令启动（端口 8000）

```bash
docker compose up --build
```

服务启动后监听 **http://localhost:8000** ，SQLite 数据持久化在 `task-data` 卷中。
交互式文档（Swagger UI）： http://localhost:8000/docs

不用 Docker 时：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

可用环境变量：`DB_PATH`（默认 `./data/tasks.db`）、`DEFAULT_LEASE_SECONDS`
（默认 30，任务可用 `lease_seconds` 单独覆盖）。

## 调用示例

以下示例使用 `curl` 与 `jq`。

### 1. 提交 DAG

```bash
curl -s -X POST http://localhost:8000/dags \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "etl",
    "tasks": [
      {"key": "extract",   "lease_seconds": 60},
      {"key": "transform", "deps": ["extract"], "max_retries": 2, "lease_seconds": 60},
      {"key": "load",      "deps": ["transform"]}
    ]
  }'
```

非法图会被拒绝（422）：

```bash
# 缺失依赖
curl -i -X POST http://localhost:8000/dags -H 'Content-Type: application/json' \
  -d '{"tasks":[{"key":"a","deps":["ghost"]}]}'
# -> 422 {"error":{"code":"missing_dependency", ...}}

# 环
curl -i -X POST http://localhost:8000/dags -H 'Content-Type: application/json' \
  -d '{"tasks":[{"key":"a","deps":["c"]},{"key":"b","deps":["a"]},{"key":"c","deps":["b"]}]}'
# -> 422 {"error":{"code":"cycle_detected","details":{"cycle":["a","c","b","a"]}}}
```

### 2. 工作器领取任务

只有 `extract` 没有前置，可以被领取；`transform`/`load` 被门控：

```bash
curl -s -X POST http://localhost:8000/tasks/claim \
  -H 'Content-Type: application/json' \
  -d '{"worker_id": "worker-1", "dag_id": "<DAG_ID>", "limit": 10}'
```

```json
{
  "claimed": [
    {
      "task_id": "…", "dag_id": "…", "task_key": "extract",
      "attempt": 1,
      "lease_token": "f1a3…", "lease_expires_at": "2026-09-17T11:00:00Z",
      "lease_seconds": 60.0, "payload": null
    }
  ],
  "count": 1
}
```

### 3. 续租（心跳）

任务可能运行超过租约时长时，在到期前凭当前令牌续租：

```bash
curl -s -X POST http://localhost:8000/tasks/<TASK_ID>/heartbeat \
  -H 'Content-Type: application/json' \
  -d '{"lease_token": "f1a3…", "extend_seconds": 120}'
```

租约过期并被他人重新领取后，再用旧令牌续租会得到 `409 lease_expired` /
`409 invalid_lease`。

### 4. 回报结果

回报必须携带客户端生成的幂等键 `completion_id`（建议用 UUID 等全局唯一值）：

```bash
# 成功
curl -s -X POST http://localhost:8000/tasks/<TASK_ID>/complete \
  -H 'Content-Type: application/json' \
  -d '{"completion_id": "3f1c…", "lease_token": "f1a3…", "success": true, "result": {"rows": 42}}'

# 失败（仍有重试额度时回到 pending；额度耗尽则 failed 并阻塞后继）
curl -s -X POST http://localhost:8000/tasks/<TASK_ID>/complete \
  -H 'Content-Type: application/json' \
  -d '{"completion_id": "9b22…", "lease_token": "…", "success": false, "error": "connection reset"}'
```

幂等语义：

- 同一 `task_id + completion_id` 且 `lease_token`/`success`/`result`/`error`
  等价的重复请求（网络重试等）直接返回**首次响应**（同状态码、同响应体）；
  即使任务租约已过期并被其他工作器重新领取，重放也不经过当前租约校验，
  既不会重复递增 `failure_count`，也不会重复阻塞后继。
- 相同 `task_id + completion_id` 但上述任一字段不等价 →
  `409 {"error":{"code":"idempotency_conflict"}}`。`result` 为 JSON 对象时键序
  不同视为等价（规范化比较）。
- 不同的 `completion_id` 视为新回报，仍须通过当前租约校验（旧令牌照例 409）。
- 幂等记录持久化在 `task-data` 卷的 SQLite 中，服务/容器重启后重放仍然有效。

> 租约过期后旧令牌**首次**使用新 `completion_id` 的回报返回 **409**，
> 不会作用于已开始的新尝试；只有曾成功生效过的幂等键才会回放首次响应。

### 5. 状态查询

```bash
# DAG 概览（状态计数 + 任务列表，含阻塞原因）
curl -s http://localhost:8000/dags/<DAG_ID> | jq '.states, .tasks[] | {key,state,attempt,blocked_reason}'

# 任务详情（含每次尝试与当前租约）
curl -s http://localhost:8000/tasks/<TASK_ID> | jq

# 全局任务列表，可按 dag_id / state 过滤
curl -s 'http://localhost:8000/tasks?state=blocked'
```

任务详情示例：

```json
{
  "key": "transform", "state": "failed", "attempt": 3, "max_retries": 2,
  "lease": null, "blocked_reason": null,
  "attempts": [
    {"attempt": 1, "state": "failed",    "error": "connection reset", "lease_token": "…", "expires_at": "…", "heartbeat_at": null, "expired": false},
    {"attempt": 2, "state": "expired",   "error": null,               "lease_token": "…", "expires_at": "…", "heartbeat_at": null, "expired": true},
    {"attempt": 3, "state": "failed",    "error": "fatal",            "lease_token": "…", "expires_at": "…", "heartbeat_at": null, "expired": false}
  ]
}
```

被阻塞后继示例：`{"key": "load", "state": "blocked", "blocked_reason":
"upstream 'transform' permanently failed", "lease": null}`。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/dags` | 提交 DAG（校验缺失依赖与环） |
| `GET` | `/dags/{dag_id}` | DAG 概览：状态计数与任务列表 |
| `POST` | `/tasks/claim` | 原子领取前置全部成功的任务 |
| `GET` | `/tasks?dag_id=&state=` | 任务列表（可过滤） |
| `GET` | `/tasks/{task_id}` | 任务详情：尝试历史、租约、阻塞原因 |
| `POST` | `/tasks/{task_id}/heartbeat` | 凭当前租约令牌续租 |
| `POST` | `/tasks/{task_id}/complete` | 凭租约令牌 + `completion_id` 幂等回报成功/失败 |
| `GET` | `/health` | 健康检查 |

## 任务状态机

```
pending ──claim──▶ running ──complete(success)──────────▶ succeeded
  ▲                  │  │
  │ fail (可重试)    │  └─complete(fail, 重试耗尽)─▶ failed ──▶ blocked(所有后继)
  └──────────────────┘
                     └─租约过期─▶ 可被再次 claim（attempt+1，新令牌；旧令牌全部 409）
```

## 开发与测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

测试覆盖：DAG 校验（缺失依赖/环/重复键）、依赖门控、并发原子领取、
attempt 递增与令牌唯一、过期后旧令牌 409、心跳续租、重试与耗尽后递归阻塞、
幂等回报（重放同响应、内容冲突 409、键序等价、重启后持久化）、旧 SQLite 数据迁移、
状态接口。
