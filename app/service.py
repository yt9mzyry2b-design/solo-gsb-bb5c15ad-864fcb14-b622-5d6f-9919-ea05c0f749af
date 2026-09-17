"""核心业务逻辑：提交校验、原子领取、租约心跳、结果回报与状态查询。"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from .db import get_conn, get_default_lease_seconds

# 任务状态
PENDING = "pending"
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"
BLOCKED = "blocked"

# 尝试状态
A_ACTIVE = "active"
A_EXPIRED = "expired"
A_SUCCEEDED = "succeeded"
A_FAILED = "failed"


class APIError(Exception):
    def __init__(self, status_code: int, code: str, message: str, details: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details


class ReplayedResponse:
    """幂等命中：携带首次有效回报保存的 HTTP 状态码与原始响应体。

    路由层必须将其原样回放（同样的状态码、逐字节的响应体），不得重新序列化。
    """

    def __init__(self, status_code: int, body: str):
        self.status_code = status_code
        self.body = body


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat().replace("+00:00", "Z") if dt is not None else None


def _parse(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _dumps(value: Any) -> str | None:
    return None if value is None else json.dumps(value, ensure_ascii=False)


def _canonical(value: Any) -> str:
    """规范化 JSON：递归排序对象键，使键序不同但内容相同的请求视为等价。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


# ---------------------------------------------------------------- DAG 提交

def _find_cycle(keys: set[str], deps: dict[str, list[str]]) -> list[str]:
    """DFS 检测环，返回构成环的任务键序列（首尾相同），无环返回 []。"""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {k: WHITE for k in keys}
    stack: list[str] = []

    def visit(node: str) -> list[str]:
        color[node] = GRAY
        stack.append(node)
        for dep in deps[node]:
            if dep not in keys:  # 缺失依赖已单独处理
                continue
            if color[dep] == GRAY:
                start = stack.index(dep)
                return stack[start:] + [dep]
            if color[dep] == WHITE:
                found = visit(dep)
                if found:
                    return found
        stack.pop()
        color[node] = BLACK
        return []

    for key in keys:
        if color[key] == WHITE:
            found = visit(key)
            if found:
                return found
    return []


def submit_dag(body: dict[str, Any]) -> dict[str, Any]:
    raw_tasks = body["tasks"]
    name = body.get("name") or "dag"
    dag_id = body.get("id") or uuid.uuid4().hex

    keys: list[str] = []
    seen: set[str] = set()
    duplicates: list[str] = []
    deps_map: dict[str, list[str]] = {}
    tasks_by_key: dict[str, dict[str, Any]] = {}

    for t in raw_tasks:
        key = t["key"]
        if key in seen:
            duplicates.append(key)
            continue
        seen.add(key)
        keys.append(key)
        deps_map[key] = list(dict.fromkeys(t.get("deps") or []))
        tasks_by_key[key] = t

    if duplicates:
        raise APIError(422, "duplicate_task_key", "任务键在同一 DAG 中重复", {"keys": duplicates})

    # 缺失依赖（包含指向不存在任务的边）
    missing = sorted({d for d_list in deps_map.values() for d in d_list if d not in seen})
    if missing:
        raise APIError(
            422, "missing_dependency", "存在指向未声明任务的依赖边", {"missing": missing}
        )

    # 环检测
    cycle = _find_cycle(seen, deps_map)
    if cycle:
        raise APIError(422, "cycle_detected", "依赖图存在环，必须是 DAG", {"cycle": cycle})

    default_lease = get_default_lease_seconds()
    now = _iso(_now())

    with get_conn() as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO dags (id, name, created_at) VALUES (?, ?, ?)",
                (dag_id, name, now),
            )
        except Exception:
            conn.execute("ROLLBACK")
            raise APIError(409, "dag_exists", f"DAG id {dag_id} 已存在")

        try:
            for key in keys:
                t = tasks_by_key[key]
                lease = float(t.get("lease_seconds") or default_lease)
                conn.execute(
                    """INSERT INTO tasks (id, dag_id, task_key, name, payload,
                           max_retries, lease_seconds, state, attempt, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)""",
                    (
                        uuid.uuid4().hex,
                        dag_id,
                        key,
                        t.get("name") or key,
                        _dumps(t.get("payload")),
                        int(t.get("max_retries") or 0),
                        lease,
                        PENDING,
                        now,
                    ),
                )
                for dep in deps_map[key]:
                    conn.execute(
                        "INSERT INTO dependencies (dag_id, task_key, dep_key) VALUES (?, ?, ?)",
                        (dag_id, key, dep),
                    )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return get_dag(dag_id)


# ---------------------------------------------------------------- 原子领取

_CLAIMABLE_SQL = """
    SELECT t.*
    FROM tasks t
    WHERE (:dag_id IS NULL OR t.dag_id = :dag_id)
      AND (
            -- 从未被领取
            (t.state = 'pending' AND t.attempt = 0)
            -- 重试后重新排队
            OR (t.state = 'pending' AND t.attempt > 0)
            -- 租约已过期，可被（重新）领取，产生新的尝试
            OR (t.state = 'running' AND t.lease_expires_at IS NOT NULL
                AND t.lease_expires_at < :now)
          )
      AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks p ON p.dag_id = d.dag_id AND p.task_key = d.dep_key
            WHERE d.dag_id = t.dag_id AND d.task_key = t.task_key
              AND p.state <> 'succeeded'
          )
    ORDER BY t.created_at, t.id
    LIMIT :limit
"""


def claim_tasks(worker_id: str | None, dag_id: str | None, limit: int) -> dict[str, Any]:
    if dag_id is not None:
        with get_conn() as conn:
            if not conn.execute("SELECT 1 FROM dags WHERE id = ?", (dag_id,)).fetchone():
                raise APIError(404, "dag_not_found", f"DAG {dag_id} 不存在")

    claimed: list[dict[str, Any]] = []
    now = _now()

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")  # 保留锁：同一时刻只有一个领取者
        try:
            rows = conn.execute(
                _CLAIMABLE_SQL,
                {"dag_id": dag_id, "now": _iso(now), "limit": limit},
            ).fetchall()

            for row in rows:
                attempt_no = row["attempt"] + 1
                token = uuid.uuid4().hex
                expires = now + timedelta(seconds=row["lease_seconds"])

                # 旧尝试（若存在）标记过期，保证历史尝试不再有效
                conn.execute(
                    "UPDATE attempts SET state = 'expired' WHERE task_id = ? AND state = 'active'",
                    (row["id"],),
                )
                cur = conn.execute(
                    """UPDATE tasks
                       SET state = 'running', attempt = ?, lease_token = ?,
                           lease_expires_at = ?, worker_id = ?, started_at = COALESCE(started_at, ?)
                       WHERE id = ?""",
                    (attempt_no, token, _iso(expires), worker_id, _iso(now), row["id"]),
                )
                if cur.rowcount != 1:  # 被并发事务改动则放弃本任务
                    continue
                conn.execute(
                    """INSERT INTO attempts (task_id, attempt_no, lease_token, worker_id,
                           leased_at, expires_at, state)
                       VALUES (?, ?, ?, ?, ?, ?, 'active')""",
                    (row["id"], attempt_no, token, worker_id, _iso(now), _iso(expires)),
                )
                claimed.append(
                    {
                        "task_id": row["id"],
                        "dag_id": row["dag_id"],
                        "task_key": row["task_key"],
                        "name": row["name"],
                        "payload": json.loads(row["payload"]) if row["payload"] else None,
                        "attempt": attempt_no,
                        "lease_token": token,
                        "lease_expires_at": _iso(expires),
                        "lease_seconds": row["lease_seconds"],
                    }
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return {"claimed": claimed, "count": len(claimed)}


# ---------------------------------------------------------------- 租约工具

def _load_for_token(conn, lease_token: str) -> dict[str, Any]:
    """校验令牌并返回任务行；令牌错误/任务已不在租约中一律 409。"""
    row = conn.execute("SELECT * FROM tasks WHERE lease_token = ?", (lease_token,)).fetchone()
    if row is None:
        raise APIError(
            409, "invalid_lease", "租约令牌无效：租约已过期、被新尝试取代或任务已结束"
        )
    return row


def _assert_active(row) -> None:
    expires = _parse(row["lease_expires_at"])
    if expires is not None and expires <= _now():
        raise APIError(
            409,
            "lease_expired",
            f"租约已于 {row['lease_expires_at']} 过期，该任务已可被重新领取",
        )


def heartbeat(task_id: str, body: dict[str, Any]) -> dict[str, Any]:
    token = body["lease_token"]
    extend = body.get("extend_seconds")
    now = _now()

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = _load_for_token(conn, token)
            if row["id"] != task_id:
                raise APIError(409, "invalid_lease", "租约令牌与任务不匹配或已失效")
            _assert_active(row)
            seconds = float(extend) if extend is not None else row["lease_seconds"]
            new_expires = now + timedelta(seconds=seconds)
            conn.execute(
                "UPDATE tasks SET lease_expires_at = ? WHERE id = ?",
                (_iso(new_expires), row["id"]),
            )
            conn.execute(
                """UPDATE attempts SET expires_at = ?, heartbeat_at = ?
                   WHERE lease_token = ? AND state = 'active'""",
                (_iso(new_expires), _iso(now), token),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return {
        "task_id": row["id"],
        "attempt": row["attempt"],
        "lease_token": token,
        "lease_expires_at": _iso(new_expires),
    }


# ---------------------------------------------------------------- 结果回报

def _block_descendants(conn, dag_id: str, root_key: str) -> int:
    """永久失败后，递归阻塞全部直接/间接后继。返回阻塞任务数。"""
    rows = conn.execute(
        """
        WITH RECURSIVE descendants(task_key) AS (
            SELECT task_key
            FROM dependencies
            WHERE dag_id = ? AND dep_key = ?
            UNION
            SELECT d.task_key
            FROM dependencies d
            JOIN descendants ds ON d.dag_id = ? AND d.dep_key = ds.task_key
        )
        SELECT DISTINCT task_key FROM descendants
        """,
        (dag_id, root_key, dag_id),
    ).fetchall()

    count = 0
    for r in rows:
        target = conn.execute(
            "SELECT * FROM tasks WHERE dag_id = ? AND task_key = ?",
            (dag_id, r["task_key"]),
        ).fetchone()
        # 已结束的任务不改变；运行中的任务即使租约有效也视为被阻塞
        if target["state"] in (SUCCEEDED, FAILED, BLOCKED):
            continue
        reason = f"upstream '{root_key}' permanently failed"
        conn.execute(
            """UPDATE tasks
               SET state = 'blocked', blocked_reason = ?,
                   lease_token = NULL, lease_expires_at = NULL, worker_id = NULL
               WHERE id = ?""",
            (reason, target["id"]),
        )
        conn.execute(
            "UPDATE attempts SET state = 'expired' WHERE task_id = ? AND state = 'active'",
            (target["id"],),
        )
        count += 1
    return count


def complete_task(task_id: str, body: dict[str, Any]) -> dict[str, Any] | ReplayedResponse:
    completion_id = body["completion_id"]
    token = body["lease_token"]
    success = bool(body["success"])
    result = body.get("result")
    error = body.get("error")
    # 规范化指纹：JSON 对象键序不影响等价判定
    fingerprint = _canonical(
        {"lease_token": token, "success": success, "result": result, "error": error}
    )
    now = _now()

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            # 幂等记录必须在同一保留事务内检查：并发同键回报只有一个能成为首次
            existing = conn.execute(
                "SELECT * FROM completions WHERE task_id = ? AND completion_id = ?",
                (task_id, completion_id),
            ).fetchone()
            if existing is not None:
                # 重放优先于当前租约状态：任务之后是否被重新领取都不影响回放
                conn.execute("ROLLBACK")
                if existing["fingerprint"] != fingerprint:
                    raise APIError(
                        409,
                        "idempotency_conflict",
                        "相同 task_id + completion_id 已用于内容不同的回报",
                        {
                            "task_id": task_id,
                            "completion_id": completion_id,
                        },
                    )
                return ReplayedResponse(existing["status_code"], existing["response_body"])

            # 不同键（或首次请求）：执行当前租约校验
            row = _load_for_token(conn, token)
            if row["id"] != task_id:
                raise APIError(409, "invalid_lease", "租约令牌与任务不匹配或已失效")
            _assert_active(row)
            task_id = row["id"]
            blocked_count = 0

            if success:
                conn.execute(
                    """UPDATE tasks SET state = 'succeeded', lease_token = NULL,
                           lease_expires_at = NULL, finished_at = ?
                       WHERE id = ?""",
                    (_iso(now), task_id),
                )
                conn.execute(
                    "UPDATE attempts SET state = 'succeeded' WHERE lease_token = ?",
                    (token,),
                )
                state = SUCCEEDED
            else:
                # 显式失败才消耗重试预算：在同一事务内原子递增独立计数，
                # 租约过期后的重新领取（attempt 增加）不影响该计数。
                cur = conn.execute(
                    "UPDATE tasks SET failure_count = failure_count + 1 WHERE id = ?",
                    (task_id,),
                )
                if cur.rowcount != 1:  # pragma: no cover - 行已在本事务中加载
                    raise APIError(409, "task_changed", "任务在回报期间被改动，请重试")
                failure_count = row["failure_count"] + 1
                will_retry = failure_count <= row["max_retries"]
                if will_retry:
                    # 回到待领取队列；下一次领取产生 attempt+1 的新尝试与新令牌
                    conn.execute(
                        """UPDATE tasks SET state = 'pending', lease_token = NULL,
                               lease_expires_at = NULL, worker_id = NULL
                           WHERE id = ?""",
                        (task_id,),
                    )
                    conn.execute(
                        "UPDATE attempts SET state = 'failed', error = ? WHERE lease_token = ?",
                        (error, token),
                    )
                    state = PENDING
                else:
                    conn.execute(
                        """UPDATE tasks SET state = 'failed', lease_token = NULL,
                               lease_expires_at = NULL, finished_at = ?
                           WHERE id = ?""",
                        (_iso(now), task_id),
                    )
                    conn.execute(
                        "UPDATE attempts SET state = 'failed', error = ? WHERE lease_token = ?",
                        (error, token),
                    )
                    state = FAILED
                    blocked_count = _block_descendants(conn, row["dag_id"], row["task_key"])

            if success:
                retries_remaining = 0
                saved_failure_count = row["failure_count"]
            else:
                # 本次失败后还会被调度的额外尝试次数
                retries_remaining = max(0, row["max_retries"] - failure_count + 1)
                saved_failure_count = failure_count

            response = {
                "task_id": task_id,
                "state": state,
                "attempt": row["attempt"],
                "failure_count": saved_failure_count,
                "retries_remaining": retries_remaining,
                "blocked_descendants": blocked_count,
            }
            # 与状态变更同一事务持久化：保存规范化指纹、HTTP 状态码和响应体
            conn.execute(
                """INSERT INTO completions (task_id, completion_id, lease_token, success,
                       fingerprint, status_code, response_body, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    task_id,
                    completion_id,
                    token,
                    1 if success else 0,
                    fingerprint,
                    200,
                    # 与 Starlette JSONResponse 的序列化保持一致，确保重放逐字节相同
                    json.dumps(response, ensure_ascii=False, separators=(",", ":")),
                    _iso(now),
                ),
            )
            conn.execute("COMMIT")
        except Exception:
            # 命中冲突分支时事务已回滚；仅在仍有活动事务时回滚
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise

    return response


# ---------------------------------------------------------------- 状态查询

def _attempts(conn, task_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM attempts WHERE task_id = ? ORDER BY attempt_no", (task_id,)
    ).fetchall()
    out = []
    for a in rows:
        out.append(
            {
                "attempt": a["attempt_no"],
                "state": a["state"],
                "lease_token": a["lease_token"],
                "worker_id": a["worker_id"],
                "leased_at": a["leased_at"],
                "expires_at": a["expires_at"],
                "heartbeat_at": a["heartbeat_at"],
                "expired": a["state"] == A_ACTIVE
                and (now := _parse(a["expires_at"])) is not None
                and now <= _now(),
                "error": a["error"],
            }
        )
    return out


def _task_dict(conn, row, with_attempts: bool = True) -> dict[str, Any]:
    deps = [
        r["dep_key"]
        for r in conn.execute(
            "SELECT dep_key FROM dependencies WHERE dag_id = ? AND task_key = ? ORDER BY dep_key",
            (row["dag_id"], row["task_key"]),
        ).fetchall()
    ]
    lease = None
    if row["lease_token"] is not None:
        exp = _parse(row["lease_expires_at"])
        lease = {
            "token": row["lease_token"],
            "expires_at": row["lease_expires_at"],
            "expired": exp is not None and exp <= _now(),
            "worker_id": row["worker_id"],
        }
    return {
        "id": row["id"],
        "dag_id": row["dag_id"],
        "key": row["task_key"],
        "name": row["name"],
        "payload": json.loads(row["payload"]) if row["payload"] else None,
        "state": row["state"],
        "deps": deps,
        "attempt": row["attempt"],
        "failure_count": row["failure_count"],
        "max_retries": row["max_retries"],
        # 剩余重试额度只与显式失败计数挂钩，与 attempt（含租约过期重领）无关
        "retries_remaining": max(0, row["max_retries"] - row["failure_count"]),
        "lease": lease,
        "blocked_reason": row["blocked_reason"],
        "created_at": row["created_at"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "attempts": _attempts(conn, row["id"]) if with_attempts else None,
    }


def get_task(task_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if row is None:
            raise APIError(404, "task_not_found", f"任务 {task_id} 不存在")
        return _task_dict(conn, row)


def _dag_summary(conn, dag_id: str) -> dict[str, Any]:
    counts = {
        s: (
            conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE dag_id = ? AND state = ?", (dag_id, s)
            ).fetchone()[0]
        )
        for s in (PENDING, RUNNING, SUCCEEDED, FAILED, BLOCKED)
    }
    return counts


def get_dag(dag_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        dag = conn.execute("SELECT * FROM dags WHERE id = ?", (dag_id,)).fetchone()
        if dag is None:
            raise APIError(404, "dag_not_found", f"DAG {dag_id} 不存在")
        rows = conn.execute(
            "SELECT * FROM tasks WHERE dag_id = ? ORDER BY created_at, task_key", (dag_id,)
        ).fetchall()
        return {
            "id": dag["id"],
            "name": dag["name"],
            "created_at": dag["created_at"],
            "task_count": len(rows),
            "states": _dag_summary(conn, dag_id),
            "tasks": [_task_dict(conn, r, with_attempts=False) for r in rows],
        }


def list_tasks(dag_id: str | None, state: str | None) -> dict[str, Any]:
    sql = "SELECT * FROM tasks"
    where, params = [], []
    if dag_id is not None:
        where.append("dag_id = ?")
        params.append(dag_id)
    if state is not None:
        where.append("state = ?")
        params.append(state)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at, id"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
        return {"tasks": [_task_dict(conn, r, with_attempts=False) for r in rows], "count": len(rows)}
