"""端到端行为测试。

运行: pytest -q （需先 pip install -r requirements-dev.txt）
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from pathlib import Path

import pytest


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_file = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient

    from app.main import app

    with TestClient(app) as c:
        yield c


def submit(client, tasks, name="dag", dag_id=None):
    body = {"name": name, "tasks": tasks}
    if dag_id:
        body["id"] = dag_id
    r = client.post("/dags", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def by_key(dag, key):
    return next(t for t in dag["tasks"] if t["key"] == key)


# ------------------------------------------------------------- 提交校验

def test_submit_valid_dag(client):
    dag = submit(
        client,
        [
            {"key": "a"},
            {"key": "b", "deps": ["a"]},
        ],
    )
    assert dag["task_count"] == 2
    assert dag["states"]["pending"] == 2


def test_submit_rejects_missing_dependency(client):
    r = client.post("/dags", json={"tasks": [{"key": "a", "deps": ["ghost"]}]})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "missing_dependency"
    assert "ghost" in r.json()["error"]["details"]["missing"]


def test_submit_rejects_self_dependency_cycle(client):
    r = client.post("/dags", json={"tasks": [{"key": "a", "deps": ["a"]}]})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "cycle_detected"


def test_submit_rejects_long_cycle(client):
    r = client.post(
        "/dags",
        json={"tasks": [{"key": "a", "deps": ["c"]}, {"key": "b", "deps": ["a"]}, {"key": "c", "deps": ["b"]}]},
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "cycle_detected"
    assert r.json()["error"]["details"]["cycle"][0] == r.json()["error"]["details"]["cycle"][-1]


def test_submit_rejects_duplicate_key(client):
    r = client.post("/dags", json={"tasks": [{"key": "a"}, {"key": "a"}]})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "duplicate_task_key"


# ------------------------------------------------------------- 依赖门控

def test_claim_respects_dependencies(client):
    dag = submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}])
    r = client.post("/tasks/claim", json={"limit": 10})
    assert r.status_code == 200
    keys = {t["task_key"] for t in r.json()["claimed"]}
    assert keys == {"a"}

    # a 已在 running 且租约未过期，不重复领取；b 的前置未完成，也不可领
    r = client.post("/tasks/claim", json={"dag_id": dag["id"]})
    assert r.json()["count"] == 0


def test_full_dependency_flow(client):
    dag = submit(
        client,
        [
            {"key": "a"},
            {"key": "b", "deps": ["a"]},
            {"key": "c", "deps": ["a"]},
            {"key": "d", "deps": ["b", "c"]},
        ],
    )
    dag_id = dag["id"]

    # 只有 a 可领
    claimed = client.post("/tasks/claim", json={"limit": 10}).json()["claimed"]
    assert [t["task_key"] for t in claimed] == ["a"]
    token_a = claimed[0]["lease_token"]
    assert claimed[0]["attempt"] == 1

    # 完成 a 后 b、c 同时可领
    assert client.post(f"/tasks/{claimed[0]['task_id']}/complete",
                       json={"completion_id": "cmp-a", "lease_token": token_a, "success": True}).status_code == 200
    claimed = client.post("/tasks/claim", json={"dag_id": dag_id, "limit": 10}).json()["claimed"]
    assert {t["task_key"] for t in claimed} == {"b", "c"}
    for t in claimed:
        assert t["attempt"] == 1

    # d 仍被门控
    assert client.post("/tasks/claim", json={"dag_id": dag_id}).json()["count"] == 0

    for t in claimed:
        client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": f"cmp-{t['task_key']}",
                          "lease_token": t["lease_token"], "success": True})
    d = client.post("/tasks/claim", json={"dag_id": dag_id}).json()["claimed"]
    assert len(d) == 1 and d[0]["task_key"] == "d"


# ------------------------------------------------------------- 尝试与唯一令牌

def test_attempt_increments_and_tokens_unique(client):
    dag = submit(client, [{"key": "a", "lease_seconds": 0.2, "max_retries": 3}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t1["attempt"] == 1
    time.sleep(0.25)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["task_id"] == t1["task_id"]
    assert t2["attempt"] == 2
    assert t2["lease_token"] != t1["lease_token"]


def test_old_token_operations_return_409_after_reclaim(client):
    submit(client, [{"key": "a", "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    old_token, task_id = t1["lease_token"], t1["task_id"]
    time.sleep(0.25)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2

    # 旧令牌续租 → 409
    r = client.post(f"/tasks/{task_id}/heartbeat",
                    json={"lease_token": old_token})
    assert r.status_code == 409
    assert r.json()["error"]["code"] in ("invalid_lease", "lease_expired")

    # 旧令牌回报结果 → 409，绝不能覆盖新尝试（completion_id 首次使用也必须先过租约校验）
    r = client.post(f"/tasks/{task_id}/complete",
                    json={"completion_id": "cmp-old", "lease_token": old_token, "success": True})
    assert r.status_code == 409

    status = client.get(f"/tasks/{task_id}").json()
    assert status["state"] == "running"
    assert status["attempt"] == 2
    assert status["lease"]["token"] == t2["lease_token"]


def test_heartbeat_extends_lease(client):
    submit(client, [{"key": "a", "lease_seconds": 0.3}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    time.sleep(0.2)
    r = client.post(f"/tasks/{t['task_id']}/heartbeat",
                    json={"lease_token": t["lease_token"], "extend_seconds": 5})
    assert r.status_code == 200
    time.sleep(0.2)  # 没有心跳的话此刻已过期
    # 租约仍有效：不可再领取，且旧令牌仍可回报
    assert client.post("/tasks/claim", json={}).json()["count"] == 0
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "cmp-hb", "lease_token": t["lease_token"], "success": True})
    assert r.status_code == 200


def test_completion_id_is_required(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"lease_token": t["lease_token"], "success": True})
    assert r.status_code == 422


def test_wrong_token_returns_409(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "cmp-x",
                          "lease_token": uuid.uuid4().hex, "success": True})
    assert r.status_code == 409


# ------------------------------------------------------------- 幂等回报

def _complete(client, task, cid, **extra):
    body = {"completion_id": cid, "lease_token": task["lease_token"]}
    body.update(extra)
    return client.post(f"/tasks/{task['task_id']}/complete", json=body)


def test_idempotent_replay_returns_identical_response_after_reclaim(client):
    """任务被重新领取后，旧 completion_id 的等价回报仍原样返回首次响应。"""
    submit(client, [{"key": "a", "max_retries": 1, "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]

    payload = {"completion_id": "c1", "lease_token": t1["lease_token"],
               "success": False, "error": "boom"}
    r1 = client.post(f"/tasks/{t1['task_id']}/complete", json=payload)
    assert r1.status_code == 200
    first = r1.json()
    assert first["state"] == "pending" and first["attempt"] == 1
    assert first["failure_count"] == 1 and first["retries_remaining"] == 1

    # 租约过期后被重新领取：新尝试、新令牌
    time.sleep(0.25)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2 and t2["lease_token"] != t1["lease_token"]

    # 用旧令牌 + 相同 completion_id/内容重放：原样回放，不经过当前租约校验
    r2 = client.post(f"/tasks/{t1['task_id']}/complete", json=payload)
    assert r2.status_code == r1.status_code
    assert r2.text == r1.text  # 逐字节相同的响应体
    assert r2.json() == first

    # 不得再次增加 failure_count，也不影响正在运行的新尝试
    status = client.get(f"/tasks/{t1['task_id']}").json()
    assert status["failure_count"] == 1
    assert status["state"] == "running" and status["attempt"] == 2
    assert status["lease"]["token"] == t2["lease_token"]

    # 新令牌的回报（不同 completion_id）仍走当前租约校验并正常生效
    r3 = _complete(client, t2, "c2", success=True)
    assert r3.status_code == 200 and r3.json()["state"] == "succeeded"
    assert client.get(f"/tasks/{t1['task_id']}").json()["state"] == "succeeded"


def test_idempotent_replay_json_key_order_equivalent(client):
    """result 的 JSON 对象键序不同（含嵌套）视为等价，原样回放首次响应。"""
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]

    r1 = _complete(client, t, "ck", success=True, result={"outer": {"b": 1, "a": [2, 1]}, "k": 0})
    assert r1.status_code == 200
    # 键序调换、空白变化：仍视为同一请求
    r2 = client.post(
        f"/tasks/{t['task_id']}/complete",
        content=json.dumps({"completion_id": "ck", "lease_token": t["lease_token"],
                            "success": True,
                            "result": {"k": 0, "outer": {"a":  [2, 1], "b": 1}}}),
        headers={"Content-Type": "application/json"},
    )
    assert r2.status_code == 200 and r2.text == r1.text
    assert client.get(f"/tasks/{t['task_id']}").json()["state"] == "succeeded"


def test_same_key_different_content_returns_conflict(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r1 = _complete(client, t, "ck", success=True, result={"rows": 1})
    assert r1.status_code == 200

    # success 不同
    r2 = _complete(client, t, "ck", success=False, error="now failed")
    assert r2.status_code == 409
    assert r2.json()["error"]["code"] == "idempotency_conflict"
    assert r2.json()["error"]["details"]["completion_id"] == "ck"

    # 即便租约令牌也无效，只要键存在且内容不同，仍是冲突而非租约错误
    r3 = client.post(
        f"/tasks/{t['task_id']}/complete",
        json={"completion_id": "ck", "lease_token": uuid.uuid4().hex,
              "success": True, "result": {"rows": 2}},
    )
    assert r3.status_code == 409
    assert r3.json()["error"]["code"] == "idempotency_conflict"


def test_different_key_still_requires_valid_lease(client):
    submit(client, [{"key": "a", "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert _complete(client, t1, "c1", success=True).status_code == 200

    # 任务已结束，使用新 completion_id 必须重新通过租约校验 → invalid_lease
    r = client.post(
        f"/tasks/{t1['task_id']}/complete",
        json={"completion_id": "c2", "lease_token": t1["lease_token"], "success": True},
    )
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "invalid_lease"


def test_idempotent_exhausted_failure_blocks_descendants_once(client):
    dag = submit(client, [{"key": "a", "max_retries": 0}, {"key": "b", "deps": ["a"]}])
    dag_id = dag["id"]
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    payload = {"completion_id": "cf", "lease_token": t["lease_token"],
               "success": False, "error": "fatal"}
    r1 = client.post(f"/tasks/{t['task_id']}/complete", json=payload)
    assert r1.json()["state"] == "failed"
    assert r1.json()["failure_count"] == 1
    assert r1.json()["blocked_descendants"] == 1

    # 任务结束后用相同键重放：仍是首次响应，计数与阻塞都不重复发生
    r2 = client.post(f"/tasks/{t['task_id']}/complete", json=payload)
    assert r2.status_code == 200 and r2.text == r1.text
    full = client.get(f"/dags/{dag_id}").json()
    assert full["states"]["failed"] == 1 and full["states"]["blocked"] == 1
    assert client.get(f"/tasks/{t['task_id']}").json()["failure_count"] == 1


def test_idempotency_record_survives_process_restart(tmp_path, monkeypatch):
    """幂等记录持久化在 SQLite 中：重新打开应用后重放仍返回首次响应。"""
    db_file = tmp_path / "persist.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as c1:
        submit(c1, [{"key": "a"}])
        t = c1.post("/tasks/claim", json={}).json()["claimed"][0]
        r1 = c1.post(f"/tasks/{t['task_id']}/complete",
                     json={"completion_id": "restart-1",
                           "lease_token": t["lease_token"], "success": True})
        assert r1.status_code == 200

    # 模拟进程重启：重新走 lifespan/init_db，旧库中的 completions 记录仍然有效
    with TestClient(app) as c2:
        r2 = c2.post(f"/tasks/{t['task_id']}/complete",
                     json={"completion_id": "restart-1",
                           "lease_token": t["lease_token"], "success": True})
        assert r2.status_code == 200 and r2.text == r1.text

        r3 = c2.post(f"/tasks/{t['task_id']}/complete",
                     json={"completion_id": "restart-1",
                           "lease_token": t["lease_token"], "success": False})
        assert r3.status_code == 409
        assert r3.json()["error"]["code"] == "idempotency_conflict"


# ------------------------------------------------------------- 重试与阻塞

def test_retry_then_success(client):
    submit(client, [{"key": "a", "max_retries": 2}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "cmp-r1",
                          "lease_token": t["lease_token"], "success": False, "error": "boom"})
    assert r.json()["state"] == "pending"
    assert r.json()["retries_remaining"] == 2

    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2
    assert client.post(f"/tasks/{t2['task_id']}/complete",
                       json={"completion_id": "cmp-r2",
                             "lease_token": t2["lease_token"], "success": True}).status_code == 200


def test_exhausted_retries_blocks_descendants(client):
    dag = submit(
        client,
        [
            {"key": "a", "max_retries": 1},
            {"key": "b", "deps": ["a"]},
            {"key": "c", "deps": ["b"]},
        ],
    )
    dag_id = dag["id"]

    for expected_attempt in (1, 2):
        t = client.post("/tasks/claim", json={}).json()["claimed"][0]
        assert t["attempt"] == expected_attempt
        r = client.post(f"/tasks/{t['task_id']}/complete",
                        json={"completion_id": f"cmp-block-{expected_attempt}",
                              "lease_token": t["lease_token"], "success": False, "error": "x"})
    assert r.json()["state"] == "failed"
    assert r.json()["blocked_descendants"] == 2

    # 后继永远领取不到
    assert client.post("/tasks/claim", json={"dag_id": dag_id, "limit": 10}).json()["count"] == 0

    full = client.get(f"/dags/{dag_id}").json()
    b = by_key(full, "b")
    c = by_key(full, "c")
    assert b["state"] == "blocked" and "permanently failed" in b["blocked_reason"]
    assert c["state"] == "blocked" and "permanently failed" in c["blocked_reason"]
    assert full["states"]["failed"] == 1 and full["states"]["blocked"] == 2


def test_lease_expiry_reclaim_does_not_consume_retry_budget(client):
    submit(client, [{"key": "a", "max_retries": 1, "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t1["attempt"] == 1
    time.sleep(0.25)

    # 过期后重领：attempt+1、新令牌，但显式失败计数不变、重试预算保留
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2 and t2["lease_token"] != t1["lease_token"]
    status = client.get(f"/tasks/{t2['task_id']}").json()
    assert status["failure_count"] == 0
    assert status["retries_remaining"] == 1

    # 旧令牌回报失败 -> 409，不得计入失败次数（首次未通过租约校验，不产生幂等记录）
    r = client.post(f"/tasks/{t1['task_id']}/complete",
                    json={"completion_id": "cmp-late",
                          "lease_token": t1["lease_token"], "success": False, "error": "late"})
    assert r.status_code == 409

    # 第一次显式失败：仍可重试（回到 pending），attempts 历史保留过期记录
    r = client.post(f"/tasks/{t2['task_id']}/complete",
                    json={"completion_id": "cmp-e1",
                          "lease_token": t2["lease_token"], "success": False, "error": "e1"})
    assert r.json()["state"] == "pending"
    assert r.json()["failure_count"] == 1
    status = client.get(f"/tasks/{t2['task_id']}").json()
    assert status["state"] == "pending" and status["attempt"] == 2
    assert status["failure_count"] == 1 and status["retries_remaining"] == 0
    assert [a["state"] for a in status["attempts"]] == ["expired", "failed"]

    # 第二次显式失败：预算耗尽 -> failed
    t3 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t3["attempt"] == 3
    r = client.post(f"/tasks/{t3['task_id']}/complete",
                    json={"completion_id": "cmp-e2",
                          "lease_token": t3["lease_token"], "success": False, "error": "e2"})
    assert r.json()["state"] == "failed"
    assert r.json()["failure_count"] == 2
    assert r.json()["retries_remaining"] == 0


def test_retries_remaining_tracks_only_explicit_failures(client):
    submit(client, [{"key": "a", "max_retries": 0, "lease_seconds": 0.2}])
    # 多次过期重领均未显式失败：成功后计数仍为 0
    for expected in (1, 2):
        t = client.post("/tasks/claim", json={}).json()["claimed"][0]
        assert t["attempt"] == expected
        time.sleep(0.25)
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t["attempt"] == 3
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "cmp-ok", "lease_token": t["lease_token"], "success": True})
    assert r.json()["state"] == "succeeded"
    assert r.json()["failure_count"] == 0
    status = client.get(f"/tasks/{t['task_id']}").json()
    assert status["failure_count"] == 0 and status["retries_remaining"] == 0
    assert [a["state"] for a in status["attempts"]] == ["expired", "expired", "succeeded"]


def test_legacy_sqlite_db_gets_failure_count_default(tmp_path, monkeypatch):
    """旧版 tasks 表（无 failure_count 列）在 init_db 时补齐默认值 0。"""
    import sqlite3

    db_file = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_file)
    conn.executescript(
        """
        CREATE TABLE dags (id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, task_key TEXT NOT NULL, name TEXT NOT NULL,
            payload TEXT, max_retries INTEGER NOT NULL DEFAULT 0, lease_seconds REAL NOT NULL,
            state TEXT NOT NULL, blocked_reason TEXT, lease_token TEXT, lease_expires_at TEXT,
            worker_id TEXT, attempt INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            UNIQUE(dag_id, task_key));
        CREATE TABLE dependencies (dag_id TEXT, task_key TEXT, dep_key TEXT,
            PRIMARY KEY (dag_id, task_key, dep_key));
        CREATE TABLE attempts (id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT, attempt_no INTEGER,
            lease_token TEXT UNIQUE, worker_id TEXT, leased_at TEXT, expires_at TEXT,
            heartbeat_at TEXT, state TEXT, error TEXT);
        INSERT INTO dags VALUES ('d1', 'old', '2026-01-01T00:00:00Z');
        -- 旧任务 attempt=3（曾因过期被重领），max_retries=1，处于 pending
        INSERT INTO tasks (id, dag_id, task_key, name, max_retries, lease_seconds, state,
                           attempt, created_at)
        VALUES ('t1', 'd1', 'a', 'a', 1, 30, 'pending', 3, '2026-01-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_PATH", str(db_file))
    # db_path() 每次调用动态读取环境变量，无需重新导入模块
    from app.db import get_conn, init_db
    init_db()  # 不应报错，且只迁移一次
    init_db()

    conn = sqlite3.connect(db_file)
    row = conn.execute(
        "SELECT attempt, failure_count, max_retries FROM tasks WHERE id = 't1'"
    ).fetchone()
    conn.close()
    assert row == (3, 0, 1)  # 兼容默认值：过期重领不追溯消耗失败预算

    from app import service
    t = service.claim_tasks(None, None, 1)["claimed"][0]
    assert t["attempt"] == 4
    # 第一次显式失败仍有重试额度（旧库迁移后 completions 表可用）
    result = service.complete_task(
        t["task_id"],
        {"completion_id": "cmp-legacy", "lease_token": t["lease_token"], "success": False},
    )
    assert result["state"] == "pending" and result["failure_count"] == 1


# ------------------------------------------------------------- 状态接口

def test_status_lists_attempts_and_lease(client):
    submit(client, [{"key": "a", "lease_seconds": 60}])
    t = client.post("/tasks/claim", json={"worker_id": "w-1"}).json()["claimed"][0]
    client.post(f"/tasks/{t['task_id']}/heartbeat",
                json={"lease_token": t["lease_token"]})

    status = client.get(f"/tasks/{t['task_id']}").json()
    assert status["attempt"] == 1
    assert status["state"] == "running"
    assert status["lease"]["token"] == t["lease_token"]
    assert status["lease"]["expired"] is False
    assert status["lease"]["worker_id"] == "w-1"
    att = status["attempts"][0]
    assert att["attempt"] == 1 and att["state"] == "active"
    assert att["heartbeat_at"] is not None

    # 列表接口支持状态过滤
    r = client.get("/tasks", params={"state": "running"})
    assert r.json()["count"] == 1


# ------------------------------------------------------------- 原子领取
def test_concurrent_claim_is_atomic(client):
    submit(client, [{"key": "a"}])
    results = []
    errors = []

    def worker():
        try:
            r = client.post("/tasks/claim", json={"worker_id": f"w-{threading.get_ident()}"})
            results.extend(r.json()["claimed"])
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors
    assert len(results) == 1
    assert results[0]["attempt"] == 1


# ------------------------------------------------------------- 事件流

def _all_events(client, **params):
    """逐页拉取全部事件，断言跨页无重复/遗漏。"""
    seen = []
    after = params.get("after", 0)
    while True:
        page = client.get("/events", params={**params, "after": after}).json()
        seqs = [e["sequence"] for e in page["events"]]
        assert seqs == sorted(seqs) and len(seqs) == len(set(seqs))
        seen.extend(page["events"])
        if not page["events"]:
            assert page["next_after"] == after  # 空页沿用 after
            break
        assert page["next_after"] == seqs[-1]
        after = page["next_after"]
    return seen


def test_dag_created_event_nulls_task_fields(client):
    dag = submit(client, [{"key": "a"}], dag_id="dag-evt-1")
    events = client.get("/events", params={"dag_id": "dag-evt-1"}).json()["events"]
    assert len(events) == 1
    e = events[0]
    assert e["sequence"] >= 1
    assert e["type"] == "dag.created"
    assert e["dag_id"] == dag["id"]
    assert e["task_id"] is None
    assert e["attempt"] is None
    assert e["from_state"] is None
    assert e["to_state"] is None
    assert e["occurred_at"]  # 非空时间戳


def test_task_lifecycle_event_sequence(client):
    dag = submit(
        client,
        [
            {"key": "a", "max_retries": 1},
            {"key": "b", "deps": ["a"]},
        ],
    )
    dag_id = dag["id"]

    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    evts = client.get("/events", params={"dag_id": dag_id}).json()["events"]
    claimed = evts[-1]
    assert claimed["type"] == "task.claimed"
    assert claimed["task_id"] == t["task_id"]
    assert claimed["attempt"] == 1
    assert claimed["from_state"] == "pending"
    assert claimed["to_state"] == "running"

    # 显式失败但仍有额度 -> requeued: running -> pending
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "c-fail", "lease_token": t["lease_token"],
                          "success": False, "error": "boom"})
    assert r.status_code == 200
    evts = client.get("/events", params={"dag_id": dag_id}).json()["events"]
    requeued = evts[-1]
    assert requeued["type"] == "task.requeued"
    assert requeued["attempt"] == 1
    assert requeued["from_state"] == "running"
    assert requeued["to_state"] == "pending"

    # 重新领取 -> claimed（pending -> running，attempt=2）
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    evts = client.get("/events", params={"dag_id": dag_id}).json()["events"]
    claimed2 = evts[-1]
    assert claimed2["type"] == "task.claimed"
    assert claimed2["attempt"] == 2
    assert claimed2["from_state"] == "pending"
    assert claimed2["to_state"] == "running"

    # 再次失败且额度耗尽（max_retries=1，failure_count=2）-> failed，后继 b -> blocked
    client.post(f"/tasks/{t2['task_id']}/complete",
                json={"completion_id": "c-fail-2", "lease_token": t2["lease_token"],
                      "success": False, "error": "fatal"})
    evts = client.get("/events", params={"dag_id": dag_id}).json()["events"]
    failed, blocked = evts[-2], evts[-1]
    assert failed["type"] == "task.failed"
    assert failed["task_id"] == t2["task_id"]
    assert failed["attempt"] == 2
    assert failed["from_state"] == "running" and failed["to_state"] == "failed"
    assert blocked["type"] == "task.blocked"
    assert blocked["task_id"] != t2["task_id"]
    assert blocked["from_state"] == "pending" and blocked["to_state"] == "blocked"

    # sequence 全局严格递增，且 failed 先于 blocked
    seqs = [e["sequence"] for e in evts]
    assert seqs == sorted(set(seqs))
    assert failed["sequence"] < blocked["sequence"]


def test_succeeded_event(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    client.post(f"/tasks/{t['task_id']}/complete",
                json={"completion_id": "c-ok", "lease_token": t["lease_token"], "success": True})
    events = client.get("/events").json()["events"]
    succeeded = events[-1]
    assert succeeded["type"] == "task.succeeded"
    assert succeeded["task_id"] == t["task_id"]
    assert succeeded["attempt"] == 1
    assert succeeded["from_state"] == "running"
    assert succeeded["to_state"] == "succeeded"


def test_claim_after_lease_expiry_event_from_running(client):
    submit(client, [{"key": "a", "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    time.sleep(0.25)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2
    events = client.get("/events").json()["events"]
    claimed2 = events[-1]
    assert claimed2["type"] == "task.claimed"
    assert claimed2["attempt"] == 2
    assert claimed2["from_state"] == "running"  # 过期重领：任务此前停留在 running
    assert claimed2["to_state"] == "running"


def test_events_pagination_no_gaps_or_duplicates(client):
    # 制造超过单页容量的事件：3 个 DAG，每个 2 个任务走完整流程
    for i in range(3):
        dag = submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}], dag_id=f"page-{i}")
        for _ in range(2):
            t = client.post("/tasks/claim", json={"dag_id": dag["id"]}).json()["claimed"][0]
            client.post(f"/tasks/{t['task_id']}/complete",
                        json={"completion_id": f"c-{i}-{t['task_key']}",
                              "lease_token": t["lease_token"], "success": True})

    full = client.get("/events", params={"limit": 100}).json()["events"]
    assert len(full) > 3

    # 逐页（limit=1）拉取应与一次性拉取完全一致
    paged = _all_events(client, limit=1)
    assert [e["sequence"] for e in paged] == [e["sequence"] for e in full]

    # 末页之后再翻页：空页沿用 after
    last_seq = full[-1]["sequence"]
    tail = client.get("/events", params={"after": last_seq, "limit": 10}).json()
    assert tail == {"events": [], "next_after": last_seq}


def test_events_filter_by_dag_id_across_global_sequence(client):
    d1 = submit(client, [{"key": "a"}], dag_id="f-1")
    d2 = submit(client, [{"key": "x"}], dag_id="f-2")
    for dag in (d1, d2):
        t = client.post("/tasks/claim", json={"dag_id": dag["id"]}).json()["claimed"][0]
        client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": f"c-{dag['id']}",
                          "lease_token": t["lease_token"], "success": True})

    paged = _all_events(client, dag_id="f-1", limit=1)
    assert {e["dag_id"] for e in paged} == {"f-1"}
    assert [e["type"] for e in paged] == ["dag.created", "task.claimed", "task.succeeded"]

    # 过滤后无新事件：空页沿用传入的全局 after
    tail = client.get("/events", params={"dag_id": "f-1", "after": 10_000}).json()
    assert tail == {"events": [], "next_after": 10_000}


def test_events_default_params(client):
    submit(client, [{"key": "a"}])
    body = client.get("/events").json()
    assert body["next_after"] == body["events"][-1]["sequence"]
    assert body["events"][0]["type"] == "dag.created"


def test_events_rejects_invalid_params(client):
    assert client.get("/events", params={"limit": 0}).status_code == 422
    assert client.get("/events", params={"limit": 101}).status_code == 422
    assert client.get("/events", params={"after": -1}).status_code == 422


def test_idempotent_replay_emits_no_event(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    payload = {"completion_id": "c1", "lease_token": t["lease_token"], "success": True}
    assert client.post(f"/tasks/{t['task_id']}/complete", json=payload).status_code == 200
    before = client.get("/events", params={"limit": 100}).json()["events"]

    # 幂等重放不产生新事件
    assert client.post(f"/tasks/{t['task_id']}/complete", json=payload).status_code == 200
    after = client.get("/events", params={"limit": 100}).json()["events"]
    assert [e["sequence"] for e in after] == [e["sequence"] for e in before]


def test_failed_submit_rolls_back_event(client):
    submit(client, [{"key": "a"}], dag_id="dup-dag")
    # 重复 DAG id：dag 行插入即冲突，整个事务回滚，不留第二条创建事件
    r = client.post("/dags", json={"id": "dup-dag", "tasks": [{"key": "b"}]})
    assert r.status_code == 409
    events = client.get("/events", params={"dag_id": "dup-dag"}).json()["events"]
    assert len(events) == 1 and events[0]["type"] == "dag.created"

    # 图校验失败（环）发生在开事务之前：完全不留事件
    r = client.post("/dags", json={"tasks": [{"key": "a", "deps": ["a"]}]})
    assert r.status_code == 422
    all_events = client.get("/events", params={"limit": 100}).json()["events"]
    assert len(all_events) == 1


def test_events_persist_across_restart(tmp_path, monkeypatch):
    db_file = tmp_path / "events.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as c1:
        submit(c1, [{"key": "a"}], dag_id="restart-dag")

    with TestClient(app) as c2:
        events = c2.get("/events", params={"dag_id": "restart-dag"}).json()["events"]
        assert len(events) == 1
        assert events[0]["type"] == "dag.created"
        assert events[0]["sequence"] == 1  # 重启后 sequence 不重置，不漏不重


def test_event_stream_is_immutable_in_storage(client):
    submit(client, [{"key": "a"}])
    import sqlite3
    from app.db import db_path

    conn = sqlite3.connect(db_path())
    with pytest.raises(sqlite3.Error):
        conn.execute("UPDATE events SET event_type = 'tampered' WHERE sequence = 1")
    conn.rollback()
    with pytest.raises(sqlite3.Error):
        conn.execute("DELETE FROM events")
    conn.rollback()
    conn.close()

    events = client.get("/events").json()["events"]
    assert events[0]["type"] == "dag.created"
