"""SQLite 连接与表结构。

所有对任务状态的修改都在单个 SQLite 连接的一个事务内完成，
依靠 BEGIN IMMEDIATE 取得的保留锁保证同一时刻只有一个工作器
能从 PENDING 队列领取到给定任务（原子领取）。
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

# 默认租约时长（秒），任务可用 lease_seconds 覆盖。
DEFAULT_LEASE_SECONDS = 30.0


def db_path() -> Path:
    return Path(os.environ.get("DB_PATH", "./data/tasks.db"))


def get_default_lease_seconds() -> float:
    return float(os.environ.get("DEFAULT_LEASE_SECONDS", DEFAULT_LEASE_SECONDS))


def _connect(path: Path | None = None) -> sqlite3.Connection:
    path = path or db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(path),
        timeout=30.0,
        isolation_level=None,  # 手工管理事务
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db(path: Path | None = None) -> None:
    with _connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS dags (
                id            TEXT PRIMARY KEY,
                name          TEXT NOT NULL,
                created_at    TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id                  TEXT PRIMARY KEY,
                dag_id              TEXT NOT NULL REFERENCES dags(id),
                task_key            TEXT NOT NULL,
                name                TEXT NOT NULL,
                payload             TEXT,
                max_retries         INTEGER NOT NULL DEFAULT 0,
                lease_seconds       REAL NOT NULL,
                state               TEXT NOT NULL,        -- pending/running/succeeded/failed/blocked
                blocked_reason      TEXT,
                lease_token         TEXT,
                lease_expires_at    TEXT,
                worker_id           TEXT,
                attempt             INTEGER NOT NULL DEFAULT 0,
                -- 独立的显式失败计数：只有 complete(success=false) 才递增；
                -- 租约过期后的重新领取只增加 attempt，不消耗重试预算。
                failure_count       INTEGER NOT NULL DEFAULT 0,
                created_at          TEXT NOT NULL,
                started_at          TEXT,
                finished_at         TEXT,
                UNIQUE(dag_id, task_key)
            );

            CREATE TABLE IF NOT EXISTS dependencies (
                dag_id     TEXT NOT NULL REFERENCES dags(id),
                task_key   TEXT NOT NULL,
                dep_key    TEXT NOT NULL,
                PRIMARY KEY (dag_id, task_key, dep_key)
            );

            CREATE TABLE IF NOT EXISTS attempts (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id        TEXT NOT NULL REFERENCES tasks(id),
                attempt_no     INTEGER NOT NULL,
                lease_token    TEXT NOT NULL UNIQUE,
                worker_id      TEXT,
                leased_at      TEXT NOT NULL,
                expires_at     TEXT NOT NULL,
                heartbeat_at   TEXT,
                state          TEXT NOT NULL,   -- active/expired/succeeded/failed
                error          TEXT
            );

            -- 结果回报的幂等记录：(task_id, completion_id) 唯一标识一次逻辑回报。
            -- 首次有效回报在租约校验/状态变更的同一事务内写入规范化请求指纹与
            -- 首次响应（HTTP 状态码 + 原始响应体）；重放即使发生在任务被重新领取
            -- 之后也只返回存储的响应，不再改动 failure_count 或阻塞后继。
            CREATE TABLE IF NOT EXISTS completions (
                task_id        TEXT NOT NULL REFERENCES tasks(id),
                completion_id  TEXT NOT NULL,
                lease_token    TEXT NOT NULL,
                success        INTEGER NOT NULL,
                fingerprint    TEXT NOT NULL,   -- lease_token/success/result/error 的规范化 JSON
                status_code    INTEGER NOT NULL,
                response_body  TEXT NOT NULL,   -- 首次响应的原始 JSON 文本
                created_at     TEXT NOT NULL,
                PRIMARY KEY (task_id, completion_id)
            );

            -- 不可变事件流：只允许 INSERT。sequence 为全局单调递增游标，
            -- 与对应业务变更在同一事务内写入，事务回滚则事件一并消失。
            CREATE TABLE IF NOT EXISTS events (
                sequence      INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type    TEXT NOT NULL,   -- dag.created/task.claimed/task.requeued/task.succeeded/task.failed/task.blocked
                dag_id        TEXT NOT NULL,
                task_id       TEXT,            -- dag.created 时为 NULL
                attempt       INTEGER,         -- 任务当前尝试序号；dag.created 时为 NULL
                from_state    TEXT,            -- 变更前状态；dag.created 时为 NULL
                to_state      TEXT,            -- 变更后状态；dag.created 时为 NULL
                occurred_at   TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_dag_state ON tasks(dag_id, state);
            CREATE INDEX IF NOT EXISTS idx_deps_dep ON dependencies(dag_id, dep_key);
            CREATE INDEX IF NOT EXISTS idx_attempts_task ON attempts(task_id, attempt_no);
            CREATE INDEX IF NOT EXISTS idx_events_dag_seq ON events(dag_id, sequence);

            -- 事件流不可变：任何 UPDATE/DELETE 都被中止（INSERT 与事务回滚不受影响）
            CREATE TRIGGER IF NOT EXISTS events_no_update
            BEFORE UPDATE ON events
            BEGIN
                SELECT RAISE(ABORT, 'event stream is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS events_no_delete
            BEFORE DELETE ON events
            BEGIN
                SELECT RAISE(ABORT, 'event stream is immutable');
            END;
            """
        )

        # 旧版 SQLite 数据库迁移：为已有任务补齐显式失败计数列。
        # ADD COLUMN ... NOT NULL DEFAULT 0 会自动为所有历史任务填入 0
        # —— 这些任务此前尚无独立失败计数，默认从未显式失败，保留原重试预算。
        columns = {r[1] for r in conn.execute("PRAGMA table_info(tasks)").fetchall()}
        if "failure_count" not in columns:
            conn.execute(
                "ALTER TABLE tasks ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0"
            )


@contextmanager
def get_conn() -> Iterator[sqlite3.Connection]:
    """返回一个开启外键/WAL 的连接，调用方负责 commit/rollback。"""
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()
