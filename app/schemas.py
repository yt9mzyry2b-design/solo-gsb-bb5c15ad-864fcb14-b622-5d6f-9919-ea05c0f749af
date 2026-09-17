"""Pydantic 请求模型。"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class TaskIn(BaseModel):
    key: str = Field(..., min_length=1, description="DAG 内唯一任务键")
    name: str = ""
    payload: Any = None
    deps: list[str] = Field(default_factory=list, description="前置任务的 key 列表")
    max_retries: int = Field(0, ge=0, description="失败后的额外重试次数")
    lease_seconds: float | None = Field(None, gt=0, description="租约时长，缺省取服务端默认值")


class DAGSubmitIn(BaseModel):
    name: str = "dag"
    tasks: list[TaskIn] = Field(..., min_length=1)
    id: str | None = Field(None, description="可选自定义 DAG id；已存在则返回 409")


class ClaimIn(BaseModel):
    worker_id: str | None = None
    dag_id: str | None = None
    limit: int = Field(1, ge=1, le=100)


class HeartbeatIn(BaseModel):
    lease_token: str
    extend_seconds: float | None = Field(None, gt=0)


class CompleteIn(BaseModel):
    completion_id: str = Field(
        ...,
        min_length=1,
        description="客户端生成的幂等键；同 task_id + completion_id 的等价回报只生效一次",
    )
    lease_token: str
    success: bool
    result: Any = None
    error: str | None = None
