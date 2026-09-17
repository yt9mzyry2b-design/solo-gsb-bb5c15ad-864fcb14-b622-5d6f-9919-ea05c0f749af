"""FastAPI 入口：依赖感知的任务租约 API。

启动: uvicorn app.main:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from fastapi.responses import Response, JSONResponse

from . import service
from .db import init_db
from .schemas import ClaimIn, CompleteIn, DAGSubmitIn, HeartbeatIn


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Dependency-Aware Task Lease API",
    version="1.0.0",
    description="提交 DAG、原子领取前置已完成的任务、基于租约令牌回报结果。",
    lifespan=lifespan,
)


@app.exception_handler(service.APIError)
async def api_error_handler(_request: Request, exc: service.APIError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message, "details": exc.details}},
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/dags", status_code=201)
def submit_dag(body: DAGSubmitIn) -> dict:
    return service.submit_dag(body.model_dump())


@app.get("/dags/{dag_id}")
def get_dag(dag_id: str) -> dict:
    return service.get_dag(dag_id)


@app.post("/tasks/claim")
def claim_tasks(body: ClaimIn) -> dict:
    return service.claim_tasks(body.worker_id, body.dag_id, body.limit)


@app.get("/tasks")
def list_tasks(dag_id: str | None = None, state: str | None = None) -> dict:
    return service.list_tasks(dag_id, state)


@app.get("/events")
def list_events(
    after: int = Query(0, ge=0, description="只返回 sequence 大于该值的事件"),
    limit: int = Query(50, ge=1, le=100, description="本页最多返回的事件数（1~100）"),
    dag_id: str | None = Query(None, description="可选：只返回该 DAG 的事件"),
) -> dict:
    return service.list_events(after, limit, dag_id)


@app.get("/tasks/{task_id}")
def get_task(task_id: str) -> dict:
    return service.get_task(task_id)


@app.post("/tasks/{task_id}/heartbeat")
def heartbeat(task_id: str, body: HeartbeatIn) -> dict:
    return service.heartbeat(task_id, body.model_dump())


@app.post("/tasks/{task_id}/complete")
def complete_task(task_id: str, body: CompleteIn):
    result = service.complete_task(task_id, body.model_dump())
    if isinstance(result, service.ReplayedResponse):
        # 幂等命中：逐字节回放首次有效回报保存的状态码与响应体
        return Response(
            content=result.body,
            status_code=result.status_code,
            media_type="application/json",
        )
    return result
