"""
FastAPI chat application backed by PostgreSQL.

Endpoints:
- POST /api/conversations          Create conversation
- GET  /api/conversations          List conversations
- GET  /api/conversations/{id}/messages   Get messages
- POST /api/conversations/{id}/messages   Add message
- POST /api/conversations/{id}/stream     Streaming message (holds transaction)
- GET  /api/conversations/search?q=term  Search messages
- DELETE /api/conversations/{id}   Delete conversation
- POST /api/notifications/broadcast       Broadcast notification to all users
- GET  /api/notifications                 List user's notifications
- GET  /api/notifications/unread-count    Count unread notifications
- POST /api/notifications/mark-read       Mark all notifications read
- GET  /api/notifications/poll            Long-poll for new notifications
- GET  /health                     Health check
- GET  /metrics                    Prometheus-format metrics
"""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel

from app.models import (
    add_message,
    broadcast_notification,
    broadcast_notification_serializable,
    create_conversation,
    create_schema,
    delete_conversation,
    ensure_default_user,
    get_messages,
    get_unread_count,
    list_notifications,
    mark_all_read,
    poll_notifications,
    search_messages,
    list_conversations,
)
from app.pool import create_pool
from app.streaming import stream_response

# Default user for simplicity (a real app would have auth)
DEFAULT_USER_ID = os.environ.get(
    "DEFAULT_USER_ID", "00000000-0000-4000-8000-000000000001"
)

# Metrics counters
_metrics = {
    "requests_total": 0,
    "requests_5xx": 0,
    "latency_sum_ms": 0.0,
    "latency_count": 0,
    "latency_max_ms": 0.0,
    # Histogram buckets for latency (ms)
    "latency_bucket_10": 0,
    "latency_bucket_50": 0,
    "latency_bucket_100": 0,
    "latency_bucket_250": 0,
    "latency_bucket_500": 0,
    "latency_bucket_1000": 0,
    "latency_bucket_5000": 0,
    "latency_bucket_inf": 0,
}

_start_time = time.monotonic()
_pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    dsn = os.environ.get("DATABASE_URL", "postgresql://chatapp:chatapp@postgres:5432/chatdb")
    _pool = await create_pool(dsn)
    await create_schema(_pool)
    await ensure_default_user(_pool, DEFAULT_USER_ID)
    yield
    if _pool:
        await _pool.close()


app = FastAPI(title="Chat DB App", lifespan=lifespan)


# ---------- Middleware for metrics ----------

@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.monotonic()
    response = None
    try:
        response = await call_next(request)
    except Exception:
        _metrics["requests_5xx"] += 1
        _metrics["requests_total"] += 1
        raise
    finally:
        elapsed_ms = (time.monotonic() - start) * 1000
        _metrics["requests_total"] += 1
        _metrics["latency_sum_ms"] += elapsed_ms
        _metrics["latency_count"] += 1
        if elapsed_ms > _metrics["latency_max_ms"]:
            _metrics["latency_max_ms"] = elapsed_ms

        # Histogram buckets
        if elapsed_ms <= 10:
            _metrics["latency_bucket_10"] += 1
        if elapsed_ms <= 50:
            _metrics["latency_bucket_50"] += 1
        if elapsed_ms <= 100:
            _metrics["latency_bucket_100"] += 1
        if elapsed_ms <= 250:
            _metrics["latency_bucket_250"] += 1
        if elapsed_ms <= 500:
            _metrics["latency_bucket_500"] += 1
        if elapsed_ms <= 1000:
            _metrics["latency_bucket_1000"] += 1
        if elapsed_ms <= 5000:
            _metrics["latency_bucket_5000"] += 1
        _metrics["latency_bucket_inf"] += 1

        if response and response.status_code >= 500:
            _metrics["requests_5xx"] += 1

    return response


# ---------- Middleware for unread notification count ----------

@app.middleware("http")
async def unread_count_middleware(request: Request, call_next):
    """Attach X-Unread-Count header to all /api/ responses."""
    response = await call_next(request)
    if request.url.path.startswith("/api/"):
        try:
            count = await get_unread_count(_pool, DEFAULT_USER_ID)
            response.headers["X-Unread-Count"] = str(count)
        except Exception:
            pass
    return response


# ---------- Request/Response models ----------

class CreateConversationRequest(BaseModel):
    title: str = "New conversation"


class AddMessageRequest(BaseModel):
    content: str
    role: str = "user"
    token_count: int = 0


class StreamRequest(BaseModel):
    content: str
    token_count: int = 0


class BroadcastRequest(BaseModel):
    type: str = "system"
    payload: dict = {}


class PollRequest(BaseModel):
    since: str | None = None


# ---------- Endpoints ----------

@app.post("/api/conversations")
async def api_create_conversation(req: CreateConversationRequest):
    conv = await create_conversation(_pool, DEFAULT_USER_ID, req.title)
    return _serialize(conv)


@app.get("/api/conversations")
async def api_list_conversations():
    convs = await list_conversations(_pool, DEFAULT_USER_ID)
    return [_serialize(c) for c in convs]


@app.get("/api/conversations/search")
async def api_search_messages(q: str = ""):
    if not q.strip():
        return []
    results = await search_messages(_pool, DEFAULT_USER_ID, q.strip())
    return [_serialize(r) for r in results]


@app.get("/api/conversations/{conversation_id}/messages")
async def api_get_messages(conversation_id: str):
    msgs = await get_messages(_pool, conversation_id)
    return [_serialize(m) for m in msgs]


@app.post("/api/conversations/{conversation_id}/messages")
async def api_add_message(conversation_id: str, req: AddMessageRequest):
    try:
        msg = await add_message(
            _pool, conversation_id, req.role, req.content, req.token_count
        )
        return _serialize(msg)
    except asyncpg.ForeignKeyViolationError:
        raise HTTPException(status_code=404, detail="Conversation not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/conversations/{conversation_id}/stream")
async def api_stream_message(conversation_id: str, req: StreamRequest):
    """
    Simulate streaming response. Holds transaction open during generation.
    Returns the response chunks (a real app would use SSE/WebSocket).
    """
    try:
        chunks = await stream_response(
            _pool, conversation_id, req.content, req.token_count
        )
        return {"chunks": chunks, "full_response": "".join(chunks)}
    except asyncpg.ForeignKeyViolationError:
        raise HTTPException(status_code=404, detail="Conversation not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/conversations/{conversation_id}")
async def api_delete_conversation(conversation_id: str):
    deleted = await delete_conversation(_pool, conversation_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return {"deleted": True}


# ---------- Notification endpoints ----------

@app.post("/api/notifications/broadcast")
async def api_broadcast_notification(req: BroadcastRequest):
    """Broadcast a notification to all users."""
    try:
        count = await broadcast_notification(_pool, req.type, req.payload)
        return {"broadcast": True, "recipients": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/notifications/broadcast-serializable")
async def api_broadcast_notification_serializable(req: BroadcastRequest):
    """Broadcast with SERIALIZABLE isolation (for serialize chaos type)."""
    try:
        count = await broadcast_notification_serializable(_pool, req.type, req.payload)
        return {"broadcast": True, "recipients": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/notifications")
async def api_list_notifications(user_id: str | None = None):
    """List notifications for a user."""
    uid = user_id or DEFAULT_USER_ID
    try:
        notifs = await list_notifications(_pool, uid)
        return [_serialize(n) for n in notifs]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/notifications/unread-count")
async def api_unread_count(user_id: str | None = None):
    """Get unread notification count."""
    uid = user_id or DEFAULT_USER_ID
    try:
        count = await get_unread_count(_pool, uid)
        return {"unread_count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/notifications/mark-read")
async def api_mark_read(user_id: str | None = None):
    """Mark all notifications as read for a user."""
    uid = user_id or DEFAULT_USER_ID
    try:
        count = await mark_all_read(_pool, uid)
        return {"marked_read": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/notifications/poll")
async def api_poll_notifications(user_id: str | None = None, since: str | None = None):
    """Long-poll for new notifications."""
    uid = user_id or DEFAULT_USER_ID
    try:
        notifs = await poll_notifications(_pool, uid, since)
        return [_serialize(n) for n in notifs]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    """Health check - verifies pool connectivity."""
    try:
        async with _pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        pool_size = _pool.get_size()
        return {
            "status": "healthy",
            "pool_size": pool_size,
            "pool_free": _pool.get_idle_size(),
            "uptime_seconds": round(time.monotonic() - _start_time, 1),
        }
    except Exception as e:
        return Response(
            content=f'{{"status": "unhealthy", "error": "{e}"}}',
            status_code=503,
            media_type="application/json",
        )


@app.get("/metrics")
async def metrics():
    """Prometheus-format metrics endpoint."""
    pool_size = _pool.get_size() if _pool else 0
    pool_free = _pool.get_idle_size() if _pool else 0
    pool_used = pool_size - pool_free
    pool_min = _pool.get_min_size() if _pool else 0
    pool_max = _pool.get_max_size() if _pool else 0

    uptime = time.monotonic() - _start_time
    avg_latency = (
        _metrics["latency_sum_ms"] / _metrics["latency_count"]
        if _metrics["latency_count"] > 0
        else 0
    )
    rps = _metrics["requests_total"] / uptime if uptime > 0 else 0
    error_rate = (
        _metrics["requests_5xx"] / _metrics["requests_total"] * 100
        if _metrics["requests_total"] > 0
        else 0
    )

    # Calculate p50 and p99 approximations from histogram buckets
    total = _metrics["latency_bucket_inf"]
    p50_ms = _estimate_percentile(0.50, total)
    p99_ms = _estimate_percentile(0.99, total)

    lines = [
        "# HELP chatdb_pool_connections_active Active connections in the pool",
        "# TYPE chatdb_pool_connections_active gauge",
        f"chatdb_pool_connections_active {pool_used}",
        "",
        "# HELP chatdb_pool_connections_idle Idle connections in the pool",
        "# TYPE chatdb_pool_connections_idle gauge",
        f"chatdb_pool_connections_idle {pool_free}",
        "",
        "# HELP chatdb_pool_connections_total Total connections in the pool",
        "# TYPE chatdb_pool_connections_total gauge",
        f"chatdb_pool_connections_total {pool_size}",
        "",
        "# HELP chatdb_pool_connections_max_size Max pool size (0 = unlimited)",
        "# TYPE chatdb_pool_connections_max_size gauge",
        f"chatdb_pool_connections_max_size {pool_max}",
        "",
        "# HELP chatdb_pool_connections_min_size Min pool size",
        "# TYPE chatdb_pool_connections_min_size gauge",
        f"chatdb_pool_connections_min_size {pool_min}",
        "",
        "# HELP chatdb_requests_total Total HTTP requests",
        "# TYPE chatdb_requests_total counter",
        f"chatdb_requests_total {_metrics['requests_total']}",
        "",
        "# HELP chatdb_requests_5xx_total Total 5xx responses",
        "# TYPE chatdb_requests_5xx_total counter",
        f"chatdb_requests_5xx_total {_metrics['requests_5xx']}",
        "",
        "# HELP chatdb_request_duration_ms_avg Average request duration in ms",
        "# TYPE chatdb_request_duration_ms_avg gauge",
        f"chatdb_request_duration_ms_avg {avg_latency:.2f}",
        "",
        "# HELP chatdb_request_duration_ms_p50 P50 request duration in ms",
        "# TYPE chatdb_request_duration_ms_p50 gauge",
        f"chatdb_request_duration_ms_p50 {p50_ms:.2f}",
        "",
        "# HELP chatdb_request_duration_ms_p99 P99 request duration in ms",
        "# TYPE chatdb_request_duration_ms_p99 gauge",
        f"chatdb_request_duration_ms_p99 {p99_ms:.2f}",
        "",
        "# HELP chatdb_request_duration_ms_max Max request duration in ms",
        "# TYPE chatdb_request_duration_ms_max gauge",
        f"chatdb_request_duration_ms_max {_metrics['latency_max_ms']:.2f}",
        "",
        "# HELP chatdb_requests_per_second Current requests per second",
        "# TYPE chatdb_requests_per_second gauge",
        f"chatdb_requests_per_second {rps:.2f}",
        "",
        "# HELP chatdb_error_rate_percent Percentage of 5xx responses",
        "# TYPE chatdb_error_rate_percent gauge",
        f"chatdb_error_rate_percent {error_rate:.2f}",
        "",
        "# HELP chatdb_uptime_seconds Application uptime in seconds",
        "# TYPE chatdb_uptime_seconds gauge",
        f"chatdb_uptime_seconds {uptime:.1f}",
        "",
        "# HELP chatdb_request_duration_ms_bucket Request duration histogram",
        "# TYPE chatdb_request_duration_ms_bucket histogram",
        f'chatdb_request_duration_ms_bucket{{le="10"}} {_metrics["latency_bucket_10"]}',
        f'chatdb_request_duration_ms_bucket{{le="50"}} {_metrics["latency_bucket_50"]}',
        f'chatdb_request_duration_ms_bucket{{le="100"}} {_metrics["latency_bucket_100"]}',
        f'chatdb_request_duration_ms_bucket{{le="250"}} {_metrics["latency_bucket_250"]}',
        f'chatdb_request_duration_ms_bucket{{le="500"}} {_metrics["latency_bucket_500"]}',
        f'chatdb_request_duration_ms_bucket{{le="1000"}} {_metrics["latency_bucket_1000"]}',
        f'chatdb_request_duration_ms_bucket{{le="5000"}} {_metrics["latency_bucket_5000"]}',
        f'chatdb_request_duration_ms_bucket{{le="+Inf"}} {_metrics["latency_bucket_inf"]}',
        "",
    ]
    return Response(content="\n".join(lines), media_type="text/plain")


def _estimate_percentile(pct: float, total: int) -> float:
    """Estimate percentile from histogram buckets."""
    if total == 0:
        return 0.0
    target = pct * total
    buckets = [
        (10, _metrics["latency_bucket_10"]),
        (50, _metrics["latency_bucket_50"]),
        (100, _metrics["latency_bucket_100"]),
        (250, _metrics["latency_bucket_250"]),
        (500, _metrics["latency_bucket_500"]),
        (1000, _metrics["latency_bucket_1000"]),
        (5000, _metrics["latency_bucket_5000"]),
    ]
    for bound, count in buckets:
        if count >= target:
            return float(bound)
    return 5000.0


def _serialize(row: dict) -> dict:
    """Convert UUID/datetime fields to strings for JSON serialization."""
    result = {}
    for k, v in row.items():
        if hasattr(v, "hex"):  # UUID
            result[k] = str(v)
        elif hasattr(v, "isoformat"):  # datetime
            result[k] = v.isoformat()
        else:
            result[k] = v
    return result
