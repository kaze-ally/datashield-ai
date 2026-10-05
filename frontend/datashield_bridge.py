"""
DataShield AI - Dashboard Event Bridge (v2)
-------------------------------------------
Runs on the HOST (not in Docker) and gives the Pipeline Observatory page three things:

  1. LIVE Kafka events over a WebSocket (/ws) and a REST fallback (/events/recent)
  2. HISTORY: on startup it replays the last few messages of every watched topic so
     the feed is not empty when you open the page (flagged "replayed": true)
  3. SERVICE STATUS: GET /services polls all nine service APIs server-side and returns
     one JSON document. Doing this here instead of from the browser avoids CORS
     problems entirely (only schema_healer sets CORS headers) and works from file://.

Run (from anywhere on the host; Kafka's host port 29092 is already published):

    pip install fastapi "uvicorn[standard]" kafka-python
    python datashield_bridge.py

Env vars (all optional):
    DATASHIELD_KAFKA_BOOTSTRAP   default localhost:29092
    BRIDGE_PORT                  default 8099
    BRIDGE_REPLAY_PER_TOPIC      default 8   (0 disables history replay)
    BRIDGE_SERVICES_HOST         default localhost
    BRIDGE_MONITOR_NAME          default orders_pipeline_drift
    KAFKA_SECURITY_PROTOCOL / KAFKA_SASL_MECHANISM / KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD
"""
import asyncio
import json
import os
import threading
import time
import urllib.error
import urllib.request
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer, TopicPartition

KAFKA_BOOTSTRAP = os.environ.get("DATASHIELD_KAFKA_BOOTSTRAP", "localhost:29092")
BRIDGE_PORT = int(os.environ.get("BRIDGE_PORT", 8099))
REPLAY_PER_TOPIC = int(os.environ.get("BRIDGE_REPLAY_PER_TOPIC", 8))
SERVICES_HOST = os.environ.get("BRIDGE_SERVICES_HOST", "localhost")
MONITOR_NAME = os.environ.get("BRIDGE_MONITOR_NAME", "orders_pipeline_drift")
AIRFLOW_HOST_PORT = int(os.environ.get("BRIDGE_AIRFLOW_PORT", "18080"))
BACKLOG_SIZE = 300

KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = os.environ.get("KAFKA_SASL_USERNAME")
KAFKA_SASL_PASSWORD = os.environ.get("KAFKA_SASL_PASSWORD")

TOPICS = [
    "raw-data", "validated-data", "dead-letter-queue",
    "drift-events", "anomaly-alerts",
    "schema-events", "schema-evolution-requests",
    "remediation-requests", "remediation-dlq",
    "openlineage-events", "catalog-updates",
]

# topic -> dashboard card id
TOPIC_TO_CARD = {
    "raw-data": "kafka",
    "validated-data": "sidecar",
    "dead-letter-queue": "schema",
    "drift-events": "drift",
    "anomaly-alerts": "isoforest",
    "schema-events": "catalog",            # schema-events feeds the AI Catalog consumer
    "schema-evolution-requests": "schema",
    "remediation-requests": "breaker",
    "remediation-dlq": "breaker",
    "openlineage-events": "catalog",
    "catalog-updates": "catalog",
}

# id -> (host port, path)
SERVICES = {
    "sidecar":   (8092, "/contracts"),
    "isoforest": (8093, "/health"),
    "drift":     (8010, "/health"),
    "breaker":   (8020, "/health"),
    "rca":       (8030, "/health"),
    "schema":    (8040, "/health"),
    "gateway":   (8090, "/health"),
    "catalog":   (8091, "/catalog"),
    "airflow":   (AIRFLOW_HOST_PORT, "/health"),
}
# Richer, read-only detail endpoints shown on the cards (all GET, none mutate state).
EXTRA = {
    "sidecar_health": (8092, "/health"),
    "breaker_state":   (8020, f"/state/{MONITOR_NAME}"),
    "breaker_anomaly_state": (8020, "/state/isolation_forest_anomaly"),
    "breaker_events":  (8020, f"/events/{MONITOR_NAME}?limit=5"),
    "breaker_anomaly_events": (8020, "/events/isolation_forest_anomaly?limit=5"),
    "rca_latest":      (8030, f"/reports/{MONITOR_NAME}?limit=1"),
    "rca_anomaly_latest": (8030, "/reports/isolation_forest_anomaly?limit=1"),
    "isoforest_model": (8093, "/model-info"),
    "schema_reports":  (8040, "/reports/orders_v1?limit=5"),
    "schema_guard":    (8040, "/guard/orders_v1"),
    "drift_reports":   (8010, "/reports?limit=5"),
}

backlog: deque = deque(maxlen=BACKLOG_SIZE)
clients: set = set()
main_loop = None
stats: Dict[str, Any] = {
    "kafka_connected": False, "last_error": None, "last_event_at": None,
    "topic_counts": Counter(), "replayed": 0,
}
_http_pool = ThreadPoolExecutor(max_workers=12, thread_name_prefix="svc-poll")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global main_loop
    main_loop = asyncio.get_running_loop()
    threading.Thread(target=consume_forever, daemon=True, name="kafka-consumer").start()
    yield


app = FastAPI(title="DataShield Dashboard Bridge", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
# ^ dev-only bridge for a local dashboard; tighten if you ever expose this beyond localhost.


# --------------------------------------------------------------------------- Kafka
def _security_kwargs() -> dict:
    if KAFKA_SECURITY_PROTOCOL == "PLAINTEXT":
        return {}
    return dict(
        security_protocol=KAFKA_SECURITY_PROTOCOL, sasl_mechanism=KAFKA_SASL_MECHANISM,
        sasl_plain_username=KAFKA_SASL_USERNAME, sasl_plain_password=KAFKA_SASL_PASSWORD,
    )


def _make_event(topic: str, partition: int, offset: int, raw: str, ts_ms: Optional[int], replayed: bool) -> dict:
    try:
        payload: Any = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        payload = {"raw": raw}
    when = (
        datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc) if (replayed and ts_ms) else datetime.now(timezone.utc)
    )
    card = TOPIC_TO_CARD.get(topic, "kafka")
    if topic == "validated-data" and isinstance(payload, dict) and "_schema_healer" in payload:
        card = "schema"
    return {
        "id": f"{topic}:{partition}:{offset}",
        "topic": topic, "card": card, "replayed": replayed,
        "received_at": when.isoformat(), "payload": payload,
    }


def _publish(event: dict) -> None:
    backlog.append(event)
    stats["topic_counts"][event["topic"]] += 1
    stats["last_event_at"] = event["received_at"]
    if main_loop is not None:
        asyncio.run_coroutine_threadsafe(broadcast(event), main_loop)


def replay_history() -> None:
    """Read-only: seed the backlog with the last N messages per topic (no consumer group, no commits)."""
    if REPLAY_PER_TOPIC <= 0:
        return
    c = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP, group_id=None, enable_auto_commit=False,
        value_deserializer=lambda v: v.decode("utf-8", errors="replace"), consumer_timeout_ms=1500,
        **_security_kwargs(),
    )
    try:
        tps = []
        for topic in TOPICS:
            for p in sorted(c.partitions_for_topic(topic) or []):
                tps.append(TopicPartition(topic, p))
        if not tps:
            return
        c.assign(tps)
        ends, begins = c.end_offsets(tps), c.beginning_offsets(tps)
        for tp in tps:
            c.seek(tp, max(begins[tp], ends[tp] - REPLAY_PER_TOPIC))
        collected = []
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and any(c.position(tp) < ends[tp] for tp in tps):
            for batch in c.poll(timeout_ms=500).values():
                collected.extend(batch)
        collected.sort(key=lambda m: m.timestamp or 0)
        # keep only the newest REPLAY_PER_TOPIC per topic overall (partitions are merged)
        per_topic: Dict[str, list] = {}
        for m in collected:
            per_topic.setdefault(m.topic, []).append(m)
        picked = [m for ms in per_topic.values() for m in ms[-REPLAY_PER_TOPIC:]]
        picked.sort(key=lambda m: m.timestamp or 0)
        for m in picked:
            backlog.append(_make_event(m.topic, m.partition, m.offset, m.value, m.timestamp, replayed=True))
        stats["replayed"] = len(picked)
        print(f"[bridge] replayed {len(picked)} historical events (up to {REPLAY_PER_TOPIC}/topic)")
    finally:
        c.close()


def consume_forever() -> None:
    replayed_once = False
    while True:
        try:
            if not replayed_once:
                try:
                    replay_history()
                except Exception as e:  # history is a nicety - never block live streaming on it
                    print(f"[bridge] history replay skipped: {e}")
                replayed_once = True
            consumer = KafkaConsumer(
                *TOPICS, bootstrap_servers=KAFKA_BOOTSTRAP, auto_offset_reset="latest", group_id=None,
                enable_auto_commit=False, value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
                consumer_timeout_ms=1000, **_security_kwargs(),
            )
            stats["kafka_connected"], stats["last_error"] = True, None
            print(f"[bridge] live: connected to Kafka at {KAFKA_BOOTSTRAP}, watching {len(TOPICS)} topics")
            while True:
                for msg in consumer:
                    _publish(_make_event(msg.topic, msg.partition, msg.offset, msg.value, msg.timestamp, replayed=False))
        except Exception as e:
            stats["kafka_connected"], stats["last_error"] = False, str(e)
            print(f"[bridge] Kafka connection issue ({e}); retrying in 5s")
            time.sleep(5)


async def broadcast(event: dict) -> None:
    dead = []
    for ws in list(clients):
        try:
            await ws.send_json(event)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


# --------------------------------------------------------------------------- service polling
def _get(port: int, path: str, timeout: float = 3.0) -> dict:
    url = f"http://{SERVICES_HOST}:{port}{path}"
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            body = r.read(200_000).decode("utf-8", errors="replace")
            try:
                data: Any = json.loads(body)
            except json.JSONDecodeError:
                data = body[:500]
            return {"ok": 200 <= r.status < 300, "status": r.status, "latency_ms": round((time.monotonic() - t0) * 1000),
                    "path": path, "data": data}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code, "latency_ms": round((time.monotonic() - t0) * 1000), "path": path,
                "error": f"HTTP {e.code}"}
    except Exception as e:  # connection refused (container stopped), timeout, ...
        return {"ok": False, "status": None, "latency_ms": round((time.monotonic() - t0) * 1000), "path": path,
                "error": type(e).__name__ + ": " + str(getattr(e, "reason", e))}


def poll_all() -> dict:
    jobs = {k: _http_pool.submit(_get, *v) for k, v in {**SERVICES, **{f"x:{k}": v for k, v in EXTRA.items()}}.items()}
    results = {k: f.result() for k, f in jobs.items()}
    return {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "kafka": {
            "connected": stats["kafka_connected"], "bootstrap": KAFKA_BOOTSTRAP, "last_error": stats["last_error"],
            "last_event_at": stats["last_event_at"], "topic_counts": dict(stats["topic_counts"]),
            "replayed_on_start": stats["replayed"],
        },
        "services": {k: v for k, v in results.items() if not k.startswith("x:")},
        "extra": {k[2:]: v for k, v in results.items() if k.startswith("x:")},
    }


@app.get("/services")
async def services():
    return await asyncio.get_running_loop().run_in_executor(None, poll_all)


# --------------------------------------------------------------------------- API
@app.get("/health")
def health():
    return {
        "status": "ok", "kafka_bootstrap": KAFKA_BOOTSTRAP, "kafka_connected": stats["kafka_connected"],
        "last_error": stats["last_error"], "last_event_at": stats["last_event_at"],
        "topics": TOPICS, "connected_clients": len(clients), "backlog_size": len(backlog),
        "replayed_on_start": stats["replayed"], "topic_counts": dict(stats["topic_counts"]),
    }


@app.get("/events/recent")
def recent(limit: int = 100):
    return list(backlog)[-limit:]


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    try:
        for event in list(backlog)[-40:]:
            await websocket.send_json(event)
        while True:
            await websocket.receive_text()  # keep-alive; the client sends nothing meaningful
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        clients.discard(websocket)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=BRIDGE_PORT)
