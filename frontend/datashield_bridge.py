"""
E-commerce real-time sales pipeline guard - Dashboard Event Bridge (v2)
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
import random
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
from fastapi.responses import FileResponse
from kafka import KafkaConsumer, TopicPartition

KAFKA_BOOTSTRAP = os.environ.get("DATASHIELD_KAFKA_BOOTSTRAP", "localhost:29092")
BRIDGE_PORT = int(os.environ.get("BRIDGE_PORT", 8099))
REPLAY_PER_TOPIC = int(os.environ.get("BRIDGE_REPLAY_PER_TOPIC", 8))
SERVICES_HOST = os.environ.get("BRIDGE_SERVICES_HOST", "localhost")
MONITOR_NAME = os.environ.get("BRIDGE_MONITOR_NAME", "orders_pipeline_drift")
AIRFLOW_HOST_PORT = int(os.environ.get("BRIDGE_AIRFLOW_PORT", "18080"))
DASHBOARD_PATH = os.environ.get("DASHBOARD_HTML", "/app/dashboard.html")
BACKLOG_SIZE = 300
# Free-tier / showcase mode: no Kafka or backend services needed. The bridge emits
# synthetic pipeline events and reports synthetic service status.
DEMO_MODE = os.environ.get("DATASHIELD_DEMO_MODE", "0").lower() in ("1", "true", "yes")

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
SERVICE_HOSTPORTS = {
    8092: os.environ.get("DATASHIELD_SIDECAR_HOSTPORT"),
    8093: os.environ.get("DATASHIELD_ISOFOREST_HOSTPORT"),
    8010: os.environ.get("DATASHIELD_DRIFT_HOSTPORT"),
    8020: os.environ.get("DATASHIELD_BREAKER_HOSTPORT"),
    8030: os.environ.get("DATASHIELD_RCA_HOSTPORT"),
    8040: os.environ.get("DATASHIELD_SCHEMA_HOSTPORT"),
    8090: os.environ.get("DATASHIELD_GATEWAY_HOSTPORT"),
    8091: os.environ.get("DATASHIELD_CATALOG_HOSTPORT"),
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
    target = demo_forever if DEMO_MODE else consume_forever
    threading.Thread(target=target, daemon=True, name="demo-feed" if DEMO_MODE else "kafka-consumer").start()
    yield


app = FastAPI(title="E-commerce real-time sales pipeline guard Dashboard Bridge", lifespan=lifespan)
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


# --------------------------------------------------------------------------- demo mode
def _demo_order(i: int, drift: float = 0.0) -> dict:
    total = round(max(1.0, random.gauss(50 + drift * 90, 15)), 2)
    return {
        "order_id": f"ord-{i:07d}", "order_total": total, "item_count": max(1, int(random.gauss(3, 1))),
        "payment_method": random.choice(["card", "paypal", "upi", "cod"]),
        "region": random.choice(["NA", "EU", "APAC", "LATAM"]),
        "customer_id": f"cust-{random.randint(1, 50000)}",
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def demo_forever() -> None:
    """Synthetic event feed that mimics the real topic flow, including anomalies,
    schema mismatches (healed or sent to the DLQ), drift and circuit-breaker events."""
    stats["kafka_connected"], stats["last_error"] = True, None
    i, drift, seq = 0, 0.0, Counter()

    def emit(topic: str, payload: dict) -> None:
        seq[topic] += 1
        _publish(_make_event(topic, 0, seq[topic], json.dumps(payload), None, replayed=False))

    while True:
        i += 1
        drift = min(1.0, drift + 0.01) if (i // 120) % 2 == 0 else max(0.0, drift - 0.02)
        rec = _demo_order(i, drift)
        emit("raw-data", rec)
        r = random.random()
        if r < 0.06:                      # statistical outlier -> Isolation Forest
            rec["order_total"] = round(random.uniform(5000, 25000), 2)
            emit("anomaly-alerts", {"order_id": rec["order_id"], "score": round(random.uniform(-0.8, -0.55), 3),
                                    "reason": "isolation_forest_outlier", "order_total": rec["order_total"]})
        elif r < 0.12:                    # schema mismatch -> healer or DLQ
            bad = dict(rec); bad["order_amt"] = str(bad.pop("order_total"))
            emit("schema-evolution-requests", {"order_id": bad["order_id"], "mismatch": {"missing": ["order_total"], "unexpected": ["order_amt"]}})
            if random.random() < 0.75:
                bad["order_total"] = float(bad.pop("order_amt")); bad["_schema_healer"] = {"mapping": "order_amt->order_total"}
                emit("validated-data", bad)
            else:
                emit("dead-letter-queue", {"order_id": bad["order_id"], "reason": "healing_failed"})
                emit("remediation-requests", {"monitor": "orders_pipeline_drift", "attempt": random.randint(1, 3)})
        else:
            emit("validated-data", rec)
        if i % 40 == 0:
            emit("drift-events", {"monitor": MONITOR_NAME, "psi": round(0.05 + drift * 0.4, 3), "drift_detected": drift > 0.5})
        if i % 90 == 0:
            emit("openlineage-events", {"job": "orders_validation", "state": "COMPLETE"})
        time.sleep(random.uniform(0.4, 1.2))


def _demo_services() -> dict:
    def ok(path, data):
        return {"ok": True, "status": 200, "latency_ms": random.randint(4, 40), "path": path, "data": data}
    svc = {
        "sidecar": ok("/contracts", ["orders_v1"]),
        "isoforest": ok("/health", {"status": "ok", "model_loaded": True, "bootstrap_buffer_size": 500}),
        "drift": ok("/health", {"status": "ok", "monitor_name": MONITOR_NAME}),
        "breaker": ok("/health", {"status": "ok"}),
        "rca": ok("/health", {"status": "ok"}),
        "schema": ok("/health", {"status": "ok"}),
        "gateway": ok("/health", {"status": "ok"}),
        "catalog": ok("/catalog", {"orders_v1": {}}),
        "airflow": ok("/health", {"status": "ok"}),
    }
    extra = {
        "breaker_state": ok(f"/state/{MONITOR_NAME}", {"state": "CLOSED", "trip_count": 0, "consecutive_failures": 0}),
        "rca_latest": ok(f"/reports/{MONITOR_NAME}?limit=1", [{"severity": "medium", "root_cause_category": "upstream_schema_change"}]),
        "isoforest_model": ok("/model-info", {"n_samples": 500}),
    }
    return {
        "checked_at": datetime.now(timezone.utc).isoformat(), "demo": True,
        "kafka": {"connected": True, "bootstrap": "demo-mode (synthetic)", "last_error": None,
                  "last_event_at": stats["last_event_at"], "topic_counts": dict(stats["topic_counts"]), "replayed_on_start": 0},
        "services": svc, "extra": extra,
    }


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
    host = SERVICE_HOSTPORTS.get(port) or f"{SERVICES_HOST}:{port}"
    url = f"http://{host}{path}"
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


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(DASHBOARD_PATH, media_type="text/html")


@app.get("/services")
async def services():
    if DEMO_MODE:
        return _demo_services()
    return await asyncio.get_running_loop().run_in_executor(None, poll_all)


# --------------------------------------------------------------------------- API
@app.get("/health")
def health():
    return {
        "status": "ok", "demo_mode": DEMO_MODE, "kafka_bootstrap": KAFKA_BOOTSTRAP, "kafka_connected": stats["kafka_connected"],
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

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", BRIDGE_PORT)))
