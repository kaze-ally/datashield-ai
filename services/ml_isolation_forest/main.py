"""
DataShield AI — Streaming Isolation Forest Anomaly Detector (Papers 1 & 3)
============================================================================
scikit-learn's IsolationForest is a batch algorithm — there's no online
`.partial_fit()` for it. So this service, like every production system that
uses it for streaming data, splits training and serving:

  - SERVING (this process, always running): consumes `validated-data`,
    extracts a small numeric feature vector per order, scores it against
    the currently-loaded model, and publishes anything flagged to
    `anomaly-alerts`.
  - TRAINING (triggered externally, e.g. by Airflow's
    `isolation_forest_retrain` DAG calling POST /retrain): refits on a
    rolling window of recent feature vectors stored in Postgres, and saves
    a new joblib artifact.

Two independent mechanisms pick up a new model, matching how real systems
avoid restart-driven downtime:
  1. `/retrain` updates the in-process model directly (fast path, when this
     service itself did the fit).
  2. A background thread polls the model file's mtime and reloads it if it
     changed on disk (covers the case where something else — a notebook, a
     different training job — wrote a new artifact directly to the shared
     volume).

Cold start: a fresh Isolation Forest can't score anything until it's seen
some data. On startup it first tries persisted feature rows, then existing
validated orders; if neither has enough rows, the first
`MIN_BOOTSTRAP_SAMPLES` validated records are buffered in memory and used to
fit an initial baseline model automatically.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import joblib
import psycopg2
from fastapi import FastAPI
from kafka import KafkaConsumer, KafkaProducer
from sklearn.ensemble import IsolationForest

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ml_isolation_forest")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_APP_DSN = os.environ.get("POSTGRES_APP_DSN", "")
SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "validated-data")
ANOMALY_TOPIC = os.environ.get("ANOMALY_TOPIC", "anomaly-alerts")
MODEL_DIR = Path(os.environ.get("MODEL_DIR", "/app/models"))
MODEL_PATH = MODEL_DIR / "isolation_forest_latest.joblib"
MIN_BOOTSTRAP_SAMPLES = int(os.environ.get("MIN_BOOTSTRAP_SAMPLES", "50"))
RETRAIN_WINDOW_SIZE = int(os.environ.get("RETRAIN_WINDOW_SIZE", "2000"))
RELOAD_POLL_SECONDS = int(os.environ.get("RELOAD_POLL_SECONDS", "30"))

FEATURE_NAMES = ["order_total", "hour_of_day"]
_consumer_thread: Optional[threading.Thread] = None

app = FastAPI(title="DataShield Isolation Forest Detector")


class ModelState:
    """Holds the currently-loaded model plus enough metadata to explain itself via /model-info."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.model: Optional[IsolationForest] = None
        self.trained_at: Optional[str] = None
        self.n_samples: int = 0
        self.last_mtime: float = 0.0
        self.bootstrap_buffer: list[list[float]] = []

    def load_from_disk_if_changed(self) -> bool:
        if not MODEL_PATH.exists():
            return False
        mtime = MODEL_PATH.stat().st_mtime
        if mtime <= self.last_mtime:
            return False
        artifact = joblib.load(MODEL_PATH)
        artifact_features = artifact.get("feature_names")
        if artifact_features != FEATURE_NAMES:
            logger.warning(
                "Ignoring model with incompatible features (%s); expected %s",
                artifact_features, FEATURE_NAMES,
            )
            return False
        with self.lock:
            self.model = artifact["model"]
            self.trained_at = artifact["trained_at"]
            self.n_samples = artifact["n_samples"]
            self.last_mtime = mtime
        logger.info("Loaded model from disk: trained_at=%s, n_samples=%d", self.trained_at, self.n_samples)
        return True

    def set_fitted(self, model: IsolationForest, n_samples: int) -> None:
        trained_at = datetime.now(timezone.utc).isoformat()
        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        joblib.dump(
            {"model": model, "trained_at": trained_at, "n_samples": n_samples, "feature_names": FEATURE_NAMES},
            MODEL_PATH,
        )
        with self.lock:
            self.model = model
            self.trained_at = trained_at
            self.n_samples = n_samples
            self.last_mtime = MODEL_PATH.stat().st_mtime

    def snapshot(self) -> Optional[IsolationForest]:
        with self.lock:
            return self.model


state = ModelState()
_producer: Optional[KafkaProducer] = None
_producer_lock = threading.Lock()


# --------------------------------------------------------------------------
# Feature extraction
# --------------------------------------------------------------------------
def extract_features(record: dict[str, Any]) -> Optional[list[float]]:
    """Use fields present in both validated-order history and live events.

    Isolation Forest splits on raw feature values rather than distances, so
    unlike k-NN/clustering methods it doesn't need the features scaled —
    part of why Paper 1 picked it over distance-based alternatives.
    """
    try:
        order_total = float(record["order_total"])
    except (KeyError, TypeError, ValueError):
        return None

    hour = 0.0
    timestamp = next((record.get(k) for k in ("created_at", "order_timestamp", "ingested_at") if record.get(k)), None)
    if timestamp:
        try:
            hour = float(datetime.fromisoformat(str(timestamp).replace("Z", "+00:00")).hour)
        except ValueError:
            pass

    return [order_total, hour]


# --------------------------------------------------------------------------
# Kafka + Postgres plumbing
# --------------------------------------------------------------------------
def get_producer() -> KafkaProducer:
    global _producer
    with _producer_lock:
        if _producer is None:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
            )
        return _producer


def ensure_schema() -> None:
    with psycopg2.connect(POSTGRES_APP_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS order_features (
                id             BIGSERIAL PRIMARY KEY,
                features       JSONB NOT NULL,
                order_total    DOUBLE PRECISION,
                is_anomaly     BOOLEAN,
                anomaly_score  DOUBLE PRECISION,
                created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        conn.commit()


def persist_features(features: list[float], is_anomaly: Optional[bool], score: Optional[float]) -> None:
    try:
        with psycopg2.connect(POSTGRES_APP_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO order_features (features, order_total, is_anomaly, anomaly_score)
                VALUES (%s, %s, %s, %s)
                """,
                (json.dumps(features), features[0], is_anomaly, score),
            )
            conn.commit()
    except psycopg2.Error as exc:
        logger.error("Failed to persist features: %s", exc)


def log_pipeline_event(event_type: str, payload: dict[str, Any]) -> None:
    try:
        with psycopg2.connect(POSTGRES_APP_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline_events (event_type, source_topic, payload) VALUES (%s, %s, %s)",
                (event_type, SOURCE_TOPIC, json.dumps(payload)),
            )
            conn.commit()
    except psycopg2.Error as exc:
        logger.error("Failed to write pipeline_event (%s): %s", event_type, exc)


# --------------------------------------------------------------------------
# Training
# --------------------------------------------------------------------------
def fit_from_window(features_window: list[list[float]]) -> IsolationForest:
    model = IsolationForest(n_estimators=100, contamination="auto", random_state=42)
    model.fit(features_window)
    return model


def retrain_from_postgres() -> dict[str, Any]:
    """Refit on the most recent RETRAIN_WINDOW_SIZE feature rows. Called by /retrain
    (manual or Airflow-triggered) — this is the 'training as background process' half
    of the hot-swap pattern; the mtime-polling thread is the other half."""
    with psycopg2.connect(POSTGRES_APP_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT features FROM order_features "
            "WHERE jsonb_array_length(features) = %s "
            "ORDER BY created_at DESC LIMIT %s",
            (len(FEATURE_NAMES), RETRAIN_WINDOW_SIZE),
        )
        rows = cur.fetchall()
        if len(rows) >= MIN_BOOTSTRAP_SAMPLES:
            window = [row[0] for row in rows]
            source = "order_features"
        else:
            cur.execute(
                """
                SELECT order_total, ingested_at
                FROM validated_orders
                WHERE order_total IS NOT NULL
                ORDER BY ingested_at DESC
                LIMIT %s
                """,
                (RETRAIN_WINDOW_SIZE,),
            )
            order_rows = cur.fetchall()
            window = [
                features
                for order_total, ingested_at in order_rows
                if (
                    features := extract_features(
                        {"order_total": order_total, "created_at": ingested_at}
                    )
                )
                is not None
            ]
            source = "validated_orders"

    if len(window) < MIN_BOOTSTRAP_SAMPLES:
        return {
            "status": "skipped",
            "reason": f"only {len(window)} samples in {source}, need {MIN_BOOTSTRAP_SAMPLES}",
        }

    model = fit_from_window(window)
    state.set_fitted(model, len(window))
    log_pipeline_event(
        "model_retrain",
        {"n_samples": len(window), "source": source, "trained_at": state.trained_at},
    )
    logger.info("Retrained on %d samples from %s", len(window), source)
    return {
        "status": "retrained",
        "n_samples": len(window),
        "source": source,
        "trained_at": state.trained_at,
    }


# --------------------------------------------------------------------------
# Streaming inference loop
# --------------------------------------------------------------------------
def handle_record(record: dict[str, Any]) -> None:
    features = extract_features(record)
    if features is None:
        return  # not our concern here — the sidecar validator already gates malformed records

    model = state.snapshot()

    if model is None:
        # Cold start: buffer until we have enough to fit a baseline model.
        state.bootstrap_buffer.append(features)
        persist_features(features, is_anomaly=None, score=None)
        if len(state.bootstrap_buffer) >= MIN_BOOTSTRAP_SAMPLES:
            logger.info("Bootstrap buffer full (%d samples) — fitting initial model", len(state.bootstrap_buffer))
            initial_model = fit_from_window(state.bootstrap_buffer)
            state.set_fitted(initial_model, len(state.bootstrap_buffer))
            log_pipeline_event("model_bootstrap", {"n_samples": len(state.bootstrap_buffer)})
            state.bootstrap_buffer.clear()
        return

    prediction = model.predict([features])[0]     # -1 = anomaly, 1 = normal
    score = float(model.decision_function([features])[0])  # lower = more anomalous
    is_anomaly = bool(prediction == -1)  # numpy.bool_ -> Python bool; psycopg2 can't adapt the former

    persist_features(features, is_anomaly, score)

    if is_anomaly:
        envelope = {
            "event_type": "anomaly_detected",
            "report_id": str(uuid.uuid4()),
            "monitor_name": "isolation_forest_anomaly",
            "order_id": record.get("order_id"),
            "features": dict(zip(FEATURE_NAMES, features)),
            "anomaly_score": score,
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }
        get_producer().send(ANOMALY_TOPIC, value=envelope)
        log_pipeline_event("anomaly", envelope)
        logger.info("Anomaly flagged: order_id=%s score=%.4f", record.get("order_id"), score)


def consume_loop() -> None:
    consumer = KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: v,
        auto_offset_reset="earliest",
        group_id="ml-isolation-forest",
    )
    logger.info("Listening on '%s' for validated records", SOURCE_TOPIC)
    for message in consumer:
        try:
            record = json.loads(message.value.decode("utf-8"))
            handle_record(record)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue  # already validated upstream — this shouldn't happen, but never crash the loop
        except Exception as exc:  # noqa: BLE001
            logger.exception("Unhandled error scoring record: %s", exc)


def reload_watch_loop() -> None:
    while True:
        try:
            state.load_from_disk_if_changed()
        except Exception as exc:  # noqa: BLE001
            logger.error("Model reload check failed: %s", exc)
        time.sleep(RELOAD_POLL_SECONDS)


@app.on_event("startup")
def startup() -> None:
    global _consumer_thread
    ensure_schema()
    state.load_from_disk_if_changed()  # pick up a model from a previous run, if the volume has one
    if state.model is None:
        # The bootstrap buffer is in-memory and the consumer resumes from its committed
        # offset after a restart, so recover a baseline from persisted Postgres data first.
        try:
            logger.info("No model on disk - warm start from Postgres: %s", retrain_from_postgres())
        except Exception as exc:  # noqa: BLE001
            logger.warning("Warm start skipped (%s); falling back to live bootstrap", exc)
    _consumer_thread = threading.Thread(target=consume_loop, daemon=True, name="isolation-forest-consumer")
    _consumer_thread.start()
    threading.Thread(target=reload_watch_loop, daemon=True).start()


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "model_loaded": state.model is not None,
        "consumer_alive": bool(_consumer_thread and _consumer_thread.is_alive()),
        "bootstrap_buffer_size": len(state.bootstrap_buffer),
    }


@app.get("/model-info")
def model_info() -> dict[str, Any]:
    processed_records = anomaly_records = 0
    last_scored_at = latest_prediction = None
    try:
        with psycopg2.connect(POSTGRES_APP_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*), COUNT(*) FILTER (WHERE is_anomaly), MAX(created_at) "
                "FROM order_features WHERE is_anomaly IS NOT NULL"
            )
            processed_records, anomaly_records, last_scored_at = cur.fetchone()
            cur.execute(
                "SELECT order_total, anomaly_score, is_anomaly, created_at "
                "FROM order_features WHERE is_anomaly IS NOT NULL "
                "ORDER BY created_at DESC LIMIT 1"
            )
            latest_prediction = cur.fetchone()
    except psycopg2.Error:
        logger.exception("Could not read inference statistics")
    return {
        "trained_at": state.trained_at,
        "n_samples": state.n_samples,
        "feature_names": FEATURE_NAMES,
        "bootstrap_buffer_size": len(state.bootstrap_buffer),
        "processed_records": processed_records,
        "anomaly_records": anomaly_records,
        "last_scored_at": last_scored_at.isoformat() if last_scored_at else None,
        "latest_prediction": (
            {"order_total": latest_prediction[0], "anomaly_score": latest_prediction[1],
             "is_anomaly": latest_prediction[2], "scored_at": latest_prediction[3].isoformat()}
            if latest_prediction else None
        ),
    }


@app.post("/retrain")
def retrain() -> dict[str, Any]:
    return retrain_from_postgres()
