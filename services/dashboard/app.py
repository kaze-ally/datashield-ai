"""DataShield AI live service-health and catalog dashboard."""
import os

import psycopg2
import requests
import streamlit as st
from kafka import KafkaAdminClient

POSTGRES_APP_DSN = os.environ.get("POSTGRES_APP_DSN", "")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
LLM_GATEWAY_URL = os.environ.get("LLM_GATEWAY_URL", "http://llm-gateway:8000")
AI_CATALOG_URL = os.environ.get("AI_CATALOG_URL", "http://ai-catalog:8000")
ML_ISOLATION_FOREST_URL = os.environ.get("ML_ISOLATION_FOREST_URL", "http://ml-isolation-forest:8000")

st.set_page_config(page_title="DataShield AI", layout="wide")
st.title("🛡️ DataShield AI — System Health")
st.caption("Live status for the DataShield services and AI-generated data catalog.")

col1, col2, col3, col4, col5 = st.columns(5)


def check_postgres() -> str:
    try:
        conn = psycopg2.connect(POSTGRES_APP_DSN, connect_timeout=3)
        conn.close()
        return "🟢 connected"
    except Exception as exc:  # noqa: BLE001
        return f"🔴 {exc}"


def check_kafka() -> str:
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, request_timeout_ms=3000)
        topics = admin.list_topics()
        admin.close()
        return f"🟢 {len(topics)} topics"
    except Exception as exc:  # noqa: BLE001
        return f"🔴 {exc}"


def check_http(url: str) -> str:
    try:
        r = requests.get(f"{url}/health", timeout=3)
        return "🟢 healthy" if r.ok else f"🟡 status {r.status_code}"
    except Exception as exc:  # noqa: BLE001
        return f"🔴 {exc}"


with col1:
    st.metric("Postgres", "")
    st.write(check_postgres())
with col2:
    st.metric("Kafka", "")
    st.write(check_kafka())
with col3:
    st.metric("LLM Gateway", "")
    st.write(check_http(LLM_GATEWAY_URL))
with col4:
    st.metric("AI Catalog", "")
    st.write(check_http(AI_CATALOG_URL))
with col5:
    st.metric("Isolation Forest", "")
    try:
        info = requests.get(f"{ML_ISOLATION_FOREST_URL}/model-info", timeout=3).json()
        if info.get("trained_at"):
            st.write(f"🟢 trained on {info['n_samples']} samples")
        else:
            st.write(f"🟡 bootstrapping ({info['bootstrap_buffer_size']}/50)")
    except Exception as exc:  # noqa: BLE001
        st.write(f"🔴 {exc}")

st.divider()
st.subheader("📚 AI-Generated Data Catalog (live)")
try:
    entries = requests.get(f"{AI_CATALOG_URL}/catalog", timeout=5).json()
    if entries:
        st.dataframe(entries, use_container_width=True)
    else:
        st.info("No catalog entries yet — publish a schema event to `schema-events` to populate the catalog.")
except Exception as exc:  # noqa: BLE001
    st.warning(f"AI Catalog not reachable yet: {exc}")
