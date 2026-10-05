"""
DataShield AI — LLM Gateway (Innovation #2, now 100% free)
============================================================
Every LLM call in DataShield (RCA Copilot, Schema Healer, AI Catalog
enrichment) is routed through this single gateway instead of calling a
model provider directly. That buys us two FrugalGPT-style cost controls
(Chen, Zaharia & Zou, 2023 — "FrugalGPT"), and running the models
themselves through Ollama instead of a paid API means the whole cascade
costs $0 in tokens, only compute:

  1. Semantic cache: embed the incoming prompt (sentence-transformers,
     local CPU inference, no API cost) and look for a cosine-similar prompt
     already answered, stored in Postgres/pgvector. A hit skips inference
     entirely.
  2. Model cascade: on a cache miss, try the small model first
     (default `llama3.2:1b`). If its own reply looks confident, return it
     immediately. Otherwise escalate to the larger model
     (default `llama3.2:3b`) — still free, just slower and more capable.

Both models run inside the `ollama` container on the same Docker network;
this service never talks to an external API. Swapping back to a paid
provider later (e.g. for higher quality) is a small, isolated change to
`_call_model` — nothing else in the gateway's cache/cascade logic depends
on which backend actually generates the text.
"""
from __future__ import annotations

import os
from typing import Optional

import ollama
import psycopg2
from fastapi import FastAPI
from pgvector.psycopg2 import register_vector
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://ollama:11434")
CHEAP_MODEL = os.environ.get("CHEAP_MODEL", "phi4-mini")
STRONG_MODEL = os.environ.get("STRONG_MODEL", "llama3.1:8b")
POSTGRES_APP_DSN = os.environ.get("POSTGRES_APP_DSN", "")
CACHE_THRESHOLD = float(os.environ.get("SEMANTIC_CACHE_THRESHOLD", "0.94"))

app = FastAPI(title="DataShield LLM Gateway")
_embedder: Optional[SentenceTransformer] = None
_ollama_client: Optional[ollama.Client] = None


def get_embedder() -> SentenceTransformer:
    global _embedder
    if _embedder is None:
        _embedder = SentenceTransformer("all-MiniLM-L6-v2")
    return _embedder


def get_ollama_client() -> ollama.Client:
    global _ollama_client
    if _ollama_client is None:
        _ollama_client = ollama.Client(host=OLLAMA_HOST)
    return _ollama_client


def get_db_conn():
    conn = psycopg2.connect(POSTGRES_APP_DSN)
    register_vector(conn)  # without this, psycopg2 has no idea how to send a Python list as a `vector`
    return conn


class InvokeRequest(BaseModel):
    prompt: str
    system: Optional[str] = None
    max_tokens: int = 512
    agent: str = "unknown"  # e.g. "rca_copilot", "schema_healer", "ai_catalog"


class InvokeResponse(BaseModel):
    response: str
    model_used: str
    cache_hit: bool


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/invoke", response_model=InvokeResponse)
def invoke(req: InvokeRequest) -> InvokeResponse:
    embedding = get_embedder().encode(req.prompt).tolist()

    # 1. Semantic cache lookup -------------------------------------------------
    with get_db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, response_text, model_used,
                   1 - (embedding <=> %s::vector) AS similarity
            FROM llm_semantic_cache
            ORDER BY embedding <=> %s::vector
            LIMIT 1
            """,
            (embedding, embedding),
        )
        row = cur.fetchone()
        if row and row[3] >= CACHE_THRESHOLD:
            cache_id, response_text, model_used, _similarity = row
            cur.execute(
                """
                UPDATE llm_semantic_cache
                SET hit_count = hit_count + 1, last_hit_at = now()
                WHERE id = %s
                """,
                (cache_id,),
            )
            conn.commit()
            return InvokeResponse(response=response_text, model_used=model_used, cache_hit=True)

    # 2. Cascade: cheap model first, escalate only if it flags low confidence -
    client = get_ollama_client()
    cheap_reply = _call_model(client, CHEAP_MODEL, req)
    if _is_confident(cheap_reply):
        final_text, model_used = cheap_reply, CHEAP_MODEL
    else:
        strong_reply = _call_model(client, STRONG_MODEL, req)
        final_text, model_used = strong_reply, STRONG_MODEL

    # 3. Write-through cache ---------------------------------------------------
    with get_db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO llm_semantic_cache (prompt_text, embedding, response_text, model_used)
            VALUES (%s, %s::vector, %s, %s)
            """,
            (req.prompt, embedding, final_text, model_used),
        )
        conn.commit()

    return InvokeResponse(response=final_text, model_used=model_used, cache_hit=False)


def _call_model(client: ollama.Client, model: str, req: InvokeRequest) -> str:
    messages = []
    if req.system:
        messages.append({"role": "system", "content": req.system})
    messages.append({"role": "user", "content": req.prompt})
    result = client.chat(
        model=model,
        messages=messages,
        options={"num_predict": req.max_tokens},
    )
    return result["message"]["content"]


def _is_confident(reply_text: str) -> bool:
    """
    Cheap heuristic confidence gate for the cascade's first hop. Week 2+:
    replace with a real self-scoring prompt (ask the cheap model to also
    emit a confidence score) per the FrugalGPT "LLM cascade" pattern.
    """
    hedging_markers = ("i'm not sure", "i am not sure", "unclear", "cannot determine", "insufficient information")
    lowered = reply_text.lower()
    return not any(marker in lowered for marker in hedging_markers) and len(reply_text) > 0

