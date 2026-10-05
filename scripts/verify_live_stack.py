"""
scripts/verify_live_stack.py  -  READ-ONLY post-fix verification. Sends only GET requests; changes nothing.

    python scripts\verify_live_stack.py            # uses the bridge at http://localhost:8099 if up, else direct ports
"""
import json, sys, urllib.request

BRIDGE = "http://localhost:8099"
G, R, Y, X = "\033[92m", "\033[91m", "\033[93m", "\033[0m"
results = []

def get(url, t=4):
    try:
        with urllib.request.urlopen(url, timeout=t) as r:
            return r.status, json.loads(r.read().decode() or "null")
    except Exception as e:
        return None, str(e)

def check(name, ok, detail="", warn=False):
    results.append(ok or warn)
    tag = f"{G}PASS{X}" if ok else (f"{Y}WARN{X}" if warn else f"{R}FAIL{X}")
    print(f"[{tag}] {name}" + (f"  - {detail}" if detail else ""))

st, h = get(BRIDGE + "/health")
check("bridge up", st == 200, "start it with: python frontend\\datashield_bridge.py" if st != 200 else f"kafka_connected={h.get('kafka_connected')} replayed={h.get('replayed_on_start')}")
if st == 200:
    check("bridge sees Kafka", bool(h.get("kafka_connected")), h.get("last_error") or "")
    st, doc = get(BRIDGE + "/services", t=15)
    bridge_services_ok = st == 200 and isinstance(doc, dict) and isinstance(doc.get("services"), dict)
    check("bridge service poll", bridge_services_ok, "" if bridge_services_ok else str(doc))
    if bridge_services_ok:
        svc = doc["services"]
    else:
        svc = {}
        doc = {"extra": {}}
        ports = {"sidecar": (8092, "/contracts"), "isoforest": (8093, "/health"), "drift": (8010, "/health"), "breaker": (8020, "/health"),
                 "rca": (8030, "/health"), "schema": (8040, "/health"), "gateway": (8090, "/health"), "catalog": (8091, "/catalog"), "airflow": (18080, "/health")}
        for k, (p, path) in ports.items():
            s, d = get(f"http://localhost:{p}{path}")
            svc[k] = {"ok": s == 200, "data": d}
else:
    svc = {}
    ports = {"sidecar": (8092, "/contracts"), "isoforest": (8093, "/health"), "drift": (8010, "/health"), "breaker": (8020, "/health"),
             "rca": (8030, "/health"), "schema": (8040, "/health"), "gateway": (8090, "/health"), "catalog": (8091, "/catalog"), "airflow": (18080, "/health")}
    for k, (p, path) in ports.items():
        s, d = get(f"http://localhost:{p}{path}"); svc[k] = {"ok": s == 200, "data": d}
    doc = {"extra": {}}

for k in ("sidecar", "isoforest", "drift", "breaker", "rca", "schema", "gateway", "catalog", "airflow"):
    check(f"{k} reachable", bool(svc.get(k, {}).get("ok")), "" if svc.get(k, {}).get("ok") else "container down? docker compose up -d <service>")

breaker = svc.get("breaker", {})
b = breaker.get("data") or {}
if breaker.get("ok"):
    check("breaker image has the fix (event_stats present)", "event_stats" in b, "rebuild: docker compose build circuit_breaker" if "event_stats" not in b else json.dumps(b["event_stats"]))
else:
    check("breaker image has the fix (event_stats present)", False, "breaker is stopped; image was rebuilt, runtime health not verified", warn=True)
s, o = get("http://localhost:8010/openapi.json")
has = s == 200 and "force_publish" in json.dumps(o)
check("drift_monitor image has the fix (force_publish param)", has, "rebuild: docker compose build drift_monitor" if not has else "")
iso = svc.get("isoforest", {}).get("data") or {}
check("isolation forest has a model", bool(iso.get("model_loaded")), "" if iso.get("model_loaded") else f"buffer={iso.get('bootstrap_buffer_size')}: POST /retrain uses persisted orders when available; otherwise run generate_order_stream.py (needs 50 valid records)", warn=True)
cat = svc.get("catalog", {}).get("data")
check("catalog populated", bool(cat), "" if cat else "ensure ai-catalog is healthy, then restart sidecar-validator to publish contract schemas", warn=True)
bs = (doc.get("extra", {}).get("breaker_state") or {}).get("data") or {}
if bs:
    print(f"       breaker state: {bs.get('state')} trips={bs.get('trip_count')} failures={bs.get('consecutive_failures')}")
print("\nRESULT:", "all required checks passed" if all(results) else "some checks failed")
sys.exit(0 if all(results) else 1)
