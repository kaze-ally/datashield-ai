FROM python:3.11-slim

WORKDIR /app

COPY services/drift_monitor/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/drift_monitor/drift_detector/ ./drift_detector/
COPY services/drift_monitor/config/ ./config/
COPY services/drift_monitor/main.py ./main.py
COPY shared/kafka_config.py ./kafka_config.py

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
