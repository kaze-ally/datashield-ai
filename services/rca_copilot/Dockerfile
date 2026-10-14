FROM python:3.11-slim

WORKDIR /app

COPY services/rca_copilot/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/rca_copilot/rca/ ./rca/
COPY services/rca_copilot/config/ ./config/
COPY services/rca_copilot/main.py ./main.py
COPY shared/kafka_config.py ./kafka_config.py

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
