FROM python:3.11-slim

WORKDIR /app

COPY services/ml_isolation_forest/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/ml_isolation_forest/main.py ./main.py
COPY shared/kafka_config.py ./kafka_config.py

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
