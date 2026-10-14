FROM python:3.11-slim

WORKDIR /app

COPY services/schema_healer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/schema_healer/ ./
COPY contracts/ ./contracts/
COPY shared/kafka_config.py ./kafka_config.py

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
