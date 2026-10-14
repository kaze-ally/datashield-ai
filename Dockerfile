FROM python:3.11-slim

WORKDIR /app
COPY frontend/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY frontend/datashield_bridge.py ./datashield_bridge.py
COPY frontend/pipeline-observatory.html ./dashboard.html
COPY scripts/render_init_db.py ./render_init_db.py
COPY postgres/init/ /app/db-init/

EXPOSE 10000
CMD ["python", "datashield_bridge.py"]
