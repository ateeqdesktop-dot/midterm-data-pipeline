FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src:/app/config
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config config
COPY src src
COPY data data
COPY reports reports
COPY README.md .
CMD ["python", "src/main.py", "--input", "data/sample_orders.csv", "--backend", "memory", "--reports", "reports/results.json"]
