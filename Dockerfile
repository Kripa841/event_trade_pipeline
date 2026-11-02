FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Point to reliable official PyPI and mirrors with retries
COPY requirements.txt .
RUN pip install --no-cache-dir \
    -i https://pypi.org/simple \
    --default-timeout=120 \
    --retries 5 \
    -r requirements.txt

# Copy application source code
COPY app/ ./app/
COPY tests/ ./tests/

CMD ["python", "-m", "app.consumer"]
