FROM python:3.10-slim

WORKDIR /app

COPY services/poller/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép model schema dùng chung
COPY common/ common/
COPY services/poller/ services/poller/

ENV PYTHONPATH=/app
CMD ["python", "services/poller/main.py"]
