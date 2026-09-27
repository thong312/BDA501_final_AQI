FROM python:3.11-slim
WORKDIR /app
COPY services/notifier/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/notifier/ services/notifier/
ENV PYTHONPATH=/app
CMD ["python", "-m", "services.notifier.main"]
