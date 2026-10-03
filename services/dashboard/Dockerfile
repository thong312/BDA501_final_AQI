FROM python:3.11-slim
WORKDIR /app
COPY services/dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/dashboard/ .
EXPOSE 5050
CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:5050", "app:app"]
