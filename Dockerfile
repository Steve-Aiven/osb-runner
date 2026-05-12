FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir "opensearch-benchmark>=2.1.0"

COPY run.py .

EXPOSE 8080

CMD ["python3", "run.py"]
