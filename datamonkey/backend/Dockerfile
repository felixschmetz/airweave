FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends build-essential curl git && rm -rf /var/lib/apt/lists/*

COPY ../requirements.txt /app/datamonkey/requirements.txt
RUN pip install --no-cache-dir -r /app/datamonkey/requirements.txt

COPY .. /app/datamonkey

EXPOSE 8081
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "datamonkey.backend.app:app", "--host", "0.0.0.0", "--port", "8081"]


