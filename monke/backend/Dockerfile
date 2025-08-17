FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends build-essential curl git && rm -rf /var/lib/apt/lists/*

COPY ../requirements.txt /app/monke/requirements.txt
RUN pip install --no-cache-dir -r /app/monke/requirements.txt

COPY .. /app/monke

EXPOSE 8081
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "monke.backend.app:app", "--host", "0.0.0.0", "--port", "8081"]


