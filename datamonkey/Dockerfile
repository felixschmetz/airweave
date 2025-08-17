FROM python:3.12-slim as backend
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential curl git && rm -rf /var/lib/apt/lists/*
COPY datamonkey/requirements.txt /app/datamonkey/requirements.txt
RUN pip install --no-cache-dir -r /app/datamonkey/requirements.txt
COPY datamonkey /app/datamonkey

# Frontend build stage
FROM node:20-slim as frontend
WORKDIR /ui
COPY datamonkey/frontend/package.json datamonkey/frontend/package-lock.json* ./
RUN npm install --no-audit --no-fund
COPY datamonkey/frontend ./
RUN npm run build

# Final image
FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY --from=backend /usr/local/lib/python3.12 /usr/local/lib/python3.12
COPY --from=backend /usr/local/bin /usr/local/bin
COPY --from=backend /app/datamonkey /app/datamonkey
COPY --from=frontend /ui/dist /app/datamonkey/frontend_dist

EXPOSE 8081
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "uvicorn", "datamonkey.backend.app:app", "--host", "0.0.0.0", "--port", "8081"]


