"""Datamonkey backend package.

FastAPI app lives in `datamonkey.backend.app`. WebSockets are used for live logs.
Optional Redis Pub/Sub (env: REDIS_HOST/PORT) can be enabled for cross-process
log fan-out. If not configured, an in-process event bus is used.
"""


