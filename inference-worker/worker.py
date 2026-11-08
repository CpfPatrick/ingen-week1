"""Subscribes only to the fixed development heartbeat topic; no frame processing."""
import json
import logging
import os
import signal
import time
import uuid
from types import FrameType
from typing import Any

from kafka import KafkaConsumer


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({
            "timestamp": record.created,
            "level": record.levelname,
            "service": "inference-worker-stub",
            "trace_id": getattr(record, "trace_id", "startup"),
            "message": record.getMessage(),
        })


def heartbeat(logger: logging.Logger, trace_id: str, assigned: int) -> None:
    logger.info("heartbeat assigned_partitions=%s", assigned, extra={"trace_id": trace_id})


def main() -> int:
    logger = logging.getLogger("fleet.worker")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.handlers[:] = [handler]
    # Do not leak unsanitized client diagnostics (server names/payloads) to stderr.
    client_logger = logging.getLogger("kafka")
    client_logger.handlers[:] = [logging.NullHandler()]
    client_logger.propagate = False
    trace_id = str(uuid.uuid4())
    context = {"trace_id": trace_id}
    logger.info("worker_starting", extra=context)
    # Fail closed BEFORE any client construction. Never include supplied values in errors.
    topic = os.getenv("KAFKA_TOPIC", "fleet.dev.heartbeat.v1")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").strip()
    if topic != "fleet.dev.heartbeat.v1" or not bootstrap:
        logger.error("worker_config_invalid", extra=context)
        return 2
    running = True

    def stop(_signum: int, _frame: FrameType | None) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    consumer: Any = None
    # Three bounded startup attempts; constructor API discovery has a 2-second bound.
    for attempt in range(3):
        if not running:
            return 0
        try:
            consumer = KafkaConsumer(
                topic, bootstrap_servers=bootstrap,
                group_id="inference-stub-v1", enable_auto_commit=False,
                auto_offset_reset="earliest", max_poll_records=10,
                api_version_auto_timeout_ms=2000,
            )
            break
        except Exception:
            logger.warning("worker_startup_failed attempt=%s", attempt + 1, extra=context)
            if attempt < 2:
                time.sleep(2)
    if consumer is None:
        logger.error("worker_startup_exhausted", extra=context)
        return 1
    last = 0.0
    status = 0
    try:
        while running:
            consumer.poll(timeout_ms=1000)
            if time.monotonic() - last >= 5:
                heartbeat(logger, trace_id, len(consumer.assignment()))
                last = time.monotonic()
    except Exception:
        logger.error("worker_runtime_failed", extra=context)
        status = 1
    finally:
        try:
            consumer.close(autocommit=False, timeout_ms=2000)
        except Exception:
            logger.error("worker_close_failed", extra=context)
            status = 1
        logger.info("worker_stopped", extra=context)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
