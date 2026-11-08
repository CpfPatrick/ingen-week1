import json
import logging
import signal
import uuid
from collections.abc import Callable, Iterator
from types import FrameType
from unittest.mock import Mock

import pytest
import worker

from worker import JsonFormatter, heartbeat


@pytest.fixture(autouse=True)
def isolated_worker_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.delenv("KAFKA_TOPIC", raising=False)
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.setattr("worker.signal.signal", Mock())
    loggers = [logging.getLogger("fleet.worker"), logging.getLogger("kafka")]
    states = [(logger.handlers[:], logger.level, logger.propagate) for logger in loggers]
    yield
    for logger, (handlers, level, propagate) in zip(loggers, states, strict=True):
        logger.handlers[:] = handlers
        logger.setLevel(level)
        logger.propagate = propagate


def safe_events(output: str) -> list[dict[str, object]]:
    assert "SYNTHETIC_SECRET_DO_NOT_LOG" not in output
    assert "Traceback" not in output
    events: list[dict[str, object]] = [json.loads(line) for line in output.splitlines()]
    assert events
    traces = {str(event["trace_id"]) for event in events}
    assert len(traces) == 1
    assert str(uuid.UUID(traces.pop())) == str(events[0]["trace_id"])
    assert all(event["service"] == "inference-worker-stub" for event in events)
    return events


def test_json_log_has_trace_and_escapes() -> None:
    record = logging.LogRecord("test", logging.INFO, "", 0, "hello\nworld", (), None)
    record.trace_id = "test-123"
    payload = json.loads(JsonFormatter().format(record))
    assert payload["trace_id"] == "test-123"
    assert payload["message"] == "hello\nworld"


def test_heartbeat(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO):
        heartbeat(logging.getLogger("test"), "trace-1", 6)
    assert getattr(caplog.records[0], "trace_id") == "trace-1"
    assert "assigned_partitions=6" in caplog.text


@pytest.mark.parametrize("topic", [
    "", " ", "fleet.frames.raw.v1", "other", "fleet.dev.heartbeat.v1 ",
    "FLEET.DEV.HEARTBEAT.V1", "fleet.dev.heartbeat.v1\n", "fleet.dev.heartbeat.v1\r",
    "fleet.dev.heartbeat.v1\u200b", "fleet.dev.heartbeat.v１", "fleet.dev.*",
    "fleet.dev.heartbeat.v1,fleet.frames.raw.v1", "fleet.dev.heartbeat.v1\x1b[0m",
    "fleet.dev.heartbeat.v1\x00", "SYNTHETIC_SECRET_DO_NOT_LOG",
])
def test_forbidden_topic_never_constructs_or_polls(
    monkeypatch: pytest.MonkeyPatch, topic: str, capsys: pytest.CaptureFixture[str],
) -> None:
    constructor = Mock()
    monkeypatch.setattr(worker, "KafkaConsumer", constructor)
    # getenv can be injected even for NUL, which OS environment APIs cannot store.
    monkeypatch.setattr("worker.os.getenv", lambda key, default=None:
                        topic if key == "KAFKA_TOPIC" else default)
    assert worker.main() == 2
    constructor.assert_not_called()
    constructor.return_value.poll.assert_not_called()
    assert safe_events(capsys.readouterr().err)[-1]["message"] == "worker_config_invalid"


@pytest.mark.parametrize("bootstrap", ["", "   "])
def test_empty_bootstrap_never_constructs(
    monkeypatch: pytest.MonkeyPatch, bootstrap: str,
) -> None:
    constructor = Mock()
    monkeypatch.setattr(worker, "KafkaConsumer", constructor)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", bootstrap)
    assert worker.main() == 2
    constructor.assert_not_called()
    constructor.return_value.poll.assert_not_called()


def test_startup_failure_is_bounded_sanitized_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    constructor = Mock(side_effect=RuntimeError("secret-server-token"))
    monkeypatch.setattr(worker, "KafkaConsumer", constructor)
    monkeypatch.setattr("worker.signal.signal", Mock())
    sleep = Mock()
    monkeypatch.setattr("worker.time.sleep", sleep)
    assert worker.main() == 1
    assert constructor.call_count == 3
    assert sleep.call_count == 2
    output = capsys.readouterr().err
    assert "secret-server-token" not in output
    assert "Traceback" not in output
    events = [json.loads(line) for line in output.splitlines()]
    assert events[0]["message"] == "worker_starting"
    assert events[-1]["message"] == "worker_startup_exhausted"
    assert len({event["trace_id"] for event in events}) == 1


def test_approved_topic_constructs_and_closes_without_commit(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    consumer = Mock()
    consumer.poll.side_effect = RuntimeError("private payload")
    constructor = Mock(return_value=consumer)
    monkeypatch.setattr(worker, "KafkaConsumer", constructor)
    monkeypatch.setattr("worker.signal.signal", Mock())
    monkeypatch.setenv("KAFKA_TOPIC", "fleet.dev.heartbeat.v1")
    assert worker.main() == 1
    assert constructor.call_args.args == ("fleet.dev.heartbeat.v1",)
    assert constructor.call_args.kwargs["enable_auto_commit"] is False
    consumer.poll.assert_called_once_with(timeout_ms=1000)
    consumer.close.assert_called_once_with(autocommit=False, timeout_ms=2000)
    consumer.commit.assert_not_called()
    assert "private payload" not in capsys.readouterr().err


@pytest.mark.parametrize("failure", ["poll", "assignment", "close", "poll_and_close"])
def test_runtime_and_close_errors_are_safe_and_correlated(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], failure: str,
) -> None:
    callbacks: dict[int, Callable[[int, FrameType | None], None]] = {}
    monkeypatch.setattr("worker.signal.signal", lambda number, callback:
                        callbacks.__setitem__(number, callback))
    consumer = Mock()
    consumer.assignment.return_value = {0, 1, 2, 3, 4, 5}
    marker = "SYNTHETIC_SECRET_DO_NOT_LOG"
    if failure in {"poll", "poll_and_close"}:
        consumer.poll.side_effect = RuntimeError(marker)
    elif failure == "assignment":
        consumer.assignment.side_effect = RuntimeError(marker)
    else:
        consumer.poll.side_effect = lambda **kwargs: callbacks[signal.SIGTERM](signal.SIGTERM, None)
    if "close" in failure:
        consumer.close.side_effect = RuntimeError(marker)
    monkeypatch.setattr(worker, "KafkaConsumer", Mock(return_value=consumer))
    monkeypatch.setattr("worker.time.monotonic", lambda: 10.0)
    assert worker.main() == 1
    events = safe_events(capsys.readouterr().err)
    messages = [event["message"] for event in events]
    assert ("worker_runtime_failed" in messages) == (failure != "close")
    assert ("worker_close_failed" in messages) == ("close" in failure)
    assert messages[-1] == "worker_stopped"
    consumer.close.assert_called_once_with(autocommit=False, timeout_ms=2000)
    consumer.commit.assert_not_called()


def test_retry_recovery_shutdown_and_client_diagnostics(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    callbacks: dict[int, Callable[[int, FrameType | None], None]] = {}
    monkeypatch.setattr("worker.signal.signal", lambda number, callback:
                        callbacks.__setitem__(number, callback))
    consumer = Mock()
    consumer.assignment.return_value = set(range(6))
    consumer.poll.side_effect = lambda **kwargs: callbacks[signal.SIGTERM](signal.SIGTERM, None)
    attempts = 0

    def connect(*args: object, **kwargs: object) -> Mock:
        nonlocal attempts
        attempts += 1
        logging.getLogger("kafka.client").error("SYNTHETIC_SECRET_DO_NOT_LOG")
        if attempts == 1:
            raise RuntimeError("SYNTHETIC_SECRET_DO_NOT_LOG")
        return consumer

    monkeypatch.setattr(worker, "KafkaConsumer", connect)
    monkeypatch.setattr("worker.time.sleep", Mock())
    monkeypatch.setattr("worker.time.monotonic", lambda: 10.0)
    assert worker.main() == 0
    assert attempts == 2
    events = safe_events(capsys.readouterr().err)
    assert [event["message"] for event in events] == [
        "worker_starting", "worker_startup_failed attempt=1",
        "heartbeat assigned_partitions=6", "worker_stopped",
    ]
    consumer.close.assert_called_once_with(autocommit=False, timeout_ms=2000)
    consumer.commit.assert_not_called()


def test_shutdown_during_retry_prevents_another_client(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    callbacks: dict[int, Callable[[int, FrameType | None], None]] = {}
    monkeypatch.setattr("worker.signal.signal", lambda number, callback:
                        callbacks.__setitem__(number, callback))
    constructor = Mock(side_effect=RuntimeError("SYNTHETIC_SECRET_DO_NOT_LOG"))
    monkeypatch.setattr(worker, "KafkaConsumer", constructor)
    monkeypatch.setattr("worker.time.sleep", lambda seconds:
                        callbacks[signal.SIGTERM](signal.SIGTERM, None))
    assert worker.main() == 0
    constructor.assert_called_once()
    safe_events(capsys.readouterr().err)
