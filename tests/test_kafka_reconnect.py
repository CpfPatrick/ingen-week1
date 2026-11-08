"""Regression for the pinned client's no-auto-commit coordinator reconnect path.

Exercises the installed kafka-python heartbeat implementation with only transport
mocked. This fails on 2.2.11 and protects the upstream fix from PR #2695:
https://github.com/dpkp/kafka-python/pull/2695
Real broker recovery remains the responsibility of scripts/verify_compose.py.
"""
import threading
import time
from unittest.mock import Mock

from kafka.coordinator.base import BaseCoordinator, HeartbeatThread, MemberState


def test_known_disconnected_coordinator_reconnects_without_pending_requests() -> None:
    client = Mock()
    client._lock = threading.RLock()
    client.is_disconnected.return_value = True
    client.connection_delay.return_value = 0
    client.connected.return_value = False
    # No network thread, broker, pending request or auto-commit can trigger recovery.
    # The heartbeat loop itself must reconnect after the backoff has expired.
    coordinator = BaseCoordinator(client, group_id="offline-regression", retry_backoff_ms=0)
    coordinator.coordinator_id = "coordinator-1"
    coordinator.state = MemberState.STABLE
    coordinator.rejoin_needed = False
    coordinator.heartbeat.last_reset = time.time() - 1000
    coordinator.heartbeat.last_receive = time.time() - 1000
    coordinator.heartbeat.last_poll = time.time()
    heartbeat = HeartbeatThread(coordinator)
    heartbeat.enabled = True
    try:
        assert coordinator.heartbeat.session_timeout_expired()
        heartbeat._run_once()
        client.poll.assert_called_once_with(timeout_ms=0)
        client.maybe_connect.assert_called_once_with("coordinator-1")
    finally:
        heartbeat.closed = True
