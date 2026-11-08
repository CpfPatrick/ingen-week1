"""Harness safety/parser regressions; mocks do not establish Docker acceptance."""
import json
from pathlib import Path
import subprocess

import pytest

from scripts.verify_compose import AcceptanceError, Harness


def harness(tmp_path: Path) -> Harness:
    return Harness(Path("/authorized/docker"), tmp_path / "evidence", "fleet-test-unique", "local")


def test_fixed_log_schema_rejects_arbitrary_fields_and_messages() -> None:
    common: dict[str, object] = {
        "timestamp": 123.5, "level": "INFO", "trace_id": "safe-trace",
        "service": "gateway", "event": "request_completed", "status": 200,
    }
    assert Harness.safe_log(json.dumps(common), "gateway") == common
    assert Harness.safe_log(json.dumps({**common, "password": "secret"}), "gateway") is None
    assert Harness.safe_log(json.dumps({**common, "status": True}), "gateway") is None
    assert Harness.safe_log(json.dumps({**common, "trace_id": "bad\ntrace"}), "gateway") is None
    worker = {key: value for key, value in common.items() if key not in {"event", "status"}}
    worker.update(service="inference-worker-stub", message="heartbeat assigned_partitions=6")
    assert Harness.safe_log(json.dumps(worker), "inference-worker") == worker
    assert Harness.safe_log(json.dumps({**worker, "message": "SECRET"}), "inference-worker") is None


def test_unsafe_log_bytes_are_not_written(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run = harness(tmp_path)
    run.ids["gateway"] = "owned-container"
    safe = json.dumps({"timestamp": 1, "level": "INFO", "trace_id": "test-trace",
                       "service": "gateway", "event": "framework_event"}).encode() + b"\n"

    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(["docker", "logs"], 0, safe, b"SECRET-PASSWORD\n")

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(AcceptanceError, match="gateway_logs_safe"):
        run.logs("gateway")
    evidence = b"".join(path.read_bytes() for path in run.output.rglob("*") if path.is_file())
    assert b"SECRET-PASSWORD" not in evidence
    assert safe in evidence
    assert b'"rejected_lines": 1' in evidence


def test_group_membership_requires_worker_ip_and_all_partitions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = harness(tmp_path)
    run.worker_identity = {"ip": "172.18.0.7"}
    table = "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\n"
    table += "".join(
        f"inference-stub-v1 fleet.dev.heartbeat.v1 {part} - 0 - "
        "member-id /172.18.0.7 kafka-python-2.2.11\n" for part in range(6)
    )

    def command(_args: list[str], timeout: float = 45) -> tuple[int, str]:
        assert "--offsets" in _args
        return 0, table

    monkeypatch.setattr(run, "command", command)
    assert run.group()[0]
    run.worker_identity = {"ip": "172.18.0.8"}
    assert not run.group()[0]
    run.worker_identity = {"ip": "172.18.0.7"}
    original = table
    table = original.replace("heartbeat.v1 5", "heartbeat.v1 4")
    assert not run.group()[0]
    table = original.replace("heartbeat.v1", "frames.v1")
    assert not run.group()[0]
    table = original.replace("member-id", "-", 1)
    assert not run.group()[0]
    table = original.replace("member-id", "second-member", 1)
    assert not run.group()[0]


def test_cleanup_refuses_wrong_project_owner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run = harness(tmp_path)
    run.creation_attempted = True
    commands: list[list[str]] = []

    def command(args: list[str], timeout: float = 45) -> tuple[int, str]:
        commands.append(args)
        return 0, json.dumps({"project": "someone-else", "service": "gateway"})

    monkeypatch.setattr(run, "resources", lambda _kind: ["foreign-id"])
    monkeypatch.setattr(run, "command", command)
    assert not run.cleanup()
    assert all("stop" not in args and "rm" not in args for args in commands)


def test_preexisting_resources_abort_before_up(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run = harness(tmp_path)
    commands: list[list[str]] = []

    def command(args: list[str], timeout: float = 45) -> tuple[int, str]:
        commands.append(args)
        if "context" in args:
            return 0, "unix:///var/run/docker.sock\n"
        if "ps" in args:
            return 0, "existing-container\n"
        return 0, "{}\n"

    monkeypatch.setattr(run, "command", command)
    with pytest.raises(AcceptanceError, match="no_preexisting_project_container"):
        run.preflight()
    assert not run.creation_attempted
    assert run.cleanup()
    assert all("up" not in args and "rm" not in args for args in commands)


def test_wait_records_http_duration_without_keyword_collision(tmp_path: Path) -> None:
    run = harness(tmp_path)
    result = run.wait("local_probe", lambda: (True, {"seconds": 0.25, "status": 200}))
    assert result["status"] == 200
    assert run.checks[-1]["status"] == "passed"


def test_child_path_finds_bundled_helpers_without_changing_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import os

    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    run = harness(tmp_path)
    assert run.env["PATH"] == "/authorized:/usr/bin:/bin"
    assert os.environ["PATH"] == "/usr/bin:/bin"


def test_source_snapshots_cover_code_build_inputs_and_detect_changes(tmp_path: Path) -> None:
    run = harness(tmp_path)
    run.root = tmp_path / "source"
    sources = {
        "gateway/src/main/java/App.java": "class App {}",
        "gateway/src/main/resources/application.yml": "server: {}",
        "gateway/build.gradle": "plugins {}",
        "deploy/Dockerfile.minio": "FROM debian:bookworm-slim",
        "inference-worker/worker.py": "print('fixed')",
        "gradle/wrapper/gradle-wrapper.jar": "wrapper-bytes",
        ".github/workflows/ci.yml": "name: CI",
        "uv.lock": "version = 1",
    }
    excluded = {
        "gateway/build/generated.class": "generated",
        "inference-worker/__pycache__/worker.pyc": "cache",
        "inference-worker/.env.foo": "SECRET",
        ".env": "SECRET",
    }
    for name, value in (sources | excluded).items():
        path = run.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)
    run.start_sources = run.source_snapshot("start")
    assert set(run.start_sources) == set(sources)
    (run.root / "gateway/build/generated.class").write_text("new generated content")
    run.finish_source_evidence()
    (run.root / "inference-worker/worker.py").write_text("print('changed')")
    with pytest.raises(AcceptanceError, match="source_inputs_unchanged_during_run"):
        run.finish_source_evidence()
    assert (run.output / "source-sha256-start.json").exists()
    assert (run.output / "source-sha256-end.json").exists()


def test_image_identity_preserves_actual_empty_repo_digests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = harness(tmp_path)
    identity: dict[str, object] = {
        "id": "sha256:" + "1" * 64, "repo_tags": ["fleet-test-unique-gateway:latest"],
        "repo_digests": [], "created": "2026-11-09T00:00:00Z", "os": "linux",
        "architecture": "arm64",
    }

    def command(args: list[str], timeout: float = 45) -> tuple[int, str]:
        assert args[1:3] == ["image", "inspect"]
        assert ".Config.Env" not in " ".join(args)
        return 0, json.dumps(identity)

    monkeypatch.setattr(run, "command", command)
    assert run.image_evidence({"image_id": identity["id"]})
    evidence = json.loads((run.output / "images.json").read_text())
    assert evidence[identity["id"]]["repo_digests"] == []


def test_dockerignore_probe_uses_separate_synthetic_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = harness(tmp_path)
    run.root = tmp_path / "source"
    run.root.mkdir()
    rules = b".env\n.env.*\n**/.env\n**/.env.*\n!.env.example\n"
    (run.root / ".dockerignore").write_bytes(rules)
    (run.root / ".env").write_text("DO-NOT-READ-OR-MODIFY")
    contexts: list[Path] = []
    image_id = "sha256:" + "2" * 64

    def command(args: list[str], timeout: float = 45) -> tuple[int, str]:
        if "build" in args:
            context = Path(args[-1])
            contexts.append(context)
            assert context != run.root
            assert (context / ".dockerignore").read_bytes() == rules
            assert (context / ".env").read_text() == "SYNTHETIC-EXCLUDED-PLACEHOLDER\n"
            assert (context / "nested/.env.example").exists()
            assert "--no-cache" in args
            assert f"com.docker.compose.project={run.project}" in args
            assert "test ! -e /context/nested/.env.example" in (context / "Dockerfile").read_text()
            return 0, "synthetic mocked build output"
        if args[-2] == "{{.Id}}":
            return 0, image_id + "\n"
        return 0, json.dumps({"id": image_id, "repo_digests": []})

    monkeypatch.setattr(run, "command", command)
    run.dockerignore_probe()
    assert contexts and not contexts[0].exists()
    assert (run.root / ".env").read_text() == "DO-NOT-READ-OR-MODIFY"
    assert all(check["status"] == "passed" for check in run.checks)
