"""Run real Week-1 Compose acceptance; never infer runtime success from static files.

Example (requires an already authorized local Docker installation/daemon):
  python3 scripts/verify_compose.py --output /tmp/fleet-evidence-unique \
      --project fleet-accept-unique --docker /absolute/path/to/docker

An optional --context selects a configured local Unix-socket context explicitly.
The exact repository Compose file and .env.example are used. Named volumes and
built images are retained and recorded; only this run's containers/network are removed.
"""
from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import cast
from types import FrameType
import uuid

SERVICES = {"kafka", "topics", "registry", "postgres", "minio", "gateway", "inference-worker"}
GROUP = "inference-stub-v1"
TOPIC = "fleet.dev.heartbeat.v1"
TRACE = re.compile(r"[a-zA-Z0-9_-]{1,64}\Z")
EXAMPLE_ENV = {
    "POSTGRES_PASSWORD": "replace-local-postgres",
    "MINIO_ROOT_USER": "localadmin",
    "MINIO_ROOT_PASSWORD": "replace-local-minio",
}
LEVELS = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "WARNING", "CRITICAL"}
SOURCE_DIRECTORIES = {
    ".github", "config", "deploy", "gateway", "gradle", "inference-worker",
    "privacy-worker", "schemas", "scripts", "stream-processor", "tests",
}
GENERATED_DIRECTORIES = {
    ".git", ".gradle", ".venv", "build", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache",
}
ROOT_BUILD_FILES = {
    ".dockerignore", ".env.example", ".gitignore", ".python-version", "build.gradle",
    "docker-compose.yml", "gradlew", "gradlew.bat", "pyproject.toml", "settings.gradle", "uv.lock",
}
IMAGE_FORMAT = (
    '{"id":{{json .Id}},"repo_tags":{{json .RepoTags}},"repo_digests":{{json .RepoDigests}},'
    '"created":{{json .Created}},"os":{{json .Os}},"architecture":{{json .Architecture}}}'
)
STATE_FORMAT = (
    '{"id":{{json .Id}},"name":{{json .Name}},"image_id":{{json .Image}},'
    '"image_reference":{{json .Config.Image}},"status":{{json .State.Status}},'
    '"health":{{with index .State "Health"}}{{json .Status}}{{else}}"none"{{end}},'
    '"exit_code":{{.State.ExitCode}},"pid":{{.State.Pid}},'
    '"started_at":{{json .State.StartedAt}},"restart_count":{{.RestartCount}},'
    '"project":{{json (index .Config.Labels "com.docker.compose.project")}},'
    '"service":{{json (index .Config.Labels "com.docker.compose.service")}},'
    '"ip":"{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"}'
)
Probe = Callable[[], tuple[bool, dict[str, object]]]


def utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def object_json(value: str) -> dict[str, object]:
    parsed: object = json.loads(value)
    if not isinstance(parsed, dict) or not all(isinstance(key, str) for key in parsed):
        raise ValueError("Expected a JSON object")
    return cast(dict[str, object], parsed)


def nested(value: dict[str, object], *keys: str) -> object:
    result: object = value
    for key in keys:
        if not isinstance(result, dict):
            return None
        result = result.get(key)
    return result


class AcceptanceError(RuntimeError):
    """A recorded acceptance assertion failed."""


class Harness:
    def __init__(self, docker: Path, output: Path, project: str, context: str | None) -> None:
        self.root = Path(__file__).resolve().parents[1]
        self.output = output
        self.project = project
        self.docker = [str(docker)]
        self.context = context
        self.env = dict(os.environ)
        # Docker Desktop's credential helper is beside its CLI. Change child PATH only.
        self.env["PATH"] = str(docker.parent) + os.pathsep + self.env.get("PATH", os.defpath)
        # Shell variables override --env-file in Compose, so remove all interpolation keys.
        for key in list(self.env):
            if key.startswith("COMPOSE_") or key in {
                "POSTGRES_PASSWORD", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD",
                "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH",
            }:
                del self.env[key]
        self.env["COMPOSE_ANSI"] = "never"
        self.env["COMPOSE_PROGRESS"] = "plain"
        self.sequence = 0
        self.creation_attempted = False
        self.compose_up_attempted = False
        self.ids: dict[str, str] = {}
        self.request_logs: dict[str, int] = {}
        self.worker_identity: dict[str, object] = {}
        self.worker_trace = ""
        self.checks: list[dict[str, object]] = []
        self.start_sources: dict[str, str] | None = None
        self.images: dict[str, dict[str, object]] = {}
        output.mkdir(parents=True, exist_ok=False)
        (output / "commands").mkdir()
        (output / "http").mkdir()
        (output / "logs").mkdir()

    def record(self, kind: str, **fields: object) -> None:
        with (self.output / "events.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"time": utc(), "kind": kind, **fields}, sort_keys=True) + "\n")

    def check(self, name: str, passed: bool, **details: object) -> None:
        entry: dict[str, object] = {
            "time": utc(), "name": name, "status": "passed" if passed else "failed", "details": details,
        }
        self.checks.append(entry)
        self.record("assertion", **entry)
        print(f"[{entry['status']}] {name}", flush=True)
        if not passed:
            raise AcceptanceError(name)

    def command(self, args: list[str], timeout: float = 45) -> tuple[int, str]:
        self.sequence += 1
        stem = f"{self.sequence:04d}"
        stdout_path = self.output / "commands" / f"{stem}.stdout"
        stderr_path = self.output / "commands" / f"{stem}.stderr"
        started = time.monotonic()
        self.record("command_started", argv=args, stdout=str(stdout_path), stderr=str(stderr_path))
        print(f"[running] {' '.join(args)}", flush=True)
        code = -1
        timed_out = False
        with stdout_path.open("wb") as out, stderr_path.open("wb") as err:
            process = subprocess.Popen(
                args, cwd=self.root, env=self.env, stdout=out, stderr=err, start_new_session=True,
            )
            try:
                while process.poll() is None:
                    remaining = timeout - (time.monotonic() - started)
                    if remaining <= 0:
                        timed_out = True
                        break
                    try:
                        process.wait(timeout=min(15, remaining))
                    except subprocess.TimeoutExpired:
                        print(f"[running] command {stem}: {time.monotonic() - started:.0f}s", flush=True)
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=5)
                code = process.returncode
                self.record(
                    "command_finished", argv=args, returncode=code, timed_out=timed_out,
                    seconds=round(time.monotonic() - started, 3),
                    stdout=str(stdout_path), stderr=str(stderr_path),
                )
        return code if not timed_out else -1, stdout_path.read_text(encoding="utf-8", errors="replace")

    def required_command(self, name: str, args: list[str], timeout: float = 45) -> str:
        code, output = self.command(args, timeout)
        self.check(name, code == 0, returncode=code)
        return output

    def compose(self, *args: str) -> list[str]:
        return [
            *self.docker, "compose", "--project-name", self.project,
            "--project-directory", str(self.root), "--env-file", str(self.root / ".env.example"),
            "--file", str(self.root / "docker-compose.yml"), *args,
        ]

    def resources(self, kind: str) -> list[str]:
        args = ["ps", "--all", "--quiet"] if kind == "container" else [kind, "ls", "--quiet"]
        output = self.required_command(
            f"list_{kind}_resources", [*self.docker, *args, "--filter",
                                     f"label=com.docker.compose.project={self.project}"],
        )
        return output.split()

    def preflight(self) -> None:
        example = dict(line.split("=", 1) for line in
                       (self.root / ".env.example").read_text().splitlines()
                       if line.strip() and not line.lstrip().startswith("#"))
        self.check("example_env_contains_only_synthetic_defaults", example == EXAMPLE_ENV)
        if self.context is None:
            self.context = self.required_command(
                "resolve_selected_context", [*self.docker, "context", "show"],
            ).strip()
        self.check("context_name_present", bool(self.context))
        endpoint = self.required_command(
            "inspect_context_endpoint", [*self.docker, "context", "inspect", self.context,
                                         "--format", '{{(index .Endpoints "docker").Host}}'],
        ).strip()
        known_sockets = {
            "/var/run/docker.sock", str(Path.home() / ".docker/run/docker.sock"),
            str(Path.home() / ".docker/desktop/docker.sock"),
        }
        self.check("selected_context_is_known_local", endpoint.startswith("unix://")
                   and endpoint.removeprefix("unix://") in known_sockets, endpoint=endpoint)
        self.docker.extend(["--context", self.context])
        self.required_command("daemon_reachable", [*self.docker, "info", "--format",
                              '{"version":{{json .ServerVersion}},"os":{{json .OSType}},'
                              '"architecture":{{json .Architecture}}}'])
        self.required_command("compose_available", [*self.docker, "compose", "version"])
        for kind in ("container", "network", "volume"):
            self.check(f"no_preexisting_project_{kind}", not self.resources(kind))
        for kind, listing in (("container", ["ps", "--all", "--quiet"]),
                              ("network", ["network", "ls", "--quiet"]),
                              ("volume", ["volume", "ls", "--quiet"])):
            prefix = "^/" if kind == "container" else "^"
            existing = self.required_command(
                f"list_{kind}_name_collisions", [*self.docker, *listing, "--filter",
                                                f"name={prefix}{self.project}[-_]"],
            )
            self.check(f"no_preexisting_project_named_{kind}", not existing.strip())
        images = self.required_command(
            "list_project_image_names", [*self.docker, "image", "ls", "--quiet", "--filter",
                                         f"reference={self.project}-*"],
        )
        self.check("no_preexisting_project_images", not images.strip())
        services = self.required_command("read_exact_compose_services", self.compose("config", "--services"))
        self.check("exact_seven_services", set(services.split()) == SERVICES)
        for port in (9092, 8081, 5432, 9000, 9001, 8080):
            available = True
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                try:
                    probe.bind(("127.0.0.1", port))
                except OSError:
                    available = False
            self.check(f"host_port_{port}_available", available)
        self.record("configuration", project=self.project, context=self.context,
                    env_file=".env.example", source_manifest="source-sha256-start.json",
                    docker_helper_directory=str(Path(self.docker[0]).parent), synthetic_only=True)

    def source_snapshot(self, phase: str) -> dict[str, str]:
        """Hash source/build inputs, excluding generated output and private .env files."""
        paths = [self.root / name for name in ROOT_BUILD_FILES if (self.root / name).is_file()]
        for source in sorted(SOURCE_DIRECTORIES):
            for current, directories, files in os.walk(self.root / source, followlinks=False):
                directory = Path(current)
                directories[:] = sorted(name for name in directories
                    if name not in GENERATED_DIRECTORIES
                    and not (directory / name).is_symlink()
                    and not (directory / name).resolve().is_relative_to(self.output))
                for name in sorted(files):
                    path = directory / name
                    if (name == ".env" or name.startswith(".env.")) and name != ".env.example":
                        continue
                    if path.resolve().is_relative_to(self.output):
                        continue
                    paths.append(path)
        manifest: dict[str, str] = {}
        for path in sorted(paths):
            # Preserve a link's actual source identity without reading an external target.
            data = (b"symlink:" + os.readlink(path).encode()) if path.is_symlink() else path.read_bytes()
            manifest[path.relative_to(self.root).as_posix()] = hashlib.sha256(data).hexdigest()
        target = self.output / f"source-sha256-{phase}.json"
        target.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        self.record("source_snapshot", phase=phase, manifest=str(target), files=len(manifest),
                    source_directories=sorted(SOURCE_DIRECTORIES),
                    root_build_files=sorted(ROOT_BUILD_FILES),
                    generated_directories_excluded=sorted(GENERATED_DIRECTORIES))
        return manifest

    def finish_source_evidence(self) -> None:
        end_sources = self.source_snapshot("end")
        if self.start_sources is not None:
            changed = sorted(name for name in self.start_sources.keys() | end_sources.keys()
                             if self.start_sources.get(name) != end_sources.get(name))
            self.check("source_inputs_unchanged_during_run", not changed, changed_paths=changed)

    def image_evidence(self, state: dict[str, object]) -> bool:
        image_id = state.get("image_id")
        if not isinstance(image_id, str) or not image_id.startswith("sha256:"):
            self.record("image_identity", status="failed", reason="missing_container_image_id")
            return False
        if image_id in self.images:
            return True
        code, output = self.command([*self.docker, "image", "inspect", "--format", IMAGE_FORMAT, image_id])
        try:
            identity = object_json(output) if code == 0 else {}
        except ValueError:
            identity = {}
        passed = identity.get("id") == image_id
        self.record("image_identity", image_id=image_id, status="passed" if passed else "failed")
        if passed:
            # Local builds commonly have no repo digest; retain []/null without inventing one.
            self.images[image_id] = identity
            (self.output / "images.json").write_text(json.dumps(self.images, indent=2) + "\n")
        return passed

    def wait(self, name: str, probe: Probe, timeout: float = 180) -> dict[str, object]:
        started = time.monotonic()
        attempts = 0
        while True:
            attempts += 1
            passed, details = probe()
            elapsed = time.monotonic() - started
            self.record("observation", name=name, attempt=attempts,
                        status="passed" if passed else "retry", elapsed_seconds=round(elapsed, 3),
                        observation=details)
            if passed or elapsed >= timeout:
                self.check(name, passed, attempts=attempts, elapsed_seconds=round(elapsed, 3),
                           observation=details)
                return details
            time.sleep(min(3, timeout - elapsed))

    def state(self, service: str) -> dict[str, object]:
        code, output = self.command([*self.docker, "inspect", "--format", STATE_FORMAT, self.ids[service]])
        if code != 0:
            return {"inspection_failed": True}
        return object_json(output)

    def discover(self) -> None:
        identities: list[dict[str, object]] = []
        for container_id in self.resources("container"):
            output = self.required_command(
                "inspect_created_container", [*self.docker, "inspect", "--format", STATE_FORMAT, container_id],
            )
            state = object_json(output)
            service = state.get("service")
            self.check("created_container_owned_by_project", state.get("project") == self.project
                       and isinstance(service, str) and service in SERVICES, container_id=container_id)
            assert isinstance(service, str)
            self.check(f"single_container_{service}", service not in self.ids)
            self.ids[service] = container_id
            identities.append(state)
            (self.output / "containers-created.json").write_text(json.dumps(identities, indent=2) + "\n")
            self.check(f"{service}_image_identity_recorded", self.image_evidence(state))
        self.check("all_seven_containers_created", set(self.ids) == SERVICES)

    def wait_state(self, service: str, *, health: str | None = None, exited: bool = False) -> None:
        def probe() -> tuple[bool, dict[str, object]]:
            state = self.state(service)
            passed = state.get("status") == ("exited" if exited else "running")
            if exited:
                passed = passed and state.get("exit_code") == 0
            if health:
                passed = passed and state.get("health") == health
            return passed, {"service": service, "state": state}
        self.wait(f"{service}_{health or ('completed_exit0' if exited else 'running')}", probe)

    def http(self, port: int, path: str, expected: int, *, gateway: bool = False,
             dependencies: tuple[bool, bool] | None = None) -> tuple[bool, dict[str, object]]:
        self.sequence += 1
        stem = self.output / "http" / f"{self.sequence:04d}"
        trace = f"accept-{uuid.uuid4().hex}"
        details: dict[str, object] = {"port": port, "path": path, "expected_status": expected}
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=12)
        started = time.monotonic()
        try:
            connection.request("GET", path, headers={"X-Trace-Id": trace} if gateway else {})
            response = connection.getresponse()
            body = response.read(1_048_577)
            headers = response.getheaders()
            stem.with_suffix(".body").write_bytes(body)
            details.update(status=response.status, headers=headers, body_file=str(stem.with_suffix(".body")))
            passed = response.status == expected and len(body) <= 1_048_576
            if gateway:
                details["trace_id"] = trace
                passed = passed and response.getheader("X-Trace-Id") == trace
                health = object_json(body.decode("utf-8"))
                passed = passed and health.get("status") == ("UP" if expected == 200 else "DOWN")
                if dependencies is not None:
                    passed = passed and (
                        nested(health, "components", "dependencies", "details", "kafka")
                        is dependencies[0] and
                        nested(health, "components", "dependencies", "details", "registry")
                        is dependencies[1]
                    )
                if passed:
                    self.request_logs[trace] = expected
            return passed, details
        except (OSError, http.client.HTTPException, ValueError) as error:
            details["error_type"] = type(error).__name__
            return False, details
        finally:
            connection.close()
            details["seconds"] = round(time.monotonic() - started, 3)
            stem.with_suffix(".json").write_text(json.dumps({"time": utc(), **details}, indent=2) + "\n")

    def gateway_health(self, phase: str, ready: bool, kafka: bool, registry: bool) -> None:
        self.wait(f"{phase}_readiness", lambda: self.http(
            8080, "/actuator/health/readiness", 200 if ready else 503,
            gateway=True, dependencies=(kafka, registry),
        ), timeout=120)
        self.wait(f"{phase}_liveness", lambda: self.http(
            8080, "/actuator/health/liveness", 200, gateway=True,
        ), timeout=30)

    @staticmethod
    def safe_log(line: str, service: str) -> dict[str, object] | None:
        try:
            record = object_json(line)
        except (ValueError, TypeError):
            return None
        timestamp = record.get("timestamp")
        trace = record.get("trace_id")
        common = {"timestamp", "level", "service", "trace_id"}
        if (type(timestamp) not in (int, float) or not math.isfinite(cast(float, timestamp))
                or record.get("level") not in LEVELS or not isinstance(trace, str)
                or TRACE.fullmatch(trace) is None):
            return None
        if service == "gateway":
            event = record.get("event")
            allowed = common | {"event"}
            if event in ("request_completed", "request_failed"):
                allowed.add("status")
                if "status" in record and (type(record["status"]) is not int
                        or not 100 <= record["status"] <= 599):
                    return None
                if event == "request_failed":
                    allowed.add("failure_kind")
                    if record.get("failure_kind") not in {"io", "servlet", "runtime", "error"}:
                        return None
            elif event not in ("framework_event", "application_startup_failure"):
                return None
            if record.get("service") != "gateway" or not common | {"event"} <= record.keys():
                return None
        else:
            allowed = common | {"message"}
            message = record.get("message")
            if not isinstance(message, str) or not (
                message in {"worker_starting", "worker_config_invalid", "worker_startup_exhausted",
                            "worker_runtime_failed", "worker_close_failed", "worker_stopped"}
                or re.fullmatch(r"heartbeat assigned_partitions=[0-6]", message)
                or re.fullmatch(r"worker_startup_failed attempt=[1-3]", message)
            ):
                return None
            if record.get("service") != "inference-worker-stub" or set(record) != allowed:
                return None
        return record if set(record) <= allowed else None

    def logs(self, service: str, since: str | None = None) -> list[dict[str, object]]:
        # Raw bytes remain in memory until each line passes the fixed-field safety boundary.
        self.sequence += 1
        args = [*self.docker, "logs"]
        if since:
            args.extend(["--since", since])
        args.append(self.ids[service])
        started = time.monotonic()
        result = subprocess.run(args, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=30, check=False)
        records: list[dict[str, object]] = []
        rejected: list[dict[str, object]] = []
        for channel, data in (("stdout", result.stdout), ("stderr", result.stderr)):
            accepted: list[bytes] = []
            for number, line in enumerate(data.splitlines(keepends=True), 1):
                if not line.strip():
                    accepted.append(line)
                    continue
                try:
                    record = self.safe_log(line.decode("utf-8"), service)
                except (UnicodeError, TypeError, OverflowError):
                    record = None
                if record is None:
                    rejected.append({"channel": channel, "line": number,
                                     "sha256": hashlib.sha256(line).hexdigest()})
                else:
                    accepted.append(line)
                    records.append(record)
            target = self.output / "logs" / f"{self.sequence:04d}-{service}.{channel}.jsonl"
            target.write_bytes(b"".join(accepted))
        self.record("safe_log_capture", argv=args, returncode=result.returncode, rejected=rejected,
                    seconds=round(time.monotonic() - started, 3), records=len(records))
        self.check(f"{service}_logs_safe", result.returncode == 0 and not rejected,
                   rejected_lines=len(rejected), records=len(records))
        return records

    def group(self) -> tuple[bool, dict[str, object]]:
        code, output = self.command(self.compose(
            "exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", "kafka:19092", "--describe", "--group", GROUP,
            "--offsets", "--timeout", "10000",
        ), timeout=25)
        # Kafka 3.9's member view may omit topic names. The offsets view retains
        # explicit topic/partition/member ownership even when offsets are uncommitted.
        members = [line.split() for line in output.splitlines()
                   if line.strip().startswith(GROUP + " ")]
        good = code == 0 and len(members) == 6 and all(len(row) == 9 for row in members)
        if good:
            good = (all(row[1] == TOPIC and row[6] != "-"
                        and row[7].lstrip("/") == self.worker_identity.get("ip")
                        and row[8].startswith("kafka-python") for row in members)
                    and len({row[6] for row in members}) == 1
                    and {row[2] for row in members} == {str(index) for index in range(6)})
        return good, {"members": members, "returncode": code}

    def worker_recovery(self, phase: str, since: str) -> None:
        state = self.state("inference-worker")
        identity_keys = ("id", "pid", "started_at", "restart_count")
        self.check(f"{phase}_same_worker_process", state.get("status") == "running"
                   and all(state.get(key) == self.worker_identity.get(key) for key in identity_keys),
                   state=state, original=self.worker_identity)
        self.wait(f"{phase}_broker_member_six_partitions", self.group, timeout=180)

        def heartbeat() -> tuple[bool, dict[str, object]]:
            records = self.logs("inference-worker", since)
            traces = {record["trace_id"] for record in records}
            has_assignment = any(record.get("message") == "heartbeat assigned_partitions=6"
                                 for record in records)
            return has_assignment and traces == {self.worker_trace}, {"records": len(records)}
        self.wait(f"{phase}_fresh_worker_heartbeat", heartbeat, timeout=45)
        final_state = self.state("inference-worker")
        self.check(f"{phase}_worker_identity_after_recovery", final_state.get("status") == "running"
                   and all(final_state.get(key) == self.worker_identity.get(key)
                           for key in identity_keys), state=final_state)

    def gateway_logs(self) -> None:
        records = self.logs("gateway")
        completed = {(row.get("trace_id"), row.get("status")) for row in records
                     if row.get("event") == "request_completed"}
        self.check("gateway_http_trace_and_log_correlation", all(
            (trace, status) in completed for trace, status in self.request_logs.items()
        ), expected_requests=len(self.request_logs))

    def dockerignore_probe(self) -> None:
        """Exercise Docker's real ignore engine using synthetic files in a separate context."""
        rules = (self.root / ".dockerignore").read_bytes()
        marker = f"SYNTHETIC-EXAMPLE-{uuid.uuid4().hex}"
        excluded = (".env", ".env.foo", ".env.local", "nested/.env", "nested/.env.foo",
                    "nested/.env.example")
        assertions = " && ".join(f"test ! -e /context/{name}" for name in excluded)
        dockerfile = (
            "FROM debian:bookworm-slim\nCOPY . /context\n"
            f"RUN test -f /context/.env.example && {assertions}\n"
        )
        tag = f"{self.project}-dockerignore-probe:acceptance"
        (self.output / "dockerignore-probe.Dockerfile").write_text(dockerfile)
        (self.output / "dockerignore-probe.rules").write_bytes(rules)
        self.record("dockerignore_probe", image_tag=tag, synthetic_only=True,
                    expected_excluded=list(excluded), expected_present=[".env.example"],
                    rules_sha256=hashlib.sha256(rules).hexdigest())
        with tempfile.TemporaryDirectory(prefix=f"{self.project}-ignore-") as temporary:
            context = Path(temporary)
            (context / "nested").mkdir()
            (context / ".dockerignore").write_bytes(rules)
            (context / "Dockerfile").write_text(dockerfile)
            (context / ".env.example").write_text(marker + "\n")
            for name in excluded:
                (context / name).write_text("SYNTHETIC-EXCLUDED-PLACEHOLDER\n")
            self.required_command("dockerignore_actual_build_probe", [
                *self.docker, "build", "--progress", "plain", "--no-cache", "--tag", tag,
                "--label", f"com.docker.compose.project={self.project}",
                "--label", "fleet.acceptance.probe=dockerignore", str(context),
            ], timeout=600)
        image_id = self.required_command(
            "dockerignore_probe_image_id", [*self.docker, "image", "inspect", "--format", "{{.Id}}", tag],
        ).strip()
        self.check("dockerignore_probe_image_identity", self.image_evidence({"image_id": image_id}))

    def exercise(self) -> None:
        self.creation_attempted = True
        self.dockerignore_probe()
        self.compose_up_attempted = True
        self.required_command("build_and_start_full_compose", self.compose("up", "--build", "--detach"), 1800)
        self.discover()
        for service in sorted(SERVICES - {"topics"}):
            self.wait_state(service)
        self.wait_state("topics", exited=True)
        self.wait_state("kafka", health="healthy")
        self.wait_state("postgres", health="healthy")
        self.wait("registry_api", lambda: self.http(8081, "/apis/registry/v2/system/info", 200))
        self.wait("minio_live", lambda: self.http(9000, "/minio/health/live", 200))
        self.gateway_health("baseline", True, True, True)
        self.worker_identity = self.state("inference-worker")
        worker_logs = self.logs("inference-worker")
        traces = {str(row["trace_id"]) for row in worker_logs}
        self.check("one_worker_lifetime_trace", len(traces) == 1)
        self.worker_trace = next(iter(traces))
        self.worker_recovery("baseline", str(self.worker_identity["started_at"]))
        self.gateway_logs()
        for service in ("kafka", "registry"):
            self.required_command(f"stop_{service}", self.compose("stop", "--timeout", "10", service))
            stopped = self.state(service)
            self.check(f"{service}_actually_stopped", stopped.get("status") == "exited", state=stopped)
            self.gateway_health(f"{service}_down", False, service != "kafka", service != "registry")
            self.gateway_logs()
            since = utc()
            self.required_command(f"restart_{service}", self.compose("start", service))
            if service == "kafka":
                self.wait_state(service, health="healthy")
            else:
                self.wait("registry_api_recovered", lambda: self.http(8081, "/apis/registry/v2/system/info", 200))
            self.gateway_health(f"{service}_recovered", True, True, True)
            self.worker_recovery(f"{service}_recovered", since)
            self.gateway_logs()

    def cleanup(self) -> bool:
        if not self.creation_attempted:
            return True
        succeeded = True
        try:
            container_ids = self.resources("container")
            owned: list[str] = []
            attached_volumes: set[str] = set()
            identities: list[dict[str, object]] = []
            for container_id in container_ids:
                code, text = self.command([*self.docker, "inspect", "--format", STATE_FORMAT, container_id])
                state = object_json(text) if code == 0 else {}
                service = state.get("service")
                self.check("cleanup_container_ownership", state.get("project") == self.project
                           and isinstance(service, str) and service in SERVICES, container_id=container_id)
                assert isinstance(service, str)
                owned.append(container_id)
                self.ids[service] = container_id
                identities.append(state)
                try:
                    image_recorded = self.image_evidence(state)
                except (OSError, subprocess.SubprocessError):
                    image_recorded = False
                succeeded = image_recorded and succeeded
                volumes = self.required_command(
                    "record_container_volume_names", [*self.docker, "inspect", "--format",
                        '{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}}{{println}}{{end}}{{end}}',
                        container_id],
                )
                attached_volumes.update(volumes.split())
            (self.output / "containers-before-cleanup.json").write_text(
                json.dumps(identities, indent=2) + "\n")
            if owned:
                code, _ = self.command([*self.docker, "stop", "--time", "10", *owned], timeout=90)
                succeeded = succeeded and code == 0
                for service in ("gateway", "inference-worker"):
                    if service in self.ids:
                        try:
                            self.logs(service)
                        except (AcceptanceError, subprocess.TimeoutExpired):
                            succeeded = False
                code, _ = self.command([*self.docker, "rm", "--force", *owned], timeout=90)
                succeeded = succeeded and code == 0
            for network in self.resources("network"):
                # Every candidate carries this unique project's label; none existed preflight.
                code, _ = self.command([*self.docker, "network", "rm", network])
                succeeded = succeeded and code == 0
            retained = sorted(attached_volumes | set(self.resources("volume")))
            (self.output / "retained-volumes.json").write_text(json.dumps(retained, indent=2) + "\n")
            self.record("retained_volumes", names=retained, removed=False)
            self.command([*self.docker, "image", "ls", "--no-trunc", "--digests",
                          "--filter", f"reference={self.project}-*",
                          "--format", '{{json .}}'])
            self.check("cleanup_no_project_containers", not self.resources("container"))
            self.check("cleanup_no_project_networks", not self.resources("network"))
        except (AcceptanceError, OSError, ValueError, subprocess.SubprocessError) as error:
            self.record("cleanup_error", error_type=type(error).__name__)
            succeeded = False
        self.record("cleanup_finished", status="passed" if succeeded else "failed")
        return succeeded


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New evidence directory; must not exist")
    parser.add_argument("--project", required=True, help="Unique disposable Compose project name")
    parser.add_argument("--docker", type=Path, required=True, help="Absolute path to authorized Docker CLI")
    parser.add_argument("--context", help="Configured local Docker context; default is selected context")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{2,48}", args.project):
        parser.error("--project must be 3–49 lowercase letters/digits/hyphens/underscores")
    if not args.docker.is_absolute() or not args.docker.is_file():
        parser.error("--docker must identify an existing executable by absolute path")
    if args.output.exists():
        parser.error("--output must be a new directory")
    harness = Harness(args.docker, args.output.resolve(), args.project, args.context)

    def terminate(_signum: int, _frame: FrameType | None) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, terminate)
    result = 1
    try:
        harness.start_sources = harness.source_snapshot("start")
        harness.preflight()
        harness.exercise()
        result = 0
    except (AcceptanceError, OSError, ValueError, subprocess.SubprocessError, KeyboardInterrupt) as error:
        harness.record("run_failed", error_type=type(error).__name__)
        print(f"[failed] {type(error).__name__}; inspect {harness.output}", file=sys.stderr)
    finally:
        # Finish bounded cleanup even after a first Ctrl-C or termination request.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        if not harness.cleanup():
            result = 1
        try:
            harness.finish_source_evidence()
        except (AcceptanceError, OSError, ValueError) as error:
            harness.record("source_evidence_failed", error_type=type(error).__name__)
            result = 1
        summary = {"finished_at": utc(), "status": "passed" if result == 0 else "failed",
                   "project": harness.project, "context": harness.context,
                   "checks": harness.checks, "compose_up_attempted": harness.compose_up_attempted,
                   "runtime_acceptance_passed": result == 0,
                   "named_volumes_removed": False}
        (harness.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return result


if __name__ == "__main__":
    raise SystemExit(main())
