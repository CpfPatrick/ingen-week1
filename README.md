# inGen event ingestion — Weeks 1–2

Java 21 / Python 3.11 development stack for a simulated robot client. Week 2 adds a
Spring Boot ingestion gateway, four registered Avro contracts, content-addressed
MinIO storage, and a PostgreSQL submission journal. The original Week 1 skeleton
and its historical verification record remain available in Git history.

## Start the development stack

Requirements: Docker with Compose, JDK 21 (including `javac`), Python 3.11 and uv.
Run these blocks in order from the repository root in the same Bash session.
Gateway and registry host ports default to 8080 and 8081. To avoid a port conflict,
run `export GATEWAY_PORT=18080 REGISTRY_PORT=18081` before these blocks; client URLs
use the exported ports while container-to-container addresses stay unchanged. Create
throwaway local credentials, install the locked Python environment, and start PostgreSQL:

```bash
set -euo pipefail
GATEWAY_URL="http://localhost:${GATEWAY_PORT:-8080}"
REGISTRY_URL="http://localhost:${REGISTRY_PORT:-8081}"
python3 - <<'PY'
from pathlib import Path
import secrets
p = Path('.env')
if not p.exists():
    p.write_text('POSTGRES_PASSWORD=' + secrets.token_hex(24) + '\n'
                 'MINIO_ROOT_USER=localadmin\n'
                 'MINIO_ROOT_PASSWORD=' + secrets.token_hex(24) + '\n')
    p.chmod(0o600)
PY
uv sync --locked
docker compose up -d --wait --wait-timeout 120 postgres
```

Fresh volumes initialize both databases. The following is safe to run for any
volume and also creates the separate registry database when upgrading an older setup:

```bash
docker compose exec -T postgres psql -U fleet -d fleet -v ON_ERROR_STOP=1 <<'SQL'
SELECT 'CREATE DATABASE registry'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'registry')\gexec
SQL
```

The journal starts with the versioned Flyway V1 migration. A pre-existing nonempty
`fleet` schema without Flyway history needs an explicit reviewed migration;
startup never automatically baselines it. Use a fresh Week 2 database or an
existing database with valid migration history.

Start the full stack and wait for readiness with bounded requests:

```bash
docker compose up -d --build
for attempt in {1..60}; do
  if curl --fail --silent --connect-timeout 2 --max-time 3 \
      "${GATEWAY_URL}/actuator/health/readiness" \
      > /tmp/ingen-readiness.json; then
    break
  fi
  sleep 2
done
curl --fail-with-body --silent --show-error --connect-timeout 2 --max-time 3 \
  "${GATEWAY_URL}/actuator/health/readiness"
```

The first MinIO build compiles a pinned official source release and may take several
minutes. The final request must return readiness `UP` before sending inputs; it
fails the shell session if the bounded wait expires without readiness. The gateway retries startup
through Compose's bounded restart policy when dependencies are not yet ready.
Health/readiness checks Kafka, Registry, the journal and object storage. Liveness
is independent of external service availability.

The complete [client walkthrough](docs/W02_Event_Contracts.md#worked-client-examples)
creates a synthetic JPEG and exercises all three endpoints:

- `POST /v1/frames`: multipart image and metadata, returning event/object identifiers.
- `POST /v1/telemetry`: validated JSON telemetry.
- `GET /v1/frames/{event_id}/status`: durable ingestion status.

OpenAPI is available at `${GATEWAY_URL}/swagger-ui` and `${GATEWAY_URL}/v3/api-docs`.
The [contract document](docs/W02_Event_Contracts.md) defines every field, topic,
limit, error, duplicate rule and the distinction between HTTP and Avro JSON.

## Verification

```sh
./gradlew --no-daemon build
docker build -f deploy/Dockerfile.minio -t ingen-minio:week2 .
./gradlew --no-daemon :gateway:integrationTest :gateway:jacocoTestReport
uv run ruff check .
uv run mypy inference-worker tests scripts
uv run pytest
```

The integration task uses Testcontainers with real Kafka, SQL-backed Apicurio,
PostgreSQL and MinIO. Docker is required; missing Docker is a failure, not a skipped
acceptance result. GitHub Actions runs the same checks and uploads JUnit/JaCoCo
artifacts. The schema compatibility gate compares current contracts to the retained
initial contract snapshots, and the Registry independently enforces `BACKWARD`.
The test report records actual results and the intentionally incompatible branch.

## State and delivery semantics

Kafka logs, PostgreSQL data and MinIO objects use named local volumes. The Registry
owns a separate `registry` database; the gateway owns its Flyway-managed ingestion
journal. Ordinary service restarts retain IDs and object references. An existing
PostgreSQL volume missing the `registry` database is handled by the idempotent
startup block above. Retained volumes provide local recovery, not backup,
replication or unlimited Kafka retention. Independent volume deletion can break
references and is not a recovery procedure.

Normal repeat/concurrent submission of `(robot_id, sha256, captured_at)` returns the
same event ID and does not publish another event. A crash between Kafka acknowledgement
and journal commit may redeliver the same ID: this is **at-least-once across failures**.
Future downstream consumers must deduplicate stable IDs. A `SUBMITTED` status means
ingestion was acknowledged; it does not mean inference or privacy processing finished.

The API has no authentication and the stack binds host ports to loopback. Use
synthetic or permitted non-identifying fixtures. This single-broker development
stack is not a production deployment. Model inference, enrichment, privacy redaction,
final business persistence, automated object retention, query APIs and the assistant
belong to later weeks. Proposed retention windows are not approved operating policy.

## Documentation and evidence

- [Week 2 event and API contracts](docs/W02_Event_Contracts.md)
- [Week 2 test report](docs/W02_Test_Report.md)
- [Week 2 decision log](weekly/Wk-02-Log.md)
- [Natural-photograph payload experiment](docs/NATURAL_PAYLOAD_EXPERIMENT.md)
- [Registry choice and third-party licenses](docs/REGISTRY_AND_LICENSES.md)
- [Historical Week 1 verification](weekly/VERIFICATION.md)

Original internal requirements, product documents, private links, real environment
files and the private Week 1 review packet are excluded from this public source
repository. The source does not redistribute proprietary product material or real
customer imagery. Third-party components retain their own licenses.
