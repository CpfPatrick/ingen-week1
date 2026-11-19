# Week 2 event contracts and ingestion API

This document describes the implemented local development gateway and the four Avro contracts in `schemas/avro/`. The gateway publishes frame and telemetry events. Detection and exception schemas are registered for later producers; this delivery does not implement model inference, privacy redaction, alert processing or exception routing. Use synthetic demonstration inputs. The API has no authentication and the development services bind to loopback.

## Contract conventions

All records use namespace `com.ingen.fleet.events`. A field marked **required** has no Avro default and does not accept null. Optional strings use exactly `["null","string"]` with `default: null`; a default is an Avro reader-evolution rule, not a fabricated measurement. The JSON examples under `schemas/avro/examples/` are synthetic Avro-JSON fixtures, not HTTP request bodies or captured device records. Avro checks structure and primitive types; producers must separately enforce image validity, numeric ranges and business rules.

`captured_at` is the client capture time; `received_at` is assigned by the gateway when accepting a new submission. Both use Avro `long` with logical type `timestamp-millis`, representing UTC Unix epoch milliseconds. HTTP clients submit an ISO-8601 timestamp with `Z` or an explicit offset. The gateway rejects naive or future timestamps, converts offsets to UTC, and truncates sub-millisecond precision. A raw monotonic device tick is not an epoch timestamp. For a repeated frame, the first stored `received_at` is retained.

`robot_id` accepts 1–64 characters matching `[A-Za-z0-9][A-Za-z0-9_.-]{0,63}`. HTTP `platform` must be one of `AIDO_ROVER`, `FARI`, `SENTINEL_PRIME_AI`, `AIDO_HUMANOID`, `SENPAI`. Avro stores an extensible string rather than a closed enum; HTTP validation is intentionally more restrictive. Adding an accepted platform is an API implementation change, not permission to change the meaning of an existing value.

### FrameSubmitted

Source: `schemas/avro/FrameSubmitted.avsc`.

| Field | Avro type / default | Meaning |
| --- | --- | --- |
| `event_id` | string; required | Stable frame-submission UUID; the application identity is described below. |
| `robot_id` | string; required | Source robot; also the Kafka key. |
| `platform` | string; required | Canonical platform discriminator. |
| `captured_at` | long / timestamp-millis; required | UTC capture time. |
| `received_at` | long / timestamp-millis; required | First gateway acceptance time. |
| `width` | int; required | Positive decoded image width in pixels. |
| `height` | int; required | Positive decoded image height in pixels. |
| `format` | string; required | Decoded `JPEG` or `PNG`, checked against the image part's declared media type. |
| `size_bytes` | long; required | Exact length of the uploaded encoded bytes. |
| `sha256` | string; required | Lowercase 64-character SHA-256 hex digest of those exact bytes. |
| `object_key` | string; required | Key in configured MinIO bucket `frames`; no credentials or signed URL. |
| `trace_id` | null or string; default null | Correlation ID; the gateway supplies a value, while null supports producers without tracing. |

The object key is `sha256/{first-two-hex-characters}/{sha256}.jpg` or `.png`. The stored bytes are the submitted bytes, not a transcoded image. Identical content shares one key even across capture times or robots. Different capture times can therefore create different events referring to the same object. An event does not imply exclusive ownership of its object; later deletion must account for shared references.

### DetectionProduced

Source: `schemas/avro/DetectionProduced.avsc`. There is no detection HTTP endpoint or inference producer in this delivery.

| Field | Avro type / default | Meaning |
| --- | --- | --- |
| `event_id` | string; required | UUID for this detection result. |
| `frame_event_id` | string; required | The processed `FrameSubmitted.event_id`. |
| `model_name` | string; required | Detector identity. |
| `model_version` | string; required | Immutable model artifact version. |
| `detections` | array of `Detection`; required | May be empty when no objects are detected; no default empty array is supplied. |
| `robot_id` | null or string; default null | Source robot when known; legacy results can be joined through `frame_event_id`. |
| `trace_id` | null or string; default null | Correlation with the originating request. |

Each `Detection` has the following required, non-null fields with no defaults:

| Field | Type | Meaning |
| --- | --- | --- |
| `class_label` | string | Normalized class label; mapping is owned by the future inference producer. |
| `confidence` | double | Finite confidence in `[0,1]`. |
| `x` | double | Left edge divided by decoded image width. |
| `y` | double | Top edge divided by decoded image height. |
| `w` | double | Box width divided by decoded image width. |
| `h` | double | Box height divided by decoded image height. |
| `inference_ms` | double | Nonnegative elapsed inference time for the whole frame, repeated on each detection from that inference; not a timestamp. |

The image coordinate origin is its top-left corner, with x increasing rightward and y downward. All four box values are finite and in `[0,1]`, with `x+w <= 1` and `y+h <= 1`. For a 640×480 image, pixel box `(64,48,128,96)` becomes `(0.1,0.1,0.2,0.2)`. These are producer obligations; the Avro type `double` alone does not enforce them. The current schema has no frame-level inference duration when `detections` is empty; add a defaulted field in a future compatible revision if that metric is needed.

### TelemetryReported

Source: `schemas/avro/TelemetryReported.avsc`.

| Field | Avro type / default | Meaning |
| --- | --- | --- |
| `event_id` | string; required | Fresh UUID for each accepted telemetry request. |
| `robot_id` | string; required | Source robot; also the Kafka key. |
| `platform` | string; required | Canonical platform discriminator. |
| `captured_at` | long / timestamp-millis; required | UTC capture time supplied by the client. |
| `received_at` | long / timestamp-millis; required | Gateway acceptance time. |
| `battery` | double; required | Finite percentage in `[0,100]`, not a fraction. |
| `pose` | record `Pose`; required | Local-map planar pose, defined below. |
| `speed` | double; required | Finite nonnegative ground-speed magnitude in metres/second. |
| `network_quality` | double; required | Finite dimensionless fraction in `[0,1]`; larger means better. |
| `operator_note` | null or string; default null | Optional free text, HTTP maximum 1,024 Java string code units. |
| `trace_id` | null or string; default null | Request/event correlation. |

`Pose.x`, `Pose.y` and `Pose.heading` are required Avro doubles with no defaults. x/y are finite metres in a consistently configured robot-local map, not latitude/longitude. Heading is finite yaw in radians about the map up-axis, counter-clockwise positive; HTTP accepts `[-π,π]`. The deployment must keep the map origin and axes consistent for that robot. A map/frame discriminator is not present in v1; cross-map comparisons require a future compatible extension and an explicit transform.

`operator_note` can contain personal information. The current gateway publishes it only to the raw telemetry Kafka event; it does not write the text to structured logs or the frame journal. This is not a de-identification implementation: downstream privacy processing is future work. Use invented notes in this development stack.

### ExceptionRaised

Source: `schemas/avro/ExceptionRaised.avsc`. Registering this contract does not mean invalid HTTP requests are published to Kafka. The gateway returns HTTP errors directly; a future processing component can use this contract for failed existing events.

| Field | Avro type / default | Meaning |
| --- | --- | --- |
| `event_id` | string; required | UUID for the exception event. |
| `stage` | string; required | Component/processing stage observing the failure. |
| `error_code` | string; required | Stable machine-readable classification. |
| `message` | string; required | Safe diagnostic, without raw payloads, credentials or stack traces. |
| `original_event_id` | null or string; default null | Failed event ID; null only when none was assigned. |
| `trace_id` | null or string; default null | Correlation ID, if available. |
| `robot_id` | null or string; default null | Source robot, if known. |
| `occurred_at` | long / timestamp-millis; required | UTC epoch milliseconds when the failure was observed. |

## Registry, wire format and evolution

The local registry is Apicurio Registry SQL `2.6.8.Final`, using a separate PostgreSQL `registry` database and its Confluent-compatible API at `/apis/ccompat/v7`, on host port 8081 by default. The gateway's configured registry URL is the origin (`http://registry:8080` inside Compose), and the client appends that API path. Changing the host's `REGISTRY_PORT` does not change that container address. Apicurio is selected under Apache-2.0; see `REGISTRY_AND_LICENSES.md` for dependency/license details.

| Record | Registry subject | Topic |
| --- | --- | --- |
| FrameSubmitted | `FrameSubmitted-value` | `fleet.frames.v1` |
| DetectionProduced | `DetectionProduced-value` | `fleet.detections.v1` |
| TelemetryReported | `TelemetryReported-value` | `fleet.telemetry.v1` |
| ExceptionRaised | `ExceptionRaised-value` | `fleet.exceptions.v1` |

These explicit subjects are independent of the Kafka topic names. Each value is a byte `0`, a positive four-byte big-endian registry schema ID, then binary Avro. It is not plain JSON. A consumer reads the ID, retrieves `/schemas/ids/{id}`, and decodes with that **writer schema** plus its own reader schema. Do not assume the latest subject schema wrote an older message. Kafka keys are UTF-8 robot IDs. Gateway events also carry a UTF-8 `trace_id` header.

Each subject uses `BACKWARD`: a new reader must be able to read old writer data. Upgrade compatible consumers before producers. The registry client configures and reads back that rule, checks compatibility with the latest version, registers the schema and verifies schema-by-ID before exposing an ID to the producer. A failed registration prevents publication through that client; it does not substitute an unrelated schema ID.

`schemas/avro/baseline/` is the initial contract baseline used by `SchemaCompatibilityTest` during the Java build. Do not edit the baseline to conceal a breaking change. The test checks each current reader against the baseline writer and includes a rejection case for a new required field without a default. Registry checks additionally compare with the deployed subject's latest version; `BACKWARD` is not `BACKWARD_TRANSITIVE`.

`evolution/FrameSubmitted.v2.avsc` adds `camera_id: [null,string]` with a null default. The codec/evolution tests use actual Avro bytes in both directions: a v2 reader supplies null when reading v1 data; a v1 reader ignores `camera_id` in v2 data. The latter is an old-reader/new-writer demonstration, not the definition of BACKWARD. This additive example supports both directions; arbitrary BACKWARD-compatible changes need not. It is a demonstration schema, not the schema emitted by the current gateway. Breaking field or semantic changes require a new version and a migration note; HTTP paths remain `/v1/` until such an API change is introduced.

The CI workflow runs the Java build/compatibility gate and a separate real-service integration suite. A negative fixture assertion is distinct from proof that an intentionally incompatible branch failed a remote CI run. Test outputs, coverage and any negative-branch execution belong in the test report, not an assumed pass statement here.

## Topics and delivery behavior

| Topic | Key | Partitions | Retention | Current role |
| --- | --- | --- | --- | --- |
| `fleet.frames.v1` | `robot_id` | 6 | 6h (`21600000` ms) | Gateway emits frame references. |
| `fleet.detections.v1` | `robot_id` | 6 | 24h (`86400000` ms) | Provisioned; future inference producer. |
| `fleet.telemetry.v1` | `robot_id` | 6 | 24h (`86400000` ms) | Gateway emits telemetry. |
| `fleet.exceptions.v1` | `robot_id` | 6 | 24h (`86400000` ms) | Provisioned; future processing failures. |
| `fleet.dev.heartbeat.v1` | Development worker convention | 6 | 1h (`3600000` ms) | Existing worker scaffold; not a robot-data contract. |

All are local replication factor 1, with effective minimum in-sync replicas 1. `acks=all` therefore acknowledges one local broker, not a replicated durable production cluster. The topic script creates missing topics; it does not migrate configuration of already-existing topics. Enriched and alerts topics/services are future work and are not provisioned here.

Six partitions allow up to six active consumers per group for each topic and per-key partition ordering at this development scale. A robot's records stay in one partition within a topic while partition count and keying remain unchanged. Kafka preserves partition log order, not capture-time order after offline replay, and does not order records across topics. One busy robot can dominate a partition; adding consumers cannot split that key. Future detection/exception producers must resolve the robot before using the documented robot-keyed stream; a null legacy `robot_id` does not define a production routing policy.

The producer sets `enable.idempotence=true`, `acks=all`, retries `Integer.MAX_VALUE`, `delivery.timeout.ms=10000`, `request.timeout.ms=3000`, `max.block.ms=5000`, and `max.in.flight.requests.per.connection=5`. Each send waits for acknowledgement, with a 12s caller bound. Values contain references/metadata only; this producer deliberately uses `max.request.size=65536` (64 KiB). The image-payload experiment's comparison with the Kafka producer's 1 MiB default is a transport-design baseline, not this gateway's configured event limit. The local broker limit is separately configured as `1048588` bytes.

Frame application idempotency is separate from Kafka retry idempotence. Its key is `(robot_id, SHA-256 of bytes, captured_at normalized to milliseconds)`. The stable UUID derives from that tuple and a PostgreSQL row persists its identity, first acceptance time, object key and `PENDING`/`SUBMITTED` state. Flyway applies `gateway/src/main/resources/db/migration/V1__ingestion_journal.sql` on first startup and validates the migration history on subsequent startups. A row lock serializes concurrent retries, including across gateway instances. After object storage and Kafka acknowledgement, the row becomes `SUBMITTED`. A normal duplicate of an acknowledged frame returns the original ID and object key without publishing a second event. Conflicting platform metadata for the same identity is rejected.

MinIO and PostgreSQL do not share a transaction with Kafka. A crash after Kafka acknowledgement but before journal commit can publish the same `event_id` again on retry. An acknowledged object can remain after a later publish failure. The guarantee across these failures is at-least-once with stable identity; consumers must deduplicate by `event_id`. No global exactly-once claim is made. `PENDING` is retried by resubmitting the same image and metadata; there is no background recovery worker. Telemetry has no application deduplication journal: every accepted HTTP resubmission receives a fresh event ID and can duplicate a logical reading.

PostgreSQL (journal and registry databases), MinIO and Kafka use named volumes; registry schema IDs, broker logs and claim-check state can survive container recreation when those volumes are retained. This is a recoverable local configuration, not unconditional availability: it still has one broker, no backup/replication guarantee, bounded Kafka retention and the cross-system acknowledgement/commit gap described above. Kafka retention does not delete image objects or journal rows. Per-platform object lifecycle, reference-aware deletion and downstream retention are future privacy/storage work. Raw objects and journal entries remain until explicitly removed by the developer; deleting volumes or restoring only part of the stack can break their consistency. Verify restart/recovery behavior with recorded tests before relying on it.

## HTTP API and errors

All request validation completes before new object/event persistence. Image submissions support JPEG and PNG, at most **5,242,880 bytes (5 MiB)**, **16,777,216 decoded pixels**, and **8,192 pixels per side**. The multipart request limit is 6 MiB, allowing metadata/envelope overhead. Declared image media type must match decoded bytes, and the full image must decode without warnings. JPEG requires its final end marker; PNG chunk structure and checksums are verified, including rejection of trailing data after IEND. Corrupt/truncated images are rejected. These limits are development decisions, not measured robot payload sizes.

The token bucket admits **10 requests/second per robot**, burst **20**, shared across both POST endpoints. Duplicates also consume tokens. The bucket is process-local, resets on restart and is not a distributed quota across replicas. It caps tracked identities at 10,000, cleaning up idle buckets; GET status does not consume tokens. Exceeding the rate yields `429` and integer-seconds `Retry-After`.

| Endpoint | Request | Success |
| --- | --- | --- |
| `POST /v1/frames` | `multipart/form-data`: `image` part with `image/jpeg` or `image/png`; `metadata` part with `application/json` containing required `robot_id, platform, captured_at`. | New/recovered submission: `201`; acknowledged duplicate: `200`. JSON: `event_id`, `object_key`, `status:"SUBMITTED"`. |
| `POST /v1/telemetry` | `application/json`; required `robot_id,platform,captured_at,battery,pose{x,y,heading},speed,network_quality`; optional omitted/null `operator_note`. | `201`; JSON: `event_id`, `status:"SUBMITTED"`. |
| `GET /v1/frames/{event_id}/status` | Canonical UUID in path. | `200`; JSON: `event_id,object_key,status,captured_at,received_at`. Times are UTC ISO-8601 strings. |

Status describes ingestion only. `SUBMITTED` means Kafka acknowledged; it does not imply inference, enrichment, privacy processing or operator review. `PENDING` means the durable identity exists but the storage/publication sequence has not been recorded as complete. Unknown or malformed frame IDs return `404 FRAME_NOT_FOUND`.

Every error body follows `{"code":"...","message":"...","trace_id":"...","details":{...}}`. `X-Trace-Id` is also returned. Details may identify a field or retry delay; they do not echo the supplied image, note, credentials or stack trace.

| HTTP | Code | Cause / response |
| --- | --- | --- |
| 413 | `UPLOAD_TOO_LARGE` | Image or multipart request exceeds its byte limit. |
| 422 | `INVALID_INPUT` | Missing/malformed body or part; invalid identifier/platform/timestamp/measurement; invalid image media type, mismatch, decode or dimension failure. Field-level validation includes `details.field` when available. |
| 429 | `RATE_LIMITED` | Shared per-robot token bucket exhausted; `Retry-After` and `details.retry_after_seconds` tell the client when to retry. |
| 404 | `FRAME_NOT_FOUND` / `NOT_FOUND` | Unknown frame identity / unknown route. |
| 415 | `UNSUPPORTED_MEDIA_TYPE` | Unsupported top-level HTTP content type. An invalid **image part** is instead a 422 validation error. |
| 405 | `METHOD_NOT_ALLOWED` | Method not supported on the route. |
| 503 | `DEPENDENCY_UNAVAILABLE` | Journal, object storage or publication is unavailable/uncertain. Retry identical frame bytes and metadata to preserve frame identity. |
| 503 | `RATE_LIMIT_CAPACITY` | Process-local limiter has too many active robot identities. |
| 500 | `INTERNAL_ERROR` | Unexpected failure with sanitized message. Servlet fallback failures use `REQUEST_FAILED`. |

## Worked client examples

Run the following blocks in order in the same Bash session from the repository root, with Java 21, Python 3.11/uv, Docker and Compose available. The setup creates throwaway credentials only when `.env` is absent, preserving an existing local configuration. Do not commit `.env`. Start PostgreSQL first so the next block can also prepare an older volume before starting the registry and gateway.

Gateway and registry host ports default to 8080 and 8081. If either is occupied, run `export GATEWAY_PORT=18080 REGISTRY_PORT=18081` before the setup blocks to select alternative ports. The examples derive their URLs from those exported variables; container-to-container addresses remain unchanged.

```bash
set -euo pipefail
GATEWAY_URL="http://localhost:${GATEWAY_PORT:-8080}"
REGISTRY_URL="http://localhost:${REGISTRY_PORT:-8081}"
python3 - <<'PY'
from pathlib import Path
import secrets
path = Path('.env')
if not path.exists():
    path.write_text('POSTGRES_PASSWORD=' + secrets.token_hex(24) + '\n'
                    'MINIO_ROOT_USER=localadmin\n'
                    'MINIO_ROOT_PASSWORD=' + secrets.token_hex(24) + '\n')
    path.chmod(0o600)
PY
uv sync --locked
docker compose up -d --wait --wait-timeout 120 postgres
```

Fresh PostgreSQL volumes create both databases through `deploy/init-databases.sql`. Initialization scripts do not rerun on existing volumes. This idempotent block creates `registry` only if it is missing and is safe to run for either case:

```bash
docker compose exec -T postgres psql -U fleet -d fleet -v ON_ERROR_STOP=1 <<'SQL'
SELECT 'CREATE DATABASE registry'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'registry')\gexec
SQL
```

The journal uses Flyway from its first table. A pre-existing nonempty `fleet` schema without Flyway history requires an explicit reviewed migration; the gateway does not automatically baseline it. These commands support a fresh Week 2 database or an existing database with valid migration history. Creating `registry` does not migrate an older journal schema.

Start the remaining services, then wait for gateway readiness with bounded requests before sending examples:

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

The final request must return `UP`; it fails the shell session if the bounded wait expires without readiness. OpenAPI JSON is at `${GATEWAY_URL}/v3/api-docs`; interactive documentation is at `${GATEWAY_URL}/swagger-ui` (redirects to the UI). All values below are generated or synthetic.

**Submit a frame** and save the real response, rather than copying an example ID:

```bash
.venv/bin/python scripts/create_demo_image.py /tmp/ingen-demo.jpg
CAPTURED_AT=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
curl --fail-with-body --silent --show-error \
  --dump-header /tmp/ingen-frame.headers \
  --output /tmp/ingen-frame.json \
  --request POST "${GATEWAY_URL}/v1/frames" \
  --header 'X-Trace-Id: demo-frame-01' \
  --form 'image=@/tmp/ingen-demo.jpg;type=image/jpeg' \
  --form "metadata={\"robot_id\":\"demo-rover-01\",\"platform\":\"AIDO_ROVER\",\"captured_at\":\"${CAPTURED_AT}\"};type=application/json"
.venv/bin/python -m json.tool /tmp/ingen-frame.json
FRAME_EVENT_ID=$(.venv/bin/python -c \
  'import json; print(json.load(open("/tmp/ingen-frame.json"))["event_id"])')
```

The first acceptance is HTTP 201 with `status:"SUBMITTED"`. Reuse the same shell variable and image to exercise application idempotency:

```bash
curl --fail-with-body --silent --show-error \
  --dump-header /tmp/ingen-duplicate.headers \
  --output /tmp/ingen-duplicate.json \
  --request POST "${GATEWAY_URL}/v1/frames" \
  --form 'image=@/tmp/ingen-demo.jpg;type=image/jpeg' \
  --form "metadata={\"robot_id\":\"demo-rover-01\",\"platform\":\"AIDO_ROVER\",\"captured_at\":\"${CAPTURED_AT}\"};type=application/json"
.venv/bin/python - <<'PY'
import json
original = json.load(open('/tmp/ingen-frame.json'))
duplicate = json.load(open('/tmp/ingen-duplicate.json'))
assert original['event_id'] == duplicate['event_id']
assert original['object_key'] == duplicate['object_key']
print(duplicate)
PY
```

The duplicate response should be HTTP 200. Equality of these HTTP IDs is not by itself a count of consumed Kafka events; the integration suite separately checks the topic.

**Query frame status** using the captured ID:

```bash
curl --fail-with-body --silent --show-error \
  "${GATEWAY_URL}/v1/frames/${FRAME_EVENT_ID}/status" \
  | .venv/bin/python -m json.tool
```

**Submit telemetry** with finite values in the documented units:

```bash
TELEMETRY_AT=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
curl --fail-with-body --silent --show-error \
  --request POST "${GATEWAY_URL}/v1/telemetry" \
  --header 'Content-Type: application/json' \
  --header 'X-Trace-Id: demo-telemetry-01' \
  --data "{\"robot_id\":\"demo-rover-01\",\"platform\":\"AIDO_ROVER\",\"captured_at\":\"${TELEMETRY_AT}\",\"battery\":80.0,\"pose\":{\"x\":1.0,\"y\":2.0,\"heading\":0.25},\"speed\":0.5,\"network_quality\":0.9,\"operator_note\":\"Synthetic local demo\"}" \
  | .venv/bin/python -m json.tool
```

The response is HTTP 201 with a fresh `event_id` and `status:"SUBMITTED"`. Omit `operator_note` or send JSON null for no note. Zero is a real measurement; do not substitute zero for missing battery, pose, speed or network-quality readings.

Inspect the registered contract without assuming its numeric ID:

```bash
curl --fail-with-body --silent --show-error \
  "${REGISTRY_URL}/apis/ccompat/v7/config/FrameSubmitted-value" \
  | .venv/bin/python -m json.tool
curl --fail-with-body --silent --show-error \
  "${REGISTRY_URL}/apis/ccompat/v7/subjects/FrameSubmitted-value/versions/latest" \
  | .venv/bin/python -m json.tool
```

To run verification rather than infer success from this document:

```bash
./gradlew --no-daemon build
docker build -f deploy/Dockerfile.minio -t ingen-minio:week2 .
./gradlew --no-daemon :gateway:integrationTest :gateway:jacocoTestReport
```

Gateway JUnit results, integration results and JaCoCo reports are under `gateway/build/test-results/` and `gateway/build/reports/`. Actual run outcomes and coverage must be taken from those artifacts and the accompanying test report.
