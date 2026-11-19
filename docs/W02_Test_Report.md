# Week 2 gateway verification

Verification date: 18 November 2026 (America/Los_Angeles).

The Week 2 change adds validated HTTP ingestion, content-addressed image storage,
versioned Avro publication, and a durable frame-submission journal. This report
separates executable evidence from planned behavior. Counts below come from
JUnit XML or the recorded Python run; a test being present in source is not
reported as a passing result.

## Results and evidence

| Check | Observed result | Evidence |
| --- | --- | --- |
| Gateway unit / Spring MVC tests | **55 passed**, 0 failures/errors/skips | `gateway/build/test-results/test/TEST-*.xml` |
| Packaged-gateway startup/logging regression | **1 passed**, 0 failures/errors/skips | `gateway/build/test-results/bootJarRegressionTest/TEST-com.ingen.fleet.GatewayStartupLoggingTest.xml` |
| Stream-processor scaffold test | **1 passed**, 0 failures/errors/skips | `stream-processor/build/test-results/test/TEST-com.ingen.fleet.StreamProcessorTest.xml` |
| Java build and configured Checkstyle | Passed in the recorded build | Delivery evidence `build.log`; root Gradle `build` includes the packaged-gateway regression |
| Python lockfile, Ruff, strict mypy and pytest | **38 pytest cases passed**; lint/type checks passed | Recorded delivery verification; these are existing worker/scaffold checks, not Week 3 inference acceptance |
| Real-service HTTP/Kafka integration | **17 passed**, 0 failures/errors/skips | `gateway/build/test-results/integrationTest/TEST-com.ingen.fleet.ingestion.GatewayIntegrationTest.xml` and integration console output |
| Gateway coverage | **Pending final JaCoCo report** | `gateway/build/reports/jacoco/test/jacocoTestReport.xml` and `html/index.html` |
| Docker Compose and verbatim documented curl commands | **Pending recorded execution** | To be linked to the delivery's Compose/API evidence |
| Remote passing CI and intentionally incompatible branch | **Pending remote evidence** | To be replaced by exact GitHub run URLs and commit SHAs |

The 55 gateway tests include 14 contract/codec/registry tests, 12 request
validation tests, 2 limiter tests, and 27 existing health/logging/trace tests.
The separate startup regression launches the packaged application; it is not
included in the 55. No minimum coverage percentage or arbitrary test-count
threshold is used as a substitute for the required behaviors.

## Real-service test configuration

The integration suite starts isolated containers and a Spring Boot HTTP server
on a random port. It consumes actual Kafka records, resolves their writer
schema IDs from the actual registry, decodes Avro bytes, and reads stored image
bytes back from MinIO. Mocks are not used in place of those integration services.

| Component | Version / configuration |
| --- | --- |
| Java | JDK 21 |
| Gradle | Wrapper 8.12.1 |
| Spring Boot | 3.4.5 |
| Apache Avro | 1.12.0 |
| Testcontainers | 1.20.6 |
| JaCoCo | 0.8.12 |
| Kafka | `apache/kafka:3.9.0`, single-node KRaft, six partitions per test topic |
| Schema registry | `apicurio/apicurio-registry-sql:2.6.8.Final`, PostgreSQL-backed `registry` database, explicit global BACKWARD default plus verified subject rules |
| PostgreSQL | `postgres:16.8`, separate ingestion and registry databases |
| MinIO | `ingen-minio:week2`, built by `deploy/Dockerfile.minio` from the pinned official MinIO source revision documented in [Registry and licenses](REGISTRY_AND_LICENSES.md) |

The integration upload limit is intentionally reduced to 1 MiB and the limiter
to a burst of 2 at 0.01 requests/second so rejection paths run deterministically.
Those are test overrides, not the default client limits in
[Event contracts](W02_Event_Contracts.md). Service credentials are generated for
the run; they are not part of the evidence report.

## Acceptance coverage

The following mappings identify the assertions verified by the 17 passing
methods in `GatewayIntegrationTest`.

| Required behavior | Executable evidence |
| --- | --- |
| Valid frame, exact stored bytes and one actual event | `validFrameStoresExactImageAndPublishesOneRegisteredAvroEvent`: checks HTTP 201, content hash/key, MinIO byte equality, one consumed record, robot key, schema ID, decoded metadata and trace correlation. |
| Duplicate returns the original event/object | `sequentialDuplicateReturnsOriginalIdWithoutSecondObjectOrKafkaRecord`: checks the second HTTP response and actual topic/object counts. |
| Concurrent duplicate safety | `concurrentDuplicateIsSerializedByDurableJournal`: concurrent HTTP submissions converge on one event ID and one consumed event. This is not a multi-JVM failover test. |
| Identity includes the robot | `sameImageFromDifferentRobotsHasDifferentEventsAndSharedContentObject`: distinct events share a content-addressed object. |
| Telemetry contract and partition key | `telemetryUsesRegisteredSchemaAndRobotPartitionKey`: verifies actual registered Avro telemetry and robot-keyed publication. |
| Corrupt image and MIME mismatch rejection | `corruptJpegAndMismatchedMimeCannotReachStorageOrKafka`: checks 422 and absence of new object/event writes. Additional unit cases cover image structure, dimensions, truncation and trailing data. |
| Missing fields; naive/future timestamps | `missingMetadataFieldsAndNaiveOrFutureTimestampsNeverWriteAnObjectOrEvent`: rejected inputs cannot add object/event records. |
| Valid timezone conversion | `offsetTimestampIsAcceptedAndConvertedToUtcEpochMilliseconds`: checks the encoded UTC instant rather than only the HTTP response. |
| Maximum upload size | `oversizedMultipartReturnsClean413WithoutPublishing`: exercises the real servlet multipart limit and uniform error response. |
| Multipart and JSON parsing failures | `malformedAndMissingMultipartPartsUseTheSameErrorEnvelope` and `telemetryValidationAndMalformedJsonNeverPublish`. |
| Per-robot rate limit | `rateLimitReturnsRetryAfterAndDoesNotPublishRejectedRequest`: checks 429, `Retry-After`, and no rejected publication. |
| Consistent framework errors | `frameworkAndNotFoundFailuresHaveTraceLinkedUniformEnvelope`: asserts `code`, `message`, `trace_id`, `details` and trace header consistency. |
| API discoverability | `openApiExposesAllThreeEndpointsAndSwaggerUi`: inspects OpenAPI paths and reachable Swagger UI. |
| Registry BACKWARD enforcement | `registryEnforcesBackwardForEveryRequiredContractAndRejectsBreakingChange`: queries real subject configuration and attempts a structurally incompatible change. |
| Persistent schema IDs after restart | `registeredSchemaIdsAndExistingEventRemainReadableAfterRegistryRestart`: restarts the SQL registry and reads an earlier event by its original ID. This does not test loss of its PostgreSQL volume. |
| Optional v2 field and v1 consumer | `realRegistryAndKafkaDemonstrateV1ReaderConsumingOptionalV2Field`: registers the packaged `camera_id` v2 schema on an isolated test subject, publishes actual bytes, resolves the writer schema, and consumes with the v1 reader. |

The local schema gate separately checks each candidate against the committed
v1 baseline. Its unit tests prove both new-reader/old-data BACKWARD behavior and
old-reader/new-data behavior for the optional-defaulted v2 field. These two
directions are deliberately distinguished. A mandatory new field without a
default must fail the baseline gate; remote negative-branch evidence remains
separate from a unit test that expects an incompatibility result.

The implementation uses Apache Avro `GenericRecord` and explicit schemas rather
than a Gradle code-generation plugin. This is intentional: generic encoding and
reader/writer resolution make the wire contract and schema-ID handling directly
testable. Generated model classes are not required for those interoperability
guarantees. This is a tooling choice, not a claim that code generation ran.

## Measured payload evidence

The integration suite emits a `W02_PAYLOAD_MEASUREMENT` JSON marker with the
actual uploaded byte length and consumed Kafka value length, including the
five-byte schema-ID envelope. Its fixture is a generated 24×16 JPEG, so it is
not natural-image evidence and must not be used to claim a typical Rover frame
size. In the successful run, the image was **819 bytes** and its Kafka value
was **288 bytes**, including the five-byte prefix. The registry assigned schema
ID **1** in that isolated run; clients must resolve the actual registered ID
rather than hard-code this observed value. The marker is retained in the
integration XML's `system-out` section.

The separate [natural-image payload experiment](NATURAL_PAYLOAD_EXPERIMENT.md)
addresses the Week 1 image-size feedback using licensed natural images and
documented encoding settings. Its measurement JSON is separate from this
gateway wire measurement; neither is a production robot benchmark.

## Reproduction and report scope

From the repository root, with JDK 21, Docker/Compose and Python 3.11 available:

```sh
./gradlew --no-daemon clean build
docker build -f deploy/Dockerfile.minio -t ingen-minio:week2 .
./gradlew --no-daemon :gateway:integrationTest :gateway:jacocoTestReport
uv sync --locked
uv run ruff check .
uv run mypy inference-worker tests scripts
uv run pytest
```

JaCoCo reports gateway main-source coverage and merges the matching execution
data under `gateway/build/jacoco/`; the final coverage entry must include the
covered/total counters, not only a rounded percentage. The packaged child JVM
startup test is independent evidence; do not infer child-process line coverage
from the harness passing. Local build directories are generated artifacts.
The CI workflow uploads JUnit/test HTML and JaCoCo artifacts even on failures;
the report's run links distinguish a completed passing commit from an earlier
baseline or intentionally failing compatibility branch.

## Limits that remain

- Kafka acknowledgement and PostgreSQL journal commit are not one transaction.
  A crash between them can republish the same stable `event_id`; downstream
  consumers must deduplicate. No global exactly-once claim or injected-crash
  proof is made. A `PENDING` submission is retried through the HTTP API.
- The local stack has one broker and no replicated failover or backup proof.
  Durable volumes improve restart behavior but do not establish high availability
  or protect against independently deleting/restoring only part of the stack.
- Rate limiting is process-local and resets on restart. It does not provide a
  cluster-wide quota across gateway replicas. Telemetry HTTP resubmissions are
  not application-deduplicated.
- Objects and journal rows have no implemented privacy/retention deletion job
  yet. Kafka retention does not delete MinIO objects. Use only approved test
  media in this development stack; later privacy gates and lifecycle work remain.
- Real inference, detection-quality evaluation, enrichment, privacy processing,
  query APIs, multi-robot performance targets and broker-failure chaos are later
  stages. Passing Week 2 tests does not establish those results.
- The documented curl workflow must be executed separately; a MockMvc or
  Testcontainers result alone does not prove that a fresh client can run every
  published command verbatim.
