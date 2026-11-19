# Avro contracts

The Week 2 contracts are `FrameSubmitted`, `DetectionProduced`,
`TelemetryReported`, and `ExceptionRaised` in namespace `com.ingen.fleet.events`.
Each field is documented in its `.avsc` file. `examples/` contains synthetic
Avro-JSON examples, not captured robot data or verified product specifications.

The gateway registers subjects named `<EventName>-value` through Apicurio
Registry 2.6.8.Final's Confluent-compatible REST API. This explicitly selected
subject convention is independent of the Kafka topic name. Wire values use
magic byte `0`, a positive four-byte big-endian registry schema ID, and binary
Avro. A consumer resolves the writer schema by that ID, then supplies its own
reader schema. Assuming the latest schema is the writer schema is unsafe.

Subject compatibility is `BACKWARD`: a new reader can read previously written
events, so deploy compatible consumers before producers. The registration
adapter verifies both the stored schema and the active compatibility rule
before returning an ID. Registration failure prevents publishing through that
adapter; it does not silently substitute an old schema ID. Docker Compose uses
Apicurio's SQL image with a separate PostgreSQL `registry` database. Registry
content IDs and compatibility rules survive a container restart as long as its
PostgreSQL volume is retained. The gateway verifies its registrations at startup.
Do not reset or restore only the registry independently of retained Kafka data:
old wire IDs must continue resolving to their original schemas. The isolated
Testcontainers suite may use an ephemeral registry because its Kafka data has
the same disposable lifetime. The SQL image automatically initialises/upgrades
its own tables; PostgreSQL database creation remains a deployment responsibility.

`baseline/` records the initial published contract surface used by the build's
compatibility gate; do not rewrite it to hide a breaking change. Every build
checks candidate schemas against this baseline. `evolution/FrameSubmitted.v2.avsc`
adds optional `camera_id` with a null default. Tests prove both directions using
actual Avro bytes: v2 readers supply the default for v1 data, and v1 readers
ignore the new field in v2 data. The second direction alone is not the definition
of BACKWARD compatibility. Breaking changes require a new contract version and
a migration note.

Run the gate with `./gradlew :gateway:test --tests '*SchemaCompatibilityTest'`.
An extra required field without a default in the candidate `FrameSubmitted.avsc`
fails `publishedBaselineMustRemainReadableByCurrentSchemas`; the baseline is
left unchanged. The deliberate negative CI branch is separate from the final
passing implementation.

Timestamps use UTC epoch milliseconds and Avro `timestamp-millis`.
`captured_at` is client capture time; `received_at` is gateway acceptance time.
Bounding boxes use a top-left origin, x rightwards and y downwards; x/y/w/h are
fractions of decoded image width/height, in [0,1], with x+w and y+h at most 1.
Detection `inference_ms` is elapsed time for its frame's inference, repeated on
each detection from that inference. An empty detection list remains valid.

Telemetry pose x/y are metres in a configured robot-local map, and heading is
counter-clockwise-positive yaw in radians. Battery is a percentage [0,100],
speed is a non-negative magnitude in metres/second, and network quality is a
dimensionless fraction [0,1]. These are implementation conventions, not claims
about device-native payloads. A future map/coordinate-frame discriminator may
be added compatibly. Optional fields use a null-first union and explicit null
default. Required measurements have no fabricated zero defaults.

Avro enforces structural types, not numeric ranges, valid images, timestamps or
privacy. The producing application owns those checks. Raw image bytes never
appear in Kafka contracts: `object_key` and `sha256` form a claim-check. Optional
`operator_note` may contain PII and must be treated as raw transient data until
the planned privacy stage redacts it. Do not send it to the heartbeat topic.
