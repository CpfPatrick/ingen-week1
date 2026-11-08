# inGen Week 1

Java and Python development scaffold with a local Kafka-based stack and GitHub Actions checks. This repository uses synthetic fixtures only. It is not a production deployment or a completed inference/privacy pipeline.

## Components

- Java 21 Spring Boot gateway: liveness/readiness probes and structured logging.
- Java stream-processor placeholder.
- Python 3.11 heartbeat consumer with reconnect and safe logging tests.
- Local Docker Compose services for Kafka, Apicurio Registry, PostgreSQL, MinIO, the gateway, and the worker.
- Placeholder directories for privacy processing and Avro contracts.

## Run checks

Use JDK 21 and the included Gradle 8.12.1 wrapper:

```sh
./gradlew --no-daemon build
```

Use Python 3.11 and uv (CI uses 0.13.4):

```sh
uv sync --locked
uv run ruff check .
uv run mypy inference-worker tests scripts
uv run pytest
```

GitHub Actions runs the Java build, JUnit and configured Checkstyle checks, plus the locked Python installation, Ruff, strict mypy and pytest on pushes and pull requests. It does not run the Docker acceptance suite.

## Local stack

Install Docker with Compose, copy `.env.example` to `.env`, and replace its placeholder values with throwaway local-development values. Do not reuse real credentials.

```sh
docker compose up --build
```

Host ports bind to loopback. Kafka uses plaintext, and detailed gateway health information is enabled for development. Do not expose this stack publicly or use real personal, customer, or production data.

The MinIO Dockerfile builds a pinned official upstream source revision and includes its AGPL license in the resulting image. No built images are distributed by this repository. Third-party dependencies retain their own licenses; the Gradle wrapper retains its license and copyright notices.

## Scope

Only the reviewed implementation, configuration, synthetic tests and build instructions are included. Internal documents, original requirements, private links, local acceptance logs, generated reports, caches and real environment files are excluded.
