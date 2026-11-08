# Verification scope and recorded results

## Source baseline

The runtime evidence below applies to source commit
[7a492fede7a0662ebec2a9c308b2fa60908a085c](https://github.com/CpfPatrick/ingen-week1/commit/7a492fede7a0662ebec2a9c308b2fa60908a085c).
The documentation/layout repair does not change the original 42 code, configuration,
build and test files. Results from the baseline are recorded evidence, not a claim that
every runtime check was repeated for a documentation-only update.

## Recorded checks

- Local Java 21 clean Gradle build: 29 JUnit cases passed, with configured Checkstyle.
  Checkstyle uses three configured rules, not an exhaustive formatting standard.
- Local Python 3.11: locked dependency installation, Ruff, strict mypy and 38 pytest cases
  passed.
- Local seven-service Docker acceptance: 142 recorded assertions passed, including
  actual consumer-group membership, dependency outages and recovery, independent
  readiness/liveness, trace-linked logging and scoped cleanup. Repeated probes are
  included in that count; it is not 142 independent tests.
- [Baseline GitHub Actions run](https://github.com/CpfPatrick/ingen-week1/actions/runs/42456505757):
  Java and Python jobs succeeded for the exact source commit above.

[Current workflow runs](https://github.com/CpfPatrick/ingen-week1/actions/workflows/ci.yml)
show each commit's separate CI outcome. A later commit is not declared green merely
because the baseline passed.

## Coverage limits

GitHub Actions runs Java build/JUnit/Checkstyle and Python Ruff/mypy/pytest. It does not
run Docker acceptance. Local Docker results must therefore remain separate from CI.

The current application is a synthetic-data skeleton. Production Avro interoperability,
frame ingestion, model inference, privacy processing, clean persistence, query/assistant
features, multi-broker resilience and production performance benchmarks are not
implemented or proved by these checks. Proposed privacy and retention settings are not
approved operating policy. No real media is approved by this verification record.
