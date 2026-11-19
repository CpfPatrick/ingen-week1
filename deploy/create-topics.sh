#!/usr/bin/env bash
set -euo pipefail
for spec in fleet.dev.heartbeat.v1:3600000 fleet.frames.v1:21600000 fleet.detections.v1:86400000 fleet.telemetry.v1:86400000 fleet.exceptions.v1:86400000; do
  topic="${spec%%:*}"
  retention="${spec##*:}"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 \
    --create --if-not-exists --topic "$topic" --partitions 6 \
    --replication-factor 1 --config "retention.ms=$retention"
done
