package com.ingen.fleet.ingestion;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.web.multipart.MultipartFile;
import com.ingen.fleet.contracts.EventContracts;

public final class IngestionService {
    private final RequestValidator validator;
    private final RobotRateLimiter limiter;
    private final FrameJournal journal;
    private final FrameObjectStore objects;
    private final EventPublisher publisher;
    private final Clock clock;

    public IngestionService(RequestValidator validator, RobotRateLimiter limiter,
            FrameJournal journal, FrameObjectStore objects, EventPublisher publisher, Clock clock) {
        this.validator = validator;
        this.limiter = limiter;
        this.journal = journal;
        this.objects = objects;
        this.publisher = publisher;
        this.clock = clock;
    }

    public FrameJournal.Submission submitFrame(MultipartFile file, ApiModels.FrameMetadata metadata, String trace) {
        var frame = validator.frame(file, metadata);
        limiter.acquire(frame.robotId());
        return journal.submit(frame, clock.instant(), trace, entry -> {
            objects.putIfAbsent(frame);
            GenericRecord event = new GenericData.Record(EventContracts.schema("FrameSubmitted"));
            event.put("event_id", entry.eventId());
            event.put("robot_id", entry.robotId());
            event.put("platform", entry.platform());
            event.put("captured_at", entry.capturedAt().toEpochMilli());
            event.put("received_at", entry.receivedAt().toEpochMilli());
            event.put("width", entry.width());
            event.put("height", entry.height());
            event.put("format", entry.format());
            event.put("size_bytes", entry.sizeBytes());
            event.put("sha256", entry.sha256());
            event.put("object_key", entry.objectKey());
            event.put("trace_id", entry.traceId());
            publisher.publish(EventPublisher.FRAME_TOPIC, entry.robotId(), event, entry.traceId());
        });
    }

    public ApiModels.TelemetryResponse submitTelemetry(ApiModels.TelemetryRequest request, String trace) {
        Instant captured = validator.telemetry(request);
        limiter.acquire(request.robotId());
        GenericRecord event = new GenericData.Record(EventContracts.schema("TelemetryReported"));
        String eventId = UUID.randomUUID().toString();
        event.put("event_id", eventId);
        event.put("robot_id", request.robotId());
        event.put("platform", request.platform());
        event.put("captured_at", captured.toEpochMilli());
        event.put("received_at", clock.instant().toEpochMilli());
        event.put("battery", request.battery());
        GenericRecord pose = new GenericData.Record(event.getSchema().getField("pose").schema());
        pose.put("x", request.pose().x());
        pose.put("y", request.pose().y());
        pose.put("heading", request.pose().heading());
        event.put("pose", pose);
        event.put("speed", request.speed());
        event.put("network_quality", request.networkQuality());
        event.put("operator_note", request.operatorNote());
        event.put("trace_id", trace);
        publisher.publish(EventPublisher.TELEMETRY_TOPIC, request.robotId(), event, trace);
        return new ApiModels.TelemetryResponse(eventId, "SUBMITTED");
    }

    public ApiModels.FrameStatus status(String eventId) {
        try {
            if (!UUID.fromString(eventId).toString().equals(eventId)) {
                throw new IllegalArgumentException("Invalid event ID");
            }
        } catch (IllegalArgumentException invalid) {
            throw new ApiException(404, "FRAME_NOT_FOUND", "No frame submission exists for this event_id.");
        }
        return journal.find(eventId).orElseThrow(() -> new ApiException(404, "FRAME_NOT_FOUND",
                "No frame submission exists for this event_id.")).response();
    }
}
