package com.ingen.fleet.ingestion;

import java.time.Instant;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

public final class ApiModels {
    private ApiModels() { }

    @Schema(name = "FrameMetadata")
    public record FrameMetadata(
            @Schema(requiredMode = Schema.RequiredMode.REQUIRED, example = "rover-001")
            @JsonProperty("robot_id") String robotId,
            @Schema(requiredMode = Schema.RequiredMode.REQUIRED, example = "AIDO_ROVER")
            String platform,
            @Schema(requiredMode = Schema.RequiredMode.REQUIRED, format = "date-time",
                    description = "Offset-aware ISO-8601; no future values; normalized to UTC milliseconds")
            @JsonProperty("captured_at") String capturedAt) { }

    @Schema(name = "TelemetryRequest")
    public record TelemetryRequest(
            @Schema(requiredMode = Schema.RequiredMode.REQUIRED, example = "rover-001")
            @JsonProperty("robot_id") String robotId,
            @Schema(requiredMode = Schema.RequiredMode.REQUIRED, example = "AIDO_ROVER")
            String platform,
            @Schema(requiredMode = Schema.RequiredMode.REQUIRED, format = "date-time")
            @JsonProperty("captured_at") String capturedAt,
            @Schema(requiredMode = Schema.RequiredMode.REQUIRED, minimum = "0", maximum = "100")
            Double battery,
            @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
            Pose pose,
            @Schema(requiredMode = Schema.RequiredMode.REQUIRED, minimum = "0", description = "Speed in metres/second")
            Double speed,
            @Schema(requiredMode = Schema.RequiredMode.REQUIRED, minimum = "0", maximum = "1")
            @JsonProperty("network_quality") Double networkQuality,
            @Schema(nullable = true, maxLength = 1024, description = "Demo text only; never logged by the gateway")
            @JsonProperty("operator_note") String operatorNote) { }

    @Schema(name = "LocalMapPose", description = "x/y in local-map metres; heading in radians")
    public record Pose(
            @Schema(requiredMode = Schema.RequiredMode.REQUIRED) Double x,
            @Schema(requiredMode = Schema.RequiredMode.REQUIRED) Double y,
            @Schema(requiredMode = Schema.RequiredMode.REQUIRED,
                    minimum = "-3.141592653589793", maximum = "3.141592653589793") Double heading) { }

    @Schema(name = "FrameResponse")
    public record FrameResponse(
            @JsonProperty("event_id") String eventId,
            @JsonProperty("object_key") String objectKey,
            String status) { }

    @Schema(name = "TelemetryResponse")
    public record TelemetryResponse(@JsonProperty("event_id") String eventId, String status) { }

    @Schema(name = "FrameStatus")
    public record FrameStatus(
            @JsonProperty("event_id") String eventId,
            @JsonProperty("object_key") String objectKey,
            String status,
            @JsonProperty("captured_at") Instant capturedAt,
            @JsonProperty("received_at") Instant receivedAt) { }

    @Schema(name = "ErrorEnvelope")
    public record ErrorEnvelope(String code, String message,
            @JsonProperty("trace_id") String traceId, Map<String, Object> details) { }
}
