package com.ingen.fleet.ingestion;

import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

@RestController
@RequestMapping("/v1")
@ConditionalOnProperty(prefix = "fleet.ingestion", name = "enabled", havingValue = "true")
public class IngestionController {
    private final IngestionService service;

    public IngestionController(IngestionService service) { this.service = service; }

    @Operation(summary = "Submit a JPEG or PNG frame", description = "Multipart image plus application/json metadata. "
            + "Repeated robot_id + SHA-256 + captured_at returns its durable original identity. "
            + "Successful ingestion is distinct from inference completion.")
    @ApiResponses({@ApiResponse(responseCode = "201", description = "Frame stored and Kafka acknowledged"),
        @ApiResponse(responseCode = "200", description = "Existing acknowledged submission"),
        @ApiResponse(responseCode = "413", description = "Upload too large"),
        @ApiResponse(responseCode = "422", description = "Invalid metadata or image"),
        @ApiResponse(responseCode = "429", description = "Rate limited; inspect Retry-After"),
        @ApiResponse(responseCode = "503", description = "Dependency unavailable; retry same frame")})
    @PostMapping(path = "/frames", consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ApiModels.FrameResponse> frame(@RequestPart("image") MultipartFile image,
            @RequestPart("metadata") ApiModels.FrameMetadata metadata) {
        var result = service.submitFrame(image, metadata, MDC.get("trace_id"));
        var entry = result.entry();
        return ResponseEntity.status(result.duplicate() ? 200 : 201)
                .body(new ApiModels.FrameResponse(entry.eventId(), entry.objectKey(), entry.status()));
    }

    @Operation(summary = "Submit telemetry", description = "battery percent; local map pose x/y metres and "
            + "heading radians in [-pi,pi]; speed m/s; network_quality normalized [0,1]. "
            + "Each accepted request has a fresh event_id; HTTP telemetry resubmission is at-least-once.")
    @ApiResponses({@ApiResponse(responseCode = "201", description = "Kafka acknowledged telemetry"),
        @ApiResponse(responseCode = "422", description = "Invalid telemetry"),
        @ApiResponse(responseCode = "429", description = "Rate limited; inspect Retry-After"),
        @ApiResponse(responseCode = "503", description = "Dependency unavailable")})
    @PostMapping(path = "/telemetry", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ApiModels.TelemetryResponse> telemetry(@RequestBody ApiModels.TelemetryRequest request) {
        return ResponseEntity.status(201).body(service.submitTelemetry(request, MDC.get("trace_id")));
    }

    @Operation(summary = "Get ingestion status", description = "PENDING means ingestion must be retried with "
            + "the same frame. SUBMITTED means Kafka acknowledged the frame, not that inference completed.")
    @ApiResponses({@ApiResponse(responseCode = "200", description = "Durable frame ingestion status"),
        @ApiResponse(responseCode = "404", description = "Unknown frame event_id"),
        @ApiResponse(responseCode = "503", description = "Journal unavailable")})
    @GetMapping(path = "/frames/{event_id}/status", produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiModels.FrameStatus status(@PathVariable("event_id") String eventId) {
        return service.status(eventId);
    }
}
