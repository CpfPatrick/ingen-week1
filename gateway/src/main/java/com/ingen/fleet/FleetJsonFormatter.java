package com.ingen.fleet;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.logging.structured.StructuredLogFormatter;

/**
 * Fail-closed log schema. Messages, arguments, logger names, exceptions and arbitrary MDC
 * or key-value fields never reach the output, including logs emitted by Spring/Kafka.
 * New application events must be explicitly added here with their field validators.
 */
public class FleetJsonFormatter implements StructuredLogFormatter<ILoggingEvent> {
    private static final Set<String> FAILURE_KINDS = Set.of("io", "servlet", "runtime", "error");
    private final ObjectMapper mapper = new ObjectMapper();
    @Override public String format(ILoggingEvent event) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("timestamp", event.getTimeStamp());
        fields.put("level", event.getLevel().toString());
        fields.put("service", "gateway");
        String trace = event.getMDCPropertyMap().get("trace_id");
        fields.put("trace_id", trace == null ? "startup"
            : trace.matches("[a-zA-Z0-9_-]{1,64}") ? trace : "invalid");
        String eventName = safeEvent(event);
        fields.put("event", eventName);
        if (eventName.equals("request_completed") || eventName.equals("request_failed")) {
            if (event.getKeyValuePairs() != null) {
                for (var pair : event.getKeyValuePairs()) {
                    if ("status".equals(pair.key) && pair.value instanceof Integer status
                            && status >= 100 && status <= 599) {
                        fields.put("status", status);
                    } else if (eventName.equals("request_failed") && "failure_kind".equals(pair.key)
                            && pair.value instanceof String kind && FAILURE_KINDS.contains(kind)) {
                        fields.put("failure_kind", kind);
                    }
                }
            }
        }
        try { return mapper.writeValueAsString(fields) + "\n"; }
        catch (JsonProcessingException ex) { throw new IllegalStateException("JSON log failure", ex); }
    }

    private static String safeEvent(ILoggingEvent event) {
        if (TraceFilter.class.getName().equals(event.getLoggerName())) {
            if ("request_completed".equals(event.getMessage())) { return "request_completed"; }
            if ("request_failed".equals(event.getMessage())) { return "request_failed"; }
        }
        if (("org.springframework.boot.SpringApplication".equals(event.getLoggerName())
                && "Application run failed".equals(event.getMessage()))
                || "org.springframework.boot.diagnostics.LoggingFailureAnalysisReporter"
                    .equals(event.getLoggerName())) {
            return "application_startup_failure";
        }
        return "framework_event";
    }
}
