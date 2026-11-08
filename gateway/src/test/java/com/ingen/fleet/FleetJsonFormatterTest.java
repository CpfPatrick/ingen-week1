package com.ingen.fleet;

import java.util.Map;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.event.KeyValuePair;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FleetJsonFormatterTest {
    private final FleetJsonFormatter formatter = new FleetJsonFormatter();
    private final ObjectMapper mapper = new ObjectMapper();

    @ParameterizedTest
    @CsvSource({
        "org.springframework.boot.SpringApplication,Application run failed,application_startup_failure",
        "org.springframework.boot.diagnostics.LoggingFailureAnalysisReporter,SECRET-binding-value,application_startup_failure",
        "org.springframework.boot.context.properties.bind.Binder,SECRET-property,framework_event",
        "org.apache.kafka.clients.admin.AdminClientConfig,SECRET-bootstrap-and-sasl,framework_event",
        "org.apache.kafka.common.utils.AppInfoParser,SECRET-config,framework_event",
        "SECRET-logger-name,request_failed,framework_event"
    })
    void thirdPartyMessagesAndThrowableDetailsNeverEscape(String logger, String message,
            String expectedEvent) throws Exception {
        var event = event(logger, message);
        event.setThrowableProxy(new ThrowableProxy(new IllegalArgumentException("SECRET-exception")));
        event.setMDCPropertyMap(Map.of("trace_id", "bad\nSECRET-trace", "password", "SECRET-mdc"));
        event.addKeyValuePair(new KeyValuePair("config", "SECRET-config"));
        event.addKeyValuePair(new KeyValuePair("status", 500));
        String output = formatter.format(event);
        JsonNode json = mapper.readTree(output);
        assertEquals(expectedEvent, json.path("event").asText());
        assertEquals("invalid", json.path("trace_id").asText());
        assertEquals(5, json.size());
        assertFalse(output.contains("SECRET"));
        assertTrue(output.endsWith("\n"));
        assertEquals(1, output.lines().count());
    }

    @Test void onlyValidatedFieldsFromKnownApplicationEventsAreSerialized() throws Exception {
        var event = event(TraceFilter.class.getName(), "request_failed");
        event.setMDCPropertyMap(Map.of("trace_id", "safe-trace_123", "token", "SECRET-mdc"));
        event.addKeyValuePair(new KeyValuePair("status", 503));
        event.addKeyValuePair(new KeyValuePair("failure_kind", "io"));
        event.addKeyValuePair(new KeyValuePair("uri", "SECRET-uri"));
        event.setThrowableProxy(new ThrowableProxy(new RuntimeException("SECRET-exception")));
        JsonNode json = mapper.readTree(formatter.format(event));
        assertEquals(7, json.size());
        assertEquals("gateway", json.path("service").asText());
        assertEquals("request_failed", json.path("event").asText());
        assertEquals("safe-trace_123", json.path("trace_id").asText());
        assertEquals(503, json.path("status").asInt());
        assertEquals("io", json.path("failure_kind").asText());
        assertFalse(json.toString().contains("SECRET"));
    }

    @Test void untrustedValuesAreNotStringifiedOrFormatted() throws Exception {
        Object untrusted = new Object() {
            @Override public String toString() { throw new AssertionError("must never stringify input"); }
        };
        var event = event(TraceFilter.class.getName(), "request_completed");
        event.setMDCPropertyMap(Map.of());
        event.setArgumentArray(new Object[]{untrusted});
        event.addKeyValuePair(new KeyValuePair("status", untrusted));
        event.addKeyValuePair(new KeyValuePair("failure_kind", untrusted));
        event.addKeyValuePair(new KeyValuePair("unknown", untrusted));
        JsonNode json = mapper.readTree(formatter.format(event));
        assertEquals("request_completed", json.path("event").asText());
        assertEquals("startup", json.path("trace_id").asText());
        assertEquals(5, json.size());
    }

    @Test void rejectsOutOfRangeAndStringStatusAndUnknownFailureKind() throws Exception {
        for (Object status : new Object[]{99, 600, "500", "SECRET-status", 500L}) {
            var event = event(TraceFilter.class.getName(), "request_failed");
            event.setMDCPropertyMap(Map.of());
            event.addKeyValuePair(new KeyValuePair("status", status));
            event.addKeyValuePair(new KeyValuePair("failure_kind", "SECRET-exception-class"));
            assertEquals(5, mapper.readTree(formatter.format(event)).size());
        }
    }

    @Test void neverRequestsFormattedMessageEvenForFrameworkErrors() throws Exception {
        var event = new LoggingEvent() {
            @Override public String getFormattedMessage() {
                throw new AssertionError("Raw framework messages must never be rendered");
            }
        };
        event.setLoggerName("org.springframework.boot.SpringApplication");
        event.setMessage("Application run failed");
        event.setLevel(Level.ERROR);
        event.setMDCPropertyMap(Map.of());
        assertEquals("application_startup_failure",
            mapper.readTree(formatter.format(event)).path("event").asText());
    }

    private static LoggingEvent event(String logger, String message) {
        var event = new LoggingEvent();
        event.setLoggerName(logger);
        event.setMessage(message);
        event.setLevel(Level.ERROR);
        event.setTimeStamp(123456789L);
        return event;
    }
}
