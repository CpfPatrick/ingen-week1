package com.ingen.fleet;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.ServletException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TraceFilterTest {
    private final Logger logger = (Logger) LoggerFactory.getLogger(TraceFilter.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>() {
        @Override protected void append(ILoggingEvent event) {
            event.prepareForDeferredProcessing();
            super.append(event);
        }
    };
    private Map<String, String> originalContext;
    private Level originalLevel;

    @BeforeEach void captureLogs() {
        originalContext = MDC.getCopyOfContextMap();
        MDC.clear();
        originalLevel = logger.getLevel();
        logger.setLevel(Level.INFO);
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach void restoreLogging() {
        logger.detachAppender(appender);
        appender.stop();
        logger.setLevel(originalLevel);
        if (originalContext == null) { MDC.clear(); }
        else { MDC.setContextMap(originalContext); }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void successfulRequestLogsSameTraceAndRestoresContext(boolean nested) throws Exception {
        Map<String, String> previous = seedContext(nested);
        var request = new MockHttpServletRequest();
        request.addHeader("X-Trace-Id", "test-123");
        var response = new MockHttpServletResponse();
        new TraceFilter().doFilter(request, response, (req, res) -> {
            assertEquals("test-123", MDC.get("trace_id"));
            response.setStatus(204);
            MDC.put("trace_id", "downstream-trace");
            MDC.put("downstream", "must-not-leak-to-next-request");
        });
        assertEquals("test-123", response.getHeader("X-Trace-Id"));
        JsonNode log = singleLog();
        assertEquals("request_completed", log.path("event").asText());
        assertEquals("test-123", log.path("trace_id").asText());
        assertEquals(204, log.path("status").asInt());
        assertEquals(previous, MDC.getCopyOfContextMap());
    }

    static Stream<Arguments> failures() {
        return Stream.of(false, true).flatMap(nested -> Stream.of(
            Arguments.of(nested, new IOException("SECRET-io"), "io"),
            Arguments.of(nested, new ServletException("SECRET-servlet"), "servlet"),
            Arguments.of(nested, new IllegalStateException("SECRET-runtime"), "runtime"),
            Arguments.of(nested, new AssertionError("SECRET-error"), "error")));
    }

    @ParameterizedTest
    @MethodSource("failures")
    void exceptionalRequestLogsAndRethrowsSameFailure(boolean nested, Throwable failure,
            String kind) throws Exception {
        Map<String, String> previous = seedContext(nested);
        var request = new MockHttpServletRequest("GET", "/SECRET-uri");
        request.addHeader("X-Trace-Id", "failure-trace");
        request.addHeader("Authorization", "SECRET-header");
        request.setContent("SECRET-body".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var response = new MockHttpServletResponse();
        Throwable observed = assertThrows(failure.getClass(), () ->
            new TraceFilter().doFilter(request, response, (req, res) -> {
                assertEquals("failure-trace", MDC.get("trace_id"));
                MDC.put("trace_id", "downstream-trace");
                MDC.put("downstream", "SECRET-context");
                if (failure instanceof IOException io) { throw io; }
                if (failure instanceof ServletException servlet) { throw servlet; }
                if (failure instanceof RuntimeException runtime) { throw runtime; }
                throw (Error) failure;
            }));
        assertSame(failure, observed);
        assertEquals("failure-trace", response.getHeader("X-Trace-Id"));
        JsonNode log = singleLog();
        assertEquals("request_failed", log.path("event").asText());
        assertEquals("ERROR", log.path("level").asText());
        assertEquals("failure-trace", log.path("trace_id").asText());
        assertEquals(kind, log.path("failure_kind").asText());
        assertEquals(200, log.path("status").asInt());
        assertFalse(log.toString().contains("SECRET"));
        assertNull(appender.list.get(0).getThrowableProxy());
        assertEquals(previous, MDC.getCopyOfContextMap());
    }

    @Test void replacesUnsafeTraceConsistently() throws Exception {
        var request = new MockHttpServletRequest();
        request.addHeader("X-Trace-Id", "SECRET\nunsafe-trace");
        var response = new MockHttpServletResponse();
        new TraceFilter().doFilter(request, response, (req, res) ->
            assertEquals(response.getHeader("X-Trace-Id"), MDC.get("trace_id")));
        String trace = response.getHeader("X-Trace-Id");
        assertTrue(trace.matches("[a-f0-9-]{36}"));
        assertEquals(trace, singleLog().path("trace_id").asText());
        assertNull(MDC.getCopyOfContextMap());
    }

    @Test void unavailableStatusStillLogsFailureAndPreservesOriginalException() throws Exception {
        var failure = new ServletException("SECRET-original");
        var request = new MockHttpServletRequest();
        request.addHeader("X-Trace-Id", "wrapper-failure-trace");
        var response = new MockHttpServletResponse() {
            @Override public int getStatus() { throw new IllegalStateException("SECRET-wrapper"); }
        };
        Throwable observed = assertThrows(ServletException.class, () ->
            new TraceFilter().doFilter(request, response,
                (req, res) -> { throw failure; }));
        assertSame(failure, observed);
        JsonNode log = singleLog();
        assertEquals("request_failed", log.path("event").asText());
        assertEquals("servlet", log.path("failure_kind").asText());
        assertEquals("wrapper-failure-trace", log.path("trace_id").asText());
        assertEquals(response.getHeader("X-Trace-Id"), log.path("trace_id").asText());
        assertFalse(log.has("status"));
        assertFalse(log.toString().contains("SECRET"));
        assertNull(MDC.getCopyOfContextMap());
    }

    private Map<String, String> seedContext(boolean nested) {
        if (nested) { MDC.setContextMap(Map.of("trace_id", "outer-trace", "outer", "preserved")); }
        return MDC.getCopyOfContextMap();
    }

    private JsonNode singleLog() throws Exception {
        assertEquals(1, appender.list.size());
        return new ObjectMapper().readTree(new FleetJsonFormatter().format(appender.list.get(0)));
    }
}
