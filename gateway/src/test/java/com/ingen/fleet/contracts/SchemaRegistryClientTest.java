package com.ingen.fleet.contracts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaRegistryClientTest {
    private HttpServer server;
    private SchemaRegistryClient client;
    private final ObjectMapper json = new ObjectMapper();
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final AtomicBoolean existing = new AtomicBoolean();
    private boolean compatible = true;
    private String level = "BACKWARD";
    private int forcedStatus;
    private String forcedBody;
    private Schema stored;
    private boolean slowBody;

    @BeforeEach void start() throws Exception {
        stored = EventContracts.schema("FrameSubmitted");
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/apis/ccompat/v7/", exchange -> {
            String path = exchange.getRequestURI().getPath().substring("/apis/ccompat/v7/".length());
            String method = exchange.getRequestMethod();
            requests.add(method + " " + path);
            int status = 200;
            Object response = Map.of();
            if (forcedStatus != 0) { status = forcedStatus; }
            else if (method.equals("GET") && path.endsWith("/versions/latest")) {
                status = existing.get() ? 200 : 404;
            } else if (method.equals("POST") && path.startsWith("compatibility/")) {
                response = Map.of("is_compatible", compatible);
            } else if (method.equals("POST") && path.endsWith("/versions")) {
                existing.set(true);
                response = Map.of("id", 17);
            } else if (method.equals("PUT") && path.startsWith("config/")) {
                var body = json.readTree(exchange.getRequestBody());
                if (!"BACKWARD".equals(body.path("compatibility").asText())) { status = 422; }
                response = Map.of("compatibility", level);
            } else if (method.equals("GET") && path.startsWith("config/")) {
                response = Map.of("compatibilityLevel", level);
            } else if (method.equals("GET") && path.equals("schemas/ids/17")) {
                response = Map.of("schema", stored.toString());
            } else { status = 404; }
            byte[] bytes = (forcedBody == null ? json.writeValueAsString(response) : forcedBody)
                .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            if (slowBody) { LockSupport.parkNanos(Duration.ofSeconds(2).toNanos()); }
            try (var output = exchange.getResponseBody()) { output.write(bytes); }
        });
        server.start();
        client = new SchemaRegistryClient("http://127.0.0.1:" + server.getAddress().getPort(),
            Duration.ofSeconds(2));
    }

    @AfterEach void stop() { if (server != null) { server.stop(0); } }

    @Test void firstRegistrationVerifiesStoredSchemaAndBackwardBeforeReturning() {
        assertEquals(17, client.register("FrameSubmitted-value", stored));
        assertTrue(requests.contains("PUT config/FrameSubmitted-value"));
        assertTrue(requests.contains("GET config/FrameSubmitted-value"));
        assertTrue(requests.contains("GET schemas/ids/17"));
        assertFalse(requests.contains("POST compatibility/subjects/FrameSubmitted-value/versions/latest"));
    }

    @Test void existingSubjectUsesCompatibilityGateBeforeRegistering() {
        existing.set(true);
        assertEquals(17, client.register("FrameSubmitted-value", stored));
        int check = requests.indexOf("POST compatibility/subjects/FrameSubmitted-value/versions/latest");
        int write = requests.indexOf("POST subjects/FrameSubmitted-value/versions");
        assertTrue(check >= 0 && write > check);
    }

    @Test void incompatibleChangeNeverPostsANewVersion() {
        existing.set(true);
        compatible = false;
        assertThrows(SchemaRegistryClient.RegistryException.class,
            () -> client.register("FrameSubmitted-value", stored));
        assertFalse(requests.contains("POST subjects/FrameSubmitted-value/versions"));
    }

    @Test void wrongCompatibilitySettingAndWrongStoredSchemaFailClosed() {
        level = "NONE";
        assertThrows(SchemaRegistryClient.RegistryException.class,
            () -> client.register("FrameSubmitted-value", stored));
        level = "BACKWARD";
        Schema expected = stored;
        stored = EventContracts.schema("TelemetryReported");
        assertThrows(SchemaRegistryClient.RegistryException.class,
            () -> client.register("FrameSubmitted-value", expected));
    }

    @Test void malformedResponsesHttpFailureAndUnsafeSubjectsAreRejected() {
        forcedStatus = 503;
        assertThrows(SchemaRegistryClient.RegistryException.class,
            () -> client.register("FrameSubmitted-value", stored));
        forcedStatus = 200;
        forcedBody = "not json";
        assertThrows(SchemaRegistryClient.RegistryException.class,
            () -> client.register("FrameSubmitted-value", stored));
        forcedBody = "[]";
        assertThrows(SchemaRegistryClient.RegistryException.class,
            () -> client.register("FrameSubmitted-value", stored));
        assertThrows(IllegalArgumentException.class, () -> client.register("../config", stored));
        assertThrows(IllegalArgumentException.class, () -> client.schemaById(0));
    }

    @Test void rejectsInvalidConfigurationWithoutLeakingItsValue() {
        assertThrows(IllegalArgumentException.class,
            () -> new SchemaRegistryClient("file:///tmp/registry", Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class,
            () -> new SchemaRegistryClient("http://user:secret@localhost", Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class,
            () -> new SchemaRegistryClient("http://localhost", Duration.ZERO));
    }

    @Test void deadlineIncludesBodyAfterHeadersHaveArrived() {
        slowBody = true;
        var impatient = new SchemaRegistryClient(
            "http://127.0.0.1:" + server.getAddress().getPort(), Duration.ofMillis(250));
        assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
            assertThrows(SchemaRegistryClient.RegistryException.class,
                () -> impatient.register("FrameSubmitted-value", stored)));
        assertFalse(requests.isEmpty());
    }
}
