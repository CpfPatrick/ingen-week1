package com.ingen.fleet.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.Schema;

/** Apicurio 2.x Confluent compatibility API, with verified per-subject BACKWARD enforcement. */
public final class SchemaRegistryClient {
    private static final int MAX_RESPONSE_BYTES = 1_048_576;
    private final URI base;
    private final Duration timeout;
    private final HttpClient http;
    private final ObjectMapper json = new ObjectMapper();

    public SchemaRegistryClient(String registryBaseUrl, Duration timeout) {
        URI origin;
        try { origin = URI.create(registryBaseUrl); }
        catch (RuntimeException ex) { throw new IllegalArgumentException("Invalid registry URL"); }
        if (origin.getHost() == null || origin.getUserInfo() != null
                || origin.getQuery() != null || origin.getFragment() != null
                || !("http".equals(origin.getScheme()) || "https".equals(origin.getScheme()))
                || !(origin.getPath().isEmpty() || "/".equals(origin.getPath()))) {
            throw new IllegalArgumentException("Registry URL must be an HTTP(S) origin without credentials");
        }
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Registry timeout must be positive");
        }
        this.base = origin.resolve("/apis/ccompat/v7/");
        this.timeout = timeout;
        this.http = HttpClient.newBuilder().connectTimeout(timeout)
            .followRedirects(HttpClient.Redirect.NEVER).build();
    }

    public Map<String, Integer> registerAll() {
        Map<String, Integer> result = new LinkedHashMap<>();
        for (String name : EventContracts.NAMES) {
            result.put(name, register(EventContracts.subject(name), EventContracts.schema(name)));
        }
        return Map.copyOf(result);
    }

    /** Returns an ID only after verifying the stored schema and the active compatibility setting. */
    public synchronized int register(String subject, Schema schema) {
        if (subject == null || !subject.matches("[A-Za-z0-9._-]{1,200}") || schema == null) {
            throw new IllegalArgumentException("Valid subject and schema are required");
        }
        String subjectPath = "subjects/" + subject;
        Response latest = send("GET", subjectPath + "/versions/latest", null);
        boolean existing = latest.status == 200;
        if (!existing && latest.status != 404) { fail("read latest version", latest.status); }

        Map<String, String> body = Map.of("schemaType", "AVRO", "schema", schema.toString());
        if (existing) {
            enforceBackward(subject);
            Response compatible = send("POST", "compatibility/" + subjectPath + "/versions/latest", body);
            requireSuccess("check compatibility", compatible);
            if (!compatible.body.path("is_compatible").isBoolean()
                    || !compatible.body.path("is_compatible").booleanValue()) {
                throw new RegistryException("Registry rejected incompatible schema");
            }
        }
        Response registered = send("POST", subjectPath + "/versions", body);
        requireSuccess("register schema", registered);
        JsonNode id = registered.body.path("id");
        if (!id.isIntegralNumber() || !id.canConvertToInt() || id.intValue() <= 0) {
            throw new RegistryException("Registry returned an invalid schema ID");
        }

        // Apicurio may require a subject to exist before attaching a subject-level rule.
        // No ID is exposed to a producer until this succeeds, including first registration.
        enforceBackward(subject);
        Schema stored = schemaById(id.intValue());
        if (!stored.equals(schema)) { throw new RegistryException("Registered schema does not match contract"); }
        return id.intValue();
    }

    public Schema schemaById(int id) {
        if (id <= 0) { throw new IllegalArgumentException("Schema ID must be positive"); }
        Response response = send("GET", "schemas/ids/" + id, null);
        requireSuccess("read registered schema", response);
        JsonNode definition = response.body.path("schema");
        if (!definition.isTextual()) { throw new RegistryException("Registry omitted schema definition"); }
        try { return new Schema.Parser().parse(definition.textValue()); }
        catch (RuntimeException ex) { throw new RegistryException("Registry returned an invalid schema"); }
    }

    private void enforceBackward(String subject) {
        Response update = send("PUT", "config/" + subject, Map.of("compatibility", "BACKWARD"));
        requireSuccess("configure compatibility", update);
        Response config = send("GET", "config/" + subject, null);
        requireSuccess("verify compatibility", config);
        String level = config.body.path("compatibilityLevel").asText(
            config.body.path("compatibility").asText(""));
        if (!"BACKWARD".equals(level)) { throw new RegistryException("Registry did not enforce BACKWARD compatibility"); }
    }

    private Response send(String method, String relativePath, Object body) {
        try {
            HttpRequest.Builder request = HttpRequest.newBuilder(base.resolve(relativePath)).timeout(timeout)
                .header("Accept", "application/vnd.schemaregistry.v1+json");
            if (body == null) { request.method(method, HttpRequest.BodyPublishers.noBody()); }
            else {
                request.header("Content-Type", "application/vnd.schemaregistry.v1+json")
                    .method(method, HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)));
            }
            // The total deadline covers the response body as well as the headers.
            // An InputStream body handler would leave later reads outside HttpRequest.timeout.
            var pending = http.sendAsync(request.build(), HttpResponse.BodyHandlers.ofByteArray());
            try {
                HttpResponse<byte[]> response = pending.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                byte[] bytes = response.body();
                if (bytes.length > MAX_RESPONSE_BYTES) { throw new RegistryException("Registry response too large"); }
                JsonNode parsed = bytes.length == 0 ? json.createObjectNode()
                    : json.readTree(new String(bytes, StandardCharsets.UTF_8));
                if (parsed == null || !parsed.isObject()) {
                    throw new RegistryException("Registry returned a non-object response");
                }
                return new Response(response.statusCode(), parsed);
            } finally {
                if (!pending.isDone()) { pending.cancel(true); }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RegistryException("Registry operation interrupted");
        } catch (IOException | IllegalArgumentException | ExecutionException | TimeoutException ex) {
            // Do not propagate response bodies, supplied URLs or underlying network diagnostics.
            throw new RegistryException("Registry is unavailable or returned an invalid response");
        }
    }

    private static void requireSuccess(String action, Response response) {
        if (response.status < 200 || response.status >= 300) { fail(action, response.status); }
    }

    private static void fail(String action, int status) {
        throw new RegistryException("Unable to " + action + " (HTTP " + status + ")");
    }

    private record Response(int status, JsonNode body) { }

    public static final class RegistryException extends RuntimeException {
        public RegistryException(String message) { super(message); }
    }
}
