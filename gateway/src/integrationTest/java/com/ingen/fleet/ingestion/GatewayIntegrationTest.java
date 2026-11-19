package com.ingen.fleet.ingestion;

import com.github.dockerjava.api.model.ExposedPort;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ingen.fleet.contracts.AvroEventCodec;
import com.ingen.fleet.contracts.EventContracts;
import com.ingen.fleet.contracts.SchemaRegistryClient;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.StatObjectArgs;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.imageio.ImageIO;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** HTTP acceptance against actual isolated Kafka, Apicurio, MinIO and PostgreSQL containers. */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
    "fleet.ingestion.enabled=true",
    "fleet.ingestion.max-image-bytes=1048576",
    "fleet.ingestion.rate-per-second=0.01",
    "fleet.ingestion.rate-burst=2",
    "spring.servlet.multipart.max-file-size=1MB",
    "spring.servlet.multipart.max-request-size=2MB"
})
class GatewayIntegrationTest {
    private static final String FRAMES = "fleet.frames.v1";
    private static final String TELEMETRY = "fleet.telemetry.v1";
    private static final String EVOLUTION = "fleet.contract-evolution.v1";
    private static final String BUCKET = "integration-frames";
    private static final String ACCESS_KEY = "it" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    private static final String SECRET_KEY = UUID.randomUUID().toString();
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5)).version(HttpClient.Version.HTTP_1_1).build();

    private static final Network NETWORK = Network.newNetwork();

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
        DockerImageName.parse("postgres:16.8"))
        .withNetwork(NETWORK).withNetworkAliases("postgres")
        .withDatabaseName("fleet_integration")
        .withUsername("integration")
        .withPassword(UUID.randomUUID().toString())
        .withInitScript("registry-db.sql");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.0"))
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        // Kafka 3.9 validates the controller's advertised address too; the single-node
        // test controller is local to this process, so bind it to routable localhost.
        .withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://localhost:9094");
    @Container
    static final GenericContainer<?> REGISTRY = new GenericContainer<>(
        DockerImageName.parse("apicurio/apicurio-registry-sql:2.6.8.Final"))
        .withNetwork(NETWORK).dependsOn(POSTGRES)
        .withEnv("REGISTRY_DATASOURCE_URL", "jdbc:postgresql://postgres:5432/registry")
        .withEnv("REGISTRY_DATASOURCE_USERNAME", POSTGRES.getUsername())
        .withEnv("REGISTRY_DATASOURCE_PASSWORD", POSTGRES.getPassword())
        .withEnv("REGISTRY_RULES_GLOBAL_COMPATIBILITY", "BACKWARD")
        .withExposedPorts(8080)
        .waitingFor(Wait.forHttp("/apis/registry/v2/system/info").forStatusCode(200))
        .withStartupTimeout(Duration.ofSeconds(90));
    @Container
    static final GenericContainer<?> MINIO = new GenericContainer<>(DockerImageName.parse(
        System.getProperty("fleet.test.minio-image", "ingen-minio:week2")))
        .withExposedPorts(9000)
        .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
        .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
        .withCommand("server", "/data")
        .waitingFor(Wait.forHttp("/minio/health/ready").forStatusCode(200));

    @LocalServerPort
    private int port;

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("fleet.kafka", KAFKA::getBootstrapServers);
        registry.add("fleet.registry", GatewayIntegrationTest::registryUrl);
        registry.add("fleet.ingestion.jdbc-url", POSTGRES::getJdbcUrl);
        registry.add("fleet.ingestion.username", POSTGRES::getUsername);
        registry.add("fleet.ingestion.password", POSTGRES::getPassword);
        registry.add("fleet.ingestion.minio-url", GatewayIntegrationTest::minioUrl);
        registry.add("fleet.ingestion.access-key", () -> ACCESS_KEY);
        registry.add("fleet.ingestion.secret-key", () -> SECRET_KEY);
        registry.add("fleet.ingestion.bucket", () -> BUCKET);
    }

    @BeforeAll
    static void topics() throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            var existing = admin.listTopics().names().get(15, TimeUnit.SECONDS);
            List<NewTopic> missing = List.of(FRAMES, TELEMETRY, EVOLUTION).stream()
                .filter(topic -> !existing.contains(topic))
                .map(topic -> new NewTopic(topic, 6, (short) 1)).toList();
            if (!missing.isEmpty()) { admin.createTopics(missing).all().get(15, TimeUnit.SECONDS); }
        }
    }

    @Test
    void validFrameStoresExactImageAndPublishesOneRegisteredAvroEvent() throws Exception {
        String robot = robot("valid");
        byte[] image = image(robot, "jpeg");
        ObjectNode metadata = metadata(robot);
        Response response = frame(image, "image/jpeg", metadata, "integration-valid-trace");
        assertEquals(201, response.status(), response.body());
        JsonNode result = response.json();
        assertEquals("SUBMITTED", result.path("status").asText());
        String eventId = result.path("event_id").asText();
        assertFalse(eventId.isBlank());
        String key = result.path("object_key").asText();
        String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(image));
        assertTrue(key.contains(digest), "Object key must be content-addressed by SHA-256");
        try (var stored = minio().getObject(GetObjectArgs.builder().bucket(BUCKET).object(key).build())) {
            assertArrayEquals(image, stored.readAllBytes());
        }
        assertEquals(image.length, minio().statObject(
            StatObjectArgs.builder().bucket(BUCKET).object(key).build()).size());
        List<ConsumerRecord<String, byte[]>> records = records(FRAMES, robot);
        assertEquals(1, records.size());
        GenericRecord event = decode(records.get(0), "FrameSubmitted");
        System.out.println("W02_PAYLOAD_MEASUREMENT " + JSON.writeValueAsString(Map.of(
            "fixture", "generated-24x16-jpeg", "image_bytes", image.length,
            "avro_wire_bytes", records.get(0).value().length,
            "schema_id", AvroEventCodec.schemaId(records.get(0).value()),
            "includes_five_byte_schema_prefix", true)));
        assertEquals(eventId, event.get("event_id").toString());
        assertEquals(robot, event.get("robot_id").toString());
        assertEquals("AIDO_ROVER", event.get("platform").toString());
        assertEquals(digest, event.get("sha256").toString());
        assertEquals(key, event.get("object_key").toString());
        assertEquals(image.length, ((Number) event.get("size_bytes")).longValue());
        assertEquals(24, event.get("width"));
        assertEquals(16, event.get("height"));
        assertEquals(Instant.parse(metadata.path("captured_at").asText()).toEpochMilli(),
            ((Number) event.get("captured_at")).longValue());
        assertTrue(((Number) event.get("received_at")).longValue()
            >= ((Number) event.get("captured_at")).longValue());
        assertNotNull(records.get(0).headers().lastHeader("trace_id"));
        assertEquals("integration-valid-trace", new String(
            records.get(0).headers().lastHeader("trace_id").value(), StandardCharsets.UTF_8));
        Response status = request("GET", "/v1/frames/" + eventId + "/status", null, null, null);
        assertEquals(200, status.status(), status.body());
        assertEquals(eventId, status.json().path("event_id").asText());
        assertEquals(key, status.json().path("object_key").asText());
        assertEquals("SUBMITTED", status.json().path("status").asText());
    }

    @Test
    void sequentialDuplicateReturnsOriginalIdWithoutSecondObjectOrKafkaRecord() throws Exception {
        String robot = robot("duplicate");
        byte[] image = image(robot, "png");
        ObjectNode metadata = metadata(robot);
        long before = objectCount();
        Response first = frame(image, "image/png", metadata, null);
        Response duplicate = frame(image, "image/png", metadata, null);
        assertEquals(201, first.status(), first.body());
        assertEquals(200, duplicate.status(), duplicate.body());
        assertEquals(first.json().path("event_id"), duplicate.json().path("event_id"));
        assertEquals(first.json().path("object_key"), duplicate.json().path("object_key"));
        assertEquals(before + 1, objectCount());
        assertEquals(1, records(FRAMES, robot).size(), "Consume topic to prove application deduplication");
    }

    @Test
    void concurrentDuplicateIsSerializedByDurableJournal() throws Exception {
        String robot = robot("concurrent");
        byte[] image = image(robot, "jpeg");
        ObjectNode metadata = metadata(robot);
        try (var pool = Executors.newFixedThreadPool(2)) {
            var first = pool.submit(() -> frame(image, "image/jpeg", metadata, null));
            var second = pool.submit(() -> frame(image, "image/jpeg", metadata, null));
            Response a = first.get(30, TimeUnit.SECONDS);
            Response b = second.get(30, TimeUnit.SECONDS);
            assertTrue(List.of(a.status(), b.status()).contains(201), a.body() + b.body());
            assertTrue(List.of(a.status(), b.status()).contains(200), a.body() + b.body());
            assertEquals(a.json().path("event_id"), b.json().path("event_id"));
            assertEquals(1, records(FRAMES, robot).size());
        }
    }

    @Test
    void sameImageFromDifferentRobotsHasDifferentEventsAndSharedContentObject() throws Exception {
        String firstRobot = robot("first");
        String secondRobot = robot("second");
        byte[] image = image(firstRobot, "png");
        long before = objectCount();
        Response a = frame(image, "image/png", metadata(firstRobot), null);
        Response b = frame(image, "image/png", metadata(secondRobot), null);
        assertEquals(201, a.status(), a.body());
        assertEquals(201, b.status(), b.body());
        assertNotEquals(a.json().path("event_id"), b.json().path("event_id"));
        assertEquals(a.json().path("object_key"), b.json().path("object_key"));
        assertEquals(before + 1, objectCount());
        assertEquals(1, records(FRAMES, firstRobot).size());
        assertEquals(1, records(FRAMES, secondRobot).size());
    }

    @Test
    void telemetryUsesRegisteredSchemaAndRobotPartitionKey() throws Exception {
        String robot = robot("telemetry");
        ObjectNode body = telemetry(robot);
        Response response = json("POST", "/v1/telemetry", body);
        assertEquals(201, response.status(), response.body());
        List<ConsumerRecord<String, byte[]>> records = records(TELEMETRY, robot);
        assertEquals(1, records.size());
        GenericRecord event = decode(records.get(0), "TelemetryReported");
        assertEquals(response.json().path("event_id").asText(), event.get("event_id").toString());
        assertEquals(robot, event.get("robot_id").toString());
        assertEquals(81.5, ((Number) event.get("battery")).doubleValue());
        assertEquals(0.9, ((Number) event.get("network_quality")).doubleValue());
        assertEquals("simulated patrol", event.get("operator_note").toString());
        GenericRecord pose = (GenericRecord) event.get("pose");
        assertEquals(1.0, ((Number) pose.get("x")).doubleValue());
    }

    @Test
    void corruptJpegAndMismatchedMimeCannotReachStorageOrKafka() throws Exception {
        String robot = robot("corrupt");
        long objects = objectCount();
        long events = records(FRAMES, null).size();
        assertError(frame("not a jpeg".getBytes(StandardCharsets.UTF_8), "image/jpeg", metadata(robot), null), 422);
        byte[] original = image(robot, "jpeg");
        byte[] truncated = Arrays.copyOf(original, original.length / 2 + 2);
        truncated[truncated.length - 2] = (byte) 0xff;
        truncated[truncated.length - 1] = (byte) 0xd9;
        assertError(frame(truncated, "image/jpeg", metadata(robot("truncated")), null), 422);
        assertError(frame(image(robot, "png"), "image/jpeg", metadata(robot("mime")), null), 422);
        assertError(frame(image(robot, "png"), "application/octet-stream", metadata(robot("type")), null), 422);
        assertEquals(objects, objectCount());
        assertEquals(events, records(FRAMES, null).size());
    }

    @Test
    void missingMetadataFieldsAndNaiveOrFutureTimestampsNeverWriteAnObjectOrEvent() throws Exception {
        long objects = objectCount();
        long events = records(FRAMES, null).size();
        for (String field : List.of("robot_id", "platform", "captured_at")) {
            String robot = robot("missing");
            ObjectNode body = metadata(robot);
            body.remove(field);
            assertError(frame(image(robot, "jpeg"), "image/jpeg", body, null), 422);
        }
        String naiveRobot = robot("naive");
        ObjectNode naive = metadata(naiveRobot).put("captured_at", "2026-01-01T12:00:00");
        assertError(frame(image(naiveRobot, "jpeg"), "image/jpeg", naive, null), 422);
        String futureRobot = robot("future");
        ObjectNode future = metadata(futureRobot).put("captured_at", Instant.now().plusSeconds(300).toString());
        assertError(frame(image(futureRobot, "jpeg"), "image/jpeg", future, null), 422);
        assertEquals(objects, objectCount());
        assertEquals(events, records(FRAMES, null).size());
    }

    @Test
    void offsetTimestampIsAcceptedAndConvertedToUtcEpochMilliseconds() throws Exception {
        String robot = robot("offset");
        ObjectNode body = metadata(robot).put("captured_at", "2026-01-01T08:00:00+08:00");
        Response response = frame(image(robot, "png"), "image/png", body, null);
        assertEquals(201, response.status(), response.body());
        GenericRecord event = decode(records(FRAMES, robot).get(0), "FrameSubmitted");
        assertEquals(Instant.parse("2026-01-01T00:00:00Z").toEpochMilli(),
            ((Number) event.get("captured_at")).longValue());
    }

    @Test
    void oversizedMultipartReturnsClean413WithoutPublishing() throws Exception {
        String robot = robot("large");
        long objects = objectCount();
        long events = records(FRAMES, null).size();
        Response response = frame(new byte[1_048_577], "image/jpeg", metadata(robot), "oversize-trace");
        assertError(response, 413);
        assertEquals("oversize-trace", response.json().path("trace_id").asText());
        assertEquals(objects, objectCount());
        assertEquals(events, records(FRAMES, null).size());
    }

    @Test
    void malformedAndMissingMultipartPartsUseTheSameErrorEnvelope() throws Exception {
        String robot = robot("multipart");
        byte[] image = image(robot, "jpeg");
        assertError(multipart(image, "image/jpeg", "{", true, true, null), 422);
        assertError(multipart(image, "image/jpeg", null, true, false, null), 422);
        assertError(multipart(image, "image/jpeg", metadata(robot).toString(), false, true, null), 422);
        assertEquals(0, records(FRAMES, robot).size());
    }

    @Test
    void telemetryValidationAndMalformedJsonNeverPublish() throws Exception {
        long events = records(TELEMETRY, null).size();
        ObjectNode missing = telemetry(robot("missing-battery"));
        missing.remove("battery");
        assertError(json("POST", "/v1/telemetry", missing), 422);
        assertError(json("POST", "/v1/telemetry", telemetry(robot("battery")).put("battery", 101)), 422);
        assertError(json("POST", "/v1/telemetry", telemetry(robot("network")).put("network_quality", 1.1)), 422);
        assertError(json("POST", "/v1/telemetry", telemetry(robot("speed")).put("speed", -1.0)), 422);
        assertError(request("POST", "/v1/telemetry", "application/json", "{".getBytes(StandardCharsets.UTF_8), null), 422);
        assertEquals(events, records(TELEMETRY, null).size());
    }

    @Test
    void rateLimitReturnsRetryAfterAndDoesNotPublishRejectedRequest() throws Exception {
        String robot = robot("rate");
        ObjectNode body = telemetry(robot);
        int accepted = 0;
        Response rejected = null;
        for (int attempt = 0; attempt < 12; attempt++) {
            Response response = json("POST", "/v1/telemetry", body);
            if (response.status() == 429) { rejected = response; break; }
            assertEquals(201, response.status(), response.body());
            accepted++;
        }
        assertNotNull(rejected, "Burst traffic must trip the per-robot limiter");
        assertError(rejected, 429);
        assertTrue(Integer.parseInt(rejected.header("Retry-After")) >= 1);
        assertEquals(accepted, records(TELEMETRY, robot).size());
        Response otherRobot = json("POST", "/v1/telemetry", telemetry(robot("unlimited-other")));
        assertEquals(201, otherRobot.status(), "A limited robot must not throttle a different robot");
    }

    @Test
    void frameworkAndNotFoundFailuresHaveTraceLinkedUniformEnvelope() throws Exception {
        assertError(request("GET", "/v1/frames/" + UUID.randomUUID() + "/status", null, null, "status-404"), 404);
        assertError(request("GET", "/v1/not-an-endpoint", null, null, "route-404"), 404);
        assertError(request("DELETE", "/v1/telemetry", null, null, "method-405"), 405);
        assertError(request("POST", "/v1/telemetry", "text/plain", "hello".getBytes(StandardCharsets.UTF_8), "type-415"), 415);
    }

    @Test
    void openApiExposesAllThreeEndpointsAndSwaggerUi() throws Exception {
        Response api = request("GET", "/v3/api-docs", null, null, null);
        assertEquals(200, api.status(), api.body());
        JsonNode paths = api.json().path("paths");
        assertTrue(paths.has("/v1/frames"));
        assertTrue(paths.has("/v1/telemetry"));
        assertTrue(paths.has("/v1/frames/{event_id}/status"));
        Response ui = request("GET", "/swagger-ui", null, null, null);
        assertTrue(ui.status() == 200 || ui.status() == 301 || ui.status() == 302, ui.body());
    }

    @Test
    void registryEnforcesBackwardForEveryRequiredContractAndRejectsBreakingChange() throws Exception {
        SchemaRegistryClient client = new SchemaRegistryClient(registryUrl(), Duration.ofSeconds(5));
        for (String name : EventContracts.NAMES) {
            String subject = EventContracts.subject(name);
            Response config = external("GET", registryUrl() + "/apis/ccompat/v7/config/" + subject, null);
            assertEquals(200, config.status(), config.body());
            assertEquals("BACKWARD", config.json().path("compatibilityLevel")
                .asText(config.json().path("compatibility").asText()));
            Response latest = external("GET", registryUrl() + "/apis/ccompat/v7/subjects/" + subject + "/versions/latest", null);
            assertEquals(200, latest.status(), latest.body());
            assertEquals(EventContracts.schema(name), new Schema.Parser().parse(latest.json().path("schema").asText()));
        }
        Schema original = EventContracts.schema("FrameSubmitted");
        String subject = "IntegrationBreakingChange-value";
        client.register(subject, original);
        ObjectNode breaking = (ObjectNode) JSON.readTree(original.toString());
        for (JsonNode field : breaking.path("fields")) {
            if (field.path("name").asText().equals("robot_id")) { ((ObjectNode) field).put("type", "int"); }
        }
        Response rejected = external("POST", registryUrl() + "/apis/ccompat/v7/subjects/" + subject + "/versions",
            Map.of("schemaType", "AVRO", "schema", breaking.toString()));
        assertEquals(409, rejected.status(), rejected.body());
        assertThrows(SchemaRegistryClient.RegistryException.class,
            () -> client.register(subject, new Schema.Parser().parse(breaking.toString())));
        Response versions = external("GET", registryUrl() + "/apis/ccompat/v7/subjects/" + subject + "/versions", null);
        assertEquals(1, versions.json().size(), "Rejected change must not create a registry version");
    }

    @Test
    void registeredSchemaIdsAndExistingEventRemainReadableAfterRegistryRestart() throws Exception {
        String robot = robot("registry-restart");
        Response response = frame(image(robot, "jpeg"), "image/jpeg", metadata(robot), null);
        assertEquals(201, response.status(), response.body());
        byte[] wireEvent = records(FRAMES, robot).get(0).value();
        int schemaId = AvroEventCodec.schemaId(wireEvent);
        SchemaRegistryClient client = new SchemaRegistryClient(registryUrl(), Duration.ofSeconds(5));
        Schema before = client.schemaById(schemaId);
        REGISTRY.getDockerClient().restartContainerCmd(REGISTRY.getContainerId()).withTimeout(5).exec();
        long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
        boolean ready = false;
        while (!ready && System.nanoTime() < deadline) {
            try {
                ready = external("GET", registryUrl() + "/apis/registry/v2/system/info", null).status() == 200;
            } catch (java.io.IOException unavailable) {
                // A bounded restart gap is expected; do not alter container identity or port mappings.
            }
            if (!ready) { TimeUnit.MILLISECONDS.sleep(200); }
        }
        assertTrue(ready, () -> "SQL-backed registry did not recover within 45 seconds: "
            + REGISTRY.getLogs().lines().filter(line -> line.contains("ERROR") || line.contains("Exception")
                || line.contains("No space")).limit(20).collect(java.util.stream.Collectors.joining("\n")));
        // Docker may allocate a different ephemeral host port after restarting the same container.
        SchemaRegistryClient recovered = new SchemaRegistryClient(registryUrl(), Duration.ofSeconds(5));
        assertEquals(before, recovered.schemaById(schemaId));
        GenericRecord after = AvroEventCodec.decode(wireEvent, recovered.schemaById(schemaId),
            EventContracts.schema("FrameSubmitted"));
        assertEquals(response.json().path("event_id").asText(), after.get("event_id").toString());
        Response config = external("GET", registryUrl() + "/apis/ccompat/v7/config/FrameSubmitted-value", null);
        assertEquals(200, config.status(), config.body());
        assertEquals("BACKWARD", config.json().path("compatibilityLevel")
            .asText(config.json().path("compatibility").asText()));
    }

    @Test
    void realRegistryAndKafkaDemonstrateV1ReaderConsumingOptionalV2Field() throws Exception {
        Schema v1 = EventContracts.schema("FrameSubmitted");
        Schema v2;
        try (var input = EventContracts.class.getResourceAsStream("/avro/evolution/FrameSubmitted.v2.avsc")) {
            assertNotNull(input, "Packaged optional-field evolution schema is required");
            v2 = new Schema.Parser().parse(input);
        }
        SchemaRegistryClient client = new SchemaRegistryClient(registryUrl(), Duration.ofSeconds(5));
        String subject = "IntegrationEvolution-value";
        client.register(subject, v1);
        int v2Id = client.register(subject, v2);
        String robot = robot("evolution");
        GenericRecord value = new GenericData.Record(v2);
        value.put("event_id", UUID.randomUUID().toString());
        value.put("robot_id", robot);
        value.put("platform", "AIDO_ROVER");
        value.put("captured_at", 1_700_000_000_000L);
        value.put("received_at", 1_700_000_000_001L);
        value.put("width", 24);
        value.put("height", 16);
        value.put("format", "JPEG");
        value.put("size_bytes", 100L);
        value.put("sha256", "a".repeat(64));
        value.put("object_key", "frames/" + "a".repeat(64));
        value.put("camera_id", "front");
        for (Schema.Field field : v2.getFields()) {
            if (value.get(field.name()) == null && field.hasDefaultValue()) {
                value.put(field.name(), GenericData.get().getDefaultValue(field));
            }
        }
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(), "acks", "all", "enable.idempotence", true),
                new StringSerializer(), new ByteArraySerializer())) {
            producer.send(new ProducerRecord<>(EVOLUTION, robot, AvroEventCodec.encode(v2Id, value)))
                .get(10, TimeUnit.SECONDS);
        }
        List<ConsumerRecord<String, byte[]>> records = records(EVOLUTION, robot);
        assertEquals(1, records.size());
        byte[] bytes = records.get(0).value();
        Schema actualWriter = client.schemaById(AvroEventCodec.schemaId(bytes));
        GenericRecord oldReader = AvroEventCodec.decode(bytes, actualWriter, v1);
        assertEquals(robot, oldReader.get("robot_id").toString());
        assertEquals(value.get("event_id"), oldReader.get("event_id").toString());
        assertEquals(v1, oldReader.getSchema());
        assertFalse(oldReader.getSchema().getFields().stream().anyMatch(field -> field.name().equals("camera_id")));
    }

    private static String registryUrl() {
        // Unlike Compose's fixed port, Docker can remap a Testcontainers ephemeral port on restart.
        // Inspect the current transport address instead of using Testcontainers' cached startup mapping.
        var bindings = REGISTRY.getDockerClient().inspectContainerCmd(REGISTRY.getContainerId()).exec()
            .getNetworkSettings().getPorts().getBindings().get(ExposedPort.tcp(8080));
        return "http://" + REGISTRY.getHost() + ":" + bindings[0].getHostPortSpec();
    }
    private static String minioUrl() { return "http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(9000); }
    private static MinioClient minio() {
        return MinioClient.builder().endpoint(minioUrl()).credentials(ACCESS_KEY, SECRET_KEY).build();
    }
    private static long objectCount() throws Exception {
        long count = 0;
        for (var object : minio().listObjects(ListObjectsArgs.builder().bucket(BUCKET).recursive(true).build())) {
            object.get();
            count++;
        }
        return count;
    }
    private static String robot(String prefix) { return prefix + "-" + UUID.randomUUID(); }
    private static ObjectNode metadata(String robot) {
        return JSON.createObjectNode().put("robot_id", robot).put("platform", "AIDO_ROVER")
            .put("captured_at", Instant.now().truncatedTo(ChronoUnit.MILLIS).minusSeconds(60).toString());
    }
    private static ObjectNode telemetry(String robot) {
        ObjectNode body = metadata(robot).put("battery", 81.5).put("speed", 0.25)
            .put("network_quality", 0.9).put("operator_note", "simulated patrol");
        body.putObject("pose").put("x", 1.0).put("y", -2.0).put("heading", 0.5);
        return body;
    }
    private static byte[] image(String seed, String format) throws Exception {
        BufferedImage image = new BufferedImage(24, 16, BufferedImage.TYPE_INT_RGB);
        int hash = seed.hashCode();
        for (int y = 0; y < image.getHeight(); y++) {
            for (int x = 0; x < image.getWidth(); x++) { image.setRGB(x, y, hash ^ (x * 13259) ^ (y * 4219)); }
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        assertTrue(ImageIO.write(image, format, output));
        return output.toByteArray();
    }
    private Response frame(byte[] image, String mime, ObjectNode metadata, String trace) throws Exception {
        return multipart(image, mime, metadata.toString(), true, true, trace);
    }
    private Response multipart(byte[] image, String mime, String metadata,
            boolean withImage, boolean withMetadata, String trace) throws Exception {
        String boundary = "integration-" + UUID.randomUUID();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        if (withImage) {
            bytes.write(("--" + boundary + "\r\nContent-Disposition: form-data; name=\"image\"; filename=\"fixture\"\r\n"
                + "Content-Type: " + mime + "\r\n\r\n").getBytes(StandardCharsets.UTF_8));
            bytes.write(image);
            bytes.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        if (withMetadata) {
            bytes.write(("--" + boundary + "\r\nContent-Disposition: form-data; name=\"metadata\"\r\n"
                + "Content-Type: application/json\r\n\r\n" + metadata + "\r\n").getBytes(StandardCharsets.UTF_8));
        }
        bytes.write(("--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
        return request("POST", "/v1/frames", "multipart/form-data; boundary=" + boundary, bytes.toByteArray(), trace);
    }
    private Response json(String method, String path, ObjectNode json) throws Exception {
        return request(method, path, "application/json", JSON.writeValueAsBytes(json), null);
    }
    private Response request(String method, String path, String mime, byte[] body, String trace) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
            .timeout(Duration.ofSeconds(30));
        if (mime != null) { request.header("Content-Type", mime); }
        if (trace != null) { request.header("X-Trace-Id", trace); }
        request.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(body));
        HttpResponse<String> response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
        return new Response(response.statusCode(), response.body(), response.headers().map());
    }
    private static Response external(String method, String url, Object body) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10));
        if (body != null) { request.header("Content-Type", "application/vnd.schemaregistry.v1+json"); }
        request.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body)));
        HttpResponse<String> response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
        return new Response(response.statusCode(), response.body(), response.headers().map());
    }
    private static void assertError(Response response, int status) throws Exception {
        assertEquals(status, response.status(), response.body());
        JsonNode body = response.json();
        for (String key : List.of("code", "message", "trace_id", "details")) {
            assertTrue(body.has(key), "Missing error field " + key + ": " + response.body());
        }
        assertFalse(body.path("code").asText().isBlank());
        assertFalse(body.path("message").asText().isBlank());
        assertFalse(body.path("trace_id").asText().isBlank());
        assertEquals(response.header("X-Trace-Id"), body.path("trace_id").asText());
        assertFalse(body.has("exception"));
        assertFalse(body.has("stackTrace"));
    }
    private static GenericRecord decode(ConsumerRecord<String, byte[]> record, String type) {
        SchemaRegistryClient registry = new SchemaRegistryClient(registryUrl(), Duration.ofSeconds(5));
        return AvroEventCodec.decode(record.value(), registry.schemaById(AvroEventCodec.schemaId(record.value())),
            EventContracts.schema(type));
    }
    /** Read through a fixed broker high-water mark after HTTP completed; do not stop at the first match. */
    private static List<ConsumerRecord<String, byte[]>> records(String topic, String robot) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        properties.put("enable.auto.commit", false);
        properties.put("auto.offset.reset", "earliest");
        properties.put("default.api.timeout.ms", 10000);
        List<ConsumerRecord<String, byte[]>> result = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties,
                new StringDeserializer(), new ByteArrayDeserializer())) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                .map(info -> new TopicPartition(topic, info.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (partitions.stream().anyMatch(partition -> consumer.position(partition) < ends.get(partition))) {
                assertTrue(System.nanoTime() < deadline, "Did not consume fixed topic high-water marks");
                for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.offset() < ends.get(new TopicPartition(topic, record.partition()))
                            && (robot == null || robot.equals(record.key()))) { result.add(record); }
                }
            }
        }
        return result;
    }
    private record Response(int status, String body, Map<String, List<String>> headers) {
        JsonNode json() throws Exception { return JSON.readTree(body); }
        String header(String name) {
            return headers.entrySet().stream().filter(entry -> entry.getKey().equalsIgnoreCase(name))
                .flatMap(entry -> entry.getValue().stream()).findFirst().orElse("");
        }
    }
}
