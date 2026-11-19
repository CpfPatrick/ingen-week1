package com.ingen.fleet.ingestion;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import javax.imageio.ImageIO;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockMultipartFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequestValidatorTest {
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-18T12:00:00Z"), ZoneOffset.UTC);
    private final RequestValidator validator = new RequestValidator(config(1024 * 1024, 100000, 512), CLOCK);

    static IngestionProperties config(long maxBytes, long maxPixels, int maxDimension) {
        return new IngestionProperties(true, maxBytes, maxPixels, maxDimension, 10, 20, 100,
                "jdbc:postgresql://localhost/fleet", "fleet", "unused", "http://localhost:9000",
                "unused", "unused", "frames");
    }

    static byte[] image(String format, int width, int height) throws Exception {
        var output = new ByteArrayOutputStream();
        ImageIO.write(new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB), format, output);
        return output.toByteArray();
    }

    private ApiModels.FrameMetadata metadata(String robot, String captured) {
        return new ApiModels.FrameMetadata(robot, "AIDO_ROVER", captured);
    }

    @Test void imageMetadataComesFromDecodedBytesAndOffsetsNormalize() throws Exception {
        var file = new MockMultipartFile("image", "irrelevant.png", "image/jpeg", image("jpg", 40, 20));
        var frame = validator.frame(file, metadata("robot-1", "2026-11-18T03:00:00-08:00"));
        assertEquals(40, frame.width());
        assertEquals(20, frame.height());
        assertEquals("JPEG", frame.format());
        assertEquals(Instant.parse("2026-11-18T11:00:00Z"), frame.capturedAt());
        assertEquals(64, frame.sha256().length());
        assertTrue(frame.objectKey().endsWith(frame.sha256() + ".jpg"));
    }

    @Test void idempotentIdentityUsesRobotHashAndCanonicalTimestamp() throws Exception {
        var file = new MockMultipartFile("image", "a.png", "image/png", image("png", 20, 20));
        var first = validator.frame(file, metadata("robot-1", "2026-11-18T11:00:00Z"));
        var same = validator.frame(file, metadata("robot-1", "2026-11-18T03:00:00-08:00"));
        var other = validator.frame(file, metadata("robot-2", "2026-11-18T11:00:00Z"));
        var later = validator.frame(file, metadata("robot-1", "2026-11-18T11:00:01Z"));
        assertEquals(first.eventId(), same.eventId());
        assertNotEquals(first.eventId(), other.eventId());
        assertNotEquals(first.eventId(), later.eventId());
        assertEquals(first.objectKey(), other.objectKey());
    }

    @Test void rejectsMislabeledContentBeforeStoringAnything() throws Exception {
        var file = new MockMultipartFile("image", "a.jpg", "image/jpeg", image("png", 20, 20));
        ApiException error = assertThrows(ApiException.class,
                () -> validator.frame(file, metadata("robot-1", "2026-11-18T11:00:00Z")));
        assertEquals(422, error.status());
        assertTrue(error.getMessage().contains("does not match"));
    }

    @Test void rejectsTruncatedJpegEvenWhenDecoderWouldTolerateIt() throws Exception {
        byte[] image = image("jpg", 20, 20);
        var file = new MockMultipartFile("image", "a.jpg", "image/jpeg",
                java.util.Arrays.copyOf(image, image.length - 2));
        assertEquals(422, assertThrows(ApiException.class,
                () -> validator.frame(file, metadata("robot-1", "2026-11-18T11:00:00Z"))).status());
    }

    @Test void rejectsTruncatedJpegWithForgedEndMarker() throws Exception {
        BufferedImage pixels = new BufferedImage(128, 128, BufferedImage.TYPE_INT_RGB);
        java.util.Random random = new java.util.Random(17);
        for (int y = 0; y < pixels.getHeight(); y++) {
            for (int x = 0; x < pixels.getWidth(); x++) { pixels.setRGB(x, y, random.nextInt()); }
        }
        var output = new ByteArrayOutputStream();
        ImageIO.write(pixels, "jpg", output);
        byte[] truncated = java.util.Arrays.copyOf(output.toByteArray(), output.size() / 2);
        truncated[truncated.length - 2] = (byte) 0xff;
        truncated[truncated.length - 1] = (byte) 0xd9;
        var file = new MockMultipartFile("image", "truncated.jpg", "image/jpeg", truncated);
        assertEquals(422, assertThrows(ApiException.class,
                () -> validator.frame(file, metadata("robot-1", "2026-11-18T11:00:00Z"))).status());
    }

    @Test void rejectsCorruptAndUnknownMime() {
        for (String type : new String[]{"image/jpeg", "application/octet-stream"}) {
            var file = new MockMultipartFile("image", "a.jpg", type, new byte[]{1, 2, 3});
            assertEquals(422, assertThrows(ApiException.class,
                    () -> validator.frame(file, metadata("robot-1", "2026-11-18T11:00:00Z"))).status());
        }
    }

    @Test void rejectsPngCrcDamageIgnoredByImageIo() throws Exception {
        byte[] png = image("png", 20, 20);
        png[29] ^= 1; // IHDR checksum; Java's PNG reader silently accepts this damaged file.
        var file = new MockMultipartFile("image", "bad-crc.png", "image/png", png);
        ApiException error = assertThrows(ApiException.class,
                () -> validator.frame(file, metadata("robot-1", "2026-11-18T11:00:00Z")));
        assertEquals(422, error.status());
        assertTrue(error.getMessage().contains("checksum"));
    }

    @Test void returns413ForOversizedValidImage() throws Exception {
        var tiny = new RequestValidator(config(8, 100000, 512), CLOCK);
        var file = new MockMultipartFile("image", "a.png", "image/png", image("png", 20, 20));
        assertEquals(413, assertThrows(ApiException.class,
                () -> tiny.frame(file, metadata("robot-1", "2026-11-18T11:00:00Z"))).status());
    }

    @Test void rejectsOversizedDecodedImageBeforePixelAllocation() throws Exception {
        var bounded = new RequestValidator(config(1024 * 1024, 100, 512), CLOCK);
        var file = new MockMultipartFile("image", "a.png", "image/png", image("png", 20, 20));
        assertEquals(422, assertThrows(ApiException.class,
                () -> bounded.frame(file, metadata("robot-1", "2026-11-18T11:00:00Z"))).status());
    }

    @Test void rejectsNaiveFutureAndMissingTimestamps() {
        for (String timestamp : new String[]{null, "", "2026-11-18T11:00:00", "2026-11-18T12:00:01Z",
            "-999999999-01-01T00:00:00Z"}) {
            assertEquals(422, assertThrows(ApiException.class, () -> validator.timestamp(timestamp)).status());
        }
        assertEquals(Instant.parse("2026-11-18T11:00:00.123Z"),
                validator.timestamp("2026-11-18T11:00:00.123456Z"));
    }

    @Test void rejectsUnsafeRobotAndUnknownPlatform() throws Exception {
        var file = new MockMultipartFile("image", "a.png", "image/png", image("png", 20, 20));
        assertThrows(ApiException.class,
                () -> validator.frame(file, metadata("../escape", "2026-11-18T11:00:00Z")));
        assertThrows(ApiException.class, () -> validator.frame(file,
                new ApiModels.FrameMetadata("r", "unknown", "2026-11-18T11:00:00Z")));
    }

    private ApiModels.TelemetryRequest telemetry(Double battery, Double speed, Double network,
            ApiModels.Pose pose, String note) {
        return new ApiModels.TelemetryRequest("robot-1", "AIDO_ROVER", "2026-11-18T11:00:00Z",
                battery, pose, speed, network, note);
    }

    @Test void validatesTelemetryRangesAndFiniteNumbers() {
        var pose = new ApiModels.Pose(0.0, 1.0, 0.2);
        assertEquals(Instant.parse("2026-11-18T11:00:00Z"),
                validator.telemetry(telemetry(80.0, 2.0, 0.7, pose, null)));
        for (var request : new ApiModels.TelemetryRequest[]{
            telemetry(null, 2.0, 0.7, pose, null), telemetry(101.0, 2.0, 0.7, pose, null),
            telemetry(80.0, -1.0, 0.7, pose, null), telemetry(80.0, 1.0, Double.NaN, pose, null),
            telemetry(80.0, 1.0, 0.7, null, null),
            telemetry(80.0, 1.0, 0.7, new ApiModels.Pose(Double.POSITIVE_INFINITY, 0.0, 0.0), null),
            telemetry(80.0, 1.0, 0.7, new ApiModels.Pose(0.0, 0.0, 4.0), null),
            telemetry(80.0, 1.0, 0.7, pose, "x".repeat(1025))}) {
            assertEquals(422, assertThrows(ApiException.class, () -> validator.telemetry(request)).status());
        }
    }
}
