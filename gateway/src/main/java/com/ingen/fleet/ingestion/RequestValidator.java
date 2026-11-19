package com.ingen.fleet.ingestion;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.MemoryCacheImageInputStream;
import org.springframework.web.multipart.MultipartFile;

public final class RequestValidator {
    public static final Set<String> PLATFORMS = Set.of("AIDO_ROVER", "FARI",
            "SENTINEL_PRIME_AI", "AIDO_HUMANOID", "SENPAI");
    private final IngestionProperties limits;
    private final Clock clock;

    public RequestValidator(IngestionProperties limits, Clock clock) {
        this.limits = limits;
        this.clock = clock;
    }

    public record ValidatedFrame(String eventId, String robotId, String platform,
            Instant capturedAt, int width, int height, String format, String sha256,
            String objectKey, byte[] image) { }

    public ValidatedFrame frame(MultipartFile image, ApiModels.FrameMetadata metadata) {
        if (metadata == null) { throw ApiException.invalid("metadata", "metadata is required."); }
        robot(metadata.robotId());
        platform(metadata.platform());
        Instant captured = timestamp(metadata.capturedAt());
        if (image == null || image.isEmpty()) {
            throw ApiException.invalid("image", "A non-empty image is required.");
        }
        if (image.getSize() > limits.maxImageBytes()) {
            throw new ApiException(413, "UPLOAD_TOO_LARGE", "Image exceeds the configured byte limit.");
        }
        String contentType = image.getContentType();
        String expected = "image/jpeg".equalsIgnoreCase(contentType) ? "JPEG"
                : "image/png".equalsIgnoreCase(contentType) ? "PNG" : null;
        if (expected == null) {
            throw ApiException.invalid("image", "Image content type must be image/jpeg or image/png.");
        }
        byte[] bytes;
        try (var stream = image.getInputStream()) {
            bytes = stream.readNBytes(Math.toIntExact(limits.maxImageBytes() + 1));
        } catch (IOException | ArithmeticException failure) {
            throw ApiException.invalid("image", "Image content could not be read.");
        }
        if (bytes.length > limits.maxImageBytes()) {
            throw new ApiException(413, "UPLOAD_TOO_LARGE", "Image exceeds the configured byte limit.");
        }
        int[] dimensions = decode(bytes, expected);
        String hash = sha256(bytes);
        String eventId = UUID.nameUUIDFromBytes((metadata.robotId() + "\n" + hash + "\n"
                + captured.toEpochMilli()).getBytes(StandardCharsets.UTF_8)).toString();
        String objectKey = "sha256/" + hash.substring(0, 2) + "/" + hash
                + (expected.equals("JPEG") ? ".jpg" : ".png");
        return new ValidatedFrame(eventId, metadata.robotId(), metadata.platform(), captured,
                dimensions[0], dimensions[1], expected, hash, objectKey, bytes);
    }

    private int[] decode(byte[] bytes, String expected) {
        try (var input = new MemoryCacheImageInputStream(new ByteArrayInputStream(bytes))) {
            var readers = ImageIO.getImageReaders(input);
            if (!readers.hasNext()) { throw ApiException.invalid("image", "Image cannot be decoded."); }
            ImageReader reader = readers.next();
            try {
                if (!reader.getFormatName().toUpperCase(Locale.ROOT).equals(expected)) {
                    throw ApiException.invalid("image", "Declared content type does not match image bytes.");
                }
                if (expected.equals("PNG")) { validatePngChunks(bytes); }
                if (expected.equals("JPEG") && (bytes.length < 2
                        || bytes[bytes.length - 2] != (byte) 0xff
                        || bytes[bytes.length - 1] != (byte) 0xd9)) {
                    throw ApiException.invalid("image", "JPEG is truncated or lacks its end marker.");
                }
                reader.setInput(input, true, true);
                AtomicBoolean warning = new AtomicBoolean();
                reader.addIIOReadWarningListener((source, message) -> warning.set(true));
                int width = reader.getWidth(0);
                int height = reader.getHeight(0);
                if (width <= 0 || height <= 0 || width > limits.maxDimension()
                        || height > limits.maxDimension() || (long) width * height > limits.maxPixels()) {
                    throw ApiException.invalid("image", "Decoded image dimensions exceed configured limits.");
                }
                if (reader.read(0) == null || warning.get()) {
                    throw ApiException.invalid("image", "Image is corrupt or cannot be decoded without warnings.");
                }
                return new int[]{width, height};
            } finally { reader.dispose(); }
        } catch (IOException failure) {
            throw ApiException.invalid("image", "Image is corrupt or cannot be decoded.");
        } catch (RuntimeException failure) {
            if (failure instanceof ApiException api) { throw api; }
            throw ApiException.invalid("image", "Image is corrupt or cannot be decoded.");
        }
    }

    /** ImageIO does not validate PNG CRCs; reject damaged chunks before decoding pixels. */
    private static void validatePngChunks(byte[] bytes) {
        int position = 8;
        while (position <= bytes.length - 12) {
            long length = Integer.toUnsignedLong(readInt(bytes, position));
            if (length > bytes.length - position - 12) { throw invalidPng(); }
            int chunkLength = (int) length;
            String type = new String(bytes, position + 4, 4, StandardCharsets.US_ASCII);
            if (position == 8 && (!type.equals("IHDR") || chunkLength != 13)) { throw invalidPng(); }
            CRC32 checksum = new CRC32();
            checksum.update(bytes, position + 4, chunkLength + 4);
            long expected = Integer.toUnsignedLong(readInt(bytes, position + 8 + chunkLength));
            if (checksum.getValue() != expected) { throw invalidPng(); }
            position += chunkLength + 12;
            if (type.equals("IEND")) {
                if (chunkLength != 0 || position != bytes.length) { throw invalidPng(); }
                return;
            }
        }
        throw invalidPng();
    }

    private static int readInt(byte[] bytes, int offset) {
        return (bytes[offset] & 255) << 24 | (bytes[offset + 1] & 255) << 16
                | (bytes[offset + 2] & 255) << 8 | bytes[offset + 3] & 255;
    }

    private static ApiException invalidPng() {
        return ApiException.invalid("image", "PNG chunk checksum or structure is invalid.");
    }

    public Instant telemetry(ApiModels.TelemetryRequest request) {
        if (request == null) { throw ApiException.invalid("body", "Telemetry body is required."); }
        robot(request.robotId());
        platform(request.platform());
        Instant captured = timestamp(request.capturedAt());
        finiteRange(request.battery(), "battery", 0, 100);
        finiteRange(request.speed(), "speed", 0, Double.MAX_VALUE);
        finiteRange(request.networkQuality(), "network_quality", 0, 1);
        if (request.pose() == null) { throw ApiException.invalid("pose", "pose is required."); }
        finiteRange(request.pose().x(), "pose.x", -Double.MAX_VALUE, Double.MAX_VALUE);
        finiteRange(request.pose().y(), "pose.y", -Double.MAX_VALUE, Double.MAX_VALUE);
        finiteRange(request.pose().heading(), "pose.heading", -Math.PI, Math.PI);
        if (request.operatorNote() != null && request.operatorNote().length() > 1024) {
            throw ApiException.invalid("operator_note", "operator_note must not exceed 1024 characters.");
        }
        return captured;
    }

    public Instant timestamp(String value) {
        if (value == null || value.length() > 64) {
            throw ApiException.invalid("captured_at", "captured_at must be an offset-aware ISO-8601 timestamp.");
        }
        try {
            Instant parsed = OffsetDateTime.parse(value).toInstant();
            if (parsed.isAfter(clock.instant())) {
                throw ApiException.invalid("captured_at", "captured_at must not be in the future.");
            }
            return Instant.ofEpochMilli(parsed.truncatedTo(ChronoUnit.MILLIS).toEpochMilli());
        } catch (DateTimeParseException | ArithmeticException failure) {
            throw ApiException.invalid("captured_at", "captured_at must be an offset-aware ISO-8601 timestamp.");
        }
    }

    public static void robot(String value) {
        if (value == null || !value.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")) {
            throw ApiException.invalid("robot_id", "robot_id must contain 1–64 safe identifier characters.");
        }
    }

    private static void platform(String value) {
        if (!PLATFORMS.contains(value == null ? "" : value)) {
            throw ApiException.invalid("platform", "platform must be a supported canonical platform name.");
        }
    }

    private static void finiteRange(Double value, String field, double min, double max) {
        if (value == null || !Double.isFinite(value) || value < min || value > max) {
            throw ApiException.invalid(field, field + " is required and must be finite within its documented range.");
        }
    }

    private static String sha256(byte[] value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value)); }
        catch (NoSuchAlgorithmException unavailable) { throw new IllegalStateException("SHA-256 unavailable"); }
    }
}
