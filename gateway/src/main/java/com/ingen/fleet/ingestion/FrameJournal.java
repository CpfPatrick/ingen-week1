package com.ingen.fleet.ingestion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import org.flywaydb.core.Flyway;

/** Durable submission identity. Row locks serialize duplicate requests across gateway instances. */
public final class FrameJournal {
    private final String url;
    private final Properties credentials;

    public record Entry(String eventId, String robotId, String platform, Instant capturedAt,
            Instant receivedAt, int width, int height, String format, long sizeBytes,
            String sha256, String objectKey, String traceId, String status) {
        public ApiModels.FrameStatus response() {
            return new ApiModels.FrameStatus(eventId, objectKey, status, capturedAt, receivedAt);
        }
    }

    public record Submission(Entry entry, boolean duplicate) { }

    @FunctionalInterface
    public interface PublishAction { void publish(Entry entry); }

    public FrameJournal(IngestionProperties config) throws SQLException {
        url = config.jdbcUrl();
        credentials = new Properties();
        credentials.setProperty("user", config.username());
        credentials.setProperty("password", config.password());
        credentials.setProperty("connectTimeout", "3");
        credentials.setProperty("socketTimeout", "30");
        Flyway.configure().dataSource(url, config.username(), config.password())
                .locations("classpath:db/migration").validateMigrationNaming(true).load().migrate();
    }

    private Connection open() throws SQLException { return DriverManager.getConnection(url, credentials); }

    public Submission submit(RequestValidator.ValidatedFrame frame, Instant receivedAt,
                             String trace, PublishAction action) {
        // Commit identity before the external writes. Failed/uncertain requests keep the same ID
        // and first received_at on retry. No image bytes or operator notes are stored here.
        try (Connection connection = open(); var insert = connection.prepareStatement("""
                INSERT INTO ingestion_frames(event_id,robot_id,platform,captured_at,received_at,
                    width,height,format,size_bytes,sha256,object_key,trace_id,status)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'PENDING') ON CONFLICT DO NOTHING
                """)) {
            insert.setString(1, frame.eventId());
            insert.setString(2, frame.robotId());
            insert.setString(3, frame.platform());
            insert.setLong(4, frame.capturedAt().toEpochMilli());
            insert.setLong(5, receivedAt.toEpochMilli());
            insert.setInt(6, frame.width());
            insert.setInt(7, frame.height());
            insert.setString(8, frame.format());
            insert.setLong(9, frame.image().length);
            insert.setString(10, frame.sha256());
            insert.setString(11, frame.objectKey());
            insert.setString(12, trace);
            insert.executeUpdate();
        } catch (SQLException unavailable) { throw ApiException.unavailable(); }

        try (Connection connection = open()) {
            connection.setAutoCommit(false);
            try (var settings = connection.createStatement()) {
                settings.execute("SET LOCAL lock_timeout = '20s'");
            }
            try (var select = connection.prepareStatement(
                    "SELECT * FROM ingestion_frames WHERE event_id=? FOR UPDATE")) {
                select.setString(1, frame.eventId());
                Entry entry;
                try (var rows = select.executeQuery()) {
                    if (!rows.next()) { throw ApiException.unavailable(); }
                    entry = read(rows);
                }
                if (!entry.platform().equals(frame.platform())) {
                    throw ApiException.invalid("platform", "Duplicate frame metadata conflicts with its original platform.");
                }
                if (entry.status().equals("SUBMITTED")) {
                    connection.commit();
                    return new Submission(entry, true);
                }
                // Publish is synchronous and waits for Kafka's acknowledgement. A process crash
                // after Kafka ack but before this commit may redeliver the SAME event_id.
                // This is at-least-once across system failures, never cross-system exactly-once.
                action.publish(entry);
                try (var update = connection.prepareStatement(
                        "UPDATE ingestion_frames SET status='SUBMITTED' WHERE event_id=?")) {
                    update.setString(1, frame.eventId());
                    update.executeUpdate();
                }
                connection.commit();
                return new Submission(new Entry(entry.eventId(), entry.robotId(), entry.platform(),
                        entry.capturedAt(), entry.receivedAt(), entry.width(), entry.height(),
                        entry.format(), entry.sizeBytes(), entry.sha256(), entry.objectKey(),
                        entry.traceId(), "SUBMITTED"), false);
            } catch (RuntimeException | SQLException failure) {
                try { connection.rollback(); } catch (SQLException ignored) { /* Connection closes next. */ }
                if (failure instanceof ApiException api) { throw api; }
                throw ApiException.unavailable();
            }
        } catch (SQLException unavailable) { throw ApiException.unavailable(); }
    }

    public Optional<Entry> find(String eventId) {
        try (Connection connection = open(); var select = connection.prepareStatement(
                "SELECT * FROM ingestion_frames WHERE event_id=?")) {
            select.setString(1, eventId);
            try (var rows = select.executeQuery()) {
                return rows.next() ? Optional.of(read(rows)) : Optional.empty();
            }
        } catch (SQLException unavailable) { throw ApiException.unavailable(); }
    }

    public boolean healthy() {
        try (Connection connection = open()) { return connection.isValid(2); }
        catch (SQLException unavailable) { return false; }
    }

    private static Entry read(ResultSet row) throws SQLException {
        return new Entry(row.getString("event_id"), row.getString("robot_id"), row.getString("platform"),
                Instant.ofEpochMilli(row.getLong("captured_at")),
                Instant.ofEpochMilli(row.getLong("received_at")), row.getInt("width"), row.getInt("height"),
                row.getString("format"), row.getLong("size_bytes"), row.getString("sha256"),
                row.getString("object_key"), row.getString("trace_id"), row.getString("status"));
    }
}
