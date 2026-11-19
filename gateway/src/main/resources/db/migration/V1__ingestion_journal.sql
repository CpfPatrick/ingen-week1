CREATE TABLE ingestion_frames (
    event_id VARCHAR(36) PRIMARY KEY,
    robot_id VARCHAR(64) NOT NULL,
    platform VARCHAR(32) NOT NULL,
    captured_at BIGINT NOT NULL,
    received_at BIGINT NOT NULL,
    width INTEGER NOT NULL,
    height INTEGER NOT NULL,
    format VARCHAR(8) NOT NULL,
    size_bytes BIGINT NOT NULL,
    sha256 VARCHAR(64) NOT NULL,
    object_key VARCHAR(128) NOT NULL,
    trace_id VARCHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('PENDING', 'SUBMITTED')),
    UNIQUE(robot_id, sha256, captured_at)
);
