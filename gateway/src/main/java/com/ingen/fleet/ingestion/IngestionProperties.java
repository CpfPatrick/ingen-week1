package com.ingen.fleet.ingestion;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("fleet.ingestion")
public record IngestionProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("5242880") long maxImageBytes,
        @DefaultValue("16777216") long maxPixels,
        @DefaultValue("8192") int maxDimension,
        @DefaultValue("10") double ratePerSecond,
        @DefaultValue("20") int rateBurst,
        @DefaultValue("10000") int maxRateRobots,
        @DefaultValue("jdbc:postgresql://localhost:5432/fleet") String jdbcUrl,
        @DefaultValue("fleet") String username,
        @DefaultValue("") String password,
        @DefaultValue("http://localhost:9000") String minioUrl,
        @DefaultValue("") String accessKey,
        @DefaultValue("") String secretKey,
        @DefaultValue("frames") String bucket) {
    public IngestionProperties {
        if (maxImageBytes < 1 || maxPixels < 1 || maxDimension < 1
                || !Double.isFinite(ratePerSecond) || ratePerSecond <= 0
                || rateBurst < 1 || maxRateRobots < 1) {
            throw new IllegalArgumentException("Invalid ingestion limits");
        }
    }
}
