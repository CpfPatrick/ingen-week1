package com.ingen.fleet.ingestion;

import java.io.ByteArrayInputStream;
import java.util.Map;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.errors.ErrorResponseException;

public final class FrameObjectStore {
    private final MinioClient client;
    private final String bucket;

    public FrameObjectStore(IngestionProperties config) throws Exception {
        client = MinioClient.builder().endpoint(config.minioUrl())
                .credentials(config.accessKey(), config.secretKey()).build();
        client.setTimeout(3000, 10000, 10000);
        bucket = config.bucket();
        if (!client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build())) {
            try { client.makeBucket(MakeBucketArgs.builder().bucket(bucket).build()); }
            catch (ErrorResponseException conflict) {
                if (!conflict.errorResponse().code().equals("BucketAlreadyOwnedByYou")) { throw conflict; }
            }
        }
    }

    public void putIfAbsent(RequestValidator.ValidatedFrame frame) {
        try {
            try {
                var existing = client.statObject(StatObjectArgs.builder().bucket(bucket)
                        .object(frame.objectKey()).build());
                if (existing.size() != frame.image().length) { throw ApiException.unavailable(); }
                return;
            } catch (ErrorResponseException missing) {
                String code = missing.errorResponse().code();
                if (!code.equals("NoSuchKey") && !code.equals("NoSuchObject")) { throw missing; }
            }
            client.putObject(PutObjectArgs.builder().bucket(bucket).object(frame.objectKey())
                    .contentType(frame.format().equals("JPEG") ? "image/jpeg" : "image/png")
                    .userMetadata(Map.of("sha256", frame.sha256()))
                    .stream(new ByteArrayInputStream(frame.image()), frame.image().length, -1).build());
        } catch (Exception unavailable) { throw ApiException.unavailable(); }
    }

    public boolean healthy() {
        try { return client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build()); }
        catch (Exception unavailable) { return false; }
    }
}
