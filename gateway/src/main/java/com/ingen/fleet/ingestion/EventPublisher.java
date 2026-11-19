package com.ingen.fleet.ingestion;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.ingen.fleet.contracts.AvroEventCodec;
import com.ingen.fleet.contracts.SchemaRegistryClient;

public final class EventPublisher implements AutoCloseable {
    public static final String FRAME_TOPIC = "fleet.frames.v1";
    public static final String TELEMETRY_TOPIC = "fleet.telemetry.v1";
    private final KafkaProducer<String, byte[]> producer;
    private final Map<String, Integer> schemaIds;

    public EventPublisher(String bootstrap, String registryUrl) throws Exception {
        schemaIds = new SchemaRegistryClient(registryUrl, Duration.ofSeconds(5)).registerAll();
        producer = new KafkaProducer<>(producerProperties(bootstrap));
    }

    public static Properties producerProperties(String bootstrap) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 65536);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        return config;
    }

    public void publish(String topic, String robot, GenericRecord value, String trace) {
        try {
            Integer schemaId = schemaIds.get(value.getSchema().getName());
            if (schemaId == null) { throw ApiException.unavailable(); }
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, robot,
                    AvroEventCodec.encode(schemaId, value));
            record.headers().add("trace_id", trace.getBytes(StandardCharsets.UTF_8));
            producer.send(record).get(12, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw ApiException.unavailable();
        } catch (Exception unavailable) { throw ApiException.unavailable(); }
    }

    @Override public void close() { producer.close(Duration.ofSeconds(5)); }
}
