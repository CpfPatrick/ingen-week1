package com.ingen.fleet.ingestion;

import java.time.Clock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(prefix = "fleet.ingestion", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(IngestionProperties.class)
public class IngestionConfiguration {
    @Bean public Clock ingestionClock() { return Clock.systemUTC(); }

    @Bean public RequestValidator requestValidator(IngestionProperties config, Clock ingestionClock) {
        return new RequestValidator(config, ingestionClock);
    }

    @Bean public RobotRateLimiter robotRateLimiter(IngestionProperties config) {
        return new RobotRateLimiter(config.ratePerSecond(), config.rateBurst(), config.maxRateRobots());
    }

    @Bean public FrameJournal frameJournal(IngestionProperties config) {
        try { return new FrameJournal(config); }
        catch (Exception unavailable) { throw new IllegalStateException("Ingestion journal initialization failed"); }
    }

    @Bean public FrameObjectStore frameObjectStore(IngestionProperties config) {
        try { return new FrameObjectStore(config); }
        catch (Exception unavailable) { throw new IllegalStateException("Frame object store initialization failed"); }
    }

    @Bean(destroyMethod = "close")
    public EventPublisher eventPublisher(@Value("${fleet.kafka}") String bootstrap,
            @Value("${fleet.registry}") String registry) {
        try { return new EventPublisher(bootstrap, registry); }
        catch (Exception unavailable) { throw new IllegalStateException("Event publisher initialization failed"); }
    }

    @Bean public IngestionService ingestionService(RequestValidator validator, RobotRateLimiter limiter,
            FrameJournal journal, FrameObjectStore objects, EventPublisher publisher, Clock ingestionClock) {
        return new IngestionService(validator, limiter, journal, objects, publisher, ingestionClock);
    }

    @Bean("ingestion") public HealthIndicator ingestionHealth(FrameJournal journal, FrameObjectStore objects) {
        return () -> {
            boolean database = journal.healthy();
            boolean storage = objects.healthy();
            return (database && storage ? Health.up() : Health.down())
                    .withDetail("journal", database).withDetail("object_store", storage).build();
        };
    }
}
