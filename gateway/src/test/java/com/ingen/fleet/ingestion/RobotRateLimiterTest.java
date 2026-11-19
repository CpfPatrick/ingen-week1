package com.ingen.fleet.ingestion;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RobotRateLimiterTest {
    @Test void enforcesBurstAndRefillsWithMonotonicTime() {
        AtomicLong clock = new AtomicLong();
        var limiter = new RobotRateLimiter(0.5, 2, 10, clock::get);
        limiter.acquire("a");
        limiter.acquire("a");
        ApiException limited = assertThrows(ApiException.class, () -> limiter.acquire("a"));
        assertEquals(429, limited.status());
        assertEquals(2, limited.retryAfter());
        limiter.acquire("b");
        clock.addAndGet(2_000_000_000L);
        assertDoesNotThrow(() -> limiter.acquire("a"));
        assertThrows(ApiException.class, () -> limiter.acquire("a"));
    }

    @Test void boundsRobotMapWithoutEvictingActiveBuckets() {
        AtomicLong clock = new AtomicLong();
        var limiter = new RobotRateLimiter(1, 1, 1, clock::get);
        limiter.acquire("a");
        assertEquals(503, assertThrows(ApiException.class, () -> limiter.acquire("b")).status());
        assertEquals(429, assertThrows(ApiException.class, () -> limiter.acquire("a")).status());
        clock.addAndGet(601_000_000_000L);
        assertDoesNotThrow(() -> limiter.acquire("b"));
    }
}
