package com.ingen.fleet.ingestion;

import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/** Monotonic-clock token buckets, bounded in memory; intended for one gateway instance. */
public final class RobotRateLimiter {
    private static final long IDLE_NANOS = 600_000_000_000L;
    private final double perSecond;
    private final int burst;
    private final int maximumRobots;
    private final LongSupplier clock;
    private final Map<String, Bucket> buckets = new HashMap<>();
    private long nextCleanup;

    private static final class Bucket {
        private double tokens;
        private long updated;
        Bucket(double tokens, long updated) { this.tokens = tokens; this.updated = updated; }
    }

    public RobotRateLimiter(double perSecond, int burst, int maximumRobots) {
        this(perSecond, burst, maximumRobots, System::nanoTime);
    }

    RobotRateLimiter(double perSecond, int burst, int maximumRobots, LongSupplier clock) {
        this.perSecond = perSecond;
        this.burst = burst;
        this.maximumRobots = maximumRobots;
        this.clock = clock;
    }

    public synchronized void acquire(String robot) {
        long now = clock.getAsLong();
        if (now >= nextCleanup) {
            buckets.entrySet().removeIf(entry -> now - entry.getValue().updated >= IDLE_NANOS);
            nextCleanup = now + 60_000_000_000L;
        }
        Bucket bucket = buckets.get(robot);
        if (bucket == null) {
            if (buckets.size() >= maximumRobots) {
                throw new ApiException(503, "RATE_LIMIT_CAPACITY", "Too many active robot identities.");
            }
            bucket = new Bucket(burst, now);
            buckets.put(robot, bucket);
        }
        bucket.tokens = Math.min(burst, bucket.tokens + Math.max(0, now - bucket.updated) / 1e9 * perSecond);
        bucket.updated = now;
        if (bucket.tokens < 1) {
            int waitSeconds = Math.max(1, (int) Math.ceil((1 - bucket.tokens) / perSecond));
            throw new ApiException(429, "RATE_LIMITED", "Per-robot ingestion rate limit exceeded.",
                    Map.of("retry_after_seconds", waitSeconds), waitSeconds);
        }
        bucket.tokens -= 1;
    }
}
