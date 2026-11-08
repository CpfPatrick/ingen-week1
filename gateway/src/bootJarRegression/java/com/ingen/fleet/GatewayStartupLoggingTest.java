package com.ingen.fleet;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Subprocess regression: this must exercise Boot's real failure-analysis logger and encoder. */
class GatewayStartupLoggingTest {
    @TempDir Path directory;

    @Test void invalidConfigurationExitsWithSafeStructuredStartupFailure() throws Exception {
        String jar = System.getProperty("gateway.bootJar");
        String java = System.getProperty("gateway.java");
        assertNotNull(jar, "Run :gateway:bootJarRegressionTest to supply the real executable jar");
        assertNotNull(java, "The regression task must use the configured Java toolchain");
        assertTrue(Files.isRegularFile(Path.of(jar)));
        String sentinel = "CONFIG_SECRET_6f147fe8";
        Path outputFile = directory.resolve("invalid-config.log");
        ProcessBuilder builder = new ProcessBuilder(java, "-jar", jar,
            "--server.port=invalid-" + sentinel,
            "--fleet.kafka=" + sentinel,
            "--fleet.registry=https://" + sentinel + "@example.invalid");
        // No inherited JVM options, SPRING_* overrides, or developer credentials.
        builder.environment().clear();
        builder.directory(directory.toFile());
        builder.redirectErrorStream(true);
        builder.redirectOutput(outputFile.toFile());
        Process process = builder.start();
        try {
            assertTrue(process.waitFor(45, TimeUnit.SECONDS), "Invalid configuration must terminate promptly");
            assertNotEquals(0, process.exitValue(), "Invalid port must fail real application startup");
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS), "Child process must be reaped");
            }
        }
        String output = Files.readString(outputFile);
        assertFalse(output.contains(sentinel), "A configuration sentinel escaped into process output");
        assertFalse(output.isBlank(), "Startup must leave an observable failure record");
        var mapper = new ObjectMapper();
        boolean startupFailure = false;
        for (String line : output.lines().filter(value -> !value.isBlank()).toList()) {
            var record = mapper.readTree(line);
            var keys = new HashSet<String>();
            record.fieldNames().forEachRemaining(keys::add);
            assertEquals(Set.of("timestamp", "level", "service", "trace_id", "event"), keys,
                "Every startup output line must use the allowlisted structured schema");
            assertTrue(record.path("timestamp").isIntegralNumber());
            assertTrue(Set.of("TRACE", "DEBUG", "INFO", "WARN", "ERROR")
                .contains(record.path("level").asText()));
            assertEquals("gateway", record.path("service").asText());
            assertEquals("startup", record.path("trace_id").asText());
            String event = record.path("event").asText();
            assertTrue(Set.of("framework_event", "application_startup_failure").contains(event));
            startupFailure |= "application_startup_failure".equals(event);
        }
        assertTrue(startupFailure, "The actual Boot failure analyzer must emit a fixed startup failure event");
    }
}
