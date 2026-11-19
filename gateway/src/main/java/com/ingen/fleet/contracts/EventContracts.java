package com.ingen.fleet.contracts;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;

/** Loads versioned event contracts without sharing mutable Avro Schema instances. */
public final class EventContracts {
    public static final List<String> NAMES = List.of(
        "FrameSubmitted", "DetectionProduced", "TelemetryReported", "ExceptionRaised");
    private static final Map<String, String> DEFINITIONS = definitions();

    private EventContracts() { }

    public static Schema schema(String eventName) {
        String definition = DEFINITIONS.get(eventName);
        if (definition == null) { throw new IllegalArgumentException("Unknown event contract"); }
        return new Schema.Parser().parse(definition);
    }

    public static Map<String, Schema> all() {
        Map<String, Schema> result = new LinkedHashMap<>();
        NAMES.forEach(name -> result.put(name, schema(name)));
        return Map.copyOf(result);
    }

    public static String subject(String eventName) {
        if (!NAMES.contains(eventName)) { throw new IllegalArgumentException("Unknown event contract"); }
        return eventName + "-value";
    }

    private static Map<String, String> definitions() {
        Map<String, String> result = new LinkedHashMap<>();
        for (String name : NAMES) {
            try (InputStream input = EventContracts.class.getResourceAsStream("/avro/" + name + ".avsc")) {
                if (input == null) { throw new IllegalStateException("Missing packaged Avro contract"); }
                String text = new String(input.readAllBytes(), StandardCharsets.UTF_8);
                new Schema.Parser().parse(text);
                result.put(name, text);
            } catch (IOException ex) {
                throw new IllegalStateException("Cannot load packaged Avro contract", ex);
            }
        }
        return Map.copyOf(result);
    }
}
