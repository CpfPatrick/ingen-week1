package com.ingen.fleet.contracts;

import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Runs in every build; modify a candidate schema to exercise the CI rejection path. */
class SchemaCompatibilityTest {
    @Test void publishedBaselineMustRemainReadableByCurrentSchemas() throws Exception {
        for (String name : EventContracts.NAMES) {
            Schema writer = resource("/avro/baseline/" + name + ".avsc");
            Schema reader = EventContracts.schema(name);
            var result = SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);
            assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, result.getType(),
                () -> name + " breaks BACKWARD compatibility: " + result.getDescription());
        }
    }

    @Test void optionalDefaultedEvolutionIsCompatibleInBothDirections() throws Exception {
        Schema v1 = EventContracts.schema("FrameSubmitted");
        Schema v2 = resource("/avro/evolution/FrameSubmitted.v2.avsc");
        assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
            SchemaCompatibility.checkReaderWriterCompatibility(v2, v1).getType());
        assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
            SchemaCompatibility.checkReaderWriterCompatibility(v1, v2).getType());
        assertNotNull(v2.getField("camera_id"));
    }

    @Test void requiredNewFieldWithoutDefaultIsRejected() {
        String current = EventContracts.schema("FrameSubmitted").toString();
        String incompatible = current.replace("\"fields\":[", "\"fields\":[{\"name\":\"mandatory_new\",\"type\":\"string\"},");
        var result = SchemaCompatibility.checkReaderWriterCompatibility(
            new Schema.Parser().parse(incompatible), EventContracts.schema("FrameSubmitted"));
        assertEquals(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE, result.getType());
    }

    @Test void contractAccessDoesNotShareMutableSchemas() {
        Schema mutable = EventContracts.schema("FrameSubmitted");
        mutable.addProp("test-only", true);
        assertFalse(EventContracts.schema("FrameSubmitted").getObjectProps().containsKey("test-only"));
        assertThrows(UnsupportedOperationException.class,
            () -> EventContracts.all().put("Other", mutable));
        assertThrows(IllegalArgumentException.class, () -> EventContracts.schema("NoSuchEvent"));
        assertThrows(IllegalArgumentException.class, () -> EventContracts.subject("NoSuchEvent"));
    }

    static Schema resource(String path) throws IOException {
        try (InputStream input = SchemaCompatibilityTest.class.getResourceAsStream(path)) {
            if (input == null) { throw new IOException("Missing test schema: " + path); }
            return new Schema.Parser().parse(input);
        }
    }
}
