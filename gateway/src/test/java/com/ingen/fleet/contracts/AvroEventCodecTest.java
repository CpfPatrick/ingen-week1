package com.ingen.fleet.contracts;

import java.io.InputStream;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AvroEventCodecTest {
    @Test void everyContractRoundTripsItsWorkedExampleWithSchemaIdPrefix() throws Exception {
        for (String name : EventContracts.NAMES) {
            GenericRecord original = example(name);
            byte[] bytes = AvroEventCodec.encode(42, original);
            assertEquals(0, bytes[0]);
            assertEquals(42, AvroEventCodec.schemaId(bytes));
            assertEquals(original, AvroEventCodec.decode(bytes, original.getSchema(), original.getSchema()));
        }
    }

    @Test void v1ConsumerReadsV2EventAndNewConsumerReadsV1Event() throws Exception {
        GenericRecord v1Record = example("FrameSubmitted");
        Schema v1 = v1Record.getSchema();
        Schema v2 = SchemaCompatibilityTest.resource("/avro/evolution/FrameSubmitted.v2.avsc");
        GenericRecord v2Record = new GenericData.Record(v2);
        for (Schema.Field field : v1.getFields()) { v2Record.put(field.name(), v1Record.get(field.name())); }
        v2Record.put("camera_id", "front-camera");

        byte[] v2Bytes = AvroEventCodec.encode(2, v2Record);
        GenericRecord oldReader = AvroEventCodec.decode(v2Bytes, v2, v1);
        assertEquals(v1Record, oldReader);

        byte[] v1Bytes = AvroEventCodec.encode(1, v1Record);
        GenericRecord newReader = AvroEventCodec.decode(v1Bytes, v1, v2);
        assertNull(newReader.get("camera_id"));
        assertEquals(v1Record.get("event_id"), newReader.get("event_id"));
    }

    @Test void rejectsInvalidEnvelopesInvalidRecordsAndTrailingBytes() throws Exception {
        GenericRecord record = example("FrameSubmitted");
        assertThrows(IllegalArgumentException.class, () -> AvroEventCodec.encode(0, record));
        assertThrows(IllegalArgumentException.class, () -> AvroEventCodec.encode(1, null));
        assertThrows(IllegalArgumentException.class, () -> AvroEventCodec.encode(1,
            new GenericData.Record(record.getSchema())));
        assertThrows(IllegalArgumentException.class, () -> AvroEventCodec.schemaId(null));
        assertThrows(IllegalArgumentException.class, () -> AvroEventCodec.schemaId(new byte[5]));
        assertThrows(IllegalArgumentException.class, () -> AvroEventCodec.schemaId(new byte[] {1, 0, 0, 0, 1, 0}));
        assertThrows(IllegalArgumentException.class, () -> AvroEventCodec.schemaId(new byte[6]));
        byte[] valid = AvroEventCodec.encode(1, record);
        byte[] trailing = Arrays.copyOf(valid, valid.length + 1);
        assertThrows(IllegalArgumentException.class,
            () -> AvroEventCodec.decode(trailing, record.getSchema(), record.getSchema()));
        byte[] truncated = Arrays.copyOf(valid, valid.length - 3);
        assertThrows(IllegalArgumentException.class,
            () -> AvroEventCodec.decode(truncated, record.getSchema(), record.getSchema()));
        assertThrows(IllegalArgumentException.class, () -> AvroEventCodec.decode(valid, null, null));
    }

    static GenericRecord example(String name) throws Exception {
        Schema schema = EventContracts.schema(name);
        try (InputStream input = AvroEventCodecTest.class.getResourceAsStream("/avro/examples/" + name + ".json")) {
            return new GenericDatumReader<GenericRecord>(schema)
                .read(null, DecoderFactory.get().jsonDecoder(schema, input));
        }
    }
}
