package com.ingen.fleet.contracts;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/** Confluent-compatible wire format: magic byte 0, four-byte big-endian schema ID, Avro. */
public final class AvroEventCodec {
    private static final int PREFIX_BYTES = 5;

    private AvroEventCodec() { }

    public static byte[] encode(int schemaId, GenericRecord record) {
        if (schemaId <= 0) { throw new IllegalArgumentException("Schema ID must be positive"); }
        if (record == null || !GenericData.get().validate(record.getSchema(), record)) {
            throw new IllegalArgumentException("Record does not satisfy its Avro contract");
        }
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            output.write(ByteBuffer.allocate(PREFIX_BYTES).put((byte) 0).putInt(schemaId).array());
            var encoder = EncoderFactory.get().binaryEncoder(output, null);
            new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
            encoder.flush();
            return output.toByteArray();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Cannot encode Avro event", ex);
        }
    }

    public static int schemaId(byte[] payload) {
        if (payload == null || payload.length <= PREFIX_BYTES || payload[0] != 0) {
            throw new IllegalArgumentException("Invalid Avro wire envelope");
        }
        int id = ByteBuffer.wrap(payload, 1, 4).getInt();
        if (id <= 0) { throw new IllegalArgumentException("Invalid schema ID in wire envelope"); }
        return id;
    }

    /** The caller must resolve writerSchema using the ID in the envelope, never assume latest. */
    public static GenericRecord decode(byte[] payload, Schema writerSchema, Schema readerSchema) {
        schemaId(payload);
        if (writerSchema == null || readerSchema == null) {
            throw new IllegalArgumentException("Writer and reader schemas are required");
        }
        try {
            var decoder = DecoderFactory.get().binaryDecoder(
                payload, PREFIX_BYTES, payload.length - PREFIX_BYTES, null);
            GenericRecord result = new GenericDatumReader<GenericRecord>(writerSchema, readerSchema)
                .read(null, decoder);
            if (!decoder.isEnd()) { throw new IllegalArgumentException("Trailing bytes after Avro event"); }
            return result;
        } catch (IOException | org.apache.avro.AvroRuntimeException ex) {
            throw new IllegalArgumentException("Cannot decode Avro event", ex);
        }
    }
}
