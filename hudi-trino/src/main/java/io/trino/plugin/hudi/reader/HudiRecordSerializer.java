/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi.reader;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.hudi.common.avro.AvroRecordSerializer;
import org.apache.hudi.common.serialization.RecordSerializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Spill serializer for the records in the merge buffer of {@link HudiTrinoReaderContext}.
 * <p>
 * Field values come in two kinds. Rows read from native log files hold Trino objects from
 * {@code Type.getObjectValue} ({@link String}, {@link SqlDecimal}, {@link SqlDate}, {@link SqlTimestamp},
 * {@link SqlVarbinary}, {@link List}, {@link Map} and boxed primitives); rows decoded from Avro log blocks hold Avro
 * values ({@link Utf8}, {@link ByteBuffer}, and {@link GenericContainer}s such as fixed decimals and nested records).
 * Avro binary encoding cannot write the Trino kinds, so each value is written with a tag for its kind and read back
 * as the same kind. The record schema is not written: as in {@link AvroRecordSerializer}, it is resolved from the
 * buffered record's schema id on read.
 * <p>
 * Not thread-safe. The merge buffer of a file group writes and reads through a single instance, which keeps the
 * schemas of the Avro containers it has written.
 */
final class HudiRecordSerializer
        implements RecordSerializer<IndexedRecord>
{
    private static final byte NULL = 0;
    private static final byte BOOLEAN = 1;
    private static final byte BYTE = 2;
    private static final byte SHORT = 3;
    private static final byte INTEGER = 4;
    private static final byte LONG = 5;
    private static final byte FLOAT = 6;
    private static final byte DOUBLE = 7;
    private static final byte STRING = 8;
    private static final byte UTF8 = 9;
    private static final byte BYTE_BUFFER = 10;
    private static final byte AVRO_CONTAINER = 11;
    private static final byte SQL_DECIMAL = 12;
    private static final byte SQL_DATE = 13;
    private static final byte SQL_TIMESTAMP = 14;
    private static final byte SQL_VARBINARY = 15;
    private static final byte LIST = 16;
    private static final byte MAP = 17;

    private final IntFunction<Schema> schemaById;
    // Avro containers are written with their own schema, referenced by its index in this list
    private final List<Schema> containerSchemas = new ArrayList<>();
    private final Map<Schema, Integer> containerSchemaIds = new IdentityHashMap<>();

    HudiRecordSerializer(IntFunction<Schema> schemaById)
    {
        this.schemaById = schemaById;
    }

    @Override
    public byte[] serialize(IndexedRecord record)
    {
        int fieldCount = record.getSchema().getFields().size();
        DynamicSliceOutput output = new DynamicSliceOutput(64 * fieldCount);
        output.writeInt(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            writeValue(output, record.get(i));
        }
        return output.slice().getBytes();
    }

    @Override
    public IndexedRecord deserialize(byte[] bytes, int schemaId)
    {
        Schema schema = schemaById.apply(schemaId);
        SliceInput input = Slices.wrappedBuffer(bytes).getInput();
        int fieldCount = input.readInt();
        checkState(fieldCount == schema.getFields().size(), "Spilled record has %s fields, but its schema has %s", fieldCount, schema.getFields().size());
        GenericData.Record record = new GenericData.Record(schema);
        for (int i = 0; i < fieldCount; i++) {
            record.put(i, readValue(input));
        }
        return record;
    }

    private void writeValue(SliceOutput output, Object value)
    {
        switch (value) {
            case null -> output.writeByte(NULL);
            case Boolean bool -> {
                output.writeByte(BOOLEAN);
                output.writeBoolean(bool);
            }
            case Byte byteValue -> {
                output.writeByte(BYTE);
                output.writeByte(byteValue);
            }
            case Short shortValue -> {
                output.writeByte(SHORT);
                output.writeShort(shortValue);
            }
            case Integer intValue -> {
                output.writeByte(INTEGER);
                output.writeInt(intValue);
            }
            case Long longValue -> {
                output.writeByte(LONG);
                output.writeLong(longValue);
            }
            case Float floatValue -> {
                output.writeByte(FLOAT);
                output.writeFloat(floatValue);
            }
            case Double doubleValue -> {
                output.writeByte(DOUBLE);
                output.writeDouble(doubleValue);
            }
            case String string -> {
                output.writeByte(STRING);
                writeBytes(output, string.getBytes(UTF_8));
            }
            case Utf8 utf8 -> {
                output.writeByte(UTF8);
                output.writeInt(utf8.getByteLength());
                output.writeBytes(utf8.getBytes(), 0, utf8.getByteLength());
            }
            case ByteBuffer buffer -> {
                output.writeByte(BYTE_BUFFER);
                byte[] bytes = new byte[buffer.remaining()];
                buffer.duplicate().get(bytes);
                writeBytes(output, bytes);
            }
            case GenericContainer container -> {
                output.writeByte(AVRO_CONTAINER);
                int containerSchemaId = containerSchemaIds.computeIfAbsent(container.getSchema(), schema -> {
                    containerSchemas.add(schema);
                    return containerSchemas.size() - 1;
                });
                output.writeInt(containerSchemaId);
                writeAvro(output, container);
            }
            case SqlDecimal decimal -> {
                output.writeByte(SQL_DECIMAL);
                writeBytes(output, decimal.getUnscaledValue().toByteArray());
                output.writeInt(decimal.getPrecision());
                output.writeInt(decimal.getScale());
            }
            case SqlDate date -> {
                output.writeByte(SQL_DATE);
                output.writeInt(date.getDays());
            }
            case SqlTimestamp timestamp -> {
                output.writeByte(SQL_TIMESTAMP);
                output.writeInt(timestamp.getPrecision());
                output.writeLong(timestamp.getEpochMicros());
                output.writeInt(timestamp.getPicosOfMicros());
            }
            case SqlVarbinary varbinary -> {
                output.writeByte(SQL_VARBINARY);
                writeBytes(output, varbinary.getBytes());
            }
            case List<?> list -> {
                output.writeByte(LIST);
                output.writeInt(list.size());
                for (Object element : list) {
                    writeValue(output, element);
                }
            }
            case Map<?, ?> map -> {
                output.writeByte(MAP);
                output.writeInt(map.size());
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    writeValue(output, entry.getKey());
                    writeValue(output, entry.getValue());
                }
            }
            default -> throw new IllegalArgumentException("Cannot spill a merge buffer value of type " + value.getClass().getName());
        }
    }

    private Object readValue(SliceInput input)
    {
        byte tag = input.readByte();
        return switch (tag) {
            case NULL -> null;
            case BOOLEAN -> input.readBoolean();
            case BYTE -> input.readByte();
            case SHORT -> input.readShort();
            case INTEGER -> input.readInt();
            case LONG -> input.readLong();
            case FLOAT -> input.readFloat();
            case DOUBLE -> input.readDouble();
            case STRING -> new String(readBytes(input), UTF_8);
            case UTF8 -> new Utf8(readBytes(input));
            case BYTE_BUFFER -> ByteBuffer.wrap(readBytes(input));
            case AVRO_CONTAINER -> readAvro(input, containerSchemas.get(input.readInt()));
            case SQL_DECIMAL -> {
                BigInteger unscaledValue = new BigInteger(readBytes(input));
                int precision = input.readInt();
                int scale = input.readInt();
                yield new SqlDecimal(unscaledValue, precision, scale);
            }
            case SQL_DATE -> new SqlDate(input.readInt());
            case SQL_TIMESTAMP -> {
                int precision = input.readInt();
                long epochMicros = input.readLong();
                int picosOfMicros = input.readInt();
                yield SqlTimestamp.newInstance(precision, epochMicros, picosOfMicros);
            }
            case SQL_VARBINARY -> new SqlVarbinary(readBytes(input));
            case LIST -> {
                int size = input.readInt();
                List<Object> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    list.add(readValue(input));
                }
                yield list;
            }
            case MAP -> {
                int size = input.readInt();
                Map<Object, Object> map = HashMap.newHashMap(size);
                for (int i = 0; i < size; i++) {
                    Object key = readValue(input);
                    map.put(key, readValue(input));
                }
                yield map;
            }
            default -> throw new IllegalStateException("Unknown merge buffer value tag " + tag);
        };
    }

    private static void writeBytes(SliceOutput output, byte[] bytes)
    {
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }

    private static byte[] readBytes(SliceInput input)
    {
        byte[] bytes = new byte[input.readInt()];
        input.readBytes(bytes);
        return bytes;
    }

    private static void writeAvro(SliceOutput output, GenericContainer container)
    {
        try {
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);
            new GenericDatumWriter<>(container.getSchema()).write(container, encoder);
            encoder.flush();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Object readAvro(SliceInput input, Schema schema)
    {
        try {
            return new GenericDatumReader<>(schema).read(null, DecoderFactory.get().directBinaryDecoder(input, null));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
