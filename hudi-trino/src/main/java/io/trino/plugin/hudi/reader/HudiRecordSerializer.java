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
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.SqlVarbinary;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.hudi.common.avro.AvroRecordSerializer;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.serialization.RecordSerializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Spill serializer for the records in the merge buffer of {@link HudiTrinoReaderContext}.
 * <p>
 * Values are written by the {@link HoodieSchemaType} of their field, so the record's {@link HoodieSchema} is the only
 * type system here. The record schema itself is not written: as in {@link AvroRecordSerializer}, it is resolved from
 * the buffered record's schema id on read.
 * <p>
 * A field value comes in one of two representations, and a marker per value says which:
 * <ul>
 * <li>{@link #AVRO}: the Avro value of the type ({@link Utf8}, {@link ByteBuffer}, {@link GenericContainer}s, boxed
 * {@code int}, {@code long}, {@code float}, {@code double} and {@code boolean}), as held by rows decoded from Avro log
 * blocks. Written in Avro binary encoding with the field's schema.</li>
 * <li>{@link #TRINO}: the object {@code Type.getObjectValue} returns for the type, as held by rows read from native
 * log files. {@link #writeTrino} is the mapping from {@link HoodieSchemaType} to that object; a type it does not list
 * cannot be spilled in this representation and fails with the field type in the message.</li>
 * </ul>
 * The Trino column types come from the metastore, so four details of a Trino object are not in {@link HoodieSchema}.
 * Tinyint and smallint are INT there, so they are read back as {@link Integer}, the same as from an Avro log block.
 * A {@link SqlDecimal}'s precision and scale and a {@link SqlTimestamp}'s precision are written with the value. So is
 * whether a string or binary type is held as a {@link String} or a {@link SqlVarbinary}: Hive sync declares UUID and
 * VECTOR as binary, for example. BLOB and VARIANT are structs in the metastore and are written as records.
 * <p>
 * Not thread-safe. The merge buffer of a file group writes and reads through a single instance.
 */
final class HudiRecordSerializer
        implements RecordSerializer<IndexedRecord>
{
    private static final byte NULL = 0;
    private static final byte AVRO = 1;
    private static final byte TRINO = 2;

    private final IntFunction<Schema> schemaById;
    // Keyed by equality, so equal copies of a schema share one entry
    private final Map<Schema, TypeNode[]> fieldTypes = new HashMap<>();
    private Schema lastSchema;
    private TypeNode[] lastFieldTypes;
    private BinaryEncoder encoder;
    private BinaryDecoder decoder;

    HudiRecordSerializer(IntFunction<Schema> schemaById)
    {
        this.schemaById = schemaById;
    }

    @Override
    public byte[] serialize(IndexedRecord record)
    {
        TypeNode[] fields = fieldTypesOf(record.getSchema());
        DynamicSliceOutput output = new DynamicSliceOutput(64 * fields.length);
        // The direct encoder does not buffer, so its writes interleave with writes to the output
        encoder = EncoderFactory.get().directBinaryEncoder(output, encoder);
        output.writeInt(fields.length);
        for (int i = 0; i < fields.length; i++) {
            writeValue(output, fields[i], record.get(i));
        }
        return output.slice().getBytes();
    }

    @Override
    public IndexedRecord deserialize(byte[] bytes, int schemaId)
    {
        Schema schema = schemaById.apply(schemaId);
        TypeNode[] fields = fieldTypesOf(schema);
        SliceInput input = Slices.wrappedBuffer(bytes).getInput();
        // The direct decoder does not read ahead, so its reads interleave with reads from the input
        decoder = DecoderFactory.get().directBinaryDecoder(input, decoder);
        int fieldCount = input.readInt();
        checkState(fieldCount == fields.length, "Spilled record has %s fields, but its schema has %s", fieldCount, fields.length);
        GenericData.Record record = new GenericData.Record(schema);
        for (int i = 0; i < fieldCount; i++) {
            record.put(i, readValue(input, fields[i]));
        }
        return record;
    }

    private TypeNode[] fieldTypesOf(Schema recordSchema)
    {
        // The records of a file group mostly share one schema instance, which skips the equality lookup
        if (recordSchema != lastSchema) {
            lastFieldTypes = fieldTypes.computeIfAbsent(recordSchema, schema -> HoodieSchema.fromAvroSchema(schema).getFields().stream()
                    .map(HoodieSchemaField::schema)
                    .map(TypeNode::new)
                    .toArray(TypeNode[]::new));
            lastSchema = recordSchema;
        }
        return lastFieldTypes;
    }

    private void writeValue(SliceOutput output, TypeNode node, Object value)
    {
        if (value == null) {
            output.writeByte(NULL);
        }
        else if (isTrinoObject(value)) {
            output.writeByte(TRINO);
            writeTrino(output, node, value);
        }
        else {
            output.writeByte(AVRO);
            try {
                node.avroWriter().write(value, encoder);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private Object readValue(SliceInput input, TypeNode node)
    {
        byte representation = input.readByte();
        return switch (representation) {
            case NULL -> null;
            case TRINO -> readTrino(input, node);
            case AVRO -> {
                try {
                    yield node.avroReader().read(null, decoder);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            default -> throw new IllegalStateException("Unknown merge buffer value representation " + representation);
        };
    }

    /**
     * Whether the value is one only Trino produces. Boxed {@code int}, {@code long}, {@code float}, {@code double} and
     * {@code boolean} are the same object in both representations and take the Avro one. {@link SqlTime} and
     * {@link SqlTimestampWithTimeZone} have no spilled form, and are listed so they fail in {@link #writeTrino} with
     * the field type in the message rather than in the Avro writer.
     */
    private static boolean isTrinoObject(Object value)
    {
        return switch (value) {
            case String _, SqlDecimal _, SqlDate _, SqlTimestamp _, SqlVarbinary _, Byte _, Short _, SqlTime _, SqlTimestampWithTimeZone _ -> true;
            // GenericData.Array is a List, and Avro maps are keyed by Utf8
            case List<?> list -> !(list instanceof GenericContainer);
            case Map<?, ?> map -> !map.isEmpty() && !(map.keySet().iterator().next() instanceof Utf8);
            default -> false;
        };
    }

    /**
     * The Trino object of each {@link HoodieSchemaType}.
     */
    private void writeTrino(SliceOutput output, TypeNode node, Object value)
    {
        switch (node.type) {
            case STRING, ENUM, UUID, BYTES, FIXED, VECTOR -> {
                switch (value) {
                    case String string -> {
                        output.writeBoolean(false);
                        writeBytes(output, string.getBytes(UTF_8));
                    }
                    case SqlVarbinary varbinary -> {
                        output.writeBoolean(true);
                        writeBytes(output, varbinary.getBytes());
                    }
                    default -> throw unsupported(node, value);
                }
            }
            // tinyint and smallint; HoodieSchema has no narrower integer than INT
            case INT -> output.writeInt(cast(Number.class, node, value).intValue());
            case DECIMAL -> {
                SqlDecimal decimal = cast(SqlDecimal.class, node, value);
                writeBytes(output, decimal.getUnscaledValue().toByteArray());
                output.writeInt(decimal.getPrecision());
                output.writeInt(decimal.getScale());
            }
            case DATE -> output.writeInt(cast(SqlDate.class, node, value).getDays());
            case TIMESTAMP -> {
                SqlTimestamp timestamp = cast(SqlTimestamp.class, node, value);
                output.writeInt(timestamp.getPrecision());
                output.writeLong(timestamp.getEpochMicros());
                output.writeInt(timestamp.getPicosOfMicros());
            }
            case ARRAY -> {
                List<?> list = cast(List.class, node, value);
                output.writeInt(list.size());
                for (Object element : list) {
                    writeValue(output, node.element(), element);
                }
            }
            case MAP -> {
                Map<?, ?> map = cast(Map.class, node, value);
                output.writeInt(map.size());
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    writeBytes(output, cast(String.class, node, entry.getKey()).getBytes(UTF_8));
                    writeValue(output, node.element(), entry.getValue());
                }
            }
            case RECORD, BLOB, VARIANT -> {
                List<?> row = cast(List.class, node, value);
                TypeNode[] fields = node.fields();
                checkState(row.size() == fields.length, "Row has %s fields, but %s has %s", row.size(), node.schema, fields.length);
                for (int i = 0; i < fields.length; i++) {
                    writeValue(output, fields[i], row.get(i));
                }
            }
            case LONG, FLOAT, DOUBLE, BOOLEAN, TIME, UNION, NULL -> throw unsupported(node, value);
        }
    }

    private Object readTrino(SliceInput input, TypeNode node)
    {
        return switch (node.type) {
            case STRING, ENUM, UUID, BYTES, FIXED, VECTOR -> {
                boolean binary = input.readBoolean();
                byte[] bytes = readBytes(input);
                yield binary ? new SqlVarbinary(bytes) : new String(bytes, UTF_8);
            }
            case INT -> input.readInt();
            case DECIMAL -> {
                BigInteger unscaledValue = new BigInteger(readBytes(input));
                int precision = input.readInt();
                int scale = input.readInt();
                yield new SqlDecimal(unscaledValue, precision, scale);
            }
            case DATE -> new SqlDate(input.readInt());
            case TIMESTAMP -> {
                int precision = input.readInt();
                long epochMicros = input.readLong();
                int picosOfMicros = input.readInt();
                yield SqlTimestamp.newInstance(precision, epochMicros, picosOfMicros);
            }
            case ARRAY -> {
                int size = input.readInt();
                List<Object> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    list.add(readValue(input, node.element()));
                }
                yield list;
            }
            case MAP -> {
                int size = input.readInt();
                Map<Object, Object> map = HashMap.newHashMap(size);
                for (int i = 0; i < size; i++) {
                    String key = new String(readBytes(input), UTF_8);
                    map.put(key, readValue(input, node.element()));
                }
                yield map;
            }
            case RECORD, BLOB, VARIANT -> {
                TypeNode[] fields = node.fields();
                List<Object> row = new ArrayList<>(fields.length);
                for (TypeNode field : fields) {
                    row.add(readValue(input, field));
                }
                yield row;
            }
            case LONG, FLOAT, DOUBLE, BOOLEAN, TIME, UNION, NULL ->
                    throw new IllegalStateException("No Trino object is spilled for " + node.schema);
        };
    }

    private static <T> T cast(Class<T> expected, TypeNode node, Object value)
    {
        if (!expected.isInstance(value)) {
            throw unsupported(node, value);
        }
        return expected.cast(value);
    }

    private static IllegalArgumentException unsupported(TypeNode node, Object value)
    {
        return new IllegalArgumentException("Cannot spill a merge buffer value of class %s for a field of type %s: %s"
                .formatted(value.getClass().getName(), node.type, node.schema));
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

    /**
     * A field, element or value type with its nullability removed, and what is derived from it on first use.
     */
    private static final class TypeNode
    {
        private final HoodieSchema schema;
        private final HoodieSchemaType type;
        private GenericDatumWriter<Object> avroWriter;
        private GenericDatumReader<Object> avroReader;
        private TypeNode element;
        private TypeNode[] fields;

        TypeNode(HoodieSchema schema)
        {
            this.schema = schema.isNullable() ? schema.getNonNullType() : schema;
            this.type = this.schema.getType();
        }

        GenericDatumWriter<Object> avroWriter()
        {
            if (avroWriter == null) {
                avroWriter = new GenericDatumWriter<>(schema.toAvroSchema());
            }
            return avroWriter;
        }

        GenericDatumReader<Object> avroReader()
        {
            if (avroReader == null) {
                avroReader = new GenericDatumReader<>(schema.toAvroSchema());
            }
            return avroReader;
        }

        // The element type of an ARRAY, or the value type of a MAP
        TypeNode element()
        {
            if (element == null) {
                element = new TypeNode(type == HoodieSchemaType.MAP ? schema.getValueType() : schema.getElementType());
            }
            return element;
        }

        TypeNode[] fields()
        {
            if (fields == null) {
                fields = schema.getFields().stream()
                        .map(HoodieSchemaField::schema)
                        .map(TypeNode::new)
                        .toArray(TypeNode[]::new);
            }
            return fields;
        }
    }
}
