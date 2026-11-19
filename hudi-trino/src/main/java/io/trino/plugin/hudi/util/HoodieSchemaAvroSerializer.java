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
package io.trino.plugin.hudi.util;

import io.airlift.slice.Slice;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.HudiUtil.getFieldFromSchema;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.apache.hudi.common.schema.HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER;

/**
 * Converts between Trino pages and Avro records for the merge path when columns carry {@link TrinoTypeMapper} types
 * ({@code hudi.schema-column-types.enabled}). The legacy {@link HudiAvroSerializer} stays the property-off path.
 * <p>
 * Every conversion is driven by the record field's {@code HoodieSchemaType}, compiled once per field from the field's
 * schema and the column's Trino type, and the switch over the type has no default branch so a new type does not
 * compile until it is mapped. Nullable unions are unwrapped exactly as {@link TrinoTypeMapper} unwraps them.
 * <ul>
 * <li>{@link #serialize} writes Avro-native values (Integer, Utf8, ByteBuffer, GenericFixed,
 * GenericEnumSymbol, GenericData.Array, Map, GenericData.Record), so a record survives the Avro binary round trip
 * payload-based merging puts it through ({@code BaseAvroPayload}) and the ordering-value conversions in
 * {@code HoodieAvroUtils}.</li>
 * <li>{@link #buildRecordInPage} accepts the shapes a record can arrive in: Avro-native values, from our own
 * {@link #serialize} or a classic Avro log block, and the java.time / BigDecimal / UUID values of a record decoded with
 * logical-type conversions.</li>
 * </ul>
 */
public class HoodieSchemaAvroSerializer
{
    private final List<HiveColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final PrefilledColumnValues prefilledColumnValues;
    // Null for a page-building-only serializer, as in HudiAvroSerializer: serialize() needs the record schema,
    // buildRecordInPage reads field positions off each record's own schema.
    private final Schema schema;
    private final int[] channelToFieldPosition;
    private final ValueToAvro[] channelToAvro;
    // Single-entry cache for buildRecordInPage: all records of a split share one schema instance. Prefilled
    // (partition / hidden) columns are not record fields; they get position -1.
    private Schema cachedRecordSchema;
    private int[] cachedFieldPositions;
    private AvroToBlock[] cachedToBlock;

    public HoodieSchemaAvroSerializer(List<HiveColumnHandle> columnHandles, PrefilledColumnValues prefilledColumnValues)
    {
        this.columnHandles = List.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        this.columnTypes = this.columnHandles.stream().map(HiveColumnHandle::getType).toList();
        this.prefilledColumnValues = requireNonNull(prefilledColumnValues, "prefilledColumnValues is null");
        this.schema = null;
        this.channelToFieldPosition = null;
        this.channelToAvro = null;
    }

    /**
     * Builds a serializer whose {@link #serialize} records carry {@code recordSchema}, the file-group reader's required
     * schema. Page channels are matched to record fields by name (case-insensitively); every projected column must be a
     * field of {@code recordSchema}.
     */
    public HoodieSchemaAvroSerializer(List<HiveColumnHandle> columnHandles, PrefilledColumnValues prefilledColumnValues, Schema recordSchema)
    {
        this.columnHandles = List.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        this.columnTypes = this.columnHandles.stream().map(HiveColumnHandle::getType).toList();
        this.prefilledColumnValues = requireNonNull(prefilledColumnValues, "prefilledColumnValues is null");
        this.schema = requireNonNull(recordSchema, "recordSchema is null");
        this.channelToFieldPosition = new int[this.columnHandles.size()];
        this.channelToAvro = new ValueToAvro[this.columnHandles.size()];
        for (int channel = 0; channel < this.columnHandles.size(); channel++) {
            String columnName = this.columnHandles.get(channel).getName();
            Schema.Field field = getFieldFromSchema(columnName, recordSchema);
            channelToFieldPosition[channel] = field.pos();
            channelToAvro[channel] = toAvro(field.name(), HoodieSchema.fromAvroSchema(field.schema()), columnTypes.get(channel));
        }
    }

    public IndexedRecord serialize(Page sourcePage, int position)
    {
        checkState(schema != null, "serialize() requires a serializer built with a record schema");
        IndexedRecord record = new GenericData.Record(schema);
        for (int channel = 0; channel < columnTypes.size(); channel++) {
            Block block = sourcePage.getBlock(channel);
            if (!block.isNull(position)) {
                record.put(channelToFieldPosition[channel], channelToAvro[channel].convert(block, position));
            }
        }
        return record;
    }

    public void buildRecordInPage(PageBuilder pageBuilder, IndexedRecord record)
    {
        pageBuilder.declarePosition();
        // The record may be wider than the projection: resolve fields by name off its own schema
        compileFor(record.getSchema());
        for (int channel = 0; channel < columnTypes.size(); channel++) {
            BlockBuilder output = pageBuilder.getBlockBuilder(channel);
            int fieldPosition = cachedFieldPositions[channel];
            if (fieldPosition < 0) {
                prefilledColumnValues.appendTo(columnHandles.get(channel), output);
                continue;
            }
            Object value = record.get(fieldPosition);
            if (value == null) {
                output.appendNull();
            }
            else {
                cachedToBlock[channel].append(value, output);
            }
        }
    }

    private void compileFor(Schema recordSchema)
    {
        if (cachedRecordSchema == recordSchema) {
            return;
        }
        int[] positions = new int[columnHandles.size()];
        AvroToBlock[] toBlock = new AvroToBlock[columnHandles.size()];
        for (int channel = 0; channel < columnHandles.size(); channel++) {
            HiveColumnHandle columnHandle = columnHandles.get(channel);
            if (prefilledColumnValues.isPrefilled(columnHandle)) {
                positions[channel] = -1;
                continue;
            }
            Schema.Field field = getFieldFromSchema(columnHandle.getName(), recordSchema);
            positions[channel] = field.pos();
            toBlock[channel] = toBlock(field.name(), HoodieSchema.fromAvroSchema(field.schema()), columnTypes.get(channel));
        }
        cachedFieldPositions = positions;
        cachedToBlock = toBlock;
        cachedRecordSchema = recordSchema;
    }

    /**
     * Reads a non-null Trino value as the Avro-native value of a field.
     */
    @FunctionalInterface
    private interface ValueToAvro
    {
        Object convert(Block block, int position);
    }

    /**
     * Appends a non-null Avro value of a field to a Trino block.
     */
    @FunctionalInterface
    private interface AvroToBlock
    {
        void append(Object value, BlockBuilder output);
    }

    // Trino block -> Avro value

    private static ValueToAvro toAvro(String fieldName, HoodieSchema fieldSchema, Type type)
    {
        HoodieSchema schema = nonNullSchema(fieldName, fieldSchema);
        return switch (schema.getType()) {
            case BOOLEAN -> (block, position) -> type.getBoolean(block, position);
            case INT -> (block, position) -> toIntExact(type.getLong(block, position));
            case LONG -> (block, position) -> type.getLong(block, position);
            case FLOAT -> (block, position) -> REAL.getFloat(block, position);
            case DOUBLE -> (block, position) -> type.getDouble(block, position);
            // Utf8, as an Avro log block decodes it: hudi-common compares ordering values of base and log rows, and of
            // delete records (AvroOrderingValueConverter turns String into Utf8), and String and Utf8 do not compare
            case STRING -> (block, position) -> new Utf8(type.getSlice(block, position).getBytes());
            case ENUM -> {
                Schema enumSchema = schema.toAvroSchema();
                yield (block, position) -> new GenericData.EnumSymbol(enumSchema, type.getSlice(block, position).toStringUtf8());
            }
            case BYTES -> (block, position) -> ByteBuffer.wrap(type.getSlice(block, position).getBytes());
            case FIXED -> {
                Schema fixedSchema = schema.toAvroSchema();
                yield (block, position) -> new GenericData.Fixed(fixedSchema, fixedBytes(fieldName, fixedSchema, type.getSlice(block, position)));
            }
            case UUID -> uuidToAvro(schema, type);
            case DECIMAL -> decimalToAvro(fieldName, (HoodieSchema.Decimal) schema, columnType(fieldName, schema, type, DecimalType.class));
            case DATE -> (block, position) -> toIntExact(type.getLong(block, position));
            case TIME -> timeToAvro((HoodieSchema.Time) schema, type);
            case TIMESTAMP -> timestampToAvro(fieldName, (HoodieSchema.Timestamp) schema, type);
            case ARRAY -> arrayToAvro(fieldName, schema, columnType(fieldName, schema, type, ArrayType.class));
            case MAP -> mapToAvro(fieldName, schema, columnType(fieldName, schema, type, MapType.class));
            case RECORD, BLOB, VARIANT -> recordToAvro(fieldName, schema, columnType(fieldName, schema, type, RowType.class));
            case VECTOR -> vectorToAvro(fieldName, (HoodieSchema.Vector) schema, columnType(fieldName, schema, type, ArrayType.class));
            // Unreachable: nonNullSchema rejects complex unions and getNonNullType() unwraps ["null", T]
            case UNION -> throw unsupported(fieldName, schema, "Trino has no union type");
            case NULL -> throw unsupported(fieldName, schema, "Trino has no null type");
        };
    }

    private static ValueToAvro uuidToAvro(HoodieSchema schema, Type type)
    {
        Schema uuidSchema = schema.toAvroSchema();
        // Avro's uuid annotates a string, or a fixed(16) holding the UUID's big-endian bytes, which is Trino's uuid value
        if (uuidSchema.getType() == Schema.Type.FIXED) {
            return (block, position) -> new GenericData.Fixed(uuidSchema, type.getSlice(block, position).getBytes());
        }
        return (block, position) -> new Utf8(trinoUuidToJavaUuid(type.getSlice(block, position)).toString());
    }

    private static ValueToAvro decimalToAvro(String fieldName, HoodieSchema.Decimal schema, DecimalType type)
    {
        if (schema.isFixed()) {
            Schema fixedSchema = schema.toAvroSchema();
            int fixedSize = schema.getFixedSize();
            return (block, position) -> new GenericData.Fixed(fixedSchema, signExtend(fieldName, unscaledValue(type, block, position), fixedSize));
        }
        // Avro's bytes-backed decimal: the unscaled value's minimal big-endian two's complement
        return (block, position) -> ByteBuffer.wrap(unscaledValue(type, block, position).toByteArray());
    }

    private static BigInteger unscaledValue(DecimalType type, Block block, int position)
    {
        if (type.isShort()) {
            return BigInteger.valueOf(type.getLong(block, position));
        }
        return ((Int128) type.getObject(block, position)).toBigInteger();
    }

    /**
     * Pads the unscaled value to the fixed size with its sign byte, which is how Avro's DecimalConversion lays a fixed
     * decimal out.
     */
    private static byte[] signExtend(String fieldName, BigInteger unscaled, int fixedSize)
    {
        byte[] unscaledBytes = unscaled.toByteArray();
        if (unscaledBytes.length > fixedSize) {
            throw new TrinoException(HUDI_BAD_DATA, "Decimal value of field %s does not fit its %s-byte fixed".formatted(fieldName, fixedSize));
        }
        byte[] fixed = new byte[fixedSize];
        int padding = fixedSize - unscaledBytes.length;
        Arrays.fill(fixed, 0, padding, unscaled.signum() < 0 ? (byte) -1 : (byte) 0);
        System.arraycopy(unscaledBytes, 0, fixed, padding, unscaledBytes.length);
        return fixed;
    }

    private static ValueToAvro timeToAvro(HoodieSchema.Time schema, Type type)
    {
        // Trino holds time as picoseconds of the day; Avro holds time-millis as an int and time-micros as a long
        return switch (schema.getPrecision()) {
            case MILLIS -> (block, position) -> toIntExact(type.getLong(block, position) / PICOSECONDS_PER_MILLISECOND);
            case MICROS -> (block, position) -> type.getLong(block, position) / PICOSECONDS_PER_MICROSECOND;
        };
    }

    private static ValueToAvro timestampToAvro(String fieldName, HoodieSchema.Timestamp schema, Type type)
    {
        if (schema.isUtcAdjusted()) {
            // An instant: Avro counts from the epoch in UTC, which is what a timestamp with time zone holds
            TimestampWithTimeZoneType timestampType = columnType(fieldName, schema, type, TimestampWithTimeZoneType.class);
            return switch (schema.getPrecision()) {
                case MILLIS -> (block, position) -> floorDiv(epochMicrosUtc(timestampType, block, position), MICROSECONDS_PER_MILLISECOND);
                case MICROS -> (block, position) -> epochMicrosUtc(timestampType, block, position);
            };
        }
        // A wall-clock time: Avro counts it from the epoch as if it were UTC, which is what a timestamp holds
        TimestampType timestampType = columnType(fieldName, schema, type, TimestampType.class);
        return switch (schema.getPrecision()) {
            case MILLIS -> (block, position) -> floorDiv(localEpochMicros(timestampType, block, position), MICROSECONDS_PER_MILLISECOND);
            case MICROS -> (block, position) -> localEpochMicros(timestampType, block, position);
        };
    }

    private static long epochMicrosUtc(TimestampWithTimeZoneType type, Block block, int position)
    {
        if (type.isShort()) {
            return multiplyExact(unpackMillisUtc(type.getLong(block, position)), MICROSECONDS_PER_MILLISECOND);
        }
        LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) type.getObject(block, position);
        return multiplyExact(timestamp.getEpochMillis(), MICROSECONDS_PER_MILLISECOND) + timestamp.getPicosOfMilli() / PICOSECONDS_PER_MICROSECOND;
    }

    private static long localEpochMicros(TimestampType type, Block block, int position)
    {
        if (type.isShort()) {
            return type.getLong(block, position);
        }
        return ((LongTimestamp) type.getObject(block, position)).getEpochMicros();
    }

    private static ValueToAvro arrayToAvro(String fieldName, HoodieSchema schema, ArrayType type)
    {
        Schema arraySchema = schema.toAvroSchema();
        ValueToAvro elementToAvro = toAvro(fieldName + "[]", schema.getElementType(), type.getElementType());
        return (block, position) -> {
            Block elements = type.getObject(block, position);
            GenericData.Array<Object> array = new GenericData.Array<>(elements.getPositionCount(), arraySchema);
            for (int element = 0; element < elements.getPositionCount(); element++) {
                array.add(elements.isNull(element) ? null : elementToAvro.convert(elements, element));
            }
            return array;
        };
    }

    private static ValueToAvro mapToAvro(String fieldName, HoodieSchema schema, MapType type)
    {
        Type keyType = type.getKeyType();
        ValueToAvro valueToAvro = toAvro(fieldName + "{}", schema.getValueType(), type.getValueType());
        return (block, position) -> {
            SqlMap sqlMap = type.getObject(block, position);
            int rawOffset = sqlMap.getRawOffset();
            Block keys = sqlMap.getRawKeyBlock();
            Block values = sqlMap.getRawValueBlock();
            Map<String, Object> map = HashMap.newHashMap(sqlMap.getSize());
            for (int entry = rawOffset; entry < rawOffset + sqlMap.getSize(); entry++) {
                map.put(keyType.getSlice(keys, entry).toStringUtf8(), values.isNull(entry) ? null : valueToAvro.convert(values, entry));
            }
            return map;
        };
    }

    private static ValueToAvro recordToAvro(String fieldName, HoodieSchema schema, RowType type)
    {
        Schema recordSchema = schema.toAvroSchema();
        List<HoodieSchemaField> fields = schema.getFields();
        int[] rowFieldIndexes = new int[fields.size()];
        ValueToAvro[] fieldToAvro = new ValueToAvro[fields.size()];
        for (int field = 0; field < fields.size(); field++) {
            HoodieSchemaField schemaField = fields.get(field);
            rowFieldIndexes[field] = rowFieldIndex(type, schemaField.name());
            if (rowFieldIndexes[field] >= 0) {
                fieldToAvro[field] = toAvro(fieldName + "." + schemaField.name(), schemaField.schema(), type.getFields().get(rowFieldIndexes[field]).getType());
            }
        }
        return (block, position) -> {
            SqlRow row = type.getObject(block, position);
            int rawIndex = row.getRawIndex();
            GenericData.Record record = new GenericData.Record(recordSchema);
            for (int field = 0; field < rowFieldIndexes.length; field++) {
                if (rowFieldIndexes[field] < 0) {
                    continue;
                }
                Block fieldBlock = row.getRawFieldBlock(rowFieldIndexes[field]);
                if (!fieldBlock.isNull(rawIndex)) {
                    record.put(field, fieldToAvro[field].convert(fieldBlock, rawIndex));
                }
            }
            return record;
        };
    }

    private static ValueToAvro vectorToAvro(String fieldName, HoodieSchema.Vector schema, ArrayType type)
    {
        Schema vectorSchema = schema.toAvroSchema();
        int dimension = schema.getDimension();
        HoodieSchema.Vector.VectorElementType elementType = schema.getVectorElementType();
        return (block, position) -> {
            Block elements = type.getObject(block, position);
            if (elements.getPositionCount() != dimension) {
                throw new TrinoException(HUDI_BAD_DATA, "Vector field %s has dimension %s but holds %s elements".formatted(fieldName, dimension, elements.getPositionCount()));
            }
            ByteBuffer bytes = ByteBuffer.allocate(schema.getFixedSize()).order(VECTOR_BYTE_ORDER);
            for (int element = 0; element < dimension; element++) {
                if (elements.isNull(element)) {
                    throw new TrinoException(HUDI_BAD_DATA, "Vector field %s holds a null element".formatted(fieldName));
                }
                switch (elementType) {
                    case FLOAT -> bytes.putFloat(REAL.getFloat(elements, element));
                    case DOUBLE -> bytes.putDouble(DOUBLE.getDouble(elements, element));
                    case INT8 -> bytes.put(TINYINT.getByte(elements, element));
                }
            }
            return new GenericData.Fixed(vectorSchema, bytes.array());
        };
    }

    private static byte[] fixedBytes(String fieldName, Schema fixedSchema, Slice value)
    {
        if (value.length() != fixedSchema.getFixedSize()) {
            throw new TrinoException(HUDI_BAD_DATA, "Fixed field %s holds %s bytes, but its size is %s".formatted(fieldName, value.length(), fixedSchema.getFixedSize()));
        }
        return value.getBytes();
    }

    // Avro value -> Trino block

    private static AvroToBlock toBlock(String fieldName, HoodieSchema fieldSchema, Type type)
    {
        HoodieSchema schema = nonNullSchema(fieldName, fieldSchema);
        return switch (schema.getType()) {
            case BOOLEAN -> (value, output) -> type.writeBoolean(output, asBoolean(fieldName, schema, value));
            case INT -> (value, output) -> type.writeLong(output, asNumber(fieldName, schema, value).intValue());
            case LONG -> (value, output) -> type.writeLong(output, asNumber(fieldName, schema, value).longValue());
            case FLOAT -> (value, output) -> REAL.writeLong(output, floatToRawIntBits(asNumber(fieldName, schema, value).floatValue()));
            case DOUBLE -> (value, output) -> type.writeDouble(output, asNumber(fieldName, schema, value).doubleValue());
            case STRING -> (value, output) -> type.writeSlice(output, asUtf8Slice(fieldName, schema, value));
            case ENUM -> (value, output) -> type.writeSlice(output, enumSymbol(fieldName, schema, value));
            case BYTES -> (value, output) -> type.writeSlice(output, asBinary(fieldName, schema, value));
            case FIXED -> (value, output) -> type.writeSlice(output, asBinary(fieldName, schema, value));
            case UUID -> (value, output) -> type.writeSlice(output, uuid(fieldName, schema, value));
            case DECIMAL -> decimalToBlock(fieldName, (HoodieSchema.Decimal) schema, columnType(fieldName, schema, type, DecimalType.class));
            case DATE -> (value, output) -> type.writeLong(output, epochDay(fieldName, schema, value));
            case TIME -> timeToBlock(fieldName, (HoodieSchema.Time) schema, type);
            case TIMESTAMP -> timestampToBlock(fieldName, (HoodieSchema.Timestamp) schema, type);
            case ARRAY -> arrayToBlock(fieldName, schema, columnType(fieldName, schema, type, ArrayType.class));
            case MAP -> mapToBlock(fieldName, schema, columnType(fieldName, schema, type, MapType.class));
            case RECORD, BLOB, VARIANT -> recordToBlock(fieldName, schema, columnType(fieldName, schema, type, RowType.class));
            case VECTOR -> vectorToBlock(fieldName, schema, columnType(fieldName, schema, type, ArrayType.class));
            // Unreachable: nonNullSchema rejects complex unions and getNonNullType() unwraps ["null", T]
            case UNION -> throw unsupported(fieldName, schema, "Trino has no union type");
            case NULL -> throw unsupported(fieldName, schema, "Trino has no null type");
        };
    }

    private static AvroToBlock decimalToBlock(String fieldName, HoodieSchema.Decimal schema, DecimalType type)
    {
        return (value, output) -> {
            BigDecimal decimal = switch (value) {
                case BigDecimal javaDecimal -> javaDecimal;
                case ByteBuffer buffer -> new BigDecimal(new BigInteger(bytesOf(buffer)), schema.getScale());
                case GenericFixed fixed -> new BigDecimal(new BigInteger(fixed.bytes()), schema.getScale());
                default -> throw unexpectedValue(fieldName, schema, value);
            };
            // The record's scale may not be the column's, as in a record written in an older schema: rescale to the
            // column's, so the unscaled value the block holds means the same number
            BigInteger unscaled = rescale(fieldName, decimal, type.getScale()).unscaledValue();
            if (unscaled.abs().compareTo(BigInteger.TEN.pow(type.getPrecision())) >= 0) {
                throw new TrinoException(HUDI_BAD_DATA, "Decimal value %s of field %s does not fit %s".formatted(decimal, fieldName, type));
            }
            if (type.isShort()) {
                type.writeLong(output, unscaled.longValueExact());
            }
            else {
                type.writeObject(output, Int128.valueOf(unscaled));
            }
        };
    }

    private static BigDecimal rescale(String fieldName, BigDecimal decimal, int scale)
    {
        try {
            return decimal.setScale(scale, UNNECESSARY);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(HUDI_BAD_DATA, "Decimal value %s of field %s does not have scale %s".formatted(decimal, fieldName, scale), e);
        }
    }

    private static AvroToBlock timeToBlock(String fieldName, HoodieSchema.Time schema, Type type)
    {
        return switch (schema.getPrecision()) {
            case MILLIS -> (value, output) -> type.writeLong(output, picosOfDay(fieldName, schema, value, PICOSECONDS_PER_MILLISECOND));
            case MICROS -> (value, output) -> type.writeLong(output, picosOfDay(fieldName, schema, value, PICOSECONDS_PER_MICROSECOND));
        };
    }

    private static long picosOfDay(String fieldName, HoodieSchema schema, Object value, long picosPerUnit)
    {
        return switch (value) {
            case Number units -> units.longValue() * picosPerUnit;
            case LocalTime time -> time.toNanoOfDay() * PICOSECONDS_PER_NANOSECOND;
            default -> throw unexpectedValue(fieldName, schema, value);
        };
    }

    private static AvroToBlock timestampToBlock(String fieldName, HoodieSchema.Timestamp schema, Type type)
    {
        if (schema.isUtcAdjusted()) {
            TimestampWithTimeZoneType timestampType = columnType(fieldName, schema, type, TimestampWithTimeZoneType.class);
            return (value, output) -> {
                long epochMicros = switch (value) {
                    case Number units -> epochMicros(schema, units);
                    case Instant instant -> multiplyExact(instant.getEpochSecond(), MICROSECONDS_PER_SECOND) + instant.getNano() / NANOSECONDS_PER_MICROSECOND;
                    default -> throw unexpectedValue(fieldName, schema, value);
                };
                long epochMillis = floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
                if (timestampType.isShort()) {
                    timestampType.writeLong(output, packDateTimeWithZone(epochMillis, UTC_KEY));
                }
                else {
                    int picosOfMilli = floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_MICROSECOND;
                    timestampType.writeObject(output, LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, UTC_KEY));
                }
            };
        }
        TimestampType timestampType = columnType(fieldName, schema, type, TimestampType.class);
        return (value, output) -> {
            long epochMicros = switch (value) {
                case Number units -> epochMicros(schema, units);
                case LocalDateTime dateTime -> multiplyExact(dateTime.toEpochSecond(UTC), MICROSECONDS_PER_SECOND) + dateTime.getNano() / NANOSECONDS_PER_MICROSECOND;
                default -> throw unexpectedValue(fieldName, schema, value);
            };
            if (timestampType.isShort()) {
                timestampType.writeLong(output, epochMicros);
            }
            else {
                timestampType.writeObject(output, new LongTimestamp(epochMicros, 0));
            }
        };
    }

    private static long epochMicros(HoodieSchema.Timestamp schema, Number units)
    {
        return switch (schema.getPrecision()) {
            case MILLIS -> multiplyExact(units.longValue(), MICROSECONDS_PER_MILLISECOND);
            case MICROS -> units.longValue();
        };
    }

    private static AvroToBlock arrayToBlock(String fieldName, HoodieSchema schema, ArrayType type)
    {
        AvroToBlock elementToBlock = toBlock(fieldName + "[]", schema.getElementType(), type.getElementType());
        return (value, output) -> {
            if (!(value instanceof Collection<?> elements)) {
                throw unexpectedValue(fieldName, schema, value);
            }
            ((ArrayBlockBuilder) output).buildEntry(elementBuilder -> {
                for (Object element : elements) {
                    if (element == null) {
                        elementBuilder.appendNull();
                    }
                    else {
                        elementToBlock.append(element, elementBuilder);
                    }
                }
            });
        };
    }

    private static AvroToBlock mapToBlock(String fieldName, HoodieSchema schema, MapType type)
    {
        Type keyType = type.getKeyType();
        AvroToBlock valueToBlock = toBlock(fieldName + "{}", schema.getValueType(), type.getValueType());
        return (value, output) -> {
            if (!(value instanceof Map<?, ?> map)) {
                throw unexpectedValue(fieldName, schema, value);
            }
            ((MapBlockBuilder) output).buildEntry((keyBuilder, valueBuilder) -> {
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    keyType.writeSlice(keyBuilder, asUtf8Slice(fieldName, schema, entry.getKey()));
                    if (entry.getValue() == null) {
                        valueBuilder.appendNull();
                    }
                    else {
                        valueToBlock.append(entry.getValue(), valueBuilder);
                    }
                }
            });
        };
    }

    private static AvroToBlock recordToBlock(String fieldName, HoodieSchema schema, RowType type)
    {
        Schema recordSchema = schema.toAvroSchema();
        List<RowType.Field> rowFields = type.getFields();
        int[] fieldPositions = new int[rowFields.size()];
        AvroToBlock[] fieldToBlock = new AvroToBlock[rowFields.size()];
        for (int field = 0; field < rowFields.size(); field++) {
            RowType.Field rowField = rowFields.get(field);
            fieldPositions[field] = rowField.getName().map(name -> fieldPosition(recordSchema, name)).orElse(-1);
            if (fieldPositions[field] >= 0) {
                Schema.Field avroField = recordSchema.getFields().get(fieldPositions[field]);
                fieldToBlock[field] = toBlock(fieldName + "." + avroField.name(), HoodieSchema.fromAvroSchema(avroField.schema()), rowField.getType());
            }
        }
        return (value, output) -> {
            if (!(value instanceof IndexedRecord record)) {
                throw unexpectedValue(fieldName, schema, value);
            }
            // A nested record normally carries the very schema instance the field declares; resolve by name otherwise
            int[] positions = record.getSchema() == recordSchema ? fieldPositions : fieldPositions(record.getSchema(), rowFields);
            ((RowBlockBuilder) output).buildEntry(fieldBuilders -> {
                for (int field = 0; field < positions.length; field++) {
                    Object fieldValue = positions[field] < 0 ? null : record.get(positions[field]);
                    if (fieldValue == null) {
                        fieldBuilders.get(field).appendNull();
                    }
                    else {
                        fieldToBlock[field].append(fieldValue, fieldBuilders.get(field));
                    }
                }
            });
        };
    }

    private static int[] fieldPositions(Schema recordSchema, List<RowType.Field> rowFields)
    {
        int[] positions = new int[rowFields.size()];
        for (int field = 0; field < rowFields.size(); field++) {
            positions[field] = rowFields.get(field).getName().map(name -> fieldPosition(recordSchema, name)).orElse(-1);
        }
        return positions;
    }

    private static AvroToBlock vectorToBlock(String fieldName, HoodieSchema schema, ArrayType type)
    {
        return (value, output) -> ParquetReadConversions.writeVector(type, asBinary(fieldName, schema, value), output);
    }

    // Avro value shapes

    private static boolean asBoolean(String fieldName, HoodieSchema schema, Object value)
    {
        if (value instanceof Boolean bool) {
            return bool;
        }
        throw unexpectedValue(fieldName, schema, value);
    }

    private static Number asNumber(String fieldName, HoodieSchema schema, Object value)
    {
        if (value instanceof Number number) {
            return number;
        }
        throw unexpectedValue(fieldName, schema, value);
    }

    private static Slice asUtf8Slice(String fieldName, HoodieSchema schema, Object value)
    {
        return switch (value) {
            // Utf8 holds the encoded bytes already; the block builder copies them
            case Utf8 utf8 -> wrappedBuffer(utf8.getBytes(), 0, utf8.getByteLength());
            case CharSequence chars -> utf8Slice(chars.toString());
            default -> throw unexpectedValue(fieldName, schema, value);
        };
    }

    private static Slice enumSymbol(String fieldName, HoodieSchema schema, Object value)
    {
        if (value instanceof GenericEnumSymbol<?> symbol) {
            return utf8Slice(symbol.toString());
        }
        return asUtf8Slice(fieldName, schema, value);
    }

    private static Slice asBinary(String fieldName, HoodieSchema schema, Object value)
    {
        return switch (value) {
            case ByteBuffer buffer -> wrappedBuffer(bytesOf(buffer));
            case GenericFixed fixed -> wrappedBuffer(fixed.bytes());
            default -> throw unexpectedValue(fieldName, schema, value);
        };
    }

    private static Slice uuid(String fieldName, HoodieSchema schema, Object value)
    {
        return switch (value) {
            case UUID uuid -> javaUuidToTrinoUuid(uuid);
            case CharSequence chars -> javaUuidToTrinoUuid(parseUuid(fieldName, chars.toString()));
            // A fixed(16) uuid holds the UUID's big-endian bytes, which is Trino's uuid value
            case GenericFixed fixed when fixed.bytes().length == 16 -> wrappedBuffer(fixed.bytes());
            default -> throw unexpectedValue(fieldName, schema, value);
        };
    }

    private static UUID parseUuid(String fieldName, String text)
    {
        try {
            return UUID.fromString(text);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(HUDI_BAD_DATA, "Invalid UUID in field %s: %s".formatted(fieldName, text), e);
        }
    }

    private static long epochDay(String fieldName, HoodieSchema schema, Object value)
    {
        return switch (value) {
            case Number days -> days.intValue();
            case LocalDate date -> date.toEpochDay();
            default -> throw unexpectedValue(fieldName, schema, value);
        };
    }

    private static byte[] bytesOf(ByteBuffer buffer)
    {
        // Read through a duplicate: the record may be serialized again, which reads the buffer from its position
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    // Shared helpers

    /**
     * Unwraps a nullable union the way {@link TrinoTypeMapper#toTypeSystem} does; Trino carries nullability on the
     * value, not in the type.
     */
    private static HoodieSchema nonNullSchema(String fieldName, HoodieSchema schema)
    {
        if (schema.isComplexUnion()) {
            throw unsupported(fieldName, schema, "Trino has no union type");
        }
        return schema.getNonNullType();
    }

    private static int rowFieldIndex(RowType type, String name)
    {
        List<RowType.Field> fields = type.getFields();
        for (int field = 0; field < fields.size(); field++) {
            if (fields.get(field).getName().map(name::equalsIgnoreCase).orElse(false)) {
                return field;
            }
        }
        return -1;
    }

    private static int fieldPosition(Schema recordSchema, String name)
    {
        Schema.Field field = recordSchema.getField(name);
        if (field != null) {
            return field.pos();
        }
        for (Schema.Field candidate : recordSchema.getFields()) {
            if (candidate.name().equalsIgnoreCase(name)) {
                return candidate.pos();
            }
        }
        return -1;
    }

    private static <T extends Type> T columnType(String fieldName, HoodieSchema schema, Type type, Class<T> expectedType)
    {
        if (!expectedType.isInstance(type)) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Field %s of Hudi type %s cannot be held by Trino type %s".formatted(fieldName, schema.getType(), type));
        }
        return expectedType.cast(type);
    }

    private static TrinoException unexpectedValue(String fieldName, HoodieSchema schema, Object value)
    {
        return new TrinoException(GENERIC_INTERNAL_ERROR, "Field %s of Hudi type %s cannot be read from a value of %s".formatted(fieldName, schema.getType(), value.getClass().getName()));
    }

    private static TrinoException unsupported(String fieldName, HoodieSchema schema, String reason)
    {
        return new TrinoException(NOT_SUPPORTED, "No Trino type for Hudi type %s (%s) of field %s: %s".formatted(schema.getType(), schema, fieldName, reason));
    }
}
