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

import io.trino.plugin.hudi.util.HoodieSchemaAvroSerializer;
import io.trino.plugin.hudi.util.HoodieSchemaAvroSerializer.AvroToBlock;
import io.trino.plugin.hudi.util.HoodieSchemaAvroSerializer.ValueToAvro;
import io.trino.plugin.hudi.util.TrinoTypeMapper;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hudi.common.avro.AvroOrderingValueConverter;
import org.apache.hudi.common.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieAvroSchemaCache;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.serialization.RecordSerializer;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.AvroJavaTypeConverter;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.exception.HoodieException;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.UnaryOperator;

import static io.trino.plugin.hudi.util.HoodieSchemaAvroSerializer.nonNullSchema;
import static io.trino.plugin.hudi.util.HoodieSchemaAvroSerializer.rowFieldIndex;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static java.util.Objects.requireNonNull;
import static org.apache.hudi.common.avro.HoodieAvroUtils.avroToBytes;
import static org.apache.hudi.common.avro.HoodieAvroUtils.bytesToAvro;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;

/**
 * The {@link RecordContext} of {@link TrinoRow}s, the records the file-group reader merges when
 * {@code hudi.schema-column-types.enabled} is on. A row is resolved by its own layout, as {@code AvroRecordContext}
 * resolves a record by its own schema; the schema hudi-common passes along is not consulted.
 * <p>
 * Avro appears only where hudi-common hands over Avro: {@link #convertAvroRecord} (classic Avro log blocks, payload
 * results) and {@link #convertToAvroRecord} (payload merges, spilling). Both reuse the per-field converters of
 * {@link HoodieSchemaAvroSerializer}.
 * <p>
 * {@link #getValue} returns the value classes {@code AvroRecordContext} orders by: the values
 * {@code HoodieAvroUtils.convertValueForAvroLogicalTypes} makes of a record decoded from Avro binary. Ordering values
 * therefore compare with delete-block ordering values, which {@link #convertOrderingValueToEngineType} converts with
 * the same {@link AvroOrderingValueConverter} as the Avro path, and {@code OrderingValues.isSameClass} agrees.
 * <p>
 * This is not {@code AvroRecordContext.getValue}, which returns the record's raw Avro value (an Integer of days for a
 * date, a ByteBuffer or GenericFixed for a decimal). Callers other than ordering see the difference: a record key in
 * data fields (meta fields not populated) reads as the writer's key generator builds it, "2024-01-01" for a date and
 * the decimal's plain text, where the Avro context reads the raw value's toString() ("19723" for that date).
 * <table>
 * <caption>getValue value class per HoodieSchemaType (nullable unions unwrapped; a null value is null)</caption>
 * <tr><th>HoodieSchemaType</th><th>Value class</th></tr>
 * <tr><td>BOOLEAN, INT, LONG, FLOAT, DOUBLE</td><td>Boolean, Integer, Long, Float, Double</td></tr>
 * <tr><td>STRING</td><td>org.apache.avro.util.Utf8</td></tr>
 * <tr><td>ENUM</td><td>GenericData.EnumSymbol</td></tr>
 * <tr><td>BYTES</td><td>java.nio.ByteBuffer</td></tr>
 * <tr><td>FIXED</td><td>GenericData.Fixed</td></tr>
 * <tr><td>UUID</td><td>Utf8 of the canonical text over a string; GenericData.Fixed over a fixed(16)</td></tr>
 * <tr><td>DECIMAL</td><td>java.math.BigDecimal with the schema's scale</td></tr>
 * <tr><td>DATE</td><td>java.time.LocalDate</td></tr>
 * <tr><td>TIME millis, micros</td><td>Integer millis of the day, Long micros of the day</td></tr>
 * <tr><td>TIMESTAMP millis, micros, UTC-adjusted</td><td>Long epoch millis, Long epoch micros; as an ordering value
 * with consistent logical timestamps on, a java.sql.Timestamp of the epoch millis (micros truncated)</td></tr>
 * <tr><td>TIMESTAMP millis, micros, local</td><td>Long epoch millis, Long epoch micros</td></tr>
 * <tr><td>ARRAY</td><td>GenericData.Array of the elements' Avro values</td></tr>
 * <tr><td>MAP</td><td>java.util.Map with String keys</td></tr>
 * <tr><td>RECORD, BLOB, VARIANT</td><td>GenericData.Record</td></tr>
 * <tr><td>VECTOR</td><td>GenericData.Fixed of the little-endian elements</td></tr>
 * </table>
 * Values nested in ARRAY, MAP and RECORD are those {@link #convertToAvroRecord} writes (String for strings). Only the
 * UTC-adjusted timestamps under {@code hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled}
 * differ between {@link #getValue} and {@link #getOrderingValue}: getValue stays lossless, because partial-update
 * merging writes getValue results back into rows through {@link #constructEngineRecord}.
 * <p>
 * Not thread safe: one instance serves one file-group read.
 */
public class TrinoRecordContext
        extends RecordContext<TrinoRow>
{
    private static final FieldReader MISSING_FIELD = _ -> null;
    private static final int[] REWRITE = new int[0];

    private final TrinoTypeMapper typeMapper;
    private final boolean consistentLogicalTimestampEnabled;
    private final boolean recordKeyPopulated;
    // Layouts typed by the mapper, for rows built from values; page-read rows carry their page source's layout
    private final Map<HoodieSchema, TrinoRow.Layout> schemaLayouts = new IdentityHashMap<>();
    private final Map<TrinoRow.Layout, LayoutAccessors> layoutAccessors = new IdentityHashMap<>();
    private LayoutAccessors lastAccessors;
    // The records of one Avro log block share their schema instance; resolving it once keeps the layout, and every
    // cache keyed by its identity, stable even when the bounded global schema cache evicts the interned schema
    private Schema lastAvroSchema;
    private TrinoRow.Layout lastAvroLayout;
    private AvroOrderingValueConverter orderingValueConverter;

    /**
     * @param mergeProps the reader's properties with the table's merge properties layered in
     * ({@code ConfigUtils.getMergeProps}), where the consistent-logical-timestamp setting is read from
     */
    public TrinoRecordContext(HoodieTableConfig tableConfig, TypedProperties mergeProps, TrinoTypeMapper typeMapper)
    {
        super(tableConfig, new AvroJavaTypeConverter());
        this.typeMapper = requireNonNull(typeMapper, "typeMapper is null");
        this.consistentLogicalTimestampEnabled = ConfigUtils.shouldKeepConsistentLogicalTimestamp(mergeProps);
        this.recordKeyPopulated = tableConfig.isRecordKeyPopulated();
    }

    /**
     * The layout of rows of {@code schema} built from values, each field typed by {@link TrinoTypeMapper}. Cached by
     * schema identity; hudi-common interns the schemas it hands over.
     */
    public TrinoRow.Layout layout(HoodieSchema schema)
    {
        return schemaLayouts.computeIfAbsent(schema, _ -> new TrinoRow.Layout(
                schema,
                schema.getFields().stream()
                        .map(field -> typeMapper.toTypeSystem(field.schema()))
                        .toList()));
    }

    /**
     * Initializes the conversion of delete-block ordering values, as {@code AvroRecordContext.initOrderingValueConverter}.
     */
    public void initOrderingValueConverter(HoodieSchema dataSchema, List<String> orderingFieldNames)
    {
        orderingValueConverter = AvroOrderingValueConverter.create(dataSchema, orderingFieldNames, consistentLogicalTimestampEnabled);
    }

    @Override
    public Object getValue(TrinoRow row, HoodieSchema schema, String fieldName)
    {
        return accessors(row.getLayout()).valueReader(fieldName).read(row);
    }

    @Override
    public Comparable getOrderingValue(TrinoRow row, HoodieSchema schema, List<String> orderingFieldNames)
    {
        if (orderingFieldNames.isEmpty()) {
            return OrderingValues.getDefault();
        }
        return OrderingValues.create(orderingFieldNames, field -> orderingValue(row, field));
    }

    @Override
    public Comparable getOrderingValue(TrinoRow row, HoodieSchema schema, String[] orderingFieldNames)
    {
        if (orderingFieldNames == null || orderingFieldNames.length == 0) {
            return OrderingValues.getDefault();
        }
        return OrderingValues.create(orderingFieldNames, field -> orderingValue(row, field));
    }

    private Comparable orderingValue(TrinoRow row, String fieldName)
    {
        Object value = accessors(row.getLayout()).orderingReader(fieldName).read(row);
        return value == null ? OrderingValues.getDefault() : ensureComparability(value);
    }

    @Override
    public String getMetaFieldValue(TrinoRow row, int pos)
    {
        if (row.isNull(pos)) {
            return null;
        }
        return row.getLayout().getType(pos).getSlice(row.getBlock(pos), row.getPosition()).toStringUtf8();
    }

    @Override
    public String getRecordKey(TrinoRow row, HoodieSchema schema)
    {
        // The key of every base row: read the meta column straight to a String instead of through a Utf8 value
        if (recordKeyPopulated) {
            int keyField = row.getLayout().fieldIndex(RECORD_KEY_METADATA_FIELD);
            if (keyField >= 0 && !row.isNull(keyField)) {
                return getMetaFieldValue(row, keyField);
            }
        }
        return super.getRecordKey(row, schema);
    }

    @Override
    public Comparable convertValueToEngineType(Comparable value)
    {
        // As AvroRecordContext: getValue holds strings as Utf8
        if (value instanceof String string) {
            return new Utf8(string);
        }
        return value;
    }

    @Override
    public Comparable convertOrderingValueToEngineType(Comparable value)
    {
        if (orderingValueConverter == null) {
            return super.convertOrderingValueToEngineType(value);
        }
        return orderingValueConverter.convert(value);
    }

    @Override
    public TrinoRow seal(HoodieSchema schema, TrinoRow row)
    {
        // Trino blocks never change once built
        return row;
    }

    @Override
    public TrinoRow toBinaryRow(HoodieSchema schema, TrinoRow row)
    {
        // A retained row must not pin the whole page it was read from
        return row.compact();
    }

    @Override
    public TrinoRow convertAvroRecord(IndexedRecord avroRecord)
    {
        TrinoRow.Layout layout = avroLayout(avroRecord.getSchema());
        AvroToBlock[] writers = accessors(layout).blockWriters();
        Block[] blocks = new Block[layout.getFieldCount()];
        for (int field = 0; field < blocks.length; field++) {
            blocks[field] = singleValueBlock(layout.getType(field), writers[field], avroRecord.get(field));
        }
        return TrinoRow.ofSingleValueBlocks(layout, blocks);
    }

    @Override
    public GenericRecord convertToAvroRecord(TrinoRow row, HoodieSchema schema)
    {
        // As AvroRecordContext, the record keeps the row's own schema
        LayoutAccessors accessors = accessors(row.getLayout());
        ValueToAvro[] readers = accessors.avroReaders();
        GenericData.Record record = new GenericData.Record(accessors.avroSchema());
        for (int field = 0; field < readers.length; field++) {
            if (!row.isNull(field)) {
                record.put(field, readers[field].convert(row.getBlock(field), row.getPosition()));
            }
        }
        return record;
    }

    @Override
    public TrinoRow extractDataFromRecord(HoodieRecord record, HoodieSchema schema, Properties properties)
    {
        try {
            return record.toIndexedRecord(schema, properties)
                    .map(HoodieAvroIndexedRecord::getData)
                    .map(this::convertAvroRecord)
                    .orElse(null);
        }
        catch (IOException e) {
            throw new HoodieException("Failed to extract data from record: " + record, e);
        }
    }

    @Override
    public TrinoRow constructEngineRecord(HoodieSchema recordSchema, Object[] fieldValues)
    {
        TrinoRow.Layout layout = layout(recordSchema);
        AvroToBlock[] writers = accessors(layout).blockWriters();
        Block[] blocks = new Block[layout.getFieldCount()];
        for (int field = 0; field < blocks.length; field++) {
            blocks[field] = singleValueBlock(layout.getType(field), writers[field], fieldValues[field]);
        }
        return TrinoRow.ofSingleValueBlocks(layout, blocks);
    }

    @Override
    public TrinoRow mergeWithEngineRecord(HoodieSchema schema, Map<Integer, Object> updateValues, BufferedRecord<TrinoRow> baseRecord)
    {
        // As AvroRecordContext, the update positions are positions of the base row's own schema
        TrinoRow base = baseRecord.getRecord().compact();
        TrinoRow.Layout layout = base.getLayout();
        AvroToBlock[] writers = accessors(layout).blockWriters();
        Block[] blocks = new Block[layout.getFieldCount()];
        for (int field = 0; field < blocks.length; field++) {
            if (updateValues.containsKey(field)) {
                blocks[field] = singleValueBlock(layout.getType(field), writers[field], updateValues.get(field));
            }
            else {
                blocks[field] = base.getBlock(field);
            }
        }
        return TrinoRow.ofSingleValueBlocks(layout, blocks);
    }

    @Override
    public TrinoRow getDeleteRow(String recordKey)
    {
        // As AvroRecordContext; the connector never asks the file-group reader to emit deletes
        throw new UnsupportedOperationException("Not supported for " + getClass().getSimpleName());
    }

    @Override
    public HoodieRecord<TrinoRow> constructHoodieRecord(BufferedRecord<TrinoRow> bufferedRecord, String partitionPath)
    {
        // Only the file-group reader's HoodieRecord iterators, the merged log scanner and write paths build HoodieRecords;
        // the connector reads engine records. Tables with a custom HoodieRecordMerger read through HudiTrinoReaderContext.
        throw new UnsupportedOperationException("TrinoRow records cannot be wrapped in a HoodieRecord; tables with a custom record merger read through HudiTrinoReaderContext");
    }

    @Override
    public HoodieRecord.HoodieRecordType getEngineRecordType()
    {
        // HoodieRecordType has no Trino value, and only the native log delete writer asks
        throw new UnsupportedOperationException("TrinoRow has no HoodieRecordType; it is read-only");
    }

    @Override
    public UnaryOperator<TrinoRow> projectRecord(HoodieSchema from, HoodieSchema to, Map<String, String> renamedColumns)
    {
        // Rows are projected from their own layout, as AvroRecordContext rewrites from the record's own schema
        TrinoRow.Layout target = layout(to);
        Schema targetAvroSchema = to.toAvroSchema();
        return new UnaryOperator<>()
        {
            // Base rows and rows from log blocks or payload results interleave, so the source fields of every layout
            // seen are kept; REWRITE marks a layout that must be rewritten
            private final Map<TrinoRow.Layout, int[]> sourceFieldsByLayout = new IdentityHashMap<>();
            private TrinoRow.Layout cachedSource;
            private int[] cachedSourceFields;
            private Block[] cachedSourceBlocks;
            private Block[] cachedProjectedBlocks;

            @Override
            public TrinoRow apply(TrinoRow row)
            {
                if (row.getLayout() != cachedSource) {
                    cachedSource = row.getLayout();
                    cachedSourceFields = sourceFieldsByLayout.computeIfAbsent(cachedSource, source -> {
                        int[] fields = sourceFields(source, target, renamedColumns);
                        return fields == null ? REWRITE : fields;
                    });
                    cachedSourceBlocks = null;
                }
                if (cachedSourceFields == REWRITE) {
                    // A field to change type or to fill with its default: rewrite through Avro as AvroRecordContext does
                    return convertAvroRecord(HoodieAvroUtils.rewriteRecordWithNewSchema(convertToAvroRecord(row, row.getSchema()), targetAvroSchema, renamedColumns));
                }
                // Rows of one page share their block array, so the projected array is built once per page
                if (row.blocks() != cachedSourceBlocks) {
                    cachedSourceBlocks = row.blocks();
                    cachedProjectedBlocks = new Block[cachedSourceFields.length];
                    for (int field = 0; field < cachedSourceFields.length; field++) {
                        cachedProjectedBlocks[field] = cachedSourceBlocks[cachedSourceFields[field]];
                    }
                }
                return row.withLayout(target, cachedProjectedBlocks);
            }
        };
    }

    /**
     * For each target field, the source field holding its value unchanged: same name, after the rename, and the same
     * Trino type. Null when any target field is missing from the source or has another type (a schema evolution type
     * promotion), so the row must be rewritten.
     */
    private static int[] sourceFields(TrinoRow.Layout source, TrinoRow.Layout target, Map<String, String> renamedColumns)
    {
        int[] sourceFields = new int[target.getFieldCount()];
        for (int field = 0; field < target.getFieldCount(); field++) {
            String targetName = target.getField(field).name();
            int sourceField = source.fieldIndex(renamedColumns.getOrDefault(targetName, targetName));
            if (sourceField < 0 || !source.getType(sourceField).equals(target.getType(field))) {
                return null;
            }
            sourceFields[field] = sourceField;
        }
        return sourceFields;
    }

    /**
     * Spills rows as Avro binary, mirroring {@code HoodieAvroReaderContext}'s {@code AvroRecordSerializer}: the row is
     * written with its own schema and read back with the schema its buffered record tracks.
     */
    public RecordSerializer<TrinoRow> recordSerializer()
    {
        return new AvroBinaryRecordSerializer(this);
    }

    private static final class AvroBinaryRecordSerializer
            implements RecordSerializer<TrinoRow>
    {
        private final TrinoRecordContext recordContext;

        private AvroBinaryRecordSerializer(TrinoRecordContext recordContext)
        {
            this.recordContext = requireNonNull(recordContext, "recordContext is null");
        }

        @Override
        public byte[] serialize(TrinoRow row)
        {
            return avroToBytes(recordContext.convertToAvroRecord(row, row.getSchema()));
        }

        @Override
        public TrinoRow deserialize(byte[] bytes, int schemaId)
        {
            HoodieSchema schema = recordContext.decodeAvroSchema(schemaId);
            try {
                return recordContext.convertAvroRecord(bytesToAvro(bytes, schema.toAvroSchema()));
            }
            catch (IOException e) {
                throw new HoodieException("Failed to deserialize a spilled row of schema " + schema.getFullName(), e);
            }
        }
    }

    private TrinoRow.Layout avroLayout(Schema avroSchema)
    {
        if (avroSchema != lastAvroSchema) {
            lastAvroLayout = layout(HoodieAvroSchemaCache.intern(avroSchema));
            lastAvroSchema = avroSchema;
        }
        return lastAvroLayout;
    }

    private LayoutAccessors accessors(TrinoRow.Layout layout)
    {
        if (lastAccessors != null && lastAccessors.layout == layout) {
            return lastAccessors;
        }
        lastAccessors = layoutAccessors.computeIfAbsent(layout, LayoutAccessors::new);
        return lastAccessors;
    }

    private static Block singleValueBlock(Type type, AvroToBlock writer, Object value)
    {
        BlockBuilder builder = type.createBlockBuilder(null, 1);
        if (value == null) {
            builder.appendNull();
        }
        else {
            writer.append(value, builder);
        }
        return builder.build();
    }

    /**
     * Reads a field of a row, null when the field is missing or null.
     */
    @FunctionalInterface
    private interface FieldReader
    {
        Object read(TrinoRow row);
    }

    /**
     * Reads a non-null value of a field from a block of its Trino type.
     */
    @FunctionalInterface
    private interface ValueReader
    {
        Object read(Block block, int position);
    }

    /**
     * What is compiled for one layout: field readers by field path, and the Avro converters of every field.
     */
    private final class LayoutAccessors
    {
        private final TrinoRow.Layout layout;
        private final Map<String, FieldReader> valueReaders = new HashMap<>();
        private final Map<String, FieldReader> orderingReaders = new HashMap<>();
        private Schema avroSchema;
        private ValueToAvro[] avroReaders;
        private AvroToBlock[] blockWriters;

        private LayoutAccessors(TrinoRow.Layout layout)
        {
            this.layout = layout;
        }

        FieldReader valueReader(String fieldPath)
        {
            return valueReaders.computeIfAbsent(fieldPath, path -> fieldReader(layout, path, false));
        }

        FieldReader orderingReader(String fieldPath)
        {
            return orderingReaders.computeIfAbsent(fieldPath, path -> fieldReader(layout, path, true));
        }

        Schema avroSchema()
        {
            if (avroSchema == null) {
                avroSchema = layout.getSchema().toAvroSchema();
            }
            return avroSchema;
        }

        ValueToAvro[] avroReaders()
        {
            if (avroReaders == null) {
                ValueToAvro[] readers = new ValueToAvro[layout.getFieldCount()];
                for (int field = 0; field < readers.length; field++) {
                    HoodieSchemaField schemaField = layout.getField(field);
                    readers[field] = HoodieSchemaAvroSerializer.toAvro(schemaField.name(), schemaField.schema(), layout.getType(field));
                }
                avroReaders = readers;
            }
            return avroReaders;
        }

        AvroToBlock[] blockWriters()
        {
            if (blockWriters == null) {
                AvroToBlock[] writers = new AvroToBlock[layout.getFieldCount()];
                for (int field = 0; field < writers.length; field++) {
                    HoodieSchemaField schemaField = layout.getField(field);
                    writers[field] = HoodieSchemaAvroSerializer.toBlock(schemaField.name(), schemaField.schema(), layout.getType(field));
                }
                blockWriters = writers;
            }
            return blockWriters;
        }
    }

    /**
     * Compiles the reader of a field path. A dotted path descends through RECORD fields, and resolves to null on a
     * missing field, a non-record step or a null step, as {@code AvroRecordContext.getFieldValueFromIndexedRecord}.
     */
    private FieldReader fieldReader(TrinoRow.Layout layout, String fieldPath, boolean orderingForm)
    {
        String[] path = fieldPath.split("\\.");
        int topField = layout.fieldIndex(path[0]);
        if (topField < 0) {
            return MISSING_FIELD;
        }
        HoodieSchema schema = layout.getField(topField).schema();
        Type type = layout.getType(topField);
        RowType[] stepTypes = new RowType[path.length - 1];
        int[] stepFields = new int[path.length - 1];
        for (int step = 0; step < stepTypes.length; step++) {
            HoodieSchema recordSchema = schema.isComplexUnion() ? schema : schema.getNonNullType();
            if (!recordSchema.hasFields() || !(type instanceof RowType rowType)) {
                return MISSING_FIELD;
            }
            String childName = path[step + 1];
            HoodieSchemaField child = recordSchema.getField(childName).orElse(null);
            int rowField = rowFieldIndex(rowType, childName);
            if (child == null || rowField < 0) {
                return MISSING_FIELD;
            }
            stepTypes[step] = rowType;
            stepFields[step] = rowField;
            schema = child.schema();
            type = rowType.getFields().get(rowField).getType();
        }
        ValueReader leaf = valueReader(fieldPath, schema, type, orderingForm);
        if (stepTypes.length == 0) {
            return row -> row.isNull(topField) ? null : leaf.read(row.getBlock(topField), row.getPosition());
        }
        return row -> {
            Block block = row.getBlock(topField);
            int position = row.getPosition();
            for (int step = 0; step < stepTypes.length; step++) {
                if (block.isNull(position)) {
                    return null;
                }
                SqlRow sqlRow = stepTypes[step].getObject(block, position);
                block = sqlRow.getRawFieldBlock(stepFields[step]);
                position = sqlRow.getRawIndex();
            }
            return block.isNull(position) ? null : leaf.read(block, position);
        };
    }

    /**
     * Compiles the reader of a field's value; see the class table. {@code orderingForm} applies the consistent logical
     * timestamp conversion to UTC-adjusted timestamps.
     */
    private ValueReader valueReader(String fieldName, HoodieSchema fieldSchema, Type type, boolean orderingForm)
    {
        HoodieSchema schema = nonNullSchema(fieldName, fieldSchema);
        return switch (schema.getType()) {
            // The Avro value already is the value convertValueForAvroLogicalTypes leaves as is
            case BOOLEAN, INT, LONG, FLOAT, DOUBLE, ENUM, BYTES, FIXED, TIME, ARRAY, MAP, RECORD, BLOB, VARIANT, VECTOR -> HoodieSchemaAvroSerializer.toAvro(fieldName, schema, type)::convert;
            // An Avro decoder reads strings as Utf8, which is what a delete-block string ordering value converts to
            case STRING -> (block, position) -> new Utf8(type.getSlice(block, position).getBytes());
            case UUID -> uuidValue(fieldName, schema, type);
            case DECIMAL -> decimalValue(fieldName, schema, type);
            case DATE -> (block, position) -> LocalDate.ofEpochDay(type.getLong(block, position));
            case TIMESTAMP -> timestampValue(fieldName, (HoodieSchema.Timestamp) schema, type, orderingForm);
            // Unreachable: nonNullSchema rejects complex unions and getNonNullType() unwraps ["null", T]
            case UNION, NULL -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Field %s has no value type: %s".formatted(fieldName, schema));
        };
    }

    private static ValueReader uuidValue(String fieldName, HoodieSchema schema, Type type)
    {
        // Avro's uuid annotates a string, decoded as Utf8, or a fixed(16), decoded as GenericFixed
        if (schema.toAvroSchema().getType() == Schema.Type.FIXED) {
            return HoodieSchemaAvroSerializer.toAvro(fieldName, schema, type)::convert;
        }
        return (block, position) -> new Utf8(trinoUuidToJavaUuid(type.getSlice(block, position)).toString());
    }

    private static ValueReader decimalValue(String fieldName, HoodieSchema schema, Type type)
    {
        if (!(type instanceof DecimalType decimalType)) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Field %s of Hudi type %s cannot be held by Trino type %s".formatted(fieldName, schema.getType(), type));
        }
        // Avro's DecimalConversion: the unscaled value at the schema's scale
        if (decimalType.isShort()) {
            return (block, position) -> BigDecimal.valueOf(decimalType.getLong(block, position), decimalType.getScale());
        }
        return (block, position) -> {
            BigInteger unscaled = ((Int128) decimalType.getObject(block, position)).toBigInteger();
            return new BigDecimal(unscaled, decimalType.getScale());
        };
    }

    private ValueReader timestampValue(String fieldName, HoodieSchema.Timestamp schema, Type type, boolean orderingForm)
    {
        // Epoch millis or micros, as Avro holds a timestamp
        ValueToAvro epochValue = HoodieSchemaAvroSerializer.toAvro(fieldName, schema, type);
        if (!schema.isUtcAdjusted() || !orderingForm || !consistentLogicalTimestampEnabled) {
            return epochValue::convert;
        }
        // HoodieAvroUtils.convertValueForAvroLogicalTypes with consistent logical timestamps: a java.sql.Timestamp of
        // the epoch millis, micros divided toward zero
        return switch (schema.getPrecision()) {
            case MILLIS -> (block, position) -> new Timestamp((Long) epochValue.convert(block, position));
            case MICROS -> (block, position) -> new Timestamp((Long) epochValue.convert(block, position) / 1000);
        };
    }
}
