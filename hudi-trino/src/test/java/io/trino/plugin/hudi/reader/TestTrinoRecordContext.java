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

import io.trino.metastore.HiveType;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.file.HudiBaseFile;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.plugin.hudi.util.HoodieSchemaAvroSerializer;
import io.trino.plugin.hudi.util.PrefilledColumnValues;
import io.trino.plugin.hudi.util.TrinoTypeMapper;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hudi.common.avro.AvroRecordContext;
import org.apache.hudi.common.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.serialization.CustomSerializer;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.TableState;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StoragePath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hudi.HudiUtil.toSchemaTypedColumnHandle;
import static io.trino.plugin.hudi.testing.SchemaColumnTypesHudiTablesInitializer.TABLE_SCHEMA;
import static io.trino.plugin.hudi.testing.SchemaColumnTypesHudiTablesInitializer.populatedRecord;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.hudi.common.avro.HoodieAvroUtils.avroToBytes;
import static org.apache.hudi.common.avro.HoodieAvroUtils.bytesToAvro;
import static org.apache.hudi.common.config.RecordMergeMode.EVENT_TIME_ORDERING;
import static org.apache.hudi.common.model.HoodieRecord.OPERATION_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.schema.HoodieSchemaType.ARRAY;
import static org.apache.hudi.common.schema.HoodieSchemaType.BLOB;
import static org.apache.hudi.common.schema.HoodieSchemaType.MAP;
import static org.apache.hudi.common.schema.HoodieSchemaType.RECORD;
import static org.apache.hudi.common.schema.HoodieSchemaType.VARIANT;
import static org.apache.hudi.common.schema.HoodieSchemaType.VECTOR;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestTrinoRecordContext
{
    private static final TrinoTypeMapper TYPE_MAPPER = new TrinoTypeMapper(new TypeOperators());
    // Types a record key or an ordering field can have: everything but the containers
    private static final Set<HoodieSchemaType> CONTAINER_TYPES = EnumSet.of(ARRAY, MAP, RECORD, BLOB, VARIANT, VECTOR);
    private static final StoragePath BASE_FILE = new StoragePath("/table/partition/file1_1-0-1_001.parquet");

    /**
     * Every ordering- or key-capable field of a record decoded from Avro binary, the shape a classic log block hands
     * over, reads to the value and class AvroRecordContext orders by; as an ordering value too, with consistent logical
     * timestamps on and off. Rows read from a page and rows converted from Avro must agree.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testValuesMatchAvroOrderingValues(boolean consistentTimestamps)
            throws IOException
    {
        TypedProperties props = consistentTimestampProps(consistentTimestamps);
        HoodieTableConfig tableConfig = tableConfig("ts");
        TrinoRecordContext recordContext = recordContext(tableConfig, props);
        AvroRecordContext avroRecordContext = new AvroRecordContext(tableConfig, null, ConfigUtils.getMergeProps(props, tableConfig));
        GenericRecord decoded = decode(populatedRecord("k3", 100, 3));

        TrinoRow converted = recordContext.convertAvroRecord(decoded);
        TrinoRow pageRow = readBaseRows(TABLE_SCHEMA, List.of(converted)).getFirst();
        for (HoodieSchemaField field : TABLE_SCHEMA.getFields()) {
            if (CONTAINER_TYPES.contains(field.getNonNullSchema().getType())) {
                continue;
            }
            Object expected = AvroRecordContext.getOrderingValueFromIndexedRecord(decoded, field.name(), false);
            Comparable expectedOrdering = avroRecordContext.getOrderingValue(decoded, TABLE_SCHEMA, List.of(field.name()));
            for (TrinoRow row : List.of(converted, pageRow)) {
                assertThat(recordContext.getValue(row, TABLE_SCHEMA, field.name()))
                        .as(field.name())
                        .isEqualTo(expected)
                        .hasSameClassAs(expected);
                assertThat(recordContext.getOrderingValue(row, TABLE_SCHEMA, List.of(field.name())))
                        .as(field.name())
                        .isEqualTo(expectedOrdering)
                        .hasSameClassAs(expectedOrdering);
                assertThat(recordContext.getOrderingValue(row, TABLE_SCHEMA, new String[] {field.name()}))
                        .as(field.name())
                        .isEqualTo(expectedOrdering);
            }
        }
        if (consistentTimestamps) {
            assertThat(recordContext.getOrderingValue(converted, TABLE_SCHEMA, List.of("c_timestamp_micros")))
                    .isEqualTo(new Timestamp(1_700_000_000_123L + 3 * 86_400_000L));
            // getValue stays lossless: partial-update merging writes it back
            assertThat(recordContext.getValue(converted, TABLE_SCHEMA, "c_timestamp_micros"))
                    .isEqualTo(1_700_000_000_123_456L + 3 * 86_400_000_000L);
        }
        assertThat(recordContext.getOrderingValue(converted, TABLE_SCHEMA, List.of())).isEqualTo(OrderingValues.getDefault());
    }

    @Test
    void testNestedAndMissingFieldPaths()
            throws IOException
    {
        TrinoRecordContext recordContext = recordContext(tableConfig("ts"), new TypedProperties());
        GenericRecord decoded = decode(populatedRecord("k3", 100, 3));
        TrinoRow row = recordContext.convertAvroRecord(decoded);
        for (String path : List.of("c_record.name", "c_record.id", "c_record.missing", "c_int.missing", "missing", "c_record")) {
            Object expected = AvroRecordContext.getOrderingValueFromIndexedRecord(decoded, path, false);
            if (path.equals("c_record")) {
                // A record value: the same fields, held as convertToAvroRecord writes them
                assertThat(recordContext.getValue(row, TABLE_SCHEMA, path)).isInstanceOf(GenericData.Record.class);
                continue;
            }
            assertThat(recordContext.getValue(row, TABLE_SCHEMA, path)).as(path).isEqualTo(expected);
        }
        assertThat(recordContext.getValue(row, TABLE_SCHEMA, "c_record.name")).isEqualTo(new Utf8("record-3"));

        // A null record on the path reads as null
        GenericRecord withNullRecord = decode(populatedRecord("k3", 100, 3));
        withNullRecord.put("c_record", null);
        assertThat(recordContext.getValue(recordContext.convertAvroRecord(withNullRecord), TABLE_SCHEMA, "c_record.name")).isNull();
    }

    @Test
    void testFixedUuidValue()
            throws IOException
    {
        HoodieSchema uuidSchema = HoodieSchema.fromAvroSchema(LogicalTypes.uuid().addToSchema(Schema.createFixed("uuid_fixed", null, null, 16)));
        HoodieSchema schema = HoodieSchema.createRecord("rec", null, null, List.of(
                HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG)),
                HoodieSchemaField.of("id", uuidSchema)));
        UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        GenericRecord record = new GenericData.Record(schema.toAvroSchema());
        record.put("ts", 1L);
        record.put("id", new GenericData.Fixed(uuidSchema.toAvroSchema(), ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array()));
        GenericRecord decoded = decode(record);

        TrinoRecordContext recordContext = recordContext(tableConfig("ts"), new TypedProperties());
        Object expected = AvroRecordContext.getOrderingValueFromIndexedRecord(decoded, "id", false);
        assertThat(recordContext.getValue(recordContext.convertAvroRecord(decoded), schema, "id"))
                .isEqualTo(expected)
                .hasSameClassAs(expected);
    }

    static Stream<DeleteCase> deleteCases()
    {
        // Each record ordering value is the seed-3 value of SchemaColumnTypesHudiTablesInitializer; each delete holds
        // a Java value a writer orders by that the delete block can serialize (epoch days for a date, as Spark and
        // Flink supply them; a decimal within the block's precision 30)
        return Stream.of(
                new DeleteCase("c_int", 299, 301),
                new DeleteCase("c_string", "string-2", "string-4"),
                new DeleteCase("c_date", 19_002, 19_004),
                new DeleteCase("c_decimal_fixed", new BigDecimal("-3.26"), new BigDecimal("-3.24")),
                new DeleteCase("c_timestamp_micros", 1_700_000_000_123_455L + 3 * 86_400_000_000L, 1_700_000_000_123_457L + 3 * 86_400_000_000L));
    }

    record DeleteCase(String orderingField, Comparable<?> lowerDeleteValue, Comparable<?> higherDeleteValue)
    {
        @Override
        public String toString()
        {
            return orderingField;
        }
    }

    /**
     * Under event-time ordering a serialized delete with a lower ordering value loses against the row and one with a
     * higher value wins: the converted delete ordering value has the row ordering value's class.
     */
    @ParameterizedTest
    @MethodSource("deleteCases")
    void testDeleteOrderingComparesWithRowOrdering(DeleteCase deleteCase)
            throws IOException
    {
        TypedProperties props = new TypedProperties();
        HoodieTableConfig tableConfig = tableConfig(deleteCase.orderingField());
        HudiTrinoRowReaderContext readerContext = readerContext(tableConfig, props, new EmptyPageSource(), List.of());
        TrinoRow row = readerContext.getRecordContext().convertAvroRecord(decode(populatedRecord("key", 100, 3)));
        BufferedRecord<TrinoRow> data = BufferedRecords.fromEngineRecord(
                row, "key", TABLE_SCHEMA, readerContext.getRecordContext(), List.of(deleteCase.orderingField()), null);
        BufferedRecordMerger<TrinoRow> merger = BufferedRecordMergerFactory.create(
                readerContext, EVENT_TIME_ORDERING, false, Option.empty(), TABLE_SCHEMA, Option.empty(), props, Option.empty());

        BufferedRecord<TrinoRow> lowerDelete = readDelete(readerContext, deleteCase.lowerDeleteValue());
        assertThat(OrderingValues.isSameClass(data.getOrderingValue(), lowerDelete.getOrderingValue())).isTrue();
        assertThat(merger.finalMerge(data, lowerDelete)).isSameAs(data);
        assertThat(merger.finalMerge(data, readDelete(readerContext, deleteCase.higherDeleteValue())).isDelete()).isTrue();
    }

    @Test
    void testMetaFieldValueAndRecordKey()
    {
        HoodieSchema schema = HoodieSchema.createRecord("rec", null, null, List.of(
                HoodieSchemaField.of(RECORD_KEY_METADATA_FIELD, HoodieSchema.createNullable(HoodieSchemaType.STRING), null, HoodieSchema.NULL_VALUE),
                HoodieSchemaField.of(OPERATION_METADATA_FIELD, HoodieSchema.createNullable(HoodieSchemaType.STRING), null, HoodieSchema.NULL_VALUE),
                HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG))));
        TrinoRecordContext recordContext = recordContext(tableConfig("ts"), new TypedProperties());
        GenericRecord record = new GenericData.Record(schema.toAvroSchema());
        record.put(RECORD_KEY_METADATA_FIELD, "k\u00e9y");
        record.put(OPERATION_METADATA_FIELD, null);
        record.put("ts", 1L);
        TrinoRow row = readBaseRows(schema, List.of(recordContext.convertAvroRecord(record))).getFirst();

        assertThat(recordContext.getMetaFieldValue(row, 0)).isEqualTo("k\u00e9y");
        assertThat(recordContext.getMetaFieldValue(row, 1)).isNull();
        assertThat(recordContext.getRecordKey(row, schema)).isEqualTo("k\u00e9y");
    }

    @Test
    void testSealAndToBinaryRow()
            throws IOException
    {
        TrinoRecordContext recordContext = recordContext(tableConfig("ts"), new TypedProperties());
        List<TrinoRow> rows = readBaseRows(TABLE_SCHEMA, List.of(
                recordContext.convertAvroRecord(decode(populatedRecord("k1", 100, 1))),
                recordContext.convertAvroRecord(decode(populatedRecord("k2", 100, 2))),
                recordContext.convertAvroRecord(decode(populatedRecord("k3", 100, 3)))));
        TrinoRow pageRow = rows.get(1);
        assertThat(pageRow.getPosition()).isEqualTo(1);
        assertThat(recordContext.seal(TABLE_SCHEMA, pageRow)).isSameAs(pageRow);
        // Rows of one page share one block array: a base row is the view object alone
        assertThat(rows.get(0).blocks()).isSameAs(pageRow.blocks());

        TrinoRow binaryRow = recordContext.toBinaryRow(TABLE_SCHEMA, pageRow);
        assertThat(binaryRow.isCompact()).isTrue();
        assertThat(binaryRow.getPosition()).isZero();
        for (int field = 0; field < TABLE_SCHEMA.getFields().size(); field++) {
            assertThat(binaryRow.getBlock(field).getPositionCount()).isEqualTo(1);
            assertThat(binaryRow.getBlock(field)).isNotSameAs(pageRow.getBlock(field));
        }
        assertThat(recordContext.convertToAvroRecord(binaryRow, TABLE_SCHEMA)).isEqualTo(recordContext.convertToAvroRecord(pageRow, TABLE_SCHEMA));
        assertThat(binaryRow.getRetainedSizeInBytes()).isLessThan(pageRow.getRetainedSizeInBytes());
        assertThat(recordContext.toBinaryRow(TABLE_SCHEMA, binaryRow)).isSameAs(binaryRow);
    }

    @Test
    void testToBinaryRowCopiesASingleRowThatViewsALargerBlock()
    {
        // A page of one row can still retain other rows' data: a one-position dictionary block keeps its dictionary
        HoodieSchema schema = HoodieSchema.createRecord("rec", null, null, List.of(HoodieSchemaField.of("s", HoodieSchema.create(HoodieSchemaType.STRING))));
        TrinoRecordContext recordContext = recordContext(tableConfig("s"), new TypedProperties());
        BlockBuilder dictionary = VARCHAR.createBlockBuilder(null, 3);
        for (String value : List.of("a", "b", "c")) {
            VARCHAR.writeSlice(dictionary, utf8Slice(value.repeat(10_000)));
        }
        TrinoRow pageRow = new TrinoRow(recordContext.layout(schema), new Block[] {DictionaryBlock.create(1, dictionary.build(), new int[] {2})}, 0);
        assertThat(pageRow.isCompact()).isFalse();

        TrinoRow binaryRow = recordContext.toBinaryRow(schema, pageRow);
        assertThat(binaryRow.isCompact()).isTrue();
        assertThat(VARCHAR.getSlice(binaryRow.getBlock(0), 0).toStringUtf8()).isEqualTo("c".repeat(10_000));
        assertThat(binaryRow.getRetainedSizeInBytes()).isLessThan(pageRow.getRetainedSizeInBytes() - 20_000);
    }

    @Test
    void testAppenderConvertsAFieldWhoseTypeIsNotTheColumnType()
    {
        // A payload may return a record in another schema than the table's: an int where the column is a bigint, a
        // decimal of another scale. The value is converted as buildRecordInPage converts it, never copied as is.
        HoodieSchema rowSchema = HoodieSchema.createRecord("rec", null, null, List.of(
                HoodieSchemaField.of("v", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.INT)), null, HoodieSchema.NULL_VALUE),
                HoodieSchemaField.of("d", HoodieSchema.createDecimal(10, 2))));
        TrinoRecordContext recordContext = recordContext(tableConfig("v"), new TypedProperties());
        GenericRecord record = new GenericData.Record(rowSchema.toAvroSchema());
        record.put("d", ByteBuffer.wrap(BigInteger.valueOf(125).toByteArray()));
        record.put("v", 42);
        GenericRecord nullValue = new GenericData.Record(rowSchema.toAvroSchema());
        nullValue.put("d", ByteBuffer.wrap(BigInteger.valueOf(-5).toByteArray()));

        DecimalType decimal = createDecimalType(10, 3);
        List<HiveColumnHandle> columns = List.of(
                createBaseColumn("d", 0, HiveType.valueOf("decimal(10,3)"), decimal, REGULAR, Optional.empty()),
                createBaseColumn("v", 1, HiveType.HIVE_LONG, BIGINT, REGULAR, Optional.empty()));
        PageBuilder pageBuilder = new PageBuilder(List.of(decimal, BIGINT));
        TrinoRowAppender appender = new TrinoRowAppender(columns, prefilledValues());
        appender.append(pageBuilder, recordContext.convertAvroRecord(record));
        appender.append(pageBuilder, recordContext.convertAvroRecord(nullValue));
        Page page = pageBuilder.build();

        assertThat(decimal.getObjectValue(page.getBlock(0), 0).toString()).isEqualTo("1.250");
        assertThat(BIGINT.getLong(page.getBlock(1), 0)).isEqualTo(42L);
        assertThat(decimal.getObjectValue(page.getBlock(0), 1).toString()).isEqualTo("-0.050");
        assertThat(page.getBlock(1).isNull(1)).isTrue();
    }

    /**
     * With the record key in data fields (no populated meta fields), the key is built from getValue. It must read as
     * the writer's key generator built it ({@code KeyGenUtils.getRecordKey}), or delete-block keys and log keys would
     * not match the base rows.
     */
    @ParameterizedTest
    @ValueSource(strings = {"c_string", "c_uuid", "c_date", "c_decimal_fixed", "c_decimal_bytes", "c_long", "c_timestamp_micros"})
    void testVirtualRecordKeyMatchesTheWriterKey(String keyField)
            throws IOException
    {
        HoodieTableConfig tableConfig = tableConfig("ts");
        tableConfig.setValue(HoodieTableConfig.POPULATE_META_FIELDS, "false");
        tableConfig.setValue(HoodieTableConfig.RECORDKEY_FIELDS, keyField);
        TrinoRecordContext recordContext = recordContext(tableConfig, new TypedProperties());
        GenericRecord decoded = decode(populatedRecord("k1", 100, 1));
        String writerKey = HoodieAvroUtils.getNestedFieldValAsString(decoded, keyField, true, false);

        TrinoRow converted = recordContext.convertAvroRecord(decoded);
        TrinoRow pageRow = readBaseRows(TABLE_SCHEMA, List.of(converted)).getFirst();
        assertThat(recordContext.getRecordKey(converted, TABLE_SCHEMA)).isEqualTo(writerKey);
        assertThat(recordContext.getRecordKey(pageRow, TABLE_SCHEMA)).isEqualTo(writerKey);
    }

    @Test
    void testProjectRecordWithoutTypeChangeViewsTheSameBlocks()
            throws IOException
    {
        TrinoRecordContext recordContext = recordContext(tableConfig("ts"), new TypedProperties());
        List<TrinoRow> rows = readBaseRows(TABLE_SCHEMA, List.of(
                recordContext.convertAvroRecord(decode(populatedRecord("k1", 100, 1))),
                recordContext.convertAvroRecord(decode(populatedRecord("k2", 100, 2)))));
        HoodieSchema projected = HoodieSchema.createRecord("projected", null, null, List.of(
                field("c_string"),
                field("key"),
                field("c_decimal_bytes")));
        UnaryOperator<TrinoRow> projection = recordContext.projectRecord(TABLE_SCHEMA, projected);

        TrinoRow first = projection.apply(rows.get(0));
        TrinoRow second = projection.apply(rows.get(1));
        assertThat(first.getSchema()).isSameAs(projected);
        assertThat(first.getBlock(0)).isSameAs(rows.get(0).getBlock(TABLE_SCHEMA.getField("c_string").get().pos()));
        assertThat(first.getBlock(1)).isSameAs(rows.get(0).getBlock(0));
        assertThat(second.blocks()).isSameAs(first.blocks());
        assertThat(second.getPosition()).isEqualTo(1);
        assertThat(recordContext.getValue(second, projected, "c_string")).isEqualTo(new Utf8("string-2"));
        assertThat(recordContext.getValue(second, projected, "key")).isEqualTo(new Utf8("k2"));

        // A renamed column: the projected name reads the file's old name
        HoodieSchema renamed = HoodieSchema.createRecord("renamed", null, null, List.of(
                HoodieSchemaField.of("label", TABLE_SCHEMA.getField("c_string").get().schema(), null, HoodieSchema.NULL_VALUE)));
        TrinoRow renamedRow = recordContext.projectRecord(TABLE_SCHEMA, renamed, Map.of("label", "c_string")).apply(rows.get(0));
        assertThat(renamedRow.getBlock(0)).isSameAs(first.getBlock(0));
        assertThat(recordContext.getValue(renamedRow, renamed, "label")).isEqualTo(new Utf8("string-1"));
    }

    @Test
    void testProjectRecordWithTypePromotionRewritesTheRow()
    {
        HoodieSchema source = HoodieSchema.createRecord("rec", null, null, List.of(
                HoodieSchemaField.of("key", HoodieSchema.create(HoodieSchemaType.STRING)),
                HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.INT))));
        HoodieSchema target = HoodieSchema.createRecord("rec", null, null, List.of(
                HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.LONG)),
                HoodieSchemaField.of("key", HoodieSchema.create(HoodieSchemaType.STRING)),
                HoodieSchemaField.of("added", HoodieSchema.createNullable(HoodieSchemaType.STRING), null, HoodieSchema.NULL_VALUE)));
        TrinoRecordContext recordContext = recordContext(tableConfig("value"), new TypedProperties());
        GenericRecord record = new GenericData.Record(source.toAvroSchema());
        record.put("key", "k1");
        record.put("value", 42);

        TrinoRow projected = recordContext.projectRecord(source, target).apply(recordContext.convertAvroRecord(record));
        assertThat(projected.getLayout().getType(0)).isEqualTo(BIGINT);
        assertThat(recordContext.getValue(projected, target, "value")).isEqualTo(42L);
        assertThat(recordContext.getValue(projected, target, "key")).isEqualTo(new Utf8("k1"));
        assertThat(recordContext.getValue(projected, target, "added")).isNull();
    }

    /**
     * Every type through the merge-path conversions: an Avro log record to a row, the row back to Avro through the
     * binary round trip, and the row to an output page equal to the one the Avro path builds.
     */
    @Test
    void testAvroConversionRoundTripAndPageOutput()
            throws IOException
    {
        TrinoRecordContext recordContext = recordContext(tableConfig("ts"), new TypedProperties());
        GenericRecord populated = decode(populatedRecord("k1", 100, 1));
        GenericRecord nulls = new GenericData.Record(TABLE_SCHEMA.toAvroSchema());
        nulls.put("key", "k2");
        nulls.put("ts", 200L);
        List<GenericRecord> records = List.of(populated, decode(nulls), decode(populatedRecord("k3", 300, 8)));

        List<HiveColumnHandle> columns = columnHandles(TABLE_SCHEMA);
        List<Type> types = columns.stream().map(HiveColumnHandle::getType).toList();
        PageBuilder fromRows = new PageBuilder(types);
        PageBuilder fromAvro = new PageBuilder(types);
        TrinoRowAppender appender = new TrinoRowAppender(columns, prefilledValues());
        HoodieSchemaAvroSerializer serializer = new HoodieSchemaAvroSerializer(columns, prefilledValues());
        for (GenericRecord record : records) {
            TrinoRow row = recordContext.convertAvroRecord(record);
            assertThat(row.isCompact()).isTrue();
            GenericRecord back = recordContext.convertToAvroRecord(row, TABLE_SCHEMA);
            assertThat(back.getSchema()).isEqualTo(record.getSchema());
            assertThat(decode(back)).isEqualTo(record);
            appender.append(fromRows, row);
            serializer.buildRecordInPage(fromAvro, record);
        }
        assertPageEquals(types, fromRows.build(), fromAvro.build());
    }

    /**
     * Each field of an Avro log record, the primitive ones wrapped directly and the rest written through a block builder,
     * holds the value the field's block writer writes, and a null field is a null block.
     */
    @Test
    void testAvroRecordFieldsHoldTheBlockWritersValues()
            throws IOException
    {
        TrinoRecordContext recordContext = recordContext(tableConfig("ts"), new TypedProperties());
        GenericRecord nulls = new GenericData.Record(TABLE_SCHEMA.toAvroSchema());
        nulls.put("key", "k2");
        nulls.put("ts", 200L);
        for (GenericRecord record : List.of(decode(populatedRecord("k1", 100, 1)), decode(nulls), decode(populatedRecord("k3", 300, 8)))) {
            assertFieldsHoldTheBlockWritersValues(recordContext, record);
        }

        // Values a decoder does not produce: a Utf8 whose buffer is longer than its bytes, multi-byte and empty strings,
        // a String, extreme numbers, and Java classes that keep the block writer
        Utf8 reused = new Utf8("abcdefghijklmnopqrs");
        reused.setByteLength(3);
        List<Map<String, Object>> edgeValues = List.of(
                Map.of("c_string", reused, "c_int", Integer.MIN_VALUE, "c_long", Long.MAX_VALUE, "c_double", Double.NaN, "c_date", 0),
                Map.of("c_string", new Utf8("na\u00efve \u65e5\u672c \uD83D\uDE00"), "c_int", Integer.MAX_VALUE, "c_long", Long.MIN_VALUE, "c_double", -0.0d, "c_date", -1),
                Map.of("c_string", new Utf8(""), "c_double", Double.NEGATIVE_INFINITY),
                Map.of("c_string", "a String", "c_int", 7L, "c_long", 8, "c_double", 1.5f, "c_date", LocalDate.of(2022, 1, 15)));
        for (Map<String, Object> values : edgeValues) {
            GenericRecord record = new GenericData.Record(TABLE_SCHEMA.toAvroSchema());
            record.put("key", new Utf8("k9"));
            record.put("ts", 900L);
            values.forEach(record::put);
            assertFieldsHoldTheBlockWritersValues(recordContext, record);
        }
        assertThat(VARCHAR.getSlice(recordContext.convertAvroRecord(recordWithString(reused)).getBlock(TABLE_SCHEMA.getField("c_string").get().pos()), 0))
                .isEqualTo(utf8Slice("abc"));
    }

    private static GenericRecord recordWithString(Utf8 value)
    {
        GenericRecord record = new GenericData.Record(TABLE_SCHEMA.toAvroSchema());
        record.put("key", new Utf8("k9"));
        record.put("ts", 900L);
        record.put("c_string", value);
        return record;
    }

    private static void assertFieldsHoldTheBlockWritersValues(TrinoRecordContext recordContext, GenericRecord record)
    {
        TrinoRow row = recordContext.convertAvroRecord(record);
        for (HoodieSchemaField field : TABLE_SCHEMA.getFields()) {
            Type type = row.getLayout().getType(field.pos());
            Block actual = row.getBlock(field.pos());
            assertThat(actual.getPositionCount()).as(field.name()).isEqualTo(1);
            Object value = record.get(field.pos());
            if (value == null) {
                assertThat(actual.isNull(0)).as(field.name()).isTrue();
                continue;
            }
            BlockBuilder expected = type.createBlockBuilder(null, 1);
            HoodieSchemaAvroSerializer.toBlock(field.name(), field.schema(), type).append(value, expected);
            Block expectedBlock = expected.build();
            assertThat(actual.getClass()).as(field.name()).isEqualTo(expectedBlock.getClass());
            assertThat(type.getObjectValue(actual, 0)).as(field.name()).isEqualTo(type.getObjectValue(expectedBlock, 0));
        }
    }

    @Test
    void testConstructEngineRecordTakesGetValueResults()
            throws IOException
    {
        TrinoRecordContext recordContext = recordContext(tableConfig("ts"), consistentTimestampProps(true));
        GenericRecord decoded = decode(populatedRecord("k1", 100, 5));
        TrinoRow row = recordContext.convertAvroRecord(decoded);
        Object[] values = TABLE_SCHEMA.getFields().stream()
                .map(field -> recordContext.getValue(row, TABLE_SCHEMA, field.name()))
                .toArray();

        TrinoRow constructed = recordContext.constructEngineRecord(TABLE_SCHEMA, values);
        assertThat(decode(recordContext.convertToAvroRecord(constructed, TABLE_SCHEMA))).isEqualTo(decoded);
    }

    @Test
    void testMergeWithEngineRecordReplacesUpdatedFields()
            throws IOException
    {
        TrinoRecordContext recordContext = recordContext(tableConfig("ts"), new TypedProperties());
        GenericRecord decoded = decode(populatedRecord("k1", 100, 5));
        BufferedRecord<TrinoRow> base = new BufferedRecord<>("k1", 100L, recordContext.convertAvroRecord(decoded), null, null);
        int intField = TABLE_SCHEMA.getField("c_int").get().pos();
        int stringField = TABLE_SCHEMA.getField("c_string").get().pos();

        TrinoRow merged = recordContext.mergeWithEngineRecord(TABLE_SCHEMA, Map.of(intField, 7, stringField, new Utf8("updated")), base);
        GenericRecord expected = decode(populatedRecord("k1", 100, 5));
        expected.put("c_int", 7);
        expected.put("c_string", new Utf8("updated"));
        assertThat(decode(recordContext.convertToAvroRecord(merged, TABLE_SCHEMA))).isEqualTo(expected);
    }

    @Test
    void testSpilledRecordsRoundTrip()
            throws IOException
    {
        HudiTrinoRowReaderContext readerContext = readerContext(tableConfig("ts"), new TypedProperties(), new EmptyPageSource(), List.of());
        TrinoRecordContext recordContext = (TrinoRecordContext) readerContext.getRecordContext();
        GenericRecord decoded = decode(populatedRecord("k1", 100, 1));
        TrinoRow row = recordContext.convertAvroRecord(decoded);
        Integer schemaId = recordContext.encodeSchema(TABLE_SCHEMA);
        CustomSerializer<BufferedRecord<TrinoRow>> serializer = readerContext.getRecordSerializer();

        BufferedRecord<TrinoRow> spilled = serializer.deserialize(serializer.serialize(
                new BufferedRecord<>("k1", recordContext.getOrderingValue(row, TABLE_SCHEMA, List.of("c_string")), row, schemaId, HoodieOperation.UPDATE_AFTER)));
        assertThat(spilled.getRecordKey()).isEqualTo("k1");
        assertThat(spilled.getOrderingValue()).isEqualTo(new Utf8("string-1"));
        assertThat(spilled.getSchemaId()).isEqualTo(schemaId);
        assertThat(spilled.getHoodieOperation()).isEqualTo(HoodieOperation.UPDATE_AFTER);
        assertThat(decode(recordContext.convertToAvroRecord(spilled.getRecord(), TABLE_SCHEMA))).isEqualTo(decoded);

        BufferedRecord<TrinoRow> delete = serializer.deserialize(serializer.serialize(BufferedRecords.createDelete("k2", 5L)));
        assertThat(delete.getRecord()).isNull();
        assertThat(delete.isDelete()).isTrue();

        // A buffered row is compact; the estimate counts its blocks, not the page it was read from
        assertThat(readerContext.getRecordSizeEstimator().sizeEstimate(new BufferedRecord<>("k1", 100L, row, schemaId, null)))
                .isGreaterThan(row.getRetainedSizeInBytes())
                .isLessThan(row.getRetainedSizeInBytes() + 1024);
    }

    @Test
    void testUnsupportedOperations()
    {
        TrinoRecordContext recordContext = recordContext(tableConfig("ts"), new TypedProperties());
        assertThatThrownBy(() -> recordContext.getDeleteRow("k1"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> recordContext.constructHoodieRecord(BufferedRecords.createDelete("k1", 1L), ""))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("HudiTrinoReaderContext");
        assertThatThrownBy(recordContext::getEngineRecordType)
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static HoodieTableConfig tableConfig(String orderingField)
    {
        HoodieTableConfig tableConfig = new HoodieTableConfig();
        tableConfig.setValue(HoodieTableConfig.VERSION, "9");
        tableConfig.setValue(HoodieTableConfig.RECORD_MERGE_MODE, EVENT_TIME_ORDERING.name());
        tableConfig.setValue(HoodieTableConfig.ORDERING_FIELDS, orderingField);
        return tableConfig;
    }

    private static TypedProperties consistentTimestampProps(boolean consistentTimestamps)
    {
        TypedProperties props = new TypedProperties();
        props.setProperty(KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), Boolean.toString(consistentTimestamps));
        return props;
    }

    private static TrinoRecordContext recordContext(HoodieTableConfig tableConfig, TypedProperties props)
    {
        return new TrinoRecordContext(tableConfig, ConfigUtils.getMergeProps(props, tableConfig), TYPE_MAPPER);
    }

    private static HudiTrinoRowReaderContext readerContext(HoodieTableConfig tableConfig, TypedProperties props, ConnectorPageSource basePageSource, List<HiveColumnHandle> baseProjection)
    {
        HudiTrinoRowReaderContext readerContext = new HudiTrinoRowReaderContext(
                new TrinoStorageConfiguration(), tableConfig, basePageSource, baseProjection,
                (path, start, length, projection) -> {
                    throw new AssertionError("Unexpected log file read");
                },
                props, TYPE_MAPPER);
        TypedProperties mergeProps = ConfigUtils.getMergeProps(props, tableConfig);
        readerContext.initRecordMerger(mergeProps);
        readerContext.setHasLogFiles(false);
        readerContext.setHasBootstrapBaseFile(false);
        readerContext.setSchemaHandler(new FileGroupReaderSchemaHandler<>(
                readerContext, TABLE_SCHEMA, TABLE_SCHEMA, Option.empty(), mergeProps,
                TableState.of(new StoragePath("/table"), tableConfig, Option.empty(), Option.empty())));
        return readerContext;
    }

    /**
     * Writes the rows to one page of {@code schema}'s columns, in reverse column order so channels and fields differ,
     * and reads the page back as base-file rows.
     */
    private static List<TrinoRow> readBaseRows(HoodieSchema schema, List<TrinoRow> rows)
    {
        List<HiveColumnHandle> columns = new ArrayList<>(columnHandles(schema)).reversed();
        PageBuilder pageBuilder = new PageBuilder(columns.stream().map(HiveColumnHandle::getType).toList());
        TrinoRowAppender appender = new TrinoRowAppender(columns, prefilledValues());
        for (TrinoRow row : rows) {
            appender.append(pageBuilder, row);
        }
        HudiTrinoRowReaderContext readerContext = new HudiTrinoRowReaderContext(
                new TrinoStorageConfiguration(), tableConfig("ts"), new FixedPageSource(List.of(pageBuilder.build()), MemoryContext.NO_LIMIT), columns,
                (path, start, length, projection) -> {
                    throw new AssertionError("Unexpected log file read");
                },
                new TypedProperties(), TYPE_MAPPER);
        List<TrinoRow> read = new ArrayList<>();
        try (ClosableIterator<TrinoRow> iterator = readerContext.getFileRecordIterator(BASE_FILE, 0, 1, schema, schema, null)) {
            iterator.forEachRemaining(read::add);
        }
        assertThat(read).allSatisfy(row -> assertThat(row.getSchema()).isSameAs(schema));
        return read;
    }

    private static List<HiveColumnHandle> columnHandles(HoodieSchema schema)
    {
        return schema.getFields().stream()
                .map(field -> toSchemaTypedColumnHandle(field, TYPE_MAPPER))
                .toList();
    }

    private static HoodieSchemaField field(String name)
    {
        HoodieSchema schema = TABLE_SCHEMA.getField(name).get().schema();
        return schema.isNullable() ? HoodieSchemaField.of(name, schema, null, HoodieSchema.NULL_VALUE) : HoodieSchemaField.of(name, schema);
    }

    private static GenericRecord decode(IndexedRecord record)
            throws IOException
    {
        // What a classic Avro log block hands over: values as the Avro decoder reads them (Utf8, ByteBuffer, raw logical types)
        return bytesToAvro(avroToBytes(record), record.getSchema());
    }

    private static BufferedRecord<TrinoRow> readDelete(HudiTrinoRowReaderContext readerContext, Comparable<?> orderingValue)
            throws IOException
    {
        HoodieDeleteBlock written = new HoodieDeleteBlock(List.of(Pair.of(DeleteRecord.create("key", "", orderingValue), -1L)), Map.of());
        HoodieDeleteBlock read = new HoodieDeleteBlock(Option.of(written.getContentBytes(null).toByteArray()),
                null, false, Option.empty(), Map.of(), Map.of());
        return read.getRecordsToDelete(readerContext).getFirst();
    }

    private static void assertPageEquals(List<Type> types, Page actual, Page expected)
    {
        assertThat(actual.getPositionCount()).isEqualTo(expected.getPositionCount());
        for (int channel = 0; channel < types.size(); channel++) {
            Type type = types.get(channel);
            Block actualBlock = actual.getBlock(channel);
            Block expectedBlock = expected.getBlock(channel);
            for (int position = 0; position < expected.getPositionCount(); position++) {
                assertThat(type.getObjectValue(actualBlock, position))
                        .as("channel %s, position %s", channel, position)
                        .isEqualTo(type.getObjectValue(expectedBlock, position));
            }
        }
    }

    private static PrefilledColumnValues prefilledValues()
    {
        HudiBaseFile baseFile = new HudiBaseFile("s3://bucket/table/file1.parquet", "file1.parquet", 1234, 1700000000123L, 0, 1234);
        HudiSplit split = new HudiSplit(baseFile, List.of(), "001", TupleDomain.all(), List.<HivePartitionKey>of(), SplitWeight.standard());
        return PrefilledColumnValues.create(split);
    }
}
