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

import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.spi.connector.EmptyPageSource;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.schema.HoodieSchema;
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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StoragePath;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.config.RecordMergeMode.EVENT_TIME_ORDERING;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

class TestHudiTrinoReaderContext
{
    @ParameterizedTest
    @CsvSource({"9, false", "11, true"})
    void testDateOrderingWithSerializedDelete(int deleteEpochDay, boolean expectDeleted)
            throws IOException
    {
        HoodieSchema schema = schemaWithOrdering(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));
        TypedProperties props = new TypedProperties();
        HudiTrinoReaderContext readerContext = createReaderContext(schema, tableConfig(), props);
        GenericRecord record = new GenericData.Record(schema.toAvroSchema());
        record.put("id", "key");
        record.put("ordering", 10);
        BufferedRecord<IndexedRecord> data = BufferedRecords.fromEngineRecord(
                record, "key", schema, readerContext.getRecordContext(), List.of("ordering"), null);
        BufferedRecord<IndexedRecord> delete = readDelete(readerContext, deleteEpochDay);
        BufferedRecordMerger<IndexedRecord> merger = BufferedRecordMergerFactory.create(
                readerContext, EVENT_TIME_ORDERING, false, Option.empty(), schema, Option.empty(), props, Option.empty());

        assertThat(merger.finalMerge(data, delete)).isSameAs(expectDeleted ? delete : data);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTimestampOrderingKeepsStoredForm(boolean consistentTimestamp)
            throws IOException
    {
        HoodieSchema schema = schemaWithOrdering(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)));
        TypedProperties props = new TypedProperties();
        props.setProperty(KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), Boolean.toString(consistentTimestamp));
        HudiTrinoReaderContext readerContext = createReaderContext(schema, tableConfig(), props);
        GenericRecord record = new GenericData.Record(schema.toAvroSchema());
        record.put("id", "key");
        record.put("ordering", 1_234_567L);

        assertThat(readerContext.getRecordContext().getOrderingValue(record, schema, List.of("ordering"))).isEqualTo(1_234_567L);
        assertThat(readDelete(readerContext, 1_234_567L).getOrderingValue()).isEqualTo(1_234_567L);
        assertThat(readDelete(readerContext, Timestamp.from(Instant.ofEpochSecond(1, 234_567_000))).getOrderingValue()).isEqualTo(1_234_567L);
    }

    private static HoodieSchema schemaWithOrdering(Schema orderingSchema)
    {
        return HoodieSchema.fromAvroSchema(SchemaBuilder.record("test_record").fields()
                .requiredString("id")
                .name("ordering").type(orderingSchema).noDefault()
                .endRecord());
    }

    private static HoodieTableConfig tableConfig()
    {
        HoodieTableConfig tableConfig = new HoodieTableConfig();
        tableConfig.setValue(HoodieTableConfig.VERSION, "9");
        tableConfig.setValue(HoodieTableConfig.RECORD_MERGE_MODE, EVENT_TIME_ORDERING.name());
        tableConfig.setValue(HoodieTableConfig.ORDERING_FIELDS, "ordering");
        return tableConfig;
    }

    private static HudiTrinoReaderContext createReaderContext(HoodieSchema schema, HoodieTableConfig tableConfig, TypedProperties props)
    {
        // These tests consume classic log records directly; no page source is read.
        HudiTrinoReaderContext readerContext = new HudiTrinoReaderContext(
                new TrinoStorageConfiguration(), tableConfig, new EmptyPageSource(), List.of(), null,
                (path, start, length, projection) -> {
                    throw new AssertionError("Unexpected parquet read");
                });
        TypedProperties mergeProps = ConfigUtils.getMergeProps(props, tableConfig);
        readerContext.initRecordMerger(mergeProps);
        readerContext.setHasLogFiles(false);
        readerContext.setHasBootstrapBaseFile(false);
        HoodieSchema projectedSchema = HoodieSchema.fromAvroSchema(SchemaBuilder.record("test_record").fields().requiredString("id").endRecord());
        // The ordering field can be absent from the projection, so initialization must use the table schema.
        readerContext.setSchemaHandler(new FileGroupReaderSchemaHandler<>(
                readerContext, schema, projectedSchema, Option.empty(), mergeProps,
                TableState.of(new StoragePath("/test-table"), tableConfig, Option.empty(), Option.empty())));
        return readerContext;
    }

    private static BufferedRecord<IndexedRecord> readDelete(HudiTrinoReaderContext readerContext, Comparable<?> orderingValue)
            throws IOException
    {
        HoodieDeleteBlock written = new HoodieDeleteBlock(List.of(Pair.of(DeleteRecord.create("key", "", orderingValue), -1L)), Map.of());
        HoodieDeleteBlock read = new HoodieDeleteBlock(Option.of(written.getContentBytes(null).toByteArray()),
                null, false, Option.empty(), Map.of(), Map.of());
        return read.getRecordsToDelete(readerContext).getFirst();
    }
}
