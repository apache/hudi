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
package io.trino.plugin.hudi.testing;

import com.google.common.collect.ImmutableList;
import io.trino.metastore.Column;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;

/**
 * Creates a non-partitioned table whose METASTORE column list omits the five {@code _hoodie_*} meta columns the
 * base file itself carries -- the layout hive sync leaves behind with
 * {@code hoodie.datasource.hive_sync.omit_metadata_fields=true}. Every data column's metastore ordinal is therefore
 * five below its physical position, which is the shape {@code hudi.parquet.use-column-names=false} resolves
 * positionally and the one apache/hudi#19387 was about. Every other fixture in this module registers the meta
 * columns ({@code ResourceHudiTablesInitializer.TestingTable.getDataColumns} prepends them unconditionally), so
 * metastore ordinal equals physical ordinal there and the bug cannot appear.
 * <p>
 * The column list looks padded on purpose. Physical fields 0..4 are always the meta columns, so a data column at
 * metastore ordinal {@code i} is read positionally at physical field {@code i}, which is a meta column for any
 * {@code i < 5}. A meta column can never be part of a projection here -- the metastore does not expose it -- so
 * {@code descriptorsByPath} has no entry for it and a stray domain is silently discarded rather than misapplied.
 * Only from the SIXTH data column on does the stale ordinal land on a real, projectable column. Hence
 * {@code late_value} at metastore ordinal 5, shadowed by {@code shadowed_value} at physical field 5, and the four
 * fillers in between.
 * <p>
 * The values are disjoint so the damage is unambiguous: {@code shadowed_value} stays in 1..5 while
 * {@code late_value} is above 1000, so a domain meant for {@code late_value} but matched against
 * {@code shadowed_value}'s statistics prunes the only row group and the query returns nothing.
 * <p>
 * A single bulk-insert commit, so the file slice has no log files: predicate pushdown is only enabled for
 * base-file-only splits. See {@code TestHudiConnectorParquetColumnNamesTest}.
 */
public class OmittedMetaColumnsHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "omitted_meta_columns_mor";

    /** The column the predicate goes on: metastore ordinal 5, physical field 10. */
    public static final String LATE_COLUMN = "late_value";
    /** The column physically sitting at {@link #LATE_COLUMN}'s stale ordinal, and therefore the one that shadows it. */
    public static final String SHADOWED_COLUMN = "shadowed_value";
    /** Below every {@link #LATE_COLUMN} value and above every {@link #SHADOWED_COLUMN} one. */
    public static final long THRESHOLD = 900;

    private static final int ROW_COUNT = 5;
    private static final List<String> FILLER_COLUMNS = ImmutableList.of("filler_1", "filler_2", "filler_3", "filler_4");

    public OmittedMetaColumnsHudiTablesInitializer()
    {
        super(TABLE_NAME);
    }

    @Override
    protected boolean includeMetaColumnsInMetastore()
    {
        return false;
    }

    @Override
    protected List<Column> dataColumns()
    {
        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        columns.add(new Column(SHADOWED_COLUMN, HIVE_LONG, Optional.empty(), Map.of()));
        FILLER_COLUMNS.forEach(name -> columns.add(new Column(name, HIVE_LONG, Optional.empty(), Map.of())));
        columns.add(new Column(LATE_COLUMN, HIVE_LONG, Optional.empty(), Map.of()));
        columns.add(new Column(RECORD_KEY_FIELD, HIVE_STRING, Optional.empty(), Map.of()));
        columns.add(new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()));
        return columns.build();
    }

    @Override
    protected Schema avroSchema()
    {
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field(SHADOWED_COLUMN, Schema.create(Schema.Type.LONG)));
        FILLER_COLUMNS.forEach(name -> fields.add(new Schema.Field(name, Schema.create(Schema.Type.LONG))));
        fields.add(new Schema.Field(LATE_COLUMN, Schema.create(Schema.Type.LONG)));
        fields.add(new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)));
        fields.add(new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)));
        return Schema.createRecord(TABLE_NAME, null, null, false, fields);
    }

    @Override
    protected void configureTableConfig(HoodieTableMetaClient.TableBuilder tableBuilder)
    {
        tableBuilder.setRecordMergeMode(RecordMergeMode.COMMIT_TIME_ORDERING);
    }

    @Override
    protected void configureWriteConfig(HoodieWriteConfig.Builder writeConfigBuilder)
    {
        writeConfigBuilder.withRecordMergeMode(RecordMergeMode.COMMIT_TIME_ORDERING);
    }

    @Override
    protected void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
    {
        Schema schema = avroSchema();
        List<HoodieRecord<HoodieAvroPayload>> records = new ArrayList<>();
        for (int row = 1; row <= ROW_COUNT; row++) {
            records.add(record(schema, "k" + row, row, 1000L + row));
        }
        // One commit only: a file slice with log files would take the merge path, which disables pushdown.
        String commit = client.startCommit();
        List<WriteStatus> statuses = client.bulkInsert(records, commit);
        client.commit(commit, statuses);
    }

    /** The expected rows of {@code SELECT key, shadowed_value, late_value ... WHERE late_value > THRESHOLD}. */
    public static String expectedRowsAboveThreshold()
    {
        List<String> rows = new ArrayList<>();
        for (int row = 1; row <= ROW_COUNT; row++) {
            rows.add("('k%s', CAST(%s AS BIGINT), CAST(%s AS BIGINT))".formatted(row, row, 1000 + row));
        }
        return "VALUES " + String.join(", ", rows);
    }

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, String key, long shadowedValue, long lateValue)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(SHADOWED_COLUMN, shadowedValue);
        FILLER_COLUMNS.forEach(name -> record.put(name, 0L));
        record.put(LATE_COLUMN, lateValue);
        record.put(RECORD_KEY_FIELD, key);
        record.put(ORDERING_FIELD, 100L);
        return avroRecord(record, key);
    }
}
