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

import static io.trino.metastore.HiveType.HIVE_DOUBLE;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;

/**
 * Creates a table whose base file was written BEFORE its columns were widened, while the metastore reports the types
 * they were widened TO -- the state every unrewritten base file is in after a Hudi type evolution followed by a hive
 * sync. The two halves are declared independently on purpose: {@link #avroSchema()} is what the write client puts in
 * the file, {@link #dataColumns()} is what the metastore hands the connector, and only their disagreement is being
 * modelled. No schema-evolution write path is exercised, because none is needed to reproduce apache/hudi#19457.
 * <p>
 * The three widenings are the ones Hudi allows and the parquet reader can serve: {@code float -> double},
 * {@code int -> long} and {@code int -> string}. Reading them has always worked. Putting a predicate on one is what
 * used to fail the query, because the statistics in the file are still of the original type while the pushed-down
 * domain carries the widened one.
 * <p>
 * Unlike {@link OmittedMetaColumnsHudiTablesInitializer} this fixture registers the Hudi meta columns, so every
 * metastore ordinal equals its physical one and nothing here depends on how columns are resolved. What the two
 * {@code hudi.parquet.use-column-names} modes must agree on is the TYPE handling alone.
 * <p>
 * A single bulk-insert commit, so the file slice has no log files: predicate pushdown is only enabled for
 * base-file-only splits.
 */
public class SchemaEvolutionHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "schema_evolved_mor";

    /** Written as Avro {@code float}, reported by the metastore as {@code double}. */
    public static final String FLOAT_TO_DOUBLE_COLUMN = "float_value";
    /** Written as Avro {@code int}, reported by the metastore as {@code bigint}. */
    public static final String INT_TO_BIGINT_COLUMN = "int_value";
    /** Written as Avro {@code int}, reported by the metastore as {@code string}. */
    public static final String INT_TO_VARCHAR_COLUMN = "string_value";

    /** Sits between the third and the fourth row, so a predicate on it keeps some rows and drops others. */
    public static final String DOUBLE_THRESHOLD = "1003.0";
    public static final long BIGINT_THRESHOLD = 1003;
    public static final String VARCHAR_THRESHOLD = "1003";

    private static final int ROW_COUNT = 5;
    private static final int BASE_VALUE = 1000;

    public SchemaEvolutionHudiTablesInitializer()
    {
        super(TABLE_NAME);
    }

    @Override
    protected List<Column> dataColumns()
    {
        return ImmutableList.of(
                new Column(FLOAT_TO_DOUBLE_COLUMN, HIVE_DOUBLE, Optional.empty(), Map.of()),
                new Column(INT_TO_BIGINT_COLUMN, HIVE_LONG, Optional.empty(), Map.of()),
                new Column(INT_TO_VARCHAR_COLUMN, HIVE_STRING, Optional.empty(), Map.of()),
                new Column(RECORD_KEY_FIELD, HIVE_STRING, Optional.empty(), Map.of()),
                new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()));
    }

    @Override
    protected Schema avroSchema()
    {
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field(FLOAT_TO_DOUBLE_COLUMN, Schema.create(Schema.Type.FLOAT)));
        fields.add(new Schema.Field(INT_TO_BIGINT_COLUMN, Schema.create(Schema.Type.INT)));
        fields.add(new Schema.Field(INT_TO_VARCHAR_COLUMN, Schema.create(Schema.Type.INT)));
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
            records.add(record(schema, row));
        }
        // One commit only: a file slice with log files would take the merge path, which disables pushdown.
        String commit = client.startCommit();
        List<WriteStatus> statuses = client.bulkInsert(records, commit);
        client.commit(commit, statuses);
    }

    /**
     * The expected rows of {@code SELECT key, float_value, int_value, string_value ... WHERE <column> > <threshold>}
     * for the rows whose index is over {@code firstMatchingRow}.
     * <p>
     * Every float value is an exact binary fraction, so widening it to double is lossless and the expected literals
     * can be written out in full rather than compared with a tolerance.
     */
    public static String expectedRowsFrom(int firstMatchingRow)
    {
        List<String> rows = new ArrayList<>();
        for (int row = firstMatchingRow; row <= ROW_COUNT; row++) {
            rows.add("('k%s', CAST(%s AS DOUBLE), CAST(%s AS BIGINT), '%s')".formatted(
                    row, floatValue(row), BASE_VALUE + row, BASE_VALUE + row));
        }
        return "VALUES " + String.join(", ", rows);
    }

    private static float floatValue(int row)
    {
        return BASE_VALUE + row + 0.5f;
    }

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, int row)
    {
        String key = "k" + row;
        GenericRecord record = new GenericData.Record(schema);
        record.put(FLOAT_TO_DOUBLE_COLUMN, floatValue(row));
        record.put(INT_TO_BIGINT_COLUMN, BASE_VALUE + row);
        record.put(INT_TO_VARCHAR_COLUMN, BASE_VALUE + row);
        record.put(RECORD_KEY_FIELD, key);
        record.put(ORDERING_FIELD, 100L);
        return avroRecord(record, key);
    }
}
