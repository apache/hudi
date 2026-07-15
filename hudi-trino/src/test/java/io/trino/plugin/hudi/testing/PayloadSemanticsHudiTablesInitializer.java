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
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.HiveType;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.hudi.HudiConnector;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.QueryRunner;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.bootstrap.index.NoOpBootstrapIndex;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_REALTIME_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static java.nio.file.Files.createTempDirectory;

/**
 * Creates three non-partitioned Merge-On-Read tables at test runtime whose merge semantics come from a
 * PAYLOAD CLASS persisted in the table config (issue apache/hudi#18898):
 * <ul>
 *   <li>{@code mor_dms} ({@link AWSDmsAvroPayload}): at the current table version this "deprecated"
 *       payload is translated at creation into COMMIT_TIME_ORDERING plus PREFIXED delete-key props
 *       ({@code hoodie.record.merge.property.hoodie.payload.delete.field=Op}, marker {@code D}); a log
 *       record with {@code Op='D'} deletes the row at merge time via {@code DeleteContext}, with the
 *       payload never executing at read.</li>
 *   <li>{@code mor_overwrite_non_defaults} ({@link OverwriteNonDefaultsWithLatestAvroPayload}):
 *       translated into COMMIT_TIME_ORDERING plus {@code PARTIAL_UPDATE_MODE=IGNORE_DEFAULTS}; an
 *       update whose column is null (the schema default) must keep the STORED value for that column.</li>
 *   <li>{@code mor_summing} ({@link SummingTestPayload}): a user-defined payload NOT in the deprecation
 *       set, persisted as RECORD_MERGE_MODE=CUSTOM with the payload-based merge strategy id; reads
 *       resolve {@code HoodieAvroRecordMerger} (no {@code hudi.record-merger-impls} needed) and run the
 *       payload's {@code combineAndGetUpdateValue}, observable as SUMMED values.</li>
 * </ul>
 * Records are wrapped in {@link HoodieAvroPayload} (a pass-through that is NOT a {@code BaseAvroPayload}),
 * so rows carrying {@code Op='D'} are written as DATA records and every merge decision happens at read
 * time. Inline compaction is disabled so log files survive. Each table registers a read-optimized and a
 * {@code _rt} metastore table.
 */
public class PayloadSemanticsHudiTablesInitializer
        implements HudiTablesInitializer
{
    public static final String DMS_TABLE_NAME = "mor_dms";
    public static final String DMS_RT_TABLE_NAME = DMS_TABLE_NAME + "_rt";
    public static final String NON_DEFAULTS_TABLE_NAME = "mor_overwrite_non_defaults";
    public static final String NON_DEFAULTS_RT_TABLE_NAME = NON_DEFAULTS_TABLE_NAME + "_rt";
    public static final String SUMMING_TABLE_NAME = "mor_summing";
    public static final String SUMMING_RT_TABLE_NAME = SUMMING_TABLE_NAME + "_rt";

    private static final String RECORD_KEY_FIELD = "key";
    private static final String ORDERING_FIELD = "ts";
    private static final String DMS_OP_FIELD = "Op";
    private static final String PARTITION_PATH = "";

    @Override
    public void initializeTables(QueryRunner queryRunner, Location externalLocation, String schemaName)
            throws Exception
    {
        TrinoFileSystem fileSystem = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
        HiveMetastore metastore = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        java.nio.file.Path tempDir = createTempDirectory("payload-semantics-mor");
        try {
            writeDmsTable(new Path(tempDir.resolve(DMS_TABLE_NAME).toUri()));
            writeNonDefaultsTable(new Path(tempDir.resolve(NON_DEFAULTS_TABLE_NAME).toUri()));
            writeSummingTable(new Path(tempDir.resolve(SUMMING_TABLE_NAME).toUri()));

            Map<String, List<Column>> tableColumns = new LinkedHashMap<>();
            tableColumns.put(DMS_TABLE_NAME, dmsColumns());
            tableColumns.put(NON_DEFAULTS_TABLE_NAME, nonDefaultsColumns());
            tableColumns.put(SUMMING_TABLE_NAME, summingColumns());

            for (Map.Entry<String, List<Column>> entry : tableColumns.entrySet()) {
                String tableName = entry.getKey();
                Location tableLocation = externalLocation.appendPath(tableName);
                ResourceHudiTablesInitializer.copyDir(tempDir.resolve(tableName), fileSystem, tableLocation);
                metastore.createTable(createTableDefinition(schemaName, tableName, entry.getValue(), tableLocation, false), PrincipalPrivileges.NO_PRIVILEGES);
                metastore.createTable(createTableDefinition(schemaName, tableName + "_rt", entry.getValue(), tableLocation, true), PrincipalPrivileges.NO_PRIVILEGES);
            }
        }
        finally {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }

    private static void writeDmsTable(Path tablePath)
    {
        Schema schema = dmsAvroSchema();
        try (HoodieJavaWriteClient<HoodieAvroPayload> writeClient =
                createWriteClient(schema, tablePath, DMS_TABLE_NAME, AWSDmsAvroPayload.class.getName())) {
            String firstCommit = writeClient.startCommit();
            List<WriteStatus> firstStatuses = writeClient.bulkInsert(ImmutableList.of(
                    dmsRecord(schema, "k1", "k1_base", 10L, "I", 100L),
                    dmsRecord(schema, "k2", "k2_base", 20L, "I", 100L)), firstCommit);
            writeClient.commit(firstCommit, firstStatuses);

            // Log record with Op='D'. The pass-through HoodieAvroPayload writes it as a DATA record;
            // DeleteContext (delete key Op, marker D from the translated table config) deletes the row
            // at merge time.
            String secondCommit = writeClient.startCommit();
            List<WriteStatus> secondStatuses = writeClient.upsert(ImmutableList.of(
                    dmsRecord(schema, "k2", "k2_deleted", 22L, "D", 200L)), secondCommit);
            writeClient.commit(secondCommit, secondStatuses);
        }
    }

    private static void writeNonDefaultsTable(Path tablePath)
    {
        Schema schema = nonDefaultsAvroSchema();
        try (HoodieJavaWriteClient<HoodieAvroPayload> writeClient =
                createWriteClient(schema, tablePath, NON_DEFAULTS_TABLE_NAME, OverwriteNonDefaultsWithLatestAvroPayload.class.getName())) {
            String firstCommit = writeClient.startCommit();
            List<WriteStatus> firstStatuses = writeClient.bulkInsert(ImmutableList.of(
                    nonDefaultsRecord(schema, "k1", "base_a", "base_b", 100L)), firstCommit);
            writeClient.commit(firstCommit, firstStatuses);

            // Update with b=null (the schema default): IGNORE_DEFAULTS partial merging must keep the
            // stored 'base_b' while taking the updated 'new_a'.
            String secondCommit = writeClient.startCommit();
            List<WriteStatus> secondStatuses = writeClient.upsert(ImmutableList.of(
                    nonDefaultsRecord(schema, "k1", "new_a", null, 200L)), secondCommit);
            writeClient.commit(secondCommit, secondStatuses);
        }
    }

    private static void writeSummingTable(Path tablePath)
    {
        Schema schema = summingAvroSchema();
        try (HoodieJavaWriteClient<HoodieAvroPayload> writeClient =
                createWriteClient(schema, tablePath, SUMMING_TABLE_NAME, SummingTestPayload.class.getName())) {
            String firstCommit = writeClient.startCommit();
            List<WriteStatus> firstStatuses = writeClient.bulkInsert(ImmutableList.of(
                    summingRecord(schema, "k1", 10L, 100L)), firstCommit);
            writeClient.commit(firstCommit, firstStatuses);

            // The payload's combineAndGetUpdateValue SUMS stored and incoming values: 10 + 99 = 109 --
            // a result neither overwrite (99) nor base-only (10) can produce.
            String secondCommit = writeClient.startCommit();
            List<WriteStatus> secondStatuses = writeClient.upsert(ImmutableList.of(
                    summingRecord(schema, "k1", 99L, 200L)), secondCommit);
            writeClient.commit(secondCommit, secondStatuses);
        }
    }

    private static HoodieJavaWriteClient<HoodieAvroPayload> createWriteClient(Schema schema, Path tablePath, String tableName, String payloadClassName)
    {
        Configuration conf = new Configuration();
        try {
            // Only the payload class is set (NO merge mode / strategy id): table creation translates it
            // exactly as a real writer would -- deprecated payloads into merge mode + adjunct configs,
            // user-defined payloads into CUSTOM with the payload-based merge strategy.
            HoodieTableMetaClient.newTableBuilder()
                    .setTableType(HoodieTableType.MERGE_ON_READ)
                    .setTableName(tableName)
                    .setTimelineLayoutVersion(1)
                    .setBootstrapIndexClass(NoOpBootstrapIndex.class.getName())
                    .setPayloadClassName(payloadClassName)
                    .setRecordKeyFields(RECORD_KEY_FIELD)
                    .setOrderingFields(ORDERING_FIELD)
                    .initTable(new HadoopStorageConfiguration(conf), tablePath.toString());
        }
        catch (IOException e) {
            throw new RuntimeException("Could not init table " + tableName, e);
        }

        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
                .withPath(tablePath.toString())
                .withSchema(schema.toString())
                .withParallelism(2, 2)
                .withDeleteParallelism(2)
                .forTable(tableName)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                .withWritePayLoad(payloadClassName)
                // Keep log files around so payload semantics apply at read time.
                .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                        .withInlineCompaction(false)
                        .withMaxNumDeltaCommitsBeforeCompaction(100)
                        .build())
                .withEmbeddedTimelineServerEnabled(false)
                .withMarkersType(MarkerType.DIRECT.name())
                // MDT writes require hbase deps not present in the Trino runtime; disable as other initializers do.
                .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
                .build();
        return new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(new HadoopStorageConfiguration(conf)), cfg);
    }

    private static HoodieRecord<HoodieAvroPayload> dmsRecord(Schema schema, String key, String name, long value, String op, long ts)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put("name", name);
        record.put("value", value);
        record.put(DMS_OP_FIELD, op);
        record.put(ORDERING_FIELD, ts);
        return passThroughRecord(key, record);
    }

    private static HoodieRecord<HoodieAvroPayload> nonDefaultsRecord(Schema schema, String key, String a, String b, long ts)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put("a", a);
        record.put("b", b);
        record.put(ORDERING_FIELD, ts);
        return passThroughRecord(key, record);
    }

    private static HoodieRecord<HoodieAvroPayload> summingRecord(Schema schema, String key, long value, long ts)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put(SummingTestPayload.SUM_COLUMN, value);
        record.put(ORDERING_FIELD, ts);
        return passThroughRecord(key, record);
    }

    private static HoodieRecord<HoodieAvroPayload> passThroughRecord(String key, GenericRecord record)
    {
        // HoodieAvroPayload passes the record through untouched (it is not a BaseAvroPayload), so rows
        // that a semantic payload would drop at write time (e.g. Op='D') land as data records and all
        // merge decisions are made at read time from the table config.
        return new HoodieAvroRecord<>(new HoodieKey(key, PARTITION_PATH), new HoodieAvroPayload(Option.of(record)), null);
    }

    private static Schema dmsAvroSchema()
    {
        List<Schema.Field> fields = ImmutableList.of(
                new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field("name", Schema.create(Schema.Type.STRING)),
                new Schema.Field("value", Schema.create(Schema.Type.LONG)),
                new Schema.Field(DMS_OP_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)));
        return Schema.createRecord(DMS_TABLE_NAME, null, null, false, new ArrayList<>(fields));
    }

    private static Schema nonDefaultsAvroSchema()
    {
        Schema nullableString = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
        List<Schema.Field> fields = ImmutableList.of(
                new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field("a", nullableString, null, JsonProperties.NULL_VALUE),
                new Schema.Field("b", nullableString, null, JsonProperties.NULL_VALUE),
                new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)));
        return Schema.createRecord(NON_DEFAULTS_TABLE_NAME, null, null, false, new ArrayList<>(fields));
    }

    private static Schema summingAvroSchema()
    {
        List<Schema.Field> fields = ImmutableList.of(
                new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field(SummingTestPayload.SUM_COLUMN, Schema.create(Schema.Type.LONG)),
                new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)));
        return Schema.createRecord(SUMMING_TABLE_NAME, null, null, false, new ArrayList<>(fields));
    }

    private static List<Column> dmsColumns()
    {
        return ImmutableList.<Column>builder()
                .addAll(metaColumns())
                .add(column(RECORD_KEY_FIELD, HIVE_STRING))
                .add(column("name", HIVE_STRING))
                .add(column("value", HIVE_LONG))
                // The Avro/parquet field is 'Op' (AWSDms hardcodes that casing), but a real Hive
                // metastore lowercases column names on DDL -- exactly the case mismatch the connector's
                // merge-column matching must bridge
                .add(column(DMS_OP_FIELD.toLowerCase(Locale.ROOT), HIVE_STRING))
                .add(column(ORDERING_FIELD, HIVE_LONG))
                .build();
    }

    private static List<Column> nonDefaultsColumns()
    {
        return ImmutableList.<Column>builder()
                .addAll(metaColumns())
                .add(column(RECORD_KEY_FIELD, HIVE_STRING))
                .add(column("a", HIVE_STRING))
                .add(column("b", HIVE_STRING))
                .add(column(ORDERING_FIELD, HIVE_LONG))
                .build();
    }

    private static List<Column> summingColumns()
    {
        return ImmutableList.<Column>builder()
                .addAll(metaColumns())
                .add(column(RECORD_KEY_FIELD, HIVE_STRING))
                .add(column(SummingTestPayload.SUM_COLUMN, HIVE_LONG))
                .add(column(ORDERING_FIELD, HIVE_LONG))
                .build();
    }

    private static List<Column> metaColumns()
    {
        return ImmutableList.of(
                column("_hoodie_commit_time", HIVE_STRING),
                column("_hoodie_commit_seqno", HIVE_STRING),
                column("_hoodie_record_key", HIVE_STRING),
                column("_hoodie_partition_path", HIVE_STRING),
                column("_hoodie_file_name", HIVE_STRING));
    }

    private static Column column(String name, HiveType type)
    {
        return new Column(name, type, Optional.empty(), Map.of());
    }

    private static Table createTableDefinition(String schemaName, String tableName, List<Column> columns, Location location, boolean isRtTable)
    {
        StorageFormat storageFormat = StorageFormat.create(
                PARQUET_HIVE_SERDE_CLASS,
                isRtTable ? HUDI_PARQUET_REALTIME_INPUT_FORMAT : HUDI_PARQUET_INPUT_FORMAT,
                MAPRED_PARQUET_OUTPUT_FORMAT_CLASS);

        return Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setTableType(EXTERNAL_TABLE.name())
                .setOwner(Optional.of("public"))
                .setDataColumns(columns)
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(storageFormat)
                        .setLocation(location.toString()))
                .build();
    }
}
