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
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_REALTIME_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.metastore.HiveType.HIVE_BOOLEAN;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static java.nio.file.Files.createTempDirectory;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;

/**
 * Creates two non-partitioned Merge-On-Read tables at test runtime that exercise the read-side
 * MERGE-MODE dispatch with deletes (issue apache/hudi#18898). Both tables set ONLY a record merge mode
 * (no payload class) so table creation persists the mode as-is:
 * <ul>
 *   <li>{@code mor_deletes} ({@link RecordMergeMode#EVENT_TIME_ORDERING}): a base commit followed by a
 *       log commit with an update, a soft delete ({@code _hoodie_is_deleted=true}), an OBSOLETE soft
 *       delete and an OBSOLETE update (both with an ordering value LOWER than the base row's, so
 *       event-time merging must keep the base row), plus a hard-delete commit
 *       ({@code writeClient.delete}) that produces a native delete log file read back through the
 *       connector's {@code getFileRecordIterator}.</li>
 *   <li>{@code mor_commit_time} ({@link RecordMergeMode#COMMIT_TIME_ORDERING}): the same
 *       lower-ordering update shape, where latest-write-wins must KEEP the update -- the exact mirror
 *       of the event-time case, discriminating the two merger dispatches.</li>
 * </ul>
 * Records are wrapped in {@link HoodieAvroPayload}, which implements {@code HoodieRecordPayload}
 * directly (NOT {@code BaseAvroPayload}), so rows with {@code _hoodie_is_deleted=true} are written as
 * DATA records and delete semantics are evaluated at READ time. Inline compaction is disabled so log
 * files survive. Each table registers a read-optimized and a {@code _rt} metastore table.
 */
public class MergeModeSemanticsHudiTablesInitializer
        implements HudiTablesInitializer
{
    public static final String DELETES_TABLE_NAME = "mor_deletes";
    public static final String DELETES_RT_TABLE_NAME = DELETES_TABLE_NAME + "_rt";
    public static final String COMMIT_TIME_TABLE_NAME = "mor_commit_time";
    public static final String COMMIT_TIME_RT_TABLE_NAME = COMMIT_TIME_TABLE_NAME + "_rt";

    private static final String RECORD_KEY_FIELD = "key";
    private static final String ORDERING_FIELD = "ts";
    private static final String PARTITION_PATH = "";

    private static final List<Column> META_COLUMNS = ImmutableList.of(
            new Column("_hoodie_commit_time", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_commit_seqno", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_record_key", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_partition_path", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_file_name", HIVE_STRING, Optional.empty(), Map.of()));

    private static final List<Column> DELETES_DATA_COLUMNS = ImmutableList.<Column>builder()
            .addAll(META_COLUMNS)
            .add(new Column(RECORD_KEY_FIELD, HIVE_STRING, Optional.empty(), Map.of()))
            .add(new Column("name", HIVE_STRING, Optional.empty(), Map.of()))
            .add(new Column("value", HIVE_LONG, Optional.empty(), Map.of()))
            .add(new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()))
            .add(new Column(HOODIE_IS_DELETED_FIELD, HIVE_BOOLEAN, Optional.empty(), Map.of()))
            .build();

    private static final List<Column> COMMIT_TIME_DATA_COLUMNS = ImmutableList.<Column>builder()
            .addAll(META_COLUMNS)
            .add(new Column(RECORD_KEY_FIELD, HIVE_STRING, Optional.empty(), Map.of()))
            .add(new Column("name", HIVE_STRING, Optional.empty(), Map.of()))
            .add(new Column("value", HIVE_LONG, Optional.empty(), Map.of()))
            .add(new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()))
            .build();

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

        java.nio.file.Path tempDir = createTempDirectory("merge-mode-semantics-mor");
        try {
            writeDeletesTable(new Path(tempDir.resolve(DELETES_TABLE_NAME).toUri()));
            writeCommitTimeTable(new Path(tempDir.resolve(COMMIT_TIME_TABLE_NAME).toUri()));

            for (String tableName : List.of(DELETES_TABLE_NAME, COMMIT_TIME_TABLE_NAME)) {
                Location tableLocation = externalLocation.appendPath(tableName);
                ResourceHudiTablesInitializer.copyDir(tempDir.resolve(tableName), fileSystem, tableLocation);
                List<Column> columns = tableName.equals(DELETES_TABLE_NAME) ? DELETES_DATA_COLUMNS : COMMIT_TIME_DATA_COLUMNS;
                metastore.createTable(createTableDefinition(schemaName, tableName, columns, tableLocation, false), PrincipalPrivileges.NO_PRIVILEGES);
                metastore.createTable(createTableDefinition(schemaName, tableName + "_rt", columns, tableLocation, true), PrincipalPrivileges.NO_PRIVILEGES);
            }
        }
        finally {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }

    private static void writeDeletesTable(Path tablePath)
    {
        Schema schema = deletesAvroSchema();
        try (HoodieJavaWriteClient<HoodieAvroPayload> writeClient =
                createWriteClient(schema, tablePath, DELETES_TABLE_NAME, RecordMergeMode.EVENT_TIME_ORDERING)) {
            // First commit: base parquet file with 6 keys, all at ordering value (ts) 100.
            String firstCommit = writeClient.startCommit();
            List<WriteStatus> firstStatuses = writeClient.bulkInsert(ImmutableList.of(
                    deletesRecord(schema, "k1", "k1_base", 10L, 100L, false),
                    deletesRecord(schema, "k2", "k2_base", 20L, 100L, false),
                    deletesRecord(schema, "k3", "k3_base", 30L, 100L, false),
                    deletesRecord(schema, "k4", "k4_base", 40L, 100L, false),
                    deletesRecord(schema, "k5", "k5_base", 50L, 100L, false),
                    deletesRecord(schema, "k6", "k6_base", 60L, 100L, false)), firstCommit);
            writeClient.commit(firstCommit, firstStatuses);

            // Second commit (log file). Event-time merging must resolve each key by ordering value:
            //  - k1: update with HIGHER ts (200) -> update wins
            //  - k3: soft delete with HIGHER ts (200) -> row deleted at read time
            //  - k4: soft delete with LOWER ts (50) -> OBSOLETE delete, base row survives
            //  - k6: update with LOWER ts (50) -> OBSOLETE update, base row survives
            String secondCommit = writeClient.startCommit();
            List<WriteStatus> secondStatuses = writeClient.upsert(ImmutableList.of(
                    deletesRecord(schema, "k1", "k1_updated", 11L, 200L, false),
                    deletesRecord(schema, "k3", "k3_deleted", 33L, 200L, true),
                    deletesRecord(schema, "k4", "k4_deleted", 44L, 50L, true),
                    deletesRecord(schema, "k6", "k6_updated", 66L, 50L, false)), secondCommit);
            writeClient.commit(secondCommit, secondStatuses);

            // Third commit: HARD delete of k2. At the current table version this produces a native
            // delete log file, which the file-group reader reads back through the connector's
            // getFileRecordIterator with the synthetic delete-log schema (record key + ordering).
            // Hard deletes carry the sentinel ordering value and win regardless of merge mode.
            String deleteCommit = writeClient.startCommit();
            List<WriteStatus> deleteStatuses = writeClient.delete(
                    ImmutableList.of(new HoodieKey("k2", PARTITION_PATH)), deleteCommit);
            writeClient.commit(deleteCommit, deleteStatuses);
        }
    }

    private static void writeCommitTimeTable(Path tablePath)
    {
        Schema schema = commitTimeAvroSchema();
        try (HoodieJavaWriteClient<HoodieAvroPayload> writeClient =
                createWriteClient(schema, tablePath, COMMIT_TIME_TABLE_NAME, RecordMergeMode.COMMIT_TIME_ORDERING)) {
            // First commit: base parquet file with 3 keys at ts 100.
            String firstCommit = writeClient.startCommit();
            List<WriteStatus> firstStatuses = writeClient.bulkInsert(ImmutableList.of(
                    commitTimeRecord(schema, "k1", "k1_base", 10L, 100L),
                    commitTimeRecord(schema, "k2", "k2_base", 20L, 100L),
                    commitTimeRecord(schema, "k3", "k3_base", 30L, 100L)), firstCommit);
            writeClient.commit(firstCommit, firstStatuses);

            // Second commit (log file): update k1 with a LOWER ts (50). Commit-time ordering keeps the
            // LATEST WRITE regardless of the ordering value -- the exact mirror of the event-time k6
            // case above, which discriminates OverwriteWithLatestMerger from event-time merging.
            String secondCommit = writeClient.startCommit();
            List<WriteStatus> secondStatuses = writeClient.upsert(ImmutableList.of(
                    commitTimeRecord(schema, "k1", "k1_updated", 11L, 50L)), secondCommit);
            writeClient.commit(secondCommit, secondStatuses);

            // Third commit: hard delete of k2 (commit-time deletes always win).
            String deleteCommit = writeClient.startCommit();
            List<WriteStatus> deleteStatuses = writeClient.delete(
                    ImmutableList.of(new HoodieKey("k2", PARTITION_PATH)), deleteCommit);
            writeClient.commit(deleteCommit, deleteStatuses);
        }
    }

    private static HoodieJavaWriteClient<HoodieAvroPayload> createWriteClient(Schema schema, Path tablePath, String tableName, RecordMergeMode mergeMode)
    {
        Configuration conf = new Configuration();
        try {
            // Only the merge mode is set (NO payload class): table creation persists the mode as-is,
            // which is exactly the dispatch input HudiTrinoReaderContext.getRecordMerger switches on.
            HoodieTableMetaClient.newTableBuilder()
                    .setTableType(HoodieTableType.MERGE_ON_READ)
                    .setTableName(tableName)
                    .setTimelineLayoutVersion(1)
                    .setBootstrapIndexClass(NoOpBootstrapIndex.class.getName())
                    .setRecordKeyFields(RECORD_KEY_FIELD)
                    .setOrderingFields(ORDERING_FIELD)
                    .setRecordMergeMode(mergeMode)
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
                .withRecordMergeMode(mergeMode)
                // Keep log files around so merge-mode semantics apply at read time.
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

    private static HoodieRecord<HoodieAvroPayload> deletesRecord(Schema schema, String key, String name, long value, long ts, boolean deleted)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put("name", name);
        record.put("value", value);
        record.put(ORDERING_FIELD, ts);
        record.put(HOODIE_IS_DELETED_FIELD, deleted);
        // HoodieAvroPayload passes the record through untouched (it is not a BaseAvroPayload), so a row
        // with _hoodie_is_deleted=true is WRITTEN as a data record and only deleted at merge/read time.
        return new HoodieAvroRecord<>(new HoodieKey(key, PARTITION_PATH), new HoodieAvroPayload(Option.of(record)), null);
    }

    private static HoodieRecord<HoodieAvroPayload> commitTimeRecord(Schema schema, String key, String name, long value, long ts)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put("name", name);
        record.put("value", value);
        record.put(ORDERING_FIELD, ts);
        return new HoodieAvroRecord<>(new HoodieKey(key, PARTITION_PATH), new HoodieAvroPayload(Option.of(record)), null);
    }

    private static Schema deletesAvroSchema()
    {
        List<Schema.Field> fields = ImmutableList.of(
                new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field("name", Schema.create(Schema.Type.STRING)),
                new Schema.Field("value", Schema.create(Schema.Type.LONG)),
                new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)),
                new Schema.Field(
                        HOODIE_IS_DELETED_FIELD,
                        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN)),
                        null,
                        JsonProperties.NULL_VALUE));
        return Schema.createRecord(DELETES_TABLE_NAME, null, null, false, new ArrayList<>(fields));
    }

    private static Schema commitTimeAvroSchema()
    {
        List<Schema.Field> fields = ImmutableList.of(
                new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field("name", Schema.create(Schema.Type.STRING)),
                new Schema.Field("value", Schema.create(Schema.Type.LONG)),
                new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)));
        return Schema.createRecord(COMMIT_TIME_TABLE_NAME, null, null, false, new ArrayList<>(fields));
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
