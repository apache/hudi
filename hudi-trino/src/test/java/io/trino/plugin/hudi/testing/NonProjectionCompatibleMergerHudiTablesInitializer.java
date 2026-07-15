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
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static java.nio.file.Files.createTempDirectory;

/**
 * Creates a non-partitioned Merge-On-Read table at test runtime configured with the
 * NON-projection-compatible {@link NonProjectionCompatibleRankMerger} via {@link RecordMergeMode#CUSTOM}.
 * One {@code bulkInsert} (base files) is followed by one {@code upsert} of the same keys (log files), with
 * inline compaction disabled so the merger runs at read time over a full-table-schema read.
 * <p>
 * Data is laid out so each merge direction is distinguishable: one key's winning rank comes from the LOG
 * record and the other's from the BASE record, so a correct result proves both sides of the merge supplied
 * the (never projected) {@code merge_rank} column. See {@code TestHudiNonProjectionCompatibleMerger}.
 */
public class NonProjectionCompatibleMergerHudiTablesInitializer
        implements HudiTablesInitializer
{
    public static final String TABLE_NAME = "non_projection_merger_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";

    private static final String RECORD_KEY_FIELD = "key";
    private static final String RANK_FIELD = NonProjectionCompatibleRankMerger.RANK_COLUMN;
    private static final String ORDERING_FIELD = "ts";
    private static final String PARTITION_PATH = "";

    private static final List<Column> DATA_COLUMNS = ImmutableList.of(
            new Column("_hoodie_commit_time", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_commit_seqno", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_record_key", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_partition_path", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_file_name", HIVE_STRING, Optional.empty(), Map.of()),
            new Column(RECORD_KEY_FIELD, HIVE_STRING, Optional.empty(), Map.of()),
            new Column("name", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("value", HIVE_LONG, Optional.empty(), Map.of()),
            new Column(RANK_FIELD, HIVE_LONG, Optional.empty(), Map.of()),
            new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()));

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

        Location tableLocation = externalLocation.appendPath(TABLE_NAME);

        java.nio.file.Path tempDir = createTempDirectory("non-projection-merger-mor");
        try {
            java.nio.file.Path tempTableDir = tempDir.resolve(TABLE_NAME);
            writeTable(new Path(tempTableDir.toUri()));
            ResourceHudiTablesInitializer.copyDir(tempTableDir, fileSystem, tableLocation);

            metastore.createTable(createTableDefinition(schemaName, TABLE_NAME, tableLocation, false), PrincipalPrivileges.NO_PRIVILEGES);
            metastore.createTable(createTableDefinition(schemaName, RT_TABLE_NAME, tableLocation, true), PrincipalPrivileges.NO_PRIVILEGES);
        }
        finally {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }

    private static void writeTable(Path tablePath)
    {
        Schema schema = createAvroSchema();
        try (HoodieJavaWriteClient<HoodieAvroPayload> writeClient = createWriteClient(schema, tablePath)) {
            // First commit: bulk insert base records (produces base parquet files).
            String firstCommit = writeClient.startCommit();
            List<WriteStatus> firstStatuses = writeClient.bulkInsert(ImmutableList.of(
                    record(schema, "k1", "k1_base", 10L, 5L, 1L),
                    record(schema, "k2", "k2_base", 100L, 9L, 1L)), firstCommit);
            writeClient.commit(firstCommit, firstStatuses);

            // Second commit: upserts the same keys (produces log files since inline compaction is disabled).
            // k1 update has a HIGHER rank (7 > 5) -> merge keeps the update (99): the LOG record's rank decides.
            // k2 update has a LOWER rank (1 < 9) -> merge keeps the base record (100): the BASE record's rank
            // decides, which only works when the base read carries merge_rank despite it never being projected.
            String secondCommit = writeClient.startCommit();
            List<WriteStatus> secondStatuses = writeClient.upsert(ImmutableList.of(
                    record(schema, "k1", "k1_updated", 99L, 7L, 2L),
                    record(schema, "k2", "k2_updated", 4L, 1L, 2L)), secondCommit);
            writeClient.commit(secondCommit, secondStatuses);
        }
    }

    private static HoodieJavaWriteClient<HoodieAvroPayload> createWriteClient(Schema schema, Path tablePath)
    {
        Configuration conf = new Configuration();
        try {
            HoodieTableMetaClient.newTableBuilder()
                    .setTableType(HoodieTableType.MERGE_ON_READ)
                    .setTableName(TABLE_NAME)
                    .setTimelineLayoutVersion(1)
                    .setBootstrapIndexClass(NoOpBootstrapIndex.class.getName())
                    .setPayloadClassName(HoodieAvroPayload.class.getName())
                    .setRecordKeyFields(RECORD_KEY_FIELD)
                    .setOrderingFields(ORDERING_FIELD)
                    .setRecordMergeMode(RecordMergeMode.CUSTOM)
                    .setRecordMergeStrategyId(NonProjectionCompatibleRankMerger.MERGE_STRATEGY_ID)
                    .initTable(new HadoopStorageConfiguration(conf), tablePath.toString());
        }
        catch (IOException e) {
            throw new RuntimeException("Could not init table " + TABLE_NAME, e);
        }

        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
                .withPath(tablePath.toString())
                .withSchema(schema.toString())
                .withParallelism(2, 2)
                .withDeleteParallelism(2)
                .forTable(TABLE_NAME)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                // Ordering field is carried by the table config (setOrderingFields); avoid the deprecated
                // withPreCombineField builder method which fails the -Werror compile gate.
                .withRecordMergeMode(RecordMergeMode.CUSTOM)
                .withRecordMergeStrategyId(NonProjectionCompatibleRankMerger.MERGE_STRATEGY_ID)
                .withRecordMergeImplClasses(NonProjectionCompatibleRankMerger.class.getName())
                // Keep log files around so the custom merger runs at read time.
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

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, String key, String name, long value, long rank, long ts)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put("name", name);
        record.put("value", value);
        record.put(RANK_FIELD, rank);
        record.put(ORDERING_FIELD, ts);
        HoodieKey hoodieKey = new HoodieKey(key, PARTITION_PATH);
        return new HoodieAvroRecord<>(hoodieKey, new HoodieAvroPayload(Option.of(record)), null);
    }

    private static Schema createAvroSchema()
    {
        List<Schema.Field> fields = ImmutableList.of(
                new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field("name", Schema.create(Schema.Type.STRING)),
                new Schema.Field("value", Schema.create(Schema.Type.LONG)),
                new Schema.Field(RANK_FIELD, Schema.create(Schema.Type.LONG)),
                new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)));
        return Schema.createRecord(TABLE_NAME, null, null, false, new ArrayList<>(fields));
    }

    private static Table createTableDefinition(String schemaName, String tableName, Location location, boolean isRtTable)
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
                .setDataColumns(DATA_COLUMNS)
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(storageFormat)
                        .setLocation(location.toString()))
                .build();
    }
}
