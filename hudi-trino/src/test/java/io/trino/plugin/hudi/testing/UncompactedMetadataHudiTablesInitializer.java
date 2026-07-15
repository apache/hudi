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
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
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
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static java.nio.file.Files.createTempDirectory;

/**
 * Creates a partitioned COW table at test runtime with an ENABLED, UNCOMPACTED metadata table (MDT):
 * {@code hoodie.metadata.compact.max.delta.commits} is set high (the zip fixtures use {@code =1}, so
 * their MDTs are always freshly compacted) and several commits are written, leaving the MDT's
 * {@code files}/{@code column_stats}/{@code partition_stats} partitions with native HFILE LOG deltas
 * that the connector must read at query time (issue apache/hudi#19279).
 * <p>
 * Note: MDT writing here is fully native -- HFILE base files and log blocks are written via hudi-io's
 * pure-Java {@code HFileWriterImpl}, so no hbase dependency is involved (the "requires hbase" note in
 * older initializers is stale).
 * <p>
 * Data layout (partitions {@code part_col=p1} / {@code part_col=p2}, hive-style paths so the MDT
 * partition listing and the metastore agree on names):
 * <pre>
 * commit 1 (insert): k1(p1, price 10,  ts 100), k2(p2, price 1000, ts 100)
 * commit 2 (insert): k3(p1, price 20,  ts 200), k4(p2, price 2000, ts 200)
 * commit 3 (upsert): k1(p1, price 15,  ts 300)
 * </pre>
 * Final rows: k1=15, k2=1000, k3=20, k4=2000. Partition p1 holds prices [15, 20] and p2 holds
 * [1000, 2000], so a predicate like {@code price < 100} lets the partition-stats index prune p2.
 */
public class UncompactedMetadataHudiTablesInitializer
        implements HudiTablesInitializer
{
    public static final String TABLE_NAME = "hudi_uncompacted_mdt_pt_cow";

    private static final String RECORD_KEY_FIELD = "id";
    private static final String PARTITION_FIELD = "part_col";
    private static final String ORDERING_FIELD = "ts";
    private static final List<String> PARTITION_PATHS = ImmutableList.of(PARTITION_FIELD + "=p1", PARTITION_FIELD + "=p2");

    private static final List<Column> DATA_COLUMNS = ImmutableList.of(
            new Column("_hoodie_commit_time", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_commit_seqno", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_record_key", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_partition_path", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_file_name", HIVE_STRING, Optional.empty(), Map.of()),
            new Column(RECORD_KEY_FIELD, HIVE_STRING, Optional.empty(), Map.of()),
            new Column("name", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("price", HIVE_LONG, Optional.empty(), Map.of()),
            new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()));

    private static final List<Column> PARTITION_COLUMNS =
            ImmutableList.of(new Column(PARTITION_FIELD, HIVE_STRING, Optional.empty(), Map.of()));

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

        java.nio.file.Path tempDir = createTempDirectory("uncompacted-mdt");
        try {
            java.nio.file.Path tempTableDir = tempDir.resolve(TABLE_NAME);
            writeTable(new Path(tempTableDir.toUri()));
            ResourceHudiTablesInitializer.copyDir(tempTableDir, fileSystem, tableLocation);

            metastore.createTable(createTableDefinition(schemaName, tableLocation), PrincipalPrivileges.NO_PRIVILEGES);
            metastore.addPartitions(schemaName, TABLE_NAME, createPartitions(schemaName, tableLocation));
        }
        finally {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }

    private static void writeTable(Path tablePath)
    {
        Schema schema = createAvroSchema();
        try (HoodieJavaWriteClient<HoodieAvroPayload> writeClient = createWriteClient(schema, tablePath)) {
            String firstCommit = writeClient.startCommit();
            List<WriteStatus> firstStatuses = writeClient.insert(ImmutableList.of(
                    record(schema, "k1", "k1_c1", 10L, 100L, PARTITION_PATHS.get(0)),
                    record(schema, "k2", "k2_c1", 1000L, 100L, PARTITION_PATHS.get(1))), firstCommit);
            writeClient.commit(firstCommit, firstStatuses);

            String secondCommit = writeClient.startCommit();
            List<WriteStatus> secondStatuses = writeClient.insert(ImmutableList.of(
                    record(schema, "k3", "k3_c2", 20L, 200L, PARTITION_PATHS.get(0)),
                    record(schema, "k4", "k4_c2", 2000L, 200L, PARTITION_PATHS.get(1))), secondCommit);
            writeClient.commit(secondCommit, secondStatuses);

            String thirdCommit = writeClient.startCommit();
            List<WriteStatus> thirdStatuses = writeClient.upsert(ImmutableList.of(
                    record(schema, "k1", "k1_c3", 15L, 300L, PARTITION_PATHS.get(0))), thirdCommit);
            writeClient.commit(thirdCommit, thirdStatuses);
        }
    }

    private static HoodieJavaWriteClient<HoodieAvroPayload> createWriteClient(Schema schema, Path tablePath)
    {
        Configuration conf = new Configuration();
        try {
            HoodieTableMetaClient.newTableBuilder()
                    .setTableType(HoodieTableType.COPY_ON_WRITE)
                    .setTableName(TABLE_NAME)
                    .setTimelineLayoutVersion(1)
                    .setBootstrapIndexClass(NoOpBootstrapIndex.class.getName())
                    .setPayloadClassName(HoodieAvroPayload.class.getName())
                    .setRecordKeyFields(RECORD_KEY_FIELD)
                    .setPartitionFields(PARTITION_FIELD)
                    .setHiveStylePartitioningEnable(true)
                    .setOrderingFields(ORDERING_FIELD)
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
                .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                        .withInlineCompaction(false)
                        .withMaxNumDeltaCommitsBeforeCompaction(100)
                        .build())
                .withEmbeddedTimelineServerEnabled(false)
                .withMarkersType(MarkerType.DIRECT.name())
                // The whole point of this initializer: an ENABLED metadata table with stats indexes,
                // whose compaction never fires within this test (the zip fixtures use
                // compact.max.delta.commits=1), so its partitions keep native HFILE log deltas.
                // MDT HFILE writing is native (hudi-io HFileWriterImpl) -- no hbase involved.
                .withMetadataConfig(HoodieMetadataConfig.newBuilder()
                        .enable(true)
                        .withMetadataIndexColumnStats(true)
                        .withMetadataIndexPartitionStats(true)
                        .withMaxNumDeltaCommitsBeforeCompaction(100)
                        .build())
                .build();
        return new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(new HadoopStorageConfiguration(conf)), cfg);
    }

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, String key, String name, long price, long ts, String partitionPath)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put("name", name);
        record.put("price", price);
        record.put(ORDERING_FIELD, ts);
        // Keep the partition column in the data files like Spark-written tables do; Trino reads its
        // value from the metastore partition, not from parquet
        record.put(PARTITION_FIELD, partitionPath.substring(partitionPath.indexOf('=') + 1));
        HoodieKey hoodieKey = new HoodieKey(key, partitionPath);
        return new HoodieAvroRecord<>(hoodieKey, new HoodieAvroPayload(Option.of(record)), null);
    }

    private static Schema createAvroSchema()
    {
        List<Schema.Field> fields = ImmutableList.of(
                new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field("name", Schema.create(Schema.Type.STRING)),
                new Schema.Field("price", Schema.create(Schema.Type.LONG)),
                new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)),
                new Schema.Field(PARTITION_FIELD, Schema.create(Schema.Type.STRING)));
        return Schema.createRecord(TABLE_NAME, null, null, false, new ArrayList<>(fields));
    }

    private static Table createTableDefinition(String schemaName, Location location)
    {
        return Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(TABLE_NAME)
                .setTableType(EXTERNAL_TABLE.name())
                .setOwner(Optional.of("public"))
                .setDataColumns(DATA_COLUMNS)
                .setPartitionColumns(PARTITION_COLUMNS)
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(storageFormat())
                        .setLocation(location.toString()))
                .build();
    }

    private static List<PartitionWithStatistics> createPartitions(String schemaName, Location tableLocation)
    {
        List<PartitionWithStatistics> partitions = new ArrayList<>();
        for (String partitionName : PARTITION_PATHS) {
            // Hive-style partition paths, so the partition NAME and the relative PATH coincide
            Partition partition = Partition.builder()
                    .setDatabaseName(schemaName)
                    .setTableName(TABLE_NAME)
                    .setValues(extractPartitionValues(partitionName))
                    .withStorage(storageBuilder -> storageBuilder
                            .setStorageFormat(storageFormat())
                            .setLocation(tableLocation.appendPath(partitionName).toString()))
                    .setColumns(DATA_COLUMNS)
                    .build();
            partitions.add(new PartitionWithStatistics(partition, partitionName, PartitionStatistics.empty()));
        }
        return partitions;
    }

    private static StorageFormat storageFormat()
    {
        return StorageFormat.create(
                PARQUET_HIVE_SERDE_CLASS,
                HUDI_PARQUET_INPUT_FORMAT,
                MAPRED_PARQUET_OUTPUT_FORMAT_CLASS);
    }
}
