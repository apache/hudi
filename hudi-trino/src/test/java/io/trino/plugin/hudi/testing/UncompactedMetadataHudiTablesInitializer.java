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
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
 * {@code hoodie.metadata.compact.max.delta.commits} is set high and several commits are written,
 * leaving the MDT's {@code files}/{@code column_stats}/{@code partition_stats} partitions with
 * NATIVE HFILE log files that the connector must read at query time (issue apache/hudi#19279):
 * {@code *.log.hfile} deltas where the whole file is an HFile, as current writers produce. The zip
 * fixtures cannot cover this even where their MDTs are uncompacted (e.g. {@code hudi_trips_cow_v8}):
 * they predate the native-log write path, so their MDT deltas are {@code #HUDI#} block-format logs
 * carrying HFILE_DATA_BLOCKs, which the connector already read through its HFile content reader;
 * only whole-file native HFILE logs hit the previously unimplemented
 * {@code getFileFormatUtils(HFILE)} path.
 * <p>
 * Note: MDT writing here is fully native -- HFILE base files and log files are written via hudi-io's
 * pure-Java {@code HFileWriterImpl}, so no hbase dependency is involved (the "requires hbase" note in
 * older initializers is stale).
 * <p>
 * Data layout (partitions {@code part_col=p1} / {@code part_col=p2}, hive-style paths so the MDT
 * partition listing and the metastore agree on names):
 * <pre>
 * commit 1 (insert, MDT off): k1(p1, price 10,  ts 100), k2(p2, price 1000, ts 100)
 * commit 2 (insert, MDT on):  k3(p1, price 20,  ts 200), k4(p2, price 2000, ts 200)
 * commit 3 (upsert, MDT on):  k1(p1, price 15,  ts 300)
 * </pre>
 * The MDT is enabled only from commit 2 so its bootstrap sees existing data; a bootstrap over an
 * empty table would register the col-stats index definition with no source fields, permanently
 * disabling stats-based pruning in the connector (see {@code writeTable}).
 * Final rows: k1=15, k2=1000, k3=20, k4=2000. Partition p1 holds prices [15, 20] and p2 holds
 * [1000, 2000], so a predicate like {@code price < 100} lets the partition-stats index prune p2.
 * <p>
 * A second, identical table {@link #CORRUPTED_TABLE_NAME} is written whose MDT log files are
 * corrupted in place before upload: every MDT read of it throws, pinning the connector's
 * fallbacks (direct file listing in {@code HudiSnapshotDirectoryLister}, unpruned split
 * generation in {@code HudiBackgroundSplitLoader}) instead of only the clean-read path.
 */
public class UncompactedMetadataHudiTablesInitializer
        implements HudiTablesInitializer
{
    public static final String TABLE_NAME = "hudi_uncompacted_mdt_pt_cow";
    public static final String CORRUPTED_TABLE_NAME = "hudi_corrupted_mdt_pt_cow";

    private static final String RECORD_KEY_FIELD = "id";
    private static final String PARTITION_FIELD = "part_col";
    private static final String ORDERING_FIELD = "ts";
    private static final List<String> PARTITION_PATHS = ImmutableList.of(PARTITION_FIELD + "=p1", PARTITION_FIELD + "=p2");

    private static final List<Column> DATA_COLUMNS = ImmutableList.<Column>builder()
            .addAll(AbstractMergerHudiTablesInitializer.HUDI_META_COLUMNS)
            .add(new Column(RECORD_KEY_FIELD, HIVE_STRING, Optional.empty(), Map.of()))
            .add(new Column("name", HIVE_STRING, Optional.empty(), Map.of()))
            .add(new Column("price", HIVE_LONG, Optional.empty(), Map.of()))
            .add(new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()))
            .build();

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

        java.nio.file.Path tempDir = createTempDirectory("uncompacted-mdt");
        try {
            for (String tableName : ImmutableList.of(TABLE_NAME, CORRUPTED_TABLE_NAME)) {
                java.nio.file.Path tempTableDir = tempDir.resolve(tableName);
                writeTable(new Path(tempTableDir.toUri()), tableName);
                if (tableName.equals(CORRUPTED_TABLE_NAME)) {
                    corruptMetadataLogFiles(tempTableDir);
                    verifyMetadataTableUnreadable(new Path(tempTableDir.toUri()));
                }
                Location tableLocation = externalLocation.appendPath(tableName);
                ResourceHudiTablesInitializer.copyDir(tempTableDir, fileSystem, tableLocation);

                metastore.createTable(createTableDefinition(schemaName, tableName, tableLocation), PrincipalPrivileges.NO_PRIVILEGES);
                metastore.addPartitions(schemaName, tableName, createPartitions(schemaName, tableName, tableLocation));
            }
        }
        finally {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }

    /**
     * Corrupts the MDT log files so that any metadata-table read of the table throws. The deltas
     * here are NATIVE HFILE log files (the whole file is an HFile): hudi-io HFiles end with a
     * fixed 4096-byte trailer whose magic and protobuf fields sit at the trailer's START, followed
     * by padding, so the corrupted window is placed ~4KB before EOF to land on those fields --
     * flipping bytes near EOF would only hit padding and reads would still succeed.
     * {@link #verifyMetadataTableUnreadable} then proves the corruption took. Every MDT partition
     * directory must end up with at least one corrupted log file, so that none of the read paths
     * (files listing, col-stats skipping, partition-stats pruning) can see a clean partition.
     */
    private static void corruptMetadataLogFiles(java.nio.file.Path tableDir)
            throws IOException
    {
        java.nio.file.Path metadataDir = tableDir.resolve(".hoodie").resolve("metadata");
        List<java.nio.file.Path> logFiles;
        try (Stream<java.nio.file.Path> walk = Files.walk(metadataDir)) {
            logFiles = walk
                    .filter(Files::isRegularFile)
                    .filter(file -> file.getFileName().toString().contains(".log."))
                    // Only files directly inside an MDT partition directory, not timeline or
                    // marker leftovers under the MDT's own .hoodie
                    .filter(file -> metadataDir.equals(file.getParent().getParent()))
                    .collect(toImmutableList());
        }
        Set<java.nio.file.Path> partitionsWithLogFiles = new HashSet<>();
        Set<java.nio.file.Path> partitionsCorrupted = new HashSet<>();
        for (java.nio.file.Path logFile : logFiles) {
            partitionsWithLogFiles.add(logFile.getParent());
            byte[] bytes = Files.readAllBytes(logFile);
            int start = bytes.length - 4160;
            int end = bytes.length - 3648;
            if (start < 64) {
                // Too small to even hold an HFile trailer: a record-less file-group bootstrap
                // marker that no read decodes. The per-partition check below still requires a
                // corrupted data-bearing file next to it.
                continue;
            }
            for (int i = start; i < end; i++) {
                bytes[i] ^= 0x5A;
            }
            Files.write(logFile, bytes);
            partitionsCorrupted.add(logFile.getParent());
        }
        checkState(!partitionsWithLogFiles.isEmpty(), "No MDT log files found under %s", metadataDir);
        Set<java.nio.file.Path> untouched = new HashSet<>(partitionsWithLogFiles);
        untouched.removeAll(partitionsCorrupted);
        checkState(untouched.isEmpty(),
                "No MDT log file corrupted in partition(s) %s; reads of those partitions would still succeed and their fallback tests would pass vacuously", untouched);
    }

    /**
     * Proves the corruption is effective, not just that bytes were flipped: a metadata-table read
     * of the corrupted table must throw. This guards the fallback tests against the fixed trailer
     * offsets in {@link #corruptMetadataLogFiles} ever missing (e.g. after an HFile layout change),
     * in which case those tests would pass vacuously against a clean MDT read.
     */
    private static void verifyMetadataTableUnreadable(Path tablePath)
            throws Exception
    {
        HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(new Configuration());
        List<String> partitions;
        try (HoodieTableMetadata metadata = new HoodieBackedTableMetadata(
                new HoodieJavaEngineContext(storageConf),
                HoodieStorageUtils.getStorage(tablePath.toString(), storageConf),
                HoodieMetadataConfig.newBuilder().enable(true).build(),
                tablePath.toString())) {
            // The files partition backs getAllPartitionPaths; its log delta is corrupted like all others
            partitions = metadata.getAllPartitionPaths();
        }
        catch (HoodieMetadataException expected) {
            // Only the wrapper BaseTableMetadata puts around a genuine read failure counts: the
            // bare IllegalArgumentException it throws for a never-initialized MDT must NOT pass,
            // or this guard would go quiet if the fixture's MDT bootstrap ever stopped happening
            return;
        }
        throw new IllegalStateException(
                "Metadata table of " + CORRUPTED_TABLE_NAME + " is still readable (partitions: " + partitions
                        + "); corruptMetadataLogFiles no longer lands on the HFile trailer fields");
    }

    private static void writeTable(Path tablePath, String tableName)
    {
        Schema schema = createAvroSchema();
        initTable(tablePath, tableName);

        // Commit 1 runs with the MDT off so the MDT bootstraps AFTER data exists. Index
        // definitions get their source fields from ColumnStatsIndexer.postInitialization, which
        // registers an EMPTY field list when the bootstrap sees no records, and
        // HoodieJavaWriteClient.updateColumnsToIndexWithColStats is a no-op (HUDI-8801; the
        // Spark client refreshes the definition on each commit), so a definition registered over
        // an empty table keeps empty source fields forever and the connector's canApply() then
        // rejects the col-stats/partition-stats indexes, silently disabling pruning. Once
        // HUDI-8801 fixes the Java client, this two-client split can collapse back to one.
        try (HoodieJavaWriteClient<HoodieAvroPayload> writeClient = createWriteClient(schema, tablePath, tableName, false)) {
            String firstCommit = writeClient.startCommit();
            List<WriteStatus> firstStatuses = writeClient.insert(ImmutableList.of(
                    record(schema, "k1", "k1_c1", 10L, 100L, PARTITION_PATHS.get(0)),
                    record(schema, "k2", "k2_c1", 1000L, 100L, PARTITION_PATHS.get(1))), firstCommit);
            writeClient.commit(firstCommit, firstStatuses);
        }

        try (HoodieJavaWriteClient<HoodieAvroPayload> writeClient = createWriteClient(schema, tablePath, tableName, true)) {
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

    private static void initTable(Path tablePath, String tableName)
    {
        try {
            HoodieTableMetaClient.newTableBuilder()
                    .setTableType(HoodieTableType.COPY_ON_WRITE)
                    .setTableName(tableName)
                    .setTimelineLayoutVersion(1)
                    .setBootstrapIndexClass(NoOpBootstrapIndex.class.getName())
                    .setPayloadClassName(HoodieAvroPayload.class.getName())
                    .setRecordKeyFields(RECORD_KEY_FIELD)
                    .setPartitionFields(PARTITION_FIELD)
                    .setHiveStylePartitioningEnable(true)
                    .setOrderingFields(ORDERING_FIELD)
                    .initTable(new HadoopStorageConfiguration(new Configuration()), tablePath.toString());
        }
        catch (IOException e) {
            throw new RuntimeException("Could not init table " + tableName, e);
        }
    }

    private static HoodieJavaWriteClient<HoodieAvroPayload> createWriteClient(Schema schema, Path tablePath, String tableName, boolean metadataEnabled)
    {
        Configuration conf = new Configuration();
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
                .withPath(tablePath.toString())
                .withSchema(schema.toString())
                .withParallelism(2, 2)
                .withDeleteParallelism(2)
                .forTable(tableName)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                        .withInlineCompaction(false)
                        .withMaxNumDeltaCommitsBeforeCompaction(100)
                        .build())
                .withEmbeddedTimelineServerEnabled(false)
                .withMarkersType(MarkerType.DIRECT.name())
                // The whole point of this initializer: an ENABLED metadata table with stats
                // indexes whose compaction never fires within this test, so its partitions keep
                // native HFILE log deltas. MDT HFILE writing is native (hudi-io HFileWriterImpl)
                // -- no hbase involved. metadataEnabled is false for the first commit only; see
                // writeTable.
                .withMetadataConfig(HoodieMetadataConfig.newBuilder()
                        .enable(metadataEnabled)
                        .withMetadataIndexColumnStats(metadataEnabled)
                        .withMetadataIndexPartitionStats(metadataEnabled)
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

    private static Table createTableDefinition(String schemaName, String tableName, Location location)
    {
        return Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
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

    private static List<PartitionWithStatistics> createPartitions(String schemaName, String tableName, Location tableLocation)
    {
        List<PartitionWithStatistics> partitions = new ArrayList<>();
        for (String partitionName : PARTITION_PATHS) {
            // Hive-style partition paths, so the partition NAME and the relative PATH coincide
            Partition partition = Partition.builder()
                    .setDatabaseName(schemaName)
                    .setTableName(tableName)
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
