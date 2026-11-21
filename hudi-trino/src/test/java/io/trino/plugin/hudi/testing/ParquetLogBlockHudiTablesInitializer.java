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
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.metastore.HiveType.HIVE_BOOLEAN;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;

/**
 * Creates a non-partitioned Merge-On-Read table at TABLE VERSION 9 whose log files hold PARQUET data blocks
 * ({@code hoodie.logfile.data.block.format=parquet}), the layout a Spark record merger writes. The file-group reader
 * hands such a block to the connector as an {@code inlinefs://} path into the log file. From table version 10 on,
 * delta commits write native parquet log files instead, so the version is pinned.
 * <p>
 * A base commit is followed by two log commits, written one record per block so a log file holds several parquet
 * blocks, and a hard-delete commit. Merging is event-time ordered, so the log records carry both winning and losing updates
 * and a soft delete. The fixture fails if the log files carry anything but parquet data and delete blocks, so the
 * read tests cannot pass on Avro blocks. See {@code TestHudiParquetLogBlocks}.
 */
public class ParquetLogBlockHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "parquet_log_blocks_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";

    private static final HoodieTableVersion TABLE_VERSION = HoodieTableVersion.NINE;

    public ParquetLogBlockHudiTablesInitializer()
    {
        super(TABLE_NAME);
    }

    @Override
    protected List<Column> dataColumns()
    {
        return ImmutableList.of(
                new Column(RECORD_KEY_FIELD, HIVE_STRING, Optional.empty(), Map.of()),
                new Column("name", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("value", HIVE_LONG, Optional.empty(), Map.of()),
                new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()),
                new Column(HOODIE_IS_DELETED_FIELD, HIVE_BOOLEAN, Optional.empty(), Map.of()));
    }

    @Override
    protected Schema avroSchema()
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
        return Schema.createRecord(TABLE_NAME, null, null, false, new ArrayList<>(fields));
    }

    @Override
    protected void configureTableConfig(HoodieTableMetaClient.TableBuilder tableBuilder)
    {
        tableBuilder
                .setTableVersion(TABLE_VERSION)
                .setRecordMergeMode(RecordMergeMode.EVENT_TIME_ORDERING);
    }

    @Override
    protected void configureWriteConfig(HoodieWriteConfig.Builder writeConfigBuilder)
    {
        writeConfigBuilder
                .withWriteTableVersion(TABLE_VERSION.versionCode())
                // Without this the write client silently upgrades the table to the current version on the first commit.
                .withAutoUpgradeVersion(false)
                .withRecordMergeMode(RecordMergeMode.EVENT_TIME_ORDERING)
                // A one-byte block limit makes the writer flush a block per record, so a log file holds several
                // parquet blocks and the later ones sit past the earlier ones' bytes
                .withStorageConfig(HoodieStorageConfig.newBuilder()
                        .logFileDataBlockFormat("parquet")
                        .logFileDataBlockMaxSize(1)
                        .build());
    }

    @Override
    protected void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
            throws IOException
    {
        Schema schema = avroSchema();
        String baseCommit = client.startCommit();
        List<WriteStatus> baseStatuses = client.bulkInsert(ImmutableList.of(
                record(schema, "k1", "k1_base", 10L, 100L, false),
                record(schema, "k2", "k2_base", 20L, 100L, false),
                record(schema, "k3", "k3_base", 30L, 100L, false),
                record(schema, "k4", "k4_base", 40L, 100L, false),
                record(schema, "k5", "k5_base", 50L, 100L, false),
                record(schema, "k6", "k6_base", 60L, 100L, false)), baseCommit);
        client.commit(baseCommit, baseStatuses);

        //  - k1: newer update (ts 200), superseded by the next commit
        //  - k2: soft delete with a newer ts -> deleted
        //  - k3: OBSOLETE update (ts 50) -> the base row survives
        String firstLogCommit = client.startCommit();
        List<WriteStatus> firstLogStatuses = client.upsert(ImmutableList.of(
                record(schema, "k1", "k1_first", 11L, 200L, false),
                record(schema, "k2", "k2_deleted", 22L, 200L, true),
                record(schema, "k3", "k3_obsolete", 33L, 50L, false)), firstLogCommit);
        client.commit(firstLogCommit, firstLogStatuses);

        //  - k1: newer update again (ts 300) -> wins over both the base row and the first log block
        //  - k4: newer update (ts 150) -> wins
        String secondLogCommit = client.startCommit();
        List<WriteStatus> secondLogStatuses = client.upsert(ImmutableList.of(
                record(schema, "k1", "k1_second", 111L, 300L, false),
                record(schema, "k4", "k4_updated", 44L, 150L, false)), secondLogCommit);
        client.commit(secondLogCommit, secondLogStatuses);

        // k5: hard delete (a delete block in the log)
        String deleteCommit = client.startCommit();
        List<WriteStatus> deleteStatuses = client.delete(ImmutableList.of(hoodieKey("k5")), deleteCommit);
        client.commit(deleteCommit, deleteStatuses);

        checkLogBlockTypes();
    }

    private void checkLogBlockTypes()
            throws IOException
    {
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                .setConf(new HadoopStorageConfiguration(new Configuration()))
                .setBasePath(stagingTableDirectory().toUri().toString())
                .build();
        Set<HoodieLogBlockType> blockTypes = EnumSet.noneOf(HoodieLogBlockType.class);
        int maxParquetBlocksPerFile = 0;
        List<Path> logFiles;
        try (Stream<Path> files = Files.list(stagingTableDirectory())) {
            logFiles = files.filter(file -> FSUtils.isLogFile(new StoragePath(file.toUri()))).toList();
        }
        for (Path logFile : logFiles) {
            int parquetBlocks = 0;
            try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(metaClient, new HoodieLogFile(new StoragePath(logFile.toUri())), null)) {
                while (reader.hasNext()) {
                    HoodieLogBlockType blockType = reader.next().getBlockType();
                    blockTypes.add(blockType);
                    if (blockType == HoodieLogBlockType.PARQUET_DATA_BLOCK) {
                        parquetBlocks++;
                    }
                }
            }
            maxParquetBlocksPerFile = Math.max(maxParquetBlocksPerFile, parquetBlocks);
        }
        checkState(blockTypes.equals(EnumSet.of(HoodieLogBlockType.PARQUET_DATA_BLOCK, HoodieLogBlockType.DELETE_BLOCK)),
                "Expected parquet data blocks and a delete block in the log files %s, found %s", logFiles, blockTypes);
        checkState(maxParquetBlocksPerFile > 1, "Expected a log file with several parquet blocks in %s", logFiles);
    }

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, String key, String name, long value, long ts, boolean deleted)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put("name", name);
        record.put("value", value);
        record.put(ORDERING_FIELD, ts);
        record.put(HOODIE_IS_DELETED_FIELD, deleted);
        return avroRecord(record, key);
    }
}
