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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_REALTIME_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.metastore.HiveType.HIVE_BOOLEAN;
import static io.trino.metastore.HiveType.HIVE_DOUBLE;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.requireNonNull;

/**
 * Creates a non-partitioned Merge-On-Read table configured with the {@link MaxRankRecordMerger} custom merger
 * ({@link RecordMergeMode#CUSTOM}) and drives a controlled sequence of commits so an end-to-end test can read the
 * table through Trino after every commit and validate the merged result.
 * <p>
 * The schema has 30 data columns (record key, the {@code merge_rank} decision column, an ordering field, and 27
 * additional columns spanning string/long/int/double/boolean types). {@link #NUM_RECORDS} record keys are
 * bulk-inserted in the first commit; each subsequent commit upserts roughly two thirds of the keys with freshly
 * derived values. Inline compaction is disabled so every delta commit survives as a log file and must be merged at
 * read time.
 * <p>
 * Every record column is a pure deterministic function of {@code (recordIndex, commitNumber)} (see
 * {@link #valueFor}). Because the merge keeps the record with the maximum {@code merge_rank} (ties to the newer
 * commit), the winning commit for each key is known in closed form, so the full expected row for the real-time
 * table is reproduced exactly by {@link #expectedRows()} without reading anything back.
 * <p>
 * Two metastore tables are registered: a read-optimized table (base files only) and a real-time table (suffix
 * {@code _rt}) that merges base + log files through the file group reader. The Hudi write client writes to a local
 * staging directory; after each commit the staging directory is mirrored into the Trino filesystem location the
 * connector reads from (see {@link #syncToTrino()}).
 */
public class IncrementalCustomMergerHudiTablesInitializer
        implements HudiTablesInitializer
{
    public static final String TABLE_NAME = "custom_merger_e2e";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";

    public static final int TOTAL_COMMITS = 20;
    public static final int NUM_RECORDS = 10_000;

    private static final String RECORD_KEY_FIELD = "key";
    private static final String RANK_FIELD = MaxRankRecordMerger.RANK_COLUMN;
    private static final String ORDERING_FIELD = "ts";
    private static final String PARTITION_PATH = "";

    private enum Kind
    {
        STRING, LONG, INT, DOUBLE, BOOLEAN
    }

    private record ColumnSpec(String name, Kind kind) {}

    /** The 30 data columns, in the order they appear in the schema and in query projections. */
    private static final List<ColumnSpec> COLUMN_SPECS = buildColumnSpecs();

    private static final List<Column> DATA_COLUMNS = buildDataColumns();
    private static final Schema AVRO_SCHEMA = buildAvroSchema();

    // Mutable state driven across commits.
    private TrinoFileSystem fileSystem;
    private Location tableLocation;
    private java.nio.file.Path stagingDir;
    private Path stagingTablePath;
    private HoodieJavaWriteClient<HoodieAvroPayload> writeClient;

    /** Winning commit per record index under the max-rank merge policy (-1 = not yet inserted). */
    private final int[] winningCommit = new int[NUM_RECORDS];
    /** Most recent commit that touched each record index (used to measure divergence from newest-wins). */
    private final int[] latestCommit = new int[NUM_RECORDS];
    private int currentCommit;

    @Override
    public void initializeTables(QueryRunner queryRunner, Location externalLocation, String schemaName)
            throws Exception
    {
        fileSystem = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
        HiveMetastore metastore = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        tableLocation = externalLocation.appendPath(TABLE_NAME);
        stagingDir = createTempDirectory("custom-merger-e2e");
        stagingTablePath = new Path(stagingDir.resolve(TABLE_NAME).toUri());

        java.util.Arrays.fill(winningCommit, -1);
        java.util.Arrays.fill(latestCommit, -1);

        initTable();
        writeClient = createWriteClient();

        // First commit: bulk insert all keys (produces the base parquet files the read-optimized table reads).
        currentCommit = 1;
        String firstCommit = writeClient.startCommit();
        List<WriteStatus> statuses = writeClient.bulkInsert(buildRecords(currentCommit), firstCommit);
        writeClient.commit(firstCommit, statuses);
        recordCommit(currentCommit);
        syncToTrino();

        metastore.createTable(createTableDefinition(schemaName, TABLE_NAME, tableLocation, false), PrincipalPrivileges.NO_PRIVILEGES);
        metastore.createTable(createTableDefinition(schemaName, RT_TABLE_NAME, tableLocation, true), PrincipalPrivileges.NO_PRIVILEGES);
    }

    /**
     * Writes the next delta commit (upsert of ~2/3 of the keys) and mirrors it into the Trino filesystem so the
     * connector observes the new commit on the next query. Must be called after {@link #initializeTables}.
     */
    public void writeAndSyncNextCommit()
    {
        currentCommit++;
        String commitTime = writeClient.startCommit();
        List<WriteStatus> statuses = writeClient.upsert(buildRecords(currentCommit), commitTime);
        writeClient.commit(commitTime, statuses);
        recordCommit(currentCommit);
        syncToTrino();
    }

    /** Ordered data column names (record key first), matching {@link #expectedRows()} value positions. */
    public List<String> dataColumnNames()
    {
        return COLUMN_SPECS.stream().map(ColumnSpec::name).collect(toImmutableList());
    }

    /** Expected real-time (merged) rows after the commits written so far: key -> values in column order. */
    public Map<String, Object[]> expectedRows()
    {
        Map<String, Object[]> rows = new LinkedHashMap<>();
        for (int ki = 0; ki < NUM_RECORDS; ki++) {
            if (winningCommit[ki] < 0) {
                continue;
            }
            Object[] row = rowFor(ki, winningCommit[ki]);
            rows.put((String) row[0], row);
        }
        return rows;
    }

    /** Expected read-optimized (base-file-only) rows: always the first-commit values for every key. */
    public Map<String, Object[]> baseRows()
    {
        Map<String, Object[]> rows = new LinkedHashMap<>();
        for (int ki = 0; ki < NUM_RECORDS; ki++) {
            Object[] row = rowFor(ki, 1);
            rows.put((String) row[0], row);
        }
        return rows;
    }

    /** Number of keys whose merged winner is not their most recently committed version (custom != newest-wins). */
    public int divergentKeyCount()
    {
        int count = 0;
        for (int ki = 0; ki < NUM_RECORDS; ki++) {
            if (winningCommit[ki] >= 0 && winningCommit[ki] != latestCommit[ki]) {
                count++;
            }
        }
        return count;
    }

    public void close()
            throws IOException
    {
        if (writeClient != null) {
            writeClient.close();
            writeClient = null;
        }
        if (stagingDir != null) {
            deleteRecursively(stagingDir, ALLOW_INSECURE);
            stagingDir = null;
        }
    }

    private void recordCommit(int commit)
    {
        for (int ki = 0; ki < NUM_RECORDS; ki++) {
            if (!isUpdated(ki, commit)) {
                continue;
            }
            latestCommit[ki] = commit;
            int prev = winningCommit[ki];
            // Mirror MaxRankRecordMerger: the incoming (newer) record wins on a tie or a strictly larger rank.
            if (prev < 0 || mergeRank(ki, commit) >= mergeRank(ki, prev)) {
                winningCommit[ki] = commit;
            }
        }
    }

    private List<HoodieRecord<HoodieAvroPayload>> buildRecords(int commit)
    {
        List<HoodieRecord<HoodieAvroPayload>> records = new ArrayList<>();
        for (int ki = 0; ki < NUM_RECORDS; ki++) {
            if (isUpdated(ki, commit)) {
                records.add(record(ki, commit));
            }
        }
        return records;
    }

    private static boolean isUpdated(int recordIndex, int commit)
    {
        // The first commit inserts every key; later commits upsert ~2/3 of the keys, rotating which third is skipped.
        return commit == 1 || Math.floorMod(recordIndex + commit, 3) != 0;
    }

    private static HoodieRecord<HoodieAvroPayload> record(int recordIndex, int commit)
    {
        GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
        for (int i = 0; i < COLUMN_SPECS.size(); i++) {
            record.put(COLUMN_SPECS.get(i).name(), valueFor(i, recordIndex, commit));
        }
        HoodieKey hoodieKey = new HoodieKey(key(recordIndex), PARTITION_PATH);
        return new HoodieAvroRecord<>(hoodieKey, new HoodieAvroPayload(Option.of(record)), null);
    }

    private static Object[] rowFor(int recordIndex, int commit)
    {
        Object[] row = new Object[COLUMN_SPECS.size()];
        for (int i = 0; i < row.length; i++) {
            row[i] = valueFor(i, recordIndex, commit);
        }
        return row;
    }

    /**
     * The single source of truth for every column value, used both when writing records and when reproducing the
     * expected rows. Returns boxed types matching how Trino surfaces the column (String/Long/Integer/Double/Boolean).
     */
    private static Object valueFor(int columnIndex, int recordIndex, int commit)
    {
        ColumnSpec spec = COLUMN_SPECS.get(columnIndex);
        if (spec.name().equals(RECORD_KEY_FIELD)) {
            return key(recordIndex);
        }
        if (spec.name().equals(RANK_FIELD)) {
            return mergeRank(recordIndex, commit);
        }
        if (spec.name().equals(ORDERING_FIELD)) {
            return (long) commit;
        }
        return switch (spec.kind()) {
            case STRING -> "v_" + columnIndex + "_" + recordIndex + "_" + commit;
            case LONG -> recordIndex * 1_000_003L + (long) commit * columnIndex;
            case INT -> (int) Math.floorMod(recordIndex * 31L + (long) commit * columnIndex, 1_000_000L);
            case DOUBLE -> recordIndex + commit * 0.5 + columnIndex * 0.125;
            case BOOLEAN -> (recordIndex + commit + columnIndex) % 2 == 0;
        };
    }

    private static String key(int recordIndex)
    {
        return format("key%05d", recordIndex);
    }

    /** Deterministic, non-monotonic rank in [0, 100000) so the winning commit is rarely the latest one. */
    private static long mergeRank(int recordIndex, int commit)
    {
        return Math.floorMod(recordIndex * 2_654_435_761L + commit * 40_503L, 100_000L);
    }

    private void syncToTrino()
    {
        try {
            if (fileSystem.directoryExists(tableLocation).orElse(false)) {
                fileSystem.deleteDirectory(tableLocation);
            }
            ResourceHudiTablesInitializer.copyDir(stagingDir.resolve(TABLE_NAME), fileSystem, tableLocation);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to sync staged Hudi table to Trino filesystem", e);
        }
    }

    private void initTable()
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
                    .setRecordMergeStrategyId(MaxRankRecordMerger.MERGE_STRATEGY_ID)
                    .initTable(new HadoopStorageConfiguration(conf), stagingTablePath.toString());
        }
        catch (IOException e) {
            throw new RuntimeException("Could not init table " + TABLE_NAME, e);
        }
    }

    private HoodieJavaWriteClient<HoodieAvroPayload> createWriteClient()
    {
        Configuration conf = new Configuration();
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
                .withPath(stagingTablePath.toString())
                .withSchema(AVRO_SCHEMA.toString())
                .withParallelism(2, 2)
                .withDeleteParallelism(2)
                .forTable(TABLE_NAME)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                .withRecordMergeMode(RecordMergeMode.CUSTOM)
                .withRecordMergeStrategyId(MaxRankRecordMerger.MERGE_STRATEGY_ID)
                .withRecordMergeImplClasses(MaxRankRecordMerger.class.getName())
                // Keep log files around so the custom merger runs at read time for every delta commit.
                .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                        .withInlineCompaction(false)
                        .withMaxNumDeltaCommitsBeforeCompaction(TOTAL_COMMITS + 100)
                        .build())
                .withEmbeddedTimelineServerEnabled(false)
                .withMarkersType(MarkerType.DIRECT.name())
                // MDT writes require hbase deps not present in the Trino runtime; disable as other initializers do.
                .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
                .build();
        return new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(new HadoopStorageConfiguration(conf)), cfg);
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

    private static List<ColumnSpec> buildColumnSpecs()
    {
        ImmutableList.Builder<ColumnSpec> specs = ImmutableList.builder();
        specs.add(new ColumnSpec(RECORD_KEY_FIELD, Kind.STRING));
        specs.add(new ColumnSpec(RANK_FIELD, Kind.LONG));
        specs.add(new ColumnSpec(ORDERING_FIELD, Kind.LONG));
        for (int i = 0; i < 7; i++) {
            specs.add(new ColumnSpec("s" + i, Kind.STRING));
        }
        for (int i = 0; i < 7; i++) {
            specs.add(new ColumnSpec("l" + i, Kind.LONG));
        }
        for (int i = 0; i < 6; i++) {
            specs.add(new ColumnSpec("i" + i, Kind.INT));
        }
        for (int i = 0; i < 5; i++) {
            specs.add(new ColumnSpec("d" + i, Kind.DOUBLE));
        }
        for (int i = 0; i < 2; i++) {
            specs.add(new ColumnSpec("b" + i, Kind.BOOLEAN));
        }
        List<ColumnSpec> built = specs.build();
        requireNonNull(built);
        if (built.size() != 30) {
            throw new IllegalStateException("Expected 30 data columns but built " + built.size());
        }
        return built;
    }

    private static List<Column> buildDataColumns()
    {
        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        // Hudi metadata columns prepended to every table.
        for (String meta : List.of("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name")) {
            columns.add(new Column(meta, HIVE_STRING, Optional.empty(), Map.of()));
        }
        for (ColumnSpec spec : COLUMN_SPECS) {
            columns.add(new Column(spec.name(), hiveType(spec.kind()), Optional.empty(), Map.of()));
        }
        return columns.build();
    }

    private static Schema buildAvroSchema()
    {
        List<Schema.Field> fields = new ArrayList<>();
        for (ColumnSpec spec : COLUMN_SPECS) {
            fields.add(new Schema.Field(spec.name(), Schema.create(avroType(spec.kind()))));
        }
        return Schema.createRecord(TABLE_NAME, null, null, false, fields);
    }

    private static HiveType hiveType(Kind kind)
    {
        return switch (kind) {
            case STRING -> HIVE_STRING;
            case LONG -> HIVE_LONG;
            case INT -> HIVE_INT;
            case DOUBLE -> HIVE_DOUBLE;
            case BOOLEAN -> HIVE_BOOLEAN;
        };
    }

    private static Schema.Type avroType(Kind kind)
    {
        return switch (kind) {
            case STRING -> Schema.Type.STRING;
            case LONG -> Schema.Type.LONG;
            case INT -> Schema.Type.INT;
            case DOUBLE -> Schema.Type.DOUBLE;
            case BOOLEAN -> Schema.Type.BOOLEAN;
        };
    }
}
