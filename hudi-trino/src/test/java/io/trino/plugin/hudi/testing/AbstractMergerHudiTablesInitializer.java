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
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_REALTIME_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.requireNonNull;

/**
 * Shared machinery for the non-partitioned Merge-On-Read fixtures that exist to exercise record merging at
 * read time. The table is written by the Hudi Java write client into a local staging directory and then
 * mirrored into the Trino filesystem the connector reads from; two metastore tables are registered against
 * that location, a read-optimized one (base files only) and a real-time one (suffix {@code _rt}) that merges
 * base + log files through the file group reader.
 * <p>
 * Inline compaction is off for every fixture so the log files written by the delta commits survive and must
 * be merged at read time, and the metadata table is off because MDT writes need hbase dependencies that are
 * not on the Trino test classpath.
 * <p>
 * Subclasses supply the schema, the merge-related table and write configuration, and the commits; everything
 * a fixture does not vary lives here.
 */
public abstract class AbstractMergerHudiTablesInitializer
        implements HudiTablesInitializer
{
    /** Every fixture built on this base is keyed on {@code key} and ordered by {@code ts}, in a single unnamed partition. */
    protected static final String RECORD_KEY_FIELD = "key";
    protected static final String ORDERING_FIELD = "ts";

    private static final String PARTITION_PATH = "";

    /** Hudi metadata columns, prepended to every table's data columns in the metastore definition. */
    private static final List<Column> HUDI_META_COLUMNS = ImmutableList.of(
            new Column("_hoodie_commit_time", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_commit_seqno", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_record_key", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_partition_path", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_file_name", HIVE_STRING, Optional.empty(), Map.of()));

    private final String tableName;

    private TrinoFileSystem fileSystem;
    private Location tableLocation;
    private java.nio.file.Path stagingDir;
    private Path stagingTablePath;
    private HoodieJavaWriteClient<HoodieAvroPayload> writeClient;

    protected AbstractMergerHudiTablesInitializer(String tableName)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @Override
    public final void initializeTables(QueryRunner queryRunner, Location externalLocation, String schemaName)
            throws Exception
    {
        fileSystem = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
        HiveMetastore metastore = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        tableLocation = externalLocation.appendPath(tableName);
        stagingDir = createTempDirectory(tableName.replace('_', '-'));
        stagingTablePath = new Path(stagingDir.resolve(tableName).toUri());

        boolean initialized = false;
        try {
            initTable();
            afterTableInit();
            writeClient = createWriteClient();
            writeInitialCommits(writeClient);
            syncToTrino();

            metastore.createTable(createTableDefinition(schemaName, tableName, false), PrincipalPrivileges.NO_PRIVILEGES);
            metastore.createTable(createTableDefinition(schemaName, tableName + "_rt", true), PrincipalPrivileges.NO_PRIVILEGES);
            initialized = true;
        }
        finally {
            // Only fixtures that keep writing commits after initialization need the staging directory and the
            // write client to outlive this call; they are responsible for calling close().
            if (!initialized || !keepsWriterOpen()) {
                close();
            }
        }
    }

    /** The table's data columns, in schema order; the Hudi metadata columns are prepended by this class. */
    protected abstract List<Column> dataColumns();

    /** The Avro schema the write client writes, matching {@link #dataColumns()}. */
    protected abstract Schema avroSchema();

    /**
     * Applies the fixture's merge-related table configuration (merge mode, merge strategy id, payload class).
     * The table type, record key fields and ordering fields are set by this class.
     */
    protected abstract void configureTableConfig(HoodieTableMetaClient.TableBuilder tableBuilder);

    /** Applies the fixture's merge-related write configuration (merge mode, merge strategy id, merger impl classes). */
    protected abstract void configureWriteConfig(HoodieWriteConfig.Builder writeConfigBuilder);

    /** Writes the commits that make up the fixture's initial state. */
    protected abstract void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
            throws IOException;

    /** Runs on the freshly created table, before the write client exists and before any data is written. */
    protected void afterTableInit()
            throws IOException {}

    /** Inline compaction is disabled, so this only has to stay above the number of delta commits a fixture writes. */
    protected int maxDeltaCommitsBeforeCompaction()
    {
        return 100;
    }

    /** Whether the staging directory and write client survive {@link #initializeTables}; such fixtures must call {@link #close()}. */
    protected boolean keepsWriterOpen()
    {
        return false;
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

    /** Local directory the write client writes into, before {@link #syncToTrino()} mirrors it to the connector. */
    protected java.nio.file.Path stagingTableDirectory()
    {
        return stagingDir.resolve(tableName);
    }

    protected HoodieJavaWriteClient<HoodieAvroPayload> writeClient()
    {
        return writeClient;
    }

    protected static HoodieRecord<HoodieAvroPayload> avroRecord(GenericRecord record, String key)
    {
        return new HoodieAvroRecord<>(hoodieKey(key), new HoodieAvroPayload(Option.of(record)), null);
    }

    /** Addresses a record in the single unnamed partition, e.g. for hard deletes via {@code writeClient.delete}. */
    protected static HoodieKey hoodieKey(String key)
    {
        return new HoodieKey(key, PARTITION_PATH);
    }

    /** Mirrors the staged table into the Trino filesystem so the connector observes the commits written so far. */
    protected void syncToTrino()
    {
        try {
            if (fileSystem.directoryExists(tableLocation).orElse(false)) {
                fileSystem.deleteDirectory(tableLocation);
            }
            ResourceHudiTablesInitializer.copyDir(stagingTableDirectory(), fileSystem, tableLocation);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to sync staged Hudi table to Trino filesystem", e);
        }
    }

    private void initTable()
    {
        HoodieTableMetaClient.TableBuilder tableBuilder = HoodieTableMetaClient.newTableBuilder()
                .setTableType(HoodieTableType.MERGE_ON_READ)
                .setTableName(tableName)
                .setTimelineLayoutVersion(1)
                .setBootstrapIndexClass(NoOpBootstrapIndex.class.getName())
                .setRecordKeyFields(RECORD_KEY_FIELD)
                .setOrderingFields(ORDERING_FIELD);
        configureTableConfig(tableBuilder);
        try {
            tableBuilder.initTable(new HadoopStorageConfiguration(new Configuration()), stagingTablePath.toString());
        }
        catch (IOException e) {
            throw new RuntimeException("Could not init table " + tableName, e);
        }
    }

    private HoodieJavaWriteClient<HoodieAvroPayload> createWriteClient()
    {
        Configuration conf = new Configuration();
        HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder()
                .withPath(stagingTablePath.toString())
                .withSchema(avroSchema().toString())
                .withParallelism(2, 2)
                .withDeleteParallelism(2)
                .forTable(tableName)
                // No withPreCombineField here: the ordering field is carried by the table config
                // (setOrderingFields), and the deprecated builder method fails the -Werror compile gate.
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                // Keep log files around so merging runs at read time.
                .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                        .withInlineCompaction(false)
                        .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommitsBeforeCompaction())
                        .build())
                .withEmbeddedTimelineServerEnabled(false)
                .withMarkersType(MarkerType.DIRECT.name())
                // MDT writes require hbase deps not present in the Trino runtime.
                .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build());
        configureWriteConfig(writeConfigBuilder);
        return new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(new HadoopStorageConfiguration(conf)), writeConfigBuilder.build());
    }

    private Table createTableDefinition(String schemaName, String metastoreTableName, boolean isRtTable)
    {
        StorageFormat storageFormat = StorageFormat.create(
                PARQUET_HIVE_SERDE_CLASS,
                isRtTable ? HUDI_PARQUET_REALTIME_INPUT_FORMAT : HUDI_PARQUET_INPUT_FORMAT,
                MAPRED_PARQUET_OUTPUT_FORMAT_CLASS);

        return Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(metastoreTableName)
                .setTableType(EXTERNAL_TABLE.name())
                .setOwner(Optional.of("public"))
                .setDataColumns(ImmutableList.<Column>builder()
                        .addAll(HUDI_META_COLUMNS)
                        .addAll(dataColumns())
                        .build())
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(storageFormat)
                        .setLocation(tableLocation.toString()))
                .build();
    }
}
