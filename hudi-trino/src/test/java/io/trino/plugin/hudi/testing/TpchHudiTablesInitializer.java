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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
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
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchColumnType;
import io.trino.tpch.TpchColumnTypes;
import io.trino.tpch.TpchTable;
import org.apache.avro.LogicalTypes;
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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.intellij.lang.annotations.Language;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hudi.testing.HudiTestUtils.hiveSyncStorageSchema;
import static io.trino.plugin.hudi.testing.HudiTestUtils.metastoreDataColumns;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.requireNonNull;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;

public class TpchHudiTablesInitializer
        implements HudiTablesInitializer
{
    public static final String FIELD_UUID = "_uuid";
    private static final CatalogSchemaName TPCH_TINY = new CatalogSchemaName("tpch", "tiny");
    private static final String PARTITION_PATH = "";
    private static final Logger log = Logger.get(TpchHudiTablesInitializer.class);

    private final List<TpchTable<?>> tpchTables;

    public TpchHudiTablesInitializer(List<TpchTable<?>> tpchTables)
    {
        this.tpchTables = requireNonNull(tpchTables, "tpchTables is null");
    }

    @Override
    public void initializeTables(QueryRunner queryRunner, Location externalLocation, String schemaName)
            throws Exception
    {
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog(TPCH_TINY.getCatalogName(), "tpch", ImmutableMap.of());
        TrinoFileSystem fileSystem = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
        HiveMetastore metastore = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                        .getInstance(HiveMetastoreFactory.class)
                        .createMetastore(Optional.empty());

        Location dataLocation = externalLocation.appendPath("tpch");

        java.nio.file.Path tempDir = createTempDirectory("test");
        try {
            for (TpchTable<?> tpchTable : tpchTables) {
                java.nio.file.Path tempTableDir = tempDir.resolve(tpchTable.getTableName());
                load(tpchTable, queryRunner, tempTableDir);

                Location tableLocation = dataLocation.appendPath(tpchTable.getTableName());
                ResourceHudiTablesInitializer.copyDir(tempTableDir, fileSystem, tableLocation);

                // Hive sync's columns for the table as written (its HMS path, support_timestamp off as by default)
                List<Column> columns = metastoreDataColumns(hiveSyncStorageSchema(createMetaClient(tempTableDir), false), false);
                Table table = createTableDefinition(schemaName, tpchTable, tableLocation, columns);
                metastore.createTable(table, PrincipalPrivileges.NO_PRIVILEGES);
            }
        }
        finally {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }

    public void load(TpchTable<?> tpchTables, QueryRunner queryRunner, java.nio.file.Path tableDirectory)
    {
        try (HoodieJavaWriteClient<HoodieAvroPayload> writeClient = createWriteClient(tpchTables, new Path(tableDirectory.toUri()))) {
            RecordConverter recordConverter = createRecordConverter(tpchTables);

            @Language("SQL") String sql = generateScanSql(TPCH_TINY, tpchTables);
            log.info("Executing %s", sql);
            MaterializedResult result = queryRunner.execute(sql);

            List<HoodieRecord<HoodieAvroPayload>> records = result.getMaterializedRows()
                    .stream()
                    .map(MaterializedRow::getFields)
                    .map(recordConverter::toRecord)
                    .collect(Collectors.toList());
            String instantTime = writeClient.startCommit();
            List<WriteStatus> writeStatuses = writeClient.insert(records, instantTime);
            writeClient.commit(instantTime, writeStatuses);
        }
    }

    private static String generateScanSql(CatalogSchemaName catalogSchemaName, TpchTable<?> table)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        String columnList = table.getColumns()
                .stream()
                .map(c -> quote(c.getSimplifiedColumnName()))
                .collect(Collectors.joining(", "));
        builder.append(columnList);
        String tableName = format("%s.%s", catalogSchemaName.toString(), table.getTableName());
        builder.append(" FROM ").append(tableName);
        return builder.toString();
    }

    private static HoodieTableMetaClient createMetaClient(java.nio.file.Path tableDirectory)
    {
        return HoodieTableMetaClient.builder()
                .setConf(new HadoopStorageConfiguration(new Configuration()))
                .setBasePath(new Path(tableDirectory.toUri()).toString())
                .build();
    }

    private static Table createTableDefinition(String schemaName, TpchTable<?> table, Location location, List<Column> columns)
    {
        StorageFormat storageFormat = StorageFormat.create(
                PARQUET_HIVE_SERDE_CLASS,
                HUDI_PARQUET_INPUT_FORMAT,
                MAPRED_PARQUET_OUTPUT_FORMAT_CLASS);

        return Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(table.getTableName())
                .setTableType(EXTERNAL_TABLE.name())
                .setOwner(Optional.of("public"))
                .setDataColumns(columns)
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(storageFormat)
                        .setLocation(location.toString()))
                .build();
    }

    private static HoodieJavaWriteClient<HoodieAvroPayload> createWriteClient(TpchTable<?> table, Path tablePath)
    {
        Schema schema = createAvroSchema(table);
        Configuration conf = new Configuration();

        try {
            HoodieTableMetaClient.newTableBuilder()
                    .setTableType(COPY_ON_WRITE)
                    .setTableName(table.getTableName())
                    .setTimelineLayoutVersion(1)
                    .setBootstrapIndexClass(NoOpBootstrapIndex.class.getName())
                    .setPayloadClassName(HoodieAvroPayload.class.getName())
                    .setRecordKeyFields(FIELD_UUID)
                    .initTable(new HadoopStorageConfiguration(conf), tablePath.toString());
        }
        catch (IOException e) {
            throw new RuntimeException("Could not init table " + table.getTableName(), e);
        }

        HoodieIndexConfig indexConfig = HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build();
        HoodieArchivalConfig archivalConfig = HoodieArchivalConfig.newBuilder().archiveCommitsWith(20, 30).build();
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
                .withPath(tablePath.toString())
                .withSchema(schema.toString())
                .withParallelism(2, 2)
                .withDeleteParallelism(2)
                .forTable(table.getTableName())
                .withIndexConfig(indexConfig)
                .withArchivalConfig(archivalConfig)
                .withEmbeddedTimelineServerEnabled(false)
                .withMarkersType(MarkerType.DIRECT.name())
                // Disabling Hudi metadata table (MDT) in tests as the support of
                // reading MDT is broken after removal of Hudi dependencies from compile time
                // IMPORTANT: Writing to MDT requires hbase dependencies, which is not available in Trino runtime
                .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
                .build();
        return new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(new HadoopStorageConfiguration(conf)), cfg);
    }

    private static RecordConverter createRecordConverter(TpchTable<?> table)
    {
        Schema schema = createAvroSchema(table);
        List<? extends TpchColumn<?>> columns = table.getColumns();

        int numberOfColumns = columns.size();
        List<String> columnNames = columns.stream()
                .map(TpchColumn::getSimplifiedColumnName)
                .toList();
        List<Function<Object, Object>> columnConverters = columns.stream()
                .map(TpchColumn::getType)
                .map(TpchHudiTablesInitializer::avroEncoderOf)
                .toList();

        return row -> {
            checkArgument(row.size() == numberOfColumns);

            // Create a GenericRecord
            GenericRecord record = new GenericData.Record(schema);
            for (int i = 0; i < numberOfColumns; i++) {
                record.put(columnNames.get(i), columnConverters.get(i).apply(row.get(i)));
            }
            // Add extra uuid column
            String uuid = UUID.randomUUID().toString();
            record.put(FIELD_UUID, uuid);

            // wrap to a HoodieRecord
            HoodieKey key = new HoodieKey(uuid, PARTITION_PATH);
            HoodieAvroPayload data = new HoodieAvroPayload(Option.of(record));
            return new HoodieAvroRecord<>(key, data, null);
        };
    }

    private static Schema createAvroSchema(TpchTable<?> table)
    {
        List<? extends TpchColumn<?>> tpchColumns = table.getColumns();
        List<Schema.Field> fields = new ArrayList<>(tpchColumns.size() + 1);
        for (TpchColumn<?> column : tpchColumns) {
            String columnName = column.getSimplifiedColumnName();
            fields.add(new Schema.Field(columnName, toSchema(column.getType())));
        }
        fields.add(new Schema.Field(FIELD_UUID, Schema.create(Schema.Type.STRING)));
        String name = table.getTableName();
        return Schema.createRecord(name, null, null, false, fields);
    }

    private static Schema toSchema(TpchColumnType columnType)
    {
        return TpchColumnTypeAdapter.of(columnType).avroSchema.get();
    }

    private static Function<Object, Object> avroEncoderOf(TpchColumnType columnType)
    {
        return TpchColumnTypeAdapter.of(columnType).avroEncoder;
    }

    private static String quote(String name)
    {
        return "\"" + name + "\"";
    }

    private enum TpchColumnTypeAdapter
    {
        INTEGER(() -> Schema.create(Schema.Type.INT), Function.identity()),
        IDENTIFIER(() -> Schema.create(Schema.Type.LONG), Function.identity()),
        // The connector types columns from the table schema, so a date has to be written as one
        DATE(() -> LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)), TpchColumnTypeAdapter::convertDate),
        DOUBLE(() -> Schema.create(Schema.Type.DOUBLE), Function.identity()),
        VARCHAR(() -> Schema.create(Schema.Type.STRING), Function.identity()),
        /**/;

        static TpchColumnTypeAdapter of(TpchColumnType columnType)
        {
            if (columnType == TpchColumnTypes.INTEGER) {
                return INTEGER;
            }
            else if (columnType == TpchColumnTypes.IDENTIFIER) {
                return IDENTIFIER;
            }
            else if (columnType == TpchColumnTypes.DATE) {
                return DATE;
            }
            else if (columnType == TpchColumnTypes.DOUBLE) {
                return DOUBLE;
            }
            else {
                if (columnType.getBase() != TpchColumnType.Base.VARCHAR || columnType.getPrecision().isEmpty()) {
                    throw new IllegalArgumentException("Illegal column type: " + columnType);
                }
                return VARCHAR;
            }
        }

        private final Supplier<Schema> avroSchema;
        private final Function<Object, Object> avroEncoder;

        TpchColumnTypeAdapter(Supplier<Schema> avroSchema, Function<Object, Object> avroEncoder)
        {
            this.avroSchema = avroSchema;
            this.avroEncoder = avroEncoder;
        }

        private static Object convertDate(Object input)
        {
            LocalDate date = (LocalDate) input;
            return (int) date.getLong(ChronoField.EPOCH_DAY);
        }
    }

    private interface RecordConverter
    {
        HoodieRecord<HoodieAvroPayload> toRecord(List<Object> row);
    }
}
