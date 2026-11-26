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
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.hudi.HudiConnector;
import io.trino.plugin.hudi.storage.HudiTrinoStorage;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.QueryRunner;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_REALTIME_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.metastore.HiveType.HIVE_BOOLEAN;
import static io.trino.metastore.HiveType.HIVE_BYTE;
import static io.trino.metastore.HiveType.HIVE_DATE;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_SHORT;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.metastore.HiveType.HIVE_TIMESTAMP;
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hudi.testing.HudiTestUtils.hiveSyncStorageSchema;
import static io.trino.plugin.hudi.testing.HudiTestUtils.metastoreDataColumns;
import static io.trino.plugin.hudi.testing.HudiTestUtils.withDeclaredTypes;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.decimalHiveType;

public class ResourceHudiTablesInitializer
        implements HudiTablesInitializer
{
    private static final Logger log = Logger.get(ResourceHudiTablesInitializer.class);
    private static final String HASH_ALGORITHM = "SHA-256";
    private static final String TEST_RESOURCE_NAME = "hudi-testing-data";

    // There might be other entry points that are using this initializer, make the location unique so it is more identifiable via logs
    private final String baseLocationPrefix = UUID.randomUUID().toString();
    private final Path tempDir;

    /**
     * Manually declaring a temp directory here and performing a manual cleanup as this constructor is invoked in HudiQueryRunner in a @BeforeAll static function.
     * This means that jupiter's managed @TempDir annotation cannot be used as the path will be passed as null.
     */
    public ResourceHudiTablesInitializer()
    {
        // There are multiple entry points and they may perform unzipping together, ensure that they are all unzipping to different paths
        try {
            this.tempDir = Files.createTempDirectory(TEST_RESOURCE_NAME + "_" + baseLocationPrefix);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initializeTables(QueryRunner queryRunner, Location externalLocation, String schemaName)
            throws Exception
    {
        // Inflate all deflated test resource archives to a temporary directory
        HudiTableUnzipper.unzipAllItemsInResource(TEST_RESOURCE_NAME, tempDir);
        TrinoFileSystem fileSystem = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
        String locationSuffix = schemaName + "_" + baseLocationPrefix;
        Location baseLocation = externalLocation.appendSuffix(locationSuffix);
        log.info("Initialized test resource directory as: %s", baseLocation.toString());
        copyDir(tempDir, fileSystem, baseLocation);
        // Perform a cleanup
        HudiTableUnzipper.deleteInflatedFiles(tempDir);

        for (TestingTable table : TestingTable.values()) {
            String tableName = table.getTableName();
            Location tablePath = baseLocation.appendPath(tableName);
            HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                    .setStorage(new HudiTrinoStorage(fileSystem, new TrinoStorageConfiguration()))
                    .setBasePath(tablePath.toString())
                    .build();
            List<Column> dataColumns = table.getDataColumns(metaClient);

            // Always create ro table by default
            createTable(
                    queryRunner,
                    schemaName,
                    tablePath,
                    tableName,
                    dataColumns,
                    table.getPartitionColumns(),
                    table.getPartitions(),
                    false);

            if (table.isCreateRtTable()) {
                createTable(
                        queryRunner,
                        schemaName,
                        tablePath,
                        table.getRtTableName(),
                        dataColumns,
                        table.getPartitionColumns(),
                        table.getPartitions(),
                        true);
            }

            // Set table version
            table.setTableVersion(metaClient.getTableConfig().getTableVersion());
        }
    }

    private void createTable(
            QueryRunner queryRunner,
            String schemaName,
            Location tablePath,
            String tableName,
            List<Column> dataColumns,
            List<Column> partitionColumns,
            Map<String, String> partitions,
            boolean isRtTable)
    {
        StorageFormat roStorageFormat = StorageFormat.create(
                PARQUET_HIVE_SERDE_CLASS,
                HUDI_PARQUET_INPUT_FORMAT,
                MAPRED_PARQUET_OUTPUT_FORMAT_CLASS);

        StorageFormat rtStorageFormat = StorageFormat.create(
                PARQUET_HIVE_SERDE_CLASS,
                HUDI_PARQUET_REALTIME_INPUT_FORMAT,
                MAPRED_PARQUET_OUTPUT_FORMAT_CLASS);

        Table table = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setTableType(EXTERNAL_TABLE.name())
                .setOwner(Optional.of("public"))
                .setDataColumns(dataColumns)
                .setPartitionColumns(partitionColumns)
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(isRtTable ? rtStorageFormat : roStorageFormat)
                        .setLocation(tablePath.toString()))
                .build();

        HiveMetastore metastore = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
        metastore.createTable(table, PrincipalPrivileges.NO_PRIVILEGES);

        List<PartitionWithStatistics> partitionsToAdd = new ArrayList<>();
        partitions.forEach((partitionName, partitionPath) -> {
            Partition partition = Partition.builder()
                    .setDatabaseName(schemaName)
                    .setTableName(tableName)
                    .setValues(extractPartitionValues(partitionName))
                    .withStorage(storageBuilder -> storageBuilder
                            .setStorageFormat(isRtTable ? rtStorageFormat : roStorageFormat)
                            .setLocation(tablePath.appendPath(partitionPath).toString()))
                    .setColumns(dataColumns)
                    .build();
            partitionsToAdd.add(new PartitionWithStatistics(partition, partitionName, PartitionStatistics.empty()));
        });
        metastore.addPartitions(schemaName, tableName, partitionsToAdd);
    }

    private static Column column(String name, HiveType type)
    {
        return new Column(name, type, Optional.empty(), Map.of());
    }

    public static void copyDir(Path sourceDirectory, TrinoFileSystem fileSystem, Location destinationDirectory)
            throws IOException
    {
        try (Stream<Path> paths = Files.walk(sourceDirectory)) {
            for (Iterator<Path> iterator = paths.iterator(); iterator.hasNext(); ) {
                Path path = iterator.next();
                if (path.toFile().isDirectory()) {
                    continue;
                }

                // hudi blows up if crc files are present
                if (path.toString().endsWith(".crc")) {
                    continue;
                }

                HashAndSizeResult srcHashAndSize;
                try {
                    srcHashAndSize = calculateHashAndSize(path);
                }
                catch (NoSuchAlgorithmException e) {
                    throw new IOException("Failed to calculate source hash: Algorithm not found", e);
                }

                Location location = destinationDirectory.appendPath(sourceDirectory.relativize(path).toString());
                fileSystem.createDirectory(location.parentDirectory());
                try (OutputStream out = fileSystem.newOutputFile(location).create()) {
                    if (srcHashAndSize.size > 0) {
                        Files.copy(path, out);
                        // Flush all data before close() to ensure durability
                        out.flush();
                    }
                }

                HashAndSizeResult dstHashAndSize;
                try {
                    dstHashAndSize = srcHashAndSize.size > 0 ? calculateHashAndSize(location, fileSystem)
                        : new HashAndSizeResult(new byte[0], 0);
                }
                catch (NoSuchAlgorithmException e) {
                    throw new IOException("Failed to calculate destination hash: Algorithm not found", e);
                }
                catch (IOException e) {
                    throw new IOException("Failed to read back " + location + " for hash verification", e);
                }

                if (!Arrays.equals(srcHashAndSize.hash, dstHashAndSize.hash)) {
                    // Hashes do not match, file is corrupt or copy failed
                    String errorMessage = String.format(
                            "Hash mismatch for file: %s (source size: %d bytes) copied to %s (destination size: %d bytes). Content hashes differ",
                            path,
                            srcHashAndSize.size,
                            location,
                            dstHashAndSize.size);
                    throw new IOException(errorMessage);
                }
            }
        }
    }

    /**
     * Helper method to calculate hash and size from an input stream
     */
    private static HashAndSizeResult calculateHashAndSize(InputStream inputStream)
            throws IOException, NoSuchAlgorithmException
    {
        MessageDigest md = MessageDigest.getInstance(HASH_ALGORITHM);
        try (DigestInputStream dis = new DigestInputStream(inputStream, md)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            long streamSize = 0;
            while ((bytesRead = dis.read(buffer)) != -1) {
                streamSize += bytesRead;
            }
            return new HashAndSizeResult(md.digest(), streamSize);
        }
    }

    /**
     * Helper method to calculate hash for a local Path
     */
    private static HashAndSizeResult calculateHashAndSize(Path path)
            throws IOException, NoSuchAlgorithmException
    {
        if (Files.size(path) == 0) {
            return new HashAndSizeResult(new byte[0], 0);
        }
        try (InputStream is = Files.newInputStream(path)) {
            return calculateHashAndSize(is);
        }
    }

    /**
     * Helper method to calculate hash for a file on TrinoFileSystem
     */
    private static HashAndSizeResult calculateHashAndSize(Location location, TrinoFileSystem fileSystem)
            throws IOException, NoSuchAlgorithmException
    {
        try (InputStream is = fileSystem.newInputFile(location).newStream()) {
            return calculateHashAndSize(is);
        }
    }

    /** How a resource table was written, which decides the {@code support_timestamp} its Hive sync runs with. */
    private enum Writer
    {
        /** Spark SQL syncs with {@code support_timestamp=true} ({@code ProvidesHoodieConfig}). */
        SPARK_SQL(true),
        /** The Spark datasource writer and HoodieStreamer keep the Hive sync default, {@code support_timestamp=false}. */
        SPARK_DATASOURCE_OR_STREAMER(false);

        private final boolean supportTimestamp;

        Writer(boolean supportTimestamp)
        {
            this.supportTimestamp = supportTimestamp;
        }
    }

    public enum TestingTable
    {
        HUDI_NON_PART_COW(Writer.SPARK_SQL),
        HUDI_NON_PART_MOR(Writer.SPARK_DATASOURCE_OR_STREAMER, ImmutableList.of(), ImmutableMap.of(), true),
        HUDI_TRIPS_COW_V8(Writer.SPARK_DATASOURCE_OR_STREAMER),
        HUDI_COW_TABLE_WITH_FIELD_NAMES_IN_CAPS(Writer.SPARK_DATASOURCE_OR_STREAMER),
        HUDI_COW_PT_TABLE_WITH_FIELD_NAMES_IN_CAPS(Writer.SPARK_DATASOURCE_OR_STREAMER, hudiTableWithFieldNamesInCapsPartitionColumns(), hudiTableWithFieldNamesInCapsPartitions(), false), // delete
        HUDI_COW_TABLE_WITH_MULTI_KEYS_AND_FIELD_NAMES_IN_CAPS(Writer.SPARK_DATASOURCE_OR_STREAMER),
        HUDI_MOR_TABLE_WITH_FIELD_NAMES_IN_CAPS(Writer.SPARK_DATASOURCE_OR_STREAMER, ImmutableList.of(), ImmutableMap.of(), true),
        HUDI_COW_PT_TBL(Writer.SPARK_SQL, multiPartitionColumns(), multiPartitions(), false),
        STOCK_TICKS_COW(Writer.SPARK_DATASOURCE_OR_STREAMER, stockTicksPartitionColumns(), stockTicksPartitions(), false),
        STOCK_TICKS_MOR(Writer.SPARK_DATASOURCE_OR_STREAMER, stockTicksPartitionColumns(), stockTicksPartitions(), false),
        HUDI_STOCK_TICKS_COW(Writer.SPARK_DATASOURCE_OR_STREAMER, hudiStockTicksPartitionColumns(), hudiStockTicksPartitions(), false),
        HUDI_STOCK_TICKS_MOR(Writer.SPARK_DATASOURCE_OR_STREAMER, hudiStockTicksPartitionColumns(), hudiStockTicksPartitions(), false),
        HUDI_MULTI_FG_PT_V6_MOR(Writer.SPARK_SQL, hudiMultiFgPartitionsColumn(), hudiMultiFgPartitions(), false),
        HUDI_MULTI_FG_PT_V8_MOR(Writer.SPARK_SQL, hudiMultiFgPartitionsColumn(), hudiMultiFgPartitions(), false),
        HUDI_COMPREHENSIVE_TYPES_V6_MOR(Writer.SPARK_SQL, hudiComprehensiveTypesPartitionColumns(), hudiComprehensiveTypesPartitions(), true),
        HUDI_COMPREHENSIVE_TYPES_V8_MOR(Writer.SPARK_SQL, hudiComprehensiveTypesPartitionColumns(), hudiComprehensiveTypesPartitions(), true),
        HUDI_MULTI_PT_V8_MOR(Writer.SPARK_SQL, hudiMultiPtMorPartitionColumns(), hudiMultiPtMorPartitions(), false),
        HUDI_TIMESTAMP_KEYGEN_PT_EPOCH_TO_YYYY_MM_DD_HH_V8_MOR(Writer.SPARK_SQL, hudiTimestampKeygenPartitionColumns(), hudiTimestampKeygenPartitions("EPOCHMILLISECONDS"), true),
        HUDI_TIMESTAMP_KEYGEN_PT_SCALAR_TO_YYYY_MM_DD_HH_V8_MOR(Writer.SPARK_SQL, hudiTimestampKeygenPartitionColumns(), hudiTimestampKeygenPartitions("SCALAR"), true),
        HUDI_CUSTOM_KEYGEN_PT_V8_MOR(Writer.SPARK_SQL, hudiCustomKeyGenPartitionColumns(), hudiCustomKeyGenPartitions(), false),
        HUDI_NON_EXTRACTABLE_PARTITION_PATH(Writer.SPARK_DATASOURCE_OR_STREAMER, multiPartitionColumns(), multiPartitionsWithNonExtractablePartitionPaths(), false),
        HUDI_MOR_ARCHIVED_TIMELINE(Writer.SPARK_SQL, ImmutableList.of(), ImmutableMap.of(), true),
        /**/;

        private final Writer writer;
        private final List<Column> partitionColumns;
        private final Map<String, String> partitions;
        private final boolean isCreateRtTable;
        private HoodieTableVersion tableVersion;

        TestingTable(
                Writer writer,
                List<Column> partitionColumns,
                Map<String, String> partitions,
                boolean isCreateRtTable)
        {
            this.writer = writer;
            this.partitionColumns = partitionColumns;
            this.partitions = partitions;
            this.isCreateRtTable = isCreateRtTable;
        }

        TestingTable(Writer writer)
        {
            this(writer, ImmutableList.of(), ImmutableMap.of(), false);
        }

        public String getTableName()
        {
            return name().toLowerCase(Locale.ROOT);
        }

        public String getRtTableName()
        {
            return name().toLowerCase(Locale.ROOT) + "_rt";
        }

        public String getRoTableName()
        {
            // ro tables do not have suffix
            return getTableName();
        }

        public void setTableVersion(HoodieTableVersion tableVersion)
        {
            this.tableVersion = tableVersion;
        }

        public HoodieTableVersion getHoodieTableVersion()
        {
            return this.tableVersion;
        }

        /**
         * The data columns Hive sync registers for the table: derived from its Hudi table schema, meta fields included,
         * with the {@code support_timestamp} its writer syncs with and the partition columns left out.
         */
        public List<Column> getDataColumns(HoodieTableMetaClient metaClient)
        {
            List<String> partitionKeys = partitionColumns.stream().map(Column::getName).collect(toImmutableList());
            List<Column> columns = metastoreDataColumns(hiveSyncStorageSchema(metaClient, false), writer.supportTimestamp, partitionKeys);
            if (this == HUDI_COMPREHENSIVE_TYPES_V6_MOR || this == HUDI_COMPREHENSIVE_TYPES_V8_MOR) {
                // The types the tables' Spark SQL DDL declared, which the Hudi schema holds as int and string
                return withDeclaredTypes(columns, ImmutableMap.of(
                        "col_tinyint", HIVE_BYTE,
                        "col_smallint", HIVE_SHORT,
                        "col_varchar", HiveType.valueOf("varchar(50)"),
                        "col_char", HiveType.valueOf("char(10)")));
            }
            return columns;
        }

        public List<Column> getPartitionColumns()
        {
            return partitionColumns;
        }

        public Map<String, String> getPartitions()
        {
            return partitions;
        }

        public boolean isCreateRtTable()
        {
            return isCreateRtTable;
        }

        // The actual Hudi table has "Country" as the partition field name, but the catalog provides it in lowercase.
        // Using lowercase here to stay consistent with the catalog for testing.
        private static Map<String, String> hudiTableWithFieldNamesInCapsPartitions()
        {
            return ImmutableMap.of(
                    "country=IND", "IND",
                    "country=US", "US");
        }

        // The actual Hudi table has "Country" as the partition field name, but the catalog provides it in lowercase.
        // Using lowercase here to stay consistent with the catalog for testing.
        private static List<Column> hudiTableWithFieldNamesInCapsPartitionColumns()
        {
            return ImmutableList.of(column("country", HIVE_STRING));
        }

        private static List<Column> stockTicksPartitionColumns()
        {
            return ImmutableList.of(column("dt", HIVE_STRING));
        }

        private static Map<String, String> stockTicksPartitions()
        {
            return ImmutableMap.of("dt=2018-08-31", "2018/08/31");
        }

        private static List<Column> hudiStockTicksPartitionColumns()
        {
            return ImmutableList.of(column("date", HIVE_STRING));
        }

        private static Map<String, String> hudiStockTicksPartitions()
        {
            return ImmutableMap.of("date=2018-08-31", "2018/08/31");
        }

        private static List<Column> multiPartitionColumns()
        {
            return ImmutableList.of(
                    column("dt", HIVE_STRING),
                    column("hh", HIVE_STRING));
        }

        private static Map<String, String> multiPartitions()
        {
            return ImmutableMap.of(
                    "dt=2021-12-09/hh=10", "dt=2021-12-09/hh=10",
                    "dt=2021-12-09/hh=11", "dt=2021-12-09/hh=11");
        }

        /**
         * Returns a sample map of partition specs containing multiple partition keys separated by slashes.
         *
         * Example:
         *   "dt=2018-10-05/hh=10" -> "2018/10/05/10"
         *
         * Note:
         *  - The partition spec has 2 partition keys (dt, hh).
         *  - However, the corresponding value string has 4 segments when split by slashes
         *    (year, month, day, hour).
         *  - Standard Hudi partition extractors will not be able to correctly parse this mapping,
         *    since they expect the number of slash-separated values to match the number of partition keys
         *    if there's more than one partition field.
         */
        private static Map<String, String> multiPartitionsWithNonExtractablePartitionPaths()
        {
            return ImmutableMap.of(
                    "dt=2018-10-05/hh=10", "2018/10/05/10",
                    "dt=2018-10-06/hh=5", "2018/10/06/5");
        }

        private static List<Column> hudiMultiFgPartitionsColumn()
        {
            return ImmutableList.of(
                    column("country", HIVE_STRING));
        }

        private static Map<String, String> hudiMultiFgPartitions()
        {
            return ImmutableMap.of(
                    "country=SG", "country=SG",
                    "country=US", "country=US");
        }

        private static List<Column> hudiComprehensiveTypesPartitionColumns()
        {
            return ImmutableList.of(column("part_col", HIVE_STRING));
        }

        private static Map<String, String> hudiComprehensiveTypesPartitions()
        {
            return ImmutableMap.of(
                    "part_col=A", "part_col=A",
                    "part_col=B", "part_col=B");
        }

        private static List<Column> hudiMultiPtMorPartitionColumns()
        {
            return ImmutableList.of(
                    column("part_str", HIVE_STRING),
                    column("part_int", HIVE_INT),
                    column("part_date", HIVE_DATE),
                    column("part_bigint", HIVE_LONG),
                    column("part_decimal", decimalHiveType(10, 2)),
                    column("part_timestamp", HIVE_TIMESTAMP),
                    column("part_bool", HIVE_BOOLEAN));
        }

        private static Map<String, String> hudiMultiPtMorPartitions()
        {
            return ImmutableMap.of(
                    "part_str=apparel/part_int=2024/part_date=2024-01-05/part_bigint=20000000001/part_decimal=100.00/part_timestamp=2024-01-05 18%3A00%3A00/part_bool=false", "part_str=apparel/part_int=2024/part_date=2024-01-05/part_bigint=20000000001/part_decimal=100.00/part_timestamp=2024-01-05 18%3A00%3A00/part_bool=false",
                    "part_str=electronics/part_int=2023/part_date=2023-03-10/part_bigint=10000000002/part_decimal=50.00/part_timestamp=2023-03-10 12%3A30%3A00/part_bool=true", "part_str=electronics/part_int=2023/part_date=2023-03-10/part_bigint=10000000002/part_decimal=50.00/part_timestamp=2023-03-10 12%3A30%3A00/part_bool=true",
                    "part_str=electronics/part_int=2023/part_date=2023-03-10/part_bigint=10000000002/part_decimal=50.00/part_timestamp=2023-03-10 12%3A30%3A00/part_bool=false", "part_str=electronics/part_int=2023/part_date=2023-03-10/part_bigint=10000000002/part_decimal=50.00/part_timestamp=2023-03-10 12%3A30%3A00/part_bool=false",
                    "part_str=books/part_int=2023/part_date=2023-01-15/part_bigint=10000000001/part_decimal=123.00/part_timestamp=2023-01-15 10%3A00%3A00/part_bool=true", "part_str=books/part_int=2023/part_date=2023-01-15/part_bigint=10000000001/part_decimal=123.00/part_timestamp=2023-01-15 10%3A00%3A00/part_bool=true",
                    "part_str=books/part_int=2024/part_date=2024-02-20/part_bigint=10000000003/part_decimal=75.00/part_timestamp=2024-02-20 08%3A45%3A10/part_bool=true", "part_str=books/part_int=2024/part_date=2024-02-20/part_bigint=10000000003/part_decimal=75.00/part_timestamp=2024-02-20 08%3A45%3A10/part_bool=true");
        }

        private static List<Column> hudiTimestampKeygenPartitionColumns()
        {
            // Data stored in files are long, but partition value that is synced to metastore is String
            return ImmutableList.of(column("partition_field", HIVE_STRING));
        }

        private static Map<String, String> hudiTimestampKeygenPartitions(String timestampType)
        {
            return switch (timestampType) {
                case "EPOCHMILLISECONDS" -> ImmutableMap.of(
                        "partition_field=2025-05-13 02", "2025-05-13 02",
                        "partition_field=2025-06-05 05", "2025-06-05 05",
                        "partition_field=2025-06-06 09", "2025-06-06 09",
                        "partition_field=2025-06-06 10", "2025-06-06 10",
                        "partition_field=2025-06-07 08", "2025-06-07 08");
                case "SCALAR" -> ImmutableMap.of(
                        "partition_field=2024-10-08 12", "2024-10-08 12",
                        "partition_field=2024-10-07 12", "2024-10-07 12",
                        "partition_field=2024-10-06 12", "2024-10-06 12",
                        "partition_field=2024-10-05 12", "2024-10-05 12",
                        "partition_field=2024-10-04 12", "2024-10-04 12");
                default -> ImmutableMap.of();
            };
        }

        private static List<Column> hudiCustomKeyGenPartitionColumns()
        {
            return ImmutableList.of(
                    column("partition_field_country", HIVE_STRING),
                    // Hive sync would register the field's schema type, bigint, but the custom key generator writes it
                    // into the path formatted as yyyy-MM-dd, which a bigint partition key cannot hold. The connector
                    // reads the path value; apache/hudi#20783 tracks reading the raw value as Spark does.
                    column("partition_field_date", HIVE_STRING));
        }

        private static Map<String, String> hudiCustomKeyGenPartitions()
        {
            return ImmutableMap.of(
                    "partition_field_country=US/partition_field_date=2025-06-06", "partition_field_country=US/partition_field_date=2025-06-06",
                    "partition_field_country=CN/partition_field_date=2025-06-05", "partition_field_country=CN/partition_field_date=2025-06-05",
                    "partition_field_country=MY/partition_field_date=2025-05-13", "partition_field_country=MY/partition_field_date=2025-05-13",
                    "partition_field_country=SG/partition_field_date=2025-06-06", "partition_field_country=SG/partition_field_date=2025-06-06",
                    "partition_field_country=SG/partition_field_date=2025-06-07", "partition_field_country=SG/partition_field_date=2025-06-07");
        }
    }

    static class HashAndSizeResult
    {
        final byte[] hash;
        final long size;

        HashAndSizeResult(byte[] hash, long size)
        {
            this.hash = hash;
            this.size = size;
        }
    }
}
