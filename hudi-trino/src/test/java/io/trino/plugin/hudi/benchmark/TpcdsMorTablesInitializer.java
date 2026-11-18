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
package io.trino.plugin.hudi.benchmark;

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
import io.trino.plugin.hudi.testing.HudiTablesInitializer;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.QueryRunner;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_REALTIME_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;

/**
 * Writes two TPC-DS-shaped MOR tables:
 * <ul>
 *   <li>{@code store_sales}: partitioned by {@code ss_sold_date_sk} (hive style), complex record key
 *   {@code ss_item_sk,ss_ticket_number}, decimal(7,2) measures, {@code COMMIT_TIME_ORDERING}, table version 9.
 *   One base file per partition, then four delta commits (update, update, delete, update) that leave exactly four
 *   log files in every file group.</li>
 *   <li>{@code date_dim}: a small MOR dimension with one update delta commit, so the build side of a join merges
 *   log files too.</li>
 * </ul>
 * Each table is registered twice: {@code <name>_ro} (base files only) and {@code <name>_rt} (merged snapshot).
 *
 * <p>Sizes come from system properties ({@code bench.partitions}, {@code bench.rowsPerPartition},
 * {@code bench.updateFraction}, {@code bench.deleteFraction}). The expected query answers are computed in memory
 * from the same generator so the benchmark can assert correctness.
 */
final class TpcdsMorTablesInitializer
        implements HudiTablesInitializer
{
    static final String STORE_SALES = "store_sales";
    static final String DATE_DIM = "date_dim";
    static final int FIRST_DATE_SK = 2450000;
    /** One partition every 30 days, so each month value holds partitions/12 partitions. */
    static final int DAYS_BETWEEN_PARTITIONS = 30;

    private static final Logger log = Logger.get(TpcdsMorTablesInitializer.class);
    static final String[] MEASURES = {
            "ss_wholesale_cost", "ss_list_price", "ss_sales_price", "ss_ext_discount_amt", "ss_ext_sales_price",
            "ss_ext_wholesale_cost", "ss_ext_list_price", "ss_ext_tax", "ss_coupon_amt", "ss_net_paid",
            "ss_net_paid_inc_tax", "ss_net_profit"};
    static final Schema DECIMAL_7_2 = LogicalTypes.decimal(7, 2).addToSchema(Schema.createFixed("decimal_7_2", null, "hoodie.bench", 4));
    static final Conversions.DecimalConversion DECIMAL_CONVERSION = new Conversions.DecimalConversion();

    private final int partitions;
    private final int rowsPerPartition;
    private final double updateFraction;
    private final double deleteFraction;

    /** Record key -> {@code [ss_sold_date_sk, ss_quantity, unscaled ss_net_paid]} of the merged snapshot. */
    private final Map<String, long[]> finalState = new LinkedHashMap<>();
    /** {@code ss_sold_date_sk -> [count, sum(ss_quantity)]} of the base files alone. */
    private final Map<Integer, long[]> roByDateSk = new TreeMap<>();
    private long roRowCount;
    private long roSumQuantity;
    private long roSumNetPaid;

    TpcdsMorTablesInitializer(int partitions, int rowsPerPartition, double updateFraction, double deleteFraction)
    {
        this.partitions = partitions;
        this.rowsPerPartition = rowsPerPartition;
        this.updateFraction = updateFraction;
        this.deleteFraction = deleteFraction;
    }

    static TpcdsMorTablesInitializer fromSystemProperties()
    {
        return new TpcdsMorTablesInitializer(
                Integer.getInteger("bench.partitions", 48),
                Integer.getInteger("bench.rowsPerPartition", 40_000),
                Double.parseDouble(System.getProperty("bench.updateFraction", "0.10")),
                Double.parseDouble(System.getProperty("bench.deleteFraction", "0.01")));
    }

    int partitions()
    {
        return partitions;
    }

    @Override
    public void initializeTables(QueryRunner queryRunner, Location externalLocation, String schemaName)
            throws Exception
    {
        HudiConnector connector = (HudiConnector) queryRunner.getCoordinator().getConnector(BenchmarkHudiQueryRunner.CATALOG);
        TrinoFileSystem fileSystem = connector.getInjector().getInstance(TrinoFileSystemFactory.class).create(ConnectorIdentity.ofUser("bench"));
        HiveMetastore metastore = connector.getInjector().getInstance(HiveMetastoreFactory.class).createMetastore(Optional.empty());

        Path stagingDir = createTempDirectory("hudi-trino-bench");
        try {
            long start = System.nanoTime();
            List<String> partitionPaths = writeStoreSales(stagingDir.resolve(STORE_SALES));
            writeDateDim(stagingDir.resolve(DATE_DIM));
            log.info("Wrote benchmark tables (%s partitions x %s rows) in %.1fs", partitions, rowsPerPartition, (System.nanoTime() - start) / 1e9);

            for (String table : List.of(STORE_SALES, DATE_DIM)) {
                Location tableLocation = externalLocation.appendPath(table);
                ResourceHudiTablesInitializer.copyDir(stagingDir.resolve(table), fileSystem, tableLocation);
            }
            Location storeSales = externalLocation.appendPath(STORE_SALES);
            Location dateDim = externalLocation.appendPath(DATE_DIM);
            for (boolean rt : new boolean[] {false, true}) {
                String suffix = rt ? "_rt" : "_ro";
                registerTable(metastore, schemaName, STORE_SALES + suffix, storeSales, storeSalesColumns(),
                        List.of(new Column("ss_sold_date_sk", HIVE_INT, Optional.empty(), Map.of())), partitionPaths, rt);
                registerTable(metastore, schemaName, DATE_DIM + suffix, dateDim, dateDimColumns(), List.of(), List.of(), rt);
            }
        }
        finally {
            deleteRecursively(stagingDir, ALLOW_INSECURE);
        }
    }

    /** Row count, {@code sum(ss_quantity)} and unscaled {@code sum(ss_net_paid)} of the merged snapshot. */
    long[] expectedRtTotals()
    {
        long sum = 0;
        long netPaid = 0;
        for (long[] row : finalState.values()) {
            sum += row[1];
            netPaid += row[2];
        }
        return new long[] {finalState.size(), sum, netPaid};
    }

    /** Row count, {@code sum(ss_quantity)} and unscaled {@code sum(ss_net_paid)} of the base files alone. */
    long[] expectedRoTotals()
    {
        return new long[] {roRowCount, roSumQuantity, roSumNetPaid};
    }

    /** {@code d_year -> [count, sum(ss_quantity)]} of the merged snapshot joined to {@code date_dim} on {@code d_moy = month}. */
    Map<Long, long[]> expectedRtByYearForMonth(int month)
    {
        Map<Long, long[]> result = new TreeMap<>();
        for (long[] row : finalState.values()) {
            int dateSk = (int) row[0];
            if (moy(dateSk) == month) {
                long[] acc = result.computeIfAbsent((long) year(dateSk), y -> new long[2]);
                acc[0]++;
                acc[1] += row[1];
            }
        }
        return result;
    }

    /** {@code d_year -> [count, sum(ss_quantity)]} of the base files alone joined to {@code date_dim} on {@code d_moy = month}. */
    Map<Long, long[]> expectedRoByYearForMonth(int month)
    {
        Map<Long, long[]> result = new TreeMap<>();
        roByDateSk.forEach((dateSk, totals) -> {
            if (moy(dateSk) == month) {
                long[] acc = result.computeIfAbsent((long) year(dateSk), y -> new long[2]);
                acc[0] += totals[0];
                acc[1] += totals[1];
            }
        });
        return result;
    }

    static int dateSk(int partition)
    {
        return FIRST_DATE_SK + partition * DAYS_BETWEEN_PARTITIONS;
    }

    static int moy(int dateSk)
    {
        return ((dateSk - FIRST_DATE_SK) / DAYS_BETWEEN_PARTITIONS) % 12 + 1;
    }

    static int year(int dateSk)
    {
        return 1998 + (dateSk - FIRST_DATE_SK) / (DAYS_BETWEEN_PARTITIONS * 12);
    }

    private List<String> writeStoreSales(Path tablePath)
            throws IOException
    {
        Schema schema = storeSalesSchema();
        initTable(tablePath, STORE_SALES, "ss_item_sk,ss_ticket_number", "ss_sold_date_sk");
        Random random = new Random(42);
        List<String> partitionPaths = new ArrayList<>();
        Map<String, HoodieKey> keys = new LinkedHashMap<>();

        try (HoodieJavaWriteClient<HoodieAvroPayload> client = writeClient(tablePath, STORE_SALES, schema)) {
            List<HoodieRecord<HoodieAvroPayload>> inserts = new ArrayList<>(partitions * rowsPerPartition);
            long ticket = 0;
            for (int p = 0; p < partitions; p++) {
                int dateSk = dateSk(p);
                String partitionPath = "ss_sold_date_sk=" + dateSk;
                partitionPaths.add(partitionPath);
                for (int r = 0; r < rowsPerPartition; r++) {
                    int itemSk = 1 + random.nextInt(300_000);
                    ticket++;
                    String recordKey = "ss_item_sk:" + itemSk + ",ss_ticket_number:" + ticket;
                    HoodieKey key = new HoodieKey(recordKey, partitionPath);
                    int quantity = 1 + random.nextInt(100);
                    long[] state = new long[] {dateSk, quantity, 0};
                    inserts.add(record(schema, key, itemSk, ticket, state, random, 0));
                    keys.put(recordKey, key);
                    finalState.put(recordKey, state);
                    long[] partitionTotals = roByDateSk.computeIfAbsent(dateSk, sk -> new long[2]);
                    partitionTotals[0]++;
                    partitionTotals[1] += quantity;
                    roSumQuantity += quantity;
                    roSumNetPaid += state[2];
                }
            }
            roRowCount = inserts.size();
            commit(client, "bulk_insert", inserts, null);

            List<String> recordKeys = new ArrayList<>(keys.keySet());
            for (int round = 1; round <= 4; round++) {
                if (round == 3) {
                    List<HoodieKey> deletes = new ArrayList<>();
                    for (String recordKey : sample(recordKeys, deleteFraction, random)) {
                        if (finalState.remove(recordKey) != null) {
                            deletes.add(keys.get(recordKey));
                        }
                    }
                    commit(client, "delete", null, deletes);
                    continue;
                }
                List<HoodieRecord<HoodieAvroPayload>> updates = new ArrayList<>();
                for (String recordKey : sample(recordKeys, updateFraction, random)) {
                    long[] state = finalState.get(recordKey);
                    if (state == null) {
                        continue;
                    }
                    HoodieKey key = keys.get(recordKey);
                    int quantity = 1 + random.nextInt(100);
                    state[1] = quantity;
                    String[] parts = recordKey.split(",");
                    int itemSk = Integer.parseInt(parts[0].substring("ss_item_sk:".length()));
                    long ticketNumber = Long.parseLong(parts[1].substring("ss_ticket_number:".length()));
                    updates.add(record(schema, key, itemSk, ticketNumber, state, random, round));
                }
                commit(client, "upsert", updates, null);
            }
        }
        return partitionPaths;
    }

    private void writeDateDim(Path tablePath)
            throws IOException
    {
        Schema schema = SchemaBuilder.record("date_dim_record").namespace("hoodie.bench").fields()
                .requiredInt("d_date_sk")
                .requiredInt("d_year")
                .requiredInt("d_moy")
                .requiredInt("d_dom")
                .requiredLong("__op_ts")
                .endRecord();
        initTable(tablePath, DATE_DIM, "d_date_sk", "");
        try (HoodieJavaWriteClient<HoodieAvroPayload> client = writeClient(tablePath, DATE_DIM, schema)) {
            int days = Math.max(partitions * DAYS_BETWEEN_PARTITIONS, 3650);
            List<HoodieRecord<HoodieAvroPayload>> rows = new ArrayList<>();
            List<HoodieRecord<HoodieAvroPayload>> updates = new ArrayList<>();
            for (int d = 0; d < days; d++) {
                int dateSk = FIRST_DATE_SK + d;
                rows.add(dateDimRecord(schema, dateSk, 1));
                if (d % 10 == 0) {
                    updates.add(dateDimRecord(schema, dateSk, 2));
                }
            }
            commit(client, "bulk_insert", rows, null);
            commit(client, "upsert", updates, null);
        }
    }

    private static HoodieRecord<HoodieAvroPayload> dateDimRecord(Schema schema, int dateSk, int version)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put("d_date_sk", dateSk);
        // d_moy and d_year follow the store_sales partition spacing so a d_moy filter selects whole partitions.
        record.put("d_year", year(dateSk));
        record.put("d_moy", moy(dateSk));
        record.put("d_dom", version);
        record.put("__op_ts", (long) version);
        HoodieKey key = new HoodieKey(String.valueOf(dateSk), "");
        return new HoodieAvroRecord<>(key, new HoodieAvroPayload(Option.of(record)), null);
    }

    private static void commit(HoodieJavaWriteClient<HoodieAvroPayload> client, String operation, List<HoodieRecord<HoodieAvroPayload>> records, List<HoodieKey> deletes)
    {
        String instant = client.startCommit();
        List<WriteStatus> statuses = switch (operation) {
            case "bulk_insert" -> client.bulkInsert(records, instant);
            case "upsert" -> client.upsert(records, instant);
            case "delete" -> client.delete(deletes, instant);
            default -> throw new IllegalArgumentException(operation);
        };
        client.commit(instant, statuses);
    }

    private static List<String> sample(List<String> keys, double fraction, Random random)
    {
        List<String> sample = new ArrayList<>();
        for (String key : keys) {
            if (random.nextDouble() < fraction) {
                sample.add(key);
            }
        }
        return sample;
    }

    /**
     * Builds one store_sales row from {@code state = [ss_sold_date_sk, ss_quantity, -]} and stores the generated
     * {@code ss_net_paid} (unscaled, scale 2) in {@code state[2]}.
     */
    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, HoodieKey key, int itemSk, long ticket, long[] state, Random random, long opTs)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put("ss_sold_time_sk", 28_800 + random.nextInt(36_000));
        record.put("ss_item_sk", itemSk);
        record.put("ss_customer_sk", 1 + random.nextInt(12_000_000));
        record.put("ss_store_sk", 1 + random.nextInt(1_000));
        record.put("ss_promo_sk", 1 + random.nextInt(1_500));
        record.put("ss_ticket_number", ticket);
        record.put("ss_quantity", (int) state[1]);
        for (String measure : MEASURES) {
            int unscaled = random.nextInt(2_000_000);
            if (measure.equals("ss_net_paid")) {
                state[2] = unscaled;
            }
            record.put(measure, DECIMAL_CONVERSION.toFixed(BigDecimal.valueOf(unscaled, 2), DECIMAL_7_2, DECIMAL_7_2.getLogicalType()));
        }
        record.put("ss_sold_date_sk", (int) state[0]);
        record.put("__op_ts", opTs);
        return new HoodieAvroRecord<>(key, new HoodieAvroPayload(Option.of(record)), null);
    }

    static Schema storeSalesSchema()
    {
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("store_sales_record").namespace("hoodie.bench").fields()
                .requiredInt("ss_sold_time_sk")
                .requiredInt("ss_item_sk")
                .requiredInt("ss_customer_sk")
                .requiredInt("ss_store_sk")
                .requiredInt("ss_promo_sk")
                .requiredLong("ss_ticket_number")
                .requiredInt("ss_quantity");
        for (String measure : MEASURES) {
            fields = fields.name(measure).type(DECIMAL_7_2).noDefault();
        }
        return fields.requiredInt("ss_sold_date_sk").requiredLong("__op_ts").endRecord();
    }

    private static List<Column> storeSalesColumns()
    {
        ImmutableList.Builder<Column> columns = ImmutableList.<Column>builder()
                .addAll(metaColumns())
                .add(column("ss_sold_time_sk", HIVE_INT))
                .add(column("ss_item_sk", HIVE_INT))
                .add(column("ss_customer_sk", HIVE_INT))
                .add(column("ss_store_sk", HIVE_INT))
                .add(column("ss_promo_sk", HIVE_INT))
                .add(column("ss_ticket_number", HIVE_LONG))
                .add(column("ss_quantity", HIVE_INT));
        for (String measure : MEASURES) {
            columns.add(column(measure, HiveType.valueOf("decimal(7,2)")));
        }
        return columns.add(column("__op_ts", HIVE_LONG)).build();
    }

    private static List<Column> dateDimColumns()
    {
        return ImmutableList.<Column>builder()
                .addAll(metaColumns())
                .add(column("d_date_sk", HIVE_INT))
                .add(column("d_year", HIVE_INT))
                .add(column("d_moy", HIVE_INT))
                .add(column("d_dom", HIVE_INT))
                .add(column("__op_ts", HIVE_LONG))
                .build();
    }

    private static List<Column> metaColumns()
    {
        return List.of(
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

    static void initTable(Path tablePath, String tableName, String recordKeyFields, String partitionFields)
            throws IOException
    {
        HoodieTableMetaClient.newTableBuilder()
                .setTableType(HoodieTableType.MERGE_ON_READ)
                .setTableName(tableName)
                .setTableVersion(Boolean.getBoolean("bench.nativeLogs") ? HoodieTableVersion.TEN : HoodieTableVersion.NINE)
                .setTimelineLayoutVersion(2)
                .setBootstrapIndexClass(NoOpBootstrapIndex.class.getName())
                .setRecordKeyFields(recordKeyFields)
                .setPartitionFields(partitionFields)
                .setKeyGeneratorType(partitionFields.isEmpty() ? "NON_PARTITION" : "COMPLEX")
                .setHiveStylePartitioningEnable(true)
                .setRecordMergeMode(RecordMergeMode.COMMIT_TIME_ORDERING)
                .initTable(new HadoopStorageConfiguration(new Configuration()), tablePath.toUri().toString());
    }

    private static HoodieJavaWriteClient<HoodieAvroPayload> writeClient(Path tablePath, String tableName, Schema schema)
    {
        // Write version 9 writes legacy log files (.log.N with inline data blocks), as a version-9 Spark writer does;
        // -Dbench.nativeLogs=true writes the version-10 native log files (<file>.log.parquet) instead.
        int writeVersion = Boolean.getBoolean("bench.nativeLogs") ? 10 : 9;
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
                .withPath(tablePath.toUri().toString())
                .withWriteTableVersion(writeVersion)
                .withSchema(schema.toString())
                .withParallelism(4, 4)
                .withBulkInsertParallelism(1)
                .withDeleteParallelism(4)
                .forTable(tableName)
                .withRecordMergeMode(RecordMergeMode.COMMIT_TIME_ORDERING)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                        .withInlineCompaction(false)
                        .withMaxNumDeltaCommitsBeforeCompaction(100)
                        .build())
                .withEmbeddedTimelineServerEnabled(false)
                .withMarkersType(MarkerType.DIRECT.name())
                .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
                .build();
        return new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(new HadoopStorageConfiguration(new Configuration())), config);
    }

    private static void registerTable(
            HiveMetastore metastore,
            String schemaName,
            String tableName,
            Location tableLocation,
            List<Column> dataColumns,
            List<Column> partitionColumns,
            List<String> partitionPaths,
            boolean rt)
    {
        StorageFormat storageFormat = StorageFormat.create(
                PARQUET_HIVE_SERDE_CLASS,
                rt ? HUDI_PARQUET_REALTIME_INPUT_FORMAT : HUDI_PARQUET_INPUT_FORMAT,
                MAPRED_PARQUET_OUTPUT_FORMAT_CLASS);
        metastore.createTable(Table.builder()
                        .setDatabaseName(schemaName)
                        .setTableName(tableName)
                        .setTableType(EXTERNAL_TABLE.name())
                        .setOwner(Optional.of("public"))
                        .setDataColumns(dataColumns)
                        .setPartitionColumns(partitionColumns)
                        .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                        .withStorage(storage -> storage.setStorageFormat(storageFormat).setLocation(tableLocation.toString()))
                        .build(),
                PrincipalPrivileges.NO_PRIVILEGES);
        if (partitionPaths.isEmpty()) {
            return;
        }
        List<PartitionWithStatistics> partitions = new ArrayList<>();
        for (String partitionPath : partitionPaths) {
            String value = partitionPath.substring(partitionPath.indexOf('=') + 1);
            partitions.add(new PartitionWithStatistics(
                    Partition.builder()
                            .setDatabaseName(schemaName)
                            .setTableName(tableName)
                            .setValues(List.of(value))
                            .withStorage(storage -> storage.setStorageFormat(storageFormat).setLocation(tableLocation.appendPath(partitionPath).toString()))
                            .setColumns(dataColumns)
                            .build(),
                    partitionPath,
                    PartitionStatistics.empty()));
        }
        metastore.addPartitions(schemaName, tableName, partitions);
    }

    @Override
    public String toString()
    {
        return format("TpcdsMorTables[partitions=%s, rowsPerPartition=%s, updateFraction=%s, deleteFraction=%s]", partitions, rowsPerPartition, updateFraction, deleteFraction);
    }
}
