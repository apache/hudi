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

import io.trino.plugin.hudi.reader.HudiTrinoReaderContext;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.TableState;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hudi.benchmark.TpcdsMorTablesInitializer.DECIMAL_7_2;
import static io.trino.plugin.hudi.benchmark.TpcdsMorTablesInitializer.DECIMAL_CONVERSION;
import static io.trino.plugin.hudi.benchmark.TpcdsMorTablesInitializer.MEASURES;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Micro benchmark for the MOR merge buffer of the Trino Hudi connector: one file group's log records go through
 * the same {@link ExternalSpillableMap} that {@code FileGroupRecordBuffer#initializeRecordsMap} builds (same memory
 * budget and disk map type; spill compression is off), with the size estimator and spill serializer taken from a real
 * {@link HudiTrinoReaderContext}. Changing {@code HudiTrinoReaderContext#getRecordSizeEstimator} or
 * {@code #getRecordSerializer} changes what this measures, with no query engine, cluster or S3 involved.
 *
 * <p>It measures map put and remove only: every key is unique, records are full store_sales width, and no merge
 * step runs. Per round it reports the estimated bytes per record against the merge memory budget, how many entries
 * spilled to disk, the cost of one {@code sizeEstimate} call, and the put and remove cost per record. Every removed
 * record, spilled or not, must equal the record that was put, so a spill serializer that corrupts keys or values
 * fails the run.
 *
 * <p>Run it on JDK 25 without {@code --add-opens}, which is how the Trino server runs (the module's surefire
 * {@code argLine} already matches): the reflection walk of the generic estimator only throws there. The first
 * output line prints the runtime, whether {@code java.base} is open to the test, and the estimator and serializer
 * classes in use.
 * <pre>
 * mvn -Phudi-trino,hudi-trino-tests -pl hudi-trino test -Dtest=HudiMergeBufferBenchmark \
 *     -Dsurefire.failIfNoSpecifiedTests=false -Dbench.logRecords=300000
 * </pre>
 * The default of 300k log records is about one store_sales file group of a 1TB-scale TPC-DS MOR table (about 875k
 * base rows with three 10% update rounds and a 1% delete round).
 */
final class HudiMergeBufferBenchmark
{
    private static final int LOG_RECORDS = Integer.getInteger("bench.logRecords", 300_000);
    private static final int ROUNDS = Integer.getInteger("bench.rounds", 3);

    @Test
    void mergeBuffer()
            throws Exception
    {
        Path tableDir = Files.createTempDirectory("hudi-trino-merge-buffer-bench");
        try {
            Path tablePath = tableDir.resolve("store_sales");
            TpcdsMorTablesInitializer.initTable(tablePath, "store_sales", "ss_item_sk,ss_ticket_number", "ss_sold_date_sk");
            HoodieTableConfig tableConfig = HoodieTableMetaClient.builder()
                    .setConf(new HadoopStorageConfiguration(new Configuration()))
                    .setBasePath(tablePath.toUri().toString())
                    .build()
                    .getTableConfig();
            TypedProperties props = new TypedProperties();
            TypedProperties.putAll(props, tableConfig.getProps());

            HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(HoodieSchema.fromAvroSchema(TpcdsMorTablesInitializer.storeSalesSchema()));
            Schema avroSchema = schema.toAvroSchema();
            HudiTrinoReaderContext readerContext = readerContext(tableConfig, props, schema, tablePath);
            List<BufferedRecord<IndexedRecord>> records = logRecords(readerContext, schema, avroSchema);

            SizeEstimator<BufferedRecord<IndexedRecord>> estimator = readerContext.getRecordSizeEstimator();
            System.out.printf(Locale.ROOT, "runtime=%s java.base opened to unnamed=%s estimator=%s serializer=%s logRecords=%s%n",
                    Runtime.version(), Object.class.getModule().isOpen("java.lang", HudiMergeBufferBenchmark.class.getModule()),
                    estimator.getClass().getName(), readerContext.getRecordSerializer().getClass().getName(), records.size());

            for (int round = 0; round <= ROUNDS; round++) {
                runRound(round == 0 ? "warmup" : "round" + round, readerContext, props, records, tableDir.resolve("spill-" + round));
            }
        }
        finally {
            deleteRecursively(tableDir, ALLOW_INSECURE);
        }
    }

    private static void runRound(String label, HudiTrinoReaderContext readerContext, TypedProperties props, List<BufferedRecord<IndexedRecord>> records, Path spillDir)
            throws Exception
    {
        Files.createDirectories(spillDir);
        SizeEstimator<BufferedRecord<IndexedRecord>> estimator = readerContext.getRecordSizeEstimator();

        long estimateStart = System.nanoTime();
        long estimate = estimator.sizeEstimate(records.getFirst());
        double estimateMicros = (System.nanoTime() - estimateStart) / 1e3;

        // Same keys and defaults as FileGroupRecordBuffer#initializeRecordsMap, except that spill compression is off.
        long maxMemory = props.getLong(MAX_MEMORY_FOR_MERGE.key(), MAX_MEMORY_FOR_MERGE.defaultValue());
        ExternalSpillableMap.DiskMapType diskMapType = ExternalSpillableMap.DiskMapType.valueOf(props.getString(SPILLABLE_DISK_MAP_TYPE.key(),
                SPILLABLE_DISK_MAP_TYPE.defaultValue().name()).toUpperCase(Locale.ROOT));
        try (ExternalSpillableMap<Serializable, BufferedRecord<IndexedRecord>> map = new ExternalSpillableMap<>(
                maxMemory,
                spillDir.toString(),
                new DefaultSizeEstimator<>(),
                readerContext.getRecordSizeEstimator(),
                diskMapType,
                readerContext.getRecordSerializer(),
                false,
                "HudiMergeBufferBenchmark")) {
            long putStart = System.nanoTime();
            for (BufferedRecord<IndexedRecord> record : records) {
                map.put(record.getRecordKey(), record);
            }
            double putNanosPerRecord = (System.nanoTime() - putStart) / (double) records.size();
            int spilled = map.getDiskBasedMapNumEntries();
            assertThat(map.size()).isEqualTo(records.size());

            List<BufferedRecord<IndexedRecord>> removed = new ArrayList<>(records.size());
            long removeStart = System.nanoTime();
            for (BufferedRecord<IndexedRecord> record : records) {
                removed.add(map.remove(record.getRecordKey()));
            }
            double removeNanosPerRecord = (System.nanoTime() - removeStart) / (double) records.size();
            assertThat(map.size()).isZero();

            // Outside the timed section. Spilled entries come back through the spill serializer, so this compares
            // the deserialized key and every payload field against the record that was put.
            for (int i = 0; i < records.size(); i++) {
                BufferedRecord<IndexedRecord> expected = records.get(i);
                BufferedRecord<IndexedRecord> actual = removed.get(i);
                assertThat(actual).as("removed record for key %s", expected.getRecordKey()).isNotNull();
                assertThat(actual.getRecordKey()).isEqualTo(expected.getRecordKey());
                assertThat(actual.getRecord()).as("payload of key %s", expected.getRecordKey()).isEqualTo(expected.getRecord());
                assertThat(actual).isEqualTo(expected);
            }

            System.out.printf(Locale.ROOT,
                    "%-7s estimate=%,d B/record (budget %,d B fits %,d records) sizeEstimate=%.0fus put=%.0fns/record spilled=%,d/%,d remove=%.0fns/record%n",
                    label, estimate, maxMemory, estimate == 0 ? 0 : (long) (maxMemory * 0.8 / estimate), estimateMicros,
                    putNanosPerRecord, spilled, records.size(), removeNanosPerRecord);
        }
    }

    private static HudiTrinoReaderContext readerContext(HoodieTableConfig tableConfig, TypedProperties props, HoodieSchema schema, Path tablePath)
    {
        // Same initialization order as HoodieFileGroupReader for a split with log files.
        HudiTrinoReaderContext readerContext = new HudiTrinoReaderContext(new TrinoStorageConfiguration(), tableConfig, null, List.of(), null, null, props);
        readerContext.setHasLogFiles(true);
        readerContext.getRecordContext().setPartitionPath("ss_sold_date_sk=2451000");
        readerContext.initRecordMerger(props);
        readerContext.setTablePath(tablePath.toUri().toString());
        readerContext.setLatestCommitTime("20260917212448192");
        readerContext.setShouldMergeUseRecordPosition(false);
        readerContext.setHasBootstrapBaseFile(false);
        readerContext.setSchemaHandler(new FileGroupReaderSchemaHandler<>(readerContext, schema, schema, Option.empty(), props,
                TableState.of(new StoragePath(tablePath.toUri().toString()), tableConfig, Option.empty(), Option.empty())));
        return readerContext;
    }

    private static List<BufferedRecord<IndexedRecord>> logRecords(HudiTrinoReaderContext readerContext, HoodieSchema schema, Schema avroSchema)
    {
        Random random = new Random(7);
        List<BufferedRecord<IndexedRecord>> records = new ArrayList<>(LOG_RECORDS);
        for (int i = 0; i < LOG_RECORDS; i++) {
            int itemSk = 1 + random.nextInt(300_000);
            long ticket = 1_000_000L + i;
            String recordKey = "ss_item_sk:" + itemSk + ",ss_ticket_number:" + ticket;
            GenericRecord record = new GenericData.Record(avroSchema);
            record.put("_hoodie_commit_time", "20260917212448192");
            record.put("_hoodie_commit_seqno", "20260917212448192_178_" + i);
            record.put("_hoodie_record_key", recordKey);
            record.put("_hoodie_partition_path", "ss_sold_date_sk=2451000");
            record.put("_hoodie_file_name", "d8d5a7a0-5929-44f5-9b6d-336ddfbb33dc-5_178-2744-518159");
            record.put("ss_sold_time_sk", 28_800 + random.nextInt(36_000));
            record.put("ss_item_sk", itemSk);
            record.put("ss_customer_sk", 1 + random.nextInt(12_000_000));
            record.put("ss_store_sk", 1 + random.nextInt(1_000));
            record.put("ss_promo_sk", 1 + random.nextInt(1_500));
            record.put("ss_ticket_number", ticket);
            record.put("ss_quantity", 1 + random.nextInt(100));
            for (String measure : MEASURES) {
                record.put(measure, DECIMAL_CONVERSION.toFixed(BigDecimal.valueOf(random.nextInt(2_000_000), 2), DECIMAL_7_2, DECIMAL_7_2.getLogicalType()));
            }
            record.put("ss_sold_date_sk", 2451000);
            record.put("__op_ts", 3L);
            records.add(BufferedRecords.fromEngineRecord(record, schema, readerContext.getRecordContext(), OrderingValues.getDefault(), recordKey, false));
        }
        return records;
    }
}
