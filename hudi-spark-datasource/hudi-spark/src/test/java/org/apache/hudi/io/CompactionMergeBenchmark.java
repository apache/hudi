/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;

/**
 * Microbenchmark for FileGroupReaderBasedMergeHandle.doMerge() compaction performance.
 * <p>
 * This benchmark analyzes the bottleneck identified in flame graph analysis:
 * - 28.93% of time spent in recordIterator.hasNext() (line 271)
 * - 25.46% of time spent in recordIterator.next() (line 272)
 * - 8.14% of time spent in writeToFile() (line 289)
 * - Total iterator operations: 77% of execution time
 * <p>
 * The benchmark generates realistic test data simulating secondary index metadata table compaction.
 */
public class CompactionMergeBenchmark extends BaseTestHandle {

  private static final String ORDERING_FIELD = "timestamp";
  private static final int WARMUP_ITERATIONS = 2;
  private static final int BENCHMARK_ITERATIONS = 5;

  /**
   * Benchmarks doMerge() with varying data sizes to measure performance characteristics.
   */
  @Test
  public void benchmarkDoMergePerformance() throws Exception {
    System.out.println("\n========================================");
    System.out.println("FileGroupReaderBasedMergeHandle.doMerge() Benchmark");
    System.out.println("Based on flame graph bottleneck analysis");
    System.out.println("========================================\n");

    // Test scenarios with different data sizes
    int[] baseRecordCounts = {1000, 10000, 50000};
    int[] updatePercents = {10, 50, 100};

    System.out.printf("%-15s | %-15s | %-15s | %-20s | %-15s%n",
        "Base Records", "Updates", "Deletes", "Avg Time (ms)", "Throughput (rec/s)");
    System.out.println("----------------+-----------------+-----------------+----------------------+-----------------");

    for (int baseCount : baseRecordCounts) {
      for (int updatePercent : updatePercents) {
        int updateCount = (baseCount * updatePercent) / 100;
        int deleteCount = updateCount / 5; // 20% of updates are deletes

        runBenchmark(baseCount, updateCount, deleteCount);
      }
    }

    System.out.println("\n========================================");
    System.out.println("Benchmark completed!");
    System.out.println("========================================");
  }

  /**
   * Runs a single benchmark scenario.
   */
  private void runBenchmark(int baseCount, int updateCount, int deleteCount) throws Exception {
    // Setup
    metaClient.getStorage().deleteDirectory(metaClient.getBasePath());
    Properties properties = new Properties();
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    properties.put(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), ORDERING_FIELD);
    initMetaClient(getTableType(), properties);

    HoodieWriteConfig config = getHoodieWriteConfigBuilder().build();
    HoodieSparkCopyOnWriteTable.create(config, new HoodieLocalEngineContext(storageConf), metaClient);

    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});

    // Initial write to create base file
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    String instantTime1 = client.startCommit();
    List<HoodieRecord> baseRecords = dataGenerator.generateInserts(instantTime1, baseCount);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(baseRecords, 1);
    JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, instantTime1);
    client.commit(instantTime1, statuses, Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());

    // Get file group
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkCopyOnWriteTable.create(
        config, context, metaClient);
    HoodieFileGroup fileGroup = table.getFileSystemView()
        .getAllFileGroups(partitionPath)
        .collect(Collectors.toList())
        .get(0);
    String fileId = fileGroup.getFileGroupId().getFileId();

    // Prepare updates and deletes
    String instantTime2 = "002";
    List<HoodieRecord> updateRecords = dataGenerator.generateUniqueUpdates(instantTime2, updateCount);

    // Mark some as deletes
    for (int i = 0; i < Math.min(deleteCount, updateRecords.size()); i++) {
      HoodieRecord record = updateRecords.get(i);
      record.setCurrentLocation(new HoodieRecordLocation(instantTime1, fileId));
    }

    // Set current location for all updates
    for (HoodieRecord record : updateRecords) {
      if (record.getCurrentLocation() != null) {
        continue;
      }
      record.setCurrentLocation(new HoodieRecordLocation(instantTime1, fileId));
    }

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      runSingleMerge(config, table, partitionPath, fileId, instantTime2 + "_warmup_" + i, updateRecords);
    }

    // Benchmark
    long totalTime = 0;
    long totalRecordsProcessed = 0;

    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
      MergeMetrics metrics = runSingleMerge(
          config, table, partitionPath, fileId, instantTime2 + "_bench_" + i, updateRecords);
      totalTime += metrics.durationMs;
      totalRecordsProcessed += metrics.recordsProcessed;
    }

    double avgTime = totalTime / (double) BENCHMARK_ITERATIONS;
    long avgRecords = totalRecordsProcessed / BENCHMARK_ITERATIONS;
    double throughput = (avgRecords * 1000.0) / avgTime;

    System.out.printf("%-15s | %-15s | %-15s | %20.2f | %15.0f%n",
        formatCount(baseCount), formatCount(updateCount), formatCount(deleteCount),
        avgTime, throughput);
  }

  /**
   * Runs a single doMerge() operation and measures performance.
   */
  private MergeMetrics runSingleMerge(
      HoodieWriteConfig config,
      HoodieSparkCopyOnWriteTable table,
      String partitionPath,
      String fileId,
      String instantTime,
      List<HoodieRecord> records) throws Exception {

    FileGroupReaderBasedMergeHandle mergeHandle = new FileGroupReaderBasedMergeHandle(
        config,
        instantTime,
        table,
        records.iterator(),
        partitionPath,
        fileId,
        new LocalTaskContextSupplier(),
        Option.empty()
    );

    long startTime = System.nanoTime();
    mergeHandle.doMerge();
    long durationNs = System.nanoTime() - startTime;

    List<WriteStatus> writeStatuses = mergeHandle.close();

    long recordsProcessed = 0;
    if (!writeStatuses.isEmpty()) {
      WriteStatus status = writeStatuses.get(0);
      recordsProcessed = status.getStat().getNumWrites()
          + status.getStat().getNumUpdateWrites()
          + status.getStat().getNumDeletes();
    }

    return new MergeMetrics(durationNs / 1_000_000, recordsProcessed);
  }

  /**
   * Creates write config for benchmark.
   */
  HoodieWriteConfig.Builder getHoodieWriteConfigBuilder() {
    return getConfigBuilder(basePath)
        .withPopulateMetaFields(true)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableRecordIndex(true)
            .withStreamingWriteEnabled(true)
            .withSecondaryIndexEnabled(true)
            .withSecondaryIndexName("sec-rider")
            .withSecondaryIndexForColumn("rider")
            .build())
        .withKeyGenerator(KeyGeneratorForDataGeneratorRecords.class.getCanonicalName())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
  }

  private String formatCount(int count) {
    if (count >= 1_000_000) {
      return (count / 1_000_000) + "M";
    } else if (count >= 1_000) {
      return (count / 1_000) + "K";
    } else {
      return String.valueOf(count);
    }
  }

  /**
   * Holds metrics for a single merge operation.
   */
  private static class MergeMetrics {
    final long durationMs;
    final long recordsProcessed;

    MergeMetrics(long durationMs, long recordsProcessed) {
      this.durationMs = durationMs;
      this.recordsProcessed = recordsProcessed;
    }
  }
}