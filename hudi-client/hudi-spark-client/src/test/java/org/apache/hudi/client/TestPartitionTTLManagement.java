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

package org.apache.hudi.client;

import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieTTLConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.ttl.strategy.PartitionTTLStrategyType;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;

import com.github.davidmoten.guavamini.Sets;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.getCommitTimeAtUTC;

/**
 * Test Cases for partition ttl management.
 */
public class TestPartitionTTLManagement extends HoodieClientTestBase {

  protected HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .hfileMaxFileSize(1024 * 1024 * 1024).parquetMaxFileSize(1024 * 1024 * 1024).orcMaxFileSize(1024 * 1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build());
  }

  @Test
  public void testKeepByCreationTime() {
    final HoodieWriteConfig cfg = getConfigBuilder()
        .withPath(metaClient.getBasePath())
        .withTTLConfig(HoodieTTLConfig
            .newBuilder()
            .withTTLDaysRetain(10)
            .withTTLStrategyType(PartitionTTLStrategyType.KEEP_BY_CREATION_TIME)
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .build())
        .build();
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEED);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      String partitionPath0 = dataGen.getPartitionPaths()[0];
      String instant0 = getCommitTimeAtUTC(0);
      writeRecordsForPartition(client, dataGen, partitionPath0, instant0);

      String instant1 = getCommitTimeAtUTC(1000);
      String partitionPath1 = dataGen.getPartitionPaths()[1];
      writeRecordsForPartition(client, dataGen, partitionPath1, instant1);

      String currentInstant = WriteClientTestUtils.createNewInstantTime();
      String partitionPath2 = dataGen.getPartitionPaths()[2];
      writeRecordsForPartition(client, dataGen, partitionPath2, currentInstant);

      String instantTime = client.startDeletePartitionCommit(metaClient);
      HoodieWriteResult result = client.managePartitionTTL(instantTime);

      Assertions.assertEquals(Sets.newHashSet(partitionPath0, partitionPath1), result.getPartitionToReplaceFileIds().keySet());
      Assertions.assertEquals(10, readRecords(new String[] {partitionPath0, partitionPath1, partitionPath2}).size());
    }
  }

  @Test
  public void testKeepByTime() {
    final HoodieWriteConfig cfg = getConfigBuilder()
        .withPath(metaClient.getBasePath())
        .withTTLConfig(HoodieTTLConfig
            .newBuilder()
            .withTTLDaysRetain(10)
            .withTTLStrategyType(PartitionTTLStrategyType.KEEP_BY_TIME)
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().build())
        .build();
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEED);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      String partitionPath0 = dataGen.getPartitionPaths()[0];
      String instant0 = getCommitTimeAtUTC(0);
      writeRecordsForPartition(client, dataGen, partitionPath0, instant0);

      String instant1 = getCommitTimeAtUTC(1000);
      String partitionPath1 = dataGen.getPartitionPaths()[1];
      writeRecordsForPartition(client, dataGen, partitionPath1, instant1);

      String currentInstant = WriteClientTestUtils.createNewInstantTime();
      String partitionPath2 = dataGen.getPartitionPaths()[2];
      writeRecordsForPartition(client, dataGen, partitionPath2, currentInstant);

      String instantTime = client.startDeletePartitionCommit();
      HoodieWriteResult result = client.managePartitionTTL(instantTime);
      client.commit(instantTime, result.getWriteStatuses(), Option.empty(), HoodieTimeline.REPLACE_COMMIT_ACTION,
          result.getPartitionToReplaceFileIds(), Option.empty());

      Assertions.assertEquals(Sets.newHashSet(partitionPath0, partitionPath1), result.getPartitionToReplaceFileIds().keySet());

      // remain 10 rows
      Assertions.assertEquals(10, readRecords(new String[] {partitionPath0, partitionPath1, partitionPath2}).size());
    }
  }

  @Test
  public void testInlinePartitionTTL() {
    final HoodieWriteConfig cfg = getConfigBuilder()
        .withPath(metaClient.getBasePath())
        .withTTLConfig(HoodieTTLConfig
            .newBuilder()
            .withTTLDaysRetain(10)
            .withTTLStrategyType(PartitionTTLStrategyType.KEEP_BY_TIME)
            .enableInlinePartitionTTL(true)
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().build())
        .build();
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEED);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      String partitionPath0 = dataGen.getPartitionPaths()[0];
      String instant0 = getCommitTimeAtUTC(0);
      writeRecordsForPartition(client, dataGen, partitionPath0, instant0);

      // All records will be deleted
      Assertions.assertEquals(0, readRecords(new String[] {partitionPath0}).size());

      String instant1 = getCommitTimeAtUTC(1000);
      String partitionPath1 = dataGen.getPartitionPaths()[1];
      writeRecordsForPartition(client, dataGen, partitionPath1, instant1);

      // All records will be deleted
      Assertions.assertEquals(0, readRecords(new String[] {partitionPath1}).size());

      String currentInstant = WriteClientTestUtils.createNewInstantTime();
      String partitionPath2 = dataGen.getPartitionPaths()[2];
      writeRecordsForPartition(client, dataGen, partitionPath2, currentInstant);

      // remain 10 rows
      Assertions.assertEquals(10, readRecords(new String[] {partitionPath2}).size());
    }
  }

  private void writeRecordsForPartition(SparkRDDWriteClient client, HoodieTestDataGenerator dataGen, String partition, String instantTime) {
    List<HoodieRecord> records = dataGen.generateInsertsForPartition(instantTime, 10, partition);
    WriteClientTestUtils.startCommitWithTime(client, instantTime);
    JavaRDD writeStatuses = client.insert(jsc.parallelize(records, 1), instantTime);
    client.commit(instantTime, writeStatuses);
  }

  private List<GenericRecord> readRecords(String[] partitions) {
    return HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(storageConf,
        Arrays.stream(partitions).map(p -> Paths.get(basePath, p).toString()).collect(Collectors.toList()),
        basePath, new JobConf(storageConf.unwrap()), true, true);
  }

}
