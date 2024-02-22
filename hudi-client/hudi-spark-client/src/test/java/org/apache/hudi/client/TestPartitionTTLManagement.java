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

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieTTLConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.ttl.strategy.PartitionTTLStrategyType;
import org.apache.hudi.testutils.HoodieClientTestBase;

import com.github.davidmoten.guavamini.Sets;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.getCommitTimeAtUTC;

/**
 * Test Cases for partition ttl management.
 */
public class TestPartitionTTLManagement extends HoodieClientTestBase {

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit) {
    return HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withAutoCommit(autoCommit)
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
  void testKeepByCreationTime() {
    final HoodieWriteConfig cfg = getConfigBuilder(true)
        .withPath(metaClient.getBasePathV2().toString())
        .withTTLConfig(HoodieTTLConfig
            .newBuilder()
            .withTTLDaysRetain(10)
            .withTTLStrategyType(PartitionTTLStrategyType.KEEP_BY_CREATION_TIME)
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

      String currentInstant = client.createNewInstantTime();
      String partitionPath2 = dataGen.getPartitionPaths()[2];
      writeRecordsForPartition(client, dataGen, partitionPath2, currentInstant);

      HoodieWriteResult result = client.managePartitionTTL(client.createNewInstantTime());

      Assertions.assertEquals(Sets.newHashSet(partitionPath0, partitionPath1), result.getPartitionToReplaceFileIds().keySet());
    }
  }

  @Test
  void testKeepByTime() {
    final HoodieWriteConfig cfg = getConfigBuilder(true)
        .withPath(metaClient.getBasePathV2().toString())
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

      String currentInstant = client.createNewInstantTime();
      String partitionPath2 = dataGen.getPartitionPaths()[2];
      writeRecordsForPartition(client, dataGen, partitionPath2, currentInstant);

      HoodieWriteResult result = client.managePartitionTTL(client.createNewInstantTime());

      Assertions.assertEquals(Sets.newHashSet(partitionPath0, partitionPath1), result.getPartitionToReplaceFileIds().keySet());
    }
  }

  private void writeRecordsForPartition(SparkRDDWriteClient client, HoodieTestDataGenerator dataGen, String partition, String instantTime) {
    List<HoodieRecord> records = dataGen.generateInsertsForPartition(instantTime, 10, partition);
    client.startCommitWithTime(instantTime);
    JavaRDD writeStatuses = client.insert(jsc.parallelize(records, 1), instantTime);
    client.commit(instantTime, writeStatuses);
  }

}
