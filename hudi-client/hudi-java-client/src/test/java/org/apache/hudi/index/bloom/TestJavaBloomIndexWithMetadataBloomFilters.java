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

package org.apache.hudi.index.bloom;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.JavaSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.JavaSortAndSizeExecutionStrategy;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Bloom index lookups of the Java client with the bloom filters read from the metadata table.
 */
public class TestJavaBloomIndexWithMetadataBloomFilters extends HoodieJavaClientTestHarness {

  private static final String[] PARTITIONS = {"2020/01/01", "2020/01/02"};

  private HoodieWriteConfig config() {
    return getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.BLOOM, HoodieFailedWritesCleaningPolicy.EAGER)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM)
            .bloomIndexUseMetadata(true).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withMetadataIndexBloomFilter(true)
            .withEngineType(EngineType.JAVA).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(0)
            .insertSplitSize(40).autoTuneInsertSplits(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
            .withClusteringTargetPartitions(0)
            .withClusteringPlanStrategyClass(JavaSizeBasedClusteringPlanStrategy.class.getName())
            .withClusteringExecutionStrategyClass(JavaSortAndSizeExecutionStrategy.class.getName()).build())
        .build();
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  void testUpdatesAreTaggedAfterRewriteCompactionAndClustering(HoodieTableType tableType) throws Exception {
    initMetaClient(tableType);
    HoodieTestDataGenerator generator = new HoodieTestDataGenerator(PARTITIONS);
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(context, config())) {
      String first = client.startCommit();
      List<HoodieRecord> inserts = generator.generateInserts(first, 160);
      List<WriteStatus> inserted = client.insert(inserts, first);
      assertTrue(client.commit(first, inserted));
      Set<String> insertedFileIds = inserted.stream().map(WriteStatus::getFileId).collect(Collectors.toSet());
      assertTrue(insertedFileIds.size() > 1);
      assertTrue(HoodieTableMetaClient.reload(metaClient).getTableConfig().getMetadataPartitions()
          .contains(BLOOM_FILTERS.getPartitionPath()), "The metadata table has the bloom filter partition");

      assertAllUpdated(upsert(client, generator, inserts), inserts.size());
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        Option<String> compaction = client.scheduleCompaction(Option.empty());
        assertTrue(compaction.isPresent());
        client.compact(compaction.get(), true);
      }
      assertAllUpdated(upsert(client, generator, inserts), inserts.size());

      Option<String> clustering = client.scheduleClustering(Option.empty());
      assertTrue(clustering.isPresent());
      client.cluster(clustering.get(), true);
      List<WriteStatus> afterClustering = upsert(client, generator, inserts);
      assertAllUpdated(afterClustering, inserts.size());
      assertTrue(afterClustering.stream().noneMatch(s -> insertedFileIds.contains(s.getFileId())),
          "Updates go to the file groups written by clustering");
    }
  }

  private static List<WriteStatus> upsert(HoodieJavaWriteClient client, HoodieTestDataGenerator generator,
                                          List<HoodieRecord> inserts) throws IOException {
    String instant = client.startCommit();
    List<WriteStatus> statuses = client.upsert(generator.generateUpdates(instant, inserts), instant);
    assertTrue(client.commit(instant, statuses));
    return statuses;
  }

  private static void assertAllUpdated(List<WriteStatus> statuses, int expected) {
    assertFalse(statuses.stream().anyMatch(WriteStatus::hasErrors));
    assertEquals(0, statuses.stream().mapToLong(s -> s.getStat().getNumInserts()).sum(), "No update is written as an insert");
    assertEquals(expected, statuses.stream().mapToLong(s -> s.getStat().getNumUpdateWrites()).sum());
  }
}
