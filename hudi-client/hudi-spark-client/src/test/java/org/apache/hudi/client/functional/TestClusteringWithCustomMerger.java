/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client.functional;

import org.apache.hudi.DefaultSparkRecordMerger;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduction for HUDI issue #18980: standalone/inline clustering on a MOR table configured with
 * {@code hoodie.write.record.merge.mode=CUSTOM} fails with
 * "No valid spark merger implementation set for `hoodie.write.record.merge.custom.implementation.classes`".
 *
 * <p>Both clustering execution paths in {@code MultipleSparkJobExecutionStrategy} read the source file
 * groups through the same {@code ClusteringExecutionStrategy.getReaderProperties()}/{@code getFileGroupReader},
 * so both reproduce the failure: the row-writer path ({@code readRecordsForGroupAsRow}) and the RDD path
 * ({@code readRecordsForGroup}). Since {@code hoodie.datasource.write.row.writer.enable} defaults to
 * {@code true}, the row-writer path is the one hit with default configs. The test is parameterized over that
 * flag to exercise both.
 */
@Tag("functional")
class TestClusteringWithCustomMerger extends SparkClientFunctionalTestHarness {

  public static class CustomMerger extends DefaultSparkRecordMerger {
    @Override
    public String getMergingStrategy() {
      return HoodieRecordMerger.CUSTOM_MERGE_STRATEGY_UUID;
    }
  }

  @ParameterizedTest(name = "rowWriterEnable={0}")
  @ValueSource(booleans = {true, false})
  void clusteringWithCustomMergerShouldSucceed(boolean rowWriterEnable) throws Exception {
    HoodieWriteConfig cfg = getHoodieWriteConfig(false, rowWriterEnable);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, cfg.getProps());
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      String commit1 = "001";
      WriteClientTestUtils.startCommitWithTime(client, commit1);
      List<HoodieRecord> records = dataGen.generateInserts(commit1, 400);
      Stream<HoodieBaseFile> dataFiles =
          insertRecordsToMORTable(metaClient, records.subList(0, 200), client, cfg, commit1);
      assertTrue(dataFiles.findAny().isPresent(), "should list the base files we wrote in the delta commit");

      String commit2 = "002";
      WriteClientTestUtils.startCommitWithTime(client, commit2);
      dataFiles = insertRecordsToMORTable(metaClient, records.subList(200, 400), client, cfg, commit2);
      assertTrue(dataFiles.findAny().isPresent(), "should list the base files we wrote in the delta commit");

      String commit3 = "003";
      WriteClientTestUtils.startCommitWithTime(client, commit3);
      updateRecordsInMORTable(metaClient, dataGen.generateUpdates(commit3, 100), client, cfg, commit3, false);
    }

    HoodieWriteConfig cfgClustering = getHoodieWriteConfig(true, rowWriterEnable);
    String clusteringCommitTime;
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfgClustering)) {
      clusteringCommitTime = client.scheduleClustering(Option.empty()).get().toString();
      // Issue #18980 surfaces here: the clustering execution strategy reads the source file groups
      // through HoodieFileGroupReader but does not propagate the custom merger impl classes, so
      // initRecordMerger throws "No valid spark merger implementation set".
      client.cluster(clusteringCommitTime, true);
    }

    // Clustering must have produced a completed replace commit at the scheduled instant.
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTimeline timeline = metaClient.getCommitTimeline().filterCompletedInstants();
    HoodieInstant lastInstant = timeline.lastInstant().get();
    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, lastInstant.getAction(),
        "Clustering should complete as a replace commit");
    assertEquals(clusteringCommitTime, lastInstant.requestedTime(),
        "The last commit should be the clustering instant");
    // And all 400 distinct records must still be readable after clustering.
    assertEquals(400, HoodieClientTestUtils.countRecordsOptionallySince(
        jsc(), basePath(), sqlContext(), timeline, Option.empty()),
        "All records should be readable after clustering");
  }

  private HoodieWriteConfig getHoodieWriteConfig(boolean enableInlineClustering, boolean rowWriterEnable) {
    Properties props = new Properties();
    // Selects the clustering execution path: row-writer (the default-true path the issue reporter hit)
    // when true, RDD-based when false. Both paths reach the same getReaderProperties()/getFileGroupReader
    // and reproduce the bug, so the test runs against both.
    props.setProperty("hoodie.datasource.write.row.writer.enable", String.valueOf(rowWriterEnable));
    return HoodieWriteConfig.newBuilder()
        .forTable("test-custom-merger-clustering")
        .withPath(basePath())
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withDeleteParallelism(2)
        .withProps(props)
        .withPreCombineField("timestamp")
        .withRecordMergeMode(RecordMergeMode.CUSTOM)
        .withRecordMergeImplClasses(CustomMerger.class.getName())
        .withRecordMergeStrategyId(HoodieRecordMerger.CUSTOM_MERGE_STRATEGY_UUID)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .compactionSmallFileSize(10L)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .hfileMaxFileSize(1024 * 1024 * 1024)
            .parquetMaxFileSize(1024 * 1024 * 1024).build())
        .withEmbeddedTimelineServerEnabled(true)
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withEnableBackupForRemoteFileSystemView(false).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringMaxNumGroups(10)
            .withClusteringTargetPartitions(0)
            .withInlineClustering(enableInlineClustering)
            .withInlineClusteringNumCommits(1)
            .build())
        .withRollbackUsingMarkers(false)
        .build();
  }
}
