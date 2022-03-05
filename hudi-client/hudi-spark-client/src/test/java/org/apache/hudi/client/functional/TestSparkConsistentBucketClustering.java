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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.SparkConsistentBucketClusteringPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.SparkConsistentBucketClusteringExecutionStrategy;
import org.apache.hudi.client.clustering.update.strategy.SparkDuplicateUpdateStrategy;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.HoodieSparkConsistentBucketIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieClusteringConfig.DAYBASED_LOOKBACK_PARTITIONS;
import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_PARTITION_FILTER_MODE;
import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST;

@Tag("functional")
public class TestSparkConsistentBucketClustering extends HoodieClientTestHarness {

  private final Random random = new Random();
  private HoodieIndex index;
  private HoodieWriteConfig config;
  private HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

  public void setup(int maxFileSize) throws IOException {
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initFileSystem();
    Properties props = getPropertiesForKeyGen();
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ, props);
    config = getConfigBuilder().withProps(props)
        .withAutoCommit(false)
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.BUCKET).withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING)
            .withBucketNum("8").withBucketMaxNum(14).withBucketMinNum(4).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetMaxFileSize(maxFileSize).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(SparkConsistentBucketClusteringPlanStrategy.class.getName())
            .withClusteringExecutionStrategyClass(SparkConsistentBucketClusteringExecutionStrategy.class.getName())
            .withClusteringUpdatesStrategy(SparkDuplicateUpdateStrategy.class.getName()).build())
        .build();

    writeClient = getHoodieWriteClient(config);
    index = writeClient.getIndex();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  /**
   * 1. Test resizing with bucket number upper bound and lower bound
   * TODO 2. Test resizing with different sort mode enabled
   *
   * @throws IOException
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testResizing(boolean isSplit) throws IOException {
    final int maxFileSize = isSplit ? 5120 : 128 * 1024 * 1024;
    final int targetBucketNum = isSplit ? 14 : 4;
    setup(maxFileSize);
    writeData(HoodieActiveTimeline.createNewInstantTime(), 2000, true);
    String clusteringTime = (String) writeClient.scheduleClustering(Option.empty()).get();
    writeClient.cluster(clusteringTime, true);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    final HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    Assertions.assertEquals(2000, readRecords(dataGen.getPartitionPaths()).size());

    Arrays.stream(dataGen.getPartitionPaths()).forEach(p -> {
      HoodieConsistentHashingMetadata metadata = HoodieSparkConsistentBucketIndex.loadMetadata(table, p);
      Assertions.assertEquals(targetBucketNum, metadata.getNodes().size());

      // The file slice has no log files
      table.getSliceView().getLatestFileSlices(p).forEach(fs -> {
        Assertions.assertTrue(fs.getBaseFile().isPresent());
        Assertions.assertTrue(fs.getLogFiles().count() == 0);
      });
    });
  }

  /**
   * Only one clustering job is allowed on each partition
   */
  @Test
  public void testConcurrentClustering() throws IOException {
    setup(5120);
    writeData(HoodieActiveTimeline.createNewInstantTime(), 2000, true);
    String clusteringTime = (String) writeClient.scheduleClustering(Option.empty()).get();
    // Schedule again, it should not be scheduled as the previous one are doing clustering to all partition
    Assertions.assertFalse(writeClient.scheduleClustering(Option.empty()).isPresent());
    writeClient.cluster(clusteringTime, true);

    // Schedule two clustering, each working on a single partition
    config.setValue(DAYBASED_LOOKBACK_PARTITIONS, "1");
    config.setValue(PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST, "0");
    config.setValue(PLAN_PARTITION_FILTER_MODE, ClusteringPlanPartitionFilterMode.RECENT_DAYS.toString());
    Assertions.assertTrue(writeClient.scheduleClustering(Option.empty()).isPresent());
    config.setValue(DAYBASED_LOOKBACK_PARTITIONS, "1");
    config.setValue(PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST, "1");
    Assertions.assertTrue(writeClient.scheduleClustering(Option.empty()).isPresent());
  }

  /**
   * 1. If there is any ongoing writing, cannot schedule clustering
   * 2. If the clustering is scheduled, it cannot block incoming new writers
   */
  @Test
  public void testConcurrentWrite() throws IOException {
    setup(5120);
    String writeTime = HoodieActiveTimeline.createNewInstantTime();
    List<WriteStatus> writeStatues = writeData(writeTime, 2000, false);
    // Cannot schedule clustering if there is in-flight writer
    Assertions.assertFalse(writeClient.scheduleClustering(Option.empty()).isPresent());
    Assertions.assertTrue(writeClient.commitStats(writeTime, writeStatues.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType()));
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Schedule clustering
    String clusteringTime = (String) writeClient.scheduleClustering(Option.empty()).get();
    // Concurrent is not blocked by the clustering
    writeData(HoodieActiveTimeline.createNewInstantTime(), 2000, true);
    // The records are immediately visible when the writer completes
    Assertions.assertEquals(4000, readRecords(dataGen.getPartitionPaths()).size());
    // Clustering finished, check the number of records (there will be file group switch in the background)
    writeClient.cluster(clusteringTime, true);
    Assertions.assertEquals(4000, readRecords(dataGen.getPartitionPaths()).size());
  }

  private List<GenericRecord> readRecords(String[] partitions) {
    return HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf,
        Arrays.stream(partitions).map(p -> Paths.get(basePath, p).toString()).collect(Collectors.toList()),
        basePath);
  }

  /**
   * Insert `num` records into table given the commitTime
   *
   * @param commitTime
   * @param totalRecords
   */
  private List<WriteStatus> writeData(String commitTime, int totalRecords, boolean doCommit) {
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, totalRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 2);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    writeClient.startCommitWithTime(commitTime);
    List<WriteStatus> writeStatues = writeClient.upsert(writeRecords, commitTime).collect();
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatues);
    if (doCommit) {
      Assertions.assertTrue(writeClient.commitStats(commitTime, writeStatues.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
          Option.empty(), metaClient.getCommitActionType()));
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatues;
  }

  private HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build());
  }
}
