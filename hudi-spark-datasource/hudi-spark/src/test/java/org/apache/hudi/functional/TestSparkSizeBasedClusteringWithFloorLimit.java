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

package org.apache.hudi.functional;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.SparkSizeBasedClusteringPlanStrategy;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Functional test for file clustering using SparkSizeBasedClusteringPlanStrategy with floor limit.
 */
public class TestSparkSizeBasedClusteringWithFloorLimit extends HoodieSparkClientTestHarness {

  private HoodieWriteConfig config;
  private HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0);

  public void setup(int maxFileSize, long floorFileLimit, long smallFileLimit) throws IOException {
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initFileSystem();
    Properties props = getPropertiesForKeyGen(true);
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    props.setProperty("hoodie.metadata.enable", "false");
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE, props);
    config = getConfigBuilder().withProps(props)
        .withAutoCommit(false)
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetMaxFileSize(maxFileSize).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(SparkSizeBasedClusteringPlanStrategy.class.getName())
            .withClusteringPlanSmallFileFloorLimit(floorFileLimit)
            .withClusteringPlanSmallFileLimit(smallFileLimit)
            .withClusteringTargetPartitions(3)
            .withClusteringTargetFileMaxBytes(100 * 1024)
            .withClusteringMaxBytesInGroup(200 * 1024)
            .withClusteringPlanPartitionFilterMode(ClusteringPlanPartitionFilterMode.RECENT_DAYS)
            .build())
        .build();

    writeClient = getHoodieWriteClient(config);
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  @Test
  public void testMixedFileSizesWithFloorLimit() throws IOException {
    int maxFileSize = 1 * 1024 * 1024;  // 1MB cap to ensure single file per partition
    long floorFileLimit = 460 * 1024;  // 460KB floor
    long smallFileLimit = 500 * 1024;  // 500KB ceiling
    setup(maxFileSize, floorFileLimit, smallFileLimit);

    // Get 3 partition paths from data generator
    String[] partitions = dataGen.getPartitionPaths();
    String partition1 = partitions[0];  // For files below floor limit which are not in range
    String partition2 = partitions[1];  // For files within eligible range
    String partition3 = partitions[2];  // For files above small file limit which are not in range

    String commitTime1 = HoodieActiveTimeline.createNewInstantTime();
    writeRecordsToPartition(commitTime1, 50, partition1, true);

    String commitTime2 = HoodieActiveTimeline.createNewInstantTime();
    writeRecordsToPartition(commitTime2, 500, partition2, true);

    String commitTime3 = HoodieActiveTimeline.createNewInstantTime();
    writeRecordsToPartition(commitTime3, 5000, partition3, true);

    metaClient = HoodieTableMetaClient.reload(metaClient);

    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    // Collect file sizes per partition (expecting single file per partition)
    long smallFileSize = table.getSliceView().getLatestFileSlices(partition1)
        .map(slice -> slice.getBaseFile().map(HoodieBaseFile::getFileSize).orElse(0L))
        .findFirst()
        .orElse(0L);

    long mediumFileSize = table.getSliceView().getLatestFileSlices(partition2)
        .map(slice -> slice.getBaseFile().map(HoodieBaseFile::getFileSize).orElse(0L))
        .findFirst()
        .orElse(0L);

    long largeFileSize = table.getSliceView().getLatestFileSlices(partition3)
        .map(slice -> slice.getBaseFile().map(HoodieBaseFile::getFileSize).orElse(0L))
        .findFirst()
        .orElse(0L);

    Assertions.assertTrue(smallFileSize < floorFileLimit,
        String.format("Small file (%d bytes) should be below floor limit (%d bytes)", smallFileSize, floorFileLimit));
    Assertions.assertTrue(mediumFileSize >= floorFileLimit && mediumFileSize < smallFileLimit,
        String.format("Medium file (%d bytes) should be within eligible range [%d, %d)", mediumFileSize, floorFileLimit, smallFileLimit));
    Assertions.assertTrue(largeFileSize >= smallFileLimit,
        String.format("Large file (%d bytes) should be above small file limit (%d bytes)", largeFileSize, smallFileLimit));

    // Schedule clustering
    Option<String> clusteringTime = writeClient.scheduleClustering(Option.empty());
    Assertions.assertTrue(clusteringTime.isPresent(), "Clustering should be scheduled");

    // Get clustering plan
    HoodieClusteringPlan plan = ClusteringUtils.getClusteringPlan(
        metaClient, HoodieTimeline.getReplaceCommitRequestedInstant(clusteringTime.get()))
        .map(Pair::getRight).get();

    // Validate that ONLY eligible medium files from partition 2 are in the plan
    List<HoodieClusteringGroup> inputGroups = plan.getInputGroups();
    Assertions.assertTrue(inputGroups.size() >= 1, "Should have at least 1 clustering group");

    // Collect all file paths from clustering plan
    Set<String> clusteringFilePaths = new HashSet<>();
    for (HoodieClusteringGroup group : inputGroups) {
      group.getSlices().forEach(slice -> {
        if (slice.getDataFilePath() != null) {
          clusteringFilePaths.add(slice.getDataFilePath());
        }
      });
    }

    // Verify that only partition2 (medium files) are included
    boolean hasSmallFile = false;
    boolean hasMediumFile = false;
    boolean hasLargeFile = false;

    for (String filePath : clusteringFilePaths) {
      if (filePath.contains(partition1)) {
        hasSmallFile = true;
      } else if (filePath.contains(partition2)) {
        hasMediumFile = true;
      } else if (filePath.contains(partition3)) {
        hasLargeFile = true;
      }
    }

    Assertions.assertFalse(hasSmallFile,
        "Files below floor limit should NOT be included in clustering plan");
    Assertions.assertTrue(hasMediumFile,
        "Files within eligible range SHOULD be included in clustering plan");
    Assertions.assertFalse(hasLargeFile,
        "Files above small file limit should NOT be included in clustering plan");

    // Execute clustering and validate data integrity
    HoodieWriteMetadata writeMetadata = writeClient.cluster(clusteringTime.get(), true);
    List<HoodieWriteStat> writeStats = (List<HoodieWriteStat>) writeMetadata.getWriteStats().get();
    Assertions.assertTrue(writeStats.size() >= 1, "Clustering should produce output files");

    // Verify all records are preserved
    List<Row> rows = readRecords();
    int totalRecords = 50 + 500 + 5000;  // Sum of all written records
    Assertions.assertEquals(totalRecords, rows.size(),
        "All records should be preserved after clustering");
  }

  private List<WriteStatus> writeRecordsToPartition(String commitTime, int numRecords, String partition, boolean doCommit) {
    List<HoodieRecord> records = dataGen.generateInsertsForPartition(commitTime, numRecords, partition);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);  // Force single RDD partition to create one file
    metaClient = HoodieTableMetaClient.reload(metaClient);

    writeClient.startCommitWithTime(commitTime);
    List<WriteStatus> writeStatues = writeClient.insert(writeRecords, commitTime).collect();
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatues);
    if (doCommit) {
      assertTrue(writeClient.commitStats(commitTime, context.parallelize(writeStatues, 1),
          writeStatues.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
          Option.empty(), metaClient.getCommitActionType()));
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatues;
  }

  private List<Row> readRecords() {
    Dataset<Row> roViewDF = sparkSession
        .read()
        .format("hudi")
        .load(basePath);
    roViewDF.createOrReplaceTempView("floor_limit_file_clustering_table");
    return sparkSession.sqlContext().sql("select * from floor_limit_file_clustering_table").collectAsList();
  }

  public HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(1, 1)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .forTable("floor-limit-file-clustering-table")
        .withEmbeddedTimelineServerEnabled(true);
  }
}
