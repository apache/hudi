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

package org.apache.hudi.utilities.offlinejob;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.utilities.HoodieClusteringJob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;
import static org.apache.hudi.common.table.HoodieTableMetaClient.TIMELINEFOLDER_NAME;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.utilities.UtilHelpers.PURGE_PENDING_INSTANT;
import static org.apache.hudi.utilities.testutils.UtilitiesTestBase.Helpers.deleteFileFromDfs;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test cases for {@link HoodieClusteringJob}.
 */
public class TestHoodieClusteringJob extends HoodieOfflineJobTestBase {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testHoodieClusteringJobWithClean(boolean skipClean) throws Exception {
    String tableBasePath = basePath + "/asyncClustering_" + skipClean;
    Properties props = getPropertiesForKeyGen(true);
    HoodieWriteConfig config = getWriteConfig(tableBasePath);
    props.putAll(config.getProps());
    metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(props)
        .initTable(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), tableBasePath);

    client = new SparkRDDWriteClient(context, config);

    writeData(false, 100, true);
    writeData(false, 100, true);

    // offline clustering execute without clean
    HoodieClusteringJob hoodieCluster =
        init(tableBasePath, true, "scheduleAndExecute", false, skipClean);
    hoodieCluster.cluster(0);
    HoodieOfflineJobTestBase.TestHelpers.assertNClusteringCommits(1, tableBasePath);
    HoodieOfflineJobTestBase.TestHelpers.assertNCleanCommits(0, tableBasePath);

    writeData(false, 100, true);
    writeData(false, 100, true);

    // offline clustering execute with sync clean
    hoodieCluster =
        init(tableBasePath, true, "scheduleAndExecute", false, skipClean);
    hoodieCluster.cluster(0);
    HoodieOfflineJobTestBase.TestHelpers.assertNClusteringCommits(2, tableBasePath);
    if (!skipClean) {
      HoodieOfflineJobTestBase.TestHelpers.assertNCleanCommits(1, tableBasePath);
    }
  }

  @Test
  public void testPurgePendingInstants() throws Exception {
    String tableBasePath = basePath + "/purgePendingClustering";
    Properties props = getPropertiesForKeyGen(true);
    HoodieWriteConfig config = getWriteConfig(tableBasePath);
    props.putAll(config.getProps());
    metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(props)
        .initTable(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), tableBasePath);
    client = new SparkRDDWriteClient(context, config);

    writeData(false, 100, true);
    writeData(false, 100, true);

    // offline clustering execute without clean
    HoodieClusteringJob hoodieCluster =
        init(tableBasePath, true, "scheduleAndExecute", false, false);
    hoodieCluster.cluster(0);
    HoodieOfflineJobTestBase.TestHelpers.assertNClusteringCommits(1, tableBasePath);
    HoodieOfflineJobTestBase.TestHelpers.assertNCleanCommits(0, tableBasePath);

    // remove the completed instant from timeline and trigger purge of pending clustering instant.
    HoodieInstant latestClusteringInstant = metaClient.getActiveTimeline()
        .filterCompletedInstantsOrRewriteTimeline().getCompletedReplaceTimeline().getInstants().get(0);
    String completedFilePath = tableBasePath + "/" + METAFOLDER_NAME + "/" + TIMELINEFOLDER_NAME + "/" + INSTANT_FILE_NAME_GENERATOR.getFileName(latestClusteringInstant);
    deleteFileFromDfs(fs, completedFilePath);

    // trigger purge.
    hoodieCluster =
        getClusteringConfigForPurge(tableBasePath, true, PURGE_PENDING_INSTANT, latestClusteringInstant.requestedTime());
    hoodieCluster.cluster(0);
    // validate that there are no clustering commits in timeline.
    HoodieOfflineJobTestBase.TestHelpers.assertNClusteringCommits(0, tableBasePath);

    // validate that no records match the clustering instant.
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", tableBasePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(0, HoodieClientTestUtils.read(jsc, tableBasePath, sqlContext, storage, fullPartitionPaths).filter("_hoodie_commit_time = " + latestClusteringInstant.requestedTime()).count(),
        "Must not contain any records w/ clustering instant time");
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private HoodieClusteringJob init(String tableBasePath, boolean runSchedule, String scheduleAndExecute, boolean isAutoClean, boolean skipClean) {
    HoodieClusteringJob.Config clusterConfig = buildHoodieClusteringUtilConfig(tableBasePath, runSchedule, scheduleAndExecute, skipClean);
    clusterConfig.configs.add(String.format("%s=%s", "hoodie.datasource.write.row.writer.enable", "false"));
    return new HoodieClusteringJob(jsc, clusterConfig);
  }

  private HoodieClusteringJob getClusteringConfigForPurge(String tableBasePath, boolean runSchedule, String scheduleAndExecute,
                                                          String pendingInstant) {
    HoodieClusteringJob.Config clusterConfig = buildHoodieClusteringUtilConfig(tableBasePath, runSchedule, scheduleAndExecute, false);
    clusterConfig.configs.add(String.format("%s=%s", "hoodie.datasource.write.row.writer.enable", "false"));
    clusterConfig.clusteringInstantTime = pendingInstant;
    return new HoodieClusteringJob(jsc, clusterConfig);
  }

  private HoodieClusteringJob.Config  buildHoodieClusteringUtilConfig(String basePath, boolean runSchedule, String runningMode,
                                                                      boolean skipClean) {
    HoodieClusteringJob.Config config = new HoodieClusteringJob.Config();
    config.basePath = basePath;
    config.runSchedule = runSchedule;
    config.runningMode = runningMode;
    config.configs.add("hoodie.metadata.enable=false");
    config.skipClean = skipClean;
    config.configs.add(String.format("%s=%s", HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), 1));
    config.configs.add(String.format("%s=%s", HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), 1));
    return config;
  }

  private HoodieWriteConfig getWriteConfig(String tableBasePath) {
    return HoodieWriteConfig.newBuilder()
        .forTable("asyncClustering")
        .withPath(tableBasePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withInlineClustering(false)
            .withScheduleInlineClustering(false)
            .withAsyncClustering(false).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .logFileMaxSize(1024).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .withAutoClean(false).withAsyncClean(false).build())
        .build();
  }

}
