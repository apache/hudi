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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.HoodieClusteringJob;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;

/**
 * Test cases for {@link HoodieClusteringJob}.
 */
public class TestHoodieClusteringJob extends HoodieOfflineJobTestBase {
  @Test
  public void testHoodieClusteringJobWithClean() throws Exception {
    String tableBasePath = basePath + "/asyncClustering";
    Properties props = getPropertiesForKeyGen(true);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .forTable("asyncClustering")
        .withPath(tableBasePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withAutoCommit(false)
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
    props.putAll(config.getProps());
    Properties metaClientProps = HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(props)
        .build();

    metaClient =  HoodieTableMetaClient.initTableAndGetMetaClient(jsc.hadoopConfiguration(), tableBasePath, metaClientProps);
    client = new SparkRDDWriteClient(context, config);

    writeData(false, HoodieActiveTimeline.createNewInstantTime(), 100, true);
    writeData(false, HoodieActiveTimeline.createNewInstantTime(), 100, true);

    // offline clustering execute without clean
    HoodieClusteringJob hoodieCluster =
        init(tableBasePath, true, "scheduleAndExecute", false);
    hoodieCluster.cluster(0);
    HoodieOfflineJobTestBase.TestHelpers.assertNClusteringCommits(1, tableBasePath, fs);
    HoodieOfflineJobTestBase.TestHelpers.assertNCleanCommits(0, tableBasePath, fs);

    writeData(false, HoodieActiveTimeline.createNewInstantTime(), 100, true);
    writeData(false, HoodieActiveTimeline.createNewInstantTime(), 100, true);

    // offline clustering execute with sync clean
    hoodieCluster =
        init(tableBasePath, true, "scheduleAndExecute", true);
    hoodieCluster.cluster(0);
    HoodieOfflineJobTestBase.TestHelpers.assertNClusteringCommits(2, tableBasePath, fs);
    HoodieOfflineJobTestBase.TestHelpers.assertNCleanCommits(1, tableBasePath, fs);
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private HoodieClusteringJob init(String tableBasePath, boolean runSchedule, String scheduleAndExecute, boolean isAutoClean) {
    HoodieClusteringJob.Config clusterConfig = buildHoodieClusteringUtilConfig(tableBasePath, runSchedule, scheduleAndExecute, isAutoClean);
    return new HoodieClusteringJob(jsc, clusterConfig);
  }

  private HoodieClusteringJob.Config  buildHoodieClusteringUtilConfig(String basePath, boolean runSchedule, String runningMode, boolean isAutoClean) {
    HoodieClusteringJob.Config config = new HoodieClusteringJob.Config();
    config.basePath = basePath;
    config.runSchedule = runSchedule;
    config.runningMode = runningMode;
    config.configs.add("hoodie.metadata.enable=false");
    config.configs.add(String.format("%s=%s", HoodieCleanConfig.AUTO_CLEAN.key(), isAutoClean));
    config.configs.add(String.format("%s=%s", HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), 1));
    config.configs.add(String.format("%s=%s", HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), 1));
    return config;
  }
}
