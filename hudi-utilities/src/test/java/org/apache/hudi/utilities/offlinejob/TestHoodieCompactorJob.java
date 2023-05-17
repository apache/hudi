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
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner;
import org.apache.hudi.table.storage.HoodieStorageLayout;
import org.apache.hudi.utilities.HoodieCompactor;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;

/**
 * Test cases for {@link HoodieCompactor}.
 */
public class TestHoodieCompactorJob extends HoodieOfflineJobTestBase {

  @Test
  public void testHoodieCompactorWithClean() throws Exception {
    String tableBasePath = basePath + "/asyncCompaction";
    Properties props = getPropertiesForKeyGen(true);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .forTable("asyncCompaction")
        .withPath(tableBasePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withAutoCommit(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
          .withInlineCompaction(false).withScheduleInlineCompaction(false).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
          .logFileMaxSize(1024).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
          .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
          .withAutoClean(false).withAsyncClean(false).build())
        .withLayoutConfig(HoodieLayoutConfig.newBuilder()
          .withLayoutType(HoodieStorageLayout.LayoutType.BUCKET.name())
          .withLayoutPartitioner(SparkBucketIndexPartitioner.class.getName()).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props).withIndexType(HoodieIndex.IndexType.BUCKET).withBucketNum("1").build())
        .build();
    props.putAll(config.getProps());
    Properties metaClientProps = HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(props)
        .build();

    metaClient =  HoodieTableMetaClient.initTableAndGetMetaClient(jsc.hadoopConfiguration(), tableBasePath, metaClientProps);
    client = new SparkRDDWriteClient(context, config);

    writeData(true, HoodieActiveTimeline.createNewInstantTime(), 100, true);
    writeData(true, HoodieActiveTimeline.createNewInstantTime(), 100, true);

    // offline compaction schedule
    HoodieCompactor hoodieCompactorSchedule =
        init(tableBasePath, true, "SCHEDULE", false);
    hoodieCompactorSchedule.compact(0);
    TestHelpers.assertNCompletedCommits(2, tableBasePath, fs);
    TestHelpers.assertNCleanCommits(0, tableBasePath, fs);

    writeData(true, HoodieActiveTimeline.createNewInstantTime(), 100, true);
    writeData(true, HoodieActiveTimeline.createNewInstantTime(), 100, true);

    // offline compaction execute with sync clean
    HoodieCompactor hoodieCompactorExecute =
        init(tableBasePath, false, "EXECUTE", true);
    hoodieCompactorExecute.compact(0);
    TestHelpers.assertNCompletedCommits(5, tableBasePath, fs);
    TestHelpers.assertNCleanCommits(1, tableBasePath, fs);
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private HoodieCompactor init(String tableBasePath, boolean runSchedule, String scheduleAndExecute, boolean isAutoClean) {
    HoodieCompactor.Config compactionConfig = buildCompactionConfig(tableBasePath, runSchedule, scheduleAndExecute, isAutoClean);
    return new HoodieCompactor(jsc, compactionConfig);
  }

  private HoodieCompactor.Config buildCompactionConfig(String basePath, boolean runSchedule, String runningMode, boolean isAutoClean) {
    HoodieCompactor.Config config = new HoodieCompactor.Config();
    config.basePath = basePath;
    config.runSchedule = runSchedule;
    config.runningMode = runningMode;
    config.configs.add("hoodie.metadata.enable=false");
    config.configs.add(String.format("%s=%s", HoodieCleanConfig.AUTO_CLEAN.key(), isAutoClean));
    config.configs.add(String.format("%s=%s", HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), 1));
    config.configs.add(String.format("%s=%s", HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), 1));
    return config;
  }
}
