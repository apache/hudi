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
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner;
import org.apache.hudi.table.storage.HoodieStorageLayout;
import org.apache.hudi.utilities.HoodieCompactor;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;

/**
 * Test cases for {@link HoodieCompactor}.
 */
public class TestHoodieCompactorJob extends HoodieOfflineJobTestBase {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testHoodieCompactorWithOptionalClean(boolean skipClean) throws Exception {
    String tableBasePath = basePath + "/asyncCompaction_" + skipClean;
    Properties props = getPropertiesForKeyGen(true);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .forTable("asyncCompaction")
        .withPath(tableBasePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
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
    metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(props)
        .initTable(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), tableBasePath);

    client = new SparkRDDWriteClient(context, config, true);

    writeData(true, 100, true);
    writeData(true, 100, true);

    // offline compaction schedule
    HoodieCompactor hoodieCompactorSchedule =
        init(tableBasePath, true, "SCHEDULE", skipClean);
    hoodieCompactorSchedule.compact(0);
    TestHelpers.assertNCompletedCommits(2, tableBasePath);
    TestHelpers.assertNCleanCommits(0, tableBasePath);

    writeData(true, 100, true);
    writeData(true, 100, true);

    // offline compaction execute with optional clean
    HoodieCompactor hoodieCompactorExecute =
        init(tableBasePath, false, "EXECUTE", skipClean);
    hoodieCompactorExecute.compact(0);
    TestHelpers.assertNCompletedCommits(5, tableBasePath);
    if (!skipClean) {
      TestHelpers.assertNCleanCommits(1, tableBasePath);
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private HoodieCompactor init(String tableBasePath, boolean runSchedule, String scheduleAndExecute, boolean skipClean) {
    HoodieCompactor.Config compactionConfig = buildCompactionConfig(tableBasePath, runSchedule, scheduleAndExecute, skipClean);
    return new HoodieCompactor(jsc, compactionConfig);
  }

  private HoodieCompactor.Config buildCompactionConfig(String basePath, boolean runSchedule, String runningMode, boolean skipClean) {
    HoodieCompactor.Config config = new HoodieCompactor.Config();
    config.basePath = basePath;
    config.runSchedule = runSchedule;
    config.runningMode = runningMode;
    config.configs.add("hoodie.metadata.enable=false");
    config.skipClean = skipClean;
    config.configs.add(String.format("%s=%s", HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), 1));
    config.configs.add(String.format("%s=%s", HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), 1));
    return config;
  }
}
