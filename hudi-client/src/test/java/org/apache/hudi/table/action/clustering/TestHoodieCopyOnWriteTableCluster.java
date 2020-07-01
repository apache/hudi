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

package org.apache.hudi.table.action.clustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieMemoryConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieTestDataGenerator;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieCopyOnWriteTableCluster extends HoodieClientTestHarness {
  private Configuration hadoopConf;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws Exception {
    // Initialize a local spark env
    initSparkContexts("TestHoodieCompactor");

    // Create a temp folder as the base path
    initPath();
    hadoopConf = HoodieTestUtils.getDefaultHadoopConf();
    fs = FSUtils.getFs(basePath, hadoopConf);
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE);
    initTestDataGenerator();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private HoodieWriteConfig getConfig() {
    return getConfigBuilder()
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeClustering(2).compactionSmallFileSize(0).build())
            .build();
  }

  private HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
            .withParallelism(2, 2)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().withInlineClustering(true).compactionSmallFileSize(1024 * 1024)
                    .withInlineCompaction(false).build())
            .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
            .withMemoryConfig(HoodieMemoryConfig.newBuilder().withMaxDFSStreamBufferSize(1 * 1024 * 1024).build())
            .forTable("test-trip-table")
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build());
  }

  @Test
  public void testInlineClustering() {
    HoodieWriteConfig config = getConfig();
    try (HoodieWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 10);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      newCommitTime = "101";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records1 = dataGen.generateInserts(newCommitTime, 10);
      recordsRDD = jsc.parallelize(records1, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      assertEquals(metaClient.getActiveTimeline().getCommitsTimeline().countInstants(), 3);
      assertTrue(metaClient.getActiveTimeline().getCommitsTimeline().getReverseOrderedInstants().findFirst().isPresent());
    }
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.COPY_ON_WRITE;
  }

}
