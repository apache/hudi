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

package org.apache.hudi;

import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestMetadataTableSupport extends HoodieSparkClientTestBase {
  @BeforeEach
  void start() throws Exception {
    super.setUp();
  }

  @AfterEach
  void end() throws Exception {
    super.tearDown();
  }

  @Test
  void testRecreateMDTForInsertOverwriteTableOperation() {
    HoodieWriteConfig config = getConfigBuilder()
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableRecordIndex(true).build())
        .build();

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      // Insert first batch.
      String timestamp0 = "20241015000000000";
      List<HoodieRecord> records0 = dataGen.generateInserts(timestamp0, 100);
      JavaRDD<HoodieRecord> dataset0 = jsc.parallelize(records0, 2);

      WriteClientTestUtils.startCommitWithTime(writeClient, timestamp0);
      writeClient.commit(timestamp0, writeClient.insert(dataset0, timestamp0));

      // Confirm MDT enabled.
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertTrue(metaClient.getTableConfig().isMetadataTableAvailable());

      // Confirm the instant for the first insert exists.
      StoragePath mdtBasePath =
          HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath());
      HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder()
          .setConf(storageConf.newInstance())
          .setBasePath(mdtBasePath).build();
      HoodieActiveTimeline timeline = mdtMetaClient.getActiveTimeline();
      List<HoodieInstant> instants = timeline.getInstants();
      assertEquals(5, instants.size());
      // For MDT bootstrap instant.
      assertEquals("00000000000000000", instants.get(0).requestedTime());
      // For col stats bootstrap instant.
      assertEquals("00000000000000001", instants.get(1).requestedTime());
      // For RLI bootstrap instant.
      assertEquals("00000000000000002", instants.get(2).requestedTime());
      // For partitions stats bootstrap instant.
      assertEquals("00000000000000003", instants.get(3).requestedTime());
      // For the insert instant.
      assertEquals(timestamp0, instants.get(4).requestedTime());

      // Insert second batch.
      String timestamp1 = writeClient.startCommit(REPLACE_COMMIT_ACTION);
      List<HoodieRecord> records1 = dataGen.generateInserts(timestamp1, 50);
      JavaRDD<HoodieRecord> dataset1 = jsc.parallelize(records1, 2);

      HoodieWriteResult writeResult = writeClient.insertOverwriteTable(dataset1, timestamp1);
      writeClient.commit(timestamp1, writeResult.getWriteStatuses(), Option.empty(), REPLACE_COMMIT_ACTION, writeResult.getPartitionToReplaceFileIds(), Option.empty());

      // Validate.
      mdtMetaClient = HoodieTableMetaClient.reload(mdtMetaClient);
      timeline = mdtMetaClient.getActiveTimeline();
      instants = timeline.getInstants();
      assertEquals(6, timeline.getInstants().size());
      // For MDT bootstrap instant.
      assertEquals("00000000000000000", instants.get(0).requestedTime());
      // For col stats bootstrap instant.
      assertEquals("00000000000000001", instants.get(1).requestedTime());
      // For RLI bootstrap instant.
      assertEquals("00000000000000002", instants.get(2).requestedTime());
      // For partitions stats bootstrap instant.
      assertEquals("00000000000000003", instants.get(3).requestedTime());
      // For the insert_overwrite_table instant.
      assertEquals(timestamp1, instants.get(5).requestedTime());
    }
  }
}
