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

package org.apache.hudi.table.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.apache.hudi.testutils.HoodieClientTestUtils.countRecordsOptionallySince;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestHoodieSparkCopyOnWriteTableArchiveWithReplace extends SparkClientFunctionalTestHarness {

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testDeletePartitionAndArchive(boolean metadataEnabled) throws IOException {
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);
    HoodieWriteConfig writeConfig = getConfigBuilder(true)
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(4, 5).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(metadataEnabled).build())
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig);
         HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(DEFAULT_PARTITION_PATHS)) {

      // 1st write batch; 3 commits for 3 partitions
      String instantTime1 = client.startCommit();
      client.insert(jsc().parallelize(dataGen.generateInsertsForPartition(instantTime1, 10, DEFAULT_FIRST_PARTITION_PATH), 1), instantTime1);
      String instantTime2 = client.startCommit();
      client.insert(jsc().parallelize(dataGen.generateInsertsForPartition(instantTime2, 10, DEFAULT_SECOND_PARTITION_PATH), 1), instantTime2);
      String instantTime3 = client.startCommit();
      client.insert(jsc().parallelize(dataGen.generateInsertsForPartition(instantTime3, 1, DEFAULT_THIRD_PARTITION_PATH), 1), instantTime3);

      final HoodieTimeline timeline1 = metaClient.getCommitsTimeline().filterCompletedInstants();
      assertEquals(21, countRecordsOptionallySince(jsc(), basePath(), sqlContext(), timeline1, Option.empty()));

      // delete the 1st and the 2nd partition; 1 replace commit
      final String instantTime4 = client.startCommit(HoodieActiveTimeline.REPLACE_COMMIT_ACTION);
      client.deletePartitions(Arrays.asList(DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH), instantTime4);

      // 2nd write batch; 6 commits for the 4th partition; the 6th commit to trigger archiving the replace commit
      for (int i = 5; i < 11; i++) {
        String instantTime = client.startCommit();
        client.insert(jsc().parallelize(dataGen.generateInsertsForPartition(instantTime, 1, DEFAULT_THIRD_PARTITION_PATH), 1), instantTime);
      }

      // verify archived timeline
      metaClient = HoodieTableMetaClient.reload(metaClient);
      final HoodieTimeline archivedTimeline = metaClient.getArchivedTimeline();
      assertTrue(archivedTimeline.containsInstant(instantTime1));
      assertTrue(archivedTimeline.containsInstant(instantTime2));
      assertTrue(archivedTimeline.containsInstant(instantTime3));
      assertTrue(archivedTimeline.containsInstant(instantTime4), "should contain the replace commit.");

      // verify records
      final HoodieTimeline timeline2 = metaClient.getCommitTimeline().filterCompletedInstants();
      assertEquals(7, countRecordsOptionallySince(jsc(), basePath(), sqlContext(), timeline2, Option.empty()),
          "should only have the 7 records from the 3rd partition.");
    }
  }
}