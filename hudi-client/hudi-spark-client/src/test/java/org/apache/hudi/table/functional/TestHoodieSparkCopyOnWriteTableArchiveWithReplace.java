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
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
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

  public String basePath() {
    return "file:///home/balaji/hudi_0_15_table/";
  }

  @ParameterizedTest
  @ValueSource(booleans = {false})
  public void testDeletePartitionAndArchive(boolean metadataEnabled) throws IOException {
    HoodieTableMetaClient metaClient =  HoodieTableMetaClient.builder().setConf(new HoodieSparkEngineContext(jsc(), sqlContext()).getStorageConf())
        .setBasePath(basePath())
        .build();
    HoodieWriteConfig writeConfig = getConfigBuilder(true).withWriteTableVersion(6)
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(4, 5).build())
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(metadataEnabled).build())
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig);
         HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(DEFAULT_PARTITION_PATHS)) {

      // 1st write batch; 3 commits for 3 partitions
      String instantTime1 = client.createNewInstantTime(1000);
      client.startCommitWithTime(instantTime1);
      client.insert(jsc().parallelize(dataGen.generateInsertsForPartition(instantTime1, 10, DEFAULT_FIRST_PARTITION_PATH), 1), instantTime1);
      String instantTime2 = client.createNewInstantTime(2000);
      client.startCommitWithTime(instantTime2);
      client.insert(jsc().parallelize(dataGen.generateInsertsForPartition(instantTime2, 10, DEFAULT_SECOND_PARTITION_PATH), 1), instantTime2);
      String instantTime3 = client.createNewInstantTime(3000);
      client.startCommitWithTime(instantTime3);
      client.insert(jsc().parallelize(dataGen.generateInsertsForPartition(instantTime3, 1, DEFAULT_THIRD_PARTITION_PATH), 1), instantTime3);

      final HoodieTimeline timeline1 = metaClient.getCommitsTimeline().filterCompletedInstants();
      assertEquals(21, countRecordsOptionallySince(jsc(), basePath(), sqlContext(), timeline1, Option.empty()));

      // delete the 1st and the 2nd partition; 1 replace commit
      final String instantTime4 = client.createNewInstantTime(4000);
      client.startCommitWithTime(instantTime4, HoodieActiveTimeline.REPLACE_COMMIT_ACTION);
      client.deletePartitions(Arrays.asList(DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH), instantTime4);

      // 2nd write batch; 6 commits for the 4th partition; the 6th commit to trigger archiving the replace commit
      for (int i = 5; i < 11; i++) {
        String instantTime = client.createNewInstantTime(i * 1000);
        client.startCommitWithTime(instantTime);
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
