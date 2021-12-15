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
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Collections;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
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
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(2, 3).retainCommits(1).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(metadataEnabled).build())
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig);
         HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {DEFAULT_FIRST_PARTITION_PATH})) {

      // 1st write batch; 3 commits
      for (int i = 0; i < 3; i++) {
        String instantTime = HoodieActiveTimeline.createNewInstantTime(i * 1000);
        client.startCommitWithTime(instantTime);
        client.insert(jsc().parallelize(dataGen.generateInserts(instantTime, 10), 1), instantTime);
      }

      // delete the only partition; 1 replace commit
      final String instantTime3 = HoodieActiveTimeline.createNewInstantTime(3000);
      client.startCommitWithTime(instantTime3, HoodieActiveTimeline.REPLACE_COMMIT_ACTION);
      client.deletePartitions(Collections.singletonList(DEFAULT_FIRST_PARTITION_PATH), instantTime3);

      // 2nd write batch; 3 commits; the 3rd commit to trigger archiving the replace commit
      for (int i = 4; i < 7; i++) {
        String instantTime = HoodieActiveTimeline.createNewInstantTime(i * 1000);
        client.startCommitWithTime(instantTime);
        client.insert(jsc().parallelize(dataGen.generateInserts(instantTime, 1), 1), instantTime);
      }

      // verify archived timeline
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertTrue(metaClient.getArchivedTimeline().containsInstant(instantTime3),
          "should contain the replace commit.");

      // verify records
      HoodieTimeline timeline = metaClient.getCommitsTimeline().filterCompletedInstants();
      assertEquals(3, countRecordsOptionallySince(jsc(), basePath(), sqlContext(), timeline, Option.empty()),
          "should only have the records from the 2nd write batch.");
    }
  }
}
