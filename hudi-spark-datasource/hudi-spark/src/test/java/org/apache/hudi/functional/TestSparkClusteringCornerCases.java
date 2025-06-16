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

package org.apache.hudi.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;

@Tag("functional")
public class TestSparkClusteringCornerCases extends HoodieClientTestBase {
  @Test
  void testClusteringWithEmptyPartitions() throws IOException {
    Properties props = getPropertiesForKeyGen(true);
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder().withProperties(props).build();
    initMetaClient(getTableType(), props);
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String firstInstant = client.createNewInstantTime();
      List<HoodieRecord> recordList = dataGen.generateInserts(firstInstant, 100);
      writeData(client, firstInstant, recordList);
      String secondInstant = client.createNewInstantTime();
      writeData(client, secondInstant, dataGen.generateUpdates(secondInstant, 20));
      // Delete all records.
      writeData(client, client.createNewInstantTime(), dataGen.generateDeletesFromExistingRecords(recordList));
      String clusteringInstantTime = (String) client.scheduleClustering(Option.empty()).get();
      client.cluster(clusteringInstantTime);
      metaClient.reloadActiveTimeline();
      HoodieInstant lastClusteringInstant = metaClient.getActiveTimeline().getLastClusteringInstant().get();
      HoodieReplaceCommitMetadata replaceCommitMetadata =
          metaClient.getActiveTimeline().readReplaceCommitMetadata(lastClusteringInstant);
      Assertions.assertTrue(replaceCommitMetadata.getPartitionToWriteStats().isEmpty());
      Assertions.assertEquals(3, replaceCommitMetadata.getPartitionToReplaceFileIds().size());
    }
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  private List<HoodieRecord> writeData(SparkRDDWriteClient client, String instant, List<HoodieRecord> recordList) {
    JavaRDD records = jsc.parallelize(recordList, 2);
    WriteClientTestUtils.startCommitWithTime(client, instant);
    List<WriteStatus> writeStatuses = client.upsert(records, instant).collect();
    client.commit(instant, jsc.parallelize(writeStatuses), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatuses);
    return recordList;
  }
}

