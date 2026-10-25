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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A delete runs metadata table services while bootstrapping the table, under the table lock. Only the
 * dedicated metadata knob turns that off; the general table services flag alone must not, since writers
 * that set it still rely on this path to compact the metadata table.
 */
public class TestSparkDeletePartitionTableServices extends HoodieClientTestBase {

  private static final String PARTITION = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;

  @Test
  public void leavesTheMetadataTableAloneWhenInlineMetadataServicesAreDisabled() {
    List<HoodieInstant> before = metadataInstantsAfterAnInsert();

    String deleteInstant = deletePartition(false);

    // The delete's own deltacommit is the only change: no clean, compaction or archival.
    List<HoodieInstant> added = metadataInstants().stream()
        .filter(i -> !before.contains(i)).collect(Collectors.toList());
    assertEquals(1, added.size(), "unexpected metadata instants " + added);
    assertEquals(DELTA_COMMIT_ACTION, added.get(0).getAction());
    assertEquals(deleteInstant, added.get(0).requestedTime());
    assertTrue(metadataInstants().containsAll(before), "nothing should have been archived");
  }

  @Test
  public void stillCompactsTheMetadataTableWhenOnlyTableServicesAreDisabled() {
    List<HoodieInstant> before = metadataInstantsAfterAnInsert();

    deletePartition(true);

    // Also proves the setup makes compaction due, so the test above cannot pass vacuously.
    assertTrue(metadataInstants().stream()
            .anyMatch(i -> i.getAction().equals(COMMIT_ACTION) && !before.contains(i)),
        "a compaction was due and should have run");
  }

  private List<HoodieInstant> metadataInstantsAfterAnInsert() {
    try (SparkRDDWriteClient client = getHoodieWriteClient(config(true, true))) {
      String instantTime = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, instantTime);
      List<HoodieRecord> records = dataGen.generateInsertsForPartition(instantTime, 10, PARTITION);
      client.commit(instantTime, client.insert(jsc.parallelize(records, 1), instantTime));
    }
    return metadataInstants();
  }

  private String deletePartition(boolean inlineMetadataServices) {
    try (SparkRDDWriteClient client = getHoodieWriteClient(config(false, inlineMetadataServices))) {
      String instantTime = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, instantTime, REPLACE_COMMIT_ACTION);
      HoodieWriteResult result = client.deletePartitions(Collections.singletonList(PARTITION), instantTime);
      client.commit(instantTime, result.getWriteStatuses(), Option.empty(), REPLACE_COMMIT_ACTION,
          result.getPartitionToReplaceFileIds());
      return instantTime;
    }
  }

  private HoodieWriteConfig config(boolean tableServices, boolean inlineMetadataServices) {
    return getConfigBuilder()
        .withTableServicesEnabled(tableServices)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .withInlineTableServicesEnabled(inlineMetadataServices)
            .build())
        .build();
  }

  private List<HoodieInstant> metadataInstants() {
    return HoodieTestUtils.createMetaClient(storageConf, HoodieTableMetadata.getMetadataTableBasePath(basePath))
        .getActiveTimeline().getInstants();
  }
}
