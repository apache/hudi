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

package org.apache.hudi.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.utils.TransactionUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TestTransactionUtils extends HoodieCommonTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    init();
  }

  public void init() throws Exception {
    initPath();
    initMetaClient();
    metaClient.getFs().mkdirs(new Path(basePath));
  }

  @Test
  public void testCheckpointStateMerge() throws IOException {
    HoodieActiveTimeline timeline = new HoodieActiveTimeline(metaClient);

    // Create completed commit with deltastreamer checkpoint state
    HoodieInstant commitInstantWithCheckpointState = new HoodieInstant(
        true,
        HoodieTimeline.COMMIT_ACTION,
        HoodieActiveTimeline.createNewInstantTime()
    );
    timeline.createNewInstant(commitInstantWithCheckpointState);

    HoodieCommitMetadata metadataWithCheckpoint = new HoodieCommitMetadata();
    String checkpointVal = "00001";
    metadataWithCheckpoint.addMetadata(HoodieWriteConfig.DELTASTREAMER_CHECKPOINT_KEY, checkpointVal);
    timeline.saveAsComplete(
        commitInstantWithCheckpointState,
        Option.of(metadataWithCheckpoint.toJsonString().getBytes(StandardCharsets.UTF_8))
    );

    // Inflight commit without checkpoint metadata
    HoodieInstant commitInstantWithoutCheckpointState = new HoodieInstant(
        true,
        HoodieTimeline.COMMIT_ACTION,
        HoodieActiveTimeline.createNewInstantTime()
    );
    timeline.createNewInstant(commitInstantWithoutCheckpointState);
    HoodieCommitMetadata metadataWithoutCheckpoint = new HoodieCommitMetadata();

    // Ensure that checkpoint state is merged in from previous completed commit
    MockTransactionUtils.assertCheckpointStateWasMerged(metaClient, metadataWithoutCheckpoint, checkpointVal);
  }

  private static class MockTransactionUtils extends TransactionUtils {

    public static void assertCheckpointStateWasMerged(
        HoodieTableMetaClient metaClient,
        HoodieCommitMetadata currentMetadata,
        String expectedCheckpointState) {
      TransactionUtils.mergeCheckpointStateFromPreviousCommit(metaClient, Option.of(currentMetadata));
      assertEquals(
          expectedCheckpointState,
          currentMetadata.getExtraMetadata().get(HoodieWriteConfig.DELTASTREAMER_CHECKPOINT_KEY)
      );
    }
  }
}
