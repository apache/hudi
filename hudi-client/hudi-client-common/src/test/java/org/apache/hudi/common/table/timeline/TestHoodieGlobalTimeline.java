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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.DummyActiveAction;
import org.apache.hudi.client.timeline.LSMTimelineWriter;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieGlobalTimeline}.
 */
public class TestHoodieGlobalTimeline extends HoodieCommonTestHarness {
  @BeforeEach
  public void setUp() throws Exception {
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  /**
   * The test for checking whether an instant is archived.
   */
  @Test
  void testArchivingCheck() throws Exception {
    writeArchivedTimeline(10, 10000000, 50);
    writeActiveTimeline(10000050, 10);
    HoodieGlobalTimeline globalTimeline = new HoodieGlobalTimeline(this.metaClient, Option.empty());
    assertTrue(globalTimeline.isArchived("10000049"), "The instant should be active");
    assertFalse(globalTimeline.isArchived("10000050"), "The instant should be archived");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "10000049", "10000059", "10000071"})
  void testReloading(String reloadingStartTs) throws Exception {
    writeArchivedTimeline(10, 10000000, 50);
    writeActiveTimeline(10000060, 10);
    // checks some state of the global timeline
    HoodieGlobalTimeline globalTimeline = new HoodieGlobalTimeline(this.metaClient, Option.empty());
    assertTrue(globalTimeline.isArchived("10000059"), "The instant should be archived");
    assertFalse(globalTimeline.isArchived("10000060"), "The instant should be active");
    assertTrue(globalTimeline.containsInstant("10000049"), "Instant should be included");
    // now write some new active and archived instants.
    // write archived: 10000050 ~ 10000059
    writeArchivedTimeline(10, 10000050, 10);
    // write active: 10000070 ~ 10000073
    writeActiveTimeline(10000070, 4);
    if (reloadingStartTs.isEmpty()) {
      globalTimeline = globalTimeline.reload();
    } else {
      globalTimeline = globalTimeline.reload(reloadingStartTs);
    }
    assertTrue(globalTimeline.isArchived("10000059"), "The instant should be archived");
    assertFalse(globalTimeline.isArchived("10000060"), "The instant should be active");
    assertTrue(globalTimeline.containsInstant("10000020"), "Instant should be included");
    assertTrue(globalTimeline.containsInstant("10000073"), "Instant should be included");
  }

  @Test
  void testInstantDetails() throws Exception {
    writeArchivedTimeline(10, 10000000, 50);
    writeActiveTimeline(10000050, 10);
    HoodieGlobalTimeline globalTimeline = new HoodieGlobalTimeline(this.metaClient, Option.empty());
    assertDoesNotThrow(() -> globalTimeline.getInstantDetails(new HoodieInstant(HoodieInstant.State.COMPLETED, "commit", "10000050", "10000060")),
        "Unable to fetch active instant details");
    assertDoesNotThrow(() -> globalTimeline.getInstantDetails(new HoodieInstant(HoodieInstant.State.COMPLETED, "commit", "10000039", "10000049")),
        "Unable to fetch active instant details");
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void writeActiveTimeline(long startTs, int numInstants) throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(this.metaClient);
    for (int i = 0; i < numInstants; i++) {
      testTable.addCommit(String.valueOf(startTs + i), Option.of(String.valueOf(startTs + i + 10)), Option.empty());
    }
  }

  /**
   * Writes {@code numInstants} number of archived instants with given batch size {@code batchSize}.
   */
  private void writeArchivedTimeline(int batchSize, long startTs, int numInstants) throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(this.metaClient);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(this.metaClient.getBasePathV2().toString())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
        .withMarkersType("DIRECT")
        .build();
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(new Configuration());
    LSMTimelineWriter writer = LSMTimelineWriter.getInstance(writeConfig, new LocalTaskContextSupplier(), metaClient);
    List<ActiveAction> instantBuffer = new ArrayList<>();
    for (int i = 1; i <= numInstants; i++) {
      long instantTimeTs = startTs + i - 1;
      String instantTime = String.valueOf(instantTimeTs);
      String completionTime = String.valueOf(instantTimeTs + 10);
      HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED, "commit", instantTime, completionTime);
      HoodieCommitMetadata metadata  = testTable.createCommitMetadata(instantTime, WriteOperationType.INSERT, Arrays.asList("par1", "par2"), 10, false);
      byte[] serializedMetadata = TimelineMetadataUtils.serializeCommitMetadata(metadata).get();
      instantBuffer.add(new DummyActiveAction(instant, serializedMetadata));
      if (i % batchSize == 0) {
        // archive 10 instants each time
        writer.write(instantBuffer, org.apache.hudi.common.util.Option.empty(), org.apache.hudi.common.util.Option.empty());
        writer.compactAndClean(engineContext);
        instantBuffer.clear();
      }
    }
  }
}
