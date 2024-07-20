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
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for {@link HoodieArchivedTimeline}.
 */
public class TestHoodieArchivedTimeline extends HoodieCommonTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  @Test
  public void testLoadingInstantsIncrementally() throws Exception {
    writeArchivedTimeline(10, 10000000);
    // now we got 500 instants spread in 5 parquets.
    HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline("10000043");
    assertThat(archivedTimeline.firstInstant().map(HoodieInstant::getTimestamp).orElse(""), is("10000043"));
    assertThat(archivedTimeline.lastInstant().map(HoodieInstant::getTimestamp).orElse(""), is("10000050"));
    // load incrementally
    archivedTimeline.reload("10000034");
    assertThat(archivedTimeline.firstInstant().map(HoodieInstant::getTimestamp).orElse(""), is("10000034"));
    archivedTimeline.reload("10000011");
    assertThat(archivedTimeline.firstInstant().map(HoodieInstant::getTimestamp).orElse(""), is("10000011"));
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void writeArchivedTimeline(int batchSize, long startTs) throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(this.metaClient);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(this.metaClient.getBasePath())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
        .withMarkersType("DIRECT")
        .build();
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    LSMTimelineWriter writer = LSMTimelineWriter.getInstance(writeConfig, new LocalTaskContextSupplier(), metaClient);
    List<ActiveAction> instantBuffer = new ArrayList<>();
    for (int i = 1; i <= 50; i++) {
      long instantTimeTs = startTs + i;
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
