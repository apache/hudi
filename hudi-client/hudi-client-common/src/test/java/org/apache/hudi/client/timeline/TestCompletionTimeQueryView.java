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

package org.apache.hudi.client.timeline;

import org.apache.hudi.DummyActiveAction;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ActiveAction;
import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeCommitMetadata;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link CompletionTimeQueryView}.
 */
public class TestCompletionTimeQueryView {
  @TempDir
  File tempFile;

  @Test
  void testReadCompletionTime() throws Exception {
    String tableName = "testTable";
    String tablePath = tempFile.getAbsolutePath() + Path.SEPARATOR + tableName;
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(new Configuration(), tablePath, HoodieTableType.COPY_ON_WRITE, tableName);
    prepareTimeline(tablePath, metaClient);
    try (CompletionTimeQueryView view = new CompletionTimeQueryView(metaClient, String.format("%08d", 3))) {
      // query completion time from LSM timeline
      for (int i = 3; i < 7; i++) {
        assertThat(view.getCompletionTime(String.format("%08d", i)).orElse(""), is(String.format("%08d", i + 1000)));
      }
      // query completion time from active timeline
      for (int i = 7; i < 11; i++) {
        assertTrue(view.getCompletionTime(String.format("%08d", i)).isPresent());
      }
      // lazy loading
      for (int i = 1; i < 3; i++) {
        assertThat(view.getCompletionTime(String.format("%08d", i)).orElse(""), is(String.format("%08d", i + 1000)));
      }
      assertThat("The cursor instant should be slided", view.getCursorInstant(), is(String.format("%08d", 1)));
      // query with inflight start time
      assertFalse(view.getCompletionTime(String.format("%08d", 11)).isPresent());
      // query with non-exist start time
      assertFalse(view.getCompletionTime(String.format("%08d", 12)).isPresent());
      // test with invalid base instant time
      assertThat(view.getCompletionTime("111", String.format("%08d", 3)).orElse(""), is(String.format("%08d", 3)));
    }
  }

  private void prepareTimeline(String tablePath, HoodieTableMetaClient metaClient) throws Exception {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
        .withMarkersType("DIRECT")
        .build();
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    List<ActiveAction> activeActions = new ArrayList<>();
    for (int i = 1; i < 11; i++) {
      String instantTime = String.format("%08d", i);
      String completionTime = String.format("%08d", i + 1000);
      HoodieCommitMetadata metadata = testTable.createCommitMetadata(instantTime, WriteOperationType.INSERT, Arrays.asList("par1", "par2"), 10, false);
      testTable.addCommit(instantTime, Option.of(metadata));
      activeActions.add(
          new DummyActiveAction(
              new HoodieInstant(HoodieInstant.State.COMPLETED, "commit", instantTime, completionTime),
              serializeCommitMetadata(metadata).get()));
    }
    testTable.addRequestedCommit(String.format("%08d", 11));
    List<HoodieInstant> instants = new HoodieActiveTimeline(metaClient, false).getInstantsAsStream().sorted().collect(Collectors.toList());
    LSMTimelineWriter writer = LSMTimelineWriter.getInstance(writeConfig, getMockHoodieTable(metaClient));
    // archive [1,2], [3,4], [5,6] separately
    writer.write(activeActions.subList(0, 2), Option.empty(), Option.empty());
    writer.write(activeActions.subList(2, 4), Option.empty(), Option.empty());
    writer.write(activeActions.subList(4, 6), Option.empty(), Option.empty());
    // reconcile the active timeline
    instants.subList(0, 3 * 6).forEach(instant -> HoodieActiveTimeline.deleteInstantFile(metaClient.getFs(), metaClient.getMetaPath(), instant));
    ValidationUtils.checkState(metaClient.reloadActiveTimeline().filterCompletedInstants().countInstants() == 4, "should archive 6 instants with 4 as active");
  }

  @SuppressWarnings("rawtypes")
  private HoodieTable getMockHoodieTable(HoodieTableMetaClient metaClient) {
    HoodieTable hoodieTable = mock(HoodieTable.class);
    TaskContextSupplier taskContextSupplier = mock(TaskContextSupplier.class);
    when(taskContextSupplier.getPartitionIdSupplier()).thenReturn(() -> 1);
    when(hoodieTable.getTaskContextSupplier()).thenReturn(taskContextSupplier);
    when(hoodieTable.getMetaClient()).thenReturn(metaClient);
    return hoodieTable;
  }
}
