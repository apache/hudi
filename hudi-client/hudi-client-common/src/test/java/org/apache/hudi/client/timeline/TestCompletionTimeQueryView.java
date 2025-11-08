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
import org.apache.hudi.client.timeline.versioning.v2.LSMTimelineWriter;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.timeline.ActiveAction;
import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.apache.hudi.common.testutils.HoodieTestUtils.convertMetadataToByteArray;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    String tablePath = tempFile.getAbsolutePath() + StoragePath.SEPARATOR + tableName;
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), tablePath, HoodieTableType.COPY_ON_WRITE, tableName);
    prepareTimeline(tablePath, metaClient);
    try (CompletionTimeQueryView view =
             metaClient.getTableFormat().getTimelineFactory().createCompletionTimeQueryView(metaClient)) {
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

  @Test
  void testReadStartTime() throws Exception {
    String tableName = "testTable";
    String tablePath = tempFile.getAbsolutePath() + StoragePath.SEPARATOR + tableName;
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), tablePath, HoodieTableType.COPY_ON_WRITE, tableName);
    prepareTimeline(tablePath, metaClient);
    try (CompletionTimeQueryView view =
             metaClient.getTableFormat().getTimelineFactory().createCompletionTimeQueryView(metaClient)) {
      // query start time from LSM timeline
      assertThat(getInstantTimeSetFormattedString(view, 3 + 1000, 6 + 1000), is("00000003,00000004,00000005,00000006"));
      // query start time from active timeline
      assertThat(getInstantTimeSetFormattedString(view, 7 + 1000, 10 + 1000), is("00000007,00000008,00000009,00000010"));
      // lazy loading
      assertThat(getInstantTimeSetFormattedString(view, 1 + 1000, 2 + 1000), is("00000001,00000002"));
      assertThat("The cursor instant should be slided", view.getCursorInstant(), is(String.format("%08d", 1)));
      // query with partial non-existing completion time
      assertThat(getInstantTimeSetFormattedString(view, 10 + 1000, 11 + 1000), is("00000010"));
      // query with non-existing completion time
      assertThat(getInstantTimeSetFormattedString(view, 12 + 1000, 15 + 1000), is(""));
    }
  }

  @Test
  void testGetInstantTimesWithOnlyEndCompletionTime() throws Exception {
    String tableName = "testTable";
    String tablePath = tempFile.getAbsolutePath() + StoragePath.SEPARATOR + tableName;
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), tablePath, HoodieTableType.COPY_ON_WRITE, tableName);
    prepareTimeline(tablePath, metaClient);
    try (CompletionTimeQueryView view =
             metaClient.getTimelineLayout().getTimelineFactory().createCompletionTimeQueryView(metaClient)) {
      // Fetch instant matching the completion time provided
      assertEquals(Collections.singletonList("00000009"), view.getInstantTimes(metaClient.getActiveTimeline(), Option.empty(), Option.of("00001009"), InstantRange.RangeType.CLOSED_CLOSED));
      // Fetch instant just before the completion time provided
      assertEquals(Collections.singletonList("00000010"), view.getInstantTimes(metaClient.getActiveTimeline(), Option.empty(), Option.of("00001011"), InstantRange.RangeType.CLOSED_CLOSED));
      // Fall back case where only archive instants are before the completion time provided
      assertEquals(Arrays.asList("00000001", "00000002", "00000003", "00000004", "00000005"),
          view.getInstantTimes(metaClient.getActiveTimeline(), Option.empty(), Option.of("00001005"), InstantRange.RangeType.CLOSED_CLOSED));
    }
  }

  @Test
  void testDefaultFirstFetchSize() throws Exception {
    String tableName = "testTable";
    String tablePath = tempFile.getAbsolutePath() + StoragePath.SEPARATOR + tableName;
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), tablePath, HoodieTableType.COPY_ON_WRITE, tableName);
    Instant now = Instant.now();
    List<Pair<String, String>> instantRequestedAndCompletionTime = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String instantTime = HoodieInstantTimeGenerator.getInstantFromTemporalAccessor(now.minus(40 - (i * 4), ChronoUnit.HOURS).atZone(ZoneId.systemDefault()));
      String completionTime = HoodieInstantTimeGenerator.instantTimePlusMillis(instantTime, 1000);
      instantRequestedAndCompletionTime.add(Pair.of(instantTime, completionTime));
    }
    prepareTimeline(tablePath, metaClient, instantRequestedAndCompletionTime, HoodieInstantTimeGenerator.getInstantFromTemporalAccessor(now.atZone(ZoneId.systemDefault())));
    try (CompletionTimeQueryView view =
             metaClient.getTableFormat().getTimelineFactory().createCompletionTimeQueryView(metaClient)) {
      // first completion time is from the active timeline
      assertEquals(instantRequestedAndCompletionTime.get(8).getRight(), view.getCompletionTime(instantRequestedAndCompletionTime.get(8).getLeft()).get());
      // cursor should be the earliest instant in the active timeline
      assertEquals(instantRequestedAndCompletionTime.get(6).getLeft(), view.getCursorInstant());
      // fetch completion time for the first archived instant should update the cursor instant to it.
      assertEquals(instantRequestedAndCompletionTime.get(5).getRight(), view.getCompletionTime(instantRequestedAndCompletionTime.get(5).getLeft()).get());
      assertEquals(instantRequestedAndCompletionTime.get(5).getLeft(), view.getCursorInstant());
    }
  }

  private String getInstantTimeSetFormattedString(CompletionTimeQueryView view, int completionTime1, int completionTime2) {
    return view.getInstantTimes(String.format("%08d", completionTime1), String.format("%08d", completionTime2),
            s -> String.format("%08d", Integer.parseInt(s) - 1000))
        .stream().sorted().collect(Collectors.joining(","));
  }

  private void prepareTimeline(String tablePath, HoodieTableMetaClient metaClient, List<Pair<String, String>> instantRequestedAndCompletionTime, String requestedInstantTime) throws Exception {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
        .withMarkersType("DIRECT")
        .build();
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    List<ActiveAction> activeActions = new ArrayList<>();
    for (Pair<String, String> instantTimePair : instantRequestedAndCompletionTime) {
      String instantTime = instantTimePair.getLeft();
      String completionTime = instantTimePair.getRight();
      HoodieCommitMetadata metadata = testTable.createCommitMetadata(instantTime, WriteOperationType.INSERT, Arrays.asList("par1", "par2"), 10, false);
      testTable.addCommit(instantTime, Option.of(completionTime), Option.of(metadata));
      activeActions.add(
          new DummyActiveAction(
              INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, "commit", instantTime, completionTime),
              convertMetadataToByteArray(metadata)));
    }
    testTable.addRequestedCommit(requestedInstantTime);
    List<HoodieInstant> instants = TIMELINE_FACTORY.createActiveTimeline(metaClient, false).getInstantsAsStream().sorted().collect(Collectors.toList());
    LSMTimelineWriter writer = LSMTimelineWriter.getInstance(writeConfig, getMockHoodieTable(metaClient));
    // archive [1,2], [3,4], [5,6] separately
    writer.write(activeActions.subList(0, 2), Option.empty(), Option.empty());
    writer.write(activeActions.subList(2, 4), Option.empty(), Option.empty());
    writer.write(activeActions.subList(4, 6), Option.empty(), Option.empty());
    // reconcile the active timeline
    instants.subList(0, 3 * 6).forEach(
        instant -> TimelineUtils.deleteInstantFile(metaClient.getStorage(),
            metaClient.getTimelinePath(), instant, INSTANT_FILE_NAME_GENERATOR));
    ValidationUtils.checkState(
        metaClient.reloadActiveTimeline().filterCompletedInstants().countInstants() == 4,
        "should archive 6 instants with 4 as active");
  }

  private void prepareTimeline(String tablePath, HoodieTableMetaClient metaClient) throws Exception {
    List<Pair<String, String>> instantRequestedAndCompletionTime = new ArrayList<>();
    for (int i = 1; i < 11; i++) {
      String instantTime = String.format("%08d", i);
      String completionTime = String.format("%08d", i + 1000);
      instantRequestedAndCompletionTime.add(Pair.of(instantTime, completionTime));
    }
    prepareTimeline(tablePath, metaClient, instantRequestedAndCompletionTime, String.format("%08d", 11));
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
