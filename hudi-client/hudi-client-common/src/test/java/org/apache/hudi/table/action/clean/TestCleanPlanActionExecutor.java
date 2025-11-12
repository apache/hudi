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

package org.apache.hudi.table.action.clean;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestCleanPlanActionExecutor {
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void emptyCompletedCleanReturnsPreviousCleanPlan(boolean isEmptyPlan) throws IOException {
    HoodieTable table = mock(HoodieTable.class, RETURNS_DEEP_STUBS);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(table.getMetaClient()).thenReturn(metaClient);
    InstantGenerator instantGenerator = mock(InstantGenerator.class);
    when(table.getInstantGenerator()).thenReturn(instantGenerator);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);

    // allow clean to trigger
    mockThatCleanIsRequired(table);

    // signal that last clean commit is just an empty file
    HoodieInstant lastCompletedInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, "clean", "001", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    HoodieInstant lastRequestInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, "clean", "001", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    HoodieInstant lastInflightInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, "clean", "001", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    when(instantGenerator.getCleanRequestedInstant("001")).thenReturn(lastRequestInstant);
    mockEmptyLastCompletedClean(table, lastCompletedInstant, activeTimeline, isEmptyPlan);

    HoodieEngineContext engineContext = new HoodieLocalEngineContext(new HadoopStorageConfiguration(false));
    CleanPlanActionExecutor<?, ?, ?, ?> executor = spy(new CleanPlanActionExecutor<>(engineContext, HoodieWriteConfig.newBuilder().withPath("file://tmp").build(), table, Option.empty()));
    HoodieCleanerPlan cleanerPlan = HoodieCleanerPlan.newBuilder()
        .setEarliestInstantToRetain(HoodieActionInstant.newBuilder().setAction(HoodieTimeline.COMMIT_ACTION).setTimestamp("001").setState(HoodieInstant.State.COMPLETED.name()).build())
        .setFilesToBeDeletedPerPartition(Collections.singletonMap("partition1", Collections.singletonList("file1")))
        .setFilePathsToBeDeletedPerPartition(Collections.singletonMap("partition1", Collections.singletonList(HoodieCleanFileInfo.newBuilder().setFilePath("file1").build())))
        .setLastCompletedCommitTimestamp("002")
        .setPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name())
        .setVersion(TimelineLayoutVersion.CURR_VERSION)
        .build();
    if (isEmptyPlan) {
      // empty plan will be removed and new plan will be requested
      when(activeTimeline.readCleanerPlan(lastRequestInstant)).thenThrow(new IOException("Empty plan"));
      when(activeTimeline.isEmpty(lastRequestInstant)).thenReturn(true);
      // inflight instant will be removed as well
      when(instantGenerator.getCleanInflightInstant("001")).thenReturn(lastInflightInstant);
      doReturn(cleanerPlan).when(executor).requestClean(engineContext);
    } else {
      when(activeTimeline.readCleanerPlan(lastRequestInstant)).thenReturn(cleanerPlan);
    }

    Option<HoodieCleanerPlan> actualPlan = executor.requestClean();
    assertEquals(Option.of(cleanerPlan), actualPlan);
    verify(activeTimeline).deleteEmptyInstantIfExists(lastCompletedInstant);
    if (isEmptyPlan) {
      verify(metaClient).reloadActiveTimeline();
      verify(activeTimeline).deleteEmptyInstantIfExists(lastRequestInstant);
      verify(activeTimeline).deleteEmptyInstantIfExists(lastInflightInstant);
    }
  }

  @Test
  void emptyCompletedClean_failsToReadPreviousPlan() throws IOException {
    HoodieTable table = mock(HoodieTable.class, RETURNS_DEEP_STUBS);
    InstantGenerator instantGenerator = mock(InstantGenerator.class);
    when(table.getInstantGenerator()).thenReturn(instantGenerator);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);

    // allow clean to trigger
    mockThatCleanIsRequired(table);

    // signal that last clean commit is just an empty file
    HoodieInstant lastCompletedInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, "clean", "001", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    HoodieInstant lastRequestInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, "clean", "001", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    when(instantGenerator.getCleanRequestedInstant("001")).thenReturn(lastRequestInstant);
    mockEmptyLastCompletedClean(table, lastCompletedInstant, activeTimeline, false);

    when(activeTimeline.readCleanerPlan(lastRequestInstant)).thenThrow(new HoodieIOException("failed to read"));

    HoodieEngineContext engineContext = new HoodieLocalEngineContext(new HadoopStorageConfiguration(false));
    CleanPlanActionExecutor<?, ?, ?, ?> executor = new CleanPlanActionExecutor<>(engineContext, HoodieWriteConfig.newBuilder().withPath("file://tmp").build(), table, Option.empty());

    assertThrows(HoodieIOException.class, executor::requestClean);
  }

  @Test
  void lastCleanIsNonEmpty() {
    HoodieTable table = mock(HoodieTable.class, RETURNS_DEEP_STUBS);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);

    // allow clean to trigger
    mockThatCleanIsRequired(table);

    // signal that last clean commit is just an empty file
    HoodieInstant lastCompletedInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, "clean", "001", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    when(table.getCleanTimeline().filterCompletedInstants().lastInstant()).thenReturn(Option.of(lastCompletedInstant));
    when(table.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.isEmpty(lastCompletedInstant)).thenReturn(false);

    HoodieEngineContext engineContext = new HoodieLocalEngineContext(new HadoopStorageConfiguration(false));
    CleanPlanActionExecutor<?, ?, ?, ?> executor = spy(new CleanPlanActionExecutor<>(engineContext, HoodieWriteConfig.newBuilder().withPath("file://tmp").build(), table, Option.empty()));
    HoodieCleanerPlan emptyPlan = new HoodieCleanerPlan();
    doReturn(emptyPlan).when(executor).requestClean(engineContext);
    assertEquals(Option.empty(), executor.requestClean());
  }

  @Test
  void lastCleanIsNotPresent() {
    HoodieTable table = mock(HoodieTable.class, RETURNS_DEEP_STUBS);

    // allow clean to trigger
    mockThatCleanIsRequired(table);
    // No last clean
    when(table.getCleanTimeline().filterCompletedInstants().lastInstant()).thenReturn(Option.empty());

    HoodieEngineContext engineContext = new HoodieLocalEngineContext(new HadoopStorageConfiguration(false));
    CleanPlanActionExecutor<?, ?, ?, ?> executor = spy(new CleanPlanActionExecutor<>(engineContext, HoodieWriteConfig.newBuilder().withPath("file://tmp").build(), table, Option.empty()));
    HoodieCleanerPlan emptyPlan = new HoodieCleanerPlan();
    doReturn(emptyPlan).when(executor).requestClean(engineContext);
    assertEquals(Option.empty(), executor.requestClean());
  }

  private static void mockEmptyLastCompletedClean(HoodieTable table, HoodieInstant lastCompletedInstant, HoodieActiveTimeline activeTimeline, boolean hasEmptyPlan) {
    if (hasEmptyPlan) {
      // after deleting an empty plan, we will re-fetch the last completed clean
      when(table.getCleanTimeline().filterCompletedInstants().lastInstant()).thenReturn(Option.of(lastCompletedInstant)).thenReturn(Option.empty());
    } else {
      when(table.getCleanTimeline().filterCompletedInstants().lastInstant()).thenReturn(Option.of(lastCompletedInstant));
    }
    when(table.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.isEmpty(lastCompletedInstant)).thenReturn(true);
  }

  private static void mockThatCleanIsRequired(HoodieTable table) {
    when(table.getActiveTimeline().getCleanerTimeline().filterCompletedInstants().lastInstant()).thenReturn(Option.empty());
    HoodieTimeline commitsTimeline = mock(HoodieTimeline.class);
    when(table.getActiveTimeline().getCommitsTimeline().filterCompletedInstants()).thenReturn(commitsTimeline);
    when(commitsTimeline.countInstants()).thenReturn(2);
  }
}
