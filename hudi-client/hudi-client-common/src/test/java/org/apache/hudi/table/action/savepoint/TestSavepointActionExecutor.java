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

package org.apache.hudi.table.action.savepoint;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.testutils.MockHoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestSavepointActionExecutor {
  private static final String CLEAN_REQUEST_TIME = "20240111101012345";
  private final HoodieTable mockTable = mock(HoodieTable.class, RETURNS_DEEP_STUBS);
  private final HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(new HadoopStorageConfiguration(false));
  private final String instantTime = "20240121101012345";

  @Test
  void testLastCommitRetained_noCleanCommits() {
    String expectedInstant = "20240101101012345";
    when(mockTable.getCompletedCommitsTimeline().firstInstant()).thenReturn(Option.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, expectedInstant,
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR)));
    when(mockTable.getCleanTimeline().lastInstant()).thenReturn(Option.empty());

    SavepointActionExecutor executor = new SavepointActionExecutor(engineContext, null, mockTable, instantTime, "user", "comment");
    assertEquals(expectedInstant, executor.getLastCommitRetained());
  }

  @Test
  void testLastCommitRetained_completedClean() throws IOException {
    String expectedInstant = "20240101101012345";
    HoodieInstant cleanInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLEAN_ACTION, CLEAN_REQUEST_TIME,
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    when(mockTable.getCompletedCommitsTimeline().firstInstant()).thenReturn(Option.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, expectedInstant,
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR)));
    when(mockTable.getCleanTimeline().lastInstant()).thenReturn(Option.of(cleanInstant));

    HoodieCleanMetadata cleanMetadata = new HoodieCleanMetadata();
    cleanMetadata.setEarliestCommitToRetain(expectedInstant);
    when(mockTable.getActiveTimeline().readCleanMetadata(cleanInstant)).thenReturn(cleanMetadata);

    SavepointActionExecutor executor = new SavepointActionExecutor(engineContext, null, mockTable, instantTime, "user", "comment");
    assertEquals(expectedInstant, executor.getLastCommitRetained());
  }

  @Test
  void testLastCommitRetained_requestedClean() throws IOException {
    String expectedInstant = "20240101101012345";
    String cleanRequested = CLEAN_REQUEST_TIME;
    HoodieInstant cleanInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, cleanRequested,
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    InstantGenerator instantGenerator = mock(InstantGenerator.class);
    when(mockTable.getInstantGenerator()).thenReturn(instantGenerator);
    when(instantGenerator.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, cleanRequested))
        .thenReturn(cleanInstant);

    when(mockTable.getCompletedCommitsTimeline().firstInstant()).thenReturn(Option.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, expectedInstant,
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR)));
    when(mockTable.getCleanTimeline().lastInstant()).thenReturn(Option.of(cleanInstant));

    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan();
    HoodieActionInstant lastInstantToRetain = HoodieActionInstant.newBuilder().setAction(HoodieTimeline.COMMIT_ACTION)
        .setState(HoodieInstant.State.COMPLETED.name()).setTimestamp(expectedInstant).build();
    cleanerPlan.setEarliestInstantToRetain(lastInstantToRetain);
    when(mockTable.getActiveTimeline().readCleanerPlan(cleanInstant)).thenReturn(cleanerPlan);

    SavepointActionExecutor executor = new SavepointActionExecutor(engineContext, null, mockTable, instantTime, "user", "comment");
    assertEquals(expectedInstant, executor.getLastCommitRetained());
  }

  private static Stream<Arguments> noEarliestInstantRetained() {
    String lastCommitBeforeClean = "20240110101012345";
    return Stream.of(
        Arguments.of(Stream.of("20240108101012345", "20240109101012345", lastCommitBeforeClean, "20240129101012345"), lastCommitBeforeClean),
        Arguments.of(Stream.of("20240129101012345", "20240139101012345", "20240149101012345"), CLEAN_REQUEST_TIME)
    );
  }

  @ParameterizedTest
  @MethodSource("noEarliestInstantRetained")
  void testLastCommitRetained_completedCleanWithoutEarliestInstantRetained(Stream<String> writeInstants, String expectedInstant) throws IOException {
    HoodieInstant cleanInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLEAN_ACTION, CLEAN_REQUEST_TIME,
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    when(mockTable.getCompletedCommitsTimeline().firstInstant()).thenReturn(Option.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, expectedInstant,
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR)));
    when(mockTable.getCleanTimeline().lastInstant()).thenReturn(Option.of(cleanInstant));

    HoodieCleanMetadata cleanMetadata = new HoodieCleanMetadata();
    when(mockTable.getActiveTimeline().readCleanMetadata(cleanInstant)).thenReturn(cleanMetadata);

    MockHoodieTimeline mockTimeline = new MockHoodieTimeline(writeInstants, Stream.empty());
    when(mockTable.getActiveTimeline().getWriteTimeline().filterCompletedInstants()).thenReturn(mockTimeline);

    SavepointActionExecutor executor = new SavepointActionExecutor(engineContext, null, mockTable, instantTime, "user", "comment");
    assertEquals(expectedInstant, executor.getLastCommitRetained());
  }
}
