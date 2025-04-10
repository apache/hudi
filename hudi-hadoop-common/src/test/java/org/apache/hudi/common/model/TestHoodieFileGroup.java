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

package org.apache.hudi.common.model;

import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.versioning.v2.CompletionTimeQueryViewV2;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.MockHoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.invocation.InvocationOnMock;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link HoodieFileGroup}.
 */
public class TestHoodieFileGroup {

  @Test
  public void testCommittedFileSlices() {
    // "000" is archived
    Stream<String> completed = Stream.of("001");
    Stream<String> inflight = Stream.of("002");
    MockHoodieTimeline activeTimeline = new MockHoodieTimeline(completed, inflight);
    HoodieFileGroup fileGroup = new HoodieFileGroup("", "data",
        activeTimeline.getCommitsTimeline().filterCompletedInstants());
    for (int i = 0; i < 3; i++) {
      HoodieBaseFile baseFile = new HoodieBaseFile("data_1_00" + i);
      fileGroup.addBaseFile(baseFile);
    }
    assertEquals(2, fileGroup.getAllFileSlices().count());
    assertEquals(2, fileGroup.getAllFileSlicesBeforeOn("002").count());
    assertFalse(fileGroup.getAllFileSlices().anyMatch(s -> s.getBaseInstantTime().equals("002")));
    assertEquals(3, fileGroup.getAllFileSlicesIncludingInflight().count());
    assertEquals("001", fileGroup.getLatestFileSlice().get().getBaseInstantTime());
    assertEquals("001", (new HoodieFileGroup(fileGroup)).getLatestFileSlice().get().getBaseInstantTime());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testCommittedFileSlicesWithSavepoint(boolean preTableVersion8) {
    // "000" is archived
    Stream<String> completed = Stream.of("001");
    Stream<String> inflight = Stream.of("002");
    MockHoodieTimeline activeTimeline = new MockHoodieTimeline(completed, inflight);
    CompletionTimeQueryView queryView = getMockCompletionTimeQueryView(activeTimeline);
    HoodieFileGroup fileGroup = new HoodieFileGroup("", "data",
        activeTimeline.getCommitsTimeline().filterCompletedInstants());
    for (int i = 0; i < 3; i++) {
      HoodieBaseFile baseFile = new HoodieBaseFile("data_1_00" + i);
      fileGroup.addBaseFile(baseFile);
      fileGroup.addLogFile(queryView, new HoodieLogFile(new StoragePath(FileCreateUtilsLegacy.logFileName(preTableVersion8 ? "001" : "00" + i, "data", i))));
    }

    assertEquals(2, fileGroup.getAllFileSlices().count());
    assertFalse(fileGroup.getAllFileSlices().anyMatch(s -> s.getBaseInstantTime().equals("002")));
    assertEquals(3, fileGroup.getAllFileSlicesIncludingInflight().count());
  }

  @Test
  public void testCommittedFileSlicesWithSavepointAndHoles() {
    MockHoodieTimeline activeTimeline = new MockHoodieTimeline(Stream.of(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "01"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, "01"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "03"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, "03"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "05") // this can be DELTA_COMMIT/REPLACE_COMMIT as well
    ).collect(Collectors.toList()));
    HoodieFileGroup fileGroup = new HoodieFileGroup("", "data", activeTimeline.filterCompletedAndCompactionInstants());
    for (int i = 0; i < 7; i++) {
      HoodieBaseFile baseFile = new HoodieBaseFile("data_1_0" + i);
      fileGroup.addBaseFile(baseFile);
    }
    List<FileSlice> allFileSlices = fileGroup.getAllFileSlices().collect(Collectors.toList());
    assertEquals(6, allFileSlices.size());
    assertFalse(allFileSlices.stream().anyMatch(s -> s.getBaseInstantTime().equals("06")));
    assertEquals(7, fileGroup.getAllFileSlicesIncludingInflight().count());
    assertEquals("05", fileGroup.getLatestFileSlice().get().getBaseInstantTime());
  }

  private void testFileSlicingForTableVersion(boolean useBaseInstantTime) {
    // given: a timeline with insert to logs, completed and pending compactions/commits.
    Stream<String> completed = Stream.of("001", "002", "003", "004", "005", "007");
    Stream<String> inflight = Stream.of("006", "008");
    MockHoodieTimeline activeTimeline = new MockHoodieTimeline(completed, inflight);
    CompletionTimeQueryView queryView = getMockCompletionTimeQueryView(activeTimeline);

    // when: building a file group with file slices like table version 6.
    HoodieFileGroup fileGroup = new HoodieFileGroup("", "f1", activeTimeline);

    fileGroup.addLogFile(queryView, new HoodieLogFile(new StoragePath(FileCreateUtilsLegacy.logFileName("001", "f1", 0))));
    fileGroup.addLogFile(queryView, new HoodieLogFile(new StoragePath(FileCreateUtilsLegacy.logFileName(useBaseInstantTime ? "001" : "002", "f1", 1))));

    fileGroup.addBaseFile(new HoodieBaseFile(FileCreateUtilsLegacy.baseFileName("003", "f1")));
    fileGroup.addLogFile(queryView, new HoodieLogFile(new StoragePath(FileCreateUtilsLegacy.logFileName(useBaseInstantTime ? "003" : "004", "f1", 0))));
    fileGroup.addLogFile(queryView, new HoodieLogFile(new StoragePath(FileCreateUtilsLegacy.logFileName(useBaseInstantTime ? "003" : "005", "f1", 1))));

    fileGroup.addBaseFile(new HoodieBaseFile(FileCreateUtilsLegacy.baseFileName("006", "f1")));
    fileGroup.addLogFile(queryView, new HoodieLogFile(new StoragePath(FileCreateUtilsLegacy.logFileName(useBaseInstantTime ? "006" : "007", "f1", 0))));
    fileGroup.addLogFile(queryView, new HoodieLogFile(new StoragePath(FileCreateUtilsLegacy.logFileName(useBaseInstantTime ? "006" : "008", "f1", 1))));

    // then: assert that the file slices are in-tact.
    assertEquals(3, fileGroup.getAllFileSlices().count());
    assertEquals(
        CollectionUtils.createImmutableList("006", "003", "001"),
        fileGroup.getAllFileSlices().map(FileSlice::getBaseInstantTime).collect(Collectors.toList())
    );
    assertEquals("006", fileGroup.getLatestDataFile().get().getCommitTime());
    assertTrue(fileGroup.getLatestFileSliceBefore("001").isEmpty());
    assertTrue(fileGroup.getLatestFileSliceBeforeOrOn("001").isPresent());
    assertEquals(
        CollectionUtils.createImmutableList("003", "001"),
        fileGroup.getAllFileSlicesBeforeOn("003").map(FileSlice::getBaseInstantTime).collect(Collectors.toList())
    );
  }

  @Test
  public void testTableVersionLesserThan6FileSlicing() {
    testFileSlicingForTableVersion(true);
  }

  @Test
  public void testTableVersionGreaterThan8FileSlicing() {
    testFileSlicingForTableVersion(false);
  }

  @Test
  public void testGetBaseInstantTime() {
    MockHoodieTimeline activeTimeline = new MockHoodieTimeline(Stream.of(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "001", "001"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "002", "011"), // finishes in the last
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "003", "007"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "004", "006"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "005", "007")
    ).collect(Collectors.toList()));

    CompletionTimeQueryViewV2 queryView = getMockCompletionTimeQueryView(activeTimeline);

    HoodieFileGroup fileGroup = new HoodieFileGroup("", "data", activeTimeline.filterCompletedAndCompactionInstants());

    HoodieLogFile logFile1 = new HoodieLogFile(new StoragePath(getLogFileName("001")));
    fileGroup.addLogFile(queryView, logFile1);
    assertThat("no base file in the file group, returns the delta commit instant itself",
        fileGroup.getBaseInstantTime(queryView, logFile1), is("001"));
    assertThat(collectFileSlices(fileGroup), is("001"));

    HoodieLogFile logFile2 = new HoodieLogFile(new StoragePath(getLogFileName("002")));
    fileGroup.addLogFile(queryView, logFile2);
    assertThat("no base file in the file group, returns the earliest delta commit instant",
        fileGroup.getBaseInstantTime(queryView, logFile2), is("001"));
    assertThat(collectFileSlices(fileGroup), is("001"));

    fileGroup.addNewFileSliceAtInstant("003");
    assertThat("Include the pending compaction instant time as constitute of the file slice base instant time list",
        collectFileSlices(fileGroup), is("001,003"));

    HoodieLogFile logFile3 = new HoodieLogFile(new StoragePath(getLogFileName("004")));
    fileGroup.addLogFile(queryView, logFile3);
    assertThat("Assign the log file to maximum base instant time that less than or equals its completion time",
        fileGroup.getBaseInstantTime(queryView, logFile2), is("003"));
    assertThat(collectFileSlices(fileGroup), is("001,003"));

    // now add the base files
    fileGroup.addBaseFile(new HoodieBaseFile(getBaseFileName("003")));
    fileGroup.addBaseFile(new HoodieBaseFile(getBaseFileName("005")));

    assertThat(collectFileSlices(fileGroup), is("001,003,005"));

    // check the delta commit that takes a long time to finish
    assertThat("no base file in the file group, returns the earliest delta commit instant",
        fileGroup.getBaseInstantTime(queryView, logFile2), is("005"));
  }

  private CompletionTimeQueryViewV2 getMockCompletionTimeQueryView(MockHoodieTimeline activeTimeline) {
    Map<String, String> completionTimeMap = activeTimeline.filterCompletedInstants().getInstantsAsStream()
        .collect(Collectors.toMap(HoodieInstant::requestedTime, HoodieInstant::getCompletionTime));
    CompletionTimeQueryViewV2 queryView = mock(CompletionTimeQueryViewV2.class);
    when(queryView.getCompletionTime(any(String.class), any(String.class)))
        .thenAnswer((InvocationOnMock invocationOnMock) -> {
          String instantTime = invocationOnMock.getArgument(1);
          return Option.ofNullable(completionTimeMap.get(instantTime));
        });
    return queryView;
  }

  private static String collectFileSlices(HoodieFileGroup fileGroup) {
    return fileGroup.getAllFileSlices().map(FileSlice::getBaseInstantTime).sorted().collect(Collectors.joining(","));
  }

  private static String getLogFileName(String instantTime) {
    return ".fg1_" + instantTime + ".log.1_1-0-1";
  }

  private static String getBaseFileName(String instantTime) {
    return "fg1_1-0-1_" + instantTime + ".parquet";
  }
}
