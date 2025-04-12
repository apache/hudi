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

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.MockHoodieTimeline;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link HoodieFileGroup}.
 */
public class TestHoodieFileGroup {

  @Test
  public void testCommittedFileSlices() {
    // "000" is archived
    Stream<String> completed = Arrays.asList("001").stream();
    Stream<String> inflight = Arrays.asList("002").stream();
    MockHoodieTimeline activeTimeline = new MockHoodieTimeline(completed, inflight);
    HoodieFileGroup fileGroup = new HoodieFileGroup("", "data",
        activeTimeline.getCommitsTimeline().filterCompletedInstants());
    for (int i = 0; i < 3; i++) {
      HoodieBaseFile baseFile = new HoodieBaseFile("data_1_00" + i);
      fileGroup.addBaseFile(baseFile);
    }
    assertEquals(2, fileGroup.getAllFileSlices().count());
    assertEquals(2, fileGroup.getAllFileSlicesBeforeOn("002").count());
    assertTrue(!fileGroup.getAllFileSlices().anyMatch(s -> s.getBaseInstantTime().equals("002")));
    assertEquals(3, fileGroup.getAllFileSlicesIncludingInflight().count());
    assertTrue(fileGroup.getLatestFileSlice().get().getBaseInstantTime().equals("001"));
    assertTrue((new HoodieFileGroup(fileGroup)).getLatestFileSlice().get().getBaseInstantTime().equals("001"));
  }

  @Test
  public void testCommittedFileSlicesWithSavepointAndHoles() {
    MockHoodieTimeline activeTimeline = new MockHoodieTimeline(Stream.of(
        new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "01"),
        new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, "01"),
        new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "03"),
        new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, "03"),
        new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "05") // this can be DELTA_COMMIT/REPLACE_COMMIT as well
    ).collect(Collectors.toList()));
    HoodieFileGroup fileGroup = new HoodieFileGroup("", "data", activeTimeline.filterCompletedAndCompactionInstants());
    for (int i = 0; i < 7; i++) {
      HoodieBaseFile baseFile = new HoodieBaseFile("data_1_0" + i);
      fileGroup.addBaseFile(baseFile);
    }
    List<FileSlice> allFileSlices = fileGroup.getAllFileSlices().collect(Collectors.toList());
    assertEquals(6, allFileSlices.size());
    assertTrue(!allFileSlices.stream().anyMatch(s -> s.getBaseInstantTime().equals("06")));
    assertEquals(7, fileGroup.getAllFileSlicesIncludingInflight().count());
    assertTrue(fileGroup.getLatestFileSlice().get().getBaseInstantTime().equals("05"));
  }

  private static Stream<Arguments> multipleBaseFileCases() throws IOException {
    String partition = "1";
    String groupId = UUID.randomUUID().toString();
    String relativePath = "1/some_path2.parquet";

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPath(relativePath);
    writeStat.setFileId(groupId);
    commitMetadata.addWriteStat(partition, writeStat);

    HoodieCommitMetadata unPartitionedCommitMetadata = new HoodieCommitMetadata();
    unPartitionedCommitMetadata.addWriteStat("", writeStat);

    HoodieReplaceCommitMetadata replaceCommitMetadata = new HoodieReplaceCommitMetadata();
    replaceCommitMetadata.addWriteStat(partition, writeStat);
    return Stream.of(
        Arguments.of(partition, groupId, relativePath, HoodieTimeline.COMMIT_ACTION, commitMetadata),
        Arguments.of(partition, groupId, relativePath, HoodieTimeline.DELTA_COMMIT_ACTION, commitMetadata),
        Arguments.of("", groupId, relativePath, HoodieTimeline.DELTA_COMMIT_ACTION, unPartitionedCommitMetadata),
        Arguments.of(partition, groupId, relativePath, HoodieTimeline.REPLACE_COMMIT_ACTION, replaceCommitMetadata)
    );
  }

  @ParameterizedTest
  @MethodSource("multipleBaseFileCases")
  void handleMultipleBaseFilesForSameFileSlice(String partition, String groupId, String relativePath, String action, Object commitMetadata) throws IOException {
    String commitTime = "01";
    String actualActiveBaseFilePath = "/tmp/" + relativePath;

    HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED, action, commitTime);
    HoodieInstant inflightInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, action, commitTime);
    List<HoodieInstant> allInstants = Arrays.asList(inflightInstant, instant);
    when(mockTimeline.getInstants()).thenReturn(allInstants);
    when(mockTimeline.lastInstant()).thenReturn(Option.of(instant));
    when(mockTimeline.containsOrBeforeTimelineStarts(instant.getTimestamp())).thenReturn(true);

    if (action.equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
      when(mockTimeline.deserializeInstantContent(instant, HoodieReplaceCommitMetadata.class)).thenReturn((HoodieReplaceCommitMetadata) commitMetadata);
    } else {
      when(mockTimeline.deserializeInstantContent(instant, HoodieCommitMetadata.class)).thenReturn((HoodieCommitMetadata) commitMetadata);
    }

    HoodieFileGroup fileGroup = new HoodieFileGroup(partition, groupId, mockTimeline);

    // Add 3 base files, second file is the proper one
    HoodieBaseFile baseFile1 = new HoodieBaseFile("/tmp/1/some_path1.parquet", groupId, commitTime, null);
    fileGroup.addBaseFile(baseFile1);
    HoodieBaseFile baseFile2 = new HoodieBaseFile(actualActiveBaseFilePath, groupId, commitTime, null);
    fileGroup.addBaseFile(baseFile2);
    HoodieBaseFile baseFile3 = new HoodieBaseFile("/tmp/1/some_path3.parquet", groupId, commitTime, null);
    fileGroup.addBaseFile(baseFile3);

    // Assert that only a single file slice is created and there is just a single latest base file
    assertEquals(1, fileGroup.getAllFileSlices().count());
    assertEquals(Collections.singletonList(baseFile2), fileGroup.getAllBaseFiles().collect(Collectors.toList()));
  }

  @Test
  void handleMultipleBaseFiles_failToReadCommit() throws IOException {
    String partition = "1";
    String groupId = UUID.randomUUID().toString();
    String relativePath = "1/some_path2.parquet";
    String commitTime = "01";
    String firstBaseFilePath = "/tmp/1/some_path1.parquet";
    String actualActiveBaseFilePath = "/tmp/" + relativePath;

    HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, commitTime);
    HoodieInstant inflightInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, commitTime);
    List<HoodieInstant> allInstants = Arrays.asList(inflightInstant, instant);
    when(mockTimeline.getInstants()).thenReturn(allInstants);
    when(mockTimeline.lastInstant()).thenReturn(Option.of(instant));
    when(mockTimeline.containsOrBeforeTimelineStarts(instant.getTimestamp())).thenReturn(true);

    when(mockTimeline.deserializeInstantContent(instant, HoodieCommitMetadata.class)).thenThrow(new RuntimeException("No file found"));

    HoodieFileGroup fileGroup = new HoodieFileGroup(partition, groupId, mockTimeline);

    // Add 2 base files, last file used by default since we fail to read commit metadata to determine that
    HoodieBaseFile baseFile1 = new HoodieBaseFile(firstBaseFilePath, groupId, commitTime, null);
    fileGroup.addBaseFile(baseFile1);
    HoodieBaseFile baseFile2 = new HoodieBaseFile(actualActiveBaseFilePath, groupId, commitTime, null);
    fileGroup.addBaseFile(baseFile2);

    // Assert that only a single file slice is created and there is just a single latest base file
    assertEquals(1, fileGroup.getAllFileSlices().count());
    assertEquals(Collections.singletonList(baseFile2), fileGroup.getAllBaseFiles().collect(Collectors.toList()));
  }

  @Test
  void handleMultipleBaseFiles_commitIsNotPartOfActiveTimeline() {
    String partition = "1";
    String groupId = UUID.randomUUID().toString();
    String relativePath = "1/some_path2.parquet";
    String commitTime = "01";
    String firstBaseFilePath = "/tmp/1/some_path1.parquet";
    String actualActiveBaseFilePath = "/tmp/" + relativePath;

    HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "02");
    HoodieInstant inflightInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "02");
    List<HoodieInstant> allInstants = Arrays.asList(inflightInstant, instant);
    when(mockTimeline.getInstants()).thenReturn(allInstants);
    when(mockTimeline.lastInstant()).thenReturn(Option.of(instant));
    when(mockTimeline.containsOrBeforeTimelineStarts(commitTime)).thenReturn(true);

    HoodieFileGroup fileGroup = new HoodieFileGroup(partition, groupId, mockTimeline);

    // Add 2 base files, last file is used since we cannot find the commit in the active timeline
    HoodieBaseFile baseFile1 = new HoodieBaseFile(firstBaseFilePath, groupId, commitTime, null);
    fileGroup.addBaseFile(baseFile1);
    HoodieBaseFile baseFile2 = new HoodieBaseFile(actualActiveBaseFilePath, groupId, commitTime, null);
    fileGroup.addBaseFile(baseFile2);

    // Assert that only a single file slice is created and there is just a single latest base file
    assertEquals(1, fileGroup.getAllFileSlices().count());
    assertEquals(Collections.singletonList(baseFile2), fileGroup.getAllBaseFiles().collect(Collectors.toList()));
  }

  @Test
  void handleMultipleBaseFiles_unexpectedCommitType() {
    String partition = "1";
    String groupId = UUID.randomUUID().toString();
    String relativePath = "1/some_path2.parquet";
    String commitTime = "01";
    String firstBaseFilePath = "/tmp/1/some_path1.parquet";
    String actualActiveBaseFilePath = "/tmp/" + relativePath;

    HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
    // Handle case where the commit type is not a commit, delta-commit, or replace-commit
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, commitTime);
    HoodieInstant inflightInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.SAVEPOINT_ACTION, commitTime);
    List<HoodieInstant> allInstants = Arrays.asList(inflightInstant, instant);
    when(mockTimeline.getInstants()).thenReturn(allInstants);
    when(mockTimeline.lastInstant()).thenReturn(Option.of(instant));
    when(mockTimeline.containsOrBeforeTimelineStarts(instant.getTimestamp())).thenReturn(true);

    HoodieFileGroup fileGroup = new HoodieFileGroup(partition, groupId, mockTimeline);

    // Add 2 base files, last file used by default since we cannot parse the instant
    HoodieBaseFile baseFile1 = new HoodieBaseFile(firstBaseFilePath, groupId, commitTime, null);
    fileGroup.addBaseFile(baseFile1);
    HoodieBaseFile baseFile2 = new HoodieBaseFile(actualActiveBaseFilePath, groupId, commitTime, null);
    fileGroup.addBaseFile(baseFile2);

    // Assert that only a single file slice is created and there is just a single latest base file
    assertEquals(1, fileGroup.getAllFileSlices().count());
    assertEquals(Collections.singletonList(baseFile2), fileGroup.getAllBaseFiles().collect(Collectors.toList()));
  }
}
