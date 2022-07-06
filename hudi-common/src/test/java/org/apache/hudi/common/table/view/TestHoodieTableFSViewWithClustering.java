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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestHoodieTableFSViewWithClustering extends HoodieCommonTestHarness {

  private static final String TEST_WRITE_TOKEN = "1-0-1";
  private static final String BOOTSTRAP_SOURCE_PATH = "/usr/warehouse/hive/data/tables/src1/";

  private IncrementalTimelineSyncFileSystemView fsView;
  private TableFileSystemView.BaseFileOnlyView roView;

  @BeforeEach
  public void setup() throws IOException {
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType(), BOOTSTRAP_SOURCE_PATH, false);
    basePath = metaClient.getBasePathV2().toString();
    refreshFsView();
  }

  protected void refreshFsView() throws IOException {
    super.refreshFsView();
    closeFsView();
    fsView = (IncrementalTimelineSyncFileSystemView) getFileSystemView(metaClient.getActiveTimeline().filterCompletedAndCompactionInstants());
    roView = fsView;
  }

  private void closeFsView() {
    if (null != fsView) {
      fsView.close();
      fsView = null;
    }
  }

  @AfterEach
  public void close() {
    closeFsView();
  }

  @Test
  public void testReplaceFileIdIsExcludedInView() throws IOException {
    String partitionPath1 = "2020/06/27";
    String partitionPath2 = "2020/07/14";
    new File(basePath + "/" + partitionPath1).mkdirs();
    new File(basePath + "/" + partitionPath2).mkdirs();

    // create 2 fileId in partition1 - fileId1 is replaced later on.
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();

    // create 2 fileId in partition2 - fileId3, fileId4 is replaced later on.
    String fileId3 = UUID.randomUUID().toString();
    String fileId4 = UUID.randomUUID().toString();

    assertFalse(roView.getLatestBaseFiles(partitionPath1)
            .anyMatch(dfile -> dfile.getFileId().equals(fileId1) || dfile.getFileId().equals(fileId2)),
        "No commit, should not find any data file");
    assertFalse(roView.getLatestBaseFiles(partitionPath2)
            .anyMatch(dfile -> dfile.getFileId().equals(fileId3) || dfile.getFileId().equals(fileId4)),
        "No commit, should not find any data file");
    assertFalse(fsView.fetchLatestBaseFiles(partitionPath1)
            .anyMatch(dfile -> dfile.getFileId().equals(fileId1) || dfile.getFileId().equals(fileId2)),
        "No commit, should not find any data file");
    assertFalse(fsView.fetchLatestBaseFiles(partitionPath2)
            .anyMatch(dfile -> dfile.getFileId().equals(fileId3) || dfile.getFileId().equals(fileId4)),
        "No commit, should not find any data file");

    // Only one commit
    String commitTime1 = "1";
    String fileName1 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1);
    String fileName2 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2);
    String fileName3 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId3);
    String fileName4 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId4);
    new File(basePath + "/" + partitionPath1 + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath1 + "/" + fileName2).createNewFile();
    new File(basePath + "/" + partitionPath2 + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath2 + "/" + fileName4).createNewFile();

    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    List<String> replacedFileIdsP1 = new ArrayList<>();
    replacedFileIdsP1.add(fileId1);
    partitionToReplaceFileIds.put(partitionPath1, replacedFileIdsP1);
    List<String> replacedFileIdsP2 = new ArrayList<>();
    replacedFileIdsP2.add(fileId3);
    replacedFileIdsP2.add(fileId4);
    partitionToReplaceFileIds.put(partitionPath2, replacedFileIdsP2);
    HoodieCommitMetadata commitMetadata =
        CommitUtils.buildMetadata(Collections.emptyList(), partitionToReplaceFileIds, Option.empty(), WriteOperationType.INSERT_OVERWRITE, "", HoodieTimeline.REPLACE_COMMIT_ACTION);

    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, commitTime1);
    saveAsComplete(commitTimeline, instant1, Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    refreshFsView();
    assertEquals(0, roView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId1)).count());
    assertEquals(fileName2, roView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId2)).findFirst().get().getFileName());
    assertEquals(0, roView.getLatestBaseFiles(partitionPath2)
        .filter(dfile -> dfile.getFileId().equals(fileId3)).count());
    assertEquals(0, roView.getLatestBaseFiles(partitionPath2)
        .filter(dfile -> dfile.getFileId().equals(fileId4)).count());
    assertEquals(0, fsView.fetchLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId1)).count());
    assertEquals(fileName2, fsView.fetchLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId2)).findFirst().get().getFileName());
    assertEquals(0, fsView.fetchLatestBaseFiles(partitionPath2)
        .filter(dfile -> dfile.getFileId().equals(fileId3)).count());
    assertEquals(0, fsView.fetchLatestBaseFiles(partitionPath2)
        .filter(dfile -> dfile.getFileId().equals(fileId4)).count());

    // ensure replacedFileGroupsBefore works with all instants
    List<HoodieFileGroup> replacedOnInstant1 = fsView.getReplacedFileGroupsBeforeOrOn("0", partitionPath1).collect(Collectors.toList());
    assertEquals(0, replacedOnInstant1.size());

    List<HoodieFileGroup> allReplaced = fsView.getReplacedFileGroupsBeforeOrOn("2", partitionPath1).collect(Collectors.toList());
    allReplaced.addAll(fsView.getReplacedFileGroupsBeforeOrOn("2", partitionPath2).collect(Collectors.toList()));
    assertEquals(3, allReplaced.size());
    Set<String> allReplacedFileIds = allReplaced.stream().map(fg -> fg.getFileGroupId().getFileId()).collect(Collectors.toSet());
    Set<String> actualReplacedFileIds = Stream.of(fileId1, fileId3, fileId4).collect(Collectors.toSet());
    assertEquals(actualReplacedFileIds, allReplacedFileIds);
  }

  private static void saveAsComplete(HoodieActiveTimeline timeline, HoodieInstant inflight, Option<byte[]> data) {
    if (inflight.getAction().equals(HoodieTimeline.COMPACTION_ACTION)) {
      timeline.transitionCompactionInflightToComplete(inflight, data);
    } else {
      HoodieInstant requested = new HoodieInstant(HoodieInstant.State.REQUESTED, inflight.getAction(), inflight.getTimestamp());
      timeline.createNewInstant(requested);
      timeline.transitionRequestedToInflight(requested, Option.empty());
      timeline.saveAsComplete(inflight, data);
    }
  }
}
