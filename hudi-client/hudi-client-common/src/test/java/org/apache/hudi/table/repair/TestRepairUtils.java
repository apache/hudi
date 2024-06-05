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

package org.apache.hudi.table.repair;

import org.apache.hudi.HoodieTestCommitGenerator;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.HoodieTestCommitGenerator.getBaseFilename;
import static org.apache.hudi.HoodieTestCommitGenerator.getLogFilename;
import static org.apache.hudi.HoodieTestCommitGenerator.initCommitInfoForRepairTests;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestRepairUtils {
  // Instant time -> List<Pair<relativePartitionPath, fileId>>
  private static final Map<String, List<Pair<String, String>>> BASE_FILE_INFO = new HashMap<>();
  private static final Map<String, List<Pair<String, String>>> LOG_FILE_INFO = new HashMap<>();
  // instant time -> partitionPathToFileIdAndPathMap
  private final Map<String, Map<String, List<Pair<String, String>>>> instantInfoMap = new HashMap<>();
  @TempDir
  public static java.nio.file.Path tempDir;
  private static String basePath;
  private static HoodieTableMetaClient metaClient;

  @BeforeAll
  static void initFileInfo() throws IOException {
    initCommitInfoForRepairTests(BASE_FILE_INFO, LOG_FILE_INFO);
    basePath = tempDir.toAbsolutePath().toString();
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
  }

  public void setupTimelineInFS() throws IOException {
    HoodieTestCommitGenerator.setupTimelineInFS(
        basePath, BASE_FILE_INFO, LOG_FILE_INFO, instantInfoMap);
  }

  @Test
  public void testTagInstantsOfBaseAndLogFiles() {
    Map<String, List<String>> expectedResult = new HashMap<>();
    List<StoragePath> inputPathList = new ArrayList<>();

    for (Map.Entry<String, List<Pair<String, String>>> entry : BASE_FILE_INFO.entrySet()) {
      String instantTime = entry.getKey();
      List<String> fileNameList = entry.getValue().stream()
          .map(e -> {
            String partitionPath = e.getKey();
            String fileId = e.getValue();
            return new StoragePath(
                new StoragePath(partitionPath), getBaseFilename(instantTime, fileId)).toString();
          })
          .collect(Collectors.toList());
      List<String> expectedList = expectedResult.computeIfAbsent(
          instantTime, k -> new ArrayList<>());
      expectedList.addAll(fileNameList);
      inputPathList.addAll(fileNameList.stream()
          .map(path -> new StoragePath(basePath, path)).collect(Collectors.toList()));
    }

    for (Map.Entry<String, List<Pair<String, String>>> entry : LOG_FILE_INFO.entrySet()) {
      String instantTime = entry.getKey();
      List<String> fileNameList = entry.getValue().stream()
          .map(e -> {
            String partitionPath = e.getKey();
            String fileId = e.getValue();
            return new StoragePath(
                new StoragePath(partitionPath), getLogFilename(instantTime, fileId)).toString();
          })
          .collect(Collectors.toList());
      List<String> expectedList = expectedResult.computeIfAbsent(
          instantTime, k -> new ArrayList<>());
      expectedList.addAll(fileNameList);
      inputPathList.addAll(fileNameList.stream()
          .map(path -> new StoragePath(basePath, path)).collect(Collectors.toList()));
    }

    assertEquals(expectedResult,
        RepairUtils.tagInstantsOfBaseAndLogFiles(basePath, inputPathList));
  }

  @Test
  public void testGetBaseAndLogFilePathsFromTimeline() throws IOException {
    setupTimelineInFS();
    HoodieTimeline timeline = metaClient.getActiveTimeline();
    HoodieInstant commitInstant = new HoodieInstant(
        HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001");
    HoodieInstant inflightInstant = new HoodieInstant(
        HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "005");
    HoodieInstant compactionInstant = new HoodieInstant(
        HoodieInstant.State.COMPLETED, HoodieTimeline.COMPACTION_ACTION, "006");

    Map<String, List<Pair<String, String>>> partitionToFileIdAndNameMap =
        instantInfoMap.get(commitInstant.getTimestamp());
    Set<String> expectedPaths = partitionToFileIdAndNameMap.entrySet().stream()
        .flatMap(entry ->
            entry.getValue().stream()
                .map(fileInfo -> new Path(entry.getKey(), fileInfo.getValue()).toString())
                .collect(Collectors.toList())
                .stream()
        ).collect(Collectors.toSet());
    assertEquals(Option.of(expectedPaths),
        RepairUtils.getBaseAndLogFilePathsFromTimeline(timeline, commitInstant));
    assertThrows(HoodieException.class,
        () -> RepairUtils.getBaseAndLogFilePathsFromTimeline(timeline, inflightInstant));
    assertEquals(Option.empty(),
        RepairUtils.getBaseAndLogFilePathsFromTimeline(timeline, compactionInstant));
  }

  @Test
  public void testFindInstantFilesToRemove() throws IOException {
    setupTimelineInFS();
    HoodieInstant existingInstant = new HoodieInstant(
        HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001");
    Map<String, List<Pair<String, String>>> partitionToFileIdAndNameMap =
        instantInfoMap.get(existingInstant.getTimestamp());
    List<String> fileListFromFs = partitionToFileIdAndNameMap.entrySet().stream()
        .flatMap(entry ->
            entry.getValue().stream()
                .map(fileInfo -> new StoragePath(entry.getKey(), fileInfo.getValue()).toString())
                .collect(Collectors.toList())
                .stream()
        ).collect(Collectors.toList());
    String danglingFilePath = new StoragePath("2022/01/02",
        getBaseFilename(existingInstant.getTimestamp(), UUID.randomUUID().toString())).toString();
    fileListFromFs.add(danglingFilePath);
    // Existing instant
    assertEquals(CollectionUtils.createImmutableList(danglingFilePath),
        RepairUtils.findInstantFilesToRemove(
            existingInstant.getTimestamp(), fileListFromFs,
            metaClient.getActiveTimeline(), metaClient.getArchivedTimeline()));
    // Non-existing instant
    assertEquals(fileListFromFs,
        RepairUtils.findInstantFilesToRemove(
            "004", fileListFromFs,
            metaClient.getActiveTimeline(), metaClient.getArchivedTimeline()));
  }
}
