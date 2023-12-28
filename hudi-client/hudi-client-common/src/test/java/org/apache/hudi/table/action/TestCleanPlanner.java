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

package org.apache.hudi.table.action;

import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.avro.model.HoodieSavepointPartitionMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.CleanFileInfo;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.clean.CleanPlanner;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCleanPlanner {
  private static final Configuration CONF = new Configuration();
  private final HoodieEngineContext context = new HoodieLocalEngineContext(CONF);

  private final HoodieTable<?, ?, ?, ?> mockHoodieTable = mock(HoodieTable.class);

  private SyncableFileSystemView mockFsView;

  @BeforeEach
  void setUp() {
    mockFsView = mock(SyncableFileSystemView.class);
    when(mockHoodieTable.getHoodieView()).thenReturn(mockFsView);
    SyncableFileSystemView sliceView = mock(SyncableFileSystemView.class);
    when(mockHoodieTable.getSliceView()).thenReturn(sliceView);
    when(sliceView.getPendingCompactionOperations()).thenReturn(Stream.empty());
    when(sliceView.getPendingLogCompactionOperations()).thenReturn(Stream.empty());
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(mockHoodieTable.getMetaClient()).thenReturn(metaClient);
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    HoodieTimeline mockCompletedCommitsTimeline = mock(HoodieTimeline.class);
    when(mockCompletedCommitsTimeline.countInstants()).thenReturn(10);
    when(mockHoodieTable.getCompletedCommitsTimeline()).thenReturn(mockCompletedCommitsTimeline);
  }

  @ParameterizedTest
  @MethodSource("testCases")
  void testGetDeletePaths(HoodieWriteConfig config, String earliestInstant, List<HoodieFileGroup> allFileGroups, List<Pair<String, Option<byte[]>>> savepoints,
                         List<HoodieFileGroup> replacedFileGroups, Pair<Boolean, List<CleanFileInfo>> expected) {

    // setup savepoint mocks
    Set<String> savepointTimestamps = savepoints.stream().map(Pair::getLeft).collect(Collectors.toSet());
    when(mockHoodieTable.getSavepointTimestamps()).thenReturn(savepointTimestamps);
    if (!savepoints.isEmpty()) {
      HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
      when(mockHoodieTable.getActiveTimeline()).thenReturn(activeTimeline);
      for (Pair<String, Option<byte[]>> savepoint : savepoints) {
        HoodieInstant instant = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepoint.getLeft());
        when(activeTimeline.getInstantDetails(instant)).thenReturn(savepoint.getRight());
      }
    }
    String partitionPath = "partition1";
    // setup replaced file groups mocks
    if (config.getCleanerPolicy() == HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS) {
      when(mockFsView.getAllReplacedFileGroups(partitionPath)).thenReturn(replacedFileGroups.stream());
    } else {
      when(mockFsView.getReplacedFileGroupsBefore(earliestInstant, partitionPath)).thenReturn(replacedFileGroups.stream());
    }
    // setup current file groups mocks
    when(mockFsView.getAllFileGroupsStateless(partitionPath)).thenReturn(allFileGroups.stream());

    CleanPlanner<?, ?, ?, ?> cleanPlanner = new CleanPlanner<>(context, mockHoodieTable, config);
    HoodieInstant earliestCommitToRetain = new HoodieInstant(HoodieInstant.State.COMPLETED, "COMMIT", earliestInstant);
    Pair<Boolean, List<CleanFileInfo>> actual = cleanPlanner.getDeletePaths(partitionPath, Option.of(earliestCommitToRetain));
    assertEquals(expected, actual);
  }

  static Stream<Arguments> testCases() {
    return Stream.concat(keepLatestByHoursOrCommitsArgs(), keepLatestVersionsArgs());
  }

  static Stream<Arguments> keepLatestVersionsArgs() {
    HoodieWriteConfig keepLatestVersionsConfig = HoodieWriteConfig.newBuilder().withPath("/tmp")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .retainFileVersions(2)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS)
            .build())
        .build();
    String instant1 = "20231205194919610";
    String instant2 = "20231204194919610";
    String instant3 = "20231201194919610";
    String instant4 = "20231127194919610";
    List<Arguments> arguments = new ArrayList<>();
    // Two file slices in the group: both should be retained
    arguments.add(Arguments.of(
        keepLatestVersionsConfig,
        instant1,
        Collections.singletonList(buildFileGroup(Arrays.asList(instant2, instant1))),
        Collections.emptyList(),
        Collections.emptyList(),
        Pair.of(false, Collections.emptyList())));
    // Four file slices in the group: only the latest two should be retained
    HoodieFileGroup fileGroup = buildFileGroup(Arrays.asList(instant4, instant3, instant2, instant1));
    String instant3Path = fileGroup.getAllBaseFiles()
        .filter(baseFile -> baseFile.getCommitTime().equals(instant3)).findFirst().get().getPath();
    CleanFileInfo expectedCleanFileInfoForInstant3 = new CleanFileInfo(instant3Path, false);
    String instant4Path = fileGroup.getAllBaseFiles()
        .filter(baseFile -> baseFile.getCommitTime().equals(instant4)).findFirst().get().getPath();
    CleanFileInfo expectedCleanFileInfoForInstant4 = new CleanFileInfo(instant4Path, false);
    arguments.add(Arguments.of(
        keepLatestVersionsConfig,
        instant1,
        Collections.singletonList(fileGroup),
        Collections.emptyList(),
        Collections.emptyList(),
        Pair.of(false, Arrays.asList(expectedCleanFileInfoForInstant3, expectedCleanFileInfoForInstant4))));
    // Four file slices in group but instant4 is part of savepiont: only instant 3's files should be cleaned
    List<Pair<String, Option<byte[]>>> savepoints = Collections.singletonList(Pair.of(instant4, getSavepointBytes("partition1", Collections.singletonList(instant4Path))));
    arguments.add(Arguments.of(
        keepLatestVersionsConfig,
        instant1,
        Collections.singletonList(fileGroup),
        savepoints,
        Collections.emptyList(),
        Pair.of(false, Arrays.asList(expectedCleanFileInfoForInstant3))));
    // Two file slices with a replaced file group: only replaced files cleaned up
    HoodieFileGroup replacedFileGroup = buildFileGroup(Collections.singletonList(instant4));
    String replacedFilePath = replacedFileGroup.getAllBaseFiles().findFirst().get().getPath();
    CleanFileInfo expectedReplaceCleanFileInfo = new CleanFileInfo(replacedFilePath, false);
    arguments.add(Arguments.of(
        keepLatestVersionsConfig,
        instant1,
        Collections.singletonList(buildFileGroup(Arrays.asList(instant2, instant1))),
        Collections.emptyList(),
        Collections.singletonList(replacedFileGroup),
        Pair.of(false, Collections.singletonList(expectedReplaceCleanFileInfo))));
    // replaced file groups referenced by savepoint should not be cleaned up
    List<Pair<String, Option<byte[]>>> replacedFileGroupSavepoint = Collections.singletonList(Pair.of(instant4, getSavepointBytes("partition1", Collections.singletonList(replacedFilePath))));
    arguments.add(Arguments.of(
        keepLatestVersionsConfig,
        instant1,
        Collections.singletonList(buildFileGroup(Arrays.asList(instant2, instant1))),
        replacedFileGroupSavepoint,
        Collections.singletonList(replacedFileGroup),
        Pair.of(false, Collections.emptyList())));
    return arguments.stream();
  }

  static Stream<Arguments> keepLatestByHoursOrCommitsArgs() {
    String earliestInstant = "20231204194919610";
    String earliestInstantPlusTwoDays = "20231205194919610";
    String earliestInstantMinusThreeDays = "20231201194919610";
    String earliestInstantMinusOneWeek = "20231127194919610";
    String earliestInstantMinusOneMonth = "20231104194919610";
    List<Arguments> arguments = new ArrayList<>();
    // Only one file slice in the group: should still be kept even with commit earlier than "earliestInstant"
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsCases(
        earliestInstant,
        Collections.singletonList(buildFileGroup(Collections.singletonList(earliestInstantMinusOneMonth))),
        Collections.emptyList(),
        Collections.emptyList(),
        Pair.of(false, Collections.emptyList())));
    // File group with two slices, both are before the earliestInstant. Only the latest slice should be kept.
    HoodieFileGroup fileGroupsBeforeInstant = buildFileGroup(Arrays.asList(earliestInstantMinusOneMonth, earliestInstantMinusOneWeek));
    CleanFileInfo expectedCleanFileInfoForFirstFile = new CleanFileInfo(fileGroupsBeforeInstant.getAllBaseFiles()
        .filter(baseFile -> baseFile.getCommitTime().equals(earliestInstantMinusOneMonth)).findFirst().get().getPath(), false);
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsCases(
        earliestInstant,
        Collections.singletonList(fileGroupsBeforeInstant),
        Collections.emptyList(),
        Collections.emptyList(),
        Pair.of(false, Collections.singletonList(expectedCleanFileInfoForFirstFile))));
    // File group with two slices, one is after the earliestInstant and the other is before the earliestInstant.
    // We should keep both since base files are required for queries evaluating the table at time NOW - 24hrs (24hrs is configured for test)
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsCases(
        earliestInstant,
        Collections.singletonList(buildFileGroup(Arrays.asList(earliestInstantMinusOneMonth, earliestInstantPlusTwoDays))),
        Collections.emptyList(),
        Collections.emptyList(),
        Pair.of(false, Collections.emptyList())));
    // File group with three slices, one is after the earliestInstant and the other two are before the earliestInstant.
    // Oldest slice will be removed since it is not required for queries evaluating the table at time NOW - 24hrs
    String oldestFileInstant = earliestInstantMinusOneMonth;
    HoodieFileGroup fileGroup = buildFileGroup(Arrays.asList(oldestFileInstant, earliestInstantMinusThreeDays, earliestInstantPlusTwoDays));
    String oldestFilePath = fileGroup.getAllBaseFiles().filter(baseFile -> baseFile.getCommitTime().equals(oldestFileInstant)).findFirst().get().getPath();
    CleanFileInfo expectedCleanFileInfo = new CleanFileInfo(oldestFilePath, false);
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsCases(
        earliestInstant,
        Collections.singletonList(fileGroup),
        Collections.emptyList(),
        Collections.emptyList(),
        Pair.of(false, Collections.singletonList(expectedCleanFileInfo))));
    // File group with three slices, one is after the earliestInstant and the other two are before the earliestInstant. Oldest slice is also in savepoint so should not be removed.
    List<Pair<String, Option<byte[]>>> savepoints = Collections.singletonList(Pair.of(oldestFileInstant, getSavepointBytes("partition1", Collections.singletonList(oldestFilePath))));
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsCases(
        earliestInstant,
        Collections.singletonList(fileGroup),
        savepoints,
        Collections.emptyList(),
        Pair.of(false, Collections.emptyList())));
    // File group is replaced before the earliestInstant. Should be removed.
    HoodieFileGroup replacedFileGroup = buildFileGroup(Collections.singletonList(earliestInstantMinusOneMonth));
    String replacedFilePath = replacedFileGroup.getAllBaseFiles().findFirst().get().getPath();
    CleanFileInfo expectedReplaceCleanFileInfo = new CleanFileInfo(replacedFilePath, false);
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsCases(
        earliestInstant,
        Collections.singletonList(buildFileGroup(Collections.singletonList(earliestInstantMinusOneMonth))),
        Collections.emptyList(),
        Collections.singletonList(replacedFileGroup),
        Pair.of(false, Collections.singletonList(expectedReplaceCleanFileInfo))));
    // File group is replaced before the earliestInstant but referenced in a savepoint. Should be retained.
    List<Pair<String, Option<byte[]>>> savepointsForReplacedGroup = Collections.singletonList(Pair.of(oldestFileInstant,
        getSavepointBytes("partition1", Collections.singletonList(replacedFilePath))));
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsCases(
        earliestInstant,
        Collections.singletonList(buildFileGroup(Collections.singletonList(earliestInstantMinusOneMonth))),
        savepointsForReplacedGroup,
        Collections.singletonList(replacedFileGroup),
        Pair.of(false, Collections.emptyList())));
    // Clean by commits but there are not enough commits in timeline to trigger cleaner
    HoodieWriteConfig writeConfigWithLargerRetention = HoodieWriteConfig.newBuilder().withPath("/tmp")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .retainCommits(50)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .build())
        .build();
    arguments.add(Arguments.of(
        writeConfigWithLargerRetention,
        earliestInstant,
        Collections.singletonList(buildFileGroup(Collections.singletonList(earliestInstantMinusOneMonth))),
        Collections.emptyList(),
        Collections.singletonList(replacedFileGroup),
        Pair.of(false, Collections.emptyList())));

    return arguments.stream();
  }

  private static HoodieWriteConfig getCleanByHoursConfig() {
    return HoodieWriteConfig.newBuilder().withPath("/tmp")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .cleanerNumHoursRetained(24)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS)
            .build())
        .build();
  }

  private static HoodieWriteConfig getCleanByCommitsConfig() {
    return HoodieWriteConfig.newBuilder().withPath("/tmp")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .retainCommits(5)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .build())
        .build();
  }

  // helper to build common cases for the two policies
  private static List<Arguments> buildArgumentsForCleanByHoursAndCommitsCases(String earliestInstant, List<HoodieFileGroup> allFileGroups, List<Pair<String, Option<byte[]>>> savepoints,
                                                                              List<HoodieFileGroup> replacedFileGroups, Pair<Boolean, List<CleanFileInfo>> expected) {
    return Arrays.asList(Arguments.of(getCleanByHoursConfig(), earliestInstant, allFileGroups, savepoints, replacedFileGroups, expected),
        Arguments.of(getCleanByCommitsConfig(), earliestInstant, allFileGroups, savepoints, replacedFileGroups, expected));
  }

  private static HoodieFileGroup buildFileGroup(List<String> baseFileCommitTimes) {
    String fileGroup = UUID.randomUUID() + "-0";
    HoodieFileGroupId fileGroupId =  new HoodieFileGroupId("partition1", UUID.randomUUID().toString());
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    when(timeline.lastInstant()).thenReturn(Option.of(new HoodieInstant(HoodieInstant.State.COMPLETED, "COMMIT", baseFileCommitTimes.get(baseFileCommitTimes.size() - 1))));
    HoodieFileGroup group = new HoodieFileGroup(fileGroupId, timeline);
    for (String baseFileCommitTime : baseFileCommitTimes) {
      when(timeline.containsOrBeforeTimelineStarts(baseFileCommitTime)).thenReturn(true);
      HoodieBaseFile baseFile = new HoodieBaseFile(String.format("file:///tmp/base/%s_1-0-1_%s.parquet", fileGroup, baseFileCommitTime));
      group.addBaseFile(baseFile);
    }
    return group;
  }

  private static Option<byte[]> getSavepointBytes(String partition, List<String> paths) {
    try {
      Map<String, HoodieSavepointPartitionMetadata> partitionMetadata = new HashMap<>();
      List<String> fileNames = paths.stream().map(path -> path.substring(path.lastIndexOf("/") + 1)).collect(Collectors.toList());
      partitionMetadata.put(partition, new HoodieSavepointPartitionMetadata(partition, fileNames));
      HoodieSavepointMetadata savepointMetadata =
          new HoodieSavepointMetadata("user", 1L, "comments", partitionMetadata, 1);
      return TimelineMetadataUtils.serializeSavepointMetadata(savepointMetadata);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }
}
