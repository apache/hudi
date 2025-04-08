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

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.avro.model.HoodieSavepointPartitionMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.CleanFileInfo;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantReader;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.table.timeline.versioning.v2.BaseTimelineV2;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.clean.CleanPlanner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
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

import static org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED;
import static org.apache.hudi.common.testutils.HoodieTestUtils.COMMIT_METADATA_SER_DE;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_PARSER;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.common.util.CleanerUtils.CLEAN_METADATA_VERSION_2;
import static org.apache.hudi.common.util.CleanerUtils.SAVEPOINTED_TIMESTAMPS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCleanPlanner {
  private static final StorageConfiguration<?> CONF = getDefaultStorageConf();
  private final HoodieEngineContext context = new HoodieLocalEngineContext(CONF);

  private final HoodieTable<?, ?, ?, ?> mockHoodieTable = mock(HoodieTable.class);

  private static final String PARTITION1 = "partition1";
  private static final String PARTITION2 = "partition2";
  private static final String PARTITION3 = "partition3";

  @BeforeEach
  void setUp() {
    SyncableFileSystemView sliceView = mock(SyncableFileSystemView.class);
    when(mockHoodieTable.getSliceView()).thenReturn(sliceView);
    when(sliceView.getPendingCompactionOperations()).thenReturn(Stream.empty());
    when(sliceView.getPendingLogCompactionOperations()).thenReturn(Stream.empty());
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(mockHoodieTable.getMetaClient()).thenReturn(metaClient);
    TimelineLayout layout = mock(TimelineLayout.class);
    when(metaClient.getTimelineLayout()).thenReturn(layout);
    when(layout.getCommitMetadataSerDe()).thenReturn(COMMIT_METADATA_SER_DE);
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    HoodieTimeline mockCompletedCommitsTimeline = mock(HoodieTimeline.class);
    when(mockCompletedCommitsTimeline.countInstants()).thenReturn(10);
    when(mockHoodieTable.getCompletedCommitsTimeline()).thenReturn(mockCompletedCommitsTimeline);
  }

  @ParameterizedTest
  @MethodSource("testCases")
  void testGetDeletePaths(HoodieWriteConfig config, String earliestInstant, List<HoodieFileGroup> allFileGroups, List<Pair<String, HoodieSavepointMetadata>> savepoints,
                          List<HoodieFileGroup> replacedFileGroups, Pair<Boolean, List<CleanFileInfo>> expected) throws IOException {

    SyncableFileSystemView mockFsView = mock(SyncableFileSystemView.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockHoodieTable.getMetaClient()).thenReturn(mockMetaClient);
    when(mockHoodieTable.getHoodieView()).thenReturn(mockFsView);
    when(mockHoodieTable.getInstantGenerator()).thenReturn(INSTANT_GENERATOR);
    when(mockHoodieTable.getInstantFileNameGenerator()).thenReturn(INSTANT_FILE_NAME_GENERATOR);
    when(mockHoodieTable.getInstantFileNameParser()).thenReturn(INSTANT_FILE_NAME_PARSER);

    // setup savepoint mocks
    Set<String> savepointTimestamps = savepoints.stream().map(Pair::getLeft).collect(Collectors.toSet());
    when(mockHoodieTable.getSavepointTimestamps()).thenReturn(savepointTimestamps);
    if (!savepoints.isEmpty()) {
      HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
      when(mockHoodieTable.getActiveTimeline()).thenReturn(activeTimeline);
      for (Pair<String, HoodieSavepointMetadata> savepoint : savepoints) {
        HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, savepoint.getLeft());
        when(activeTimeline.readSavepointMetadata(instant)).thenReturn(savepoint.getRight());
        when(mockMetaClient.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, savepoint.getLeft())).thenReturn(instant);
      }
    }
    String partitionPath = "partition1";
    // setup replaced file groups mocks
    if (config.getCleanerPolicy() == HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS) {
      when(mockHoodieTable.getHoodieView()).thenReturn(mockFsView); // requires extra reference when looking up latest versions
      when(mockFsView.getAllReplacedFileGroups(partitionPath)).thenReturn(replacedFileGroups.stream());
    } else {
      when(mockFsView.getReplacedFileGroupsBefore(earliestInstant, partitionPath)).thenReturn(replacedFileGroups.stream());
    }
    // setup current file groups mocks
    when(mockFsView.getAllFileGroupsStateless(partitionPath)).thenReturn(allFileGroups.stream());

    CleanPlanner<?, ?, ?, ?> cleanPlanner = new CleanPlanner<>(context, mockHoodieTable, config);
    HoodieInstant earliestCommitToRetain = INSTANT_GENERATOR.createNewInstant(COMPLETED, "COMMIT", earliestInstant);
    Pair<Boolean, List<CleanFileInfo>> actual = cleanPlanner.getDeletePaths(partitionPath, Option.of(earliestCommitToRetain));
    assertEquals(expected, actual);
  }

  /**
   * The test asserts clean planner results for APIs org.apache.hudi.table.action.clean.CleanPlanner#getEarliestSavepoint()
   * and org.apache.hudi.table.action.clean.CleanPlanner#getPartitionPathsToClean(org.apache.hudi.common.util.Option)
   * given the other input arguments for clean metadata. The idea is the two API results should match the expected results.
   */
  @ParameterizedTest
  @MethodSource("incrCleaningPartitionsTestCases")
  void testPartitionsForIncrCleaning(boolean isPartitioned, HoodieWriteConfig config, String earliestInstant,
                                     String lastCompletedTimeInLastClean, String lastCleanInstant, String earliestInstantsInLastClean, List<String> partitionsInLastClean,
                                     Map<String, List<String>> savepointsTrackedInLastClean, Option<String> expectedEarliestSavepointInLastClean,
                                     Map<String, List<String>> activeInstantsPartitions, List<String> replaceCommits, List<String> expectedPartitions, boolean areCommitsForSavepointsRemoved,
                                     Map<String, List<String>> savepoints) throws IOException, IllegalAccessException {
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieInstantReader instantReader = mock(HoodieInstantReader.class);
    when(mockHoodieTable.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.getInstantReader()).thenReturn(instantReader);
    // setup savepoint mocks
    Set<String> savepointTimestamps = savepoints.keySet().stream().collect(Collectors.toSet());
    when(mockHoodieTable.getSavepointTimestamps()).thenReturn(savepointTimestamps);
    when(mockHoodieTable.getInstantGenerator()).thenReturn(INSTANT_GENERATOR);
    when(mockHoodieTable.getInstantFileNameGenerator()).thenReturn(INSTANT_FILE_NAME_GENERATOR);
    when(mockHoodieTable.getInstantFileNameParser()).thenReturn(INSTANT_FILE_NAME_PARSER);
    if (!savepoints.isEmpty()) {
      for (Map.Entry<String, List<String>> entry : savepoints.entrySet()) {
        HoodieSavepointMetadata savepointMetadata = getSavepointMetadata(entry.getValue());
        HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, entry.getKey());
        when(activeTimeline.readSavepointMetadata(instant)).thenReturn(savepointMetadata);
      }
    }

    // prepare last Clean Metadata
    HoodieCleanMetadata cleanMetadata =
        getCleanCommitMetadata(partitionsInLastClean, lastCleanInstant, earliestInstantsInLastClean, lastCompletedTimeInLastClean,
            savepointsTrackedInLastClean.keySet(), expectedEarliestSavepointInLastClean);
    HoodieCleanerPlan cleanerPlan = mockLastCleanCommit(mockHoodieTable, lastCleanInstant, earliestInstantsInLastClean, activeTimeline, cleanMetadata, savepointsTrackedInLastClean.keySet());
    mockFewActiveInstants(mockHoodieTable, activeTimeline, activeInstantsPartitions, savepointsTrackedInLastClean, areCommitsForSavepointsRemoved, replaceCommits);

    // mock getAllPartitions
    HoodieStorage storage = mock(HoodieStorage.class);
    when(mockHoodieTable.getStorage()).thenReturn(storage);
    HoodieTableMetadata hoodieTableMetadata = mock(HoodieTableMetadata.class);
    when(mockHoodieTable.getMetadataTable()).thenReturn(hoodieTableMetadata);
    when(mockHoodieTable.getCleanTimeline().filterCompletedInstants().lastInstant())
        .thenReturn(Option.of(INSTANT_GENERATOR.createNewInstant(COMPLETED, HoodieTimeline.CLEAN_ACTION, lastCleanInstant)));
    when(hoodieTableMetadata.getAllPartitionPaths()).thenReturn(isPartitioned ? Arrays.asList(PARTITION1, PARTITION2, PARTITION3) : Collections.singletonList(StringUtils.EMPTY_STRING));

    // Trigger clean and validate partitions to clean and earliest savepoint
    CleanPlanner<?, ?, ?, ?> cleanPlanner = new CleanPlanner<>(context, mockHoodieTable, config);
    HoodieInstant earliestCommitToRetain = INSTANT_GENERATOR.createNewInstant(COMPLETED, "COMMIT", earliestInstant);
    List<String> partitionsToClean = cleanPlanner.getPartitionPathsToClean(Option.of(earliestCommitToRetain));
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    assertEquals(expectedEarliestSavepointInLastClean, ClusteringUtils.getEarliestReplacedSavepointInClean(activeTimeline, config.getCleanerPolicy(), cleanerPlan));

    Collections.sort(expectedPartitions);
    Collections.sort(partitionsToClean);
    assertEquals(expectedPartitions, partitionsToClean);
  }

  static Stream<Arguments> testCases() {
    return Stream.concat(keepLatestByHoursOrCommitsArgs(), keepLatestVersionsArgs());
  }

  static Stream<Arguments> incrCleaningPartitionsTestCases() {
    return keepLatestByHoursOrCommitsArgsIncrCleanPartitions();
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
    List<Pair<String, HoodieSavepointMetadata>> savepoints = Collections.singletonList(Pair.of(instant4, getSavepointBytes("partition1", Collections.singletonList(instant4Path))));
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
    List<Pair<String, HoodieSavepointMetadata>> replacedFileGroupSavepoint = Collections.singletonList(Pair.of(instant4, getSavepointBytes("partition1", Collections.singletonList(replacedFilePath))));
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
    List<Pair<String, HoodieSavepointMetadata>> savepoints = Collections.singletonList(Pair.of(oldestFileInstant, getSavepointBytes("partition1", Collections.singletonList(oldestFilePath))));
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
    List<Pair<String, HoodieSavepointMetadata>> savepointsForReplacedGroup = Collections.singletonList(Pair.of(oldestFileInstant,
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

  static Stream<Arguments> keepLatestByHoursOrCommitsArgsIncrCleanPartitions() {
    String earliestInstant = "20231204194919610";
    String earliestInstantPlusTwoDays = "20231206194919610";
    String lastCleanInstant = earliestInstantPlusTwoDays;
    String earliestInstantMinusThreeDays = "20231201194919610";
    String earliestInstantMinusFourDays = "20231130194919610";
    String earliestInstantMinusFiveDays = "20231129194919610";
    String earliestInstantMinusSixDays = "20231128194919610";
    String earliestInstantInLastClean = earliestInstantMinusSixDays;
    String lastCompletedInLastClean = earliestInstantMinusSixDays;
    String earliestInstantMinusOneWeek = "20231127194919610";
    String savepoint2 = earliestInstantMinusOneWeek;
    String earliestInstantMinusOneMonth = "20231104194919610";
    String savepoint3 = earliestInstantMinusOneMonth;

    List<String> threePartitionsInActiveTimeline = Arrays.asList(PARTITION1, PARTITION2, PARTITION3);
    Map<String, List<String>> activeInstantsPartitionsMap3 = new HashMap<>();
    activeInstantsPartitionsMap3.put(earliestInstantMinusThreeDays, threePartitionsInActiveTimeline);
    activeInstantsPartitionsMap3.put(earliestInstantMinusFourDays, threePartitionsInActiveTimeline);
    activeInstantsPartitionsMap3.put(earliestInstantMinusFiveDays, threePartitionsInActiveTimeline);

    List<String> twoPartitionsInActiveTimeline = Arrays.asList(PARTITION2, PARTITION3);
    Map<String, List<String>> activeInstantsPartitionsMap2 = new HashMap<>();
    activeInstantsPartitionsMap2.put(earliestInstantMinusThreeDays, twoPartitionsInActiveTimeline);
    activeInstantsPartitionsMap2.put(earliestInstantMinusFourDays, twoPartitionsInActiveTimeline);
    activeInstantsPartitionsMap2.put(earliestInstantMinusFiveDays, twoPartitionsInActiveTimeline);

    List<Arguments> arguments = new ArrayList<>();

    // Code snippet below generates different test cases which will assert the clean planner output
    // for these cases. There is no sequential dependency between these cases and they can be considered
    // independent.
    // In the test cases below earliestInstant signifies the new value of earliest instant to retain as computed
    // by CleanPlanner. The test case provides current state of the table and compare the expected values for
    // earliestSavepointInLastClean and expectedPartitions against computed values for the same by clean planner

    // no savepoints tracked in last clean and no additional savepoints. all partitions in uncleaned instants should be expected
    // earliest savepoint in last clean is empty since there is no savepoint in the timeline
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsIncrCleanPartitionsCases(true,
        earliestInstant, lastCompletedInLastClean, lastCleanInstant, earliestInstantInLastClean, Collections.singletonList(PARTITION1), Collections.emptyMap(), Option.empty(),
        activeInstantsPartitionsMap3, Collections.emptyList(), threePartitionsInActiveTimeline, false, Collections.emptyMap()));

    // a new savepoint is added after last clean. but rest of uncleaned touches all partitions, and so all partitions are expected
    // earliest savepoint in last clean is empty even though savepoint2 was added to the timeline
    // since there is no replace commit after the savepoint and before earliestInstantInLastClean (earliestInstantToRetain)
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsIncrCleanPartitionsCases(true,
        earliestInstant, lastCompletedInLastClean, lastCleanInstant, earliestInstantInLastClean, Collections.singletonList(PARTITION1),
        Collections.singletonMap(savepoint2, Collections.singletonList(PARTITION1)), Option.empty(), activeInstantsPartitionsMap3,
        Collections.emptyList(), threePartitionsInActiveTimeline, false, Collections.singletonMap(savepoint2, Collections.singletonList(PARTITION1))));

    // a new savepoint is added after last clean. but rest of uncleaned touches all partitions, and so all partitions are expected
    // earliest savepoint in last clean is empty even though savepoint2 was added to the timeline. This is because there
    // there are no replace commits between the savepoint and earliestInstant which is the earliestInstantToRetain
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsIncrCleanPartitionsCases(true,
        earliestInstant, lastCompletedInLastClean, lastCleanInstant, earliestInstantInLastClean, Collections.singletonList(PARTITION1), Collections.emptyMap(), Option.empty(),
        activeInstantsPartitionsMap3, Collections.emptyList(), threePartitionsInActiveTimeline, false,
        Collections.singletonMap(savepoint2, Collections.singletonList(PARTITION1))));

    // previous clean tracks a savepoint which exists in timeline still. only 2 partitions are touched by uncleaned instants. only 2 partitions are expected
    // earliest savepoint in last clean is empty even though savepoint2 was added to the timeline
    // since there is no replace commit after the savepoint and before earliestInstantInLastClean (earliestInstantToRetain)
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsIncrCleanPartitionsCases(true,
        earliestInstant, lastCompletedInLastClean, lastCleanInstant, earliestInstantInLastClean, Collections.singletonList(PARTITION1),
        Collections.singletonMap(savepoint2, Collections.singletonList(PARTITION1)), Option.empty(),
        activeInstantsPartitionsMap2, Collections.emptyList(), twoPartitionsInActiveTimeline, false,
        Collections.singletonMap(savepoint2, Collections.singletonList(PARTITION1))));

    // savepoint tracked in previous clean was removed(touching partition1). latest uncleaned touched 2 other partitions. But when savepoint is removed, entire list of partitions are expected.
    // earliest savepoint in last clean is empty since there is no savepoint in the timeline
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsIncrCleanPartitionsCases(true,
        earliestInstant, lastCompletedInLastClean, lastCleanInstant, earliestInstantInLastClean, Collections.singletonList(PARTITION1),
        Collections.singletonMap(savepoint2, Collections.singletonList(PARTITION1)), Option.empty(),
        activeInstantsPartitionsMap2, Collections.emptyList(), threePartitionsInActiveTimeline, false, Collections.emptyMap()));

    // previous savepoint still exists and touches partition1. uncleaned touches only partition2 and partition3. expected partition2 and partition3.
    // earliest savepoint in last clean is empty even though savepoint2 was added to the timeline
    // since there is no replace commit after the savepoint and before earliestInstantInLastClean (earliestInstantToRetain)
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsIncrCleanPartitionsCases(true,
        earliestInstant, lastCompletedInLastClean, lastCleanInstant, earliestInstantInLastClean, Collections.singletonList(PARTITION1),
        Collections.singletonMap(savepoint2, Collections.singletonList(PARTITION1)), Option.empty(),
        activeInstantsPartitionsMap2, Collections.singletonList(earliestInstantMinusThreeDays), twoPartitionsInActiveTimeline, false,
        Collections.singletonMap(savepoint2, Collections.singletonList(PARTITION1))));

    // a new savepoint was added compared to previous clean. all 2 partitions are expected since uncleaned commits touched just 2 partitions.
    // earliest savepoint in last clean is same as savepoint3 since savepoint3 is the oldest savepoint timestamp in the timeline
    // and there is a replace commit earliestInstantMinusOneWeek after the savepoint but before earliestInstantInLastClean (earliestInstantToRetain)
    Map<String, List<String>> latestSavepoints = new HashMap<>();
    latestSavepoints.put(savepoint3, Collections.singletonList(PARTITION1));
    latestSavepoints.put(savepoint2, Collections.singletonList(PARTITION1));
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsIncrCleanPartitionsCases(true,
        earliestInstant, lastCompletedInLastClean, lastCleanInstant, earliestInstantInLastClean, Collections.singletonList(PARTITION1),
        Collections.singletonMap(savepoint3, Collections.singletonList(PARTITION1)), Option.of(savepoint3),
        activeInstantsPartitionsMap2, Collections.singletonList(earliestInstantMinusOneWeek), twoPartitionsInActiveTimeline, false, latestSavepoints));

    // 2 savepoints were tracked in previous clean. one of them is removed in latest. When savepoint is removed, entire list of partitions are expected.
    // earliest savepoint in last clean is same as savepoint3 since savepoint3 is part of the timeline
    // and there is a replace commit earliestInstantMinusOneWeek after the savepoint but before earliestInstantInLastClean (earliestInstantToRetain)
    Map<String, List<String>> previousSavepoints = new HashMap<>();
    previousSavepoints.put(savepoint2, Collections.singletonList(PARTITION1));
    previousSavepoints.put(savepoint3, Collections.singletonList(PARTITION2));
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsIncrCleanPartitionsCases(true,
        earliestInstant, lastCompletedInLastClean, lastCleanInstant, earliestInstantInLastClean, Collections.singletonList(PARTITION1),
        previousSavepoints, Option.of(savepoint3), activeInstantsPartitionsMap2, Collections.singletonList(earliestInstantMinusOneWeek), threePartitionsInActiveTimeline, false,
        Collections.singletonMap(savepoint3, Collections.singletonList(PARTITION2))
    ));

    // 2 savepoints were tracked in previous clean. one of them is removed in latest. But a partition part of removed savepoint is already touched by uncleaned commits.
    // Anyways, when savepoint is removed, entire list of partitions are expected.
    // earliest savepoint in last clean is empty even though savepoint3 is part of the timeline
    // since there is no replace commit between the savepoint3 and earliestInstantInLastClean (earliestInstantToRetain)
    // The replace commit earliestInstantMinusThreeDays is after the earliestInstantInLastClean
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsIncrCleanPartitionsCases(true,
        earliestInstant, lastCompletedInLastClean, lastCleanInstant, earliestInstantInLastClean, Collections.singletonList(PARTITION1),
        previousSavepoints, Option.empty(), activeInstantsPartitionsMap3, Collections.singletonList(earliestInstantMinusThreeDays), threePartitionsInActiveTimeline, false,
        Collections.singletonMap(savepoint3, Collections.singletonList(PARTITION2))
    ));

    // unpartitioned test case. savepoint removed.
    List<String> unPartitionsInActiveTimeline = Arrays.asList(StringUtils.EMPTY_STRING);
    Map<String, List<String>> activeInstantsUnPartitionsMap = new HashMap<>();
    activeInstantsUnPartitionsMap.put(earliestInstantMinusThreeDays, unPartitionsInActiveTimeline);

    // earliest savepoint in last clean is empty since there is no savepoint in the timeline
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsIncrCleanPartitionsCases(false,
        earliestInstant, lastCompletedInLastClean, lastCleanInstant, earliestInstantInLastClean, Collections.singletonList(StringUtils.EMPTY_STRING),
        Collections.singletonMap(savepoint2, Collections.singletonList(StringUtils.EMPTY_STRING)), Option.empty(),
        activeInstantsUnPartitionsMap, Collections.emptyList(), unPartitionsInActiveTimeline, false, Collections.emptyMap()));

    // savepoint tracked in previous clean was removed(touching partition1). active instants does not have the instant corresponding to the savepoint.
    // latest uncleaned touched 2 other partitions. But when savepoint is removed, entire list of partitions are expected.
    // earliest savepoint in last clean is empty since there is no savepoint in the timeline
    activeInstantsPartitionsMap2.remove(earliestInstantMinusOneWeek);
    arguments.addAll(buildArgumentsForCleanByHoursAndCommitsIncrCleanPartitionsCases(true,
        earliestInstant, lastCompletedInLastClean, lastCleanInstant, earliestInstantInLastClean, Collections.singletonList(PARTITION1),
        Collections.singletonMap(savepoint2, Collections.singletonList(PARTITION1)), Option.empty(),
        activeInstantsPartitionsMap2, Collections.emptyList(), threePartitionsInActiveTimeline, true, Collections.emptyMap()));

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
  private static List<Arguments> buildArgumentsForCleanByHoursAndCommitsCases(String earliestInstant, List<HoodieFileGroup> allFileGroups, List<Pair<String, HoodieSavepointMetadata>> savepoints,
                                                                              List<HoodieFileGroup> replacedFileGroups, Pair<Boolean, List<CleanFileInfo>> expected) {
    return Arrays.asList(Arguments.of(getCleanByHoursConfig(), earliestInstant, allFileGroups, savepoints, replacedFileGroups, expected),
        Arguments.of(getCleanByCommitsConfig(), earliestInstant, allFileGroups, savepoints, replacedFileGroups, expected));
  }

  /**
   * The function generates test arguments for two cases. One where cleaning policy
   * is set to KEEP_LATEST_BY_HOURS and the other where cleaning policy is set to
   * KEEP_LATEST_COMMITS. Rest of the arguments are same.
   */
  private static List<Arguments> buildArgumentsForCleanByHoursAndCommitsIncrCleanPartitionsCases(boolean isPartitioned, String earliestInstant,
                                                                                                 String latestCompletedInLastClean,
                                                                                                 String lastKnownCleanInstantTime,
                                                                                                 String earliestInstantInLastClean,
                                                                                                 List<String> partitionsInLastClean,
                                                                                                 Map<String, List<String>> savepointsTrackedInLastClean,
                                                                                                 Option<String> expectedEarliestSavepointInLastClean,
                                                                                                 Map<String, List<String>> activeInstantsToPartitionsMap,
                                                                                                 List<String> replaceCommits,
                                                                                                 List<String> expectedPartitions,
                                                                                                 boolean areCommitsForSavepointsRemoved,
                                                                                                 Map<String, List<String>> savepoints) {
    return Arrays.asList(Arguments.of(isPartitioned, getCleanByHoursConfig(), earliestInstant, latestCompletedInLastClean, lastKnownCleanInstantTime,
            earliestInstantInLastClean, partitionsInLastClean, savepointsTrackedInLastClean, expectedEarliestSavepointInLastClean,
            activeInstantsToPartitionsMap, replaceCommits, expectedPartitions, areCommitsForSavepointsRemoved, savepoints),
        Arguments.of(isPartitioned, getCleanByCommitsConfig(), earliestInstant, latestCompletedInLastClean, lastKnownCleanInstantTime,
            earliestInstantInLastClean, partitionsInLastClean, savepointsTrackedInLastClean, expectedEarliestSavepointInLastClean,
            activeInstantsToPartitionsMap, replaceCommits, expectedPartitions, areCommitsForSavepointsRemoved, savepoints));
  }

  private static HoodieFileGroup buildFileGroup(List<String> baseFileCommitTimes) {
    return buildFileGroup(baseFileCommitTimes, PARTITION1);
  }

  private static HoodieFileGroup buildFileGroup(List<String> baseFileCommitTimes, String partition) {
    String fileGroup = UUID.randomUUID() + "-0";
    HoodieFileGroupId fileGroupId = new HoodieFileGroupId(partition, UUID.randomUUID().toString());
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    when(timeline.lastInstant()).thenReturn(Option.of(INSTANT_GENERATOR.createNewInstant(COMPLETED, "COMMIT", baseFileCommitTimes.get(baseFileCommitTimes.size() - 1))));
    HoodieFileGroup group = new HoodieFileGroup(fileGroupId, timeline);
    for (String baseFileCommitTime : baseFileCommitTimes) {
      when(timeline.containsOrBeforeTimelineStarts(baseFileCommitTime)).thenReturn(true);
      HoodieBaseFile baseFile = new HoodieBaseFile(String.format("file:///tmp/base/%s_1-0-1_%s.parquet", fileGroup, baseFileCommitTime));
      group.addBaseFile(baseFile);
    }
    return group;
  }

  private static HoodieSavepointMetadata getSavepointBytes(String partition, List<String> paths) {
    Map<String, HoodieSavepointPartitionMetadata> partitionMetadata = new HashMap<>();
    List<String> fileNames = paths.stream().map(path -> path.substring(path.lastIndexOf("/") + 1)).collect(Collectors.toList());
    partitionMetadata.put(partition, new HoodieSavepointPartitionMetadata(partition, fileNames));
    return new HoodieSavepointMetadata("user", 1L, "comments", partitionMetadata, 1);
  }

  private static HoodieCleanMetadata getCleanCommitMetadata(List<String> partitions, String instantTime, String earliestCommitToRetain,
                                                                                  String lastCompletedTime, Set<String> savepointsToTrack, Option<String> earliestCommitToNotArchive) {
    Map<String, HoodieCleanPartitionMetadata> partitionMetadata = new HashMap<>();
    partitions.forEach(partition -> partitionMetadata.put(partition, new HoodieCleanPartitionMetadata(partition, HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name(),
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false)));
    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(SAVEPOINTED_TIMESTAMPS, savepointsToTrack.stream().collect(Collectors.joining(",")));
    return new HoodieCleanMetadata(instantTime, 100L, 10, earliestCommitToRetain, lastCompletedTime, partitionMetadata,
        CLEAN_METADATA_VERSION_2, Collections.EMPTY_MAP, extraMetadata.isEmpty() ? null : extraMetadata);
  }

  private static HoodieSavepointMetadata getSavepointMetadata(List<String> partitions) {
    Map<String, HoodieSavepointPartitionMetadata> partitionMetadata = new HashMap<>();
    partitions.forEach(partition -> partitionMetadata.put(partition, new HoodieSavepointPartitionMetadata(partition, Collections.emptyList())));
    return new HoodieSavepointMetadata("user", 1L, "comments", partitionMetadata, 1);
  }

  private static HoodieCleanerPlan mockLastCleanCommit(HoodieTable hoodieTable, String timestamp, String earliestCommitToRetain, HoodieActiveTimeline activeTimeline,
                                                       HoodieCleanMetadata cleanMetadata, Set<String> savepointsTrackedInLastClean)
      throws IOException {
    BaseTimelineV2 cleanTimeline = mock(BaseTimelineV2.class);
    when(activeTimeline.getCleanerTimeline()).thenReturn(cleanTimeline);
    when(hoodieTable.getCleanTimeline()).thenReturn(cleanTimeline);
    BaseTimelineV2 completedCleanTimeline = mock(BaseTimelineV2.class);
    when(cleanTimeline.filterCompletedInstants()).thenReturn(completedCleanTimeline);
    HoodieInstant latestCleanInstant = INSTANT_GENERATOR.createNewInstant(COMPLETED, HoodieTimeline.CLEAN_ACTION, timestamp);
    when(completedCleanTimeline.lastInstant()).thenReturn(Option.of(latestCleanInstant));
    when(activeTimeline.isEmpty(latestCleanInstant)).thenReturn(false);
    when(activeTimeline.readCleanMetadata(latestCleanInstant)).thenReturn(cleanMetadata);
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant(earliestCommitToRetain, HoodieTimeline.COMMIT_ACTION, COMPLETED.name()),
        cleanMetadata.getLastCompletedCommitTimestamp(),
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name(), Collections.emptyMap(),
        CleanPlanner.LATEST_CLEAN_PLAN_VERSION, null, null, cleanMetadata.getExtraMetadata());

    BaseTimelineV2 commitsTimeline = mock(BaseTimelineV2.class);
    when(activeTimeline.getCommitsTimeline()).thenReturn(commitsTimeline);
    when(commitsTimeline.isBeforeTimelineStarts(earliestCommitToRetain)).thenReturn(false);

    when(hoodieTable.isPartitioned()).thenReturn(true);
    when(hoodieTable.isMetadataTable()).thenReturn(false);
    return cleanerPlan;
  }

  private static void mockFewActiveInstants(HoodieTable hoodieTable, HoodieActiveTimeline activeTimeline, Map<String, List<String>> activeInstantsToPartitions,
                                            Map<String, List<String>> savepointedCommitsToAdd, boolean areCommitsForSavepointsRemoved, List<String> replaceCommits) {
    BaseTimelineV2 commitsTimeline = new BaseTimelineV2();
    List<HoodieInstant> instants = new ArrayList<>();
    Map<String, List<String>> instantstoProcess = new HashMap<>();
    instantstoProcess.putAll(activeInstantsToPartitions);
    if (!areCommitsForSavepointsRemoved) {
      instantstoProcess.putAll(savepointedCommitsToAdd);
    }
    instantstoProcess.forEach((k, v) -> {
      HoodieInstant hoodieInstant = INSTANT_GENERATOR.createNewInstant(COMPLETED, HoodieTimeline.COMMIT_ACTION, k);
      instants.add(hoodieInstant);
      Map<String, List<HoodieWriteStat>> partitionToWriteStats = new HashMap<>();
      v.forEach(partition -> partitionToWriteStats.put(partition, Collections.emptyList()));
      HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
      v.forEach(partition -> {
        commitMetadata.getPartitionToWriteStats().put(partition, Collections.emptyList());
      });
      try {
        when(hoodieTable.getActiveTimeline().readCommitMetadata(hoodieInstant)).thenReturn(
            commitMetadata);
      } catch (IOException e) {
        throw new RuntimeException("Should not have failed", e);
      }
    });
    commitsTimeline.setInstants(instants);
    Collections.sort(instants);
    mock(HoodieTableMetaClient.class);
    when(hoodieTable.getMetaClient().getCommitMetadataSerDe()).thenReturn(COMMIT_METADATA_SER_DE);
    when(hoodieTable.getActiveTimeline().getInstantsAsStream()).thenReturn(instants.stream());
    when(hoodieTable.getCompletedCommitsTimeline()).thenReturn(commitsTimeline);

    BaseTimelineV2 savepointTimeline = new BaseTimelineV2();
    List<HoodieInstant> savepointInstants = savepointedCommitsToAdd.keySet().stream().map(sp -> INSTANT_GENERATOR.createNewInstant(COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, sp))
        .collect(Collectors.toList());
    savepointTimeline.setInstants(savepointInstants);

    BaseTimelineV2 completedReplaceTimeline = new BaseTimelineV2();
    List<HoodieInstant> completedReplaceInstants = replaceCommits.stream().map(rc -> INSTANT_GENERATOR.createNewInstant(COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, rc))
        .collect(Collectors.toList());
    completedReplaceTimeline.setInstants(completedReplaceInstants);
    when(activeTimeline.getCompletedReplaceTimeline()).thenReturn(completedReplaceTimeline);
    when(activeTimeline.getSavePointTimeline()).thenReturn(savepointTimeline);
    when(hoodieTable.isPartitioned()).thenReturn(true);
    when(hoodieTable.isMetadataTable()).thenReturn(false);
  }
}
