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

package org.apache.hudi.table.action.clean;

import org.apache.hudi.avro.AvroRecordContext;
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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantReader;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.table.timeline.versioning.v2.BaseTimelineV2;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
    Pair<Boolean, List<CleanFileInfo>> actual = cleanPlanner.getDeletePaths(partitionPath, Option.of(earliestCommitToRetain), HoodieTestDataGenerator.HOODIE_SCHEMA);
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
    when(mockHoodieTable.getTableMetadata()).thenReturn(hoodieTableMetadata);
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

  private HoodieSchema createSimpleBlobRecordSchema() {
    List<HoodieSchemaField> fields = Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("blob_field", HoodieSchema.createBlob(), null, null));
    return HoodieSchema.createRecord("TestRecord", null, null, fields);
  }

  private static GenericRecord createInlineBlob(byte[] data) {
    GenericRecord blob = new GenericData.Record(HoodieSchema.createBlob().toAvroSchema());
    blob.put(HoodieSchema.Blob.TYPE, "INLINE");
    blob.put(HoodieSchema.Blob.INLINE_DATA_FIELD, ByteBuffer.wrap(data));
    blob.put(HoodieSchema.Blob.EXTERNAL_REFERENCE, null);
    return blob;
  }

  private static GenericRecord createExternalBlob(String path, boolean isManaged) {
    Schema refSchema = HoodieSchema.createBlob().getField("reference").get().schema().getNonNullType().toAvroSchema();
    GenericRecord reference = new GenericData.Record(refSchema);
    reference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH, path);
    reference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET, null);
    reference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH, null);
    reference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED, isManaged);

    GenericRecord blob = new GenericData.Record(HoodieSchema.createBlob().toAvroSchema());
    blob.put(HoodieSchema.Blob.TYPE, "OUT_OF_LINE");
    blob.put(HoodieSchema.Blob.INLINE_DATA_FIELD, null);
    blob.put(HoodieSchema.Blob.EXTERNAL_REFERENCE, reference);
    return blob;
  }

  @Test
  public void testGetFileSliceComparisonGroups_empty() {
    List<CleanPlanner.FileSliceComparisonGroup> result =
        CleanPlanner.getFileSliceComparisonGroups(Collections.emptyMap(), Collections.emptyMap());
    assertEquals(0, result.size());
  }

  @Test
  public void testGetFileSliceComparisonGroups_allHaveRetained() {
    HoodieFileGroupId fg1 = new HoodieFileGroupId(PARTITION1, "fg1");
    HoodieFileGroupId fg2 = new HoodieFileGroupId(PARTITION1, "fg2");

    FileSlice removed1 = new FileSlice(fg1, "001");
    FileSlice removed2 = new FileSlice(fg2, "002");
    FileSlice retained1 = new FileSlice(fg1, "003");
    FileSlice retained2 = new FileSlice(fg2, "004");
    FileSlice retained3 = new FileSlice(fg2, "005");

    Map<HoodieFileGroupId, List<FileSlice>> retained = new HashMap<>();
    retained.put(fg1, Collections.singletonList(retained1));
    retained.put(fg2, Arrays.asList(retained2, retained3));

    Map<HoodieFileGroupId, List<FileSlice>> removed = new HashMap<>();
    removed.put(fg1, Collections.singletonList(removed1));
    removed.put(fg2, Collections.singletonList(removed2));

    List<CleanPlanner.FileSliceComparisonGroup> result =
        CleanPlanner.getFileSliceComparisonGroups(retained, removed);
    assertEquals(2, result.size());

    CleanPlanner.FileSliceComparisonGroup expectedGroup1 = new CleanPlanner.FileSliceComparisonGroup(Collections.singleton(retained1), Collections.singleton(removed1));
    CleanPlanner.FileSliceComparisonGroup expectedGroup2 = new CleanPlanner.FileSliceComparisonGroup(Set.of(retained2, retained3), Collections.singleton(removed2));
    assertEquals(Set.of(expectedGroup1, expectedGroup2), new HashSet<>(result));
  }

  @Test
  public void testGetFileSliceComparisonGroups_noneHaveRetained() {
    HoodieFileGroupId fg1 = new HoodieFileGroupId(PARTITION1, "fg1");
    HoodieFileGroupId fg2 = new HoodieFileGroupId(PARTITION1, "fg2");
    HoodieFileGroupId fg3 = new HoodieFileGroupId(PARTITION1, "fg3");

    FileSlice removed1 = new FileSlice(fg1, "002");
    FileSlice removed2 = new FileSlice(fg2, "003");
    // fg3 created at "004" which is > min(002, 003) = "002"
    FileSlice retained3 = new FileSlice(fg3, "004");

    Map<HoodieFileGroupId, List<FileSlice>> retained = new HashMap<>();
    retained.put(fg3, Collections.singletonList(retained3));

    Map<HoodieFileGroupId, List<FileSlice>> removed = new HashMap<>();
    removed.put(fg1, Collections.singletonList(removed1));
    removed.put(fg2, Collections.singletonList(removed2));

    List<CleanPlanner.FileSliceComparisonGroup> result =
        CleanPlanner.getFileSliceComparisonGroups(retained, removed);
    assertEquals(1, result.size());

    CleanPlanner.FileSliceComparisonGroup expectedGroup = new CleanPlanner.FileSliceComparisonGroup(Collections.singleton(retained3), Set.of(removed1, removed2));
    assertEquals(expectedGroup, result.get(0));
  }

  @Test
  public void testGetFileSliceComparisonGroups_retainedCreatedBeforeThreshold() {
    HoodieFileGroupId fg1 = new HoodieFileGroupId(PARTITION1, "fg1");
    HoodieFileGroupId fg2 = new HoodieFileGroupId(PARTITION1, "fg2");
    HoodieFileGroupId fg3 = new HoodieFileGroupId(PARTITION1, "fg3");

    FileSlice removed1 = new FileSlice(fg1, "003");
    // Both fg2 and fg3 have base instants before "003" so they should be excluded
    FileSlice retainedBefore1 = new FileSlice(fg2, "002");
    FileSlice retainedBefore2 = new FileSlice(fg3, "001");

    Map<HoodieFileGroupId, List<FileSlice>> retained = new HashMap<>();
    retained.put(fg2, Collections.singletonList(retainedBefore1));
    retained.put(fg3, Collections.singletonList(retainedBefore2));

    Map<HoodieFileGroupId, List<FileSlice>> removed = new HashMap<>();
    removed.put(fg1, Collections.singletonList(removed1));

    List<CleanPlanner.FileSliceComparisonGroup> result =
        CleanPlanner.getFileSliceComparisonGroups(retained, removed);
    assertEquals(1, result.size());

    CleanPlanner.FileSliceComparisonGroup expectedGroup = new CleanPlanner.FileSliceComparisonGroup(Collections.emptySet(), Collections.singleton(removed1));
    assertEquals(expectedGroup, result.get(0));
  }

  @Test
  public void testGetFileSliceComparisonGroups_mixed() {
    // FG1: removed at "002", retained at "004" → direct pairing
    HoodieFileGroupId fg1 = new HoodieFileGroupId(PARTITION1, "fg1");
    // FG2: removed at "002", no retained counterpart
    HoodieFileGroupId fg2 = new HoodieFileGroupId(PARTITION1, "fg2");
    // FG3: retained at "005" (after threshold "002") → included in clustering group
    HoodieFileGroupId fg3 = new HoodieFileGroupId(PARTITION1, "fg3");
    // FG4: retained at "001" (before threshold "002") → excluded
    HoodieFileGroupId fg4 = new HoodieFileGroupId(PARTITION1, "fg4");

    FileSlice removed1 = new FileSlice(fg1, "002");
    FileSlice removed2 = new FileSlice(fg2, "002");
    FileSlice retained1 = new FileSlice(fg1, "004");
    FileSlice retained3 = new FileSlice(fg3, "005");
    FileSlice retained4 = new FileSlice(fg4, "001");

    Map<HoodieFileGroupId, List<FileSlice>> retained = new HashMap<>();
    retained.put(fg1, Collections.singletonList(retained1));
    retained.put(fg3, Collections.singletonList(retained3));
    retained.put(fg4, Collections.singletonList(retained4));

    Map<HoodieFileGroupId, List<FileSlice>> removed = new HashMap<>();
    removed.put(fg1, Collections.singletonList(removed1));
    removed.put(fg2, Collections.singletonList(removed2));

    List<CleanPlanner.FileSliceComparisonGroup> result =
        CleanPlanner.getFileSliceComparisonGroups(retained, removed);
    assertEquals(2, result.size());

    CleanPlanner.FileSliceComparisonGroup expectedDirectGroup = new CleanPlanner.FileSliceComparisonGroup(Collections.singleton(retained1), Collections.singleton(removed1));
    CleanPlanner.FileSliceComparisonGroup expectedClusteringGroup = new CleanPlanner.FileSliceComparisonGroup(Set.of(retained1, retained3), Collections.singleton(removed2));
    assertEquals(Set.of(expectedDirectGroup, expectedClusteringGroup), new HashSet<>(result));
  }

  @Test
  public void testGetFileSliceComparisonGroups_thresholdFromMinOfMaxInstants() {
    // FG1 (no retained): slices at "001" and "003" → max = "003"
    HoodieFileGroupId fg1 = new HoodieFileGroupId(PARTITION1, "fg1");
    // FG2 (no retained): slices at "001" and "005" → max = "005"
    HoodieFileGroupId fg2 = new HoodieFileGroupId(PARTITION1, "fg2");
    // threshold = min("003", "005") = "003"
    // FG3 retained at "004" (> "003") → included
    HoodieFileGroupId fg3 = new HoodieFileGroupId(PARTITION1, "fg3");
    // FG4 retained at "003" (not GREATER_THAN "003") → excluded
    HoodieFileGroupId fg4 = new HoodieFileGroupId(PARTITION1, "fg4");

    FileSlice removed1a = new FileSlice(fg1, "001");
    FileSlice removed1b = new FileSlice(fg1, "003");
    FileSlice removed2a = new FileSlice(fg2, "001");
    FileSlice removed2b = new FileSlice(fg2, "005");
    FileSlice retained3 = new FileSlice(fg3, "004");
    FileSlice retained4 = new FileSlice(fg4, "003");

    Map<HoodieFileGroupId, List<FileSlice>> retained = new HashMap<>();
    retained.put(fg3, Collections.singletonList(retained3));
    retained.put(fg4, Collections.singletonList(retained4));

    Map<HoodieFileGroupId, List<FileSlice>> removed = new HashMap<>();
    removed.put(fg1, Arrays.asList(removed1a, removed1b));
    removed.put(fg2, Arrays.asList(removed2a, removed2b));

    List<CleanPlanner.FileSliceComparisonGroup> result =
        CleanPlanner.getFileSliceComparisonGroups(retained, removed);
    assertEquals(1, result.size());

    // Only fg3 (at "004") is included; fg4 (at "003") is excluded because "003" is NOT > "003"
    CleanPlanner.FileSliceComparisonGroup expectedGroup = new CleanPlanner.FileSliceComparisonGroup(Collections.singleton(retained3), Set.of(removed1a, removed1b, removed2a, removed2b));
    assertEquals(expectedGroup, result.get(0));
  }

  private void assertPathsEqual(List<String> expected, List<String> actual) {
    assertEquals(expected.size(), actual.size(), "Expected and actual path lists should have the same size");
    assertEquals(new HashSet<>(expected), new HashSet<>(actual));
  }

  private static Stream<Arguments> testBasicBlobs() {
    return Stream.of(
        Arguments.of(
            createInlineBlob("test data".getBytes()),
            Collections.emptyList()
        ),
        Arguments.of(
            createExternalBlob("/hoodie/blobs/managed.bin", true),
            Collections.singletonList("/hoodie/blobs/managed.bin")
        ),
        Arguments.of(
            createExternalBlob("/external/unmanaged.bin", false),
            Collections.emptyList()
        ),
        Arguments.of(
            null,
            Collections.emptyList()
        )
    );
  }

  @ParameterizedTest
  @MethodSource
  void testBasicBlobs(GenericRecord inputBlob, List<String> expectedPaths) {
    HoodieSchema schema = createSimpleBlobRecordSchema();
    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blob_field", inputBlob);

    CleanPlanner cleanPlanner = new CleanPlanner(context, mockHoodieTable, getCleanByHoursConfig());
    AvroRecordContext recordContext = AvroRecordContext.getFieldAccessorInstance();
    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);
    assertPathsEqual(expectedPaths, paths);
  }

  private static Stream<Arguments> testMultipleBlobFields() {
    return Stream.of(
        Arguments.of(
            createInlineBlob("data1".getBytes()),
            createExternalBlob("/path/blob2.bin", true),
            createExternalBlob("/external/blob3.bin", false),
            Collections.singletonList("/path/blob2.bin")
        ),
        Arguments.of(
            createExternalBlob("/path/blob1.bin", true),
            null,
            createExternalBlob("/path/blob3.bin", true),
            Arrays.asList("/path/blob1.bin", "/path/blob3.bin")
        )
    );
  }

  @ParameterizedTest
  @MethodSource
  public void testMultipleBlobFields(GenericRecord blob1, GenericRecord blob2, GenericRecord blob3, List<String> expectedPaths) {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null,null));
    fields.add(HoodieSchemaField.of("blob1", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    fields.add(HoodieSchemaField.of("blob2", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    fields.add(HoodieSchemaField.of("blob3", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blob1", blob1);
    record.put("blob2", blob2);
    record.put("blob3", blob3);

    CleanPlanner cleanPlanner = new CleanPlanner(context, mockHoodieTable, getCleanByHoursConfig());
    AvroRecordContext recordContext = AvroRecordContext.getFieldAccessorInstance();
    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);
    assertPathsEqual(expectedPaths, paths);
  }

  @Test
  public void testNestedRecord() {
    // Level 0: innermost record with blob
    List<HoodieSchemaField> level0Fields = new ArrayList<>();
    level0Fields.add(HoodieSchemaField.of("data", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    level0Fields.add(HoodieSchemaField.of("blob_field", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    HoodieSchema level0Schema = HoodieSchema.createRecord("Level0Record", null, null, false, level0Fields);

    // Level 1: middle record
    List<HoodieSchemaField> level1Fields = new ArrayList<>();
    level1Fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    level1Fields.add(HoodieSchemaField.of("nested", HoodieSchema.createNullable(level0Schema), null, null));
    HoodieSchema level1Schema = HoodieSchema.createRecord("Level1Record", null, null, false, level1Fields);

    // Level 2: outer record
    List<HoodieSchemaField> level2Fields = new ArrayList<>();
    level2Fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    level2Fields.add(HoodieSchemaField.of("nested", HoodieSchema.createNullable(level1Schema), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("Level2Record", null, null, false, level2Fields);

    // Create nested records
    GenericRecord level0Record = new GenericData.Record(level0Schema.toAvroSchema());
    level0Record.put("data", "deep data");
    level0Record.put("blob_field", createExternalBlob("/nested/level2.bin", true));

    GenericRecord level1Record = new GenericData.Record(level1Schema.toAvroSchema());
    level1Record.put("id", "level1");
    level1Record.put("nested", level0Record);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "level2");
    record.put("nested", level1Record);

    CleanPlanner cleanPlanner = new CleanPlanner(context, mockHoodieTable, getCleanByHoursConfig());
    AvroRecordContext recordContext = AvroRecordContext.getFieldAccessorInstance();
    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);
    assertPathsEqual(Collections.singletonList("/nested/level2.bin"), paths);
  }

  private static Stream<Arguments> testArrayOfBlobs() {
    return Stream.of(
        Arguments.of(Arrays.asList(
                createExternalBlob("/array/blob1.bin", true),
                createExternalBlob("/array/blob2.bin", true),
                createExternalBlob("/array/blob3.bin", true)),
            Arrays.asList("/array/blob1.bin", "/array/blob2.bin", "/array/blob3.bin")),
        Arguments.of(Arrays.asList(
            createInlineBlob("data1".getBytes()),
            createExternalBlob("/array/managed.bin", true),
            createExternalBlob("/external/unmanaged.bin", false)
        ), Collections.singletonList("/array/managed.bin")),
        Arguments.of(Collections.emptyList(), Collections.emptyList()));
  }

  @ParameterizedTest
  @MethodSource
  public void testArrayOfBlobs(List<GenericRecord> blobs, List<String> expectedPaths) {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    fields.add(HoodieSchemaField.of("blobs", HoodieSchema.createArray(HoodieSchema.createBlob()), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blobs", blobs);

    CleanPlanner cleanPlanner = new CleanPlanner(context, mockHoodieTable, getCleanByHoursConfig());
    AvroRecordContext recordContext = AvroRecordContext.getFieldAccessorInstance();
    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);
    assertPathsEqual(expectedPaths, paths);
  }

  @Test
  public void testArrayOfRecords() {
    // Create record schema with blob
    List<HoodieSchemaField> itemFields = new ArrayList<>();
    itemFields.add(HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    itemFields.add(HoodieSchemaField.of("blob_field", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, null));
    HoodieSchema itemSchema = HoodieSchema.createRecord("ItemRecord", null, null, false, itemFields);

    // Create schema with array of records
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    fields.add(HoodieSchemaField.of("items", HoodieSchema.createArray(itemSchema), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    // Create records
    List<GenericRecord> items = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      GenericRecord item = new GenericData.Record(itemSchema.toAvroSchema());
      item.put("name", "item" + i);
      item.put("blob_field", createExternalBlob("/array/record/blob" + i + ".bin", true));
      items.add(item);
    }

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("items", items);

    CleanPlanner cleanPlanner = new CleanPlanner(context, mockHoodieTable, getCleanByHoursConfig());
    AvroRecordContext recordContext = AvroRecordContext.getFieldAccessorInstance();
    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);
    assertPathsEqual(Arrays.asList("/array/record/blob1.bin", "/array/record/blob2.bin", "/array/record/blob3.bin"), paths);
  }

  private static Stream<Arguments> testMapOfBlobs() {
    return Stream.of(
        Arguments.of(Map.of(
            "key1", createExternalBlob("/map/blob1.bin", true),
            "key2", createExternalBlob("/map/blob2.bin", true),
            "key3", createExternalBlob("/map/blob3.bin", true)
        ), Arrays.asList("/map/blob1.bin", "/map/blob2.bin", "/map/blob3.bin")),
        Arguments.of(Map.of(
            "key1", createInlineBlob("data1".getBytes()),
            "key2", createExternalBlob("/map/managed.bin", true),
            "key3", createExternalBlob("/external/unmanaged.bin", false)), Collections.singletonList("/map/managed.bin")),
        Arguments.of(Collections.emptyMap(), Collections.emptyList())
    );
  }

  @ParameterizedTest
  @MethodSource
  public void testMapOfBlobs(Map<String, GenericRecord> blobMap, List<String> expectedPaths) {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null));
    fields.add(HoodieSchemaField.of("blob_map", HoodieSchema.createMap(HoodieSchema.createBlob()), null, null));
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, false, fields);

    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "test123");
    record.put("blob_map", blobMap);

    CleanPlanner cleanPlanner = new CleanPlanner(context, mockHoodieTable, getCleanByHoursConfig());
    AvroRecordContext recordContext = AvroRecordContext.getFieldAccessorInstance();
    List<String> paths = cleanPlanner.getManagedBlobPaths(schema, record, recordContext);
    assertPathsEqual(expectedPaths, paths);
  }
}
