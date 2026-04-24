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

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_PARSER;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.convertMetadataToByteArray;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests Clean action executor.
 */
public class TestCleanActionExecutor {

  private static final StorageConfiguration<?> CONF = getDefaultStorageConf();
  private final HoodieEngineContext context = new HoodieLocalEngineContext(CONF);
  private final HoodieTable<?, ?, ?, ?> mockHoodieTable = mock(HoodieTable.class, RETURNS_DEEP_STUBS);
  private HoodieTableMetaClient metaClient;
  private HoodieStorage storage;

  private static String PARTITION1 = "partition1";
  private static String PARTITION2 = "partition2";

  String earliestInstant = "20231204194919610";
  String earliestInstantMinusThreeDays = "20231201194919610";

  @BeforeEach
  void setUp() {
    metaClient = mock(HoodieTableMetaClient.class);
    when(mockHoodieTable.getMetaClient()).thenReturn(metaClient);
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    storage = spy(HoodieStorageUtils.getStorage(CONF));
    when(metaClient.getStorage()).thenReturn(storage);
    when(mockHoodieTable.getStorage()).thenReturn(storage);
  }

  @ParameterizedTest
  @EnumSource(CleanFailureType.class)
  void testPartialCleanFailure(CleanFailureType failureType) throws IOException {
    HoodieWriteConfig config = getCleanByCommitsConfig();
    String fileGroup = UUID.randomUUID() + "-0";
    HoodieBaseFile baseFile = new HoodieBaseFile(String.format("/tmp/base/%s_1-0-1_%s.parquet", fileGroup, "001"));
    HoodieStorage localStorage = HoodieStorageUtils.getStorage(baseFile.getPath(), CONF);
    StoragePath filePath = new StoragePath(baseFile.getPath());

    if (failureType == CleanFailureType.TRUE_ON_DELETE) {
      when(storage.deleteFile(filePath)).thenReturn(true);
    } else if (failureType == CleanFailureType.FALSE_ON_DELETE_IS_EXISTS_FALSE) {
      when(storage.deleteFile(filePath)).thenReturn(false);
      when(storage.exists(filePath)).thenReturn(false);
    } else if (failureType == CleanFailureType.FALSE_ON_DELETE_IS_EXISTS_TRUE) {
      when(storage.deleteFile(filePath)).thenReturn(false);
      when(storage.exists(filePath)).thenReturn(true);
    } else if (failureType == CleanFailureType.FILE_NOT_FOUND_EXC_ON_DELETE) {
      when(storage.deleteFile(filePath)).thenThrow(new FileNotFoundException("throwing file not found exception"));
    } else if (failureType == CleanFailureType.IO_EXCEPTION) {
      when(storage.deleteFile(filePath)).thenThrow(new IOException("throwing io exception"));
      when(storage.exists(filePath)).thenThrow(new IOException("throwing io exception"));
    } else if (failureType == CleanFailureType.IO_EXCEPTION_AND_EXISTS) {
      when(storage.deleteFile(filePath)).thenThrow(new IOException("throwing io exception"));
      when(storage.exists(filePath)).thenReturn(true);
    } else if (failureType == CleanFailureType.IO_EXCEPTION_BUT_NOT_EXISTS) {
      when(storage.deleteFile(filePath)).thenThrow(new IOException("throwing io exception"));
      when(storage.exists(filePath)).thenReturn(false);
    } else {
      // run time exception
      when(storage.deleteFile(filePath)).thenThrow(new RuntimeException("throwing run time exception"));
    }
    // we have to create the actual file after setting up mock logic for storage
    // otherwise the file created would be deleted when setting up because storage is a spy
    localStorage.create(filePath);

    Map<String, List<HoodieCleanFileInfo>> partitionCleanFileInfoMap = new HashMap<>();
    List<HoodieCleanFileInfo> cleanFileInfos = Collections.singletonList(new HoodieCleanFileInfo(baseFile.getPath(), false));
    partitionCleanFileInfoMap.put(PARTITION1, cleanFileInfos);
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant(earliestInstant, HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED.name()), earliestInstantMinusThreeDays,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name(), Collections.emptyMap(), CleanPlanner.LATEST_CLEAN_PLAN_VERSION, partitionCleanFileInfoMap, Collections.emptyList(), Collections.emptyMap());

    // add clean to the timeline.
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(mockHoodieTable.getActiveTimeline()).thenReturn(activeTimeline);
    HoodieInstant cleanInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "002");
    HoodieActiveTimeline cleanTimeline = mock(HoodieActiveTimeline.class);
    when(activeTimeline.getCleanerTimeline()).thenReturn(cleanTimeline);
    when(cleanTimeline.getInstants()).thenReturn(Collections.singletonList(cleanInstant));
    when(activeTimeline.readCleanerPlan(cleanInstant)).thenReturn(cleanerPlan);
    when(activeTimeline.readCleanerInfoAsBytes(cleanInstant)).thenReturn(Option.of(convertMetadataToByteArray(cleanerPlan)));

    when(mockHoodieTable.getCleanTimeline()).thenReturn(cleanTimeline);
    when(mockHoodieTable.getInstantGenerator()).thenReturn(INSTANT_GENERATOR);
    when(mockHoodieTable.getInstantFileNameGenerator()).thenReturn(INSTANT_FILE_NAME_GENERATOR);
    when(mockHoodieTable.getInstantFileNameParser()).thenReturn(INSTANT_FILE_NAME_PARSER);
    HoodieTimeline inflightsAndRequestedTimeline = mock(HoodieTimeline.class);
    when(cleanTimeline.filterInflightsAndRequested()).thenReturn(inflightsAndRequestedTimeline);
    when(inflightsAndRequestedTimeline.getInstants()).thenReturn(Collections.singletonList(cleanInstant));
    when(activeTimeline.transitionCleanRequestedToInflight(any())).thenReturn(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, "002"));
    when(mockHoodieTable.getMetadataWriter("002")).thenReturn(Option.empty());

    CleanActionExecutor cleanActionExecutor = new CleanActionExecutor(context, config, mockHoodieTable, "002");
    if (failureType == CleanFailureType.TRUE_ON_DELETE) {
      assertCleanExecutionSuccess(cleanActionExecutor, filePath);
    } else if (failureType == CleanFailureType.FALSE_ON_DELETE_IS_EXISTS_FALSE) {
      assertCleanExecutionSuccess(cleanActionExecutor, filePath);
    } else if (failureType == CleanFailureType.FALSE_ON_DELETE_IS_EXISTS_TRUE) {
      assertCleanExecutionFailure(cleanActionExecutor);
    } else if (failureType == CleanFailureType.FILE_NOT_FOUND_EXC_ON_DELETE) {
      assertCleanExecutionSuccess(cleanActionExecutor, filePath);
    } else if (failureType == CleanFailureType.IO_EXCEPTION) {
      assertCleanExecutionFailure(cleanActionExecutor);
    } else if (failureType == CleanFailureType.IO_EXCEPTION_AND_EXISTS) {
      assertCleanExecutionFailure(cleanActionExecutor);
    } else if (failureType == CleanFailureType.IO_EXCEPTION_BUT_NOT_EXISTS) {
      assertCleanExecutionSuccess(cleanActionExecutor, filePath);
    } else {
      // run time exception
      assertCleanExecutionFailure(cleanActionExecutor);
    }
  }

  private void assertCleanExecutionFailure(CleanActionExecutor cleanActionExecutor) {
    assertThrows(HoodieException.class, () -> {
      cleanActionExecutor.execute();
    });
  }

  private void assertCleanExecutionSuccess(CleanActionExecutor cleanActionExecutor, StoragePath filePath) {
    HoodieCleanMetadata cleanMetadata = cleanActionExecutor.execute();
    assertTrue(cleanMetadata.getPartitionMetadata().containsKey(PARTITION1));
    HoodieCleanPartitionMetadata cleanPartitionMetadata = cleanMetadata.getPartitionMetadata().get(PARTITION1);
    assertTrue(cleanPartitionMetadata.getDeletePathPatterns().contains(filePath.getName()));
  }

  private static HoodieWriteConfig getCleanByCommitsConfig() {
    return HoodieWriteConfig.newBuilder().withPath("/tmp")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
  }

  enum CleanFailureType {
    TRUE_ON_DELETE,
    FALSE_ON_DELETE_IS_EXISTS_FALSE,
    FALSE_ON_DELETE_IS_EXISTS_TRUE,
    FILE_NOT_FOUND_EXC_ON_DELETE,
    IO_EXCEPTION,
    IO_EXCEPTION_AND_EXISTS,
    IO_EXCEPTION_BUT_NOT_EXISTS,
    RUNTIME_EXC_ON_DELETE
  }

  @Test
  void testGetBlobInspectionGroups() {
    // two file groups, each with a removed slice ("001") and a retained slice ("002")
    HoodieFileGroupId fileGroupId1 = new HoodieFileGroupId(PARTITION1, UUID.randomUUID() + "-0");
    HoodieBaseFile baseFile1 = new HoodieBaseFile(baseFilePath(fileGroupId1.getFileId(), "001"));
    FileSlice fileSlice1 = new FileSlice(fileGroupId1, "001", baseFile1, Collections.emptyList());
    HoodieBaseFile baseFile2 = new HoodieBaseFile(baseFilePath(fileGroupId1.getFileId(), "002"));
    FileSlice fileSlice2 = new FileSlice(fileGroupId1, "002", baseFile2, Collections.emptyList());
    HoodieFileGroup fileGroup1 = mockFileGroup(fileGroupId1, Option.empty(), fileSlice1, fileSlice2);

    HoodieFileGroupId fileGroupId2 = new HoodieFileGroupId(PARTITION1, UUID.randomUUID() + "-0");
    HoodieBaseFile baseFile3 = new HoodieBaseFile(baseFilePath(fileGroupId2.getFileId(), "001"));
    FileSlice fileSlice3 = new FileSlice(fileGroupId2, "001", baseFile3, Collections.emptyList());
    HoodieBaseFile baseFile4 = new HoodieBaseFile(baseFilePath(fileGroupId2.getFileId(), "002"));
    HoodieLogFile logFile = new HoodieLogFile(String.format("/tmp/base/.%s_%s.log.1_1-0-1", fileGroupId2.getFileId(), "002"));
    FileSlice fileSlice4 = new FileSlice(fileGroupId2, "002", baseFile4, Collections.singletonList(logFile));
    HoodieFileGroup fileGroup2 = mockFileGroup(fileGroupId2, Option.empty(), fileSlice3, fileSlice4);

    when(mockHoodieTable.getHoodieView().getAllFileGroups(PARTITION1)).thenReturn(Stream.of(fileGroup1, fileGroup2));

    CleanActionExecutor cleanActionExecutor = new CleanActionExecutor(context, getCleanByCommitsConfig(), mockHoodieTable, "003");
    Map<String, List<HoodieCleanFileInfo>> partitionCleanFileInfoMap = new HashMap<>();
    partitionCleanFileInfoMap.put(PARTITION1, Arrays.asList(
        new HoodieCleanFileInfo(baseFile1.getPath(), false), new HoodieCleanFileInfo(baseFile3.getPath(), false)));
    List<CleanActionExecutor.FileComparisonGroup> grouping = cleanActionExecutor.getBlobInspectionGroups(buildCleanerPlan(partitionCleanFileInfoMap));

    CleanActionExecutor.FileComparisonGroup expectedGroup1 = CleanActionExecutor.FileComparisonGroup.builder()
        .removedFilePaths(Collections.singleton(baseFile1.getPath()))
        .retainedFilePaths(Collections.singleton(baseFile2.getPath())).build();
    CleanActionExecutor.FileComparisonGroup expectedGroup2 = CleanActionExecutor.FileComparisonGroup.builder()
        .removedFilePaths(Collections.singleton(baseFile3.getPath()))
        .retainedFilePaths(new HashSet<>(Arrays.asList(baseFile4.getPath(), logFile.getPath().toString()))).build();
    assertEquals(new HashSet<>(Arrays.asList(expectedGroup1, expectedGroup2)), new HashSet<>(grouping));
  }

  @Test
  void testGetBlobInspectionGroups_emptyCleanPlan() {
    CleanActionExecutor cleanActionExecutor = new CleanActionExecutor(context, getCleanByCommitsConfig(), mockHoodieTable, "003");
    List<CleanActionExecutor.FileComparisonGroup> grouping = cleanActionExecutor.getBlobInspectionGroups(buildCleanerPlan(Collections.emptyMap()));
    assertEquals(0, grouping.size());
  }

  @Test
  void testGetBlobInspectionGroups_replaceOrClustering() {
    // fileGroupA: one slice at "001" (removed), no retained slices → enters else branch
    HoodieFileGroupId fileGroupIdA = new HoodieFileGroupId(PARTITION1, UUID.randomUUID() + "-0");
    HoodieBaseFile baseFileA = new HoodieBaseFile(baseFilePath(fileGroupIdA.getFileId(), "001"));
    FileSlice fileSliceA = new FileSlice(fileGroupIdA, "001", baseFileA, Collections.emptyList());
    HoodieFileGroup fileGroupA = mockFileGroup(fileGroupIdA, Option.of(fileSliceA), fileSliceA);

    // fileGroupB: unmatched (not in clean plan), one slice at "002"
    HoodieFileGroupId fileGroupIdB = new HoodieFileGroupId(PARTITION1, UUID.randomUUID() + "-0");
    HoodieBaseFile baseFileB = new HoodieBaseFile(baseFilePath(fileGroupIdB.getFileId(), "002"));
    FileSlice fileSliceB = new FileSlice(fileGroupIdB, "002", baseFileB, Collections.emptyList());
    HoodieFileGroup fileGroupB = mockFileGroup(fileGroupIdB, Option.empty(), fileSliceB);

    when(mockHoodieTable.getHoodieView().getAllFileGroups(PARTITION1)).thenReturn(Stream.of(fileGroupA, fileGroupB));

    CleanActionExecutor cleanActionExecutor = new CleanActionExecutor(context, getCleanByCommitsConfig(), mockHoodieTable, "003");
    List<CleanActionExecutor.FileComparisonGroup> grouping = cleanActionExecutor.getBlobInspectionGroups(buildCleanerPlan(Collections.singletonMap(PARTITION1,
        Collections.singletonList(new HoodieCleanFileInfo(baseFileA.getPath(), false)))));

    // remainingFileComparisonGroupStream: removed=baseFileA, retained=baseFileB (from unmatched fileGroupB at "002" >= earliestBaseInstantTime "001")
    CleanActionExecutor.FileComparisonGroup replaceGroup = CleanActionExecutor.FileComparisonGroup.builder()
        .removedFilePaths(Collections.singleton(baseFileA.getPath()))
        .retainedFilePaths(Collections.singleton(baseFileB.getPath())).build();
    assertEquals(Collections.singleton(replaceGroup), new HashSet<>(grouping));
    assertEquals(1, grouping.size());
  }

  @Test
  void testGetBlobInspectionGroups_replaceOrClustering_unmatchedSliceFilteredByTime() {
    // Same replace/clustering scenario, but fileGroupC has a slice at "000" < earliestBaseInstantTime ("001") → excluded
    HoodieFileGroupId fileGroupIdA = new HoodieFileGroupId(PARTITION1, UUID.randomUUID() + "-0");
    HoodieBaseFile baseFileA = new HoodieBaseFile(baseFilePath(fileGroupIdA.getFileId(), "001"));
    FileSlice fileSliceA = new FileSlice(fileGroupIdA, "001", baseFileA, Collections.emptyList());
    HoodieFileGroup fileGroupA = mockFileGroup(fileGroupIdA, Option.of(fileSliceA), fileSliceA);

    // fileGroupB: unmatched, slice at "002" → included (002 >= earliestBaseInstantTime "001")
    HoodieFileGroupId fileGroupIdB = new HoodieFileGroupId(PARTITION1, UUID.randomUUID() + "-0");
    HoodieBaseFile baseFileB = new HoodieBaseFile(baseFilePath(fileGroupIdB.getFileId(), "002"));
    FileSlice fileSliceB = new FileSlice(fileGroupIdB, "002", baseFileB, Collections.emptyList());
    HoodieFileGroup fileGroupB = mockFileGroup(fileGroupIdB, Option.empty(), fileSliceB);

    // fileGroupC: unmatched, slice at "000" → excluded (000 < earliestBaseInstantTime "001")
    HoodieFileGroupId fileGroupIdC = new HoodieFileGroupId(PARTITION1, UUID.randomUUID() + "-0");
    HoodieBaseFile baseFileC = new HoodieBaseFile(baseFilePath(fileGroupIdC.getFileId(), "000"));
    FileSlice fileSliceC = new FileSlice(fileGroupIdC, "000", baseFileC, Collections.emptyList());
    HoodieFileGroup fileGroupC = mockFileGroup(fileGroupIdC, Option.empty(), fileSliceC);

    when(mockHoodieTable.getHoodieView().getAllFileGroups(PARTITION1)).thenReturn(Stream.of(fileGroupA, fileGroupB, fileGroupC));

    CleanActionExecutor cleanActionExecutor = new CleanActionExecutor(context, getCleanByCommitsConfig(), mockHoodieTable, "003");
    List<CleanActionExecutor.FileComparisonGroup> grouping = cleanActionExecutor.getBlobInspectionGroups(buildCleanerPlan(Collections.singletonMap(PARTITION1,
        Collections.singletonList(new HoodieCleanFileInfo(baseFileA.getPath(), false)))));

    // fileGroupC at "000" is excluded because "000" < earliestBaseInstantTime "001"; only fileGroupB at "002" contributes to retained
    CleanActionExecutor.FileComparisonGroup replaceGroup = CleanActionExecutor.FileComparisonGroup.builder()
        .removedFilePaths(Collections.singleton(baseFileA.getPath()))
        .retainedFilePaths(Collections.singleton(baseFileB.getPath())).build();
    assertEquals(Collections.singleton(replaceGroup), new HashSet<>(grouping));
    assertEquals(1, grouping.size());
  }

  @Test
  void testGetBlobInspectionGroups_noRetainedAndNoUnmatched() {
    // fileGroupA: one slice at "001" (removed), no unmatched file groups → both streams produce identical empty-retained groups
    HoodieFileGroupId fileGroupIdA = new HoodieFileGroupId(PARTITION1, UUID.randomUUID() + "-0");
    HoodieBaseFile baseFileA = new HoodieBaseFile(baseFilePath(fileGroupIdA.getFileId(), "001"));
    FileSlice fileSliceA = new FileSlice(fileGroupIdA, "001", baseFileA, Collections.emptyList());
    HoodieFileGroup fileGroupA = mockFileGroup(fileGroupIdA, Option.of(fileSliceA), fileSliceA);
    when(mockHoodieTable.getHoodieView().getAllFileGroups(PARTITION1)).thenReturn(Stream.of(fileGroupA));

    CleanActionExecutor cleanActionExecutor = new CleanActionExecutor(context, getCleanByCommitsConfig(), mockHoodieTable, "003");
    List<CleanActionExecutor.FileComparisonGroup> grouping = cleanActionExecutor.getBlobInspectionGroups(buildCleanerPlan(Collections.singletonMap(PARTITION1,
        Collections.singletonList(new HoodieCleanFileInfo(baseFileA.getPath(), false)))));

    // Both remainingFileComparisonGroupStream and regular stream produce the same group (no unmatched groups, no retained paths)
    CleanActionExecutor.FileComparisonGroup expectedGroup = CleanActionExecutor.FileComparisonGroup.builder()
        .removedFilePaths(Collections.singleton(baseFileA.getPath()))
        .retainedFilePaths(Collections.emptySet()).build();
    assertEquals(Collections.singleton(expectedGroup), new HashSet<>(grouping));
    assertEquals(1, grouping.size());
  }

  @Test
  void testGetBlobInspectionGroups_slicesAfterInstantTimeFiltered() {
    // fileGroupA: slices at "001" (removed) and "004" (not removed); instantTime="003" filters out "004"
    HoodieFileGroupId fileGroupIdA = new HoodieFileGroupId(PARTITION1, UUID.randomUUID() + "-0");
    HoodieBaseFile baseFileA = new HoodieBaseFile(baseFilePath(fileGroupIdA.getFileId(), "001"));
    FileSlice fileSliceAt001 = new FileSlice(fileGroupIdA, "001", baseFileA, Collections.emptyList());
    HoodieBaseFile baseFileB = new HoodieBaseFile(baseFilePath(fileGroupIdA.getFileId(), "004"));
    FileSlice fileSliceAt004 = new FileSlice(fileGroupIdA, "004", baseFileB, Collections.emptyList());
    HoodieFileGroup fileGroupA = mockFileGroup(fileGroupIdA, Option.of(fileSliceAt001), fileSliceAt001, fileSliceAt004);
    when(mockHoodieTable.getHoodieView().getAllFileGroups(PARTITION1)).thenReturn(Stream.of(fileGroupA));

    CleanActionExecutor cleanActionExecutor = new CleanActionExecutor(context, getCleanByCommitsConfig(), mockHoodieTable, "003");
    List<CleanActionExecutor.FileComparisonGroup> grouping = cleanActionExecutor.getBlobInspectionGroups(buildCleanerPlan(Collections.singletonMap(PARTITION1,
        Collections.singletonList(new HoodieCleanFileInfo(baseFileA.getPath(), false)))));

    // Slice at "004" is filtered out (004 > instantTime "003"), so baseFileB is NOT in retained paths
    // Both streams produce the same group with empty retained paths
    CleanActionExecutor.FileComparisonGroup expectedGroup = CleanActionExecutor.FileComparisonGroup.builder()
        .removedFilePaths(Collections.singleton(baseFileA.getPath()))
        .retainedFilePaths(Collections.emptySet()).build();
    assertEquals(Collections.singleton(expectedGroup), new HashSet<>(grouping));
    assertEquals(1, grouping.size());
  }

  @Test
  void testGetBlobInspectionGroups_multiplePartitions() {
    // PARTITION1: one slice at "001" (removed) and "002" (retained)
    HoodieFileGroupId fileGroupId1 = new HoodieFileGroupId(PARTITION1, UUID.randomUUID() + "-0");
    HoodieBaseFile baseFile1 = new HoodieBaseFile(baseFilePath(fileGroupId1.getFileId(), "001"));
    FileSlice fileSlice1 = new FileSlice(fileGroupId1, "001", baseFile1, Collections.emptyList());
    HoodieBaseFile baseFile2 = new HoodieBaseFile(baseFilePath(fileGroupId1.getFileId(), "002"));
    FileSlice fileSlice2 = new FileSlice(fileGroupId1, "002", baseFile2, Collections.emptyList());
    HoodieFileGroup fileGroup1 = mockFileGroup(fileGroupId1, Option.empty(), fileSlice1, fileSlice2);
    when(mockHoodieTable.getHoodieView().getAllFileGroups(PARTITION1)).thenReturn(Stream.of(fileGroup1));

    // PARTITION2: one slice at "001" (removed) and "002" (retained)
    HoodieFileGroupId fileGroupId3 = new HoodieFileGroupId(PARTITION2, UUID.randomUUID() + "-0");
    HoodieBaseFile baseFile3 = new HoodieBaseFile(baseFilePath(fileGroupId3.getFileId(), "001"));
    FileSlice fileSlice3 = new FileSlice(fileGroupId3, "001", baseFile3, Collections.emptyList());
    HoodieBaseFile baseFile4 = new HoodieBaseFile(baseFilePath(fileGroupId3.getFileId(), "002"));
    FileSlice fileSlice4 = new FileSlice(fileGroupId3, "002", baseFile4, Collections.emptyList());
    HoodieFileGroup fileGroup3 = mockFileGroup(fileGroupId3, Option.empty(), fileSlice3, fileSlice4);
    when(mockHoodieTable.getHoodieView().getAllFileGroups(PARTITION2)).thenReturn(Stream.of(fileGroup3));

    CleanActionExecutor cleanActionExecutor = new CleanActionExecutor(context, getCleanByCommitsConfig(), mockHoodieTable, "003");
    Map<String, List<HoodieCleanFileInfo>> partitionCleanFileInfoMap = new HashMap<>();
    partitionCleanFileInfoMap.put(PARTITION1, Collections.singletonList(new HoodieCleanFileInfo(baseFile1.getPath(), false)));
    partitionCleanFileInfoMap.put(PARTITION2, Collections.singletonList(new HoodieCleanFileInfo(baseFile3.getPath(), false)));
    List<CleanActionExecutor.FileComparisonGroup> grouping = cleanActionExecutor.getBlobInspectionGroups(buildCleanerPlan(partitionCleanFileInfoMap));

    CleanActionExecutor.FileComparisonGroup expectedGroup1 = CleanActionExecutor.FileComparisonGroup.builder()
        .removedFilePaths(Collections.singleton(baseFile1.getPath()))
        .retainedFilePaths(Collections.singleton(baseFile2.getPath())).build();
    CleanActionExecutor.FileComparisonGroup expectedGroup2 = CleanActionExecutor.FileComparisonGroup.builder()
        .removedFilePaths(Collections.singleton(baseFile3.getPath()))
        .retainedFilePaths(Collections.singleton(baseFile4.getPath())).build();
    assertEquals(new HashSet<>(Arrays.asList(expectedGroup1, expectedGroup2)), new HashSet<>(grouping));
    assertEquals(2, grouping.size());
  }

  private static String baseFilePath(String fileId, String commitTime) {
    return String.format("/tmp/base/%s_1-0-1_%s.parquet", fileId, commitTime);
  }

  private HoodieFileGroup mockFileGroup(HoodieFileGroupId fileGroupId, Option<FileSlice> latestFileSlice, FileSlice... slices) {
    HoodieFileGroup fg = mock(HoodieFileGroup.class);
    when(fg.getFileGroupId()).thenReturn(fileGroupId);
    when(fg.getAllFileSlices()).thenReturn(Arrays.stream(slices));
    when(fg.getLatestFileSlice()).thenReturn(latestFileSlice);
    return fg;
  }

  private HoodieCleanerPlan buildCleanerPlan(Map<String, List<HoodieCleanFileInfo>> partitionCleanFileInfoMap) {
    return new HoodieCleanerPlan(
        new HoodieActionInstant(earliestInstant, HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED.name()),
        earliestInstantMinusThreeDays,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name(),
        Collections.emptyMap(),
        CleanPlanner.LATEST_CLEAN_PLAN_VERSION,
        partitionCleanFileInfoMap,
        Collections.emptyList(),
        Collections.emptyMap());
  }
}
