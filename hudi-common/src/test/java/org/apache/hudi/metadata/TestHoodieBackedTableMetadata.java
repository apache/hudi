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

package org.apache.hudi.metadata;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.LatestFileSliceCacheForPartition.CacheKey;

import com.github.benmanes.caffeine.cache.Cache;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodieBackedTableMetadata {

  private static final String RECORD_INDEX_PARTITION = MetadataPartitionType.RECORD_INDEX.getPartitionPath();
  private static final String INSTANT_TIME = "000111";
  private static final int METADATA_FILE_GROUP_COUNT = 3;

  private MockedStatic<LatestFileSliceCacheForPartition> mockedFileSliceCache;
  private MockedStatic<HoodieTableMetadataUtil> mockedMetadataUtil;

  private HoodieTableFileSystemView fileSystemView;
  private HoodieTableMetaClient dataTableMetaClient;

  // Tracking flags for verifying cache usage
  private boolean[] cacheUsed;
  private boolean[] nonCachedPathUsed;
  private boolean[] loaderFunctionCalled;

  // Tracking list for file slices passed to lookupKeysFromFileSlice
  private List<FileSlice> fileSlicesToLookUp;

  @BeforeEach
  void setUp() {
    mockedFileSliceCache = Mockito.mockStatic(LatestFileSliceCacheForPartition.class);
    mockedMetadataUtil = Mockito.mockStatic(HoodieTableMetadataUtil.class);

    fileSystemView = mock(HoodieTableFileSystemView.class);
    dataTableMetaClient = createDataTableMetaClient();

    cacheUsed = new boolean[] {false};
    nonCachedPathUsed = new boolean[] {false};
    loaderFunctionCalled = new boolean[] {false};
    fileSlicesToLookUp = new ArrayList<>();

    // Mock getBaseFileReader to capture file slices passed to lookupKeysFromFileSlice
    mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.getBaseFileReader(
            any(FileSlice.class), any(), any(), any()))
        .thenAnswer(invocation -> {
          FileSlice slice = invocation.getArgument(0);
          fileSlicesToLookUp.add(slice);
          // Return null to stop execution (readers not available in unit test)
          return null;
        });
  }

  @AfterEach
  void tearDown() {
    if (mockedFileSliceCache != null) {
      mockedFileSliceCache.close();
    }
    if (mockedMetadataUtil != null) {
      mockedMetadataUtil.close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRliLookupWithFileSliceCacheConfig(boolean enableCache) throws Exception {
    HoodieMetadataConfig metadataConfig = createMetadataConfig(enableCache);
    HoodieTableMetaClient metadataMetaClient = createMetadataTableMetaClient(Option.of(INSTANT_TIME));
    List<FileSlice> fileSlices = createFileSlices(RECORD_INDEX_PARTITION, METADATA_FILE_GROUP_COUNT);

    setupCacheMock(createCacheMock(fileSlices, RECORD_INDEX_PARTITION));
    setupNonCachedPathMock(RECORD_INDEX_PARTITION, fileSlices);

    int index = 1;
    mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(any(String.class), anyInt()))
        .thenReturn(index);

    ConcurrentHashMap<String, List<FileSlice>> partitionFileSliceMap = new ConcurrentHashMap<>();
    HoodieBackedTableMetadata metadata = createMockedMetadataInstance(
        metadataConfig, metadataMetaClient, partitionFileSliceMap);

    invokeReadRecordIndexSafely(metadata, Arrays.asList("key1", "key2", "key3"));

    if (enableCache) {
      assertTrue(cacheUsed[0], "Cache should be used when enabled for RLI lookup");
      assertFalse(nonCachedPathUsed[0], "Non-cached path should not be used when cache is enabled");
    } else {
      assertFalse(cacheUsed[0], "Cache should not be used when disabled");
      assertTrue(nonCachedPathUsed[0], "Non-cached path should be used when cache is disabled");
    }

    assertFileSlicesStored(partitionFileSliceMap, RECORD_INDEX_PARTITION, fileSlices);
    assertFileSlicesToLookUp(Collections.singletonList(fileSlices.get(index)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testPartitionedRliLookupFiltersFileSlicesByPartition(boolean enableCache) throws Exception {
    String dataPartitionName = "2026-01-01";
    String otherPartitionName = "2026-01-02";

    HoodieMetadataConfig metadataConfig = createMetadataConfig(enableCache);
    HoodieTableMetaClient metadataMetaClient = createMetadataTableMetaClient(Option.of(INSTANT_TIME));

    String fileId1 = dataPartitionName + "_fileId1";
    String fileId2 = dataPartitionName + "_fileId2";
    String fileId3 = dataPartitionName + "_fileId3";
    String fileId4 = otherPartitionName + "_fileId1";
    String fileId5 = otherPartitionName + "_fileId2";
    String fileId6 = otherPartitionName + "_fileId3";

    FileSlice fileSlice1 = new FileSlice(RECORD_INDEX_PARTITION, INSTANT_TIME, fileId1);
    FileSlice fileSlice2 = new FileSlice(RECORD_INDEX_PARTITION, INSTANT_TIME, fileId2);
    FileSlice fileSlice3 = new FileSlice(RECORD_INDEX_PARTITION, INSTANT_TIME, fileId3);
    FileSlice fileSlice4 = new FileSlice(RECORD_INDEX_PARTITION, INSTANT_TIME, fileId4);
    FileSlice fileSlice5 = new FileSlice(RECORD_INDEX_PARTITION, INSTANT_TIME, fileId5);
    FileSlice fileSlice6 = new FileSlice(RECORD_INDEX_PARTITION, INSTANT_TIME, fileId6);
    List<FileSlice> allFileSlices = Arrays.asList(
        fileSlice1, fileSlice2, fileSlice3, fileSlice4, fileSlice5, fileSlice6);
    setupCacheMock(createCacheMock(allFileSlices, RECORD_INDEX_PARTITION));
    setupNonCachedPathMock(RECORD_INDEX_PARTITION, allFileSlices);

    mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.getDataTablePartitionNameFromFileGroupName(fileId1))
        .thenReturn(dataPartitionName);
    mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.getDataTablePartitionNameFromFileGroupName(fileId2))
        .thenReturn(dataPartitionName);
    mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.getDataTablePartitionNameFromFileGroupName(fileId3))
        .thenReturn(dataPartitionName);
    mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.getDataTablePartitionNameFromFileGroupName(fileId4))
        .thenReturn(otherPartitionName);
    mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.getDataTablePartitionNameFromFileGroupName(fileId5))
        .thenReturn(otherPartitionName);
    mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.getDataTablePartitionNameFromFileGroupName(fileId6))
        .thenReturn(otherPartitionName);
    mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(eq("key1"), anyInt()))
        .thenReturn(1);
    mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.verifyRLIFile(any(String.class), eq(true)))
        .thenReturn(true);

    ConcurrentHashMap<String, List<FileSlice>> partitionFileSliceMap = new ConcurrentHashMap<>();
    HoodieBackedTableMetadata metadata = createMockedMetadataInstance(
        metadataConfig, metadataMetaClient, partitionFileSliceMap);

    invokeGetRecordsByKeysSafely(metadata, Collections.singletonList("key1"), RECORD_INDEX_PARTITION, Option.of(otherPartitionName));

    assertFileSlicesStored(partitionFileSliceMap, RECORD_INDEX_PARTITION, allFileSlices);
    assertFileSlicesToLookUp(Collections.singletonList(fileSlice5));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testNonRliPartitionDoesNotUseCacheRegardlessOfConfig(boolean cacheEnabled) throws Exception {
    String nonRliPartition = MetadataPartitionType.FILES.getPartitionPath();

    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    when(metadataConfig.shouldEnableFileSliceCacheOptimizationForRliLookup()).thenReturn(cacheEnabled);

    HoodieTableMetaClient metadataMetaClient = createMetadataTableMetaClient(Option.of(INSTANT_TIME));
    List<FileSlice> fileSlices = createFileSlices(nonRliPartition, 1);

    setupCacheMock(createCacheMock(fileSlices, nonRliPartition));
    setupNonCachedPathMock(nonRliPartition, fileSlices);

    ConcurrentHashMap<String, List<FileSlice>> partitionFileSliceMap = new ConcurrentHashMap<>();
    HoodieBackedTableMetadata metadata = createMockedMetadataInstance(
        metadataConfig, metadataMetaClient, partitionFileSliceMap);

    invokeGetRecordsByKeysSafely(metadata, Arrays.asList("key1", "key2"), nonRliPartition, Option.empty());

    assertFalse(cacheUsed[0], "Cache should not be used for non-RLI partitions");
    assertTrue(nonCachedPathUsed[0], "Non-cached path should be used for non-RLI partitions");

    assertFileSlicesStored(partitionFileSliceMap, nonRliPartition, fileSlices);
    assertFileSlicesToLookUp(fileSlices);
  }

  @Test
  void testFreshTableWithEmptyTimelineReturnsEmptyFileSlices() throws Exception {
    HoodieMetadataConfig metadataConfig = createMetadataConfig(true);
    HoodieTableMetaClient metadataMetaClient = createMetadataTableMetaClient(Option.empty());

    setupCacheMock(null);
    setupNonCachedPathMock(RECORD_INDEX_PARTITION, Collections.emptyList());

    ConcurrentHashMap<String, List<FileSlice>> partitionFileSliceMap = new ConcurrentHashMap<>();
    HoodieBackedTableMetadata metadata = createMockedMetadataInstance(
        metadataConfig, metadataMetaClient, partitionFileSliceMap);

    // Fresh table should throw because numFileSlices == 0
    IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
        metadata.readRecordIndex(Arrays.asList("key1", "key2", "key3")));
    assertTrue(exception.getMessage().contains(
        "Number of file slices for partition record_index should be > 0"),
        "Exception should mention file slices validation");

    // Verify neither path was used because we returned early
    assertFalse(cacheUsed[0], "Cache should not be used for fresh table");
    assertFalse(nonCachedPathUsed[0], "Non-cached path should not be used for fresh table");

    // Verify empty list was stored
    List<FileSlice> storedFileSlices = partitionFileSliceMap.get(RECORD_INDEX_PARTITION);
    assertNotNull(storedFileSlices, "Empty list should be stored in partitionFileSliceMap");
    assertTrue(storedFileSlices.isEmpty(), "File slices should be empty for fresh table");
  }

  @Test
  void testCacheMissTriggersLoaderFunction() throws Exception {
    HoodieMetadataConfig metadataConfig = createMetadataConfig(true);
    HoodieTableMetaClient metadataMetaClient = createMetadataTableMetaClient(Option.of(INSTANT_TIME));
    List<FileSlice> fileSlices = createFileSlices(RECORD_INDEX_PARTITION, 2);

    setupCacheMockWithLoaderInvocation();
    setupNonCachedPathMock(RECORD_INDEX_PARTITION, fileSlices);

    int index = 0;
    mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(any(String.class), anyInt()))
        .thenReturn(index);

    ConcurrentHashMap<String, List<FileSlice>> partitionFileSliceMap = new ConcurrentHashMap<>();
    HoodieBackedTableMetadata metadata = createMockedMetadataInstance(
        metadataConfig, metadataMetaClient, partitionFileSliceMap);

    invokeReadRecordIndexSafely(metadata, Arrays.asList("key1", "key2"));

    assertTrue(cacheUsed[0], "LatestFileSliceCacheForPartition.getCache should be called");
    assertTrue(loaderFunctionCalled[0], "Cache loader function should be called on cache miss");
    assertTrue(nonCachedPathUsed[0], "Cache loader function should trigger non-cached path lookup on cache miss");

    assertFileSlicesStored(partitionFileSliceMap, RECORD_INDEX_PARTITION, fileSlices);
  }

  private HoodieMetadataConfig createMetadataConfig(boolean enableCache) {
    return HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withEnableRecordIndex(true)
        .withRecordIndexFileGroupCount(METADATA_FILE_GROUP_COUNT, METADATA_FILE_GROUP_COUNT)
        .withEnableFileSliceCacheOptimizationForRliLookup(enableCache)
        .build();
  }

  private HoodieTableMetaClient createMetadataTableMetaClient(Option<String> lastInstantTime) {
    HoodieTableMetaClient metadataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline completedTimeline = mock(HoodieTimeline.class);

    when(metadataMetaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.filterCompletedInstants()).thenReturn(completedTimeline);

    if (lastInstantTime.isPresent()) {
      HoodieInstant lastInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, lastInstantTime.get());
      when(completedTimeline.lastInstant()).thenReturn(Option.of(lastInstant));
    } else {
      when(completedTimeline.lastInstant()).thenReturn(Option.empty());
    }

    return metadataMetaClient;
  }

  private HoodieBackedTableMetadata createMockedMetadataInstance(
      HoodieMetadataConfig metadataConfig,
      HoodieTableMetaClient metadataMetaClient,
      ConcurrentHashMap<String, List<FileSlice>> partitionFileSliceMap) throws Exception {

    HoodieBackedTableMetadata metadata = mock(HoodieBackedTableMetadata.class, Mockito.CALLS_REAL_METHODS);

    setField(metadata, BaseTableMetadata.class, "metadataConfig", metadataConfig);
    setField(metadata, BaseTableMetadata.class, "dataMetaClient", dataTableMetaClient);
    setField(metadata, BaseTableMetadata.class, "isMetadataTableInitialized", true);
    setField(metadata, BaseTableMetadata.class, "metrics", Option.empty());
    setField(metadata, BaseTableMetadata.class, "hiveStylePartitioningEnabled", false);
    setField(metadata, BaseTableMetadata.class, "urlEncodePartitioningEnabled", false);

    setField(metadata, HoodieBackedTableMetadata.class, "metadataMetaClient", metadataMetaClient);
    setField(metadata, HoodieBackedTableMetadata.class, "metadataFileSystemView", fileSystemView);
    setField(metadata, HoodieBackedTableMetadata.class, "reuse", false);
    setField(metadata, HoodieBackedTableMetadata.class, "partitionFileSliceMap", partitionFileSliceMap);

    doReturn(fileSystemView).when(metadata).getMetadataFileSystemView();
    doReturn(new HoodieLocalEngineContext(new Configuration())).when(metadata).getEngineContext();

    return metadata;
  }

  private HoodieTableMetaClient createDataTableMetaClient() {
    HoodieTableMetaClient client = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(client.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)).thenReturn(true);
    return client;
  }

  private List<FileSlice> createFileSlices(String partitionName, int count) {
    FileSlice[] slices = new FileSlice[count];
    for (int i = 0; i < count; i++) {
      slices[i] = new FileSlice(partitionName, INSTANT_TIME, "fileId_" + (char) ('A' + i));
    }
    return Arrays.asList(slices);
  }

  @SuppressWarnings("unchecked")
  private Cache<CacheKey, List<FileSlice>> createCacheMock(List<FileSlice> fileSlices, String partitionPath) {
    Cache<CacheKey, List<FileSlice>> cache = mock(Cache.class);
    CacheKey expectedKey = CacheKey.of(INSTANT_TIME, partitionPath);
    when(cache.get(eq(expectedKey), any())).thenReturn(fileSlices);
    return cache;
  }

  private void setupCacheMock(Cache<CacheKey, List<FileSlice>> cache) {
    mockedFileSliceCache.when(() -> LatestFileSliceCacheForPartition.getCache(
            any(HoodieTableFileSystemView.class),
            any(CacheKey.class),
            anyInt(),
            anyInt()))
        .thenAnswer(invocation -> {
          cacheUsed[0] = true;
          return cache;
        });
  }

  @SuppressWarnings("unchecked")
  private void setupCacheMockWithLoaderInvocation() {
    Cache<CacheKey, List<FileSlice>> cache = mock(Cache.class);
    when(cache.get(any(), any())).thenAnswer(invocation -> {
      java.util.function.Function<CacheKey, List<FileSlice>> loader = invocation.getArgument(1);
      loaderFunctionCalled[0] = true;
      return loader.apply(invocation.getArgument(0));
    });

    mockedFileSliceCache.when(() -> LatestFileSliceCacheForPartition.getCache(
            any(HoodieTableFileSystemView.class),
            any(CacheKey.class),
            anyInt(),
            anyInt()))
        .thenAnswer(invocation -> {
          cacheUsed[0] = true;
          return cache;
        });
  }

  private void setupNonCachedPathMock(String partitionName, List<FileSlice> fileSlices) {
    mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(
            any(HoodieTableMetaClient.class),
            any(HoodieTableFileSystemView.class),
            eq(partitionName)))
        .thenAnswer(invocation -> {
          nonCachedPathUsed[0] = true;
          return fileSlices;
        });
  }

  private void invokeReadRecordIndexSafely(HoodieBackedTableMetadata metadata, List<String> keys) {
    try {
      metadata.readRecordIndex(keys);
    } catch (Exception e) {
      // Expected - lookupKeysFromFileSlice will fail without real readers
    }
  }

  private void invokeGetRecordsByKeysSafely(HoodieBackedTableMetadata metadata, List<String> keys,
                                            String partitionName, Option<String> dataTablePartition) throws Exception {
    java.lang.reflect.Method method = HoodieBackedTableMetadata.class.getDeclaredMethod(
        "getRecordsByKeys", List.class, String.class, Option.class);
    method.setAccessible(true);
    try {
      method.invoke(metadata, keys, partitionName, dataTablePartition);
    } catch (Exception e) {
      // Expected - lookupKeysFromFileSlice will fail without real readers
    }
  }

  private void assertFileSlicesStored(ConcurrentHashMap<String, List<FileSlice>> map,
                                      String partitionName, List<FileSlice> expected) {
    List<FileSlice> stored = map.get(partitionName);
    assertNotNull(stored, "File slices should be stored in partitionFileSliceMap");
    assertEquals(expected.size(), stored.size(), "Should have same number of file slices");
    assertEquals(expected, stored, "File slices should match expected");
  }

  private void assertFileSlicesToLookUp(List<FileSlice> expectedFileSlices) {
    assertEquals(expectedFileSlices.size(), fileSlicesToLookUp.size(),
        "Number of file slices passed to lookupKeysFromFileSlice should match expected");
    for (int i = 0; i < expectedFileSlices.size(); i++) {
      assertEquals(expectedFileSlices.get(i).getFileId(), fileSlicesToLookUp.get(i).getFileId(),
          "File slice " + i + " passed to lookupKeysFromFileSlice should match expected");
    }
  }

  private void setField(Object target, Class<?> clazz, String fieldName, Object value) throws Exception {
    Field field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
