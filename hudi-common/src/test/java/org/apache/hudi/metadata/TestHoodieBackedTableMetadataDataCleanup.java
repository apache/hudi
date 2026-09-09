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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests to verify that HoodieBackedTableMetadata methods correctly
 * invoke dataCleanupManager.ensureDataCleanupOnException.
 */
public class TestHoodieBackedTableMetadataDataCleanup {

  private HoodieBackedTableMetadata mockMetadata;
  private HoodieTableMetaClient mockDataMetaClient;
  private HoodieTableConfig mockTableConfig;
  private HoodieDataCleanupManager spyCleanupManager;
  private HoodiePairData mockPairData;
  private HoodiePairData mockResult;
  private HoodieData mockHoodieData;

  @BeforeEach
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
    // Create mocks
    mockMetadata = mock(HoodieBackedTableMetadata.class);
    mockDataMetaClient = mock(HoodieTableMetaClient.class);
    mockTableConfig = mock(HoodieTableConfig.class);
    spyCleanupManager = spy(new HoodieDataCleanupManager());
    mockPairData = mock(HoodiePairData.class);
    mockResult = mock(HoodiePairData.class);
    mockHoodieData = mock(HoodieData.class);
    
    // Setup table config
    when(mockDataMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    when(mockTableConfig.isMetadataPartitionAvailable(any())).thenReturn(true);
    
    // Inject mocks using reflection
    injectMocks(mockMetadata, mockDataMetaClient, spyCleanupManager);
  }
  
  private void injectMocks(HoodieBackedTableMetadata metadata, 
                          HoodieTableMetaClient dataMetaClient,
                          HoodieDataCleanupManager cleanupManager) throws NoSuchFieldException, IllegalAccessException {
    Field dataMetaClientField = BaseTableMetadata.class.getDeclaredField("dataMetaClient");
    dataMetaClientField.setAccessible(true);
    dataMetaClientField.set(metadata, dataMetaClient);
    
    Field cleanupManagerField = HoodieBackedTableMetadata.class.getDeclaredField("dataCleanupManager");
    cleanupManagerField.setAccessible(true);
    cleanupManagerField.set(metadata, cleanupManager);
  }

  /**
   * Test using reflection to verify cleanup manager is invoked during readRecordIndex.
   */
  @Test
  public void testReadRecordIndexInvokesCleanupManager() {
    // Create test data
    HoodieData<String> recordKeys = HoodieListData.eager(Arrays.asList("key1", "key2"));
    
    // Setup mock behavior
    when(mockMetadata.readIndexRecordsWithKeys(any(), any())).thenReturn(mockPairData);
    when(mockMetadata.readIndexRecordsWithKeys(any(), any(), any())).thenReturn(mockPairData);
    when(mockPairData.mapToPair(any())).thenReturn(mockResult);
    
    // Call real method on the mock
    when(mockMetadata.readRecordIndexLocationsWithKeys(recordKeys)).thenCallRealMethod();
    when(mockMetadata.readRecordIndexLocationsWithKeys(recordKeys, Option.empty())).thenCallRealMethod();
    
    // Execute the method
    HoodiePairData result = mockMetadata.readRecordIndexLocationsWithKeys(recordKeys);
    
    // Verify cleanup manager was invoked
    verify(spyCleanupManager).ensureDataCleanupOnException(any());
    
    // Verify result
    assertEquals(mockResult, result);
  }

  /**
   * Test readRecordIndexLocations invokes cleanup manager.
   */
  @Test
  public void testReadRecordIndexLocationsInvokesCleanupManager() {
    // Create test data
    HoodieData<String> recordKeys = HoodieListData.eager(Arrays.asList("key1", "key2"));
    
    // Setup mock behavior for readIndexRecords
    HoodieData mockIndexRecords = mock(HoodieData.class);
    when(mockMetadata.readIndexRecords(any(), anyString(), any())).thenReturn(mockIndexRecords);
    when(mockIndexRecords.map(any())).thenReturn(mockHoodieData);
    
    // Call real method on the mock
    when(mockMetadata.readRecordIndexLocations(recordKeys)).thenCallRealMethod();
    
    // Execute the method
    HoodieData result = mockMetadata.readRecordIndexLocations(recordKeys);
    
    // Verify cleanup manager was invoked
    verify(spyCleanupManager).ensureDataCleanupOnException(any());
    
    // Verify result
    assertNotNull(result);
  }
  
  /**
   * Test readSecondaryIndex invokes cleanup manager.
   */
  @Test
  public void testReadSecondaryIndexInvokesCleanupManager() {
    // Create test data
    HoodieData<String> secondaryKeys = HoodieListData.eager(Arrays.asList("skey1", "skey2"));
    String partitionName = "test_partition";
    
    // Mock the static method existingIndexVersionOrDefault
    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      mockedUtil.when(() -> HoodieTableMetadataUtil.existingIndexVersionOrDefault(anyString(), any()))
          .thenReturn(HoodieIndexVersion.V2);
      
      // Setup mock behavior for V2 path
      when(mockMetadata.readSecondaryIndexDataTableRecordKeysV2(any(), anyString())).thenReturn(mockHoodieData);
      when(mockPairData.mapToPair(any())).thenReturn(mockResult);
      
      // Call real method on the mock
      when(mockMetadata.readSecondaryIndexLocationsWithKeys(secondaryKeys, partitionName)).thenCallRealMethod();
      
      // Execute the method - it may throw NPE due to mocks, but we just want to verify cleanup manager is called
      try {
        mockMetadata.readSecondaryIndexLocationsWithKeys(secondaryKeys, partitionName);
      } catch (Exception e) {
        // Expected - we're testing with mocks
      }
      
      // Verify cleanup manager was invoked
      verify(spyCleanupManager).ensureDataCleanupOnException(any());
    }
  }
  
  /**
   * Test readSecondaryIndexLocations invokes cleanup manager.
   */
  @Test
  public void testReadSecondaryIndexLocationsInvokesCleanupManager() {
    // Create test data
    HoodieData<String> secondaryKeys = HoodieListData.eager(Arrays.asList("skey1", "skey2"));
    String partitionName = "test_partition";
    
    // Mock the static method existingIndexVersionOrDefault
    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      mockedUtil.when(() -> HoodieTableMetadataUtil.existingIndexVersionOrDefault(anyString(), any()))
          .thenReturn(HoodieIndexVersion.V2);
      
      // Setup mock behavior for V2 path
      when(mockPairData.values()).thenReturn(mockHoodieData);
      when(mockMetadata.readSecondaryIndexDataTableRecordKeysV2(any(), anyString())).thenReturn(mockHoodieData);
      when(mockMetadata.readRecordIndexLocations(any(HoodieData.class))).thenReturn(mockHoodieData);
      
      // Call real method on the mock
      when(mockMetadata.readSecondaryIndexLocations(secondaryKeys, partitionName)).thenCallRealMethod();
      
      // Execute the method - it may throw NPE due to mocks, but we just want to verify cleanup manager is called
      try {
        mockMetadata.readSecondaryIndexLocations(secondaryKeys, partitionName);
      } catch (Exception e) {
        // Expected - we're testing with mocks
      }
      
      // Verify cleanup manager was invoked
      verify(spyCleanupManager).ensureDataCleanupOnException(any());
    }
  }

  /**
   * Test cleanup manager propagates exceptions correctly.
   */
  @Test
  public void testCleanupManagerPropagatesExceptions() throws NoSuchFieldException, IllegalAccessException {
    // Create a mock cleanup manager that throws exception
    HoodieDataCleanupManager mockCleanupManager = mock(HoodieDataCleanupManager.class);
    injectMocks(mockMetadata, mockDataMetaClient, mockCleanupManager);
    
    // Make cleanup manager throw exception
    HoodieException testException = new HoodieException("Test exception from cleanup manager");
    doThrow(testException).when(mockCleanupManager).ensureDataCleanupOnException(any());
    
    // Call real method on the mock
    when(mockMetadata.readRecordIndexLocationsWithKeys(any())).thenCallRealMethod();
    when(mockMetadata.readRecordIndexLocationsWithKeys(any(), any())).thenCallRealMethod();
    
    // Execute and verify exception is propagated
    HoodieData<String> recordKeys = HoodieListData.eager(Arrays.asList("key1"));
    try {
      mockMetadata.readRecordIndexLocationsWithKeys(recordKeys);
      fail("Expected exception was not thrown");
    } catch (HoodieException e) {
      assertEquals("Test exception from cleanup manager", e.getMessage());
    }
    
    // Verify cleanup manager was called
    verify(mockCleanupManager).ensureDataCleanupOnException(any());
  }

  @Test
  public void testSecondaryIndexUnsupportedVersion() {
    HoodieData<String> keys = HoodieListData.eager(Collections.singletonList("key"));
    HoodieIndexVersion unsupportedVersion = mock(HoodieIndexVersion.class);
    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      mockedUtil.when(() -> HoodieTableMetadataUtil.existingIndexVersionOrDefault(anyString(), any()))
          .thenReturn(unsupportedVersion);

      when(mockMetadata.readSecondaryIndexLocationsWithKeys(keys, "secondary_index_test")).thenCallRealMethod();
      when(mockMetadata.readSecondaryIndexLocations(keys, "secondary_index_test")).thenCallRealMethod();
      when(mockMetadata.readSecondaryIndexDataTableRecordKeysWithKeys(keys, "secondary_index_test")).thenCallRealMethod();

      assertThrows(IllegalArgumentException.class,
          () -> mockMetadata.readSecondaryIndexLocationsWithKeys(keys, "secondary_index_test"));
      assertThrows(IllegalArgumentException.class,
          () -> mockMetadata.readSecondaryIndexLocations(keys, "secondary_index_test"));
      assertThrows(IllegalArgumentException.class,
          () -> mockMetadata.readSecondaryIndexDataTableRecordKeysWithKeys(keys, "secondary_index_test"));
    }
  }

  @Test
  public void testSecondaryIndexEmptyAndMissingPartition() {
    HoodieData<String> emptyKeys = HoodieListData.eager(Collections.emptyList());
    String partitionName = "secondary_index_test";
    when(mockMetadata.readSecondaryIndexDataTableRecordKeysWithKeys(emptyKeys, partitionName)).thenCallRealMethod();

    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      mockedUtil.when(() -> HoodieTableMetadataUtil.existingIndexVersionOrDefault(anyString(), any()))
          .thenReturn(HoodieIndexVersion.V1);
      assertTrue(mockMetadata.readSecondaryIndexDataTableRecordKeysWithKeys(emptyKeys, partitionName)
          .collectAsList().isEmpty());

      mockedUtil.when(() -> HoodieTableMetadataUtil.existingIndexVersionOrDefault(anyString(), any()))
          .thenReturn(HoodieIndexVersion.V2);
      assertTrue(mockMetadata.readSecondaryIndexDataTableRecordKeysWithKeys(emptyKeys, partitionName)
          .collectAsList().isEmpty());

      HoodieData<String> keys = HoodieListData.eager(Collections.singletonList("key"));
      mockedUtil.when(() -> HoodieTableMetadataUtil.existingIndexVersionOrDefault(anyString(), any()))
          .thenReturn(HoodieIndexVersion.V1);
      when(mockTableConfig.getMetadataPartitions()).thenReturn(Collections.emptySet());
      when(mockMetadata.readSecondaryIndexLocationsWithKeys(keys, partitionName)).thenCallRealMethod();
      assertThrows(IllegalStateException.class,
          () -> mockMetadata.readSecondaryIndexLocationsWithKeys(keys, partitionName));
    }
  }

  @Test
  public void testEmptyRecordIndexAndMetadataTimeline() throws Exception {
    Field fileSliceMapField = HoodieBackedTableMetadata.class.getDeclaredField("partitionFileSliceMap");
    fileSliceMapField.setAccessible(true);
    Map<String, List<FileSlice>> fileSliceMap = new HashMap<>();
    fileSliceMap.put(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), Collections.emptyList());
    fileSliceMapField.set(mockMetadata, fileSliceMap);

    when(mockMetadata.readRecordIndexLocations(
        org.mockito.ArgumentMatchers.<SerializableFunctionUnchecked<List<FileSlice>, List<FileSlice>>>any()))
        .thenCallRealMethod();
    assertTrue(mockMetadata.readRecordIndexLocations(slices -> slices).collectAsList().isEmpty());

    when(mockMetadata.getSyncedInstantTime()).thenCallRealMethod();
    when(mockMetadata.getLatestCompactionTime()).thenCallRealMethod();
    assertFalse(mockMetadata.getSyncedInstantTime().isPresent());
    assertFalse(mockMetadata.getLatestCompactionTime().isPresent());
  }

  @Test
  public void testPartitionFilterFallbackAndBucketValidation() throws Exception {
    List<String> selectedPartitions = Arrays.asList("year=2025", "year=2026");
    Expression expression = mock(Expression.class);
    when(expression.accept(any())).thenReturn(expression);
    when(mockMetadata.getPartitionPathWithPathPrefixes(any())).thenReturn(selectedPartitions);
    when(mockMetadata.getPartitionPathWithPathPrefixUsingFilterExpression(any(), any(), any()))
        .thenCallRealMethod();

    assertEquals(selectedPartitions, mockMetadata.getPartitionPathWithPathPrefixUsingFilterExpression(
        Collections.singletonList("year="), mock(Types.RecordType.class), expression));

    Field partitionedMapField =
        HoodieBackedTableMetadata.class.getDeclaredField("partitionedRLIFileSliceMap");
    partitionedMapField.setAccessible(true);
    partitionedMapField.set(mockMetadata, new HashMap<>());
    when(mockMetadata.getBucketizedFileGroupsForPartitionedRLI(any())).thenCallRealMethod();
    assertThrows(IllegalArgumentException.class,
        () -> mockMetadata.getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.FILES));

    when(mockMetadata.getFilegroupsForPartition(MetadataPartitionType.RECORD_INDEX))
        .thenReturn(Collections.emptyList());
    assertTrue(mockMetadata.getBucketizedFileGroupsForPartitionedRLI(
        MetadataPartitionType.RECORD_INDEX).isEmpty());

    FileSlice nonPartitionedSlice = mock(FileSlice.class);
    when(nonPartitionedSlice.getFileId()).thenReturn("record-index-0000");
    when(mockMetadata.getFilegroupsForPartition(MetadataPartitionType.RECORD_INDEX))
        .thenReturn(Collections.singletonList(nonPartitionedSlice));
    assertThrows(IllegalArgumentException.class,
        () -> mockMetadata.getBucketizedFileGroupsForPartitionedRLI(
            MetadataPartitionType.RECORD_INDEX));
  }

  @Test
  public void testPartitionedRecordIndexLookupGuards() throws Exception {
    Method lookupMethod = HoodieBackedTableMetadata.class.getDeclaredMethod(
        "lookupIndexRecords", HoodieData.class, String.class, List.class, Option.class);
    lookupMethod.setAccessible(true);
    FileSlice partitionedSlice = mock(FileSlice.class);
    when(partitionedSlice.getFileId()).thenReturn("record-index-partition-x-0000");
    List<FileSlice> slices = Collections.singletonList(partitionedSlice);

    HoodieData<?> emptyResult = (HoodieData<?>) lookupMethod.invoke(
        mockMetadata,
        HoodieListData.eager(Collections.emptyList()),
        MetadataPartitionType.RECORD_INDEX.getPartitionPath(),
        slices,
        Option.empty());
    assertTrue(emptyResult.collectAsList().isEmpty());

    InvocationTargetException exception = assertThrows(
        InvocationTargetException.class,
        () -> lookupMethod.invoke(
            mockMetadata,
            HoodieListData.eager(Collections.singletonList("key")),
            MetadataPartitionType.RECORD_INDEX.getPartitionPath(),
            slices,
            Option.empty()));
    assertTrue(exception.getCause() instanceof IllegalArgumentException);
  }

  @Test
  public void testSecondaryIndexEmptyIteratorPath() throws Exception {
    Method method = HoodieBackedTableMetadata.class.getDeclaredMethod(
        "readSliceAndFilterByKeys", String.class, List.class, FileSlice.class);
    method.setAccessible(true);
    Object iterator = method.invoke(
        mockMetadata,
        MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "test",
        Collections.emptyList(),
        mock(FileSlice.class));
    assertFalse(((org.apache.hudi.common.util.collection.ClosableIterator<?>) iterator).hasNext());
  }

  @Test
  public void testInitializationFailureDisablesMetadata() throws Exception {
    Field initializedField = BaseTableMetadata.class.getDeclaredField("isMetadataTableInitialized");
    initializedField.setAccessible(true);
    initializedField.set(mockMetadata, true);
    Field metadataBasePathField =
        HoodieBackedTableMetadata.class.getDeclaredField("metadataBasePath");
    metadataBasePathField.setAccessible(true);
    metadataBasePathField.set(mockMetadata, "/table/.hoodie/metadata");
    when(mockMetadata.getStorage()).thenReturn(mock(HoodieStorage.class));

    HoodieTableMetaClient.Builder builder = mock(HoodieTableMetaClient.Builder.class);
    when(builder.setStorage(any())).thenReturn(builder);
    when(builder.setBasePath(anyString())).thenReturn(builder);
    when(builder.build()).thenThrow(new HoodieException("initialization failed"));

    try (MockedStatic<HoodieTableMetaClient> metaClientStatic =
             mockStatic(HoodieTableMetaClient.class)) {
      metaClientStatic.when(HoodieTableMetaClient::builder).thenReturn(builder);
      Method initMethod = HoodieBackedTableMetadata.class.getDeclaredMethod("initIfNeeded");
      initMethod.setAccessible(true);
      initMethod.invoke(mockMetadata);
    }

    assertFalse(mockMetadata.isMetadataTableInitialized());
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testSecondaryIndexRecordMapping() throws Exception {
    prepareFileSliceRead(false);
    HoodieRecord<HoodieMetadataPayload> metadataRecord =
        HoodieMetadataPayload.createPartitionFilesRecord(
            "key", Collections.emptyMap(), Collections.emptyList());
    IndexedRecord indexedRecord =
        (IndexedRecord) metadataRecord.getData().getInsertValue(
            HoodieMetadataRecord.getClassSchema()).get();

    HoodieFileGroupReader.HoodieFileGroupReaderBuilder builder =
        mock(HoodieFileGroupReader.HoodieFileGroupReaderBuilder.class);
    HoodieFileGroupReader fileGroupReader = mock(HoodieFileGroupReader.class);
    when(builder.withReaderContext(any())).thenReturn(builder);
    when(builder.withHoodieTableMetaClient(any())).thenReturn(builder);
    when(builder.withLatestCommitTime(anyString())).thenReturn(builder);
    when(builder.withBaseFileOption(any())).thenReturn(builder);
    when(builder.withLogFiles(any())).thenReturn(builder);
    when(builder.withPartitionPath(anyString())).thenReturn(builder);
    when(builder.withDataSchema(any())).thenReturn(builder);
    when(builder.withRequestedSchema(any())).thenReturn(builder);
    when(builder.withProps(any())).thenReturn(builder);
    when(builder.withRecordBufferLoader(any())).thenReturn(builder);
    when(builder.build()).thenReturn(fileGroupReader);
    when(fileGroupReader.getClosableIterator()).thenReturn(
        ClosableIterator.wrap(Collections.singletonList(indexedRecord).iterator()));

    FileSlice fileSlice = mock(FileSlice.class);
    when(fileSlice.getPartitionPath()).thenReturn(
        MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "test");
    when(fileSlice.getBaseFile()).thenReturn(Option.empty());
    when(fileSlice.getLogFiles()).thenReturn(Stream.empty());

    try (MockedStatic<HoodieFileGroupReader> readerStatic =
             mockStatic(HoodieFileGroupReader.class)) {
      readerStatic.when(HoodieFileGroupReader::builder).thenReturn(builder);
      Method method = HoodieBackedTableMetadata.class.getDeclaredMethod(
          "readSliceAndFilterByKeys", String.class, List.class, FileSlice.class);
      method.setAccessible(true);
      ClosableIterator<Pair<String, HoodieRecord<HoodieMetadataPayload>>> iterator =
          (ClosableIterator<Pair<String, HoodieRecord<HoodieMetadataPayload>>>) method.invoke(
              mockMetadata,
              MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "test",
              Collections.singletonList("key"),
              fileSlice);

      assertTrue(iterator.hasNext());
      assertEquals("key", iterator.next().getLeft());
      assertFalse(iterator.hasNext());
      iterator.close();

      when(fileGroupReader.getClosableIterator())
          .thenThrow(new IOException("iterator failed"));
      InvocationTargetException exception = assertThrows(
          InvocationTargetException.class,
          () -> method.invoke(
              mockMetadata,
              MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "test",
              Collections.singletonList("key"),
              fileSlice));
      assertTrue(exception.getCause() instanceof org.apache.hudi.exception.HoodieIOException);

      Method scanMethod = HoodieBackedTableMetadata.class.getDeclaredMethod(
          "scanRecordsItr", FileSlice.class, SerializableFunctionUnchecked.class);
      scanMethod.setAccessible(true);
      InvocationTargetException scanException = assertThrows(
          InvocationTargetException.class,
          () -> scanMethod.invoke(
              mockMetadata,
              fileSlice,
              (SerializableFunctionUnchecked<org.apache.avro.generic.GenericRecord,
                  HoodieRecord<HoodieMetadataPayload>>) record -> null));
      assertTrue(scanException.getCause() instanceof org.apache.hudi.exception.HoodieIOException);
    }
  }

  @Test
  public void testReusableReaderIOExceptionIsWrapped() throws Exception {
    prepareFileSliceRead(true);
    HoodieStorage storage = mock(HoodieStorage.class);
    when(mockMetadata.getStorage()).thenReturn(storage);

    HoodieBaseFile baseFile = mock(HoodieBaseFile.class);
    when(baseFile.getPathInfo()).thenReturn(mock(StoragePathInfo.class));
    FileSlice fileSlice = mock(FileSlice.class);
    when(fileSlice.getPartitionPath()).thenReturn(MetadataPartitionType.FILES.getPartitionPath());
    when(fileSlice.getFileGroupId()).thenReturn(
        new org.apache.hudi.common.model.HoodieFileGroupId(
            MetadataPartitionType.FILES.getPartitionPath(), "file-id"));
    when(fileSlice.getBaseFile()).thenReturn(Option.of(baseFile));

    HoodieIOFactory ioFactory = mock(HoodieIOFactory.class);
    HoodieFileReaderFactory readerFactory = mock(HoodieFileReaderFactory.class);
    when(ioFactory.getReaderFactory(HoodieRecord.HoodieRecordType.AVRO))
        .thenReturn(readerFactory);
    when(readerFactory.getFileReader(
        any(HoodieConfig.class),
        any(StoragePathInfo.class),
        any(HoodieFileFormat.class),
        any(Option.class)))
        .thenThrow(new IOException("reader failed"));

    try (MockedStatic<HoodieIOFactory> ioFactoryStatic =
             mockStatic(HoodieIOFactory.class)) {
      ioFactoryStatic.when(() -> HoodieIOFactory.getIOFactory(storage)).thenReturn(ioFactory);
      Method method = HoodieBackedTableMetadata.class.getDeclaredMethod(
          "readSliceWithFilter",
          org.apache.hudi.expression.Predicate.class,
          FileSlice.class);
      method.setAccessible(true);
      InvocationTargetException exception = assertThrows(
          InvocationTargetException.class,
          () -> method.invoke(
              mockMetadata,
              mock(org.apache.hudi.expression.Predicate.class),
              fileSlice));
      assertTrue(exception.getCause() instanceof org.apache.hudi.exception.HoodieIOException);
    }
  }

  @Test
  public void testEmptyShardReturnsEmptyIterator() throws Exception {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieData<String> emptyKeys = HoodieListData.eager(Collections.emptyList());
    HoodieData<HoodieRecord<HoodieMetadataPayload>> emptyResult =
        HoodieListData.eager(Collections.emptyList());
    when(mockMetadata.getEngineContext()).thenReturn(engineContext);
    when(engineContext.parallelize(
        any(List.class), org.mockito.ArgumentMatchers.eq(1))).thenReturn(emptyKeys);
    when(engineContext.mapGroupsByKey(any(), any(), any(), org.mockito.ArgumentMatchers.eq(true)))
        .thenAnswer(invocation -> {
          SerializableFunction<java.util.Iterator<String>,
              java.util.Iterator<HoodieRecord<HoodieMetadataPayload>>> processFunction =
              invocation.getArgument(1);
          assertFalse(processFunction.apply(Collections.emptyIterator()).hasNext());
          return emptyResult;
        });

    Method method = HoodieBackedTableMetadata.class.getDeclaredMethod(
        "lookupIndexRecords", HoodieData.class, String.class, List.class,
        Option.class);
    method.setAccessible(true);
    Object result = method.invoke(
        mockMetadata,
        emptyKeys,
        MetadataPartitionType.FILES.getPartitionPath(),
        Arrays.asList(mock(FileSlice.class), mock(FileSlice.class)),
        Option.empty());

    assertEquals(emptyResult, result);
  }

  private void prepareFileSliceRead(boolean reuse) throws Exception {
    HoodieTableMetaClient metadataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline timeline = mock(HoodieActiveTimeline.class);
    when(metadataMetaClient.getActiveTimeline()).thenReturn(timeline);
    when(timeline.filterCompletedInstants()).thenReturn(timeline);
    when(timeline.lastInstant()).thenReturn(Option.empty());
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    when(tableConfig.getBaseFileFormat()).thenReturn(HoodieFileFormat.PARQUET);
    when(metadataMetaClient.getTableConfig()).thenReturn(tableConfig);

    Field metadataMetaClientField =
        HoodieBackedTableMetadata.class.getDeclaredField("metadataMetaClient");
    metadataMetaClientField.setAccessible(true);
    metadataMetaClientField.set(mockMetadata, metadataMetaClient);
    Field validInstantsField =
        HoodieBackedTableMetadata.class.getDeclaredField("validInstantTimestamps");
    validInstantsField.setAccessible(true);
    validInstantsField.set(mockMetadata, Collections.singleton("001"));
    Field reuseField = HoodieBackedTableMetadata.class.getDeclaredField("reuse");
    reuseField.setAccessible(true);
    reuseField.set(mockMetadata, reuse);
    Field metadataConfigField = BaseTableMetadata.class.getDeclaredField("metadataConfig");
    metadataConfigField.setAccessible(true);
    metadataConfigField.set(
        mockMetadata, HoodieMetadataConfig.newBuilder().enable(true).build());
    Field storageConfField =
        AbstractHoodieTableMetadata.class.getDeclaredField("storageConf");
    storageConfField.setAccessible(true);
    storageConfField.set(mockMetadata, mock(StorageConfiguration.class));
  }
}
