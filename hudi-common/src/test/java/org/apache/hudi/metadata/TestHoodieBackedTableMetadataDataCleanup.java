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

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
      when(mockMetadata.readRecordIndexLocations(any())).thenReturn(mockHoodieData);
      
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
}