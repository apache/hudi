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

package org.apache.hudi.commit;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;
import org.apache.hudi.config.HoodieInternalConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DatasetBulkInsertOverwriteCommitActionExecutor}.
 */
public class TestDatasetBulkInsertOverwriteCommitActionExecutor {

  /**
   * Tests that getPartitionToReplacedFileIds correctly short-circuits for unpartitioned tables,
   * returning a single entry with empty string key without calling distinct() on WriteStatus.
   * This validates the optimization added to avoid expensive shuffle operations on unpartitioned tables.
   */
  @Test
  public void testGetPartitionToReplacedFileIds_UnpartitionedTable() throws Exception {
    // Setup: Mock dependencies
    HoodieWriteConfig mockConfig = mock(HoodieWriteConfig.class);
    when(mockConfig.getStringOrDefault(HoodieInternalConfig.STATIC_OVERWRITE_PARTITION_PATHS))
        .thenReturn(""); // No static paths set

    SparkRDDWriteClient mockClient = mock(SparkRDDWriteClient.class);
    HoodieSparkTable mockTable = mock(HoodieSparkTable.class);

    // Key mock: isPartitioned() returns false to trigger our new code path
    when(mockTable.isPartitioned()).thenReturn(false);

    // Mock file slice view to return some file IDs for the empty partition
    SliceView mockView = mock(SliceView.class);
    when(mockTable.getSliceView()).thenReturn(mockView);

    FileSlice mockSlice1 = mock(FileSlice.class);
    FileSlice mockSlice2 = mock(FileSlice.class);
    when(mockSlice1.getFileId()).thenReturn("file-id-1");
    when(mockSlice2.getFileId()).thenReturn("file-id-2");

    when(mockView.getLatestFileSlices(anyString()))
        .thenReturn(Stream.of(mockSlice1, mockSlice2));

    // Create executor instance
    DatasetBulkInsertOverwriteCommitActionExecutor executor =
        new DatasetBulkInsertOverwriteCommitActionExecutor(mockConfig, mockClient, "001");

    // Use reflection to set the mocked table since it's a protected field
    Field tableField = DatasetBulkInsertOverwriteCommitActionExecutor.class
        .getSuperclass()
        .getDeclaredField("table");
    tableField.setAccessible(true);
    tableField.set(executor, mockTable);

    @SuppressWarnings("unchecked")
    HoodieData<WriteStatus> mockWriteStatuses = mock(HoodieData.class);

    // Execute: Call the method under test
    Map<String, List<String>> result = executor.getPartitionToReplacedFileIds(mockWriteStatuses);

    // Verify: Unpartitioned table should return exactly 1 entry with empty string key
    assertEquals(1, result.size(), "Unpartitioned table should return exactly 1 entry");
    assertTrue(result.containsKey(""), "Key should be empty string for unpartitioned table");

    List<String> fileIds = result.get("");
    assertEquals(2, fileIds.size(), "Should have 2 file IDs from mocked file slices");
    assertTrue(fileIds.contains("file-id-1"), "Should contain first mocked file ID");
    assertTrue(fileIds.contains("file-id-2"), "Should contain second mocked file ID");
  }
}
