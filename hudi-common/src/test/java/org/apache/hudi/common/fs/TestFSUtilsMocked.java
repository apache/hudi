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

package org.apache.hudi.common.fs;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestFSUtilsMocked {

  @Mock
  private HoodieStorage mockStorage;

  private final StoragePath basePath = new StoragePath("/base/path");
  private final Set<String> fileNames = new HashSet<>(Arrays.asList("file1.txt", "file2.txt"));
  private StoragePathInfo mockFile1;
  private StoragePathInfo mockFile2;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    mockFile1 = new StoragePathInfo(new StoragePath("/base/path/file1.txt"), 100, false, (short) 3, 1024, 0);
    mockFile2 = new StoragePathInfo(new StoragePath("/base/path/file2.txt"), 200, false, (short) 3, 1024, 0);
  }

  @Test
  public void testGetPathInfoUnderPartitionWithListStatus() throws IOException, IOException {
    // Setup
    when(mockStorage.getScheme()).thenReturn("file"); // Assuming "file" is list status friendly
    List<StoragePathInfo> listingResult = new ArrayList<>();
    listingResult.add(mockFile1);
    listingResult.add(mockFile2);
    when(mockStorage.listDirectEntries(eq(basePath), any())).thenReturn(listingResult);

    // Execute
    List<Option<StoragePathInfo>> result = FSUtils.getPathInfoUnderPartition(mockStorage, basePath, fileNames, false);

    // Verify
    assertEquals(2, result.size());
    assertTrue(result.get(0).isPresent());
    assertTrue(result.get(1).isPresent());

    // Cleanup
    verify(mockStorage, times(1)).listDirectEntries((StoragePath) any(), any());
  }

  @Test
  public void testGetPathInfoUnderPartitionIgnoringMissingFiles() throws IOException {
    // Setup for scenario where file2.txt does not exist
    when(mockStorage.getScheme()).thenReturn("hdfs"); // Assuming "hdfs" is not list status friendly
    when(mockStorage.getPathInfo(new StoragePath("/base/path/file1.txt"))).thenReturn(mockFile1);
    when(mockStorage.getPathInfo(new StoragePath("/base/path/file2.txt"))).thenThrow(new FileNotFoundException());

    // Execute
    List<Option<StoragePathInfo>> result = FSUtils.getPathInfoUnderPartition(mockStorage, basePath, fileNames, true);

    // Verify
    assertEquals(2, result.size());
    assertTrue(result.get(0).isPresent());
    assertFalse(result.get(1).isPresent()); // Missing file results in an empty Option

    // Cleanup
    verify(mockStorage, times(2)).getPathInfo(any());
  }

  @Test
  public void testGetPathInfoUnderPartitionThrowsHoodieIOException() throws IOException {
    // Setup
    when(mockStorage.getScheme()).thenReturn("file"); // Assuming "file" is list status friendly
    when(mockStorage.listDirectEntries((StoragePath) any(), any())).thenThrow(new IOException());

    // Execute & Verify
    assertThrows(HoodieIOException.class, () ->
        FSUtils.getPathInfoUnderPartition(mockStorage, basePath, fileNames, false));

    // Cleanup
    verify(mockStorage, times(1)).listDirectEntries((StoragePath) any(), any());
  }
}
