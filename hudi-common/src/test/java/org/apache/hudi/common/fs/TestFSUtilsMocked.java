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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.FileNotFoundException;
import java.io.IOException;
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
  private FileSystem mockFileSystem;

  private final Path basePath = new Path("/base/path");
  private final Set<String> fileNames = new HashSet<>(Arrays.asList("file1.txt", "file2.txt"));
  private FileStatus mockFileStatus1;
  private FileStatus mockFileStatus2;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    mockFileStatus1 = new FileStatus(100, false, 3, 1024, 0, new Path("/base/path/file1.txt"));
    mockFileStatus2 = new FileStatus(200, false, 3, 1024, 0, new Path("/base/path/file2.txt"));
  }

  @Test
  public void testGetFileStatusesUnderPartitionWithListStatus() throws IOException, IOException {
    // Setup
    when(mockFileSystem.getScheme()).thenReturn("file"); // Assuming "file" is list status friendly
    when(mockFileSystem.listStatus(eq(basePath), any())).thenReturn(new FileStatus[] {mockFileStatus1, mockFileStatus2});

    // Execute
    List<Option<FileStatus>> result = FSUtils.getFileStatusesUnderPartition(mockFileSystem, basePath, fileNames, false);

    // Verify
    assertEquals(2, result.size());
    assertTrue(result.get(0).isPresent());
    assertTrue(result.get(1).isPresent());

    // Cleanup
    verify(mockFileSystem, times(1)).listStatus((Path) any(), any());
  }

  @Test
  public void testGetFileStatusesUnderPartitionIgnoringMissingFiles() throws IOException {
    // Setup for scenario where file2.txt does not exist
    when(mockFileSystem.getScheme()).thenReturn("hdfs"); // Assuming "hdfs" is not list status friendly
    when(mockFileSystem.getFileStatus(new Path("/base/path/file1.txt"))).thenReturn(mockFileStatus1);
    when(mockFileSystem.getFileStatus(new Path("/base/path/file2.txt"))).thenThrow(new FileNotFoundException());

    // Execute
    List<Option<FileStatus>> result = FSUtils.getFileStatusesUnderPartition(mockFileSystem, basePath, fileNames, true);

    // Verify
    assertEquals(2, result.size());
    assertTrue(result.get(0).isPresent());
    assertFalse(result.get(1).isPresent()); // Missing file results in an empty Option

    // Cleanup
    verify(mockFileSystem, times(2)).getFileStatus(any());
  }

  @Test
  public void testGetFileStatusesUnderPartitionThrowsHoodieIOException() throws IOException {
    // Setup
    when(mockFileSystem.getScheme()).thenReturn("file"); // Assuming "file" is list status friendly
    when(mockFileSystem.listStatus((Path) any(), any())).thenThrow(new IOException());

    // Execute & Verify
    assertThrows(HoodieIOException.class, () ->
        FSUtils.getFileStatusesUnderPartition(mockFileSystem, basePath, fileNames, false));

    // Cleanup
    verify(mockFileSystem, times(1)).listStatus((Path) any(), any());
  }
}
