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

package org.apache.hudi.common.util;

import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestExternalFilePathUtil {

  private static final String COMMIT_TIME = "20240101000000";
  private static final String EXTERNAL_FILE_SUFFIX = "_hudiext";

  @Test
  public void testAppendCommitTimeAndExternalFileMarker() {
    String filePath = "partition1/file1.parquet";
    String result = ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(filePath, COMMIT_TIME);
    assertEquals("partition1/file1.parquet_20240101000000_hudiext", result);
  }

  @Test
  public void testAppendCommitTimeAndExternalFileMarkerWithPrefix() {
    String fileName = "file1.parquet";
    String prefix = "bucket-0";
    String result = ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(fileName, COMMIT_TIME, prefix);
    assertEquals("file1.parquet_20240101000000_fg%3Dbucket-0_hudiext", result);
  }

  @Test
  public void testAppendCommitTimeAndExternalFileMarkerWithNullPrefix() {
    String fileName = "file1.parquet";
    String result = ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(fileName, COMMIT_TIME, null);
    assertEquals("file1.parquet_20240101000000_hudiext", result);
  }

  @Test
  public void testAppendCommitTimeAndExternalFileMarkerWithEmptyPrefix() {
    String fileName = "file1.parquet";
    String result = ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(fileName, COMMIT_TIME, "");
    assertEquals("file1.parquet_20240101000000_hudiext", result);
  }

  @Test
  public void testIsExternallyCreatedFile() {
    assertTrue(ExternalFilePathUtil.isExternallyCreatedFile("file1.parquet_20240101000000_hudiext"));
    assertTrue(ExternalFilePathUtil.isExternallyCreatedFile("file1.parquet_20240101000000_fg%3Dbucket-0_hudiext"));
    assertFalse(ExternalFilePathUtil.isExternallyCreatedFile("file1_1-0-1_20240101000000.parquet"));
    assertFalse(ExternalFilePathUtil.isExternallyCreatedFile("file1.parquet"));
  }

  @Test
  public void testParseFileIdAndCommitTimeFromExternalFile_LegacyFormat() {
    String fileName = "myfile.parquet_20240101000000_hudiext";
    String[] result = ExternalFilePathUtil.parseFileIdAndCommitTimeFromExternalFile(fileName);

    assertNotNull(result);
    assertEquals(2, result.length);
    assertEquals("myfile.parquet", result[0]); // fileId
    assertEquals("20240101000000", result[1]); // commitTime
  }

  @Test
  public void testParseFileIdAndCommitTimeFromExternalFile_WithPrefix() {
    String fileName = "data.parquet_20240101000000_fg%3Dbucket-0_hudiext";
    String[] result = ExternalFilePathUtil.parseFileIdAndCommitTimeFromExternalFile(fileName);

    assertNotNull(result);
    assertEquals(2, result.length);
    assertEquals("bucket-0/data.parquet", result[0]); // fileId includes prefix
    assertEquals("20240101000000", result[1]); // commitTime
  }

  @Test
  public void testParseFileIdAndCommitTimeFromExternalFile_FileWithUnderscores() {
    String fileName = "my_data_file.parquet_20240101000000_hudiext";
    String[] result = ExternalFilePathUtil.parseFileIdAndCommitTimeFromExternalFile(fileName);

    assertNotNull(result);
    assertEquals(2, result.length);
    assertEquals("my_data_file.parquet", result[0]);
    assertEquals("20240101000000", result[1]);
  }

  @Test
  public void testParseFileIdAndCommitTimeFromExternalFile_FileWithMultipleDots() {
    String fileName = "data.backup.parquet_20240101000000_hudiext";
    String[] result = ExternalFilePathUtil.parseFileIdAndCommitTimeFromExternalFile(fileName);

    assertNotNull(result);
    assertEquals(2, result.length);
    assertEquals("data.backup.parquet", result[0]);
    assertEquals("20240101000000", result[1]);
  }

  @Test
  public void testParseFileIdAndCommitTimeFromExternalFile_NoExtension() {
    String fileName = "datafile_20240101000000_hudiext";
    String[] result = ExternalFilePathUtil.parseFileIdAndCommitTimeFromExternalFile(fileName);

    assertNotNull(result);
    assertEquals(2, result.length);
    assertEquals("datafile", result[0]);
    assertEquals("20240101000000", result[1]);
  }

  @Test
  public void testGetFullPath_OfPartition_WithPrefix() {
    StoragePath parent = new StoragePath("/table/partition1/bucket-0");
    String fileName = "file1.parquet_20240101000000_fg%3Dbucket-0_hudiext";

    StoragePath result = ExternalFilePathUtil.getFullPathOfPartition(parent, fileName);
    assertEquals(new StoragePath("/table/partition1"), result);
  }

  @Test
  public void testGetFullPath_OfPartition_WithNestedPrefix_2Levels() {
    StoragePath parent = new StoragePath("/table/partition1/bucket-0/subdir");
    String fileName = "file1.parquet_20240101000000_fg%3Dbucket-0%2Fsubdir_hudiext";

    StoragePath result = ExternalFilePathUtil.getFullPathOfPartition(parent, fileName);
    assertEquals(new StoragePath("/table/partition1"), result);
  }

  @Test
  public void testGetFullPath_OfPartition_WithNestedPrefix_3Levels() {
    StoragePath parent = new StoragePath("/table/partition1/bucket-0/subdir1/subdir2");
    String fileName = "file1.parquet_20240101000000_fg%3Dbucket-0%2Fsubdir1%2Fsubdir2_hudiext";

    StoragePath result = ExternalFilePathUtil.getFullPathOfPartition(parent, fileName);
    assertEquals(new StoragePath("/table/partition1"), result);
  }

  @Test
  public void testGetFullPath_OfPartition_WithoutPrefix() {
    StoragePath parent = new StoragePath("/table/partition1");
    String fileName = "file1.parquet_20240101000000_hudiext";

    StoragePath result = ExternalFilePathUtil.getFullPathOfPartition(parent, fileName);
    assertEquals(new StoragePath("/table/partition1"), result);
  }

  @Test
  public void testMaybeHandleExternallyGeneratedFileName_NullPathInfo() {
    StoragePathInfo result = ExternalFilePathUtil.maybeHandleExternallyGeneratedFileName(null, "fileId");
    assertNull(result);
  }

  @Test
  public void testMaybeHandleExternallyGeneratedFileName_NonExternalFile() {
    StoragePath path = new StoragePath("/table/partition1/file_1-0-1_20240101000000.parquet");
    StoragePathInfo pathInfo = new StoragePathInfo(path, 1000, false, (short) 1, 128 * 1024 * 1024, 0);

    StoragePathInfo result = ExternalFilePathUtil.maybeHandleExternallyGeneratedFileName(pathInfo, "fileId");
    assertEquals(pathInfo, result);
  }

  @Test
  public void testMaybeHandleExternallyGeneratedFileName_ExternalFileLegacy() {
    StoragePath originalPath = new StoragePath("/table/partition1/file1.parquet_20240101000000_hudiext");
    StoragePathInfo pathInfo = new StoragePathInfo(originalPath, 1000, false, (short) 1, 128 * 1024 * 1024, 12345);
    String fileId = "file1.parquet";

    StoragePathInfo result = ExternalFilePathUtil.maybeHandleExternallyGeneratedFileName(pathInfo, fileId);

    assertNotNull(result);
    assertEquals(new StoragePath("/table/partition1/file1.parquet"), result.getPath());
    assertEquals(1000, result.getLength());
    assertEquals(12345, result.getModificationTime());
  }

  @Test
  public void testMaybeHandleExternallyGeneratedFileName_ExternalFileWithPrefix() {
    StoragePath originalPath = new StoragePath("/table/partition1/bucket-0/file1.parquet_20240101000000_fg%3Dbucket-0_hudiext");
    StoragePathInfo pathInfo = new StoragePathInfo(originalPath, 1000, false, (short) 1, 128 * 1024 * 1024, 12345);
    String fileId = "bucket-0/file1.parquet";

    StoragePathInfo result = ExternalFilePathUtil.maybeHandleExternallyGeneratedFileName(pathInfo, fileId);

    assertNotNull(result);
    // Parent should go up two levels (bucket-0 and then original parent), resulting in /table/partition1/bucket-0/file1.parquet
    assertEquals(new StoragePath("/table/partition1/bucket-0/file1.parquet"), result.getPath());
    assertEquals(1000, result.getLength());
    assertEquals(12345, result.getModificationTime());
  }

  @Test
  public void testRoundTrip_LegacyFormat() {
    String originalFile = "mydata.parquet";
    String withMarker = ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(originalFile, COMMIT_TIME);

    assertTrue(ExternalFilePathUtil.isExternallyCreatedFile(withMarker));

    String[] parsed = ExternalFilePathUtil.parseFileIdAndCommitTimeFromExternalFile(withMarker);
    assertEquals(originalFile, parsed[0]);
    assertEquals(COMMIT_TIME, parsed[1]);
  }

  @Test
  public void testRoundTrip_WithPrefix() {
    String originalFile = "mydata.parquet";
    String prefix = "bucket-0";
    String withMarker = ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(originalFile, COMMIT_TIME, prefix);

    assertTrue(ExternalFilePathUtil.isExternallyCreatedFile(withMarker));

    String[] parsed = ExternalFilePathUtil.parseFileIdAndCommitTimeFromExternalFile(withMarker);
    assertEquals(prefix + "/" + originalFile, parsed[0]);
    assertEquals(COMMIT_TIME, parsed[1]);
  }
}
