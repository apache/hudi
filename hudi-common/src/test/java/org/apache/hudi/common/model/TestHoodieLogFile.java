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

package org.apache.hudi.common.model;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.FileNameParser;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieLogFile {
  private final String pathStr = "file:///tmp/hoodie/2021/01/01/.136281f3-c24e-423b-a65a-95dbfbddce1d_100.log.2_1-0-1";
  private final String fileId = "136281f3-c24e-423b-a65a-95dbfbddce1d";
  private final String baseCommitTime = "100";
  private final int logVersion = 2;
  private final String writeToken = "1-0-1";
  private final String fileExtension = "log";

  private final int length = 10;
  private final short blockReplication = 2;
  private final long blockSize = 1000000L;

  @Test
  void createFromLogFile() {
    StoragePathInfo pathInfo = new StoragePathInfo(new StoragePath(pathStr), length, false, blockReplication, blockSize, 0);
    HoodieLogFile hoodieLogFile = new HoodieLogFile(pathInfo);
    assertFileGetters(pathInfo, new HoodieLogFile(hoodieLogFile), length);
  }

  @Test
  void createFromFileStatus() {
    StoragePathInfo pathInfo = new StoragePathInfo(new StoragePath(pathStr), length, false, blockReplication, blockSize, 0);
    HoodieLogFile hoodieLogFile = new HoodieLogFile(pathInfo);
    assertFileGetters(pathInfo, hoodieLogFile, length);
  }

  @Test
  void createFromPath() {
    HoodieLogFile hoodieLogFile = new HoodieLogFile(new StoragePath(pathStr));
    assertFileGetters(null, hoodieLogFile, -1);
  }

  @Test
  void createFromPathAndLength() {
    HoodieLogFile hoodieLogFile = new HoodieLogFile(new StoragePath(pathStr), length);
    assertFileGetters(null, hoodieLogFile, length);
  }

  @Test
  void createFromString() {
    HoodieLogFile hoodieLogFile = new HoodieLogFile(pathStr);
    assertFileGetters(null, hoodieLogFile, -1);
  }

  @Test
  void createFromStringWithSuffix() {
    String suffix = ".cdc";
    String pathWithSuffix = pathStr + suffix;
    HoodieLogFile hoodieLogFile = new HoodieLogFile(pathWithSuffix);
    assertFileGetters(pathWithSuffix, null, hoodieLogFile, -1, suffix);
  }

  @Test
  void createFromNativeParquetLogFile() {
    String nativeLogPathStr = "file:///tmp/hoodie/2021/01/01/"
        + "136281f3-c24e-423b-a65a-95dbfbddce1d_1-0-1_20250409161256974_2.log.parquet";
    StoragePath nativeLogPath = new StoragePath(nativeLogPathStr);
    HoodieLogFile hoodieLogFile = new HoodieLogFile(nativeLogPath);

    assertTrue(FSUtils.isLogFile(nativeLogPath));
    assertFalse(FSUtils.isNativeDeleteLogFile(nativeLogPath.getName()));
    assertFalse(FSUtils.isBaseFile(nativeLogPath));
    assertEquals(fileId, hoodieLogFile.getFileId());
    assertEquals("20250409161256974", hoodieLogFile.getDeltaCommitTime());
    assertEquals(2, hoodieLogFile.getLogVersion());
    assertEquals("1-0-1", hoodieLogFile.getLogWriteToken());
    assertEquals("log", hoodieLogFile.getFileExtension());
    assertEquals("parquet", hoodieLogFile.getSuffix());
    assertEquals("log", FSUtils.getFileExtensionFromLog(nativeLogPath));
    assertEquals("20250409161256974", FSUtils.getCommitTime(nativeLogPath.getName()));
    assertEquals("20250409161256974", FSUtils.getDeltaCommitTimeFromLogPath(nativeLogPath));
    assertEquals(2, FSUtils.getFileVersionFromLog(nativeLogPath));
    assertEquals("1-0-1", FSUtils.getWriteTokenFromLogPath(nativeLogPath));
    assertEquals(1, FSUtils.getTaskPartitionIdFromLogPath(nativeLogPath));
    assertEquals(0, FSUtils.getStageIdFromLogPath(nativeLogPath));
    assertEquals(1, FSUtils.getTaskAttemptIdFromLogPath(nativeLogPath));
  }

  @Test
  void inlineLogFileDetectionOnlyMatchesInlineLogs() {
    assertTrue(FileNameParser.parseInlineLogFile(pathStr).isPresent());
    assertFalse(FileNameParser.parseInlineLogFile(nativeLogPath(LogExtensions.DATA_LOG_EXTENSION, 2)).isPresent());
    assertFalse(FileNameParser.parseInlineLogFile(fileId + "_1-0-1_100.parquet").isPresent());
    assertTrue(FSUtils.isInlineLogFile(new StoragePath(pathStr)));
    assertFalse(FSUtils.isInlineLogFile(new StoragePath(nativeLogPath(LogExtensions.DATA_LOG_EXTENSION, 2))));
    assertFalse(FSUtils.isInlineLogFile(new StoragePath(fileId + "_1-0-1_100.parquet")));
  }

  @Test
  void createFromNativeDeleteParquetLogFile() {
    String nativeDeleteLogPathStr = "file:///tmp/hoodie/2021/01/01/"
        + "136281f3-c24e-423b-a65a-95dbfbddce1d_1-0-1_20250409161256974_3.deletes.parquet";
    StoragePath nativeDeleteLogPath = new StoragePath(nativeDeleteLogPathStr);
    HoodieLogFile hoodieLogFile = new HoodieLogFile(nativeDeleteLogPath);

    assertTrue(FSUtils.isLogFile(nativeDeleteLogPath));
    assertTrue(FSUtils.isNativeDeleteLogFile(nativeDeleteLogPath.getName()));
    assertFalse(FSUtils.isBaseFile(nativeDeleteLogPath));
    assertEquals(fileId, hoodieLogFile.getFileId());
    assertEquals("20250409161256974", hoodieLogFile.getDeltaCommitTime());
    assertEquals(3, hoodieLogFile.getLogVersion());
    assertEquals("1-0-1", hoodieLogFile.getLogWriteToken());
    assertEquals("deletes", hoodieLogFile.getFileExtension());
    assertEquals("parquet", hoodieLogFile.getSuffix());
    assertEquals("deletes", FSUtils.getFileExtensionFromLog(nativeDeleteLogPath));
  }

  @Test
  void createFromNativeCdcParquetLogFile() {
    String nativeCdcLogPathStr = "file:///tmp/hoodie/2021/01/01/"
        + "136281f3-c24e-423b-a65a-95dbfbddce1d_1-0-1_20250409161256974_4.cdc.parquet";
    StoragePath nativeCdcLogPath = new StoragePath(nativeCdcLogPathStr);
    HoodieLogFile hoodieLogFile = new HoodieLogFile(nativeCdcLogPath);

    assertTrue(FSUtils.isLogFile(nativeCdcLogPath));
    assertFalse(FSUtils.isNativeDeleteLogFile(nativeCdcLogPath.getName()));
    assertFalse(FSUtils.isBaseFile(nativeCdcLogPath));
    assertEquals(fileId, hoodieLogFile.getFileId());
    assertEquals("20250409161256974", hoodieLogFile.getDeltaCommitTime());
    assertEquals(4, hoodieLogFile.getLogVersion());
    assertEquals("1-0-1", hoodieLogFile.getLogWriteToken());
    assertEquals("cdc", hoodieLogFile.getFileExtension());
    assertEquals("parquet", hoodieLogFile.getSuffix());
    assertEquals("cdc", FSUtils.getFileExtensionFromLog(nativeCdcLogPath));
    assertTrue(hoodieLogFile.isCDC());
    assertTrue(FSUtils.isNativeCDCLogFile(nativeCdcLogPathStr));
    assertTrue(FSUtils.isCDCLogFile(nativeCdcLogPathStr));
  }

  @Test
  void createFromNativeLanceLogFile() {
    String nativeLogPathStr = "file:///tmp/hoodie/2021/01/01/"
        + "136281f3-c24e-423b-a65a-95dbfbddce1d_1-0-1_20250409161256974_5.log.lance";
    StoragePath nativeLogPath = new StoragePath(nativeLogPathStr);
    HoodieLogFile hoodieLogFile = new HoodieLogFile(nativeLogPath);

    assertTrue(FSUtils.isLogFile(nativeLogPath));
    assertFalse(FSUtils.isNativeDeleteLogFile(nativeLogPath.getName()));
    assertFalse(FSUtils.isBaseFile(nativeLogPath));
    assertFalse(hoodieLogFile.isCDC());
    assertEquals(fileId, hoodieLogFile.getFileId());
    assertEquals("20250409161256974", hoodieLogFile.getDeltaCommitTime());
    assertEquals(5, hoodieLogFile.getLogVersion());
    assertEquals("1-0-1", hoodieLogFile.getLogWriteToken());
    assertEquals("log", hoodieLogFile.getFileExtension());
    assertEquals("lance", hoodieLogFile.getSuffix());
  }

  @Test
  void createFromNativeCdcLanceLogFile() {
    String nativeCdcLogPathStr = "file:///tmp/hoodie/2021/01/01/"
        + "136281f3-c24e-423b-a65a-95dbfbddce1d_1-0-1_20250409161256974_6.cdc.lance";
    StoragePath nativeCdcLogPath = new StoragePath(nativeCdcLogPathStr);
    HoodieLogFile hoodieLogFile = new HoodieLogFile(nativeCdcLogPath);

    assertTrue(FSUtils.isLogFile(nativeCdcLogPath));
    assertFalse(FSUtils.isNativeDeleteLogFile(nativeCdcLogPath.getName()));
    assertFalse(FSUtils.isBaseFile(nativeCdcLogPath));
    assertEquals(fileId, hoodieLogFile.getFileId());
    assertEquals("20250409161256974", hoodieLogFile.getDeltaCommitTime());
    assertEquals(6, hoodieLogFile.getLogVersion());
    assertEquals("1-0-1", hoodieLogFile.getLogWriteToken());
    assertEquals("cdc", hoodieLogFile.getFileExtension());
    assertEquals("lance", hoodieLogFile.getSuffix());
    assertTrue(hoodieLogFile.isCDC());
    assertTrue(FSUtils.isNativeCDCLogFile(nativeCdcLogPathStr));
    assertTrue(FSUtils.isCDCLogFile(nativeCdcLogPathStr));
  }

  @Test
  void createFromNativeLogFileWithUnknownFormatSuffix() {
    String nativeLogPathStr = "file:///tmp/hoodie/2021/01/01/"
        + "136281f3-c24e-423b-a65a-95dbfbddce1d_1-0-1_20250409161256974_7.log.custom";
    StoragePath nativeLogPath = new StoragePath(nativeLogPathStr);
    HoodieLogFile hoodieLogFile = new HoodieLogFile(nativeLogPath);

    assertTrue(FSUtils.isLogFile(nativeLogPath));
    assertFalse(FSUtils.isNativeDeleteLogFile(nativeLogPath.getName()));
    assertFalse(FSUtils.isBaseFile(nativeLogPath));
    assertFalse(hoodieLogFile.isCDC());
    assertEquals(fileId, hoodieLogFile.getFileId());
    assertEquals("20250409161256974", hoodieLogFile.getDeltaCommitTime());
    assertEquals(7, hoodieLogFile.getLogVersion());
    assertEquals("1-0-1", hoodieLogFile.getLogWriteToken());
    assertEquals("log", hoodieLogFile.getFileExtension());
    assertEquals("custom", hoodieLogFile.getSuffix());
    assertFalse(FSUtils.isNativeCDCLogFile(nativeLogPathStr));
    assertFalse(FSUtils.isCDCLogFile(nativeLogPathStr));
  }

  @Test
  void logFileComparatorOrdersNativeDeletesAfterLogsForSameVersion() {
    HoodieLogFile logFile = new HoodieLogFile(new StoragePath(
        nativeLogPath(LogExtensions.DATA_LOG_EXTENSION, 8)));
    HoodieLogFile deleteFile = new HoodieLogFile(new StoragePath(
        nativeLogPath(LogExtensions.DELETE_LOG_EXTENSION, 8)));

    List<HoodieLogFile> logFiles = Arrays.asList(deleteFile, logFile);
    logFiles.sort(HoodieLogFile.getLogFileComparator());

    assertEquals(logFile, logFiles.get(0));
    assertEquals(deleteFile, logFiles.get(1));
  }

  @Test
  void logFileComparatorOrdersNativeLogExtensionsByPrecedenceForSameVersion() {
    HoodieLogFile logFile = new HoodieLogFile(new StoragePath(
        nativeLogPath(LogExtensions.DATA_LOG_EXTENSION, 8)));
    HoodieLogFile deleteFile = new HoodieLogFile(new StoragePath(
        nativeLogPath(LogExtensions.DELETE_LOG_EXTENSION, 8)));
    HoodieLogFile cdcFile = new HoodieLogFile(new StoragePath(
        nativeLogPath(LogExtensions.CDC_LOG_EXTENSION, 8)));

    List<HoodieLogFile> logFiles = Arrays.asList(cdcFile, deleteFile, logFile);
    logFiles.sort(HoodieLogFile.getLogFileComparator());

    assertEquals(Arrays.asList(logFile, deleteFile, cdcFile), logFiles);
    assertFalse(FSUtils.isNativeLogFile(nativeLogPath(LogExtensions.ARCHIVE_LOG_EXTENSION, 8)));
  }

  private String nativeLogPath(String extension, int version) {
    return String.format("/tmp/%s_%s_%s_%d.%s.parquet",
        fileId, writeToken, "20250409161256974", version, extension);
  }

  private void assertFileGetters(StoragePathInfo pathInfo, HoodieLogFile hoodieLogFile,
                                 long fileLength) {
    assertFileGetters(pathStr, pathInfo, hoodieLogFile, fileLength, "");
  }

  private void assertFileGetters(String pathStr, StoragePathInfo pathInfo,
                                 HoodieLogFile hoodieLogFile,
                                 long fileLength, String suffix) {
    assertEquals(fileId, hoodieLogFile.getFileId());
    assertEquals(baseCommitTime, hoodieLogFile.getDeltaCommitTime());
    assertEquals(logVersion, hoodieLogFile.getLogVersion());
    assertEquals(writeToken, hoodieLogFile.getLogWriteToken());
    assertEquals(fileExtension, hoodieLogFile.getFileExtension());
    assertEquals(new StoragePath(pathStr), hoodieLogFile.getPath());
    assertEquals(fileLength, hoodieLogFile.getFileSize());
    assertEquals(pathInfo, hoodieLogFile.getPathInfo());
    assertEquals(suffix, hoodieLogFile.getSuffix());
  }
}
