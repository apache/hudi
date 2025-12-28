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

import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.fs.HoodieWrapperFileSystem;
import org.apache.hudi.hadoop.fs.inline.HadoopInLineFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieFileFormat.HOODIE_LOG;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToHadoopPath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests file system utils.
 */
public class TestFSUtils extends HoodieCommonTestHarness {

  private static final String TEST_WRITE_TOKEN = "1-0-1";

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @BeforeEach
  public void setUp() throws IOException {
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  @Test
  public void testMakeDataFileName() {
    String instantTime = TimelineUtils.formatDate(new Date());
    String fileName = UUID.randomUUID().toString();
    assertEquals(FSUtils.makeBaseFileName(instantTime, TEST_WRITE_TOKEN, fileName, HoodieCommonTestHarness.BASE_FILE_EXTENSION),
        fileName + "_" + TEST_WRITE_TOKEN + "_" + instantTime + HoodieCommonTestHarness.BASE_FILE_EXTENSION);
  }

  @Test
  public void testMaskFileName() {
    String instantTime = TimelineUtils.formatDate(new Date());
    int taskPartitionId = 2;
    assertEquals(FSUtils.maskWithoutFileId(instantTime, taskPartitionId), "*_" + taskPartitionId + "_" + instantTime + HoodieCommonTestHarness.BASE_FILE_EXTENSION);
  }

  /**
   * Tests if process Files return only paths excluding marker directories Cleaner, Rollback and compaction-scheduling
   * logic was recursively processing all subfolders including that of ".hoodie" when looking for partition-paths. This
   * causes a race when they try to list all folders (recursively) but the marker directory (that of compaction inside
   * of ".hoodie" folder) is deleted underneath by compactor. This code tests the fix by ensuring ".hoodie" and their
   * subfolders are never processed.
   */
  @Test
  public void testProcessFiles() throws Exception {
    // All directories including marker dirs.
    List<String> folders =
        Arrays.asList("2016/04/15", "2016/05/16", ".hoodie/.temp/2/2016/04/15", ".hoodie/.temp/2/2016/05/16");
    folders.forEach(f -> {
      try {
        metaClient.getStorage().createDirectory(new StoragePath(new StoragePath(basePath), f));
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });

    // Files inside partitions and marker directories
    List<String> files = Stream.of("2016/04/15/1_1-0-1_20190528120000",
            "2016/05/16/2_1-0-1_20190528120000",
            ".hoodie/.temp/2/2016/05/16/2_1-0-1_20190528120000",
            ".hoodie/.temp/2/2016/04/15/1_1-0-1_20190528120000")
        .map(fileName -> fileName + HoodieCommonTestHarness.BASE_FILE_EXTENSION)
        .collect(Collectors.toList());

    files.forEach(f -> {
      try {
        metaClient.getStorage().create(new StoragePath(new StoragePath(basePath), f));
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });

    // Test excluding meta-folder
    final List<String> collected = new ArrayList<>();
    FSUtils.processFiles(metaClient.getStorage(), basePath, (status) -> {
      collected.add(status.getPath().toString());
      return true;
    }, true);

    assertTrue(collected.stream().noneMatch(s -> s.contains(HoodieTableMetaClient.METAFOLDER_NAME)),
        "Hoodie MetaFolder MUST be skipped but got :" + collected);
    // Check if only files are listed
    assertEquals(2, collected.size());

    // Test including meta-folder
    final List<String> collected2 = new ArrayList<>();
    FSUtils.processFiles(metaClient.getStorage(), basePath, (status) -> {
      collected2.add(status.getPath().toString());
      return true;
    }, false);

    assertFalse(collected2.stream().noneMatch(s -> s.contains(HoodieTableMetaClient.METAFOLDER_NAME)),
        "Hoodie MetaFolder will be present :" + collected2);
    // Check if only files are listed including hoodie.properties
    assertEquals(5, collected2.size(), "Collected=" + collected2);
  }

  @Test
  public void testGetCommitTime() {
    String instantTime = TimelineUtils.formatDate(new Date());
    String fileName = UUID.randomUUID().toString();
    String fullFileName = FSUtils.makeBaseFileName(instantTime, TEST_WRITE_TOKEN, fileName, HoodieCommonTestHarness.BASE_FILE_EXTENSION);
    assertEquals(instantTime, FSUtils.getCommitTime(fullFileName));
    // test log file name
    fullFileName = FSUtils.makeLogFileName(fileName, HOODIE_LOG.getFileExtension(), instantTime, 1, TEST_WRITE_TOKEN);
    assertEquals(instantTime, FSUtils.getCommitTime(fullFileName));
  }

  @Test
  public void testGetFileNameWithoutMeta() {
    String instantTime = TimelineUtils.formatDate(new Date());
    String fileName = UUID.randomUUID().toString();
    String fullFileName = FSUtils.makeBaseFileName(instantTime, TEST_WRITE_TOKEN, fileName, HoodieCommonTestHarness.BASE_FILE_EXTENSION);
    assertEquals(fileName, FSUtils.getFileId(fullFileName));
  }

  @Test
  public void testEnvVarVariablesPickedup() {
    environmentVariables.set("HOODIE_ENV_fs_DOT_key1", "value1");
    Configuration conf = HadoopFSUtils.prepareHadoopConf(HoodieTestUtils.getDefaultStorageConf().unwrap());
    assertEquals("value1", conf.get("fs.key1"));
    conf.set("fs.key1", "value11");
    conf.set("fs.key2", "value2");
    assertEquals("value11", conf.get("fs.key1"));
    assertEquals("value2", conf.get("fs.key2"));
  }

  @Test
  public void testGetRelativePartitionPath() {
    Path basePath = new Path("/test/apache");
    Path partitionPath = new Path("/test/apache/hudi/sub");
    assertEquals("hudi/sub", HadoopFSUtils.getRelativePartitionPath(basePath, partitionPath));

    Path nonPartitionPath = new Path("/test/something/else");
    assertThrows(IllegalArgumentException.class, () -> HadoopFSUtils.getRelativePartitionPath(basePath, nonPartitionPath));
  }

  @ParameterizedTest
  @CsvSource({
      "/test,/test,",
      "s3://test,s3://test,",
      "s3://test/foo,s3://test/foo,",
      "/test/foo,/test/foo,",
      "/test/apache/apache,/test/apache/apache/hudi,hudi",
      "/test/apache,/test/apache/hudi,hudi",
      "s3://test/apache,s3://test/apache/apache/hudi,apache/hudi"})
  public void testGetRelativePartitionPath(String basePathStr, String partitionPathStr, String expected) {
    StoragePath basePath = new StoragePath(basePathStr);
    StoragePath partitionPath = new StoragePath(partitionPathStr);
    String result = FSUtils.getRelativePartitionPath(basePath, partitionPath);
    assertEquals(expected == null ? "" : expected, result);
  }

  @Test
  public void testOldLogFileName() {
    // Check if old log file names are still parsable by FSUtils method
    String partitionPath = "2019/01/01/";
    String fileName = UUID.randomUUID().toString();
    String oldLogFile = makeOldLogFileName(fileName, ".log", "100", 1);
    StoragePath rlPath = new StoragePath(new StoragePath(partitionPath), oldLogFile);
    assertTrue(FSUtils.isLogFile(rlPath));
    assertEquals(fileName, FSUtils.getFileIdFromLogPath(rlPath));
    assertEquals("100", FSUtils.getDeltaCommitTimeFromLogPath(rlPath));
    assertEquals(1, FSUtils.getFileVersionFromLog(rlPath));
    assertNull(FSUtils.getTaskPartitionIdFromLogPath(rlPath));
    assertNull(FSUtils.getStageIdFromLogPath(rlPath));
    assertNull(FSUtils.getTaskAttemptIdFromLogPath(rlPath));
    assertNull(FSUtils.getWriteTokenFromLogPath(rlPath));
  }

  @Test
  public void tesLogFileName() {
    // Check if log file names are parsable by FSUtils method
    String partitionPath = "2019/01/01/";
    String fileName = UUID.randomUUID().toString();
    String logFile = FSUtils.makeLogFileName(fileName, ".log", "100", 2, "1-0-1");
    StoragePath rlPath = new StoragePath(new StoragePath(partitionPath), logFile);
    StoragePath inlineFsPath = HadoopInLineFSUtils.getInlineFilePath(
        new StoragePath(rlPath.toUri()), "file", 0, 100);
    assertTrue(FSUtils.isLogFile(rlPath));
    assertTrue(FSUtils.isLogFile(inlineFsPath));
    assertEquals(fileName, FSUtils.getFileIdFromLogPath(rlPath));
    assertEquals("100", FSUtils.getDeltaCommitTimeFromLogPath(rlPath));
    assertEquals(2, FSUtils.getFileVersionFromLog(rlPath));
    assertEquals(1, FSUtils.getTaskPartitionIdFromLogPath(rlPath));
    assertEquals(0, FSUtils.getStageIdFromLogPath(rlPath));
    assertEquals(1, FSUtils.getTaskAttemptIdFromLogPath(rlPath));

    assertEquals(logFile, FSUtils.getFileNameFromPath("/tmp/path/" + logFile));
    assertEquals(logFile, FSUtils.getFileNameFromPath("/tmp/abc/def/path/" + logFile));
    assertEquals(logFile, FSUtils.getFileNameFromPath("/tmp/" + logFile));
  }

  @Test
  public void testCdcLogFileName() {
    String partitionPath = "2022/11/04/";
    String fileName = UUID.randomUUID().toString();
    String logFile = FSUtils.makeLogFileName(fileName, ".log", "100", 2, "1-0-1") + HoodieCDCUtils.CDC_LOGFILE_SUFFIX;
    StoragePath path = new StoragePath(new StoragePath(partitionPath), logFile);

    assertTrue(FSUtils.isLogFile(path));
    assertEquals("log", FSUtils.getFileExtensionFromLog(path));
    assertEquals(fileName, FSUtils.getFileIdFromLogPath(path));
    assertEquals("100", FSUtils.getDeltaCommitTimeFromLogPath(path));
    assertEquals(1, FSUtils.getTaskPartitionIdFromLogPath(path));
    assertEquals("1-0-1", FSUtils.getWriteTokenFromLogPath(path));
    assertEquals(0, FSUtils.getStageIdFromLogPath(path));
    assertEquals(1, FSUtils.getTaskAttemptIdFromLogPath(path));
    assertEquals(2, FSUtils.getFileVersionFromLog(path));
  }

  @Test
  public void testArchiveLogFileName() {
    String partitionPath = "2022/11/04/";
    String fileName = "commits";
    String logFile = FSUtils.makeLogFileName(fileName, ".archive", "", 2, "1-0-1");
    StoragePath path = new StoragePath(new StoragePath(partitionPath), logFile);

    assertFalse(FSUtils.isLogFile(path));
    assertEquals("archive", FSUtils.getFileExtensionFromLog(path));
    assertEquals(fileName, FSUtils.getFileIdFromLogPath(path));
    assertEquals("", FSUtils.getDeltaCommitTimeFromLogPath(path));
    assertEquals(1, FSUtils.getTaskPartitionIdFromLogPath(path));
    assertEquals("1-0-1", FSUtils.getWriteTokenFromLogPath(path));
    assertEquals(0, FSUtils.getStageIdFromLogPath(path));
    assertEquals(1, FSUtils.getTaskAttemptIdFromLogPath(path));
    assertEquals(2, FSUtils.getFileVersionFromLog(path));
  }

  /**
   * Test Log File Comparisons when log files do not have write tokens.
   */
  @Test
  public void testOldLogFilesComparison() {
    String log1Ver0 = makeOldLogFileName("file1", ".log", "1", 0);
    String log1Ver1 = makeOldLogFileName("file1", ".log", "1", 1);
    String log1base2 = makeOldLogFileName("file1", ".log", "2", 0);
    List<HoodieLogFile> logFiles = Stream.of(log1base2, log1Ver1, log1Ver0).map(HoodieLogFile::new)
        .sorted(HoodieLogFile.getLogFileComparator()).collect(Collectors.toList());
    assertEquals(log1Ver0, logFiles.get(0).getFileName());
    assertEquals(log1Ver1, logFiles.get(1).getFileName());
    assertEquals(log1base2, logFiles.get(2).getFileName());
  }

  /**
   * Test Log File Comparisons when log files do not have write tokens.
   */
  @Test
  public void testLogFilesComparison() {
    String log1Ver0W0 = FSUtils.makeLogFileName("file1", ".log", "1", 0, "0-0-1");
    String log1Ver0W1 = FSUtils.makeLogFileName("file1", ".log", "1", 0, "1-1-1");
    String log1Ver1W0 = FSUtils.makeLogFileName("file1", ".log", "1", 1, "0-0-1");
    String log1Ver1W1 = FSUtils.makeLogFileName("file1", ".log", "1", 1, "1-1-1");
    String log1base2W0 = FSUtils.makeLogFileName("file1", ".log", "2", 0, "0-0-1");
    String log1base2W1 = FSUtils.makeLogFileName("file1", ".log", "2", 0, "1-1-1");

    List<HoodieLogFile> logFiles =
        Stream.of(log1Ver1W1, log1base2W0, log1base2W1, log1Ver1W0, log1Ver0W1, log1Ver0W0)
            .map(HoodieLogFile::new).sorted(HoodieLogFile.getLogFileComparator()).collect(Collectors.toList());
    assertEquals(log1Ver0W0, logFiles.get(0).getFileName());
    assertEquals(log1Ver0W1, logFiles.get(1).getFileName());
    assertEquals(log1Ver1W0, logFiles.get(2).getFileName());
    assertEquals(log1Ver1W1, logFiles.get(3).getFileName());
    assertEquals(log1base2W0, logFiles.get(4).getFileName());
    assertEquals(log1base2W1, logFiles.get(5).getFileName());
  }

  @Test
  public void testLogFilesComparisonWithCDCFile() {
    HoodieLogFile log1 = new HoodieLogFile(new StoragePath(FSUtils.makeLogFileName("file1", ".log", "1", 0, "0-0-1")));
    HoodieLogFile log2 = new HoodieLogFile(new StoragePath(FSUtils.makeLogFileName("file1", ".log", "2", 0, "0-0-1")));
    HoodieLogFile log3 = new HoodieLogFile(new StoragePath(FSUtils.makeLogFileName("file1", ".log", "2", 1, "0-0-1")));
    HoodieLogFile log4 = new HoodieLogFile(new StoragePath(FSUtils.makeLogFileName("file1", ".log", "2", 1, "1-1-1")));
    HoodieLogFile log5 = new HoodieLogFile(new StoragePath(FSUtils.makeLogFileName("file1", ".log", "2", 1, "1-1-1") + HoodieCDCUtils.CDC_LOGFILE_SUFFIX));

    TreeSet<HoodieLogFile> logFilesSet = new TreeSet<>(HoodieLogFile.getLogFileComparator());
    logFilesSet.add(log1);
    logFilesSet.add(log2);
    logFilesSet.add(log3);
    logFilesSet.add(log4);
    logFilesSet.add(log5);

    List<HoodieLogFile> logFilesList = new ArrayList<>(logFilesSet);
    assertEquals(log1, logFilesList.get(0));
    assertEquals(log2, logFilesList.get(1));
    assertEquals(log3, logFilesList.get(2));
    assertEquals(log4, logFilesList.get(3));
    assertEquals(log5, logFilesList.get(4));
  }

  public static String makeOldLogFileName(String fileId, String logFileExtension, String deltaCommitTime, int version) {
    return "." + String.format("%s_%s%s.%d", fileId, deltaCommitTime, logFileExtension, version);
  }

  @Test
  public void testFileNameRelatedFunctions() throws Exception {
    String instantTime = "20160501010101";
    String partitionStr = "2016/05/01";
    int taskPartitionId = 456;
    String writeToken = "4-5-6";
    String fileId = "Id123";
    int version = 1;
    final String LOG_STR = "log";
    final String LOG_EXTENSION = "." + LOG_STR;

    // data file name
    String dataFileName = FSUtils.makeBaseFileName(instantTime, writeToken, fileId, HoodieCommonTestHarness.BASE_FILE_EXTENSION);
    assertEquals(instantTime, FSUtils.getCommitTime(dataFileName));
    assertEquals(fileId, FSUtils.getFileId(dataFileName));

    String logFileName = FSUtils.makeLogFileName(fileId, LOG_EXTENSION, instantTime, version, writeToken);
    assertTrue(FSUtils.isLogFile(new StoragePath(logFileName)));
    assertEquals(instantTime, FSUtils.getDeltaCommitTimeFromLogPath(new StoragePath(logFileName)));
    assertEquals(fileId, FSUtils.getFileIdFromLogPath(new StoragePath(logFileName)));
    assertEquals(version, FSUtils.getFileVersionFromLog(new StoragePath(logFileName)));
    assertEquals(LOG_STR, FSUtils.getFileExtensionFromLog(new StoragePath(logFileName)));

    // create three versions of log file
    java.nio.file.Path partitionPath = Paths.get(basePath, partitionStr);
    Files.createDirectories(partitionPath);
    String log1 = FSUtils.makeLogFileName(fileId, LOG_EXTENSION, instantTime, 1, writeToken);
    Files.createFile(partitionPath.resolve(log1));
    String log2 = FSUtils.makeLogFileName(fileId, LOG_EXTENSION, instantTime, 2, writeToken);
    Files.createFile(partitionPath.resolve(log2));
    String log3 = FSUtils.makeLogFileName(fileId, LOG_EXTENSION, instantTime, 3, writeToken);
    Files.createFile(partitionPath.resolve(log3));

    assertEquals(3, (int) FSUtils.getLatestLogVersion(
            HoodieStorageUtils.getStorage(basePath, HadoopFSUtils.getStorageConf(new Configuration())),
            new StoragePath(partitionPath.toString()), fileId, LOG_EXTENSION, instantTime).get()
        .getLeft());
  }

  @Test
  public void testGetFilename() {
    assertEquals("file1.parquet", FSUtils.getFileName("/2022/07/29/file1.parquet", "/2022/07/29"));
    assertEquals("file2.parquet", FSUtils.getFileName("2022/07/29/file2.parquet", "2022/07/29"));
    assertEquals("file3.parquet", FSUtils.getFileName("/file3.parquet", ""));
    assertEquals("file4.parquet", FSUtils.getFileName("file4.parquet", ""));
  }

  private void prepareTestDirectory(HoodieStorage storage, StoragePath rootDir) throws IOException {
    // Directory structure
    // .hoodie/.temp/
    //  - subdir1
    //    - file1.txt
    //  - subdir2
    //    - file2.txt
    //  - file3
    String subDir1 = rootDir + "/subdir1";
    String file1 = subDir1 + "/file1.txt";
    String subDir2 = rootDir + "/subdir2";
    String file2 = subDir2 + "/file2.txt";
    String file3 = rootDir + "/file3.txt";
    String[] dirs = new String[] {rootDir.toString(), subDir1, subDir2};
    String[] files = new String[] {file1, file2, file3};
    // clean up first
    cleanUpTestDirectory(storage, rootDir);
    for (String dir : dirs) {
      storage.createDirectory(new StoragePath(dir));
    }
    for (String filename : files) {
      storage.create(new StoragePath(filename));
    }
  }

  private void cleanUpTestDirectory(HoodieStorage storage, StoragePath rootDir) throws IOException {
    storage.deleteDirectory(rootDir);
  }

  @Test
  public void testDeleteExistingDir() throws IOException {
    StoragePath rootDir = getHoodieTempDir();
    HoodieStorage storage = metaClient.getStorage();
    prepareTestDirectory(storage, rootDir);

    assertTrue(storage.exists(rootDir));
    assertTrue(FSUtils.deleteDir(
        new HoodieLocalEngineContext(metaClient.getStorageConf()), storage, rootDir, 2));
    assertFalse(storage.exists(rootDir));
  }

  @Test
  public void testDeleteNonExistingDir() throws IOException {
    StoragePath rootDir = getHoodieTempDir();
    cleanUpTestDirectory(metaClient.getStorage(), rootDir);

    assertFalse(FSUtils.deleteDir(
        new HoodieLocalEngineContext(metaClient.getStorageConf()), metaClient.getStorage(), rootDir, 2));
  }

  @Test
  public void testDeleteSubDirectoryRecursively() throws IOException {
    StoragePath rootDir = getHoodieTempDir();
    StoragePath subDir = new StoragePath(rootDir, "subdir1");
    HoodieStorage storage = metaClient.getStorage();
    prepareTestDirectory(storage, rootDir);

    assertTrue(FSUtils.deleteSubPath(subDir.toString(), storage.getConf(), true));
  }

  @Test
  public void testDeleteSubDirectoryNonRecursively() throws IOException {
    StoragePath rootDir = getHoodieTempDir();
    StoragePath subDir = new StoragePath(rootDir, "subdir1");
    HoodieStorage storage = metaClient.getStorage();
    prepareTestDirectory(storage, rootDir);

    assertThrows(
        HoodieIOException.class,
        () -> FSUtils.deleteSubPath(subDir.toString(), storage.getConf(), false));
  }

  @Test
  public void testDeleteSubPathAsFile() throws IOException {
    StoragePath rootDir = getHoodieTempDir();
    StoragePath subDir = new StoragePath(rootDir, "file3.txt");
    HoodieStorage storage = metaClient.getStorage();
    prepareTestDirectory(storage, rootDir);

    assertTrue(FSUtils.deleteSubPath(subDir.toString(), storage.getConf(), false));
  }

  @Test
  public void testDeleteNonExistingSubDirectory() throws IOException {
    StoragePath rootDir = getHoodieTempDir();
    StoragePath subDir = new StoragePath(rootDir, "subdir10");
    HoodieStorage storage = metaClient.getStorage();
    cleanUpTestDirectory(storage, rootDir);

    assertFalse(FSUtils.deleteSubPath(subDir.toString(), storage.getConf(), true));
  }

  @Test
  public void testParallelizeSubPathProcessWithExistingDir() throws IOException {
    StoragePath rootDir = getHoodieTempDir();
    HoodieStorage storage = metaClient.getStorage();
    prepareTestDirectory(storage, rootDir);
    Map<String, List<String>> result = FSUtils.parallelizeSubPathProcess(
        new HoodieLocalEngineContext(storage.getConf()), storage, rootDir, 2,
        fileStatus -> !fileStatus.getPath().getName().contains("1"),
        pairOfSubPathAndConf -> {
          Path subPath = new Path(pairOfSubPathAndConf.getKey());
          List<String> listFiles = new ArrayList<>();
          try {
            FileSystem fs = subPath.getFileSystem(pairOfSubPathAndConf.getValue().unwrapAs(Configuration.class));
            FileStatus[] fileStatuses = fs.listStatus(subPath);
            listFiles = Arrays.stream(fileStatuses)
                .map(fileStatus -> fileStatus.getPath().getName()).collect(Collectors.toList());
          } catch (IOException e) {
            e.printStackTrace();
          }
          return listFiles;
        }
    );
    assertEquals(2, result.size());
    for (String subPath : result.keySet()) {
      if (subPath.contains("subdir2")) {
        assertEquals(Collections.singletonList("file2.txt"), result.get(subPath));
      } else if (subPath.contains("file3")) {
        assertEquals(Collections.singletonList("file3.txt"), result.get(subPath));
      }
    }
  }

  @Test
  public void testGetFileStatusAtLevel() throws IOException {
    StoragePath hoodieTempDir = getHoodieTempDir();
    HoodieStorage storage = metaClient.getStorage();
    prepareTestDirectory(storage, hoodieTempDir);
    List<FileStatus> fileStatusList = HadoopFSUtils.getFileStatusAtLevel(
        new HoodieLocalEngineContext(storage.getConf()), (FileSystem) storage.getFileSystem(),
        new Path(baseUri), 3, 2);
    assertEquals(CollectionUtils.createImmutableSet(
            new Path(baseUri.toString(), ".hoodie/.temp/subdir1/file1.txt"),
            new Path(baseUri.toString(), ".hoodie/.temp/subdir2/file2.txt")),
        fileStatusList.stream()
            .map(FileStatus::getPath)
            .filter(filePath -> filePath.getName().endsWith(".txt"))
            .collect(Collectors.toSet()));
  }

  @Test
  public void testGetFileExtension() {
    String pathWithExtension = "/path/to/some/file/sample.parquet";
    String pathWithoutExtension = "/path/to/some/file/sample";
    String justFileNameWithExtension = "sample.orc";
    String justFileNameWithoutExtension = "sample";

    // file with extension
    String result1 = FSUtils.getFileExtension(pathWithExtension);
    assertEquals(".parquet", result1);

    // file without extension
    String result2 = FSUtils.getFileExtension(pathWithoutExtension);
    assertEquals("", result2);

    // just a file name with extension
    String result3 = FSUtils.getFileExtension(justFileNameWithExtension);
    assertEquals(".orc", result3);

    // just a file name without extension
    String result4 = FSUtils.getFileExtension(justFileNameWithoutExtension);
    assertEquals("", result4);

    // null input
    assertThrows(NullPointerException.class, () -> FSUtils.getFileExtension(null));
  }

  @Test
  public void testMakeQualified() {
    FileSystem fs = HadoopFSUtils.getFs("file:///a/b/c", new Configuration());
    FileSystem wrapperFs = new HoodieWrapperFileSystem(fs, new NoOpConsistencyGuard());
    HoodieStorage storage = HoodieStorageUtils.getStorage(
        HadoopFSUtils.convertToStoragePath(new Path("file:///a/b/c")),
        HadoopFSUtils.getStorageConf(fs.getConf()));
    HoodieStorage wrapperStorage = HoodieStorageUtils.getStorage(
        HadoopFSUtils.convertToStoragePath(new Path("file:///a/b/c")),
        HadoopFSUtils.getStorageConf(wrapperFs.getConf()));
    assertEquals(new StoragePath("file:///x/y"),
        FSUtils.makeQualified(storage, new StoragePath("/x/y")));
    assertEquals(new StoragePath("file:///x/y"),
        FSUtils.makeQualified(wrapperStorage, new StoragePath("/x/y")));
    assertEquals(new StoragePath("s3://x/y"),
        FSUtils.makeQualified(storage, new StoragePath("s3://x/y")));
    assertEquals(new StoragePath("s3://x/y"),
        FSUtils.makeQualified(wrapperStorage, new StoragePath("s3://x/y")));
  }

  @Test
  public void testSetModificationTime() throws IOException {
    StoragePath path = new StoragePath(basePath, "dummy.txt");
    FileSystem fs = HadoopFSUtils.getFs(basePath, new Configuration());
    HoodieStorage storage = HoodieStorageUtils.getStorage(
        HadoopFSUtils.convertToStoragePath(new Path(basePath)),
        HadoopFSUtils.getStorageConf(fs.getConf()));
    storage.create(path);
    long modificationTime = System.currentTimeMillis();
    storage.setModificationTime(path, modificationTime);
    //  Modification from local FS is in seconds precision.
    assertEquals((modificationTime / 1000), fs.getFileStatus(convertToHadoopPath(path)).getModificationTime() / 1000);
  }

  @Test
  void testComparePathsWithoutScheme() {
    String path1 = "s3://test_bucket_one/table/base/path";
    String path2 = "s3a://test_bucket_two/table/base/path";
    assertFalse(FSUtils.comparePathsWithoutScheme(path1, path2), "should return false since bucket names dont match");

    path1 = "s3a://test_bucket_one/table/new_base/path";
    path2 = "s3a://test_bucket_one/table/old_base/path";
    assertFalse(FSUtils.comparePathsWithoutScheme(path1, path2), "should return false since paths don't match");

    path1 = "s3://test_bucket_one/table/base/path";
    path2 = "s3a://test_bucket_one/table/base/path";
    assertTrue(FSUtils.comparePathsWithoutScheme(path1, path2), "should return false since bucket names match without file shema");

    path1 = "s3a://test_bucket_one/table/base/path";
    path2 = "s3a://test_bucket_one/table/base/path";
    assertTrue(FSUtils.comparePathsWithoutScheme(path1, path2), "should return true since bucket names and path matches");

    path1 = "gs://test_bucket_one/table/base/path";
    path2 = "gs://test_bucket_two/table/base/path";
    assertFalse(FSUtils.comparePathsWithoutScheme(path1, path2), "should return true since bucket names and path matches");

    path1 = "gs://test_bucket_one/table/base/path";
    path2 = "gs://test_bucket_one/table/base/path";
    assertTrue(FSUtils.comparePathsWithoutScheme(path1, path2), "should return true since bucket names and path matches");

    path1 = "file:/var/table/base/path";
    path2 = "/var/table/base/path";
    assertTrue(FSUtils.comparePathsWithoutScheme(path1, path2), "should return true since path matches");

    path1 = "file:/var/table/base/path";
    path2 = "file:/var/table/old_base/path";
    assertFalse(FSUtils.comparePathsWithoutScheme(path1, path2), "should return false since path doesn't matches");

    path1 = "table/base/path";
    path2 = "table/base/path";
    assertTrue(FSUtils.comparePathsWithoutScheme(path1, path2), "should return true since relative path doesn't matches");
  }

  @Test
  void testGetPathWithoutScheme() {
    String path1 = "s3://test_bucket_one/table/base/path";
    assertEquals(FSUtils.getPathWithoutScheme(new StoragePath(path1)).toUri().toString(), "//test_bucket_one/table/base/path", "should return false since bucket names dont match");

    path1 = "s3a://test_bucket_one/table/base/path";
    assertEquals(FSUtils.getPathWithoutScheme(new StoragePath(path1)).toUri().toString(), "//test_bucket_one/table/base/path", "should return false since bucket names dont match");

    path1 = "gs://test_bucket_one/table/base/path";
    assertEquals(FSUtils.getPathWithoutScheme(new StoragePath(path1)).toUri().toString(), "//test_bucket_one/table/base/path", "should return false since bucket names dont match");
  }

  @Test
  void testS3aToS3_AWS() {
    // Test cases for AWS S3 URLs
    assertEquals("s3://my-bucket/path/to/object", FSUtils.s3aToS3("s3a://my-bucket/path/to/object"));
    assertEquals("s3://my-bucket", FSUtils.s3aToS3("s3a://my-bucket"));
    assertEquals("s3://MY-BUCKET/PATH/TO/OBJECT", FSUtils.s3aToS3("s3a://MY-BUCKET/PATH/TO/OBJECT"));
    assertEquals("s3://my-bucket/path/to/object", FSUtils.s3aToS3("S3a://my-bucket/path/to/object"));
    assertEquals("s3://my-bucket/path/to/object", FSUtils.s3aToS3("s3A://my-bucket/path/to/object"));
    assertEquals("s3://my-bucket/path/to/object", FSUtils.s3aToS3("S3A://my-bucket/path/to/object"));
    assertEquals("s3://my-bucket/s3a://another-bucket/another/path", FSUtils.s3aToS3("s3a://my-bucket/s3a://another-bucket/another/path"));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "gs://my-bucket/path/to/object",
      "gs://my-bucket",
      "gs://MY-BUCKET/PATH/TO/OBJECT",
      "https://myaccount.blob.core.windows.net/mycontainer/path/to/blob",
      "https://myaccount.blob.core.windows.net/MYCONTAINER/PATH/TO/BLOB",
      "https://example.com/path/to/resource",
      "http://example.com",
      "ftp://example.com/resource",
      "",
      "gs://my-bucket/path/to/s3a://object",
      "gs://my-bucket s3a://my-object",
  })
  
  void testUriDoesNotChange(String uri) {
    assertEquals(uri, FSUtils.s3aToS3(uri));
  }

  private StoragePath getHoodieTempDir() {
    return new StoragePath(baseUri.toString(), ".hoodie/.temp");
  }
}
