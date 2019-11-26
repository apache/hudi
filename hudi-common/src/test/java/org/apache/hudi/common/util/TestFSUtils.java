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

import org.apache.hudi.common.HoodieCommonTestHarness;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests file system utils.
 */
public class TestFSUtils extends HoodieCommonTestHarness {

  private static String TEST_WRITE_TOKEN = "1-0-1";

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Before
  public void setUp() throws IOException {
    initMetaClient();
  }

  @Test
  public void testMakeDataFileName() {
    String commitTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    int taskPartitionId = 2;
    String fileName = UUID.randomUUID().toString();
    assertTrue(FSUtils.makeDataFileName(commitTime, TEST_WRITE_TOKEN, fileName)
        .equals(fileName + "_" + TEST_WRITE_TOKEN + "_" + commitTime + ".parquet"));
  }

  @Test
  public void testMaskFileName() {
    String commitTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    int taskPartitionId = 2;
    assertTrue(FSUtils.maskWithoutFileId(commitTime, taskPartitionId)
        .equals("*_" + taskPartitionId + "_" + commitTime + ".parquet"));
  }

  @Test
  /**
   * Tests if process Files return only paths excluding marker directories Cleaner, Rollback and compaction-scheduling
   * logic was recursively processing all subfolders including that of ".hoodie" when looking for partition-paths. This
   * causes a race when they try to list all folders (recursively) but the marker directory (that of compaction inside
   * of ".hoodie" folder) is deleted underneath by compactor. This code tests the fix by ensuring ".hoodie" and their
   * subfolders are never processed.
   */
  public void testProcessFiles() throws Exception {
    // All directories including marker dirs.
    List<String> folders =
        Arrays.asList("2016/04/15", "2016/05/16", ".hoodie/.temp/2/2016/04/15", ".hoodie/.temp/2/2016/05/16");
    folders.stream().forEach(f -> {
      try {
        metaClient.getFs().mkdirs(new Path(new Path(basePath), f));
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });

    // Files inside partitions and marker directories
    List<String> files = Arrays.asList("2016/04/15/1_1-0-1_20190528120000.parquet",
        "2016/05/16/2_1-0-1_20190528120000.parquet", ".hoodie/.temp/2/2016/05/16/2_1-0-1_20190528120000.parquet",
        ".hoodie/.temp/2/2016/04/15/1_1-0-1_20190528120000.parquet");

    files.stream().forEach(f -> {
      try {
        metaClient.getFs().create(new Path(new Path(basePath), f));
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });

    // Test excluding meta-folder
    final List<String> collected = new ArrayList<>();
    FSUtils.processFiles(metaClient.getFs(), basePath, (status) -> {
      collected.add(status.getPath().toString());
      return true;
    }, true);

    Assert.assertTrue("Hoodie MetaFolder MUST be skipped but got :" + collected,
        collected.stream().noneMatch(s -> s.contains(HoodieTableMetaClient.METAFOLDER_NAME)));
    // Check if only files are listed
    Assert.assertEquals(2, collected.size());

    // Test including meta-folder
    final List<String> collected2 = new ArrayList<>();
    FSUtils.processFiles(metaClient.getFs(), basePath, (status) -> {
      collected2.add(status.getPath().toString());
      return true;
    }, false);

    Assert.assertFalse("Hoodie MetaFolder will be present :" + collected2,
        collected2.stream().noneMatch(s -> s.contains(HoodieTableMetaClient.METAFOLDER_NAME)));
    // Check if only files are listed including hoodie.properties
    Assert.assertEquals("Collected=" + collected2, 5, collected2.size());
  }

  @Test
  public void testGetCommitTime() {
    String commitTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    int taskPartitionId = 2;
    String fileName = UUID.randomUUID().toString();
    String fullFileName = FSUtils.makeDataFileName(commitTime, TEST_WRITE_TOKEN, fileName);
    assertTrue(FSUtils.getCommitTime(fullFileName).equals(commitTime));
  }

  @Test
  public void testGetFileNameWithoutMeta() {
    String commitTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    int taskPartitionId = 2;
    String fileName = UUID.randomUUID().toString();
    String fullFileName = FSUtils.makeDataFileName(commitTime, TEST_WRITE_TOKEN, fileName);
    assertTrue(FSUtils.getFileId(fullFileName).equals(fileName));
  }

  @Test
  public void testEnvVarVariablesPickedup() {
    environmentVariables.set("HOODIE_ENV_fs_DOT_key1", "value1");
    Configuration conf = FSUtils.prepareHadoopConf(HoodieTestUtils.getDefaultHadoopConf());
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
    assertEquals("hudi/sub", FSUtils.getRelativePartitionPath(basePath, partitionPath));
  }

  @Test
  public void testGetRelativePartitionPathSameFolder() {
    Path basePath = new Path("/test");
    Path partitionPath = new Path("/test");
    assertEquals("", FSUtils.getRelativePartitionPath(basePath, partitionPath));
  }

  @Test
  public void testGetRelativePartitionPathRepeatedFolderNameBasePath() {
    Path basePath = new Path("/test/apache/apache");
    Path partitionPath = new Path("/test/apache/apache/hudi");
    assertEquals("hudi", FSUtils.getRelativePartitionPath(basePath, partitionPath));
  }

  @Test
  public void testGetRelativePartitionPathRepeatedFolderNamePartitionPath() {
    Path basePath = new Path("/test/apache");
    Path partitionPath = new Path("/test/apache/apache/hudi");
    assertEquals("apache/hudi", FSUtils.getRelativePartitionPath(basePath, partitionPath));
  }

  @Test
  public void testOldLogFileName() {
    // Check if old log file names are still parseable by FSUtils method
    String partitionPath = "2019/01/01/";
    String fileName = UUID.randomUUID().toString();
    String oldLogFile = makeOldLogFileName(fileName, ".log", "100", 1);
    Path rlPath = new Path(new Path(partitionPath), oldLogFile);
    Assert.assertTrue(FSUtils.isLogFile(rlPath));
    assertEquals(fileName, FSUtils.getFileIdFromLogPath(rlPath));
    assertEquals("100", FSUtils.getBaseCommitTimeFromLogPath(rlPath));
    assertEquals(1, FSUtils.getFileVersionFromLog(rlPath));
    Assert.assertNull(FSUtils.getTaskPartitionIdFromLogPath(rlPath));
    Assert.assertNull(FSUtils.getStageIdFromLogPath(rlPath));
    Assert.assertNull(FSUtils.getTaskAttemptIdFromLogPath(rlPath));
    Assert.assertNull(FSUtils.getWriteTokenFromLogPath(rlPath));
  }

  @Test
  public void tesLogFileName() {
    // Check if log file names are parseable by FSUtils method
    String partitionPath = "2019/01/01/";
    String fileName = UUID.randomUUID().toString();
    String logFile = FSUtils.makeLogFileName(fileName, ".log", "100", 2, "1-0-1");
    System.out.println("Log File =" + logFile);
    Path rlPath = new Path(new Path(partitionPath), logFile);
    Assert.assertTrue(FSUtils.isLogFile(rlPath));
    assertEquals(fileName, FSUtils.getFileIdFromLogPath(rlPath));
    assertEquals("100", FSUtils.getBaseCommitTimeFromLogPath(rlPath));
    assertEquals(2, FSUtils.getFileVersionFromLog(rlPath));
    assertEquals(new Integer(1), FSUtils.getTaskPartitionIdFromLogPath(rlPath));
    assertEquals(new Integer(0), FSUtils.getStageIdFromLogPath(rlPath));
    assertEquals(new Integer(1), FSUtils.getTaskAttemptIdFromLogPath(rlPath));
  }

  /**
   * Test Log File Comparisons when log files do not have write tokens.
   */
  @Test
  public void testOldLogFilesComparison() {
    String log1Ver0 = makeOldLogFileName("file1", ".log", "1", 0);
    String log1Ver1 = makeOldLogFileName("file1", ".log", "1", 1);
    String log1base2 = makeOldLogFileName("file1", ".log", "2", 0);
    List<HoodieLogFile> logFiles = Arrays.asList(log1base2, log1Ver1, log1Ver0).stream().map(f -> new HoodieLogFile(f))
        .collect(Collectors.toList());
    logFiles.sort(HoodieLogFile.getLogFileComparator());
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
        Arrays.asList(log1Ver1W1, log1base2W0, log1base2W1, log1Ver1W0, log1Ver0W1, log1Ver0W0).stream()
            .map(f -> new HoodieLogFile(f)).collect(Collectors.toList());
    logFiles.sort(HoodieLogFile.getLogFileComparator());
    assertEquals(log1Ver0W0, logFiles.get(0).getFileName());
    assertEquals(log1Ver0W1, logFiles.get(1).getFileName());
    assertEquals(log1Ver1W0, logFiles.get(2).getFileName());
    assertEquals(log1Ver1W1, logFiles.get(3).getFileName());
    assertEquals(log1base2W0, logFiles.get(4).getFileName());
    assertEquals(log1base2W1, logFiles.get(5).getFileName());
  }

  public static String makeOldLogFileName(String fileId, String logFileExtension, String baseCommitTime, int version) {
    Pattern oldLogFilePattern = Pattern.compile("\\.(.*)_(.*)\\.(.*)\\.([0-9]*)(\\.([0-9]*))");
    return "." + String.format("%s_%s%s.%d", fileId, baseCommitTime, logFileExtension, version);
  }
}
