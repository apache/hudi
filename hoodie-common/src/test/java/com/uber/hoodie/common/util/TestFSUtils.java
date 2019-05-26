/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.uber.hoodie.common.model.HoodieTestUtils;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class TestFSUtils {

  private static String TEST_WRITE_TOKEN = "1-0-1";

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

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
    assertEquals("hudi/sub",FSUtils.getRelativePartitionPath(basePath, partitionPath));
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
    Assert.assertEquals(fileName, FSUtils.getFileIdFromLogPath(rlPath));
    Assert.assertEquals("100", FSUtils.getBaseCommitTimeFromLogPath(rlPath));
    Assert.assertEquals(1, FSUtils.getFileVersionFromLog(rlPath));
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
    Assert.assertEquals(fileName, FSUtils.getFileIdFromLogPath(rlPath));
    Assert.assertEquals("100", FSUtils.getBaseCommitTimeFromLogPath(rlPath));
    Assert.assertEquals(2, FSUtils.getFileVersionFromLog(rlPath));
    Assert.assertEquals(new Integer(1), FSUtils.getTaskPartitionIdFromLogPath(rlPath));
    Assert.assertEquals(new Integer(0), FSUtils.getStageIdFromLogPath(rlPath));
    Assert.assertEquals(new Integer(1), FSUtils.getTaskAttemptIdFromLogPath(rlPath));

  }

  public static String makeOldLogFileName(String fileId, String logFileExtension,
      String baseCommitTime, int version) {
    Pattern oldLogFilePattern =
        Pattern.compile("\\.(.*)_(.*)\\.(.*)\\.([0-9]*)(\\.([0-9]*))");
    return "." + String
        .format("%s_%s%s.%d", fileId, baseCommitTime, logFileExtension, version);
  }
}
