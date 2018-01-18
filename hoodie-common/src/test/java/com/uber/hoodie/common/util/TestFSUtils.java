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
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class TestFSUtils {

  @Rule
  public final EnvironmentVariables environmentVariables
      = new EnvironmentVariables();

  @Test
  public void testMakeDataFileName() {
    String commitTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    int taskPartitionId = 2;
    String fileName = UUID.randomUUID().toString();
    assertTrue(FSUtils.makeDataFileName(commitTime, taskPartitionId, fileName)
        .equals(fileName + "_" + taskPartitionId + "_" + commitTime + ".parquet"));
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
    String fullFileName = FSUtils.makeDataFileName(commitTime, taskPartitionId, fileName);
    assertTrue(FSUtils.getCommitTime(fullFileName).equals(commitTime));
  }

  @Test
  public void testGetFileNameWithoutMeta() {
    String commitTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    int taskPartitionId = 2;
    String fileName = UUID.randomUUID().toString();
    String fullFileName = FSUtils.makeDataFileName(commitTime, taskPartitionId, fileName);
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
}
