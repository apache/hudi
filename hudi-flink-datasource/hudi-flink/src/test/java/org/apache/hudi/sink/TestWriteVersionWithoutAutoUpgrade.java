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

package org.apache.hudi.sink;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.sink.utils.TestFunctionWrapper;
import org.apache.hudi.sink.utils.TestWriteBase;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that a Flink writer whose write version differs from the table version is refused when
 * auto upgrade is disabled. The write tasks build their own write configs, so the refusal has to
 * fail the job when the coordinator starts, before any task writes a file.
 */
public class TestWriteVersionWithoutAutoUpgrade extends TestWriteBase {

  @Override
  protected void setUp(Configuration conf) {
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  @ParameterizedTest
  @CsvSource({"6,9", "9,6", "9,"})
  public void testMismatchedWriteVersionIsRefused(int tableVersion, Integer writeVersion) throws Exception {
    conf.set(FlinkOptions.WRITE_TABLE_VERSION, tableVersion);
    preparePipeline(conf)
        .consume(TestData.DATA_SET_INSERT)
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .checkWrittenData(EXPECTED1, 4)
        .end();
    Map<String, List<String>> logFilesBefore = listLogFiles();
    int instantsBefore = countInstants();

    Configuration writerConf = conf.clone();
    if (writeVersion != null) {
      writerConf.set(FlinkOptions.WRITE_TABLE_VERSION, writeVersion);
    } else {
      // an unset write version uses the default, the current table version
      writerConf.removeConfig(FlinkOptions.WRITE_TABLE_VERSION);
    }
    writerConf.setString(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "false");
    TestFunctionWrapper<RowData> pipeline = TestData.getWritePipeline(tempFile.getAbsolutePath(), writerConf);
    try {
      // The coordinator rethrows a start failure, so the job fails before any write task opens.
      Throwable cause = assertThrows(HoodieException.class, pipeline::openFunction);
      while (cause != null && !(cause instanceof HoodieUpgradeDowngradeException)) {
        cause = cause.getCause();
      }
      assertNotNull(cause, "the coordinator should fail to start on the version mismatch");
      assertTrue(cause.getMessage().contains("does not match write version"), cause.getMessage());
    } finally {
      // only the coordinator was started; it holds the write client created before the check
      pipeline.getCoordinator().close();
    }

    assertEquals(HoodieTableVersion.fromVersionCode(tableVersion),
        StreamerUtil.createMetaClient(writerConf).getTableConfig().getTableVersion());
    assertEquals(instantsBefore, countInstants(), "the refused writer must not create any instant");
    assertEquals(logFilesBefore, listLogFiles());
    TestData.checkWrittenDataMOR(tempFile, EXPECTED1, 4);
  }

  private int countInstants() {
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    return metaClient.getActiveTimeline().countInstants();
  }

  /**
   * Lists log file names per partition.
   */
  private Map<String, List<String>> listLogFiles() {
    Map<String, List<String>> result = new TreeMap<>();
    File[] partitionDirs = tempFile.listFiles(file -> !file.getName().startsWith(".") && file.isDirectory());
    assertNotNull(partitionDirs);
    for (File partitionDir : partitionDirs) {
      List<String> names = new ArrayList<>();
      File[] files = partitionDir.listFiles(file -> file.getName().contains(".log.") && !file.getName().endsWith(".crc"));
      assertNotNull(files);
      for (File file : files) {
        names.add(file.getName());
      }
      names.sort(String::compareTo);
      result.put(partitionDir.getName(), names);
    }
    return result;
  }
}
