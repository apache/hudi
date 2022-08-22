/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.format.cow;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieTableSource;
import org.apache.hudi.table.format.TestInputFormat;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for CopyOnWriteInputFormat
 */
public class TestCopyOnWriteInputFormat {
  private HoodieTableSource tableSource;
  private Configuration conf;

  @TempDir
  File tempFile;

  void beforeEach(Map<String, String> options) throws Exception {
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false); // close the async compaction
    options.forEach((key, value) -> conf.setString(key, value));
    options.put(FlinkOptions.READ_AS_STREAMING.key(), "false");

    StreamerUtil.initTableIfNotExists(conf);
    this.tableSource = getTableSource(conf);
  }

  private HoodieTableSource getTableSource(Configuration conf) {
    return new HoodieTableSource(
        TestConfigurations.TABLE_SCHEMA,
        new Path(tempFile.getAbsolutePath()),
        Collections.singletonList("partition"),
        "default",
        conf);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testReadCOWCreateInputSplitAsync(boolean createSplitAsync) throws Exception {
    HashMap<String, String> map = new HashMap<>();
    map.put(FlinkOptions.READ_COW_CREATE_SPLIT_ASYNC_ENABLED.key(), String.valueOf(createSplitAsync));
    beforeEach(map);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    List<RowData> result = TestInputFormat.readData(this.tableSource.getInputFormat());
    TestData.assertRowDataEquals(result, TestData.DATA_SET_INSERT);
  }

  @Test
  void testCOWGetFileStatusAndBlockSpecs() throws Exception {
    // create input splits async
    HashMap<String, String> map1 = new HashMap<>();
    map1.put(FlinkOptions.READ_COW_CREATE_SPLIT_ASYNC_ENABLED.key(), "true");
    beforeEach(map1);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    assertThat(this.tableSource.getInputFormat(), instanceOf(CopyOnWriteInputFormat.class));

    CopyOnWriteInputFormat inputFormat = (CopyOnWriteInputFormat) this.tableSource.getInputFormat();
    CopyOnWriteInputFormat.FileStatusSpec totalFileStatusSpec1 = inputFormat.getTotalFileStatusSpec(inputFormat.getFilePaths());
    List<CopyOnWriteInputFormat.FileBlockSpec> totalFileBlockSpec1 = inputFormat.getTotalFileBlockSpec(totalFileStatusSpec1.getFileStatuses());

    // create input splits sync
    HashMap<String, String> map2 = new HashMap<>();
    map2.put(FlinkOptions.READ_COW_CREATE_SPLIT_ASYNC_ENABLED.key(), "false");
    beforeEach(map2);

    inputFormat = (CopyOnWriteInputFormat) this.tableSource.getInputFormat();

    CopyOnWriteInputFormat.FileStatusSpec totalFileStatusSpec2 = inputFormat.getTotalFileStatusSpec(inputFormat.getFilePaths());
    List<CopyOnWriteInputFormat.FileBlockSpec> totalFileBlockSpec2 = inputFormat.getTotalFileBlockSpec(totalFileStatusSpec2.getFileStatuses());

    assertThat(totalFileStatusSpec1.getFileStatuses().size(), equalTo(totalFileStatusSpec2.getFileStatuses().size()));
    assertThat(totalFileStatusSpec1.getFileLength(), equalTo(totalFileStatusSpec2.getFileLength()));
    assertThat(totalFileStatusSpec1.getUnsplitable(), equalTo(totalFileStatusSpec2.getUnsplitable()));

    assertThat(totalFileBlockSpec1.size(), equalTo(totalFileBlockSpec2.size()));
    Map<FileStatus, BlockLocation[]> blockLocationsMap1 = totalFileBlockSpec1.stream().collect(
        Collectors.toMap(CopyOnWriteInputFormat.FileBlockSpec::getFileStatus, CopyOnWriteInputFormat.FileBlockSpec::getBlockLocations));
    Map<FileStatus, BlockLocation[]> blockLocationsMap2 = totalFileBlockSpec2.stream().collect(
        Collectors.toMap(CopyOnWriteInputFormat.FileBlockSpec::getFileStatus, CopyOnWriteInputFormat.FileBlockSpec::getBlockLocations));

    for (Map.Entry<FileStatus, BlockLocation[]> fileStatusIntegerEntry : blockLocationsMap1.entrySet()) {
      BlockLocation[] blockLocations1 = fileStatusIntegerEntry.getValue();
      BlockLocation[] blockLocations2 = blockLocationsMap2.get(fileStatusIntegerEntry.getKey());
      Arrays.sort(blockLocations1);
      Arrays.sort(blockLocations2);
      assertThat(Arrays.toString(blockLocations1), is(Arrays.toString(blockLocations2)));
    }
  }
}
