/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieTableSource;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for MergeOnReadInputFormat and ParquetInputFormat.
 */
public class TestInputFormat {

  private HoodieTableSource tableSource;
  private Configuration conf;

  @TempDir
  File tempFile;

  void beforeEach(HoodieTableType tableType) throws IOException {
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false); // close the async compaction

    StreamerUtil.initTableIfNotExists(conf);
    this.tableSource = new HoodieTableSource(
        TestConfigurations.TABLE_SCHEMA,
        new Path(tempFile.getAbsolutePath()),
        Collections.singletonList("partition"),
        "default",
        conf);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testRead(HoodieTableType tableType) throws Exception {
    beforeEach(tableType);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();

    List<RowData> result = readData(inputFormat);

    String actual = TestData.rowDataToString(result);
    String expected = TestData.rowDataToString(TestData.DATA_SET_INSERT);
    assertThat(actual, is(expected));

    // write another commit to read again
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);

    // refresh the input format
    this.tableSource.reloadActiveTimeline();
    inputFormat = this.tableSource.getInputFormat();

    result = readData(inputFormat);

    actual = TestData.rowDataToString(result);
    expected = "[id1,Danny,24,1970-01-01T00:00:00.001,par1, "
        + "id10,Ella,38,1970-01-01T00:00:00.007,par4, "
        + "id11,Phoebe,52,1970-01-01T00:00:00.008,par4, "
        + "id2,Stephen,34,1970-01-01T00:00:00.002,par1, "
        + "id3,Julian,54,1970-01-01T00:00:00.003,par2, "
        + "id4,Fabian,32,1970-01-01T00:00:00.004,par2, "
        + "id5,Sophia,18,1970-01-01T00:00:00.005,par3, "
        + "id6,Emma,20,1970-01-01T00:00:00.006,par3, "
        + "id7,Bob,44,1970-01-01T00:00:00.007,par4, "
        + "id8,Han,56,1970-01-01T00:00:00.008,par4, "
        + "id9,Jane,19,1970-01-01T00:00:00.006,par3]";
    assertThat(actual, is(expected));
  }

  @Test
  void testReadBaseAndLogFiles() throws Exception {
    beforeEach(HoodieTableType.MERGE_ON_READ);

    // write parquet first with compaction
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();

    List<RowData> result = readData(inputFormat);

    String actual = TestData.rowDataToString(result);
    String expected = TestData.rowDataToString(TestData.DATA_SET_INSERT);
    assertThat(actual, is(expected));

    // write another commit using logs and read again
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);

    // refresh the input format
    this.tableSource.reloadActiveTimeline();
    inputFormat = this.tableSource.getInputFormat();

    result = readData(inputFormat);

    actual = TestData.rowDataToString(result);
    expected = "[id1,Danny,24,1970-01-01T00:00:00.001,par1, "
        + "id10,Ella,38,1970-01-01T00:00:00.007,par4, "
        + "id11,Phoebe,52,1970-01-01T00:00:00.008,par4, "
        + "id2,Stephen,34,1970-01-01T00:00:00.002,par1, "
        + "id3,Julian,54,1970-01-01T00:00:00.003,par2, "
        + "id4,Fabian,32,1970-01-01T00:00:00.004,par2, "
        + "id5,Sophia,18,1970-01-01T00:00:00.005,par3, "
        + "id6,Emma,20,1970-01-01T00:00:00.006,par3, "
        + "id7,Bob,44,1970-01-01T00:00:00.007,par4, "
        + "id8,Han,56,1970-01-01T00:00:00.008,par4, "
        + "id9,Jane,19,1970-01-01T00:00:00.006,par3]";
    assertThat(actual, is(expected));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testReadWithPartitionPrune(HoodieTableType tableType) throws Exception {
    beforeEach(tableType);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    Map<String, String> prunedPartitions = new HashMap<>();
    prunedPartitions.put("partition", "par1");
    // prune to only be with partition 'par1'
    HoodieTableSource newSource = (HoodieTableSource) tableSource
        .applyPartitionPruning(Collections.singletonList(prunedPartitions));
    InputFormat<RowData, ?> inputFormat = newSource.getInputFormat();

    List<RowData> result = readData(inputFormat);

    String actual = TestData.rowDataToString(result);
    String expected = "[id1,Danny,23,1970-01-01T00:00:00.001,par1, id2,Stephen,33,1970-01-01T00:00:00.002,par1]";
    assertThat(actual, is(expected));
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  @SuppressWarnings("unchecked, rawtypes")
  private static List<RowData> readData(InputFormat inputFormat) throws IOException {
    InputSplit[] inputSplits = inputFormat.createInputSplits(1);

    List<RowData> result = new ArrayList<>();

    for (InputSplit inputSplit : inputSplits) {
      inputFormat.open(inputSplit);
      while (!inputFormat.reachedEnd()) {
        result.add(TestConfigurations.SERIALIZER.copy((RowData) inputFormat.nextRecord(null))); // no reuse
      }
      inputFormat.close();
    }
    return result;
  }
}
