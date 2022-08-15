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

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.table.HoodieTableSource;
import org.apache.hudi.table.format.cow.CopyOnWriteInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.util.AvroSchemaConverter;
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
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.utils.TestData.insertRow;
import static org.hamcrest.CoreMatchers.instanceOf;
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
    beforeEach(tableType, Collections.emptyMap());
  }

  void beforeEach(HoodieTableType tableType, Map<String, String> options) throws IOException {
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false); // close the async compaction
    options.forEach((key, value) -> conf.setString(key, value));

    StreamerUtil.initTableIfNotExists(conf);
    this.tableSource = getTableSource(conf);
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
    this.tableSource.reset();
    inputFormat = this.tableSource.getInputFormat();

    result = readData(inputFormat);

    actual = TestData.rowDataToString(result);
    expected = "["
        + "+I[id1, Danny, 24, 1970-01-01T00:00:00.001, par1], "
        + "+I[id2, Stephen, 34, 1970-01-01T00:00:00.002, par1], "
        + "+I[id3, Julian, 54, 1970-01-01T00:00:00.003, par2], "
        + "+I[id4, Fabian, 32, 1970-01-01T00:00:00.004, par2], "
        + "+I[id5, Sophia, 18, 1970-01-01T00:00:00.005, par3], "
        + "+I[id6, Emma, 20, 1970-01-01T00:00:00.006, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:00.007, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:00.008, par4], "
        + "+I[id9, Jane, 19, 1970-01-01T00:00:00.006, par3], "
        + "+I[id10, Ella, 38, 1970-01-01T00:00:00.007, par4], "
        + "+I[id11, Phoebe, 52, 1970-01-01T00:00:00.008, par4]]";
    assertThat(actual, is(expected));
  }

  @Test
  void testReadBaseAndLogFiles() throws Exception {
    beforeEach(HoodieTableType.MERGE_ON_READ);

    // write base first with compaction
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();

    List<RowData> result = readData(inputFormat);

    String actual = TestData.rowDataToString(result);
    String expected = TestData.rowDataToString(TestData.DATA_SET_INSERT);
    assertThat(actual, is(expected));

    // write another commit using logs and read again
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);

    // write another commit using logs with separate partition
    // so the file group has only logs
    TestData.writeData(TestData.DATA_SET_INSERT_SEPARATE_PARTITION, conf);

    // refresh the input format
    this.tableSource.reset();
    inputFormat = this.tableSource.getInputFormat();

    result = readData(inputFormat);

    actual = TestData.rowDataToString(result);
    expected = "["
        + "+I[id1, Danny, 24, 1970-01-01T00:00:00.001, par1], "
        + "+I[id2, Stephen, 34, 1970-01-01T00:00:00.002, par1], "
        + "+I[id3, Julian, 54, 1970-01-01T00:00:00.003, par2], "
        + "+I[id4, Fabian, 32, 1970-01-01T00:00:00.004, par2], "
        + "+I[id5, Sophia, 18, 1970-01-01T00:00:00.005, par3], "
        + "+I[id6, Emma, 20, 1970-01-01T00:00:00.006, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:00.007, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:00.008, par4], "
        + "+I[id9, Jane, 19, 1970-01-01T00:00:00.006, par3], "
        + "+I[id10, Ella, 38, 1970-01-01T00:00:00.007, par4], "
        + "+I[id11, Phoebe, 52, 1970-01-01T00:00:00.008, par4], "
        + "+I[id12, Monica, 27, 1970-01-01T00:00:00.009, par5], "
        + "+I[id13, Phoebe, 31, 1970-01-01T00:00:00.010, par5], "
        + "+I[id14, Rachel, 52, 1970-01-01T00:00:00.011, par6], "
        + "+I[id15, Ross, 29, 1970-01-01T00:00:00.012, par6]]";
    assertThat(actual, is(expected));
  }

  @Test
  void testReadBaseAndLogFilesWithDeletes() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.CHANGELOG_ENABLED.key(), "true");
    beforeEach(HoodieTableType.MERGE_ON_READ, options);

    // write base first with compaction.
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    // write another commit using logs and read again.
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    TestData.writeData(TestData.DATA_SET_UPDATE_DELETE, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));

    // when isEmitDelete is false.
    List<RowData> result1 = readData(inputFormat);

    final String actual1 = TestData.rowDataToString(result1);
    final String expected1 = "["
        + "+I[id1, Danny, 24, 1970-01-01T00:00:00.001, par1], "
        + "+I[id2, Stephen, 34, 1970-01-01T00:00:00.002, par1], "
        + "+I[id4, Fabian, 31, 1970-01-01T00:00:00.004, par2], "
        + "+I[id6, Emma, 20, 1970-01-01T00:00:00.006, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:00.007, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:00.008, par4]]";
    assertThat(actual1, is(expected1));

    // refresh the input format and set isEmitDelete to true.
    this.tableSource.reset();
    inputFormat = this.tableSource.getInputFormat();
    ((MergeOnReadInputFormat) inputFormat).isEmitDelete(true);

    List<RowData> result2 = readData(inputFormat);

    final String actual2 = TestData.rowDataToString(result2);
    final String expected2 = "["
        + "+I[id1, Danny, 24, 1970-01-01T00:00:00.001, par1], "
        + "+I[id2, Stephen, 34, 1970-01-01T00:00:00.002, par1], "
        + "-D[id3, Julian, 53, 1970-01-01T00:00:00.003, par2], "
        + "+I[id4, Fabian, 31, 1970-01-01T00:00:00.004, par2], "
        + "-D[id5, Sophia, 18, 1970-01-01T00:00:00.005, par3], "
        + "+I[id6, Emma, 20, 1970-01-01T00:00:00.006, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:00.007, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:00.008, par4], "
        + "-D[id9, Jane, 19, 1970-01-01T00:00:00.006, par3]]";
    assertThat(actual2, is(expected2));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testReadBaseAndLogFilesWithDisorderUpdateDelete(boolean compact) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.CHANGELOG_ENABLED.key(), "true");
    beforeEach(HoodieTableType.MERGE_ON_READ, options);

    // write base first with compaction.
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    TestData.writeData(TestData.DATA_SET_SINGLE_INSERT, conf);

    // write another commit using logs and read again.
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, compact);
    TestData.writeData(TestData.DATA_SET_DISORDER_UPDATE_DELETE, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));

    // when isEmitDelete is false.
    List<RowData> result1 = readData(inputFormat);

    final String rowKind = compact ? "I" : "U";
    final String expected = "[+" + rowKind + "[id1, Danny, 22, 1970-01-01T00:00:00.004, par1]]";

    final String actual1 = TestData.rowDataToString(result1);
    assertThat(actual1, is(expected));

    // refresh the input format and set isEmitDelete to true.
    this.tableSource.reset();
    inputFormat = this.tableSource.getInputFormat();
    ((MergeOnReadInputFormat) inputFormat).isEmitDelete(true);

    List<RowData> result2 = readData(inputFormat);

    final String actual2 = TestData.rowDataToString(result2);
    assertThat(actual2, is(expected));
  }

  @Test
  void testReadWithDeletesMOR() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.CHANGELOG_ENABLED.key(), "true");
    beforeEach(HoodieTableType.MERGE_ON_READ, options);

    // write another commit to read again
    TestData.writeData(TestData.DATA_SET_UPDATE_DELETE, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));
    ((MergeOnReadInputFormat) inputFormat).isEmitDelete(true);

    List<RowData> result = readData(inputFormat);

    final String actual = TestData.rowDataToString(result);
    final String expected = "["
        + "+I[id1, Danny, 24, 1970-01-01T00:00:00.001, par1], "
        + "+I[id2, Stephen, 34, 1970-01-01T00:00:00.002, par1], "
        + "-D[id3, Julian, 53, 1970-01-01T00:00:00.003, par2], "
        + "-D[id5, Sophia, 18, 1970-01-01T00:00:00.005, par3], "
        + "-D[id9, Jane, 19, 1970-01-01T00:00:00.006, par3]]";
    assertThat(actual, is(expected));
  }

  @Test
  void testReadWithDeletesMORChangeLogDisabled() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.CHANGELOG_ENABLED.key(), "false");
    options.put(FlinkOptions.PARTITION_PATH_FIELD.key(), "partition,name");
    options.put(FlinkOptions.KEYGEN_TYPE.key(), KeyGeneratorType.COMPLEX.name());
    beforeEach(HoodieTableType.MERGE_ON_READ, options);

    // write another commit to read again
    TestData.writeData(TestData.DATA_SET_UPDATE_DELETE, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));
    ((MergeOnReadInputFormat) inputFormat).isEmitDelete(true);

    List<RowData> result = readData(inputFormat);

    final String actual = TestData.rowDataToString(result);
    final String expected = "["
        + "+I[id1, Danny, 24, 1970-01-01T00:00:00.001, par1], "
        + "+I[id2, Stephen, 34, 1970-01-01T00:00:00.002, par1], "
        + "-D[id3, Julian, null, 1970-01-01T00:00:00.003, par2], "
        + "-D[id5, Sophia, null, 1970-01-01T00:00:00.005, par3], "
        + "-D[id9, Jane, null, 1970-01-01T00:00:00.006, par3]]";
    assertThat(actual, is(expected));
  }

  @Test
  void testReadWithDeletesCOW() throws Exception {
    beforeEach(HoodieTableType.COPY_ON_WRITE);

    // write another commit to read again
    TestData.writeData(TestData.DATA_SET_UPDATE_DELETE, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();
    assertThat(inputFormat, instanceOf(CopyOnWriteInputFormat.class));

    List<RowData> result = readData(inputFormat);

    final String actual = TestData.rowDataToString(result);
    final String expected = "["
        + "+I[id1, Danny, 24, 1970-01-01T00:00:00.001, par1], "
        + "+I[id2, Stephen, 34, 1970-01-01T00:00:00.002, par1]]";
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
    tableSource.applyPartitions(Collections.singletonList(prunedPartitions));
    InputFormat<RowData, ?> inputFormat = tableSource.getInputFormat();

    List<RowData> result = readData(inputFormat);

    String actual = TestData.rowDataToString(result);
    String expected = "["
        + "+I[id1, Danny, 23, 1970-01-01T00:00:00.001, par1], "
        + "+I[id2, Stephen, 33, 1970-01-01T00:00:00.002, par1]]";
    assertThat(actual, is(expected));
  }

  @Test
  void testReadChangesMergedMOR() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.CHANGELOG_ENABLED.key(), "true");
    beforeEach(HoodieTableType.MERGE_ON_READ, options);

    // write another commit to read again
    TestData.writeData(TestData.DATA_SET_INSERT_UPDATE_DELETE, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> result1 = readData(inputFormat);

    final String actual1 = TestData.rowDataToString(result1);
    // the data set is merged when the data source is bounded.
    final String expected1 = "[]";
    assertThat(actual1, is(expected1));

    // refresh the input format and set isEmitDelete to true.
    this.tableSource.reset();
    inputFormat = this.tableSource.getInputFormat();
    ((MergeOnReadInputFormat) inputFormat).isEmitDelete(true);

    List<RowData> result2 = readData(inputFormat);

    final String actual2 = TestData.rowDataToString(result2);
    final String expected2 = "[-D[id1, Danny, 22, 1970-01-01T00:00:00.005, par1]]";
    assertThat(actual2, is(expected2));
  }

  @Test
  void testReadChangesUnMergedMOR() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.CHANGELOG_ENABLED.key(), "true");
    options.put(FlinkOptions.READ_AS_STREAMING.key(), "true");
    beforeEach(HoodieTableType.MERGE_ON_READ, options);

    // write another commit to read again
    TestData.writeData(TestData.DATA_SET_INSERT_UPDATE_DELETE, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> result = readData(inputFormat);

    final String actual = TestData.rowDataToString(result);
    // the data set is merged when the data source is bounded.
    final String expected = "["
        + "+I[id1, Danny, 19, 1970-01-01T00:00:00.001, par1], "
        + "-U[id1, Danny, 19, 1970-01-01T00:00:00.001, par1], "
        + "+U[id1, Danny, 20, 1970-01-01T00:00:00.002, par1], "
        + "-U[id1, Danny, 20, 1970-01-01T00:00:00.002, par1], "
        + "+U[id1, Danny, 21, 1970-01-01T00:00:00.003, par1], "
        + "-U[id1, Danny, 21, 1970-01-01T00:00:00.003, par1], "
        + "+U[id1, Danny, 22, 1970-01-01T00:00:00.004, par1], "
        + "-D[id1, Danny, 22, 1970-01-01T00:00:00.005, par1]]";
    assertThat(actual, is(expected));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testReadIncrementally(HoodieTableType tableType) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.QUERY_TYPE.key(), FlinkOptions.QUERY_TYPE_INCREMENTAL);
    beforeEach(tableType, options);

    // write another commit to read again
    for (int i = 0; i < 6; i += 2) {
      List<RowData> dataset = TestData.dataSetInsert(i + 1, i + 2);
      TestData.writeData(dataset, conf);
    }

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(tempFile.getAbsolutePath(), HadoopConfigurations.getHadoopConf(conf));
    List<String> commits = metaClient.getCommitsTimeline().filterCompletedInstants().getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());

    assertThat(commits.size(), is(3));

    // only the start commit
    conf.setString(FlinkOptions.READ_START_COMMIT, commits.get(1));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat1 = this.tableSource.getInputFormat();
    assertThat(inputFormat1, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual1 = readData(inputFormat1);
    final List<RowData> expected1 = TestData.dataSetInsert(3, 4, 5, 6);
    TestData.assertRowDataEquals(actual1, expected1);

    // only the start commit: earliest
    conf.setString(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat2 = this.tableSource.getInputFormat();
    assertThat(inputFormat2, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual2 = readData(inputFormat2);
    final List<RowData> expected2 = TestData.dataSetInsert(1, 2, 3, 4, 5, 6);
    TestData.assertRowDataEquals(actual2, expected2);

    // start and end commit: [start commit, end commit]
    conf.setString(FlinkOptions.READ_START_COMMIT, commits.get(0));
    conf.setString(FlinkOptions.READ_END_COMMIT, commits.get(1));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat3 = this.tableSource.getInputFormat();
    assertThat(inputFormat3, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual3 = readData(inputFormat3);
    final List<RowData> expected3 = TestData.dataSetInsert(1, 2, 3, 4);
    TestData.assertRowDataEquals(actual3, expected3);

    // only the end commit: point in time query
    conf.removeConfig(FlinkOptions.READ_START_COMMIT);
    conf.setString(FlinkOptions.READ_END_COMMIT, commits.get(1));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat4 = this.tableSource.getInputFormat();
    assertThat(inputFormat4, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual4 = readData(inputFormat4);
    final List<RowData> expected4 = TestData.dataSetInsert(3, 4);
    TestData.assertRowDataEquals(actual4, expected4);

    // start and end commit: start commit out of range
    conf.setString(FlinkOptions.READ_START_COMMIT, "000");
    conf.setString(FlinkOptions.READ_END_COMMIT, commits.get(1));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat5 = this.tableSource.getInputFormat();
    assertThat(inputFormat4, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual5 = readData(inputFormat5);
    final List<RowData> expected5 = TestData.dataSetInsert(1, 2, 3, 4);
    TestData.assertRowDataEquals(actual5, expected5);

    // start and end commit: both are out of range
    conf.setString(FlinkOptions.READ_START_COMMIT, "001");
    conf.setString(FlinkOptions.READ_END_COMMIT, "002");
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat6 = this.tableSource.getInputFormat();
    assertThat(inputFormat6, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual6 = readData(inputFormat6);
    TestData.assertRowDataEquals(actual6, Collections.emptyList());
  }

  @Test
  void testReadIncrementallyUsingLogFiles() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.QUERY_TYPE.key(), FlinkOptions.QUERY_TYPE_INCREMENTAL);
    options.put(FlinkOptions.CHANGELOG_ENABLED.key(), "true");
    beforeEach(HoodieTableType.MERGE_ON_READ, options);

    // write base file first with compaction.
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    conf.setBoolean(FlinkOptions.READ_BATCH_INCREMENTAL_CHANGELOG_ENABLED, true);
    TestData.writeData(TestData.DATA_SET_INSERT_UPDATE_DELETE, conf);
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(tempFile.getAbsolutePath(), HadoopConfigurations.getHadoopConf(conf));
    List<String> commits = metaClient.getCommitsTimeline().filterCompletedInstants().getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    assertThat(commits.size(), is(2));

    conf.setString(FlinkOptions.READ_START_COMMIT, commits.get(0));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));
    ((MergeOnReadInputFormat) inputFormat).isEmitDelete(true);
    final String baseResult = TestData.rowDataToString(readData(inputFormat));
    String expected = "[-D[id1, Danny, 22, 1970-01-01T00:00:00.005, par1]]";
    assertThat(baseResult, is(expected));

    // write another commit using logs and read again.
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    TestData.writeData(Collections.singletonList(
        insertRow(StringData.fromString("id2"), StringData.fromString("Danny"), 24,
                  TimestampData.fromEpochMillis(3), StringData.fromString("par1"))), conf);
    TestData.writeData(Collections.singletonList(
        insertRow(StringData.fromString("id3"), StringData.fromString("Danny"), 24,
                  TimestampData.fromEpochMillis(4), StringData.fromString("par1"))), conf);
    this.tableSource.reset();
    commits = tableSource.getMetaClient().getCommitsTimeline().filterCompletedInstants().getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    assertThat(commits.size(), is(4));

    conf.setString(FlinkOptions.READ_START_COMMIT, commits.get(0));
    conf.setString(FlinkOptions.READ_END_COMMIT, commits.get(2));
    inputFormat = this.tableSource.getInputFormat();
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));
    ((MergeOnReadInputFormat) inputFormat).isEmitDelete(true);
    final String baseMergeLogFileResult = TestData.rowDataToString(readData(inputFormat));
    expected = "[-D[id1, Danny, 22, 1970-01-01T00:00:00.005, par1], +I[id2, Danny, 24, 1970-01-01T00:00:00.003, par1]]";
    assertThat(baseMergeLogFileResult, is(expected));
  }

  @Test
  void testReadIncrementallyUsingLogFilesChangeLogDisabled() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.QUERY_TYPE.key(), FlinkOptions.QUERY_TYPE_INCREMENTAL);
    options.put(FlinkOptions.CHANGELOG_ENABLED.key(), "false");
    beforeEach(HoodieTableType.MERGE_ON_READ, options);

    // write base file first with compaction.
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    conf.setBoolean(FlinkOptions.READ_BATCH_INCREMENTAL_CHANGELOG_ENABLED, true);
    TestData.writeData(TestData.DATA_SET_INSERT_UPDATE_DELETE, conf);
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(tempFile.getAbsolutePath(), HadoopConfigurations.getHadoopConf(conf));
    List<String> commits = metaClient.getCommitsTimeline().filterCompletedInstants().getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    assertThat(commits.size(), is(2));

    conf.setString(FlinkOptions.READ_START_COMMIT, commits.get(0));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));
    ((MergeOnReadInputFormat) inputFormat).isEmitDelete(true);
    final String baseResult = TestData.rowDataToString(readData(inputFormat));
    String expected = "[-D[id1, null, null, 1970-01-01T00:00:00.005, par1]]";
    assertThat(baseResult, is(expected));

    // write another commit using logs and read again.
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    TestData.writeData(Collections.singletonList(
        insertRow(StringData.fromString("id2"), StringData.fromString("Danny"), 24,
                  TimestampData.fromEpochMillis(3), StringData.fromString("par1"))), conf);
    TestData.writeData(Collections.singletonList(
        insertRow(StringData.fromString("id3"), StringData.fromString("Danny"), 24,
                  TimestampData.fromEpochMillis(4), StringData.fromString("par1"))), conf);
    this.tableSource.reset();
    commits = tableSource.getMetaClient().getCommitsTimeline().filterCompletedInstants().getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    assertThat(commits.size(), is(4));

    conf.setString(FlinkOptions.READ_START_COMMIT, commits.get(0));
    conf.setString(FlinkOptions.READ_END_COMMIT, commits.get(2));
    inputFormat = this.tableSource.getInputFormat();
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));
    ((MergeOnReadInputFormat) inputFormat).isEmitDelete(true);
    final String baseMergeLogFileResult = TestData.rowDataToString(readData(inputFormat));
    expected = "[-D[id1, null, null, 1970-01-01T00:00:00.005, par1], +I[id2, Danny, 24, 1970-01-01T00:00:00.003, par1]]";
    assertThat(baseMergeLogFileResult, is(expected));
  }

  @Test
  void testMergeOnReadDisorderUpdateAfterCompaction() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PAYLOAD_CLASS_NAME.key(), EventTimeAvroPayload.class.getName());
    beforeEach(HoodieTableType.MERGE_ON_READ, options);

    // write base file first with compaction.
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    TestData.writeData(TestData.DATA_SET_DISORDER_INSERT, conf);
    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();
    final String baseResult = TestData.rowDataToString(readData(inputFormat));
    String expected = "[+I[id1, Danny, 22, 1970-01-01T00:00:00.004, par1]]";
    assertThat(baseResult, is(expected));

    // write another commit using logs and read again.
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    TestData.writeData(TestData.DATA_SET_SINGLE_INSERT, conf);
    this.tableSource.reset();
    inputFormat = this.tableSource.getInputFormat();
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));
    final String baseMergeLogFileResult = TestData.rowDataToString(readData(inputFormat));
    assertThat(baseMergeLogFileResult, is(expected));
  }

  @Test
  void testReadArchivedCommitsIncrementally() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.QUERY_TYPE.key(), FlinkOptions.QUERY_TYPE_INCREMENTAL);
    options.put(FlinkOptions.ARCHIVE_MIN_COMMITS.key(), "3");
    options.put(FlinkOptions.ARCHIVE_MAX_COMMITS.key(), "4");
    options.put(FlinkOptions.CLEAN_RETAIN_COMMITS.key(), "2");
    // disable the metadata table to make the archiving behavior deterministic
    options.put(FlinkOptions.METADATA_ENABLED.key(), "false");
    options.put("hoodie.commits.archival.batch", "1");
    beforeEach(HoodieTableType.COPY_ON_WRITE, options);

    // write 10 batches of data set
    for (int i = 0; i < 20; i += 2) {
      List<RowData> dataset = TestData.dataSetInsert(i + 1, i + 2);
      TestData.writeData(dataset, conf);
    }
    // cleaning
    HoodieFlinkWriteClient<?> writeClient = new HoodieFlinkWriteClient<>(
        HoodieFlinkEngineContext.DEFAULT, StreamerUtil.getHoodieClientConfig(conf));
    writeClient.clean();

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(tempFile.getAbsolutePath(), HadoopConfigurations.getHadoopConf(conf));
    List<String> commits = metaClient.getCommitsTimeline().filterCompletedInstants().getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());

    assertThat(commits.size(), is(4));

    List<String> archivedCommits = metaClient.getArchivedTimeline().getCommitsTimeline().filterCompletedInstants()
        .getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());

    assertThat(archivedCommits.size(), is(6));

    // start and end commit: both are archived and cleaned
    conf.setString(FlinkOptions.READ_START_COMMIT, archivedCommits.get(0));
    conf.setString(FlinkOptions.READ_END_COMMIT, archivedCommits.get(1));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat1 = this.tableSource.getInputFormat();
    assertThat(inputFormat1, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual1 = readData(inputFormat1);
    final List<RowData> expected1 = TestData.dataSetInsert(1, 2, 3, 4);
    TestData.assertRowDataEquals(actual1, expected1);

    // only the start commit: is archived and cleaned
    conf.setString(FlinkOptions.READ_START_COMMIT, archivedCommits.get(1));
    conf.removeConfig(FlinkOptions.READ_END_COMMIT);
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat2 = this.tableSource.getInputFormat();
    assertThat(inputFormat2, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual2 = readData(inputFormat2);
    final List<RowData> expected2 = TestData.dataSetInsert(3, 4, 5, 6, 7, 8, 9, 10,
        11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
    TestData.assertRowDataEquals(actual2, expected2);

    // only the end commit: is archived and cleaned
    conf.removeConfig(FlinkOptions.READ_START_COMMIT);
    conf.setString(FlinkOptions.READ_END_COMMIT, archivedCommits.get(1));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat3 = this.tableSource.getInputFormat();
    assertThat(inputFormat3, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual3 = readData(inputFormat3);
    final List<RowData> expected3 = TestData.dataSetInsert(3, 4);
    TestData.assertRowDataEquals(actual3, expected3);

    // start and end commit: start is archived and cleaned, end is active
    conf.setString(FlinkOptions.READ_START_COMMIT, archivedCommits.get(1));
    conf.setString(FlinkOptions.READ_END_COMMIT, commits.get(0));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat4 = this.tableSource.getInputFormat();
    assertThat(inputFormat4, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual4 = readData(inputFormat4);
    final List<RowData> expected4 = TestData.dataSetInsert(3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
    TestData.assertRowDataEquals(actual4, expected4);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testReadWithWiderSchema(HoodieTableType tableType) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.SOURCE_AVRO_SCHEMA.key(),
        AvroSchemaConverter.convertToSchema(TestConfigurations.ROW_TYPE_WIDER).toString());
    beforeEach(tableType, options);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();
    List<RowData> result = readData(inputFormat);
    TestData.assertRowDataEquals(result, TestData.DATA_SET_INSERT);
  }

  /**
   * Test reading file groups with compaction plan scheduled and delta logs.
   * File-slice after pending compaction-requested instant-time should also be considered valid.
   */
  @Test
  void testReadMORWithCompactionPlanScheduled() throws Exception {
    Map<String, String> options = new HashMap<>();
    // compact for each commit
    options.put(FlinkOptions.COMPACTION_DELTA_COMMITS.key(), "1");
    options.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), "false");
    beforeEach(HoodieTableType.MERGE_ON_READ, options);

    // write three commits
    for (int i = 0; i < 6; i += 2) {
      List<RowData> dataset = TestData.dataSetInsert(i + 1, i + 2);
      TestData.writeData(dataset, conf);
    }

    InputFormat<RowData, ?> inputFormat1 = this.tableSource.getInputFormat();
    assertThat(inputFormat1, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual = readData(inputFormat1);
    final List<RowData> expected = TestData.dataSetInsert(1, 2, 3, 4, 5, 6);
    TestData.assertRowDataEquals(actual, expected);
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private HoodieTableSource getTableSource(Configuration conf) {
    return new HoodieTableSource(
        TestConfigurations.TABLE_SCHEMA,
        new Path(tempFile.getAbsolutePath()),
        Arrays.asList(conf.getString(FlinkOptions.PARTITION_PATH_FIELD).split(",")),
        "default",
        conf);
  }

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
