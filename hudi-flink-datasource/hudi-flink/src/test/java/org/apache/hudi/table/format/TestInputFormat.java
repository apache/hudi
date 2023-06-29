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

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.source.IncrementalInputSplits;
import org.apache.hudi.source.prune.PartitionPruners;
import org.apache.hudi.table.HoodieTableSource;
import org.apache.hudi.table.format.cdc.CdcInputFormat;
import org.apache.hudi.table.format.cow.CopyOnWriteInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestUtils;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
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
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  @EnumSource(value = HoodieCDCSupplementalLoggingMode.class)
  void testReadWithChangeLogCOW(HoodieCDCSupplementalLoggingMode mode) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.CDC_ENABLED.key(), "true");
    options.put(FlinkOptions.SUPPLEMENTAL_LOGGING_MODE.key(), mode.name());
    beforeEach(HoodieTableType.COPY_ON_WRITE, options);

    // write the insert data sets
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    // write another commit to read again
    TestData.writeData(TestData.DATA_SET_UPDATE_DELETE, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat(true);
    assertThat(inputFormat, instanceOf(CdcInputFormat.class));
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    IncrementalInputSplits incrementalInputSplits = IncrementalInputSplits.builder()
        .rowType(TestConfigurations.ROW_TYPE)
        .conf(conf)
        .path(FilePathUtils.toFlinkPath(metaClient.getBasePathV2()))
        .skipCompaction(false)
        .build();

    // default read the latest commit
    IncrementalInputSplits.Result splits = incrementalInputSplits.inputSplits(metaClient, null, null, true);

    List<RowData> result = readData(inputFormat, splits.getInputSplits().toArray(new MergeOnReadInputSplit[0]));

    final String actual = TestData.rowDataToString(result);
    final String expected = "["
        + "-U[id1, Danny, 23, 1970-01-01T00:00:00.001, par1], "
        + "+U[id1, Danny, 24, 1970-01-01T00:00:00.001, par1], "
        + "-U[id2, Stephen, 33, 1970-01-01T00:00:00.002, par1], "
        + "+U[id2, Stephen, 34, 1970-01-01T00:00:00.002, par1], "
        + "-D[id3, Julian, 53, 1970-01-01T00:00:00.003, par2], "
        + "-D[id5, Sophia, 18, 1970-01-01T00:00:00.005, par3]]";
    assertThat(actual, is(expected));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieCDCSupplementalLoggingMode.class)
  void testReadFromEarliestWithChangeLogCOW(HoodieCDCSupplementalLoggingMode mode) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.CDC_ENABLED.key(), "true");
    options.put(FlinkOptions.SUPPLEMENTAL_LOGGING_MODE.key(), mode.name());
    options.put(FlinkOptions.READ_START_COMMIT.key(), "earliest");
    beforeEach(HoodieTableType.COPY_ON_WRITE, options);

    // write the insert data sets
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    // write another commit to read again
    TestData.writeData(TestData.DATA_SET_UPDATE_DELETE, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat(true);
    assertThat(inputFormat, instanceOf(CdcInputFormat.class));
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    IncrementalInputSplits incrementalInputSplits = IncrementalInputSplits.builder()
        .rowType(TestConfigurations.ROW_TYPE)
        .conf(conf)
        .path(FilePathUtils.toFlinkPath(metaClient.getBasePathV2()))
        .partitionPruner(PartitionPruners.getInstance("par1", "par2", "par3", "par4"))
        .skipCompaction(false)
        .build();

    // default read the latest commit
    IncrementalInputSplits.Result splits = incrementalInputSplits.inputSplits(metaClient, null, null, true);

    List<RowData> result = readData(inputFormat, splits.getInputSplits().toArray(new MergeOnReadInputSplit[0]));

    final String actual = TestData.rowDataToString(result);
    final String expected = "["
        + "+I[id1, Danny, 24, 1970-01-01T00:00:00.001, par1], "
        + "+I[id2, Stephen, 34, 1970-01-01T00:00:00.002, par1], "
        + "+I[id4, Fabian, 31, 1970-01-01T00:00:00.004, par2], "
        + "+I[id6, Emma, 20, 1970-01-01T00:00:00.006, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:00.007, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:00.008, par4]]";
    assertThat(actual, is(expected));
  }

  @Test
  void testReadSkipCompaction() throws Exception {
    beforeEach(HoodieTableType.MERGE_ON_READ);

    org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(conf);

    // write base first with compaction
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat(true);
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    IncrementalInputSplits incrementalInputSplits = IncrementalInputSplits.builder()
        .rowType(TestConfigurations.ROW_TYPE)
        .conf(conf)
        .path(FilePathUtils.toFlinkPath(metaClient.getBasePathV2()))
        .partitionPruner(PartitionPruners.getInstance("par1", "par2", "par3", "par4"))
        .skipCompaction(true)
        .build();

    // default read the latest commit
    // the compaction base files are skipped
    IncrementalInputSplits.Result splits1 = incrementalInputSplits.inputSplits(metaClient, null, null, false);
    assertFalse(splits1.isEmpty());
    List<RowData> result1 = readData(inputFormat, splits1.getInputSplits().toArray(new MergeOnReadInputSplit[0]));

    String actual1 = TestData.rowDataToString(result1);
    String expected1 = TestData.rowDataToString(TestData.DATA_SET_INSERT);
    assertThat(actual1, is(expected1));

    // write another commit using logs and read again
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);

    // read from the compaction commit
    String secondCommit = TestUtils.getNthCompleteInstant(metaClient.getBasePath(), 0, HoodieTimeline.COMMIT_ACTION);
    conf.setString(FlinkOptions.READ_START_COMMIT, secondCommit);

    IncrementalInputSplits.Result splits2 = incrementalInputSplits.inputSplits(metaClient, null, null, false);
    assertFalse(splits2.isEmpty());
    List<RowData> result2 = readData(inputFormat, splits2.getInputSplits().toArray(new MergeOnReadInputSplit[0]));
    String actual2 = TestData.rowDataToString(result2);
    String expected2 = TestData.rowDataToString(TestData.DATA_SET_UPDATE_INSERT);
    assertThat(actual2, is(expected2));

    // write another commit using logs with separate partition
    // so the file group has only logs
    TestData.writeData(TestData.DATA_SET_INSERT_SEPARATE_PARTITION, conf);

    // refresh the input format
    this.tableSource.reset();
    inputFormat = this.tableSource.getInputFormat(true);

    // filter out the last commit by partition pruning
    IncrementalInputSplits.Result splits3 = incrementalInputSplits.inputSplits(metaClient, null, null, false);
    assertFalse(splits3.isEmpty());
    List<RowData> result3 = readData(inputFormat, splits3.getInputSplits().toArray(new MergeOnReadInputSplit[0]));
    String actual3 = TestData.rowDataToString(result3);
    String expected3 = TestData.rowDataToString(TestData.DATA_SET_UPDATE_INSERT);
    assertThat(actual3, is(expected3));
  }

  @Test
  void testReadSkipClustering() throws Exception {
    beforeEach(HoodieTableType.COPY_ON_WRITE);

    // write base first with clustering
    conf.setString(FlinkOptions.OPERATION, "insert");
    conf.setBoolean(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, true);
    conf.setBoolean(FlinkOptions.CLUSTERING_ASYNC_ENABLED, true);
    conf.setInteger(FlinkOptions.CLUSTERING_DELTA_COMMITS, 1);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat(true);
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    IncrementalInputSplits incrementalInputSplits = IncrementalInputSplits.builder()
        .rowType(TestConfigurations.ROW_TYPE)
        .conf(conf)
        .path(FilePathUtils.toFlinkPath(metaClient.getBasePathV2()))
        .partitionPruner(PartitionPruners.getInstance("par1", "par2", "par3", "par4"))
        .skipClustering(true)
        .build();

    // default read the latest commit
    // the clustering files are skipped
    IncrementalInputSplits.Result splits1 = incrementalInputSplits.inputSplits(metaClient, null, null, false);
    assertFalse(splits1.isEmpty());
    List<RowData> result1 = readData(inputFormat, splits1.getInputSplits().toArray(new MergeOnReadInputSplit[0]));

    String actual1 = TestData.rowDataToString(result1);
    String expected1 = TestData.rowDataToString(TestData.DATA_SET_INSERT);
    assertThat(actual1, is(expected1));

    // write another commit and read again
    conf.setBoolean(FlinkOptions.CLUSTERING_ASYNC_ENABLED, false);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);

    // read from the clustering commit
    String secondCommit = TestUtils.getNthCompleteInstant(metaClient.getBasePath(), 0, HoodieTimeline.REPLACE_COMMIT_ACTION);
    conf.setString(FlinkOptions.READ_START_COMMIT, secondCommit);

    IncrementalInputSplits.Result splits2 = incrementalInputSplits.inputSplits(metaClient, null, null, false);
    assertFalse(splits2.isEmpty());
    List<RowData> result2 = readData(inputFormat, splits2.getInputSplits().toArray(new MergeOnReadInputSplit[0]));
    String actual2 = TestData.rowDataToString(result2);
    String expected2 = TestData.rowDataToString(TestData.DATA_SET_UPDATE_INSERT);
    assertThat(actual2, is(expected2));

    // write another commit with separate partition
    // so the file group has only base files
    conf.setBoolean(FlinkOptions.CLUSTERING_ASYNC_ENABLED, true);
    TestData.writeData(TestData.DATA_SET_INSERT_SEPARATE_PARTITION, conf);

    // refresh the input format
    this.tableSource.reset();
    inputFormat = this.tableSource.getInputFormat(true);

    // filter out the last commit by partition pruning
    IncrementalInputSplits.Result splits3 = incrementalInputSplits.inputSplits(metaClient, null, null, false);
    assertFalse(splits3.isEmpty());
    List<RowData> result3 = readData(inputFormat, splits3.getInputSplits().toArray(new MergeOnReadInputSplit[0]));
    String actual3 = TestData.rowDataToString(result3);
    String expected3 = TestData.rowDataToString(TestData.DATA_SET_UPDATE_INSERT);
    assertThat(actual3, is(expected3));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testReadHollowInstants(HoodieTableType tableType) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put("hoodie.parquet.small.file.limit", "0"); // invalidate the small file strategy
    beforeEach(tableType, options);

    // write 4 commits
    for (int i = 0; i < 8; i += 2) {
      List<RowData> dataset = TestData.dataSetInsert(i + 1, i + 2);
      TestData.writeData(dataset, conf);
    }

    // we got 4 commits on the timeline: c1, c2, c3, c4
    // re-create the metadata file for c2 and c3 so that they have greater completion time than c4.
    // the completion time sequence become: c1, c4, c2, c3,
    // we will test with the same consumption sequence.
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(tempFile.getAbsolutePath(), HadoopConfigurations.getHadoopConf(conf));
    List<HoodieInstant> oriInstants = metaClient.getCommitsTimeline().filterCompletedInstants().getInstants();
    assertThat(oriInstants.size(), is(4));
    List<HoodieCommitMetadata> metadataList = new ArrayList<>();
    // timeline: c1, c2.inflight, c3.inflight, c4
    for (int i = 1; i <= 2; i++) {
      HoodieInstant instant = oriInstants.get(i);
      metadataList.add(TestUtils.deleteInstantFile(metaClient, instant));
    }

    List<HoodieInstant> instants = metaClient.reloadActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstants();
    assertThat(instants.size(), is(2));

    String c4 = instants.get(1).getTimestamp();

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat(true);
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));

    IncrementalInputSplits incrementalInputSplits = IncrementalInputSplits.builder()
        .rowType(TestConfigurations.ROW_TYPE)
        .conf(conf)
        .path(FilePathUtils.toFlinkPath(metaClient.getBasePathV2()))
        .build();

    // timeline: c1, c2.inflight, c3.inflight, c4
    // default read the latest commit
    IncrementalInputSplits.Result splits1 = incrementalInputSplits.inputSplits(metaClient, null, null, false);
    assertFalse(splits1.isEmpty());
    List<RowData> result1 = readData(inputFormat, splits1.getInputSplits().toArray(new MergeOnReadInputSplit[0]));
    TestData.assertRowDataEquals(result1, TestData.dataSetInsert(7, 8));

    // timeline: c1, c2.inflight, c3.inflight, c4
    // -> c1
    conf.setString(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);
    IncrementalInputSplits.Result splits2 = incrementalInputSplits.inputSplits(metaClient, null, null, false);
    assertFalse(splits2.isEmpty());
    List<RowData> result2 = readData(inputFormat, splits2.getInputSplits().toArray(new MergeOnReadInputSplit[0]));
    TestData.assertRowDataEquals(result2, TestData.dataSetInsert(1, 2, 7, 8));

    // timeline: c1, c2, c3.inflight, c4
    // c4 -> c2
    TestUtils.saveInstantAsComplete(metaClient, oriInstants.get(1), metadataList.get(0)); // complete c2
    assertThat(splits2.getEndInstant(), is(c4));
    IncrementalInputSplits.Result splits3 = incrementalInputSplits.inputSplits(metaClient, splits2.getEndInstant(), splits2.getOffset(), false);
    assertFalse(splits3.isEmpty());
    List<RowData> result3 = readData(inputFormat, splits3.getInputSplits().toArray(new MergeOnReadInputSplit[0]));
    TestData.assertRowDataEquals(result3, TestData.dataSetInsert(3, 4));

    // test c2 and c4, c2 completion time > c1, so it is not a hollow instant
    IncrementalInputSplits.Result splits4 = incrementalInputSplits.inputSplits(metaClient, oriInstants.get(0).getTimestamp(), oriInstants.get(0).getStateTransitionTime(), false);
    assertFalse(splits4.isEmpty());
    List<RowData> result4 = readData(inputFormat, splits4.getInputSplits().toArray(new MergeOnReadInputSplit[0]));
    TestData.assertRowDataEquals(result4, TestData.dataSetInsert(3, 4, 7, 8));

    // timeline: c1, c2, c3, c4
    // c4 -> c3
    TestUtils.saveInstantAsComplete(metaClient, oriInstants.get(2), metadataList.get(1)); // complete c3
    assertThat(splits3.getEndInstant(), is(c4));
    IncrementalInputSplits.Result splits5 = incrementalInputSplits.inputSplits(metaClient, splits3.getEndInstant(), splits3.getOffset(), false);
    assertFalse(splits5.isEmpty());
    List<RowData> result5 = readData(inputFormat, splits5.getInputSplits().toArray(new MergeOnReadInputSplit[0]));
    TestData.assertRowDataEquals(result5, TestData.dataSetInsert(5, 6));

    // c4 ->
    assertThat(splits5.getEndInstant(), is(c4));
    IncrementalInputSplits.Result splits6 = incrementalInputSplits.inputSplits(metaClient, splits5.getEndInstant(), splits5.getOffset(), false);
    assertTrue(splits6.isEmpty());

    // test c2 and c4, c2 is recognized as a hollow instant
    // the (version_number, completion_time) pair is not consistent, just for test purpose
    IncrementalInputSplits.Result splits7 = incrementalInputSplits.inputSplits(metaClient, oriInstants.get(2).getTimestamp(), oriInstants.get(3).getStateTransitionTime(), false);
    assertFalse(splits7.isEmpty());
    List<RowData> result7 = readData(inputFormat, splits7.getInputSplits().toArray(new MergeOnReadInputSplit[0]));
    TestData.assertRowDataEquals(result7, TestData.dataSetInsert(3, 4, 7, 8));
  }

  @Test
  void testReadBaseFilesWithStartCommit() throws Exception {
    beforeEach(HoodieTableType.COPY_ON_WRITE);

    // write base files
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat(true);
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    IncrementalInputSplits incrementalInputSplits = IncrementalInputSplits.builder()
        .rowType(TestConfigurations.ROW_TYPE)
        .conf(conf)
        .path(FilePathUtils.toFlinkPath(metaClient.getBasePathV2()))
        .partitionPruner(PartitionPruners.getInstance("par1", "par2", "par3", "par4"))
        .build();

    // default read the latest commit
    IncrementalInputSplits.Result splits1 = incrementalInputSplits.inputSplits(metaClient, null, null, false);
    assertFalse(splits1.isEmpty());
    List<RowData> result1 = readData(inputFormat, splits1.getInputSplits().toArray(new MergeOnReadInputSplit[0]));

    String actual1 = TestData.rowDataToString(result1);
    String expected1 = TestData.rowDataToString(TestData.DATA_SET_INSERT);
    assertThat(actual1, is(expected1));

    // write another commit and read again
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);

    // read from the latest commit
    String secondCommit = TestUtils.getNthCompleteInstant(metaClient.getBasePath(), 1, HoodieTimeline.COMMIT_ACTION);
    conf.setString(FlinkOptions.READ_START_COMMIT, secondCommit);

    IncrementalInputSplits.Result splits2 = incrementalInputSplits.inputSplits(metaClient, null, null, false);
    assertFalse(splits2.isEmpty());
    List<RowData> result2 = readData(inputFormat, splits2.getInputSplits().toArray(new MergeOnReadInputSplit[0]));
    String actual2 = TestData.rowDataToString(result2);
    String expected2 = TestData.rowDataToString(TestData.DATA_SET_UPDATE_INSERT);
    assertThat(actual2, is(expected2));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testReadWithPartitionPrune(HoodieTableType tableType) throws Exception {
    beforeEach(tableType);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    // prune to only be with partition 'par1'
    FieldReferenceExpression partRef = new FieldReferenceExpression("partition", DataTypes.STRING(), 4, 4);
    ValueLiteralExpression partLiteral = new ValueLiteralExpression("par1", DataTypes.STRING().notNull());
    CallExpression partFilter = new CallExpression(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(partRef, partLiteral),
        DataTypes.BOOLEAN());
    tableSource.applyFilters(Arrays.asList(partFilter));
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
    List<String> commits = metaClient.getCommitsTimeline().filterCompletedInstants().getInstantsAsStream()
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

    List<RowData> actual6 = readData(inputFormat6);
    TestData.assertRowDataEquals(actual6, Collections.emptyList());
  }

  @Test
  void testReadChangelogIncrementally() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.QUERY_TYPE.key(), FlinkOptions.QUERY_TYPE_INCREMENTAL);
    options.put(FlinkOptions.CDC_ENABLED.key(), "true");
    options.put(FlinkOptions.INDEX_BOOTSTRAP_ENABLED.key(), "true"); // for batch update
    beforeEach(HoodieTableType.COPY_ON_WRITE, options);

    // write 3 commits first
    // write the same dataset 3 times to generate changelog
    for (int i = 0; i < 3; i++) {
      List<RowData> dataset = TestData.dataSetInsert(1, 2);
      TestData.writeDataAsBatch(dataset, conf);
    }

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(tempFile.getAbsolutePath(), HadoopConfigurations.getHadoopConf(conf));
    List<String> commits = metaClient.getCommitsTimeline().filterCompletedInstants().getInstantsAsStream()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());

    assertThat(commits.size(), is(3));

    // only the start commit
    conf.setString(FlinkOptions.READ_START_COMMIT, commits.get(1));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat1 = this.tableSource.getInputFormat();
    assertThat(inputFormat1, instanceOf(CdcInputFormat.class));

    List<RowData> actual1 = readData(inputFormat1);
    final List<RowData> expected1 = TestData.dataSetUpsert(2, 1, 2, 1);
    TestData.assertRowDataEquals(actual1, expected1);

    // only the start commit: earliest
    conf.setString(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat2 = this.tableSource.getInputFormat();
    assertThat(inputFormat2, instanceOf(CdcInputFormat.class));

    List<RowData> actual2 = readData(inputFormat2);
    final List<RowData> expected2 = TestData.dataSetInsert(1, 2);
    TestData.assertRowDataEquals(actual2, expected2);

    // start and end commit: [start commit, end commit]
    conf.setString(FlinkOptions.READ_START_COMMIT, commits.get(0));
    conf.setString(FlinkOptions.READ_END_COMMIT, commits.get(1));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat3 = this.tableSource.getInputFormat();
    assertThat(inputFormat3, instanceOf(CdcInputFormat.class));

    List<RowData> actual3 = readData(inputFormat3);
    final List<RowData> expected3 = new ArrayList<>(TestData.dataSetInsert(1));
    expected3.addAll(TestData.dataSetUpsert(1));
    expected3.addAll(TestData.dataSetInsert(2));
    expected3.addAll(TestData.dataSetUpsert(2));
    TestData.assertRowDataEquals(actual3, expected3);

    // only the end commit: point in time query
    conf.removeConfig(FlinkOptions.READ_START_COMMIT);
    conf.setString(FlinkOptions.READ_END_COMMIT, commits.get(1));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat4 = this.tableSource.getInputFormat();
    assertThat(inputFormat4, instanceOf(CdcInputFormat.class));

    List<RowData> actual4 = readData(inputFormat4);
    final List<RowData> expected4 = TestData.dataSetUpsert(2, 1);
    TestData.assertRowDataEquals(actual4, expected4);

    // start and end commit: start commit out of range
    conf.setString(FlinkOptions.READ_START_COMMIT, "000");
    conf.setString(FlinkOptions.READ_END_COMMIT, commits.get(1));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat5 = this.tableSource.getInputFormat();
    assertThat(inputFormat5, instanceOf(CdcInputFormat.class));

    List<RowData> actual5 = readData(inputFormat5);
    final List<RowData> expected5 = TestData.dataSetInsert(1, 2);
    TestData.assertRowDataEquals(actual5, expected5);

    // start and end commit: both are out of range
    conf.setString(FlinkOptions.READ_START_COMMIT, "001");
    conf.setString(FlinkOptions.READ_END_COMMIT, "002");
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat6 = this.tableSource.getInputFormat();

    List<RowData> actual6 = readData(inputFormat6);
    TestData.assertRowDataEquals(actual6, Collections.emptyList());
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

    // write another commit with delete messages
    TestData.writeData(TestData.DATA_SET_SINGLE_DELETE, conf);
    this.tableSource.reset();
    inputFormat = this.tableSource.getInputFormat();
    assertThat(inputFormat, instanceOf(MergeOnReadInputFormat.class));
    final String baseMergeLogFileResult2 = TestData.rowDataToString(readData(inputFormat));
    assertThat(baseMergeLogFileResult2, is("[]"));
  }

  /**
   * This test check 2 cases of records preCombining.
   * When the preCombine is true, the writer does an in-memory combing for incoming records,
   * when the preCombine is false, the merged log reader does the combining while reading.
   * With disorder deletes, we can check whether the '_hoodie_operation' is correctly set up.
   */
  @ParameterizedTest
  @MethodSource("preCombiningAndChangelogModeParams")
  void testMergeOnReadDisorderDeleteMerging(boolean preCombine, boolean changelogMode) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PRE_COMBINE.key(), preCombine + "");
    options.put(FlinkOptions.CHANGELOG_ENABLED.key(), changelogMode + "");
    beforeEach(HoodieTableType.MERGE_ON_READ, options);

    // write log file with disorder deletes
    TestData.writeData(TestData.DATA_SET_DISORDER_INSERT_DELETE, conf);
    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();
    final String baseResult = TestData.rowDataToString(readData(inputFormat));
    String expected = "[+I[id1, Danny, 22, 1970-01-01T00:00:00.004, par1]]";
    assertThat(baseResult, is(expected));
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
        HoodieFlinkEngineContext.DEFAULT, FlinkWriteClients.getHoodieClientConfig(conf));
    writeClient.clean();

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(tempFile.getAbsolutePath(), HadoopConfigurations.getHadoopConf(conf));
    List<String> commits = metaClient.getCommitsTimeline().filterCompletedInstants().getInstantsAsStream()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());

    assertThat(commits.size(), is(4));

    List<String> archivedCommits = metaClient.getArchivedTimeline().getCommitsTimeline().filterCompletedInstants()
        .getInstantsAsStream().map(HoodieInstant::getTimestamp).collect(Collectors.toList());

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

    // start and end commit: start is archived and cleaned, end is active and cleaned
    conf.setString(FlinkOptions.READ_START_COMMIT, archivedCommits.get(1));
    conf.setString(FlinkOptions.READ_END_COMMIT, commits.get(0));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat4 = this.tableSource.getInputFormat();
    // assertThat(inputFormat4, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual4 = readData(inputFormat4);
    // final List<RowData> expected4 = TestData.dataSetInsert(3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
    TestData.assertRowDataEquals(actual4, Collections.emptyList());
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

  /**
   * Return test params => (preCombining, changelog mode).
   */
  private static Stream<Arguments> preCombiningAndChangelogModeParams() {
    Object[][] data =
        new Object[][] {
            {true, true},
            {true, false},
            {false, true},
            {false, false}};
    return Stream.of(data).map(Arguments::of);
  }

  private HoodieTableSource getTableSource(Configuration conf) {
    return new HoodieTableSource(
        TestConfigurations.TABLE_SCHEMA,
        new Path(tempFile.getAbsolutePath()),
        Collections.singletonList("partition"),
        "default",
        conf);
  }

  @SuppressWarnings("rawtypes")
  private static List<RowData> readData(InputFormat inputFormat) throws IOException {
    InputSplit[] inputSplits = inputFormat.createInputSplits(1);
    return readData(inputFormat, inputSplits);
  }

  @SuppressWarnings("unchecked, rawtypes")
  private static List<RowData> readData(InputFormat inputFormat, InputSplit[] inputSplits) throws IOException {
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
