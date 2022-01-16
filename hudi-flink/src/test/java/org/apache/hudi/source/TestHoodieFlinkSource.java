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

package org.apache.hudi.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.utils.SuccessException;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.utils.TestData.assertRowDataEquals;

/**
 * Test cases for {@link StreamReadMonitoringFunction}.
 */
public class TestHoodieFlinkSource extends AbstractTestBase {
  protected StreamExecutionEnvironment execEnv;
  protected StreamTableEnvironment tEnv;

  private Configuration conf;

  @TempDir
  File tempFile;

  @BeforeEach
  void beforeEach() {
    final String basePath = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(basePath);
    conf.setString("execution.checkpointing.interval", "2s");
    // configure not to retry after failure
    conf.setString("restart-strategy", "fixed-delay");
    conf.setString("restart-strategy.fixed-delay.attempts", "0");

    execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    tEnv = StreamTableEnvironment.create(execEnv);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testConsumeFromSpecifiedCommit(HoodieTableType tableType) throws Exception {
    // write 2 commits first, use the second commit time as the specified start instant,
    // all the splits should come from the second commit.
    TestData.writeData(TestData.DATA_SET_SOURCE_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT, conf);

    String specifiedCommit = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.TABLE_TYPE, tableType.toString());
    conf.setString(FlinkOptions.READ_START_COMMIT, specifiedCommit);
    conf.setBoolean(FlinkOptions.READ_AS_STREAMING, true);

    testRowData(conf, TestData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testConsumeFromLatestCommit(HoodieTableType tableType) throws Exception {
    // write 2 commits first， and all the splits should come from the second commit.
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);

    String latestCommit = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.TABLE_TYPE, tableType.toString());
    conf.setString(FlinkOptions.READ_START_COMMIT, latestCommit);
    conf.setBoolean(FlinkOptions.READ_AS_STREAMING, true);

    testRowData(conf, TestData.DATA_SET_UPDATE_INSERT);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testConsumeFromEarliestCommit(HoodieTableType tableType) throws Exception {
    // write 2 commits first, then specify the start commit as 'earliest',
    // all the splits should come from the earliest commit.
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    conf.setBoolean(FlinkOptions.READ_AS_STREAMING, true);

    conf.setString(FlinkOptions.TABLE_TYPE, tableType.toString());
    conf.setString(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);

    testRowData(conf, TestData.DATA_SET_MERGED);
  }

  @Test
  void testStreamWriteBatchRead() throws Exception {
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    conf.setString(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    conf.setString(FlinkOptions.READ_START_COMMIT, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    conf.setBoolean(FlinkOptions.READ_AS_STREAMING, false);

    testRowData(conf, TestData.DATA_SET_INSERT);
  }

  private void testRowData(Configuration conf, List<RowData> expected) throws Exception {
    HoodieSourceContext hoodieSourceContext = new HoodieSourceContext(
        TestConfigurations.TABLE_SCHEMA,
        new Path(tempFile.getAbsolutePath()),
        Arrays.asList(conf.getString(FlinkOptions.PARTITION_PATH_FIELD).split(",")),
        "default-par",
        conf);

    DataStream<RowData> dataStream = HoodieFlinkSource.builder()
        .env(execEnv)
        .tableSchema(hoodieSourceContext.getSchema())
        .path(hoodieSourceContext.getPath())
        .partitionKeys(hoodieSourceContext.getPartitionKeys())
        .defaultPartName(hoodieSourceContext.getDefaultPartName())
        .conf(hoodieSourceContext.getConf())
        .requiredPartitions(hoodieSourceContext.getRequiredPartitions())
        .requiredPos(hoodieSourceContext.getRequiredPos())
        .limit(hoodieSourceContext.getLimit())
        .filters(hoodieSourceContext.getFilters())
        .build();

    TestingSinkFunction sink = new TestingSinkFunction(expected.size());
    dataStream.addSink(sink).setParallelism(1);

    try {
      execEnv.execute("Test Consume From Specified Commit");
    } catch (Throwable e) {
      // we have to use a specific exception to indicate the job is finished,
      // because the registered Kafka source is infinite.
      if (!isCausedByJobFinished(e)) {
        // re-throw
        throw e;
      }
    }

    assertRowDataEquals(TestingSinkFunction.ROWS, expected);
  }

  private static final class TestingSinkFunction implements SinkFunction<RowData> {

    private static final long serialVersionUID = 455430015321124493L;
    private static final List<RowData> ROWS = new ArrayList<>();

    private final int expectedSize;

    private TestingSinkFunction(int expectedSize) {
      this.expectedSize = expectedSize;
      ROWS.clear();
    }

    @Override
    public void invoke(RowData value, Context context) {
      ROWS.add(value);
      if (ROWS.size() >= expectedSize) {
        // job finish
        throw new SuccessException();
      }
    }
  }

  private static boolean isCausedByJobFinished(Throwable e) {
    if (e instanceof SuccessException) {
      return true;
    } else if (e.getCause() != null) {
      return isCausedByJobFinished(e.getCause());
    } else {
      return false;
    }
  }
}
