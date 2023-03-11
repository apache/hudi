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

package org.apache.hudi.table;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.factory.CollectSinkTableFactory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.utils.TestConfigurations.sql;
import static org.apache.hudi.utils.TestData.assertRowsEquals;
import static org.apache.hudi.utils.TestData.insertRow;

/**
 * IT cases for Hoodie table source and sink.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestHoodieContinuousPartitionSource {

  private static List<RowData> DATA_SET_NEW_PARTITIONS = Arrays.asList(
      insertRow(StringData.fromString("id9"), StringData.fromString("LiLi"), 24,
          TimestampData.fromEpochMillis(9), StringData.fromString("par5")),
      insertRow(StringData.fromString("id10"), StringData.fromString("Guoguo"), 34,
          TimestampData.fromEpochMillis(10), StringData.fromString("par5")),
      insertRow(StringData.fromString("id11"), StringData.fromString("Baobao"), 34,
          TimestampData.fromEpochMillis(10), StringData.fromString("par7")),
      insertRow(StringData.fromString("id12"), StringData.fromString("LinLin"), 34,
          TimestampData.fromEpochMillis(10), StringData.fromString("part8"))
  );
  private TableEnvironment tEnv;

  @BeforeEach
  void beforeEach() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    tEnv = TableEnvironmentImpl.create(settings);
    tEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    Configuration execConf = tEnv.getConfig().getConfiguration();
    execConf.setString("execution.checkpointing.interval", "2s");
    // configure not to retry after failure
    execConf.setString("restart-strategy", "fixed-delay");
    execConf.setString("restart-strategy.fixed-delay.attempts", "0");
  }

  @TempDir
  File tempFile;

  @ParameterizedTest
  @MethodSource("tableTypeAndPartitioningParams")
  void testOr(
      HoodieTableType tableType, boolean hiveStylePartitioning) throws Exception {
    String condition = "`partition` = 'par5' or `partition` = 'par6'";
    List<RowData> result = Arrays.asList(
        insertRow(StringData.fromString("id9"), StringData.fromString("LiLi"), 24,
            TimestampData.fromEpochMillis(9), StringData.fromString("par5")),
        insertRow(StringData.fromString("id10"), StringData.fromString("Guoguo"), 34,
            TimestampData.fromEpochMillis(10), StringData.fromString("par5"))
    );
    testStreamReadContinuousPartitionPrune(tableType, hiveStylePartitioning, condition, result);
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndPartitioningParams")
  void testAnd(
      HoodieTableType tableType, boolean hiveStylePartitioning) throws Exception {
    String condition = "`partition` >= 'par5' and `partition` <= 'par6'";
    List<RowData> result = Arrays.asList(
        insertRow(StringData.fromString("id9"), StringData.fromString("LiLi"), 24,
            TimestampData.fromEpochMillis(9), StringData.fromString("par5")),
        insertRow(StringData.fromString("id10"), StringData.fromString("Guoguo"), 34,
            TimestampData.fromEpochMillis(10), StringData.fromString("par5"))
    );
    testStreamReadContinuousPartitionPrune(tableType, hiveStylePartitioning, condition, result);
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndPartitioningParams")
  void testNot(
      HoodieTableType tableType, boolean hiveStylePartitioning) throws Exception {
    String condition = "`partition` <> 'par6'";
    List<RowData> result = Arrays.asList(
        insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
            TimestampData.fromEpochMillis(1), StringData.fromString("par1")),
        insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 33,
            TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
        insertRow(StringData.fromString("id3"), StringData.fromString("Julian"), 53,
            TimestampData.fromEpochMillis(3), StringData.fromString("par2")),
        insertRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 31,
            TimestampData.fromEpochMillis(4), StringData.fromString("par2")),
        insertRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
            TimestampData.fromEpochMillis(5), StringData.fromString("par3")),
        insertRow(StringData.fromString("id6"), StringData.fromString("Emma"), 20,
            TimestampData.fromEpochMillis(6), StringData.fromString("par3")),
        insertRow(StringData.fromString("id7"), StringData.fromString("Bob"), 44,
            TimestampData.fromEpochMillis(7), StringData.fromString("par4")),
        insertRow(StringData.fromString("id8"), StringData.fromString("Han"), 56,
            TimestampData.fromEpochMillis(8), StringData.fromString("par4")),
        insertRow(StringData.fromString("id9"), StringData.fromString("LiLi"), 24,
            TimestampData.fromEpochMillis(9), StringData.fromString("par5")),
        insertRow(StringData.fromString("id10"), StringData.fromString("Guoguo"), 34,
            TimestampData.fromEpochMillis(10), StringData.fromString("par5")),
        insertRow(StringData.fromString("id11"), StringData.fromString("Baobao"), 34,
            TimestampData.fromEpochMillis(10), StringData.fromString("par7")),
        insertRow(StringData.fromString("id12"), StringData.fromString("LinLin"), 34,
            TimestampData.fromEpochMillis(10), StringData.fromString("part8"))
    );
    testStreamReadContinuousPartitionPrune(tableType, hiveStylePartitioning, condition, result);
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndPartitioningParams")
  void testNotIn(
      HoodieTableType tableType, boolean hiveStylePartitioning) throws Exception {
    String condition = "`partition` not in ('par1', 'par2', 'par3')";
    List<RowData> result = Arrays.asList(
        insertRow(StringData.fromString("id7"), StringData.fromString("Bob"), 44,
            TimestampData.fromEpochMillis(7), StringData.fromString("par4")),
        insertRow(StringData.fromString("id8"), StringData.fromString("Han"), 56,
            TimestampData.fromEpochMillis(8), StringData.fromString("par4")),
        insertRow(StringData.fromString("id9"), StringData.fromString("LiLi"), 24,
            TimestampData.fromEpochMillis(9), StringData.fromString("par5")),
        insertRow(StringData.fromString("id10"), StringData.fromString("Guoguo"), 34,
            TimestampData.fromEpochMillis(10), StringData.fromString("par5")),
        insertRow(StringData.fromString("id11"), StringData.fromString("Baobao"), 34,
            TimestampData.fromEpochMillis(10), StringData.fromString("par7")),
        insertRow(StringData.fromString("id12"), StringData.fromString("LinLin"), 34,
            TimestampData.fromEpochMillis(10), StringData.fromString("part8"))
    );
    testStreamReadContinuousPartitionPrune(tableType, hiveStylePartitioning, condition, result);
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndPartitioningParams")
  void testContainNonSimpleCall(
      HoodieTableType tableType, boolean hiveStylePartitioning) throws Exception {
    String condition = "`partition` like 'part%' and `partition` <> 'part8' ";
    List<RowData> result = new ArrayList<>();
    testStreamReadContinuousPartitionPrune(tableType, hiveStylePartitioning, condition, result);
  }

  private void testStreamReadContinuousPartitionPrune(
      HoodieTableType tableType,
      boolean hiveStylePartitioning,
      String filterCondition,
      List<RowData> results
  ) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.TABLE_NAME, "t1");
    conf.setString(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.setBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning);

    // write one commit
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 2)
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning)
        .option(FlinkOptions.READ_STREAMING_CONTINUOUS_PARTITION_PRUNE, true)
        .end();
    tEnv.executeSql(hoodieTableDDL);

    String sinkDDL = "create table sink(\n"
        + "  uuid varchar(20),\n"
        + "  name varchar(20),\n"
        + "  age int,\n"
        + "  ts timestamp,\n"
        + "  part varchar(20)"
        + ") with (\n"
        + "  'connector' = '" + CollectSinkTableFactory.FACTORY_ID + "'"
        + ")";
    TableResult tableResult = submitSelectSql(
        tEnv,
        "select uuid, name, age, ts, `partition` as part from t1 where " + filterCondition,
        sinkDDL);
    TestData.writeData(DATA_SET_NEW_PARTITIONS, conf);
    List<Row> result = stopAndFetchData(tEnv, tableResult, 10);
    assertRowsEquals(result, results);
  }

  private List<Row> stopAndFetchData(TableEnvironment tEnv, TableResult tableResult, long timeout)
      throws InterruptedException {
    // wait for the timeout then cancels the job
    TimeUnit.SECONDS.sleep(timeout);
    tableResult.getJobClient().ifPresent(JobClient::cancel);
    tEnv.executeSql("DROP TABLE IF EXISTS sink");
    return CollectSinkTableFactory.RESULT.values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * Return test params => (HoodieTableType, hive style partitioning).
   */
  private static Stream<Arguments> tableTypeAndPartitioningParams() {
    Object[][] data =
        new Object[][] {
            {HoodieTableType.COPY_ON_WRITE, false},
            {HoodieTableType.COPY_ON_WRITE, true},
            {HoodieTableType.MERGE_ON_READ, false},
            {HoodieTableType.MERGE_ON_READ, true}};
    return Stream.of(data).map(Arguments::of);
  }

  private TableResult submitSelectSql(TableEnvironment tEnv, String select, String sinkDDL) {
    tEnv.executeSql("DROP TABLE IF EXISTS sink");
    tEnv.executeSql(sinkDDL);
    TableResult tableResult = tEnv.executeSql("insert into sink " + select);
    return tableResult;
  }
}
