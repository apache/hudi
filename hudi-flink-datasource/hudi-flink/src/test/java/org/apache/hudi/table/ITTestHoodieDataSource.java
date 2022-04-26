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

import org.apache.hudi.adapter.TestTableEnvs;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestSQL;
import org.apache.hudi.utils.TestUtils;
import org.apache.hudi.utils.factory.CollectSinkTableFactory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.utils.TestConfigurations.catalog;
import static org.apache.hudi.utils.TestConfigurations.sql;
import static org.apache.hudi.utils.TestData.assertRowsEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IT cases for Hoodie table source and sink.
 */
public class ITTestHoodieDataSource extends AbstractTestBase {
  private TableEnvironment streamTableEnv;
  private TableEnvironment batchTableEnv;

  @BeforeEach
  void beforeEach() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    streamTableEnv = TableEnvironmentImpl.create(settings);
    streamTableEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
    Configuration execConf = streamTableEnv.getConfig().getConfiguration();
    execConf.setString("execution.checkpointing.interval", "2s");
    // configure not to retry after failure
    execConf.setString("restart-strategy", "fixed-delay");
    execConf.setString("restart-strategy.fixed-delay.attempts", "0");

    batchTableEnv = TestTableEnvs.getBatchTableEnv();
    batchTableEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
  }

  @TempDir
  File tempFile;

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testStreamWriteAndReadFromSpecifiedCommit(HoodieTableType tableType) throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    String firstCommit = TestUtils.getFirstCompleteInstant(tempFile.getAbsolutePath());
    streamTableEnv.executeSql("drop table t1");
    hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_START_COMMIT, firstCommit)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    List<Row> rows = execSelectSql(streamTableEnv, "select * from t1", 10);
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);

    // insert another batch of data
    execInsertSql(streamTableEnv, insertInto);
    List<Row> rows2 = execSelectSql(streamTableEnv, "select * from t1", 10);
    assertRowsEquals(rows2, TestData.DATA_SET_SOURCE_INSERT);

    streamTableEnv.getConfig().getConfiguration()
        .setBoolean("table.dynamic-table-options.enabled", true);
    // specify the start commit as earliest
    List<Row> rows3 = execSelectSql(streamTableEnv,
        "select * from t1/*+options('read.start-commit'='earliest')*/", 10);
    assertRowsEquals(rows3, TestData.DATA_SET_SOURCE_INSERT);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testStreamWriteAndRead(HoodieTableType tableType) throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    // reading from the latest commit instance.
    List<Row> rows = execSelectSql(streamTableEnv, "select * from t1", 10);
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT);

    // insert another batch of data
    execInsertSql(streamTableEnv, insertInto);
    List<Row> rows2 = execSelectSql(streamTableEnv, "select * from t1", 10);
    assertRowsEquals(rows2, TestData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testStreamReadAppendData(HoodieTableType tableType) throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    String createSource2 = TestConfigurations.getFileSourceDDL("source2", "test_source_2.data");
    streamTableEnv.executeSql(createSource);
    streamTableEnv.executeSql(createSource2);

    String createHoodieTable = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .end();
    streamTableEnv.executeSql(createHoodieTable);
    String insertInto = "insert into t1 select * from source";
    // execute 2 times
    execInsertSql(streamTableEnv, insertInto);
    // remember the commit
    String specifiedCommit = TestUtils.getFirstCompleteInstant(tempFile.getAbsolutePath());
    // another update batch
    String insertInto2 = "insert into t1 select * from source2";
    execInsertSql(streamTableEnv, insertInto2);
    // now we consume starting from the oldest commit
    String createHoodieTable2 = sql("t2")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_START_COMMIT, specifiedCommit)
        .end();
    streamTableEnv.executeSql(createHoodieTable2);
    List<Row> rows = execSelectSql(streamTableEnv, "select * from t2", 10);
    // all the data with same keys are appended within one data bucket and one log file,
    // so when consume, the same keys are merged
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_MERGED);
  }

  @Test
  void testStreamWriteBatchRead() {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    List<Row> rows = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
  }

  @Test
  void testStreamWriteBatchReadOptimized() {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        // read optimized is supported for both MOR and COR table,
        // test MOR streaming write with compaction then reads as
        // query type 'read_optimized'.
        .option(FlinkOptions.QUERY_TYPE, FlinkOptions.QUERY_TYPE_READ_OPTIMIZED)
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, 1)
        .option(FlinkOptions.COMPACTION_TASKS, 1)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    List<Row> rows = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
  }

  @Test
  void testStreamWriteReadSkippingCompaction() throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source", 4);
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, true)
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, 1)
        .option(FlinkOptions.COMPACTION_TASKS, 1)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    String instant = TestUtils.getNthCompleteInstant(tempFile.getAbsolutePath(), 2, true);

    streamTableEnv.getConfig().getConfiguration()
        .setBoolean("table.dynamic-table-options.enabled", true);
    final String query = String.format("select * from t1/*+ options('read.start-commit'='%s')*/", instant);
    List<Row> rows = execSelectSql(streamTableEnv, query, 10);
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT);
  }

  @Test
  void testStreamWriteWithCleaning() {
    // create filesystem table named source

    // the source generates 4 commits but the cleaning task
    // would always try to keep the remaining commits number as 1
    String createSource = TestConfigurations.getFileSourceDDL(
        "source", "test_source_3.data", 4);
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.CLEAN_RETAIN_COMMITS, 1)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    Configuration defaultConf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    Map<String, String> options1 = new HashMap<>(defaultConf.toMap());
    options1.put(FlinkOptions.TABLE_NAME.key(), "t1");
    Configuration conf = Configuration.fromMap(options1);
    HoodieTimeline timeline = StreamerUtil.createMetaClient(conf).getActiveTimeline();
    assertTrue(timeline.filterCompletedInstants()
            .getInstants().anyMatch(instant -> instant.getAction().equals("clean")),
        "some commits should be cleaned");
  }

  @Test
  void testStreamReadWithDeletes() throws Exception {
    // create filesystem table named source

    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.TABLE_NAME, "t1");
    conf.setString(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    conf.setBoolean(FlinkOptions.CHANGELOG_ENABLED, true);

    // write one commit
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    // write another commit with deletes
    TestData.writeData(TestData.DATA_SET_UPDATE_DELETE, conf);

    String latestCommit = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 2)
        .option(FlinkOptions.READ_START_COMMIT, latestCommit)
        .option(FlinkOptions.CHANGELOG_ENABLED, true)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);

    final String sinkDDL = "create table sink(\n"
        + "  name varchar(20),\n"
        + "  age_sum int\n"
        + ") with (\n"
        + "  'connector' = '" + CollectSinkTableFactory.FACTORY_ID + "'"
        + ")";
    List<Row> result = execSelectSql(streamTableEnv,
        "select name, sum(age) from t1 group by name", sinkDDL, 10);
    final String expected = "[+I(+I[Danny, 24]), +I(+I[Stephen, 34])]";
    assertRowsEquals(result, expected, true);
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndPartitioningParams")
  void testStreamReadFilterByPartition(HoodieTableType tableType, boolean hiveStylePartitioning) throws Exception {
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
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);

    List<Row> result = execSelectSql(streamTableEnv,
        "select * from t1 where `partition`='par1'", 10);
    final String expected = "["
        + "+I(+I[id1, Danny, 23, 1970-01-01T00:00:00.001, par1]), "
        + "+I(+I[id2, Stephen, 33, 1970-01-01T00:00:00.002, par1])]";
    assertRowsEquals(result, expected, true);
  }

  @Test
  void testStreamReadMorTableWithCompactionPlan() throws Exception {
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST)
        .option(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 2)
        // close the async compaction
        .option(FlinkOptions.COMPACTION_ASYNC_ENABLED, false)
        // generate compaction plan for each commit
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, 1)
        .noPartition()
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);

    streamTableEnv.executeSql("insert into t1 select * from source");

    List<Row> result = execSelectSql(streamTableEnv, "select * from t1", 10);
    final String expected = "["
        + "+I[id1, Danny, 23, 1970-01-01T00:00:01, par1], "
        + "+I[id2, Stephen, 33, 1970-01-01T00:00:02, par1], "
        + "+I[id3, Julian, 53, 1970-01-01T00:00:03, par2], "
        + "+I[id4, Fabian, 31, 1970-01-01T00:00:04, par2], "
        + "+I[id5, Sophia, 18, 1970-01-01T00:00:05, par3], "
        + "+I[id6, Emma, 20, 1970-01-01T00:00:06, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]";
    assertRowsEquals(result, expected);
  }

  @ParameterizedTest
  @MethodSource("executionModeAndPartitioningParams")
  void testWriteAndRead(ExecMode execMode, boolean hiveStylePartitioning) {
    TableEnvironment tableEnv = execMode == ExecMode.BATCH ? batchTableEnv : streamTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);
    // apply filters
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where uuid > 'id5'").execute().collect());
    assertRowsEquals(result2, "["
        + "+I[id6, Emma, 20, 1970-01-01T00:00:06, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]");
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndPartitioningParams")
  void testWriteAndReadWithProctimeSequence(HoodieTableType tableType, boolean hiveStylePartitioning) {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .field("uuid varchar(20)")
        .field("name varchar(10)")
        .field("age int")
        .field("tss timestamp(3)") // use a different field with default precombine field 'ts'
        .field("`partition` varchar(10)")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.INSERT_SAME_KEY_T1);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, "[+I[id1, Danny, 23, 1970-01-01T00:00:01, par1]]");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testBatchModeUpsertWithoutPartition(HoodieTableType tableType) {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_NAME, tableType.name())
        .option("hoodie.parquet.small.file.limit", "0") // invalidate the small file strategy
        .option("hoodie.parquet.max.file.size", "0")
        .noPartition()
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);

    // batchMode update
    execInsertSql(tableEnv, TestSQL.UPDATE_INSERT_T1);
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result2, TestData.DATA_SET_SOURCE_MERGED);
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndPartitioningParams")
  void testBatchModeUpsert(HoodieTableType tableType, boolean hiveStylePartitioning) {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_NAME, tableType)
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);

    // batchMode update
    execInsertSql(tableEnv, TestSQL.UPDATE_INSERT_T1);

    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result2, TestData.DATA_SET_SOURCE_MERGED);
  }

  @ParameterizedTest
  @EnumSource(value = ExecMode.class)
  void testWriteAndReadParMiddle(ExecMode execMode) throws Exception {
    boolean streaming = execMode == ExecMode.STREAM;
    String hoodieTableDDL = "create table t1(\n"
        + "  uuid varchar(20),\n"
        + "  name varchar(10),\n"
        + "  age int,\n"
        + "  `partition` varchar(20),\n" // test streaming read with partition field in the middle
        + "  ts timestamp(3),\n"
        + "  PRIMARY KEY(uuid) NOT ENFORCED\n"
        + ")\n"
        + "PARTITIONED BY (`partition`)\n"
        + "with (\n"
        + "  'connector' = 'hudi',\n"
        + "  'path' = '" + tempFile.getAbsolutePath() + "',\n"
        + "  'read.streaming.enabled' = '" + streaming + "'\n"
        + ")";
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 values\n"
        + "('id1','Danny',23,'par1',TIMESTAMP '1970-01-01 00:00:01'),\n"
        + "('id2','Stephen',33,'par1',TIMESTAMP '1970-01-01 00:00:02'),\n"
        + "('id3','Julian',53,'par2',TIMESTAMP '1970-01-01 00:00:03'),\n"
        + "('id4','Fabian',31,'par2',TIMESTAMP '1970-01-01 00:00:04'),\n"
        + "('id5','Sophia',18,'par3',TIMESTAMP '1970-01-01 00:00:05'),\n"
        + "('id6','Emma',20,'par3',TIMESTAMP '1970-01-01 00:00:06'),\n"
        + "('id7','Bob',44,'par4',TIMESTAMP '1970-01-01 00:00:07'),\n"
        + "('id8','Han',56,'par4',TIMESTAMP '1970-01-01 00:00:08')";
    execInsertSql(streamTableEnv, insertInto);

    final String expected = "["
        + "+I[id1, Danny, 23, par1, 1970-01-01T00:00:01], "
        + "+I[id2, Stephen, 33, par1, 1970-01-01T00:00:02], "
        + "+I[id3, Julian, 53, par2, 1970-01-01T00:00:03], "
        + "+I[id4, Fabian, 31, par2, 1970-01-01T00:00:04], "
        + "+I[id5, Sophia, 18, par3, 1970-01-01T00:00:05], "
        + "+I[id6, Emma, 20, par3, 1970-01-01T00:00:06], "
        + "+I[id7, Bob, 44, par4, 1970-01-01T00:00:07], "
        + "+I[id8, Han, 56, par4, 1970-01-01T00:00:08]]";

    List<Row> result = execSelectSql(streamTableEnv, "select * from t1", execMode);

    assertRowsEquals(result, expected);

    // insert another batch of data
    execInsertSql(streamTableEnv, insertInto);
    List<Row> result2 = execSelectSql(streamTableEnv, "select * from t1", execMode);
    assertRowsEquals(result2, expected);
  }

  @ParameterizedTest
  @EnumSource(value = ExecMode.class)
  void testWriteAndReadWithTimestampMicros(ExecMode execMode) throws Exception {
    boolean streaming = execMode == ExecMode.STREAM;
    String hoodieTableDDL = sql("t1")
        .field("id int")
        .field("name varchar(10)")
        .field("ts timestamp(6)")
        .pkField("id")
        .noPartition()
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.READ_AS_STREAMING, streaming)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 values\n"
        + "(1,'Danny',TIMESTAMP '2021-12-01 01:02:01.100001'),\n"
        + "(2,'Stephen',TIMESTAMP '2021-12-02 03:04:02.200002'),\n"
        + "(3,'Julian',TIMESTAMP '2021-12-03 13:14:03.300003'),\n"
        + "(4,'Fabian',TIMESTAMP '2021-12-04 15:16:04.400004')";
    execInsertSql(streamTableEnv, insertInto);

    final String expected = "["
        + "+I[1, Danny, 2021-12-01T01:02:01.100001], "
        + "+I[2, Stephen, 2021-12-02T03:04:02.200002], "
        + "+I[3, Julian, 2021-12-03T13:14:03.300003], "
        + "+I[4, Fabian, 2021-12-04T15:16:04.400004]]";

    List<Row> result = execSelectSql(streamTableEnv, "select * from t1", execMode);
    assertRowsEquals(result, expected);

    // insert another batch of data
    execInsertSql(streamTableEnv, insertInto);
    List<Row> result2 = execSelectSql(streamTableEnv, "select * from t1", execMode);
    assertRowsEquals(result2, expected);
  }

  @ParameterizedTest
  @EnumSource(value = ExecMode.class)
  void testInsertOverwrite(ExecMode execMode) {
    TableEnvironment tableEnv = execMode == ExecMode.BATCH ? batchTableEnv : streamTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    // overwrite partition 'par1' and increase in age by 1
    final String insertInto2 = "insert overwrite t1 partition(`partition`='par1') values\n"
        + "('id1','Danny',24,TIMESTAMP '1970-01-01 00:00:01'),\n"
        + "('id2','Stephen',34,TIMESTAMP '1970-01-01 00:00:02')\n";

    execInsertSql(tableEnv, insertInto2);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT_OVERWRITE);

    // overwrite the whole table
    final String insertInto3 = "insert overwrite t1 values\n"
        + "('id1','Danny',24,TIMESTAMP '1970-01-01 00:00:01', 'par1'),\n"
        + "('id2','Stephen',34,TIMESTAMP '1970-01-01 00:00:02', 'par2')\n";

    execInsertSql(tableEnv, insertInto3);

    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    final String expected = "["
        + "+I[id1, Danny, 24, 1970-01-01T00:00:01, par1], "
        + "+I[id2, Stephen, 34, 1970-01-01T00:00:02, par2]]";
    assertRowsEquals(result2, expected);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testStreamWriteAndReadWithMiniBatches(HoodieTableType tableType) throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source", 4);
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_START_COMMIT, "earliest")
        .option(FlinkOptions.WRITE_BATCH_SIZE, 0.00001)
        .noPartition()
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    // reading from the earliest commit instance.
    List<Row> rows = execSelectSql(streamTableEnv, "select * from t1", 20);
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
  }

  @ParameterizedTest
  @MethodSource("executionModeAndTableTypeParams")
  void testBatchUpsertWithMiniBatches(ExecMode execMode, HoodieTableType tableType) {
    TableEnvironment tableEnv = execMode == ExecMode.BATCH ? batchTableEnv : streamTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.WRITE_BATCH_SIZE, "0.001")
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    final String insertInto1 = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1')";

    execInsertSql(tableEnv, insertInto1);

    final String insertInto2 = "insert into t1 values\n"
        + "('id1','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),\n"
        + "('id1','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par1'),\n"
        + "('id1','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par1'),\n"
        + "('id1','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par1')";

    execInsertSql(tableEnv, insertInto2);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, "[+I[id1, Sophia, 18, 1970-01-01T00:00:05, par1]]");
  }

  @ParameterizedTest
  @MethodSource("executionModeAndTableTypeParams")
  void testBatchUpsertWithMiniBatchesGlobalIndex(ExecMode execMode, HoodieTableType tableType) {
    TableEnvironment tableEnv = execMode == ExecMode.BATCH ? batchTableEnv : streamTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.WRITE_BATCH_SIZE, "0.001")
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.INDEX_GLOBAL_ENABLED, true)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    final String insertInto1 = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1')";

    execInsertSql(tableEnv, insertInto1);

    final String insertInto2 = "insert into t1 values\n"
        + "('id1','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par2'),\n"
        + "('id1','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par1'),\n"
        + "('id1','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2'),\n"
        + "('id1','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3')";

    execInsertSql(tableEnv, insertInto2);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, "[+I[id1, Sophia, 18, 1970-01-01T00:00:05, par3]]");
  }

  @Test
  void testUpdateWithDefaultHoodieRecordPayload() {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .field("id int")
        .field("name string")
        .field("price double")
        .field("ts bigint")
        .pkField("id")
        .noPartition()
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.PAYLOAD_CLASS_NAME, DefaultHoodieRecordPayload.class.getName())
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    final String insertInto1 = "insert into t1 values\n"
        + "(1,'a1',20,20)";
    execInsertSql(tableEnv, insertInto1);

    final String insertInto4 = "insert into t1 values\n"
        + "(1,'a1',20,1)";
    execInsertSql(tableEnv, insertInto4);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, "[+I[1, a1, 20.0, 20]]");
  }

  @ParameterizedTest
  @MethodSource("executionModeAndTableTypeParams")
  void testWriteNonPartitionedTable(ExecMode execMode, HoodieTableType tableType) {
    TableEnvironment tableEnv = execMode == ExecMode.BATCH ? batchTableEnv : streamTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .noPartition()
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    final String insertInto1 = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1')";

    execInsertSql(tableEnv, insertInto1);

    final String insertInto2 = "insert into t1 values\n"
        + "('id1','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par2'),\n"
        + "('id1','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par3'),\n"
        + "('id1','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par4'),\n"
        + "('id1','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par5')";

    execInsertSql(tableEnv, insertInto2);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, "[+I[id1, Sophia, 18, 1970-01-01T00:00:05, par5]]");
  }

  @Test
  void testWriteGlobalIndex() {
    // the source generates 4 commits
    String createSource = TestConfigurations.getFileSourceDDL(
        "source", "test_source_4.data", 4);
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.INDEX_GLOBAL_ENABLED, true)
        .option(FlinkOptions.PRE_COMBINE, true)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);

    final String insertInto2 = "insert into t1 select * from source";

    execInsertSql(streamTableEnv, insertInto2);

    List<Row> result = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, "[+I[id1, Phoebe, 52, 1970-01-01T00:00:08, par4]]");
  }

  @Test
  void testWriteLocalIndex() {
    // the source generates 4 commits
    String createSource = TestConfigurations.getFileSourceDDL(
        "source", "test_source_4.data", 4);
    streamTableEnv.executeSql(createSource);
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.INDEX_GLOBAL_ENABLED, false)
        .option(FlinkOptions.PRE_COMBINE, true)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);

    final String insertInto2 = "insert into t1 select * from source";

    execInsertSql(streamTableEnv, insertInto2);

    List<Row> result = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    final String expected = "["
        + "+I[id1, Stephen, 34, 1970-01-01T00:00:02, par1], "
        + "+I[id1, Fabian, 32, 1970-01-01T00:00:04, par2], "
        + "+I[id1, Jane, 19, 1970-01-01T00:00:06, par3], "
        + "+I[id1, Phoebe, 52, 1970-01-01T00:00:08, par4]]";
    assertRowsEquals(result, expected, 3);
  }

  @Test
  void testStreamReadEmptyTablePath() throws Exception {
    // case1: table metadata path does not exists
    // create a flink source table
    String createHoodieTable = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.READ_AS_STREAMING, "true")
        .option(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .end();
    streamTableEnv.executeSql(createHoodieTable);

    // no exception expects to be thrown
    List<Row> rows1 = execSelectSql(streamTableEnv, "select * from t1", 10);
    assertRowsEquals(rows1, "[]");

    // case2: empty table without data files
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    StreamerUtil.initTableIfNotExists(conf);

    List<Row> rows2 = execSelectSql(streamTableEnv, "select * from t1", 10);
    assertRowsEquals(rows2, "[]");
  }

  @Test
  void testBatchReadEmptyTablePath() throws Exception {
    // case1: table metadata path does not exists
    // create a flink source table
    String createHoodieTable = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .end();
    batchTableEnv.executeSql(createHoodieTable);

    // no exception expects to be thrown
    assertThrows(Exception.class,
        () -> execSelectSql(batchTableEnv, "select * from t1", 10),
        "Exception should throw when querying non-exists table in batch mode");

    // case2: empty table without data files
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    StreamerUtil.initTableIfNotExists(conf);

    List<Row> rows2 = CollectionUtil.iteratorToList(batchTableEnv.executeSql("select * from t1").collect());
    assertRowsEquals(rows2, "[]");
  }

  @ParameterizedTest
  @EnumSource(value = ExecMode.class)
  void testWriteAndReadDebeziumJson(ExecMode execMode) throws Exception {
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("debezium_json.data")).toString();
    String sourceDDL = ""
        + "CREATE TABLE debezium_source(\n"
        + "  id INT NOT NULL PRIMARY KEY NOT ENFORCED,\n"
        + "  ts BIGINT,\n"
        + "  name STRING,\n"
        + "  description STRING,\n"
        + "  weight DOUBLE\n"
        + ") WITH (\n"
        + "  'connector' = 'filesystem',\n"
        + "  'path' = '" + sourcePath + "',\n"
        + "  'format' = 'debezium-json'\n"
        + ")";
    streamTableEnv.executeSql(sourceDDL);
    String hoodieTableDDL = sql("hoodie_sink")
        .field("id INT NOT NULL")
        .field("ts BIGINT")
        .field("name STRING")
        .field("weight DOUBLE")
        .pkField("id")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.READ_AS_STREAMING, execMode == ExecMode.STREAM)
        .option(FlinkOptions.PRE_COMBINE, true)
        .noPartition()
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into hoodie_sink select id, ts, name, weight from debezium_source";
    execInsertSql(streamTableEnv, insertInto);

    final String expected = "["
        + "+I[101, 1000, scooter, 3.140000104904175], "
        + "+I[102, 2000, car battery, 8.100000381469727], "
        + "+I[103, 3000, 12-pack drill bits, 0.800000011920929], "
        + "+I[104, 4000, hammer, 0.75], "
        + "+I[105, 5000, hammer, 0.875], "
        + "+I[106, 10000, hammer, 1.0], "
        + "+I[107, 11000, rocks, 5.099999904632568], "
        + "+I[108, 8000, jacket, 0.10000000149011612], "
        + "+I[109, 9000, spare tire, 22.200000762939453], "
        + "+I[110, 14000, jacket, 0.5]]";

    List<Row> result = execSelectSql(streamTableEnv, "select * from hoodie_sink", execMode);

    assertRowsEquals(result, expected);
  }

  @ParameterizedTest
  @MethodSource("indexAndPartitioningParams")
  void testBulkInsert(String indexType, boolean hiveStylePartitioning) {
    TableEnvironment tableEnv = batchTableEnv;
    // csv source
    String csvSourceDDL = TestConfigurations.getCsvSourceDDL("csv_source", "test_source_5.data");
    tableEnv.executeSql(csvSourceDDL);

    String hoodieTableDDL = sql("hoodie_sink")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, "bulk_insert")
        .option(FlinkOptions.WRITE_BULK_INSERT_SHUFFLE_INPUT, true)
        .option(FlinkOptions.INDEX_TYPE, indexType)
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    String insertInto = "insert into hoodie_sink select * from csv_source";
    execInsertSql(tableEnv, insertInto);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from hoodie_sink").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);
    // apply filters
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from hoodie_sink where uuid > 'id5'").execute().collect());
    assertRowsEquals(result2, "["
        + "+I[id6, Emma, 20, 1970-01-01T00:00:06, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]");
  }

  @Test
  void testBulkInsertNonPartitionedTable() {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, "bulk_insert")
        .noPartition()
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    final String insertInto1 = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1')";

    execInsertSql(tableEnv, insertInto1);

    final String insertInto2 = "insert into t1 values\n"
        + "('id1','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par2'),\n"
        + "('id1','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par3'),\n"
        + "('id1','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par4'),\n"
        + "('id1','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par5')";

    execInsertSql(tableEnv, insertInto2);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, "["
        + "+I[id1, Danny, 23, 1970-01-01T00:00:01, par1], "
        + "+I[id1, Stephen, 33, 1970-01-01T00:00:02, par2], "
        + "+I[id1, Julian, 53, 1970-01-01T00:00:03, par3], "
        + "+I[id1, Fabian, 31, 1970-01-01T00:00:04, par4], "
        + "+I[id1, Sophia, 18, 1970-01-01T00:00:05, par5]]", 3);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testAppendWrite(boolean clustering) {
    TableEnvironment tableEnv = streamTableEnv;
    // csv source
    String csvSourceDDL = TestConfigurations.getCsvSourceDDL("csv_source", "test_source_5.data");
    tableEnv.executeSql(csvSourceDDL);

    String hoodieTableDDL = sql("hoodie_sink")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, "insert")
        .option(FlinkOptions.INSERT_CLUSTER, clustering)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    String insertInto = "insert into hoodie_sink select * from csv_source";
    execInsertSql(tableEnv, insertInto);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from hoodie_sink").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);
    // apply filters
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from hoodie_sink where uuid > 'id5'").execute().collect());
    assertRowsEquals(result2, "["
        + "+I[id6, Emma, 20, 1970-01-01T00:00:06, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]");
  }

  @ParameterizedTest
  @EnumSource(value = ExecMode.class)
  void testWriteAndReadWithTimestampPartitioning(ExecMode execMode) {
    // can not read the hive style and timestamp based partitioning table
    // in batch mode, the code path in CopyOnWriteInputFormat relies on
    // the value on the partition path to recover the partition value,
    // but the date format has changed(milliseconds switch to hours).
    TableEnvironment tableEnv = execMode == ExecMode.BATCH ? batchTableEnv : streamTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .partitionField("ts") // use timestamp as partition path field
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);
    // apply filters
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where uuid > 'id5'").execute().collect());
    assertRowsEquals(result2, "["
        + "+I[id6, Emma, 20, 1970-01-01T00:00:06, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]");
  }

  @ParameterizedTest
  @ValueSource(strings = {FlinkOptions.PARTITION_FORMAT_DAY, FlinkOptions.PARTITION_FORMAT_DASHED_DAY})
  void testWriteAndReadWithDatePartitioning(String partitionFormat) {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .field("uuid varchar(20)")
        .field("name varchar(10)")
        .field("age int")
        .field("ts date")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.PARTITION_FORMAT, partitionFormat)
        .partitionField("ts") // use date as partition path field
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.INSERT_DATE_PARTITION_T1);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    String expected = "["
        + "+I[id1, Danny, 23, 1970-01-01], "
        + "+I[id2, Stephen, 33, 1970-01-01], "
        + "+I[id3, Julian, 53, 1970-01-01], "
        + "+I[id4, Fabian, 31, 1970-01-01], "
        + "+I[id5, Sophia, 18, 1970-01-01], "
        + "+I[id6, Emma, 20, 1970-01-01], "
        + "+I[id7, Bob, 44, 1970-01-01], "
        + "+I[id8, Han, 56, 1970-01-01]]";
    assertRowsEquals(result, expected);
  }

  @ParameterizedTest
  @ValueSource(strings = {"bulk_insert", "upsert"})
  void testWriteReadDecimals(String operation) {
    TableEnvironment tableEnv = batchTableEnv;
    String createTable = sql("decimals")
        .field("f0 decimal(3, 2)")
        .field("f1 decimal(10, 2)")
        .field("f2 decimal(20, 2)")
        .field("f3 decimal(38, 18)")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, operation)
        .option(FlinkOptions.PRECOMBINE_FIELD, "f1")
        .pkField("f0")
        .noPartition()
        .end();
    tableEnv.executeSql(createTable);

    String insertInto = "insert into decimals values\n"
        + "(1.23, 12345678.12, 12345.12, 123456789.12345)";
    execInsertSql(tableEnv, insertInto);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from decimals").execute().collect());
    assertRowsEquals(result1, "[+I[1.23, 12345678.12, 12345.12, 123456789.123450000000000000]]");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testIncrementalRead(HoodieTableType tableType) throws Exception {
    TableEnvironment tableEnv = batchTableEnv;
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.TABLE_NAME, "t1");
    conf.setString(FlinkOptions.TABLE_TYPE, tableType.name());

    // write 3 batches of data set
    TestData.writeData(TestData.dataSetInsert(1, 2), conf);
    TestData.writeData(TestData.dataSetInsert(3, 4), conf);
    TestData.writeData(TestData.dataSetInsert(5, 6), conf);

    String latestCommit = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_START_COMMIT, latestCommit)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, TestData.dataSetInsert(5, 6));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testReadWithWiderSchema(HoodieTableType tableType) throws Exception {
    TableEnvironment tableEnv = batchTableEnv;
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.TABLE_NAME, "t1");
    conf.setString(FlinkOptions.TABLE_TYPE, tableType.name());

    // write a batch of data set
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    String hoodieTableDDL = sql("t1")
        .field("uuid varchar(20)")
        .field("name varchar(10)")
        .field("age int")
        .field("salary double")
        .field("ts timestamp(3)")
        .field("`partition` varchar(10)")
        .pkField("uuid")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    final String expected = "["
        + "+I[id1, Danny, 23, null, 1970-01-01T00:00:00.001, par1], "
        + "+I[id2, Stephen, 33, null, 1970-01-01T00:00:00.002, par1], "
        + "+I[id3, Julian, 53, null, 1970-01-01T00:00:00.003, par2], "
        + "+I[id4, Fabian, 31, null, 1970-01-01T00:00:00.004, par2], "
        + "+I[id5, Sophia, 18, null, 1970-01-01T00:00:00.005, par3], "
        + "+I[id6, Emma, 20, null, 1970-01-01T00:00:00.006, par3], "
        + "+I[id7, Bob, 44, null, 1970-01-01T00:00:00.007, par4], "
        + "+I[id8, Han, 56, null, 1970-01-01T00:00:00.008, par4]]";
    assertRowsEquals(result, expected);
  }

  @ParameterizedTest
  @ValueSource(strings = {"insert", "upsert", "bulk_insert"})
  void testParquetComplexTypes(String operation) {
    TableEnvironment tableEnv = batchTableEnv;

    String hoodieTableDDL = sql("t1")
        .field("f_int int")
        .field("f_array array<varchar(10)>")
        .field("f_map map<varchar(20), int>")
        .field("f_row row(f_row_f0 int, f_row_f1 varchar(10))")
        .pkField("f_int")
        .noPartition()
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, operation)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.COMPLEX_TYPE_INSERT_T1);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    final String expected = "["
        + "+I[1, [abc1, def1], {abc1=1, def1=3}, +I[1, abc1]], "
        + "+I[2, [abc2, def2], {def2=3, abc2=1}, +I[2, abc2]], "
        + "+I[3, [abc3, def3], {def3=3, abc3=1}, +I[3, abc3]]]";
    assertRowsEquals(result, expected);
  }

  @ParameterizedTest
  @ValueSource(strings = {"insert", "upsert", "bulk_insert"})
  void testParquetComplexNestedRowTypes(String operation) {
    TableEnvironment tableEnv = batchTableEnv;

    String hoodieTableDDL = sql("t1")
        .field("f_int int")
        .field("f_array array<varchar(10)>")
        .field("int_array array<int>")
        .field("f_map map<varchar(20), int>")
        .field("f_row row(f_nested_array array<varchar(10)>, f_nested_row row(f_row_f0 int, f_row_f1 varchar(10)))")
        .pkField("f_int")
        .noPartition()
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, operation)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.COMPLEX_NESTED_ROW_TYPE_INSERT_T1);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    final String expected = "["
        + "+I[1, [abc1, def1], [1, 1], {abc1=1, def1=3}, +I[[abc1, def1], +I[1, abc1]]], "
        + "+I[2, [abc2, def2], [2, 2], {def2=3, abc2=1}, +I[[abc2, def2], +I[2, abc2]]], "
        + "+I[3, [abc3, def3], [3, 3], {def3=3, abc3=1}, +I[[abc3, def3], +I[3, abc3]]]]";
    assertRowsEquals(result, expected);
  }

  @ParameterizedTest
  @ValueSource(strings = {"insert", "upsert", "bulk_insert"})
  void testBuiltinFunctionWithCatalog(String operation) {
    TableEnvironment tableEnv = batchTableEnv;

    String hudiCatalogDDL = catalog("hudi_" + operation)
        .catalogPath(tempFile.getAbsolutePath())
        .end();

    tableEnv.executeSql(hudiCatalogDDL);
    tableEnv.executeSql("use catalog " + ("hudi_" + operation));

    String dbName = "hudi";
    tableEnv.executeSql("create database " + dbName);
    tableEnv.executeSql("use " + dbName);

    String hoodieTableDDL = sql("t1")
        .field("f_int int")
        .field("f_date DATE")
        .pkField("f_int")
        .partitionField("f_int")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath() + "/" + dbName + "/" + operation)
        .option(FlinkOptions.OPERATION, operation)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    String insertSql = "insert into t1 values (1, TO_DATE('2022-02-02')), (2, DATE '2022-02-02')";
    execInsertSql(tableEnv, insertSql);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    final String expected = "["
        + "+I[1, 2022-02-02], "
        + "+I[2, 2022-02-02]]";
    assertRowsEquals(result, expected);

    List<Row> partitionResult = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where f_int = 1").execute().collect());
    assertRowsEquals(partitionResult, "[+I[1, 2022-02-02]]");
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  private enum ExecMode {
    BATCH, STREAM
  }

  /**
   * Return test params => (execution mode, table type).
   */
  private static Stream<Arguments> executionModeAndTableTypeParams() {
    Object[][] data =
        new Object[][] {
            {ExecMode.BATCH, HoodieTableType.MERGE_ON_READ},
            {ExecMode.BATCH, HoodieTableType.COPY_ON_WRITE},
            {ExecMode.STREAM, HoodieTableType.MERGE_ON_READ},
            {ExecMode.STREAM, HoodieTableType.COPY_ON_WRITE}};
    return Stream.of(data).map(Arguments::of);
  }

  /**
   * Return test params => (execution mode, hive style partitioning).
   */
  private static Stream<Arguments> executionModeAndPartitioningParams() {
    Object[][] data =
        new Object[][] {
            {ExecMode.BATCH, false},
            {ExecMode.BATCH, true},
            {ExecMode.STREAM, false},
            {ExecMode.STREAM, true}};
    return Stream.of(data).map(Arguments::of);
  }

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

  /**
   * Return test params => (index type, hive style partitioning).
   */
  private static Stream<Arguments> indexAndPartitioningParams() {
    Object[][] data =
        new Object[][] {
            {"FLINK_STATE", false},
            {"FLINK_STATE", true},
            {"BUCKET", false},
            {"BUCKET", true}};
    return Stream.of(data).map(Arguments::of);
  }

  private void execInsertSql(TableEnvironment tEnv, String insert) {
    TableResult tableResult = tEnv.executeSql(insert);
    // wait to finish
    try {
      tableResult.getJobClient().get().getJobExecutionResult().get();
    } catch (InterruptedException | ExecutionException ex) {
      // ignored
    }
  }

  private List<Row> execSelectSql(TableEnvironment tEnv, String select, ExecMode execMode)
      throws TableNotExistException, InterruptedException {
    final String[] splits = select.split(" ");
    final String tableName = splits[splits.length - 1];
    switch (execMode) {
      case STREAM:
        return execSelectSql(tEnv, select, 10, tableName);
      case BATCH:
        return CollectionUtil.iterableToList(
            () -> tEnv.sqlQuery("select * from " + tableName).execute().collect());
      default:
        throw new AssertionError();
    }
  }

  private List<Row> execSelectSql(TableEnvironment tEnv, String select, long timeout)
      throws InterruptedException, TableNotExistException {
    return execSelectSql(tEnv, select, timeout, null);
  }

  private List<Row> execSelectSql(TableEnvironment tEnv, String select, long timeout, String sourceTable)
      throws InterruptedException, TableNotExistException {
    final String sinkDDL;
    if (sourceTable != null) {
      // use the source table schema as the sink schema if the source table was specified, .
      ObjectPath objectPath = new ObjectPath(tEnv.getCurrentDatabase(), sourceTable);
      TableSchema schema = tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTable(objectPath).getSchema();
      sinkDDL = TestConfigurations.getCollectSinkDDL("sink", schema);
    } else {
      sinkDDL = TestConfigurations.getCollectSinkDDL("sink");
    }
    return execSelectSql(tEnv, select, sinkDDL, timeout);
  }

  private List<Row> execSelectSql(TableEnvironment tEnv, String select, String sinkDDL, long timeout)
      throws InterruptedException {
    tEnv.executeSql("DROP TABLE IF EXISTS sink");
    tEnv.executeSql(sinkDDL);
    TableResult tableResult = tEnv.executeSql("insert into sink " + select);
    // wait for the timeout then cancels the job
    TimeUnit.SECONDS.sleep(timeout);
    tableResult.getJobClient().ifPresent(JobClient::cancel);
    tEnv.executeSql("DROP TABLE IF EXISTS sink");
    return CollectSinkTableFactory.RESULT.values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }
}
