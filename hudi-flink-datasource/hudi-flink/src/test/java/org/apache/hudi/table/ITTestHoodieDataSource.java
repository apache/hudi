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

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.bucket.partition.PartitionBucketIndexUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.catalog.HoodieCatalogTestUtils;
import org.apache.hudi.table.catalog.HoodieHiveCatalog;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestSQL;
import org.apache.hudi.utils.TestTableEnvs;
import org.apache.hudi.utils.TestUtils;
import org.apache.hudi.utils.factory.CollectSinkTableFactory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.utils.TestConfigurations.catalog;
import static org.apache.hudi.utils.TestConfigurations.getCollectSinkDDL;
import static org.apache.hudi.utils.TestConfigurations.sql;
import static org.apache.hudi.utils.TestData.array;
import static org.apache.hudi.utils.TestData.assertRowsEquals;
import static org.apache.hudi.utils.TestData.assertRowsEqualsUnordered;
import static org.apache.hudi.utils.TestData.map;
import static org.apache.hudi.utils.TestData.row;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IT cases for Hoodie table source and sink.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestHoodieDataSource {
  private TableEnvironment streamTableEnv;
  private TableEnvironment batchTableEnv;

  @BeforeEach
  void beforeEach() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    streamTableEnv = TableEnvironmentImpl.create(settings);
    streamTableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    Configuration execConf = streamTableEnv.getConfig().getConfiguration();
    execConf.setString("execution.checkpointing.interval", "2s");
    // configure not to retry after failure
    execConf.setString("restart-strategy", "fixed-delay");
    execConf.setString("restart-strategy.fixed-delay.attempts", "0");

    batchTableEnv = TestTableEnvs.getBatchTableEnv();
    batchTableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
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
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, false)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_START_COMMIT, firstCommit)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    List<Row> rows = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", TestData.DATA_SET_SOURCE_INSERT.size());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);

    // insert another batch of data
    execInsertSql(streamTableEnv, insertInto);
    List<Row> rows2 = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", TestData.DATA_SET_SOURCE_INSERT.size());
    assertRowsEquals(rows2, TestData.DATA_SET_SOURCE_INSERT);

    streamTableEnv.getConfig().getConfiguration()
        .setString("table.dynamic-table-options.enabled", "true");
    // specify the start commit as earliest
    List<Row> rows3 = execSelectSqlWithExpectedNum(streamTableEnv,
        "select * from t1/*+options('read.start-commit'='earliest')*/", TestData.DATA_SET_SOURCE_INSERT.size());
    assertRowsEquals(rows3, TestData.DATA_SET_SOURCE_INSERT);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieCDCSupplementalLoggingMode.class)
  void testStreamReadFromSpecifiedCommitWithChangelog(HoodieCDCSupplementalLoggingMode mode) throws Exception {
    streamTableEnv.getConfig().getConfiguration()
        .setString("table.dynamic-table-options.enabled", "true");
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, false)
        .option(FlinkOptions.CDC_ENABLED, true)
        .option(FlinkOptions.SUPPLEMENTAL_LOGGING_MODE, mode.name())
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    String firstCommit = TestUtils.getFirstCompleteInstant(tempFile.getAbsolutePath());
    List<Row> rows = execSelectSqlWithExpectedNum(streamTableEnv,
        "select * from t1/*+options('read.start-commit'='" + firstCommit + "')*/", TestData.DATA_SET_SOURCE_INSERT.size());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);

    // insert another batch of data
    execInsertSql(streamTableEnv, TestSQL.UPDATE_INSERT_T1);
    List<Row> rows2 = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", TestData.DATA_SET_SOURCE_CHANGELOG.size());
    assertRowsEquals(rows2, TestData.DATA_SET_SOURCE_CHANGELOG);

    // specify the start commit as earliest
    List<Row> rows3 = execSelectSqlWithExpectedNum(streamTableEnv,
        "select * from t1/*+options('read.start-commit'='earliest')*/", TestData.DATA_SET_SOURCE_MERGED.size());
    assertRowsEquals(rows3, TestData.DATA_SET_SOURCE_MERGED);
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
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, false)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), false)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    // reading from the latest commit instance.
    List<Row> rows = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", TestData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT.size());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT);

    // insert another batch of data
    execInsertSql(streamTableEnv, insertInto);
    List<Row> rows2 = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", TestData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT.size());
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
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, false)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_START_COMMIT, specifiedCommit)
        .end();
    streamTableEnv.executeSql(createHoodieTable2);
    List<Row> rows = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t2", TestData.DATA_SET_SOURCE_MERGED.size());
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
  void testStreamWriteBatchReadOptimized() throws Exception {
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
        // disable the metadata table because
        // the lock conflicts resolution takes time
        .option(FlinkOptions.METADATA_ENABLED, false)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    // give some buffer time for finishing the async compaction tasks
    TimeUnit.SECONDS.sleep(5);
    List<Row> rows = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());

    // the test is flaky based on whether the first compaction is pending when
    // scheduling the 2nd compaction.
    // see details in CompactionPlanOperator#scheduleCompaction.
    if (rows.size() < TestData.DATA_SET_SOURCE_INSERT.size()) {
      assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT_FIRST_COMMIT);
    } else {
      assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
    }
  }

  @Test
  void testStreamWriteBatchReadOptimizedWithoutCompaction() {
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .option(FlinkOptions.QUERY_TYPE, FlinkOptions.QUERY_TYPE_READ_OPTIMIZED)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    final String insertInto = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1')";
    execInsertSql(streamTableEnv, insertInto);

    List<Row> rows = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    assertTrue(rows.isEmpty());
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
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, 1)
        .option(FlinkOptions.COMPACTION_TASKS, 1)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    String instant = TestUtils.getNthCompleteInstant(new StoragePath(tempFile.toURI()), 2, HoodieTimeline.DELTA_COMMIT_ACTION);

    streamTableEnv.getConfig().getConfiguration()
        .setString("table.dynamic-table-options.enabled", "true");
    final String query = String.format("select * from t1/*+ options('read.start-commit'='%s')*/", instant);
    List<Row> rows = execSelectSqlWithExpectedNum(streamTableEnv, query, TestData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT.size());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT);
  }

  @Test
  void testAppendWriteReadSkippingClustering() throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source", 4);
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, "insert")
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, true)
        .option(FlinkOptions.CLUSTERING_ASYNC_ENABLED, true)
        .option(FlinkOptions.CLUSTERING_DELTA_COMMITS, 1)
        .option(FlinkOptions.CLUSTERING_TASKS, 1)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    String instant = TestUtils.getNthCompleteInstant(new StoragePath(tempFile.toURI()), 2, HoodieTimeline.COMMIT_ACTION);

    streamTableEnv.getConfig().getConfiguration()
        .setString("table.dynamic-table-options.enabled", "true");
    final String query = String.format("select * from t1/*+ options('read.start-commit'='%s')*/", instant);
    List<Row> rows = execSelectSqlWithExpectedNum(streamTableEnv, query, TestData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT.size());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT);
  }

  @Test
  void testAppendWriteWithClusteringBatchRead() throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source", 4);
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, "insert")
        .option(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, true)
        .option(FlinkOptions.CLUSTERING_ASYNC_ENABLED, true)
        .option(FlinkOptions.CLUSTERING_DELTA_COMMITS, 2)
        .option(FlinkOptions.CLUSTERING_TASKS, 1)
        .option(FlinkOptions.CLEAN_RETAIN_COMMITS, 1)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    streamTableEnv.getConfig().getConfiguration()
        .setString("table.dynamic-table-options.enabled", "true");
    final String query = String.format("select * from t1/*+ options('read.start-commit'='%s')*/",
        FlinkOptions.START_COMMIT_EARLIEST);

    List<Row> rows = execSelectSql(streamTableEnv, query);
    // batch read will not lose data when cleaned clustered files.
    assertRowsEquals(rows, CollectionUtils.combine(TestData.DATA_SET_SOURCE_INSERT_FIRST_COMMIT,
        TestData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT));
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
            .getInstantsAsStream().anyMatch(instant -> instant.getAction().equals("clean")),
        "some commits should be cleaned");
  }

  @Test
  void testBatchWriteWithCleaning() {
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.CLEAN_RETAIN_COMMITS, 1)
        .end();
    batchTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1')";
    execInsertSql(batchTableEnv, insertInto);
    execInsertSql(batchTableEnv, insertInto);
    execInsertSql(batchTableEnv, insertInto);
    Configuration defaultConf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    Map<String, String> options1 = new HashMap<>(defaultConf.toMap());
    options1.put(FlinkOptions.TABLE_NAME.key(), "t1");
    Configuration conf = Configuration.fromMap(options1);
    HoodieTimeline timeline = StreamerUtil.createMetaClient(conf).getActiveTimeline();
    assertTrue(timeline.filterCompletedInstants()
            .getInstants().stream().anyMatch(instant -> instant.getAction().equals("clean")),
        "some commits should be cleaned");
  }

  @Test
  void testStreamReadWithDeletes() throws Exception {
    // create filesystem table named source

    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    conf.set(FlinkOptions.CHANGELOG_ENABLED, true);

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
        + "  'connector' = '" + CollectSinkTableFactory.FACTORY_ID + "',\n"
        + "  'sink-expected-row-num' = '2'"
        + ")";
    List<Row> result = execSelectSqlWithExpectedNum(streamTableEnv, "select name, sum(age) from t1 group by name", sinkDDL);
    final String expected = "[+I(+I[Danny, 24]), +I(+I[Stephen, 34])]";
    assertRowsEquals(result, expected, true);
  }

  @Test
  void testDataSkippingWithRecordLevelIndex() throws Exception {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.METADATA_ENABLED, true)
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true)
        .option(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), true)
        .option(FlinkOptions.TABLE_TYPE, COPY_ON_WRITE)
        .end();
    tableEnv.executeSql(hoodieTableDDL);
    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where uuid = 'id1'").execute().collect());
    assertRowsEquals(result1, "[+I[id1, Danny, 23, 1970-01-01T00:00:01, par1]]");
    // apply filters
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where uuid in ('id7', 'id8')").execute().collect());
    assertRowsEquals(result2, "["
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]");
    List<Row> result3 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where uuid = 'id1' or uuid = 'id7' or uuid = 'id8'").execute().collect());
    assertRowsEquals(result3, "["
        + "+I[id1, Danny, 23, 1970-01-01T00:00:01, par1], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]");
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndBooleanTrueFalseParams")
  void testReadWithPartitionStatsPruning(HoodieTableType tableType, boolean hiveStylePartitioning) throws Exception {
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.METADATA_ENABLED, true)
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), false)
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
    conf.set(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true);
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning);
    // write one commit
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    List<String> sqls =
        Arrays.asList(
            // no filter
            "select * from t1",
            // filter by partition stats pruner only
            "select * from t1 where uuid > 'id5' and age > 15",
            // filter by partition stats pruner and dynamic partition pruner
            "select * from t1 where uuid > 'id5' and age > 15 and `partition` > 'par3'");
    List<String> expectResults =
        Arrays.asList(
            "[+I[id1, Danny, 23, 1970-01-01T00:00:00.001, par1], "
                + "+I[id2, Stephen, 33, 1970-01-01T00:00:00.002, par1], "
                + "+I[id3, Julian, 53, 1970-01-01T00:00:00.003, par2], "
                + "+I[id4, Fabian, 31, 1970-01-01T00:00:00.004, par2], "
                + "+I[id5, Sophia, 18, 1970-01-01T00:00:00.005, par3], "
                + "+I[id6, Emma, 20, 1970-01-01T00:00:00.006, par3], "
                + "+I[id7, Bob, 44, 1970-01-01T00:00:00.007, par4], "
                + "+I[id8, Han, 56, 1970-01-01T00:00:00.008, par4]]",
            "[+I[id6, Emma, 20, 1970-01-01T00:00:00.006, par3], "
                + "+I[id7, Bob, 44, 1970-01-01T00:00:00.007, par4], "
                + "+I[id8, Han, 56, 1970-01-01T00:00:00.008, par4]]",
            "[+I[id7, Bob, 44, 1970-01-01T00:00:00.007, par4], "
                + "+I[id8, Han, 56, 1970-01-01T00:00:00.008, par4]]");
    List<Integer> expectedNums = Arrays.asList(8, 3, 2);
    for (int i = 0; i < sqls.size(); i++) {
      List<Row> result = execSelectSqlWithExpectedNum(streamTableEnv, sqls.get(i), expectedNums.get(i));
      assertRowsEquals(result, expectResults.get(i));
    }
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndBooleanTrueFalseParams")
  void testStreamReadFilterByPartition(HoodieTableType tableType, boolean hiveStylePartitioning) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning);

    // write one commit
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 2)
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, false)
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);

    final String expected = "["
        + "+I(+I[id1, Danny, 23, 1970-01-01T00:00:00.001, par1]), "
        + "+I(+I[id2, Stephen, 33, 1970-01-01T00:00:00.002, par1])]";
    List<Row> result = execSelectSqlWithExpectedNum(streamTableEnv,
        "select * from t1 where `partition`='par1'", 2);
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

    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);
    List<Row> rows = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", TestData.DATA_SET_SOURCE_INSERT.size());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
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
  @MethodSource("tableTypeAndBooleanTrueFalseParams")
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
  @MethodSource("tableTypeAndBooleanTrueFalseParams")
  void testWriteAndReadWithProctimeSequenceWithTsColumnExisting(HoodieTableType tableType, boolean hiveStylePartitioning) {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .field("uuid varchar(20)")
        .field("name varchar(10)")
        .field("age int")
        .field("ts timestamp(3)") // use the default precombine field 'ts'
        .field("`partition` varchar(10)")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning)
        .option(FlinkOptions.ORDERING_FIELDS, FlinkOptions.NO_PRE_COMBINE)
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
        .option(FlinkOptions.TABLE_TYPE, tableType)
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
  @MethodSource("tableTypeAndBooleanTrueFalseParams")
  void testBatchModeUpsert(HoodieTableType tableType, boolean hiveStylePartitioning) {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
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
  @EnumSource(value = HoodieTableType.class)
  void testLookupJoin(HoodieTableType tableType) {
    TableEnvironment tableEnv = streamTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath() + "/t1")
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    String hoodieTableDDL2 = sql("t2")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath() + "/t2")
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .end();
    tableEnv.executeSql(hoodieTableDDL2);

    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    tableEnv.executeSql("create view t1_view as select *,"
        + "PROCTIME() as proc_time from t1");

    // Join two hudi tables with the same data
    String sql = "insert into t2 select b.* from t1_view o "
        + "       join t1/*+ OPTIONS('lookup.join.cache.ttl'= '2 day') */  "
        + "       FOR SYSTEM_TIME AS OF o.proc_time AS b on o.uuid = b.uuid";
    execInsertSql(tableEnv, sql);
    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t2").execute().collect());

    assertRowsEquals(result, TestData.DATA_SET_SOURCE_INSERT);
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
        + "  'read.streaming.enabled' = '" + streaming + "',\n"
        + "  'read.streaming.skip_compaction' = 'false'\n"
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

    List<Row> result = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", "t1", 8);
    assertRowsEquals(result, expected);

    // insert another batch of data
    execInsertSql(streamTableEnv, insertInto);
    List<Row> result2 = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", "t1", 8);
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
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, false)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 values\n"
        + "(1,'Danny',TIMESTAMP '2021-12-01 01:02:01.100001'),\n"
        + "(2,'Stephen',TIMESTAMP '2021-12-02 03:04:02.200002'),\n"
        + "(3,'Julian',TIMESTAMP '2021-12-03 13:14:03.300003'),\n"
        + "(4,'Fabian',TIMESTAMP '2021-12-04 15:16:04.400004'),\n"
        + "(5,'Tom',TIMESTAMP '2721-12-04 15:16:04.500005')";
    execInsertSql(streamTableEnv, insertInto);

    final String expected = "["
        + "+I[1, Danny, 2021-12-01T01:02:01.100001], "
        + "+I[2, Stephen, 2021-12-02T03:04:02.200002], "
        + "+I[3, Julian, 2021-12-03T13:14:03.300003], "
        + "+I[4, Fabian, 2021-12-04T15:16:04.400004], "
        + "+I[5, Tom, 2721-12-04T15:16:04.500005]]";

    List<Row> result = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", "t1", 5);
    assertRowsEquals(result, expected);

    // insert another batch of data
    execInsertSql(streamTableEnv, insertInto);
    List<Row> result2 = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", "t1", 5);
    assertRowsEquals(result2, expected);
  }

  @ParameterizedTest
  @MethodSource("indexAndTableTypeParams")
  void testInsertOverwrite(String indexType, HoodieTableType tableType) {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.INDEX_TYPE, indexType)
        .option(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), false)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    // overwrite partition 'par1' and increase in age by 1
    final String insertInto1 = "insert overwrite t1 partition(`partition`='par1') values\n"
        + "('id1','Danny',24,TIMESTAMP '1970-01-01 00:00:01'),\n"
        + "('id2','Stephen',34,TIMESTAMP '1970-01-01 00:00:02')\n";

    execInsertSql(tableEnv, insertInto1);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT_OVERWRITE);

    // execute the same statement again and check the result
    execInsertSql(tableEnv, insertInto1);

    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result2, TestData.DATA_SET_SOURCE_INSERT_OVERWRITE);

    // overwrite the dynamic partition
    final String insertInto3 = "insert overwrite t1 /*+ OPTIONS('write.partition.overwrite.mode'='dynamic') */ values\n"
        + "('id1','Danny',24,TIMESTAMP '1970-01-01 00:00:01', 'par1'),\n"
        + "('id2','Stephen',34,TIMESTAMP '1970-01-01 00:00:02', 'par2')\n";

    execInsertSql(tableEnv, insertInto3);

    List<Row> result3 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result3, TestData.DATA_SET_SOURCE_INSERT_OVERWRITE_DYNAMIC_PARTITION);

    // execute the same statement again and check the result
    execInsertSql(tableEnv, insertInto3);
    assertRowsEquals(result3, TestData.DATA_SET_SOURCE_INSERT_OVERWRITE_DYNAMIC_PARTITION);

    // overwrite the whole table
    final String insertInto4 = "insert overwrite t1 values\n"
        + "('id1','Danny',24,TIMESTAMP '1970-01-01 00:00:01', 'par1'),\n"
        + "('id2','Stephen',34,TIMESTAMP '1970-01-01 00:00:02', 'par2')\n";

    execInsertSql(tableEnv, insertInto4);

    List<Row> result4 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    final String expected = "["
        + "+I[id1, Danny, 24, 1970-01-01T00:00:01, par1], "
        + "+I[id2, Stephen, 34, 1970-01-01T00:00:02, par2]]";
    assertRowsEquals(result4, expected);

    // execute the same statement again and check the result
    execInsertSql(tableEnv, insertInto4);
    List<Row> result5 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result5, expected);
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
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, false)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_START_COMMIT, "earliest")
        .option(FlinkOptions.WRITE_BATCH_SIZE, 0.00001)
        .noPartition()
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    // reading from the earliest commit instance.
    List<Row> rows = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", TestData.DATA_SET_SOURCE_INSERT.size());
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

    // ValidationException expects to be thrown
    assertThrows(ValidationException.class,
        () -> execSelectSql(batchTableEnv, "select * from t1"),
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
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, false)
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

    List<Row> result =
        execMode == ExecMode.STREAM
            ? execSelectSqlWithExpectedNum(streamTableEnv, "select * from hoodie_sink", "hoodie_sink", 10)
            : execSelectSql(streamTableEnv, "select * from hoodie_sink");

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

  @ParameterizedTest
  @MethodSource("testBulkInsertWithPartitionBucketIndexParams")
  void testBulkInsertWithPartitionBucketIndex(String operationType, String tableType) throws IOException {
    TableEnvironment tableEnv = batchTableEnv;
    // csv source
    String csvSourceDDL = TestConfigurations.getCsvSourceDDL("csv_source", "test_source_5.data");
    tableEnv.executeSql(csvSourceDDL);
    String catalogName = "hudi_" + operationType;
    String hudiCatalogDDL = catalog(catalogName)
        .catalogPath(tempFile.getAbsolutePath())
        .end();

    tableEnv.executeSql(hudiCatalogDDL);
    String dbName = "hudi";
    tableEnv.executeSql("create database " + catalogName + "." + dbName);
    String basePath = tempFile.getAbsolutePath() + "/hudi/hoodie_sink";

    String hoodieTableDDL = sql(catalogName + ".hudi.hoodie_sink")
        .option(FlinkOptions.PATH, basePath)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.OPERATION, operationType)
        .option(FlinkOptions.WRITE_BULK_INSERT_SHUFFLE_INPUT, true)
        .option(FlinkOptions.INDEX_TYPE, "BUCKET")
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, "true")
        .option(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, "1")
        .option(FlinkOptions.BUCKET_INDEX_PARTITION_RULE, "regex")
        .option(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS, "partition=(par1|par2),2")
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    String insertInto = "insert into " + catalogName + ".hudi.hoodie_sink select * from csv_source";
    execInsertSql(tableEnv, insertInto);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from " + catalogName + ".hudi.hoodie_sink").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);
    // apply filters
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from " + catalogName + ".hudi.hoodie_sink where uuid > 'id5'").execute().collect());
    assertRowsEquals(result2, "["
        + "+I[id6, Emma, 20, 1970-01-01T00:00:06, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]");
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(basePath, new org.apache.hadoop.conf.Configuration());
    List<String> actual = PartitionBucketIndexUtils.getAllFileIDWithPartition(metaClient);

    // based on expression partition=(par1|par2),2 and default bucket number 1
    // par1 and par2 have two buckets.
    // par3 and par4 have one bucket.
    ArrayList<String> expected = new ArrayList<>();
    expected.add("partition=par1" + "00000000");
    expected.add("partition=par1" + "00000001");
    expected.add("partition=par2" + "00000000");
    expected.add("partition=par2" + "00000001");
    expected.add("partition=par3" + "00000000");
    expected.add("partition=par4" + "00000000");

    assertEquals(expected.stream().sorted().collect(Collectors.toList()), actual.stream().sorted().collect(Collectors.toList()));
  }

  @Test
  void tesQueryWithPartitionBucketIndexPruning() {
    String operationType = "upsert";
    String tableType = "MERGE_ON_READ";
    TableEnvironment tableEnv = batchTableEnv;
    // csv source
    String csvSourceDDL = TestConfigurations.getCsvSourceDDL("csv_source", "test_source_5.data");
    tableEnv.executeSql(csvSourceDDL);
    String catalogName = "hudi_" + operationType;
    String hudiCatalogDDL = catalog(catalogName)
        .catalogPath(tempFile.getAbsolutePath())
        .end();

    tableEnv.executeSql(hudiCatalogDDL);
    String dbName = "hudi";
    tableEnv.executeSql("create database " + catalogName + "." + dbName);
    String basePath = tempFile.getAbsolutePath() + "/hudi/hoodie_sink";

    String hoodieTableDDL = sql(catalogName + ".hudi.hoodie_sink")
        .option(FlinkOptions.PATH, basePath)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.OPERATION, operationType)
        .option(FlinkOptions.WRITE_BULK_INSERT_SHUFFLE_INPUT, true)
        .option(FlinkOptions.INDEX_TYPE, "BUCKET")
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, "true")
        .option(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, "1")
        .option(FlinkOptions.BUCKET_INDEX_PARTITION_RULE, "regex")
        .option(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS, "partition=(par1|par2),2")
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    String insertInto = "insert into " + catalogName + ".hudi.hoodie_sink select * from csv_source";
    execInsertSql(tableEnv, insertInto);

    List<Row> result1 = execSelectSql(tableEnv, "select * from " + catalogName + ".hudi.hoodie_sink");
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);
    // apply filters which will prune based on partition level bucket index
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from " + catalogName + ".hudi.hoodie_sink where uuid = 'id5'").execute().collect());
    assertRowsEquals(result2, "[+I[id5, Sophia, 18, 1970-01-01T00:00:05, par3]]");
  }

  @Test
  void testBulkInsertWithSortByRecordKey() {
    TableEnvironment tableEnv = batchTableEnv;

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, "bulk_insert")
        .option(FlinkOptions.WRITE_BULK_INSERT_SHUFFLE_INPUT, true)
        .option(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT, true)
        .option(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT_BY_RECORD_KEY, true)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    final String insertInto = "insert into t1 values\n"
        + "('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),\n"
        + "('id1','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par1')";

    execInsertSql(tableEnv, insertInto);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, "["
        + "+I[id1, Julian, 53, 1970-01-01T00:00:03, par1], "
        + "+I[id2, Stephen, 33, 1970-01-01T00:00:02, par1]]", 4);
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
    String sourceDDL = TestConfigurations.getFileSourceDDL("source");
    tableEnv.executeSql(sourceDDL);

    String hoodieTableDDL = sql("hoodie_sink")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, "insert")
        .option(FlinkOptions.INSERT_CLUSTER, clustering)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    String insertInto = "insert into hoodie_sink select * from source";
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
  @MethodSource("executionModeAndPartitioningParams")
  void testWriteAndReadWithTimestampPartitioning(ExecMode execMode, boolean hiveStylePartitioning) {
    TableEnvironment tableEnv = execMode == ExecMode.BATCH ? batchTableEnv : streamTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning)
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

  @Test
  void testMergeOnReadCompactionWithTimestampPartitioning() {
    TableEnvironment tableEnv = batchTableEnv;

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, 1)
        .option(FlinkOptions.COMPACTION_TASKS, 1)
        .partitionField("ts")
        .end();
    tableEnv.executeSql(hoodieTableDDL);
    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    List<Row> rows = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());

    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
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
        .option(FlinkOptions.ORDERING_FIELDS, "f1")
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
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());

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
  void testReadChangelogIncremental(HoodieTableType tableType) throws Exception {
    TableEnvironment tableEnv = streamTableEnv;
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    conf.set(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, false);
    conf.set(FlinkOptions.READ_CDC_FROM_CHANGELOG, false); // calculate the changes on the fly
    conf.set(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, true);  // for batch upsert
    conf.set(FlinkOptions.CDC_ENABLED, true);

    // write 3 batches of the same data set
    TestData.writeDataAsBatch(TestData.dataSetInsert(1, 2), conf);
    TestData.writeDataAsBatch(TestData.dataSetInsert(1, 2), conf);
    TestData.writeDataAsBatch(TestData.dataSetInsert(1, 2), conf);

    String latestCommit = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.COMPACTION_ASYNC_ENABLED, false)
        .option(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, false)
        .option(FlinkOptions.READ_CDC_FROM_CHANGELOG, false)
        .option(FlinkOptions.READ_START_COMMIT, latestCommit)
        .option(FlinkOptions.CDC_ENABLED, true)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.dataSetUpsert(2, 1));

    // write another 10 batches of dataset
    for (int i = 0; i < 10; i++) {
      TestData.writeDataAsBatch(TestData.dataSetInsert(1, 2), conf);
    }

    String firstCommit = TestUtils.getFirstCompleteInstant(tempFile.getAbsolutePath());
    final String query = String.format("select count(*) from t1/*+ options('read.start-commit'='%s')*/", firstCommit);
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery(query).execute().collect());
    assertRowsEquals(result2.subList(result2.size() - 2, result2.size()), "[-U[1], +U[2]]");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testIncrementalReadArchivedCommits(HoodieTableType tableType) throws Exception {
    TableEnvironment tableEnv = batchTableEnv;
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.set(FlinkOptions.ARCHIVE_MIN_COMMITS, 4);
    conf.set(FlinkOptions.ARCHIVE_MAX_COMMITS, 5);
    conf.set(FlinkOptions.CLEAN_RETAIN_COMMITS, 3);
    conf.setString("hoodie.commits.archival.batch", "1");

    // write 10 batches of data set
    for (int i = 0; i < 20; i += 2) {
      List<RowData> dataset = TestData.dataSetInsert(i + 1, i + 2);
      TestData.writeData(dataset, conf);
    }

    String secondArchived = TestUtils.getNthArchivedInstant(tempFile.getAbsolutePath(), 1);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_START_COMMIT, secondArchived)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, TestData.dataSetInsert(3, 4, 5, 6, 7, 8, 9, 10,
        11, 12, 13, 14, 15, 16, 17, 18, 19, 20));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testReadWithWiderSchema(HoodieTableType tableType) throws Exception {
    TableEnvironment tableEnv = batchTableEnv;
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());

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
    List<Row> expected = Arrays.asList(
        row(1, array("abc1", "def1"), map("abc1", 1, "def1", 3), row(1, "abc1")),
        row(2, array("abc2", "def2"), map("abc2", 1, "def2", 3), row(2, "abc2")),
        row(3, array("abc3", "def3"), map("abc3", 1, "def3", 3), row(3, "abc3")));
    assertRowsEqualsUnordered(result, expected);
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
    List<Row> expected = Arrays.asList(
        row(1, array("abc1", "def1"), array(1, 1), map("abc1", 1, "def1", 3), row(array("abc1", "def1"), row(1, "abc1"))),
        row(2, array("abc2", "def2"), array(2, 2), map("abc2", 1, "def2", 3), row(array("abc2", "def2"), row(2, "abc2"))),
        row(3, array("abc3", "def3"), array(3, 3), map("abc3", 1, "def3", 3), row(array("abc3", "def3"), row(3, "abc3"))));
    assertRowsEqualsUnordered(result, expected);
  }

  @ParameterizedTest
  @ValueSource(strings = {"insert", "upsert", "bulk_insert"})
  void testParquetArrayMapOfRowTypes(String operation) {
    TableEnvironment tableEnv = batchTableEnv;

    String hoodieTableDDL = sql("t1")
        .field("f_int int")
        .field("f_array array<row(f_array_row_f0 varchar(10), f_array_row_f1 int)>")
        .field("f_map map<varchar(20), row(f_map_row_f0 int, f_map_row_f1 varchar(10))>")
        .pkField("f_int")
        .noPartition()
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, operation)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.ARRAY_MAP_OF_ROW_TYPE_INSERT_T1);

    tableEnv.executeSql("ALTER TABLE t1 MODIFY (\n"
        + "    f_array array<row(f_array_row_f0 varchar(10), f_array_row_f1 int, f_array_row_f2 double)>,\n"
        + "    f_map map<varchar(20), row(f_map_row_f0 int, f_map_row_f1 varchar(10), f_map_row_f2 double)>\n"
        + ");");

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    List<Row> expected = Arrays.asList(
        row(1, array(row("abc11", 11, null), row("abc12", 12, null), row("abc13", 13, null)), map("abc11", row(11, "def11", null), "abc12", row(12, "def12", null), "abc13", row(13, "def13", null))),
        row(2, array(row("abc21", 21, null), row("abc22", 22, null), row("abc23", 23, null)), map("abc21", row(21, "def21", null), "abc22", row(22, "def22", null), "abc23", row(23, "def23", null))),
        row(3, array(row("abc31", 31, null), row("abc32", 32, null), row("abc33", 33, null)), map("abc31", row(31, "def31", null), "abc32", row(32, "def32", null), "abc33", row(33, "def33", null))));
    assertRowsEqualsUnordered(expected, result);
  }

  @ParameterizedTest
  @ValueSource(strings = {"insert", "upsert", "bulk_insert"})
  void testParquetNullChildColumnsRowTypes(String operation) {
    TableEnvironment tableEnv = batchTableEnv;

    String hoodieTableDDL = sql("t1")
        .field("f_int int")
        .field("f_row row(f_row_f0 int, f_row_f1 varchar(10))")
        .pkField("f_int")
        .noPartition()
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, operation)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.NULL_CHILD_COLUMNS_ROW_TYPE_INSERT_T1);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    final String expected = "["
        + "+I[1, +I[null, abc1]], "
        + "+I[2, +I[2, null]], "
        + "+I[3, null]]";
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

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testWriteAndReadWithDataSkipping(HoodieTableType tableType) {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.METADATA_ENABLED, true)
        .option("hoodie.metadata.index.column.stats.enable", true)
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);
    // apply filters
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where uuid > 'id5' and age > 20").execute().collect());
    assertRowsEquals(result2, "["
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]");
    // filter by timestamp
    List<Row> result3 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where ts > TIMESTAMP '1970-01-01 00:00:05'").execute().collect());
    assertRowsEquals(result3, "["
        + "+I[id6, Emma, 20, 1970-01-01T00:00:06, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]");
    // filter by in expression
    List<Row> result4 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where uuid in ('id6', 'id7', 'id8')").execute().collect());
    assertRowsEquals(result4, "["
        + "+I[id6, Emma, 20, 1970-01-01T00:00:06, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testDataSkippingOnMetadataColumns(HoodieTableType tableType) {
    String hoodieTableDDL = "create table t1(\n"
        + "  _hoodie_commit_time STRING METADATA VIRTUAL,\n"
        + "  _hoodie_commit_seqno STRING METADATA VIRTUAL,\n"
        + "  _hoodie_record_key STRING METADATA VIRTUAL,\n"
        + "  _hoodie_partition_path STRING METADATA VIRTUAL,\n"
        + "  _hoodie_file_name STRING METADATA VIRTUAL,\n"
        + "  uuid varchar(20),\n"
        + "  name varchar(10),\n"
        + "  age int,\n"
        + "  ts timestamp(3),\n"
        + "  `partition` varchar(20),\n"
        + "  PRIMARY KEY(uuid) NOT ENFORCED\n"
        + ")\n"
        + "PARTITIONED BY (`partition`)\n"
        + "with (\n"
        + "  'connector' = 'hudi',\n"
        + "  'read.data.skipping.enabled' = 'true',\n"
        + "  'hoodie.metadata.index.column.stats.enable' = 'true',\n"
        + "  'path' = '" + tempFile.getAbsolutePath() + "',\n"
        + "  'table.type' = '" + tableType + "'\n"
        + ")";
    batchTableEnv.executeSql(hoodieTableDDL);

    // virtual columns will be ignored for schema validating during insert
    String insertInto = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),"
        + "('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),"
        + "('id3','Julian',43,TIMESTAMP '1970-01-01 00:00:03','par1')";
    execInsertSql(batchTableEnv, insertInto);

    String firstCommitTime = TestUtils.getLastCompleteInstant(tempFile.toURI().toString());

    // virtual columns will be ignored for schema validating during insert
    insertInto = "insert into t1 values\n"
        + "('id4','Bob',23,TIMESTAMP '1970-01-01 00:00:01','par1'),"
        + "('id5','Lily',33,TIMESTAMP '1970-01-01 00:00:02','par1'),"
        + "('id6','Han',43,TIMESTAMP '1970-01-01 00:00:03','par1')";
    execInsertSql(batchTableEnv, insertInto);

    // select metadata and data columns
    List<Row> rows = CollectionUtil.iterableToList(
        () -> batchTableEnv.sqlQuery("select uuid, _hoodie_record_key, name, age, ts, `partition` from t1 where _hoodie_commit_time <= '" + firstCommitTime + "'").execute().collect());

    assertRowsEquals(rows,
        "[+I[id1, id1, Danny, 23, 1970-01-01T00:00:01, par1], "
            + "+I[id2, id2, Stephen, 33, 1970-01-01T00:00:02, par1], "
            + "+I[id3, id3, Julian, 43, 1970-01-01T00:00:03, par1]]");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testDataSkippingByFilteringFileSlice(HoodieTableType tableType) {
    // Case: column for different files inside one file slice can be different,
    // so if any file in the file slice satisfy the predicate based on column stats,
    // then the file slice should be read.
    // E.g., query predicate is age <> '25', base file contains: {key=k1, orderingVal=1, age=23},
    // log file contains: {key=k1, orderingVal=2, age=25}, then the file slice should be read.
    TableEnvironment tableEnv = batchTableEnv;
    String path = tempFile.getAbsolutePath();
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, path)
        .option(FlinkOptions.METADATA_ENABLED, true)
        .option("hoodie.metadata.index.column.stats.enable", true)
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, 1)
        .option(FlinkOptions.COMPACTION_TASKS, 1)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where age <> 25 and `partition` = 'par1'").execute().collect());
    assertRowsEquals(result1, "["
        + "+I[id1, Danny, 23, 1970-01-01T00:00:01, par1], "
        + "+I[id2, Stephen, 33, 1970-01-01T00:00:02, par1]]");

    batchTableEnv.executeSql("drop table t1");

    hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, path)
        .option(FlinkOptions.METADATA_ENABLED, true)
        .option("hoodie.metadata.index.column.stats.enable", true)
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    final String INSERT_T2 = "insert into t1 values\n"
        + "('id1','Danny',25,TIMESTAMP '1970-01-01 00:01:01','par1')\n";
    execInsertSql(tableEnv, INSERT_T2);
    result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where age <> 25 and `partition` = 'par1'").execute().collect());
    assertRowsEquals(result1, "[+I[id2, Stephen, 33, 1970-01-01T00:00:02, par1]]");
  }

  @Test
  void testPredicateForBaseFileWithMor() {
    // Case:
    // * records in base file can not survive from the predicate
    // * records in log file can survive from the predicate
    // * records in base file have higher ordering value
    // E.g., base file: (uuid:'k1', age: 23, ts: 1003)
    // log file: (uuid: 'k1', age: 25, ts: 1001)
    // query filter: age = 25;
    // Then the expected result should be empty, but if predicate age = 25 is pushed down
    // into the parquet reader, the result would be wrong as (uuid: 'k1', age: 25, ts: 1001)
    TableEnvironment tableEnv = batchTableEnv;
    String path = tempFile.getAbsolutePath();
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, path)
        .option(FlinkOptions.TABLE_TYPE, MERGE_ON_READ)
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, 1)
        .option(FlinkOptions.COMPACTION_TASKS, 1)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    final String INSERT_T1 = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 01:00:01','par1')\n";
    execInsertSql(tableEnv, INSERT_T1);

    batchTableEnv.executeSql("drop table t1");

    hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, path)
        .option(FlinkOptions.METADATA_ENABLED, true)
        .option(FlinkOptions.TABLE_TYPE, MERGE_ON_READ)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    final String INSERT_T2 = "insert into t1 values\n"
        + "('id1','Danny',25,TIMESTAMP '1970-01-01 00:00:01','par1')\n";
    execInsertSql(tableEnv, INSERT_T2);
    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where age = 25 and `partition` = 'par1'").execute().collect());
    assertRowsEquals(result1, "[]");
  }

  @Test
  void testParquetLogBlockDataSkipping() {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.METADATA_ENABLED, true)
        .option("hoodie.metadata.index.column.stats.enable", true)
        .option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet")
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true)
        .option(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);
    // apply filters
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where uuid > 'id5' and age > 20").execute().collect());
    assertRowsEquals(result2, "["
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]");
    // filter by timestamp
    List<Row> result3 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where ts > TIMESTAMP '1970-01-01 00:00:05'").execute().collect());
    assertRowsEquals(result3, "["
        + "+I[id6, Emma, 20, 1970-01-01T00:00:06, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]");
    // filter by in expression
    List<Row> result4 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where uuid in ('id6', 'id7', 'id8')").execute().collect());
    assertRowsEquals(result4, "["
        + "+I[id6, Emma, 20, 1970-01-01T00:00:06, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]");
  }

  @Disabled("for being flaky by HUDI-7174")
  @Test
  void testMultipleLogBlocksWithDataSkipping() {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.METADATA_ENABLED, true)
        .option("hoodie.metadata.index.column.stats.enable", true)
        .option("hoodie.metadata.index.column.stats.file.group.count", 2)
        .option("hoodie.metadata.index.column.stats.column.list", "ts")
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true)
        .option(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ)
        .option("hoodie.logfile.data.block.max.size", 1)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.INSERT_SAME_KEY_T1);

    // apply filters
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where ts > TIMESTAMP '1970-01-01 00:00:04'").execute().collect());
    assertRowsEquals(result2, "[+I[id1, Danny, 23, 1970-01-01T00:00:05, par1]]");
  }

  @Test
  void testEagerFlushWithDataSkipping() {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.METADATA_ENABLED, true)
        .option("hoodie.metadata.index.column.stats.enable", true)
        .option("hoodie.metadata.index.column.stats.file.group.count", 2)
        .option("hoodie.metadata.index.column.stats.column.list", "ts")
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true)
        .option(FlinkOptions.WRITE_BATCH_SIZE, 0.00001)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.INSERT_SAME_KEY_T1);

    // apply filters
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where ts > TIMESTAMP '1970-01-01 00:00:04'").execute().collect());
    assertRowsEquals(result2, "[+I[id1, Danny, 23, 1970-01-01T00:00:05, par1]]");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testEnableMetadataTableOnExistingTable(HoodieTableType tableType) {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.METADATA_ENABLED, false)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    // upsert 5 times so there could be multiple files under one partition
    IntStream.range(0, 5).forEach(i -> execInsertSql(tableEnv, TestSQL.INSERT_T1));

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);

    // enables the metadata table and validate
    execInsertSql(tableEnv, TestSQL.insertT1WithSQLHint("/*+options('metadata.enabled'='true')*/"));
    // check the existence of metadata table
    assertTrue(StreamerUtil.tableExists(HoodieTableMetadata.getMetadataTableBasePath(tempFile.getAbsolutePath()), new org.apache.hadoop.conf.Configuration()),
        "Metadata table should exist");
    // validate the data set with table metadata
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);

    // disable the metadata table again and validate
    execInsertSql(tableEnv, TestSQL.INSERT_T1);
    assertFalse(StreamerUtil.tableExists(HoodieTableMetadata.getMetadataTableBasePath(tempFile.getAbsolutePath()), new org.apache.hadoop.conf.Configuration()),
        "Metadata table should be deleted");
    // validate the data set without table metadata
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testBucketPruning(HoodieTableType tableType) {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.INDEX_TYPE, "BUCKET")
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);
    // apply filters
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where uuid = 'id5' and age < 20").execute().collect());
    assertRowsEquals(result2, "[+I[id5, Sophia, 18, 1970-01-01T00:00:05, par3]]");
    // filter by timestamp
    List<Row> result3 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where uuid = 'id7' and ts > TIMESTAMP '1970-01-01 00:00:05'").execute().collect());
    assertRowsEquals(result3, "[+I[id7, Bob, 44, 1970-01-01T00:00:07, par4]]");
    // filter by in expression
    List<Row> result4 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where name in ('Danny', 'Julian') and uuid='id1'").execute().collect());
    assertRowsEquals(result4, "[+I[id1, Danny, 23, 1970-01-01T00:00:01, par1]]");
  }

  @Test
  void testBuiltinFunctionWithHMSCatalog() {
    TableEnvironment tableEnv = batchTableEnv;

    HoodieHiveCatalog hoodieCatalog = HoodieCatalogTestUtils.createHiveCatalog("hudi_catalog");

    tableEnv.registerCatalog("hudi_catalog", hoodieCatalog);
    tableEnv.executeSql("use catalog hudi_catalog");

    String dbName = "hudi";
    tableEnv.executeSql("create database " + dbName);
    tableEnv.executeSql("use " + dbName);

    String hoodieTableDDL = sql("t1")
        .field("f_int int")
        .field("f_date DATE")
        .field("f_par string")
        .pkField("f_int")
        .partitionField("f_par")
        .option(FlinkOptions.RECORD_KEY_FIELD, "f_int")
        .option(FlinkOptions.ORDERING_FIELDS, "f_date")
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    String insertSql = "insert into t1 values (1, TO_DATE('2022-02-02'), '1'), (2, DATE '2022-02-02', '2')";
    execInsertSql(tableEnv, insertSql);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    final String expected = "["
        + "+I[1, 2022-02-02, 1], "
        + "+I[2, 2022-02-02, 2]]";
    assertRowsEquals(result, expected);

    List<Row> partitionResult = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where f_par = '1'").execute().collect());
    assertRowsEquals(partitionResult, "[+I[1, 2022-02-02, 1]]");
  }

  @Test
  void testWriteReadWithComputedColumns() {
    TableEnvironment tableEnv = batchTableEnv;
    String createTable = sql("t1")
        .field("f0 int")
        .field("f1 varchar(10)")
        .field("f2 bigint")
        .field("f3 as f0 + f2")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.ORDERING_FIELDS, "f1")
        .pkField("f0")
        .noPartition()
        .end();
    tableEnv.executeSql(createTable);

    String insertInto = "insert into t1 values\n"
        + "(1, 'abc', 2)";
    execInsertSql(tableEnv, insertInto);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, "[+I[1, abc, 2, 3]]");

    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select f3 from t1").execute().collect());
    assertRowsEquals(result2, "[+I[3]]");
  }

  @Test
  void testWriteReadWithComputedColumnsInTheMiddle() {
    TableEnvironment tableEnv = batchTableEnv;
    String createTable = sql("t1")
        .field("f0 int")
        .field("f1 int")
        .field("f2 as f0 + f1")
        .field("f3 varchar(10)")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.ORDERING_FIELDS, "f1")
        .pkField("f0")
        .noPartition()
        .end();
    tableEnv.executeSql(createTable);

    String insertInto = "insert into t1(f0, f1, f3) values\n"
        + "(1, 2, 'abc')";
    execInsertSql(tableEnv, insertInto);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, "[+I[1, 2, 3, abc]]");

    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select f2 from t1").execute().collect());
    assertRowsEquals(result2, "[+I[3]]");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testWriteReadWithLocalTimestamp(HoodieTableType tableType) {
    TableEnvironment tableEnv = batchTableEnv;
    tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
    String createTable = sql("t1")
        .field("f0 int")
        .field("f1 varchar(10)")
        .field("f2 TIMESTAMP_LTZ(3)")
        .field("f4 TIMESTAMP_LTZ(6)")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.ORDERING_FIELDS, "f1")
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .pkField("f0")
        .noPartition()
        .end();
    tableEnv.executeSql(createTable);

    String insertInto = "insert into t1 values\n"
        + "(1, 'abc', TIMESTAMP '1970-01-01 08:00:01', TIMESTAMP '1970-01-01 08:00:02'),\n"
        + "(2, 'def', TIMESTAMP '1970-01-01 08:00:03', TIMESTAMP '1970-01-01 08:00:04')";
    execInsertSql(tableEnv, insertInto);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    final String expected = "["
        + "+I[1, abc, 1970-01-01T00:00:01Z, 1970-01-01T00:00:02Z], "
        + "+I[2, def, 1970-01-01T00:00:03Z, 1970-01-01T00:00:04Z]]";
    assertRowsEquals(result, expected);
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndBooleanTrueFalseParams")
  void testWriteReadWithTimestampWithoutTZ(HoodieTableType tableType, boolean readUtcTimezone) {
    TableEnvironment tableEnv = batchTableEnv;
    tableEnv.getConfig().setLocalTimeZone(ZoneId.of("America/Los_Angeles"));
    String createTable = sql("t1")
        .field("f0 int")
        .field("f1 varchar(10)")
        .field("f2 TIMESTAMP(3)")
        .field("f3 TIMESTAMP(6)")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.ORDERING_FIELDS, "f1")
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.WRITE_UTC_TIMEZONE, false)
        .option(FlinkOptions.READ_UTC_TIMEZONE, readUtcTimezone)
        .pkField("f0")
        .noPartition()
        .end();
    tableEnv.executeSql(createTable);

    long epochMillis = 0L;
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    String insertInto = "insert into t1 values\n"
        + "(1"
        + ", 'abc'"
        + ", TIMESTAMP '" + formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis + 1000), ZoneId.systemDefault())) + "'"
        + ", TIMESTAMP '" + formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis + 2000), ZoneId.systemDefault())) + "'),\n"
        + "(2"
        + ", 'def'"
        + ", TIMESTAMP '" + formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis + 3000), ZoneId.systemDefault())) + "'"
        + ", TIMESTAMP '" + formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis + 4000), ZoneId.systemDefault())) + "')";
    execInsertSql(tableEnv, insertInto);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    final ZoneId expectedZoneId = readUtcTimezone ? ZoneId.of("UTC") : ZoneId.systemDefault();
    final String expected = "["
        + "+I[1"
        + ", abc"
        + ", " + formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis + 1000), expectedZoneId))
        + ", " + formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis + 2000), expectedZoneId)) + "], "
        + "+I[2"
        + ", def"
        + ", " + formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis + 3000), expectedZoneId))
        + ", " + formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis + 4000), expectedZoneId)) + "]]";

    assertRowsEquals(result, expected);
  }

  @ParameterizedTest
  @MethodSource("tableTypeQueryTypeNumInsertAndCompactionDeltaCommitsParams")
  void testReadMetaFields(HoodieTableType tableType, String queryType, int numInsertBatches, int compactionDeltaCommits) throws Exception {
    String path = tempFile.getAbsolutePath();
    String hoodieTableDDL = sql("t1")
        .field("id int")
        .field("name varchar(10)")
        .field("ts timestamp(6)")
        .field("`partition` varchar(10)")
        .pkField("id")
        .partitionField("partition")
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.QUERY_TYPE, queryType)
        .option(FlinkOptions.COMPACTION_ASYNC_ENABLED, true)
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, compactionDeltaCommits)
        .option(FlinkOptions.PATH, path)
        .end();
    batchTableEnv.executeSql(hoodieTableDDL);

    final String[] insertInto = new String[] {
        "insert into t1 values(1,'Danny',TIMESTAMP '2021-12-01 01:02:01.100001', 'par1')",
        "insert into t1 values(2,'Stephen',TIMESTAMP '2021-12-02 03:04:02.200002', 'par2')",
        "insert into t1 values(3,'Julian',TIMESTAMP '2021-12-03 13:14:03.300003', 'par3')"};

    // Queries without meta fields.
    String[] template1 = new String[] {
        "+I[1, Danny, 2021-12-01T01:02:01.100001, par1]",
        ", +I[2, Stephen, 2021-12-02T03:04:02.200002, par2]",
        ", +I[3, Julian, 2021-12-03T13:14:03.300003, par3]"
    };

    // Meta field '_hoodie_commit_time' in the first position.
    String[] template2 = new String[] {
        "+I[%s, 1, par1, 1, Danny, 2021-12-01T01:02:01.100001, par1]",
        ", +I[%s, 2, par2, 2, Stephen, 2021-12-02T03:04:02.200002, par2]",
        ", +I[%s, 3, par3, 3, Julian, 2021-12-03T13:14:03.300003, par3]"
    };

    // Meta fields at random positions.
    String[] template3 = new String[] {
        "+I[1, %s, Danny, 1, 2021-12-01T01:02:01.100001, par1, par1]",
        ", +I[2, %s, Stephen, 2, 2021-12-02T03:04:02.200002, par2, par2]",
        ", +I[3, %s, Julian, 3, 2021-12-03T13:14:03.300003, par3, par3]"
    };

    StringBuilder expected1 = new StringBuilder();
    StringBuilder expected2 = new StringBuilder();
    StringBuilder expected3 = new StringBuilder();

    expected1.append("[");
    expected2.append("[");
    expected3.append("[");
    for (int i = 0; i < numInsertBatches; i++) {
      execInsertSql(batchTableEnv, insertInto[i]);
      String commitTime = tableType.equals(HoodieTableType.MERGE_ON_READ)
          ? TestUtils.getLastDeltaCompleteInstant(path) : TestUtils.getLastCompleteInstant(path);
      expected1.append(template1[i]);
      expected2.append(String.format(template2[i], commitTime));
      expected3.append(String.format(template3[i], commitTime));
    }
    expected1.append("]");
    expected2.append("]");
    expected3.append("]");
    String readHoodieTableDDL;
    batchTableEnv.executeSql("drop table t1");
    readHoodieTableDDL = sql("t1")
        .field("id int")
        .field("name varchar(10)")
        .field("ts timestamp(6)")
        .field("`partition` varchar(10)")
        .pkField("id")
        .partitionField("partition")
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.QUERY_TYPE, queryType)
        .option(FlinkOptions.COMPACTION_ASYNC_ENABLED, true)
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, compactionDeltaCommits)
        .option(FlinkOptions.PATH, path)
        .end();
    batchTableEnv.executeSql(readHoodieTableDDL);

    List<Row> result = execSelectSql(batchTableEnv, "select * from t1");
    assertRowsEquals(result, expected1.toString());

    batchTableEnv.executeSql("drop table t1");
    readHoodieTableDDL = sql("t1")
        .field("_hoodie_commit_time string")
        .field("_hoodie_record_key string")
        .field("_hoodie_partition_path string")
        .field("id int")
        .field("name varchar(10)")
        .field("ts timestamp(6)")
        .field("`partition` varchar(10)")
        .pkField("id")
        .partitionField("partition")
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.QUERY_TYPE, queryType)
        .option(FlinkOptions.COMPACTION_ASYNC_ENABLED, true)
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, compactionDeltaCommits)
        .option(FlinkOptions.PATH, path)
        .end();
    batchTableEnv.executeSql(readHoodieTableDDL);

    result = execSelectSql(batchTableEnv, "select * from t1");
    assertRowsEquals(result, expected2.toString());

    batchTableEnv.executeSql("drop table t1");
    readHoodieTableDDL = sql("t1")
        .field("id int")
        .field("_hoodie_commit_time string")
        .field("name varchar(10)")
        .field("_hoodie_record_key string")
        .field("ts timestamp(6)")
        .field("_hoodie_partition_path string")
        .field("`partition` varchar(10)")
        .pkField("id")
        .partitionField("partition")
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.QUERY_TYPE, queryType)
        .option(FlinkOptions.COMPACTION_ASYNC_ENABLED, true)
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, compactionDeltaCommits)
        .option(FlinkOptions.PATH, path)
        .end();
    batchTableEnv.executeSql(readHoodieTableDDL);

    result = execSelectSql(batchTableEnv, "select * from t1");
    assertRowsEquals(result, expected3.toString());

  }

  @ParameterizedTest
  @MethodSource("tableTypeAndBooleanTrueFalseParams")
  void testDynamicPartitionPrune(HoodieTableType tableType, boolean hiveStylePartitioning) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning);

    // write the first commit
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 2)
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, false)
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);

    // launch a streaming query
    TableResult tableResult = submitSelectSql(streamTableEnv,
        "select uuid, name, age, ts, `partition` as part from t1 where `partition` > 'par4'",
        TestConfigurations.getCollectSinkDDLWithExpectedNum("sink", TestData.DATA_SET_INSERT_SEPARATE_PARTITION.size()));
    // write second commit
    TestData.writeData(TestData.DATA_SET_INSERT_SEPARATE_PARTITION, conf);
    // stop the streaming query and get data
    List<Row> actualResult = fetchResultWithExpectedNum(streamTableEnv, tableResult);
    assertRowsEquals(actualResult, TestData.DATA_SET_INSERT_SEPARATE_PARTITION);
  }

  @ParameterizedTest
  @MethodSource("indexAndTableTypeParams")
  void testUpdateDelete(String indexType, HoodieTableType tableType) {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.INDEX_TYPE, indexType)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    // update EQ(IN)
    final String update1 = "update t1 set age=18 where uuid in('id1', 'id2')";

    execInsertSql(tableEnv, update1);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    List<RowData> expected1 = TestData.update(TestData.DATA_SET_SOURCE_INSERT, 2, 18, 0, 1);
    assertRowsEquals(result1, expected1);

    // update GT(>)
    final String update2 = "update t1 set age=19 where uuid > 'id5'";

    execInsertSql(tableEnv, update2);

    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    List<RowData> expected2 = TestData.update(expected1, 2, 19, 5, 6, 7);
    assertRowsEquals(result2, expected2);

    // delete EQ(=)
    final String update3 = "delete from t1 where uuid = 'id1'";

    execInsertSql(tableEnv, update3);

    List<Row> result3 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    List<RowData> expected3 = TestData.delete(expected2, 0);
    assertRowsEquals(result3, expected3);

    // delete LTE(<=)
    final String update4 = "delete from t1 where uuid <= 'id5'";

    execInsertSql(tableEnv, update4);

    List<Row> result4 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    List<RowData> expected4 = TestData.delete(expected3, 0, 1, 2, 3);
    assertRowsEquals(result4, expected4);
  }

  @ParameterizedTest
  @MethodSource("parametersForMetaColumnsSkip")
  void testWriteWithoutMetaColumns(HoodieTableType tableType, WriteOperationType operation) throws Exception {
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false")
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    streamTableEnv.executeSql("drop table t1");
    hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    List<Row> rows = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", TestData.DATA_SET_SOURCE_INSERT.size());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
  }

  @Test
  void testReadWithParquetPredicatePushDown() {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1").option(FlinkOptions.PATH, tempFile.getAbsolutePath()).end();
    tableEnv.executeSql(hoodieTableDDL);
    execInsertSql(tableEnv, TestSQL.INSERT_T1);
    // apply filters to push down predicates
    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where uuid > 'id2' and age > 30 and ts > '1970-01-01 00:00:04'").execute().collect());
    assertRowsEquals(result, "["
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]");
  }

  @ParameterizedTest
  @MethodSource("indexAndPartitioningParams")
  void testWriteMultipleCommitWithDifferentLogBlockType(String indexType, boolean hiveStylePartitioning) throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    // insert first batch of data with parquet log block
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ)
        .option(FlinkOptions.INDEX_TYPE, indexType)
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning)
        .option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet")
        .option(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), false)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    streamTableEnv.executeSql("drop table t1");

    // insert second batch of data with avro log block
    hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ)
        .option(FlinkOptions.INDEX_TYPE, indexType)
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST)
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning)
        .option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "avro")
        .option(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), false)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    // reading from the earliest
    List<Row> rows = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", TestData.DATA_SET_SOURCE_INSERT.size());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
  }

  @ParameterizedTest
  @ValueSource(strings = {"FLINK_STATE", "BUCKET"})
  void testRowDataWriteModeWithParquetLogFormat(String index) throws Exception {
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    // insert first batch of data with rowdata mode writing disabled
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ)
        .option(FlinkOptions.INDEX_TYPE, index)
        .option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet")
        .option(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), false)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    // reading from the earliest
    List<Row> rows = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", TestData.DATA_SET_SOURCE_INSERT.size());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
  }

  @Test
  void testBatchInsertWithAdaptiveSchedulerDisabled() {
    // set scheduler type as Default to disable adaptive scheduler
    batchTableEnv.getConfig().getConfiguration().set(
        JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Default);
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, false)
        .option(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), false)
        .end();
    batchTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:00.001','par1')";
    execInsertSql(batchTableEnv, insertInto);

    List<Row> rows = CollectionUtil.iteratorToList(batchTableEnv.executeSql("select * from t1").collect());
    assertRowsEquals(rows, TestData.DATA_SET_SINGLE_INSERT);
  }

  @Test
  void testStreamWriteAndReadWithUpgrade() throws Exception {
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    // init and write data with table version SIX
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ)
        .option(FlinkOptions.WRITE_TABLE_VERSION, HoodieTableVersion.SIX.versionCode() + "")
        .option(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), false)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    streamTableEnv.executeSql("drop table t1");

    hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ)
        .option(FlinkOptions.WRITE_TABLE_VERSION, HoodieTableVersion.EIGHT.versionCode() + "")
        .option(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), false)
        .end();

    // write another batch of data with table version EIGHT
    streamTableEnv.executeSql(hoodieTableDDL);
    insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    List<Row> rows = execSelectSql(streamTableEnv, "select * from t1", 10);
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testWriteWithTimelineServerBasedMarker(HoodieTableType tableType) {
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(HoodieWriteConfig.MARKERS_TYPE.key(), MarkerType.TIMELINE_SERVER_BASED.name())
        .end();
    batchTableEnv.executeSql(hoodieTableDDL);

    execInsertSql(batchTableEnv, TestSQL.INSERT_T1);
    List<Row> rows = CollectionUtil.iteratorToList(batchTableEnv.executeSql("select * from t1").collect());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
  }

  @ParameterizedTest
  @MethodSource("catalogTypeAndTableTypeParams")
  void testReadMetadataColumns(String catalogType, HoodieTableType hoodieTableType) {
    AbstractCatalog catalog = null;
    switch (catalogType) {
      case "dfs":
        catalog = HoodieCatalogTestUtils.createHoodieCatalog(tempFile.getAbsolutePath());
        break;
      case "hms":
        catalog = HoodieCatalogTestUtils.createHiveCatalog("hudi_catalog");
        break;
      default:
    }
    if (catalog != null) {
      streamTableEnv.registerCatalog("hudi_catalog", catalog);
      streamTableEnv.executeSql("use catalog hudi_catalog");
    }

    String hoodieTableDDL = "create table t1(\n"
        + "  _hoodie_commit_time STRING METADATA VIRTUAL,\n"
        + "  _hoodie_commit_seqno STRING METADATA VIRTUAL,\n"
        + "  _hoodie_record_key STRING METADATA VIRTUAL,\n"
        + "  _hoodie_partition_path STRING METADATA VIRTUAL,\n"
        + "  _hoodie_file_name STRING METADATA VIRTUAL,\n"
        + "  uuid varchar(20),\n"
        + "  name varchar(10),\n"
        + "  age int,\n"
        + "  ts timestamp(3),\n"
        + "  `partition` varchar(20),\n"
        + "  PRIMARY KEY(uuid) NOT ENFORCED\n"
        + ")\n"
        + "PARTITIONED BY (`partition`)\n"
        + "with (\n"
        + "  'connector' = 'hudi',\n"
        + "  'path' = '" + tempFile.getAbsolutePath() + "',\n"
        + "  'table.type' = '" + hoodieTableType + "'\n"
        + ")";
    streamTableEnv.executeSql(hoodieTableDDL);

    // virtual columns will be ignored for schema validating during insert
    final String insertInto = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),"
        + "('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),"
        + "('id3','Julian',43,TIMESTAMP '1970-01-01 00:00:03','par1')";
    execInsertSql(streamTableEnv, insertInto);

    // select data columns
    List<Row> rows = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select uuid, name, age, ts, `partition` from t1").execute().collect());
    assertRowsEquals(rows,
        "[+I[id1, Danny, 23, 1970-01-01T00:00:01, par1], "
            + "+I[id2, Stephen, 33, 1970-01-01T00:00:02, par1], "
            + "+I[id3, Julian, 43, 1970-01-01T00:00:03, par1]]");

    // select metadata columns
    rows = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select _hoodie_commit_time, _hoodie_commit_seqno, _hoodie_record_key, _hoodie_partition_path, _hoodie_file_name from t1").execute().collect());
    rows.forEach(row -> IntStream.range(0, 5).forEach(idx -> assertNotNull(row.getField(idx))));

    // select metadata and data columns
    rows = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select uuid, _hoodie_record_key, name, age, ts, _hoodie_partition_path, `partition` from t1").execute().collect());
    assertRowsEquals(rows,
        "[+I[id1, id1, Danny, 23, 1970-01-01T00:00:01, par1, par1], "
            + "+I[id2, id2, Stephen, 33, 1970-01-01T00:00:02, par1, par1], "
            + "+I[id3, id3, Julian, 43, 1970-01-01T00:00:03, par1, par1]]");
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
  private static Stream<Arguments> catalogTypeAndTableTypeParams() {
    Object[][] data =
        new Object[][] {
            {"memory", HoodieTableType.MERGE_ON_READ},
            {"memory", HoodieTableType.COPY_ON_WRITE},
            {"dfs", HoodieTableType.MERGE_ON_READ},
            {"dfs", HoodieTableType.COPY_ON_WRITE},
            {"hms", HoodieTableType.MERGE_ON_READ},
            {"hms", HoodieTableType.COPY_ON_WRITE}};
    return Stream.of(data).map(Arguments::of);
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
   * Return test params => (HoodieTableType, true/false).
   */
  private static Stream<Arguments> tableTypeAndBooleanTrueFalseParams() {
    Object[][] data =
        new Object[][] {
            {HoodieTableType.COPY_ON_WRITE, false},
            {HoodieTableType.COPY_ON_WRITE, true},
            {HoodieTableType.MERGE_ON_READ, false},
            {HoodieTableType.MERGE_ON_READ, true}};
    return Stream.of(data).map(Arguments::of);
  }

  public static List<Arguments> testBulkInsertWithPartitionBucketIndexParams() {
    return asList(
        Arguments.of("bulk_insert", COPY_ON_WRITE.name()),
        Arguments.of("bulk_insert", MERGE_ON_READ.name()),
        Arguments.of("upsert", MERGE_ON_READ.name()),
        Arguments.of("upsert", MERGE_ON_READ.name())
    );
  }

  /**
   * Return test params => (HoodieTableType, query type, num insert batches, num compaction delta commits).
   */
  private static Stream<Arguments> tableTypeQueryTypeNumInsertAndCompactionDeltaCommitsParams() {
    return Arrays.stream(new Object[][] {
        {HoodieTableType.COPY_ON_WRITE, FlinkOptions.QUERY_TYPE_INCREMENTAL, 1, 1},
        {HoodieTableType.COPY_ON_WRITE, FlinkOptions.QUERY_TYPE_READ_OPTIMIZED, 1, 1},
        {HoodieTableType.MERGE_ON_READ, FlinkOptions.QUERY_TYPE_SNAPSHOT, 1, 1},
        {HoodieTableType.MERGE_ON_READ, FlinkOptions.QUERY_TYPE_SNAPSHOT, 1, 3},
        {HoodieTableType.MERGE_ON_READ, FlinkOptions.QUERY_TYPE_SNAPSHOT, 3, 2}
    }).map(Arguments::of);
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

  /**
   * Return test params => (index type, table type).
   */
  private static Stream<Arguments> indexAndTableTypeParams() {
    Object[][] data =
        new Object[][] {
            {"FLINK_STATE", HoodieTableType.COPY_ON_WRITE},
            {"FLINK_STATE", HoodieTableType.MERGE_ON_READ},
            {"BUCKET", HoodieTableType.COPY_ON_WRITE},
            {"BUCKET", HoodieTableType.MERGE_ON_READ}};
    return Stream.of(data).map(Arguments::of);
  }

  private static Stream<Arguments> parametersForMetaColumnsSkip() {
    Object[][] data =
        new Object[][] {
            {HoodieTableType.COPY_ON_WRITE, WriteOperationType.INSERT},
            {HoodieTableType.MERGE_ON_READ, WriteOperationType.UPSERT}
        };
    return Stream.of(data).map(Arguments::of);
  }

  private void execInsertSql(TableEnvironment tEnv, String insert) {
    TableResult tableResult = tEnv.executeSql(insert);
    // wait to finish
    try {
      tableResult.await();
    } catch (InterruptedException | ExecutionException ex) {
      // ignored
    }
  }

  /**
   * Use TableResult#collect() to collect results directly for bounded source.
   */
  private List<Row> execSelectSql(TableEnvironment tEnv, String select) {
    return CollectionUtil.iterableToList(
        () -> tEnv.sqlQuery(select).execute().collect());
  }

  /**
   * Use CollectTableSink to collect results with expected row number.
   */
  private List<Row> execSelectSqlWithExpectedNum(TableEnvironment tEnv, String select, int expectedNum) throws Exception {
    return execSelectSqlWithExpectedNum(tEnv, select, null, expectedNum);
  }

  /**
   * Use CollectTableSink to collect results with expected row number.
   */
  private List<Row> execSelectSqlWithExpectedNum(TableEnvironment tEnv, String select, String sourceTable, int expectedNum)
      throws Exception {
    final String sinkDDL;
    if (sourceTable != null) {
      // use the source table schema as the sink schema if the source table was specified.
      ObjectPath objectPath = new ObjectPath(tEnv.getCurrentDatabase(), sourceTable);
      Schema schema = tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTable(objectPath).getUnresolvedSchema();
      sinkDDL = TestConfigurations.getCollectSinkDDLWithExpectedNum("sink", schema, expectedNum);
    } else {
      sinkDDL = TestConfigurations.getCollectSinkDDLWithExpectedNum("sink", expectedNum);
    }
    return execSelectSqlWithExpectedNum(tEnv, select, sinkDDL);
  }

  /**
   * Use CollectTableSink to collect results with expected row number.
   */
  private List<Row> execSelectSqlWithExpectedNum(TableEnvironment tEnv, String select, String sinkDDL) {
    TableResult tableResult = submitSelectSql(tEnv, select, sinkDDL);
    return fetchResultWithExpectedNum(tEnv, tableResult);
  }

  private TableResult submitSelectSql(TableEnvironment tEnv, String select, String sinkDDL) {
    tEnv.executeSql("DROP TABLE IF EXISTS sink");
    tEnv.executeSql(sinkDDL);
    TableResult tableResult = tEnv.executeSql("insert into sink " + select);
    return tableResult;
  }

  private List<Row> execSelectSql(TableEnvironment tEnv, String select, long timeout) throws InterruptedException {
    TableResult tableResult = submitSelectSql(tEnv, select, getCollectSinkDDL("sink"));
    TimeUnit.SECONDS.sleep(timeout);
    // wait for the timeout then cancels the job
    tableResult.getJobClient().ifPresent(JobClient::cancel);
    tEnv.executeSql("DROP TABLE IF EXISTS sink");
    return CollectSinkTableFactory.RESULT.values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  private List<Row> fetchResultWithExpectedNum(TableEnvironment tEnv, TableResult tableResult) {
    try {
      // wait the continuous streaming query to be terminated by forced exception with expected row number
      // and max waiting timeout is 30s
      tableResult.await(30, TimeUnit.SECONDS);
    } catch (Throwable e) {
      ExceptionUtils.assertThrowable(e, CollectSinkTableFactory.SuccessException.class);
    }
    tEnv.executeSql("DROP TABLE IF EXISTS sink");
    return CollectSinkTableFactory.RESULT.values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }
}
