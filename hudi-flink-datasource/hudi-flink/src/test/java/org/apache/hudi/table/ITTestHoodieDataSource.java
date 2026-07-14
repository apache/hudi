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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.partition.PartitionBucketIndexUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.sink.buffer.BufferMemoryType;
import org.apache.hudi.sink.buffer.BufferType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
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

import lombok.extern.slf4j.Slf4j;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IT cases for Hoodie table source and sink.
 */
@ExtendWith(FlinkMiniCluster.class)
@Slf4j
public class ITTestHoodieDataSource {
  // A streaming read collected via CollectTableSink is terminated by a forced SuccessException once it
  // reaches its expected row count. A benign teardown race (see isAcceptableTerminalFailure) can instead
  // close the source stream mid-read and terminate the job before all rows are emitted, leaving an
  // incomplete result. Re-reading from the same (already committed) table is idempotent, so retry a few
  // times before giving up. See submitAndFetchWithRetry.
  private static final int MAX_STREAM_READ_ATTEMPTS = 3;

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
  @MethodSource("tableTypeAndBooleanTrueFalseParams")
  void testStreamWriteAndReadFromSpecifiedCommit(HoodieTableType tableType, boolean useSourceV2) throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, false)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_START_COMMIT, firstCommit)
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
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
  @MethodSource("cdcSupplementalLoggingModeWithSourceV2")
  void testStreamReadFromSpecifiedCommitWithChangelog(HoodieCDCSupplementalLoggingMode mode, boolean useSourceV2) throws Exception {
    streamTableEnv.getConfig().getConfiguration()
        .setString("table.dynamic-table-options.enabled", "true");
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, false)
        .option(FlinkOptions.CDC_ENABLED, true)
        .option(FlinkOptions.SUPPLEMENTAL_LOGGING_MODE, mode.name())
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
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
  @MethodSource("tableTypeAndBooleanTrueFalseParams")
  void testStreamWriteAndRead(HoodieTableType tableType, boolean useSourceV2) throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, false)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), false)
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testStreamWriteBatchRead(boolean useSourceV2) {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testStreamWriteBatchReadOptimizedWithoutCompaction(boolean useSourceV2) {
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .option(FlinkOptions.QUERY_TYPE, FlinkOptions.QUERY_TYPE_READ_OPTIMIZED)
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    final String insertInto = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1')";
    execInsertSql(streamTableEnv, insertInto);

    List<Row> rows = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    assertTrue(rows.isEmpty());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testStreamWriteReadSkippingCompaction(boolean useSourceV2) throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source", 4);
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, 1)
        .option(FlinkOptions.COMPACTION_TASKS, 1)
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testAppendWriteReadSkippingClustering(boolean useSourceV2) throws Exception {
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
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid");
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    conf.set(FlinkOptions.CHANGELOG_ENABLED, true);

    // write one commit
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    // write another commit with deletes
    TestData.writeData(TestData.DATA_SET_UPDATE_DELETE, conf);

    String latestCommit = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
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
    List<Row> result = submitAndFetchWithRetry(streamTableEnv, "select name, sum(age) from t1 group by name", sinkDDL, 2);
    final String expected = "[+I(+I[Danny, 24]), +I(+I[Stephen, 34])]";
    assertRowsEquals(result, expected, true);
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndSourceV2AndBooleanTrueFalseParams")
  void testDataSkippingWithRecordLevelIndex(HoodieTableType tableType, boolean useSourceV2, boolean mdtCompactionEnabled) throws Exception {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name())
        .option(FlinkOptions.METADATA_ENABLED, true)
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true)
        .option(FlinkOptions.TABLE_TYPE, tableType.name())
        .option(FlinkOptions.METADATA_COMPACTION_DELTA_COMMITS, mdtCompactionEnabled ? 1 : 10)
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
        .end();
    tableEnv.executeSql(hoodieTableDDL);
    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    if (mdtCompactionEnabled) {
      TestUtils.validateMdtCompactionInstant(tempFile.getAbsolutePath(), false);
    }

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
  void testDataSkippingWithPartitionedRecordLevelIndex(
      HoodieTableType tableType, boolean useSourceV2) throws Exception {
    String writerTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.RECORD_LEVEL_INDEX.name())
        .option(FlinkOptions.TABLE_TYPE, tableType.name())
        .end();
    streamTableEnv.executeSql(writerTableDDL);
    execInsertSql(streamTableEnv, TestSQL.INSERT_T1);

    String readerTableDDL = sql("t1_read")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true)
        .option(FlinkOptions.TABLE_TYPE, tableType.name())
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
        .end();
    batchTableEnv.executeSql(readerTableDDL);

    List<Row> result = CollectionUtil.iterableToList(
        () -> batchTableEnv.sqlQuery(
            "select * from t1_read where `partition` = 'par1' and uuid = 'id1'").execute().collect());
    assertRowsEquals(result, "[+I[id1, Danny, 23, 1970-01-01T00:00:01, par1]]");

    List<Row> multiPartitionResult = CollectionUtil.iterableToList(
        () -> batchTableEnv.sqlQuery(
            "select * from t1_read where `partition` in ('par1', 'par4') and uuid in ('id1', 'id7')").execute().collect());
    assertRowsEquals(multiPartitionResult, "["
        + "+I[id1, Danny, 23, 1970-01-01T00:00:01, par1], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4]]");

    // insert a record with id1 into the par4.
    String insert = "insert into t1 values ('id1','Jack',23,TIMESTAMP '1970-01-01 00:00:01','par4')";
    execInsertSql(streamTableEnv, insert);
    // test scenario query predicate also includes a partition name which doesn't exist.
    multiPartitionResult = CollectionUtil.iterableToList(
        () -> batchTableEnv.sqlQuery(
            "select * from t1_read where `partition` in ('par1', 'par4', 'par5') and uuid in ('id1', 'id7')").execute().collect());
    assertRowsEqualsUnordered(multiPartitionResult,
        Arrays.asList(
            "+I[id1, Danny, 23, 1970-01-01T00:00:01, par1]",
            "+I[id1, Jack, 23, 1970-01-01T00:00:01, par4]",
            "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4]"));
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndSourceV2AndBooleanTrueFalseParams")
  void testReadWithPartitionStatsPruning(HoodieTableType tableType, boolean useSourceV2, boolean hiveStylePartitioning) throws Exception {
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.METADATA_ENABLED, true)
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), false)
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning)
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid");
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
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
  @MethodSource("tableTypeAndSourceV2AndBooleanTrueFalseParams")
  void testStreamReadFilterByPartition(HoodieTableType tableType, boolean useSourceV2, boolean hiveStylePartitioning) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid");
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning);

    // write one commit
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 2)
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, false)
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning)
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);

    final String expected = "["
        + "+I(+I[id1, Danny, 23, 1970-01-01T00:00:00.001, par1]), "
        + "+I(+I[id2, Stephen, 33, 1970-01-01T00:00:00.002, par1])]";
    List<Row> result = execSelectSqlWithExpectedNum(streamTableEnv,
        "select * from t1 where `partition`='par1'", 2);
    assertRowsEquals(result, expected, true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testStreamReadMorTableWithCompactionPlan(boolean useSourceV2) throws Exception {
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST)
        .option(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 2)
        // close the async compaction
        .option(FlinkOptions.COMPACTION_ASYNC_ENABLED, false)
        // generate compaction plan for each commit
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, 1)
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
        .noPartition()
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);

    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);
    List<Row> rows = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", TestData.DATA_SET_SOURCE_INSERT.size());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
  }

  /**
   * Regression test for HUDI: data loss in stream read from earliest when
   * {@code read.streaming.skip_compaction = true} on a MOR table with completed
   * compaction commits. Covers the streaming earliest full table scan branch in
   * {@link org.apache.hudi.source.IncrementalInputSplits#inputSplits(HoodieTableMetaClient, String, boolean)}.
   *
   * <p>Triggering condition:
   * <ul>
   *   <li>{@code read.start-commit = earliest} (no instant range -> full table scan path);</li>
   *   <li>{@code read.streaming.skip_compaction = true} (active timeline filtered out compaction);</li>
   *   <li>MOR table with at least one completed compaction commit that produced a
   *       new base file from existing log files.</li>
   * </ul>
   *
   * <p>Construction:
   * <ol>
   *   <li>Offline write {@code DATA_SET_INSERT} (8 records, ids 1..8) and then
   *       {@code DATA_SET_UPDATE_INSERT} (8 records, where ids 1..5 update existing keys
   *       and ids 9..11 are new) via {@link TestData#writeDataAsBatch}, which deterministically
   *       triggers an inline compaction once {@code COMPACTION_DELTA_COMMITS = 1} +
   *       {@code COMPACTION_ASYNC_ENABLED = true} are set. After this step the table has
   *       both a base file (from compaction) and log files written by the UPDATE batch.</li>
   *   <li>Streaming read from earliest with {@code skip_compaction = true} and wait until
   *       the expected number of merged rows are received. Without the fix, the FS view used
   *       in the earliest full-table-scan branch is built from a compaction-filtered
   *       timeline, file slice boundaries are wrongly computed, log files are missed
   *       and the read will never reach the expected row count (the test would time out).</li>
   * </ol>
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testStreamReadMorTableWithCompactionFromEarliest(boolean useSourceV2) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.TABLE_TYPE, MERGE_ON_READ.name());
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 2);
    // mandatory for writeDataAsBatch#inlineCompaction to actually run a compaction
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    conf.set(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);

    // Step 1: offline-write two batches with deterministic inline compaction in between.
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf);
    TestData.writeDataAsBatch(TestData.DATA_SET_UPDATE_INSERT, conf);

    // Step 2: streaming read from earliest with skip_compaction = true.
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, MERGE_ON_READ)
        .option(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name())
        .option(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 2)
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST)
        .option(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 2)
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
        // skip compaction instant -> active timeline drops compaction commit
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, true)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);

    // After the UPDATE batch, the merged result must contain all up-to-date records:
    //   - 5 updated records   (id1..id5 from DATA_SET_UPDATE_INSERT)
    //   - 3 carried-over records (id6, id7, id8 from DATA_SET_INSERT, not touched by UPDATE)
    //   - 3 newly inserted records (id9, id10, id11 from DATA_SET_UPDATE_INSERT)
    // i.e. 11 records in total. Without the fix the streaming read would never reach
    // expectedNum = 11 and the test would time out via the CollectSink.
    final int expectedNum = 11;
    List<Row> rows = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", expectedNum);
    assertEquals(expectedNum, rows.size(),
        "Expect 11 up-to-date records to be visible after earliest streaming read"
            + " with skip_compaction on a MOR table that has a completed compaction commit"
            + ", actual rows: " + rows);
  }

  /**
   * Regression test for HUDI: data loss in batch read from earliest when
   * {@code read.streaming.skip_compaction = true} on a MOR table with completed
   * compaction commits. Covers the batch full-table-scan branch in
   * {@link org.apache.hudi.source.IncrementalInputSplits#inputSplits(HoodieTableMetaClient, boolean)}.
   *
   * <p>This complements {@link #testStreamReadMorTableWithCompactionFromEarliest(boolean)}
   * which only exercises the streaming code path. Without the fix, building the
   * {@link org.apache.hudi.common.table.view.HoodieTableFileSystemView} with a
   * compaction-filtered timeline would mis-classify file slice boundaries and
   * lose log files.
   */
  @Test
  void testBatchReadMorTableWithCompactionFromEarliest() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.TABLE_TYPE, MERGE_ON_READ.name());
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 2);
    // mandatory for writeDataAsBatch#inlineCompaction to actually run a compaction
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    conf.set(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);

    // Offline-write two batches against overlapping record keys, the 2nd write triggers
    // an inline compaction that merges existing log files into a new base file - exactly
    // the scenario that exposes the buggy file-slice classification when skip_compaction
    // is enabled.
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf);
    TestData.writeDataAsBatch(TestData.DATA_SET_UPDATE_INSERT, conf);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, MERGE_ON_READ)
        .option(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name())
        .option(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 2)
        .option(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST)
        // skip compaction instant -> active timeline drops compaction commit
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, true)
        .end();
    batchTableEnv.executeSql(hoodieTableDDL);

    List<Row> result = CollectionUtil.iteratorToList(
        batchTableEnv.executeSql("select * from t1").collect());
    // After update, the merged result must contain all up-to-date records:
    //   - 5 updated records   (id1..id5 from DATA_SET_UPDATE_INSERT)
    //   - 3 carried-over records (id6, id7, id8 from DATA_SET_INSERT, not touched by UPDATE)
    //   - 3 newly inserted records (id9, id10, id11 from DATA_SET_UPDATE_INSERT)
    // i.e. 11 records in total. Without the fix, log files belonging to the file slice prior
    // to the inline compaction would be silently dropped by the file system view because
    // the active timeline filtered out the compaction commit, and the result size would be
    // smaller than 11.
    assertEquals(11, result.size(),
        "Expect all up-to-date records to be visible after earliest + skip_compaction batch read"
            + ", actual rows: " + result);
  }

  /**
   * Regression test for HUDI: data loss when the start commit has been archived
   * and {@code read.streaming.skip_compaction = true} on a MOR table.
   * Covers the batch "fallback to full table scan" branch in
   * {@link org.apache.hudi.source.IncrementalInputSplits#inputSplits(HoodieTableMetaClient, boolean)}
   * which is reached when {@code hasArchivedInstants == true}.
   *
   * <p>Construction:
   * <ol>
   *   <li>Write 10 delta-commit batches of {@code (id1,id2), (id3,id4), ...} on a MOR table
   *       so that each batch only inserts new keys (clear, predictable per-commit semantics).</li>
   *   <li>Trigger one completed compaction commit by issuing an extra UPDATE batch on
   *       {@code id1..id4} with {@code COMPACTION_DELTA_COMMITS = 1} via
   *       {@link TestData#writeDataAsBatch} (which explicitly calls {@code inlineCompaction()}).
   *       This creates exactly the file-slice boundary that the buggy FS view would
   *       mis-classify.</li>
   *   <li>Pick the LAST archived delta-commit as {@code read.start-commit} (filtered by
   *       {@code action = deltacommit} to exclude any archived compaction {@code commit}
   *       instants). This is deterministic regardless of how many delta commits were
   *       archived by the cleaner+archiver and routes the reader through the
   *       "archived start commit -> fullTableScan" branch.</li>
   *   <li>Read with {@code skip_compaction = true} and assert on the SET of record-keys
   *       in the result (not just on count). The expected key set is derived dynamically
   *       from the timeline: every delta_commit whose completion time is &gt;= the chosen
   *       start_commit contributes its written ids, plus id1..id4 from the UPDATE batch
   *       are always present because the UPDATE is the latest write. Without the fix,
   *       log files of the file slice that straddles the compaction commit are silently
   *       dropped, so some of these ids would be missing.</li>
   * </ol>
   */
  @Test
  void testBatchReadMorTableWithCompactionStartCommitArchived() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid");
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
    conf.set(FlinkOptions.TABLE_TYPE, MERGE_ON_READ.name());
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 2);
    // aggressive archival to force older instants out of the active timeline
    conf.set(FlinkOptions.ARCHIVE_MIN_COMMITS, 4);
    conf.set(FlinkOptions.ARCHIVE_MAX_COMMITS, 5);
    conf.set(FlinkOptions.CLEAN_RETAIN_COMMITS, 3);
    conf.setString("hoodie.commits.archival.batch", "1");

    // Step 1: write 10 batches of 2 new records each -> 10 delta_commit instants, 20 distinct keys.
    for (int i = 0; i < 20; i += 2) {
      List<RowData> dataset = TestData.dataSetInsert(i + 1, i + 2);
      TestData.writeData(dataset, conf);
    }

    // Step 2: trigger at least one completed compaction commit by issuing one more delta_commit
    // that UPDATES the very first record keys (id1..id4) and enabling COMPACTION_DELTA_COMMITS=1.
    // The update writes new log files for the file group that contains id1..id4, and the inline
    // compaction merges them into a new base file -> a real compaction file-slice boundary.
    // NOTE: use writeDataAsBatch (which explicitly calls inlineCompaction()), since the plain
    // writeData helper does not run the compaction even with COMPACTION_DELTA_COMMITS=1.
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    conf.set(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    TestData.writeDataAsBatch(TestData.dataSetInsert(1, 2, 3, 4), conf);

    // Step 3: list the full timeline in one shot to map start_commit -> expected id set.
    // Delta_commit instants are strictly monotonically increasing, so the sorted list of all
    // delta_commits across active + archived timelines gives a 1:1 mapping to the 10 batches
    // written in Step 1: the k-th delta_commit wrote id_{2k+1} and id_{2k+2}.
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(
        new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(new Configuration())),
        tempFile.getAbsolutePath());
    // Use the merged (archived + active) timeline to capture all delta_commits,
    // even those that may have been archived by the aggressive archival settings.
    List<String> batchInstantTimes = TimelineUtils.getTimeline(metaClient, true)
        .getCommitsTimeline().filterCompletedInstants()
        .filter(instant -> HoodieTimeline.DELTA_COMMIT_ACTION.equals(instant.getAction()))
        .getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
    // Step 1 produced exactly 10 delta_commits; the 11th (if present) is from Step 2 UPDATE.
    // Keep only the first 10 to build the batch-index -> id mapping.
    assertTrue(batchInstantTimes.size() >= 10,
        "Expected at least 10 delta_commits from Step 1, got " + batchInstantTimes.size());
    batchInstantTimes = batchInstantTimes.subList(0, 10);

    // Step 4: pick the LAST archived delta_commit that belongs to Step 1's batches as
    // start commit. This avoids any drift caused by archival ordering or by compaction
    // `commit` instants being interleaved with delta_commits in the archived timeline,
    // and also ignores the Step 2 UPDATE batch in case it also got archived.
    Set<String> step1InstantTimeSet = new TreeSet<>(batchInstantTimes);
    List<HoodieInstant> archivedDeltaCommits = metaClient.getArchivedTimeline().getCommitsTimeline()
        .filterCompletedInstants()
        .filter(instant -> HoodieTimeline.DELTA_COMMIT_ACTION.equals(instant.getAction()))
        .filter(instant -> step1InstantTimeSet.contains(instant.requestedTime()))
        .getInstants();
    // make sure archival actually happened on Step 1's batches, otherwise the test premise
    // (the reader hits the archived start commit + fullTableScan branch) does not hold.
    assertTrue(!archivedDeltaCommits.isEmpty(),
        "archival did not happen as expected on Step 1's batches, archived delta commits = "
            + archivedDeltaCommits + ", Step 1 batch instant times = " + batchInstantTimes);
    HoodieInstant startInstant = archivedDeltaCommits.get(archivedDeltaCommits.size() - 1);
    String archivedStartInstant = startInstant.requestedTime();

    // The expected key set: every Step 1 batch whose instant time is >= start_commit contributes
    // its 2 ids; plus id1..id4 from the Step 2 UPDATE batch (always the latest write, never
    // excluded since its completion time is the largest).
    int firstIncludedBatchIdx = batchInstantTimes.indexOf(archivedStartInstant);
    assertTrue(firstIncludedBatchIdx >= 0,
        "chosen start_commit " + archivedStartInstant + " is not one of the Step 1 batch instant times " + batchInstantTimes);
    Set<String> expectedIds = new TreeSet<>();
    for (int i = firstIncludedBatchIdx; i < batchInstantTimes.size(); i++) {
      expectedIds.add("id" + (2 * i + 1));
      expectedIds.add("id" + (2 * i + 2));
    }
    // UPDATE batch ids — always present in the merged view because the UPDATE is the latest write.
    expectedIds.add("id1");
    expectedIds.add("id2");
    expectedIds.add("id3");
    expectedIds.add("id4");

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, MERGE_ON_READ)
        .option(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name())
        .option(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 2)
        .option(FlinkOptions.READ_START_COMMIT, archivedStartInstant)
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, true)
        .end();
    batchTableEnv.executeSql(hoodieTableDDL);

    List<Row> result = CollectionUtil.iteratorToList(
        batchTableEnv.executeSql("select uuid from t1").collect());
    Set<String> actualIds = new TreeSet<>();
    for (Row r : result) {
      actualIds.add(r.getField(0).toString());
    }
    // Without the fix, the FS view used to construct file slices for the fallback full-table-scan
    // branch is built from a compaction-filtered timeline, so log files of the file slice that
    // straddles the compaction commit are silently dropped and some ids would be missing from
    // {@code actualIds}. With the fix, every expected id must be present.
    assertEquals(expectedIds, actualIds,
        "Expected id set " + expectedIds + " but got " + actualIds
            + " when reading from archived start commit " + archivedStartInstant
            + " with skip_compaction = true on a MOR table that has a completed compaction commit");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testStreamReadMorTableWithBucketIndex(boolean partitioned) throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    String createSource2 = TestConfigurations.getFileSourceDDL("source2", "test_source_2.data");
    streamTableEnv.executeSql(createSource);
    streamTableEnv.executeSql(createSource2);

    TestConfigurations.Sql t1 = sql("t1").option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.READ_STREAMING_SKIP_COMPACT, false)
        .option(FlinkOptions.TABLE_TYPE, MERGE_ON_READ)
        .option(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name())
        .option(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), false)
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, 3);

    String hoodieTableDDL = partitioned ? t1.end() : t1.noPartition().end();

    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    // reading from the latest commit instance.
    List<Row> rows = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", TestData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT.size());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT);

    // insert another batch of data with compaction
    String insertInto2 = "insert into t1 select * from source2";
    execInsertSql(streamTableEnv, insertInto2);

    // reading from the earliest
    List<Row> rows2 = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1 /*+options('read.start-commit'='earliest')*/",
        TestData.DATA_SET_SOURCE_MERGED.size());
    assertRowsEquals(rows2, TestData.DATA_SET_SOURCE_MERGED);
  }

  @ParameterizedTest
  @MethodSource("executionModeAndPartitioningParams")
  void testWriteAndRead(ExecMode execMode, boolean hiveStylePartitioning) {
    TableEnvironment tableEnv = execMode == ExecMode.BATCH ? batchTableEnv : streamTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
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
        .option(FlinkOptions.RECORD_KEY_FIELD, "uuid")
        .option(FlinkOptions.ORDERING_FIELDS, "ts")
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    ValidationException exception =
        assertThrows(ValidationException.class, () -> execInsertSql(tableEnv, TestSQL.INSERT_SAME_KEY_T1));
    assertLinesMatch(
        Collections.singletonList("Field ts does not exist in the table schema. Please check '"
            + FlinkOptions.ORDERING_FIELDS.key() + "' option."),
        Collections.singletonList(exception.getCause().getMessage()));
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
        .option(FlinkOptions.RECORD_KEY_FIELD, "uuid")
        .option(FlinkOptions.ORDERING_FIELDS, FlinkOptions.NO_PRE_COMBINE)
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
  @MethodSource("tableTypeCacheTypeAndAsyncLookupParams")
  void testLookupJoin(HoodieTableType tableType, String cacheType, boolean async) {
    TableEnvironment tableEnv = streamTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath() + "/t1")
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    String hoodieTableDDL2 = sql("t2")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath() + "/t2")
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .end();
    tableEnv.executeSql(hoodieTableDDL2);

    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    tableEnv.executeSql("create view t1_view as select *,"
        + "PROCTIME() as proc_time from t1");

    // Join two hudi tables with the same data
    String sql = "insert into t2 select b.* from t1_view o "
        + "       join t1/*+ OPTIONS('lookup.join.cache.ttl'='2 day', 'lookup.async'='" + async + "',"
        + "       'lookup.join.cache.type'='" + cacheType + "') */  "
        + "       FOR SYSTEM_TIME AS OF o.proc_time AS b on o.uuid = b.uuid";

    // The lookup function loads the dimension table lazily on the first probe row, so a teardown /
    // commit-visibility race can occasionally make the join emit no rows. Re-running the upsert into
    // the uuid-keyed table t2 is idempotent, so retry until the expected rows materialize.
    final int expectedNum = TestData.DATA_SET_SOURCE_INSERT.size();
    List<Row> result = Collections.emptyList();
    for (int attempt = 1; attempt <= MAX_STREAM_READ_ATTEMPTS; attempt++) {
      execInsertSql(tableEnv, sql);
      result = CollectionUtil.iterableToList(
          () -> tableEnv.sqlQuery("select * from t2").execute().collect());
      if (result.size() >= expectedNum) {
        break;
      }
      log.warn("testLookupJoin collected {} of {} rows on attempt {}/{}; a teardown race produced an "
          + "empty lookup join. Retrying.", result.size(), expectedNum, attempt, MAX_STREAM_READ_ATTEMPTS);
    }

    assertRowsEquals(result, TestData.DATA_SET_SOURCE_INSERT);
  }

  private void initTablesForLookupJoin(HoodieTableType tableType) {
    String tDDL = "create table T(i INT PRIMARY KEY NOT ENFORCED, `proctime` AS PROCTIME())"
        + " with ('connector'='hudi', 'path'='" + tempFile.getAbsolutePath() + "/T')";
    streamTableEnv.executeSql(tDDL);
    String dimDDL = "CREATE TABLE DIM (i INT PRIMARY KEY NOT ENFORCED, j INT, k1 INT, k2 INT) "
        + "with ('connector'='hudi', 'table.type'='" + tableType + "',"
        + " 'path'='" + tempFile.getAbsolutePath() + "/DIM', 'continuous.discovery-interval'='1 ms')";
    streamTableEnv.executeSql(dimDDL);
  }

  @ParameterizedTest
  @MethodSource("tableTypeCacheTypeAndAsyncLookupParams")
  void testLookup(HoodieTableType tableType, String cacheType, boolean async) {
    initTablesForLookupJoin(tableType);
    execInsertSql(streamTableEnv, "INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");
    execInsertSql(streamTableEnv, "INSERT INTO T VALUES (1), (2), (3)");

    String query = "SELECT T.i, D.j, D.k1, D.k2 FROM T LEFT JOIN DIM /*+ OPTIONS('lookup.async'='" + async
        + "', 'lookup.join.cache.type'='" + cacheType + "', 'lookup.join.cache.ttl'='1s') */"
        + " for system_time as of T.proctime AS D ON T.i = D.i";
    List<Row> result = CollectionUtil.iterableToList(() -> streamTableEnv.executeSql(query).collect());
    assertThat(result).containsExactlyInAnyOrder(
        Row.of(1, 11, 111, 1111),
        Row.of(2, 22, 222, 2222),
        Row.of(3, null, null, null));

    execInsertSql(streamTableEnv, "INSERT INTO DIM VALUES (2, 44, 444, 4444), (3, 33, 333, 3333)");
    execInsertSql(streamTableEnv, "INSERT INTO T VALUES (1), (2), (3), (4)");

    result = CollectionUtil.iterableToList(() -> streamTableEnv.executeSql(query).collect());
    assertThat(result).containsExactlyInAnyOrder(
        Row.of(1, 11, 111, 1111),
        Row.of(2, 44, 444, 4444),
        Row.of(3, 33, 333, 3333),
        Row.of(4, null, null, null));
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
        .option(FlinkOptions.ORDERING_FIELDS, "ts")
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .option(FlinkOptions.ORDERING_FIELDS, "ts")
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
        .options(getDefaultKeys())
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

  @ParameterizedTest
  @MethodSource("indexAndBooleanParams")
  void testWriteGlobalIndex(String indexType, boolean bootstrapEnabled) {
    // the source generates 4 commits
    String createSource = TestConfigurations.getFileSourceDDL(
        "source", "test_source_4.data", 4);
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.INDEX_GLOBAL_ENABLED, true)
        .option(FlinkOptions.INDEX_TYPE, indexType)
        .option(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, bootstrapEnabled)
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
        .options(getDefaultKeys())
        .option(FlinkOptions.INDEX_GLOBAL_ENABLED, false)
        .option(FlinkOptions.PRE_COMBINE, true)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);

    final String insertInto2 = "insert into t1 select * from source";

    execInsertSql(streamTableEnv, insertInto2);

    List<Row> result = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    final String expected = "["
        + "+I[id1, Danny, 24, 1970-01-01T00:00:01, par1], "
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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

  @Test
  void testLanceFormatAppendOnlyWriteAndRead() {
    String createHoodieTable = sql("lance_t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, "insert")
        .option("hoodie.table.base.file.format", "LANCE")
        .end();
    batchTableEnv.executeSql(createHoodieTable);

    execInsertSql(batchTableEnv, "insert into lance_t1 values "
        + "('id1', 'Alice', 23, TIMESTAMP '1970-01-01 00:00:01', 'par1'),"
        + "('id2', 'Bob', 31, TIMESTAMP '1970-01-01 00:00:02', 'par2')");

    List<Row> rows = CollectionUtil.iteratorToList(
        batchTableEnv.executeSql("select uuid, name, age, ts, `partition` from lance_t1").collect());
    assertRowsEquals(rows,
        "[+I[id1, Alice, 23, 1970-01-01T00:00:01, par1], "
            + "+I[id2, Bob, 31, 1970-01-01T00:00:02, par2]]");

    List<Row> projectedRows = CollectionUtil.iteratorToList(
        batchTableEnv.executeSql("select name, uuid from lance_t1").collect());
    assertRowsEquals(projectedRows, "[+I[Alice, id1], +I[Bob, id2]]");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testLanceFormatNestedTypesUpsertWriteAndRead(HoodieTableType tableType) {
    String createHoodieTable = sql("lance_nested")
        .field("id int not null")
        .field("ts bigint")
        .field("f_row row(f_name varchar(10), f_age int)")
        .field("f_array array<row(f_name varchar(10), f_age int)>")
        .field("f_nested_array array<row(f_scores array<int>)>")
        .pkField("id")
        .noPartition()
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, "upsert")
        .option(FlinkOptions.ORDERING_FIELDS, "ts")
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option("hoodie.table.base.file.format", "LANCE")
        .end();
    batchTableEnv.executeSql(createHoodieTable);

    execInsertSql(batchTableEnv, "insert into lance_nested values "
        + "(1, 1, ROW('alice', 30), ARRAY[ROW('child1', 1), ROW('child2', 2)], "
        + "ARRAY[ROW(ARRAY[1, 2]), ROW(ARRAY[3])]),"
        + "(2, 2, ROW('bob', 31), ARRAY[ROW('child3', 3)], ARRAY[ROW(ARRAY[4])])");

    execInsertSql(batchTableEnv, "insert into lance_nested values "
        + "(1, 3, ROW('alice_v2', 32), ARRAY[ROW('child4', 4)], ARRAY[ROW(ARRAY[5, 6])]),"
        + "(3, 4, ROW('charlie', 33), ARRAY[ROW('child5', 5)], ARRAY[ROW(ARRAY[7])])");

    List<Row> rows = CollectionUtil.iterableToList(
        () -> batchTableEnv.sqlQuery("select * from lance_nested").execute().collect());
    assertRowsEqualsUnordered(Arrays.asList(
        row(1, 3L, row("alice_v2", 32), array(row("child4", 4)), array(row((Object) array(5, 6)))),
        row(2, 2L, row("bob", 31), array(row("child3", 3)), array(row((Object) array(4)))),
        row(3, 4L, row("charlie", 33), array(row("child5", 5)), array(row((Object) array(7))))), rows);

    List<Row> projectedRows = CollectionUtil.iterableToList(
        () -> batchTableEnv.sqlQuery("select f_nested_array, id from lance_nested").execute().collect());
    assertRowsEqualsUnordered(Arrays.asList(
        row(array(row((Object) array(5, 6))), 1),
        row(array(row((Object) array(4))), 2),
        row(array(row((Object) array(7))), 3)), projectedRows);
  }

  @Test
  void testLanceFormatCopyOnWriteUpsertWriteAndRead() {
    String createHoodieTable = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_COPY_ON_WRITE)
        .option("hoodie.table.base.file.format", "LANCE")
        .end();
    batchTableEnv.executeSql(createHoodieTable);

    execInsertSql(batchTableEnv, TestSQL.INSERT_T1);
    List<Row> rows = CollectionUtil.iteratorToList(
        batchTableEnv.executeSql("select * from t1").collect());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);

    execInsertSql(batchTableEnv, TestSQL.UPDATE_INSERT_T1);
    List<Row> updatedRows = CollectionUtil.iteratorToList(
        batchTableEnv.executeSql("select * from t1").collect());
    assertRowsEquals(updatedRows, TestData.DATA_SET_SOURCE_MERGED);
  }

  @Test
  void testLanceFormatMergeOnReadUpsertWriteAndRead() {
    String createHoodieTable = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .option(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, true)
        .option(FlinkOptions.COMPACTION_ASYNC_ENABLED, true)
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, 1)
        .option("hoodie.table.base.file.format", "LANCE")
        .end();
    batchTableEnv.executeSql(createHoodieTable);

    execInsertSql(batchTableEnv, TestSQL.INSERT_T1);
    assertTrue(TestUtils.hasCompleteCompactionInstant(tempFile.getAbsolutePath()),
        "The first MOR insert should have complete compaction");

    List<Row> rows = CollectionUtil.iteratorToList(
        batchTableEnv.executeSql("select * from t1").collect());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);

    execInsertSql(batchTableEnv, TestSQL.UPDATE_INSERT_T1);

    List<Row> updatedRows = CollectionUtil.iteratorToList(
        batchTableEnv.executeSql("select * from t1").collect());
    assertRowsEquals(updatedRows, TestData.DATA_SET_SOURCE_MERGED);
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .option(FlinkOptions.RECORD_KEY_FIELD, clustering ? "uuid" : "")
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid");
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());

    // write 3 batches of data set
    TestData.writeData(TestData.dataSetInsert(1, 2), conf);
    TestData.writeData(TestData.dataSetInsert(3, 4), conf);
    TestData.writeData(TestData.dataSetInsert(5, 6), conf);

    String latestCommit = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_START_COMMIT, latestCommit)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, TestData.dataSetInsert(5, 6));
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndBooleanTrueFalseParams")
  void testReadChangelogIncremental(HoodieTableType tableType, boolean compactionEnabled) throws Exception {
    TableEnvironment tableEnv = streamTableEnv;
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid");
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, compactionEnabled);
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
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.COMPACTION_ASYNC_ENABLED, compactionEnabled)
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
  @ValueSource(booleans = {true, false})
  void  testChangelogCompactionSchedule(Boolean compactionEnabled) throws Exception {
    TableEnvironment tableEnv = streamTableEnv;
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid");
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
    conf.set(FlinkOptions.TABLE_TYPE, MERGE_ON_READ.name());
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, compactionEnabled);
    // schedule compaction after 2 commits
    conf.set(FlinkOptions.COMPACTION_DELTA_COMMITS, 2);
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
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, MERGE_ON_READ)
        .option(FlinkOptions.COMPACTION_ASYNC_ENABLED, compactionEnabled)
        .option(FlinkOptions.COMPACTION_DELTA_COMMITS, 2)
        .option(FlinkOptions.READ_CDC_FROM_CHANGELOG, false)
        .option(FlinkOptions.READ_START_COMMIT, latestCommit)
        .option(FlinkOptions.CDC_ENABLED, true)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    String firstCommit = TestUtils.getFirstCompleteInstant(tempFile.getAbsolutePath());
    String secondCommit = TestUtils.getNthCompleteInstant(new StoragePath(tempFile.getAbsolutePath()), 1, HoodieTimeline.DELTA_COMMIT_ACTION);
    String thirdCommit = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
    final String query1 = String.format("select count(*) from t1/*+ options('read.start-commit'='%s')*/", firstCommit);
    final String query2 = String.format("select count(*) from t1/*+ options('read.start-commit'='%s')*/", secondCommit);
    final String query3 = String.format("select count(*) from t1/*+ options('read.start-commit'='%s')*/", thirdCommit);
    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery(query1).execute().collect());
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery(query2).execute().collect());
    List<Row> result3 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery(query3).execute().collect());
    assertEquals(19, result1.size());
    assertEquals(7, result2.size());
    assertEquals(3, result3.size());
    assertRowsEquals(result1.subList(result1.size() - 2, result1.size()), "[-U[1], +U[2]]");
    assertRowsEquals(result2.subList(result2.size() - 2, result2.size()), "[-D[1], +I[1]]");
    assertRowsEquals(result3.subList(result3.size() - 2, result3.size()), "[-D[1], +I[1]]");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testIncrementalReadArchivedCommits(HoodieTableType tableType) throws Exception {
    TableEnvironment tableEnv = batchTableEnv;
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid");
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
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
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_START_COMMIT, secondArchived)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, TestData.dataSetInsert(3, 4, 5, 6, 7, 8, 9, 10,
        11, 12, 13, 14, 15, 16, 17, 18, 19, 20));
  }

  // Only test COPY_ON_WRITE here: the file group reader reads records using the table (write) schema
  // persisted in the timeline, not the wider query schema defined by the Flink catalog DDL. So columns
  // that only exist in the DDL (e.g. the newly added `salary`) are not recognized and reading fails with
  // "One or more specified columns does not exist in the hudi table". MERGE_ON_READ goes through the file
  // group reader path and therefore does not support reading with a wider schema in this scenario.
  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class, names = {"COPY_ON_WRITE"})
  void testReadWithWiderSchema(HoodieTableType tableType) throws Exception {
    TableEnvironment tableEnv = batchTableEnv;
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid");
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
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
        .option(FlinkOptions.ORDERING_FIELDS, "ts")
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
    assertRowsEqualsUnordered(expected, result);
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
    assertRowsEqualsUnordered(expected, result);
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

  @Test
  void testParquetNestedRowExceedingReadBatch() {
    // Regression for NestedColumnReader#readRow throwing ArrayIndexOutOfBoundsException when a COW
    // base file holds more rows than the 2048-row vectorized read batch
    // (RecordIterators.DEFAULT_BATCH_SIZE) and a nested ROW column is read. On a full, non-final
    // batch the Dremel level stream carries a one-record lookahead, so NestedPositionUtil
    // #calculateRowOffsets returns positionsCount = batchSize + 1 = 2049 while the materialized
    // child column vectors are sized to their value count = 2048. The Hudi-specific null-row-collapse
    // loop iterates to positionsCount and reads child.isNullAt(2048), one past a length-2048 vector.
    //
    // Two conditions are both required to surface it, and drove this schema and data:
    //  1. The bad index is only reached through AbstractHeapVector#isNullAt, which short-circuits to
    //     false without touching isNull[] when the vector has no nulls. So a child vector must
    //     actually carry a null. Odd-id rows therefore store a present ROW with all-null children
    //     (row(null, ...)); the row stays present (its own isNullAt(2048) short-circuits) but the
    //     child leaf vectors get noNulls=false and overrun at the phantom index. Half the rows are
    //     null-children so the first full batch is guaranteed to contain them regardless of how
    //     bulk_insert orders keys.
    //  2. The nullable leaves must be *direct* children of the collapsed row. A sub-row child would
    //     be renewed to positionsCount (length 2049) and not overrun, so the two nested rows are
    //     top-level columns: f_scalar row(f0 int, f1 varchar(10)) covers heap-vector children, and
    //     f_dec row(d decimal(10, 2)) covers a decimal child, whose ParquetDecimalVector is not an
    //     AbstractHeapVector and must be unwrapped by NestedColumnReader#vectorLength.
    // See ITTestHoodieDataSource#testParquetNullChildColumnsRowTypes for the collapse behaviour.
    TableEnvironment tableEnv = batchTableEnv;

    // More rows than one 2048-row read batch, so the first batch is full and non-final -- that is
    // what makes the level stream carry the trailing lookahead that overshoots the vectors. The
    // rows are generated by cross joining two small VALUES lists rather than a single 2000+-row
    // VALUES literal: Calcite plans the latter pathologically slowly (minutes to hours), while two
    // ~50-element lists plan instantly and the row count is simply their product.
    final int outer = 43;
    final int inner = 50;
    final int numRows = outer * inner; // 2150 > 2048

    String hoodieTableDDL = sql("t1")
        .field("f_int int")
        .field("f_scalar row(f0 int, f1 varchar(10))")
        .field("f_dec row(d decimal(10, 2))")
        .pkField("f_int")
        .noPartition()
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, "bulk_insert")
        // Single write task => all rows land in one base file, so one read split crosses the
        // 2048-row batch boundary.
        .option(FlinkOptions.WRITE_TASKS, 1)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    // id = blk * inner + pos is unique over blk in [0, outer), pos in [0, inner) => 0 .. numRows-1.
    // Both nested rows stay present; even ids get populated leaves, odd ids get all-null leaves
    // (which the reader collapses back to a NULL row). Each ROW is cast to its named type so the
    // query output type matches the sink column exactly.
    String insert = "insert into t1 select\n"
        + "  g.id,\n"
        + "  cast(row(\n"
        + "    case when mod(g.id, 2) = 0 then g.id else cast(null as int) end,\n"
        + "    case when mod(g.id, 2) = 0 then concat('v', cast(g.id as varchar)) else cast(null as varchar(10)) end\n"
        + "  ) as row<f0 int, f1 varchar(10)>),\n"
        + "  cast(row(\n"
        + "    case when mod(g.id, 2) = 0 then cast(g.id as decimal(10, 2)) else cast(null as decimal(10, 2)) end\n"
        + "  ) as row<d decimal(10, 2)>)\n"
        + "from (\n"
        + "  select blk.b * " + inner + " + pos.p as id\n"
        + "  from (values " + valuesList(outer) + ") as blk(b)\n"
        + "  cross join (values " + valuesList(inner) + ") as pos(p)\n"
        + ") g";
    execInsertSql(tableEnv, insert);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());

    // The read completes (no AIOOBE across the batch boundary) and every row is returned. Without
    // the fix the vectorized read throws while materializing the first full batch, so this fails.
    assertEquals(numRows, result.size());

    // bulk_insert does not preserve order, so index by pk.
    Map<Integer, Row> byId = new HashMap<>();
    for (Row r : result) {
      byId.put((Integer) r.getField(0), r);
    }
    // Populated rows (even id) round-trip both nested rows -- one from the first (full) batch and
    // one with a large id past the boundary.
    assertPopulatedRow(byId.get(0), 0);
    assertPopulatedRow(byId.get(numRows - 2), numRows - 2);
    // All-null-children rows (odd id) collapse both nested rows back to NULL, including a large id.
    assertCollapsedRow(byId.get(1));
    assertCollapsedRow(byId.get(numRows - 1));
  }

  /** Builds the VALUES row list {@code (0), (1), ..., (n-1)} for the generator cross join. */
  private static String valuesList(int n) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append('(').append(i).append(')');
    }
    return sb.toString();
  }

  /** Asserts the row keyed by an even {@code id} round-trips its populated nested rows. */
  private static void assertPopulatedRow(Row row, int id) {
    assertNotNull(row, "row with pk " + id + " was not read back");
    Row scalar = (Row) row.getField(1);
    assertEquals(id, scalar.getField(0));
    assertEquals("v" + id, scalar.getField(1));
    assertNotNull(((Row) row.getField(2)).getField(0)); // decimal leaf present, not null
  }

  /** Asserts the row keyed by an odd {@code id} had both all-null nested rows collapsed to NULL. */
  private static void assertCollapsedRow(Row row) {
    assertNotNull(row, "expected an odd-id row to be read back");
    assertNull(row.getField(1)); // f_scalar collapsed to null
    assertNull(row.getField(2)); // f_dec collapsed to null
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
  void testParquetDeeplyNestedRepeatedTypes(String operation) {
    // Covers a ROW containing an ARRAY of ROW that itself contains a MAP, i.e.
    // ROW<ARRAY<ROW<INT, MAP<STRING, INT>>>>, where the MAP is a repeated field
    // nested inside another repeated field (repetition level >= 2).
    // See HUDI-18491 for the original bug report on this schema shape.
    TableEnvironment tableEnv = batchTableEnv;

    String hoodieTableDDL = sql("t1")
        .field("f_int int")
        .field("f_row row(f_nested_array array<row(f_score int, f_map map<varchar(10), int>)>)")
        .pkField("f_int")
        .noPartition()
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.OPERATION, operation)
        .end();
    tableEnv.executeSql(hoodieTableDDL);

    execInsertSql(tableEnv, TestSQL.DEEPLY_NESTED_REPEATED_TYPE_INSERT_T1);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    List<Row> expected = Arrays.asList(
        row(1, row((Object) array(row(11, map("a", 1, "b", 2)), row(12, map("c", 3))))),
        row(2, row((Object) array(row(21, map("d", 4))))),
        row(3, row((Object) array(row(31, map("e", 5)), row(32, map("f", 6, "g", 7))))));
    assertRowsEqualsUnordered(expected, result);
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .option(FlinkOptions.ORDERING_FIELDS, "ts")
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
        .option(FlinkOptions.ORDERING_FIELDS, "ts")
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
        .option(FlinkOptions.ORDERING_FIELDS, "ts")
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
        .option(FlinkOptions.ORDERING_FIELDS, "ts")
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
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid");
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning);

    // write the first commit
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false")
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    streamTableEnv.executeSql("drop table t1");
    hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    List<Row> rows = execSelectSqlWithExpectedNum(streamTableEnv, "select * from t1", TestData.DATA_SET_SOURCE_INSERT.size());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
  }

  @Test
  void testReadWithParquetPredicatePushDown() {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .end();
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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
        .options(getDefaultKeys())
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

  @Test
  void testRLIBootstrap() {
    TableEnvironment tableEnv = streamTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name())
        .option(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, true)
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true)
        .option(FlinkOptions.TABLE_TYPE, MERGE_ON_READ.name())
        .end();
    tableEnv.executeSql(hoodieTableDDL);
    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);

    // insert another batch of records
    execInsertSql(tableEnv, TestSQL.UPDATE_INSERT_T1);

    result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_MERGED);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testMiniBatchBucketAssign(HoodieTableType tableType) throws Exception {
    TableEnvironment tableEnv = streamTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name())
        .option(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, false)
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true)
        .option(FlinkOptions.TABLE_TYPE, tableType.name())
        .end();
    tableEnv.executeSql(hoodieTableDDL);
    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);

    // insert another batch of records, so that minibatch lookup results are not empty
    execInsertSql(tableEnv, TestSQL.UPDATE_INSERT_T1);
    result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_MERGED);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testStreamWriteWithManagedMemory(HoodieTableType tableType) throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.WRITE_BUFFER_MEMORY_TYPE, BufferMemoryType.MANAGED)
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);

    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    List<Row> result = CollectionUtil.iteratorToList(
        streamTableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, TestData.DATA_SET_SOURCE_INSERT);
  }

  @ParameterizedTest
  @EnumSource(value = BufferType.class,  names = {"BOUNDED_IN_MEMORY", "DISRUPTOR"})
  void testAppendWriteWithManagedMemory(BufferType bufferType) {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, MERGE_ON_READ)
        .option(FlinkOptions.OPERATION, "insert")
        .option(FlinkOptions.WRITE_BUFFER_MEMORY_TYPE, BufferMemoryType.MANAGED)
        .option(FlinkOptions.WRITE_BUFFER_TYPE, bufferType.name())
        .option(FlinkOptions.RECORD_KEY_FIELD, "uuid")
        .end();
    streamTableEnv.executeSql(hoodieTableDDL);

    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    List<Row> result = CollectionUtil.iteratorToList(
        streamTableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, TestData.DATA_SET_SOURCE_INSERT);
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndBooleanTrueFalseParams")
  void testBatchReadWithLimit(HoodieTableType tableType, boolean useSourceV2) {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
        .end();
    tableEnv.executeSql(hoodieTableDDL);
    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    // limit less than total records: only check the count since row ordering is not guaranteed
    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 limit 3").execute().collect());
    assertThat(result1).hasSize(3);

    // limit equal to total records: all 8 rows should be returned
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 limit 8").execute().collect());
    assertRowsEquals(result2, TestData.DATA_SET_SOURCE_INSERT);

    // limit greater than total records: should return all rows without error
    List<Row> result3 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 limit 100").execute().collect());
    assertRowsEquals(result3, TestData.DATA_SET_SOURCE_INSERT);
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndBooleanTrueFalseParams")
  void testBatchReadWithLimitAndFilter(HoodieTableType tableType, boolean useSourceV2) {
    TableEnvironment tableEnv = batchTableEnv;
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
        .end();
    tableEnv.executeSql(hoodieTableDDL);
    execInsertSql(tableEnv, TestSQL.INSERT_T1);

    // limit with partition filter (par1 has 2 records: id1, id2); limit to 1
    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where `partition` = 'par1' limit 1").execute().collect());
    assertThat(result1).hasSize(1);

    // limit equal to the filtered result count (par4 has exactly 2 records: id7, id8)
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where `partition` = 'par4' limit 2").execute().collect());
    assertRowsEquals(result2, "["
        + "+I[id7, Bob, 44, 1970-01-01T00:00:07, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:08, par4]]");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testIgnoreEmitDeleteForBatchReading(boolean useSourceV2) {
    String hoodieTableDDL = sql("t1")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(getDefaultKeys())
        .option(FlinkOptions.READ_AS_STREAMING, false)
        .option(FlinkOptions.TABLE_TYPE, MERGE_ON_READ)
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
        .end();

    batchTableEnv.executeSql(hoodieTableDDL);
    execInsertSql(batchTableEnv, TestSQL.INSERT_T1);
    // delete EQ(=)
    final String deleteSql = "delete from t1 where uuid = 'id1'";
    execInsertSql(batchTableEnv, deleteSql);
    List<Row> rows1 = CollectionUtil.iterableToList(
        () -> batchTableEnv.sqlQuery("select * from t1").execute().collect());
    List<RowData> expected = TestData.delete(TestData.DATA_SET_SOURCE_INSERT, 0);
    assertRowsEquals(rows1, expected);
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  private enum ExecMode {
    BATCH, STREAM
  }

  public static Map<String, String> getDefaultKeys() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid");
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
    return conf.toMap();
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

  private static Stream<Arguments> cdcSupplementalLoggingModeWithSourceV2() {
    Object[][] data =
        new Object[][] {
            {HoodieCDCSupplementalLoggingMode.DATA_BEFORE, false},
            {HoodieCDCSupplementalLoggingMode.DATA_BEFORE, true},
            {HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER, false},
            {HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER, true},
            {HoodieCDCSupplementalLoggingMode.OP_KEY_ONLY, false},
            {HoodieCDCSupplementalLoggingMode.OP_KEY_ONLY, true}};
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

  /**
   * Return test params => (HoodieTableType, true/false, true/fase).
   */
  private static Stream<Arguments> tableTypeAndSourceV2AndBooleanTrueFalseParams() {
    Object[][] data =
            new Object[][] {
                    {HoodieTableType.COPY_ON_WRITE, false, false},
                    {HoodieTableType.COPY_ON_WRITE, true, true},
                    {HoodieTableType.MERGE_ON_READ, false, false},
                    {HoodieTableType.MERGE_ON_READ, true, true},
                    {HoodieTableType.COPY_ON_WRITE, false, true},
                    {HoodieTableType.COPY_ON_WRITE, true, false},
                    {HoodieTableType.MERGE_ON_READ, false, true},
                    {HoodieTableType.MERGE_ON_READ, true, false}};
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
   * Return test params => (index type, boolean).
   */
  private static Stream<Arguments> indexAndBooleanParams() {
    Object[][] data =
        new Object[][] {
            {"FLINK_STATE", false},
            {"GLOBAL_RECORD_LEVEL_INDEX", false},
            {"GLOBAL_RECORD_LEVEL_INDEX", true}};
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

  /**
   * Return test params => (table type, async lookup).
   */
  private static Stream<Arguments> tableTypeCacheTypeAndAsyncLookupParams() {
    Object[][] data = new Object[][] {
        {HoodieTableType.COPY_ON_WRITE, "heap", false},
        {HoodieTableType.COPY_ON_WRITE, "heap", true},
        {HoodieTableType.MERGE_ON_READ, "heap", false},
        {HoodieTableType.MERGE_ON_READ, "heap", true},
        {HoodieTableType.COPY_ON_WRITE, "rocksdb", false},
        {HoodieTableType.COPY_ON_WRITE, "rocksdb", true},
        {HoodieTableType.MERGE_ON_READ, "rocksdb", false},
        {HoodieTableType.MERGE_ON_READ, "rocksdb", true}
    };
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
    return submitAndFetchWithRetry(tEnv, select, sinkDDL, expectedNum);
  }

  /**
   * Submits a streaming select that collects into the {@link CollectSinkTableFactory} sink and returns
   * the collected rows.
   *
   * <p>The streaming job is terminated by a forced {@link CollectSinkTableFactory.SuccessException} once
   * {@code expectedNum} rows are collected. A benign teardown race (see {@link #isAcceptableTerminalFailure})
   * can instead end the job before all rows are emitted, leaving a short result. Re-reading the already
   * committed table is idempotent, so retry up to {@link #MAX_STREAM_READ_ATTEMPTS} times when the result
   * is short; this keeps the race from surfacing as a confusing row-count assertion failure.
   */
  private List<Row> submitAndFetchWithRetry(TableEnvironment tEnv, String select, String sinkDDL, int expectedNum) {
    List<Row> rows = Collections.emptyList();
    for (int attempt = 1; attempt <= MAX_STREAM_READ_ATTEMPTS; attempt++) {
      TableResult tableResult = submitSelectSql(tEnv, select, sinkDDL);
      rows = fetchResultWithExpectedNum(tEnv, tableResult);
      if (expectedNum <= 0 || rows.size() >= expectedNum) {
        return rows;
      }
      log.warn("Streaming read collected {} of {} expected rows on attempt {}/{}; a tolerated teardown "
              + "race ended the job before the read completed. Retrying. select=[{}]",
          rows.size(), expectedNum, attempt, MAX_STREAM_READ_ATTEMPTS, select);
    }
    return rows;
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
      // Acceptable terminal causes:
      //   1. SuccessException: the sink reached its expected row count and intentionally
      //      threw to terminate the streaming job. This is the happy path.
      //   2. IOException("Stream is closed!") wrapped as HoodieIOException: a benign
      //      error-attribution race between the source-side cascading-shutdown path and
      //      the sink-side SuccessException terminator. When the sink throws
      //      SuccessException to end the job, the chained source's SplitFetcher can close
      //      the underlying Hadoop FSDataInputStream while the mailbox is still draining
      //      a BatchRecords queued earlier; the next row-group read on the now-closed
      //      stream surfaces an IOException("Stream is closed!"). With
      //      restart-strategy.fixed-delay.attempts=0 (set in beforeEach to keep tests
      //      deterministic) that IOException becomes the job's reported failure cause
      //      instead of the sink's SuccessException, even though the sink has already
      //      collected the expected rows by then - i.e. the functional outcome is
      //      unchanged, only the error-attribution differs. Production paths correctly
      //      fail the job on stream-closed-mid-read (the right behavior for real I/O
      //      failures), so this tolerance is scoped to the SuccessException-based test
      //      pattern below and is NOT mirrored in production code.
      //   3. NullPointerException from ParquetColumnarRowSplitReader#readNextRowGroup: the
      //      same benign teardown race as (2), observed with different timing. When the
      //      SplitFetcher's close() fully completes first, ParquetColumnarRowSplitReader#close
      //      nulls out its `reader` field, so the in-flight row-group read on the task thread
      //      surfaces as a NullPointerException (reader.readNextRowGroup() on a null reader)
      //      instead of an IOException("Stream is closed!"). Same functional outcome - the
      //      sink has already collected the expected rows - only the error symptom differs.
      //      Tolerated narrowly (an NPE originating from that exact frame) for the same
      //      reason as (2), and likewise NOT mirrored in production code.
      if (!isAcceptableTerminalFailure(e)) {
        throw new AssertionError("Unexpected job failure", e);
      }
      // The races (2)/(3) usually fire after the sink has collected its expected rows, but can also fire
      // before - ending the read with a short result. Log the tolerated cause so an incomplete read is
      // diagnosable; submitAndFetchWithRetry re-reads when the collected count is below the expectation.
      if (!isSuccessException(e)) {
        log.warn("Streaming read terminated by a tolerated teardown race ({}); collected {} rows so far.",
            describeTerminalCause(e),
            CollectSinkTableFactory.RESULT.values().stream().mapToInt(List::size).sum());
      }
    }
    tEnv.executeSql("DROP TABLE IF EXISTS sink");
    return CollectSinkTableFactory.RESULT.values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  /**
   * Whether {@code e} (or any of its causes) is one of the terminal failures that
   * {@link #fetchResultWithExpectedNum} is allowed to swallow. See the comment at the call
   * site for the rationale.
   */
  private static boolean isAcceptableTerminalFailure(Throwable e) {
    Throwable cur = e;
    while (cur != null) {
      if (cur instanceof CollectSinkTableFactory.SuccessException) {
        return true;
      }
      String msg = cur.getMessage();
      if (msg != null && msg.contains("Stream is closed")) {
        return true;
      }
      // The NPE twin of the "Stream is closed!" teardown race (cause #3 at the call site):
      // a NullPointerException whose own stack trace originates from
      // ParquetColumnarRowSplitReader#readNextRowGroup, i.e. reader.readNextRowGroup() ran on a
      // null `reader` that ParquetColumnarRowSplitReader#close had just nulled out. Scoped to
      // that exact frame so genuine NPEs - and the legitimate IOException("expecting more
      // rows...") thrown from the same method - still fail the test.
      if (isNullPointerException(cur) && containsReadNextRowGroupFrame(cur)) {
        return true;
      }
      cur = cur.getCause();
    }
    return false;
  }

  /**
   * True for a real {@link NullPointerException} as well as one wrapped in Flink's
   * {@code SerializedThrowable} when the failure is propagated back from the cluster (its
   * {@code toString()} preserves the original {@code java.lang.NullPointerException} prefix).
   */
  private static boolean isNullPointerException(Throwable t) {
    return t instanceof NullPointerException
        || t.toString().startsWith(NullPointerException.class.getName());
  }

  /**
   * Whether {@code t}'s stack trace (preserved even through {@code SerializedThrowable})
   * contains a {@code ParquetColumnarRowSplitReader#readNextRowGroup} frame.
   */
  private static boolean containsReadNextRowGroupFrame(Throwable t) {
    for (StackTraceElement frame : t.getStackTrace()) {
      if (frame.getClassName().endsWith("ParquetColumnarRowSplitReader")
          && "readNextRowGroup".equals(frame.getMethodName())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether {@code e} (or any of its causes) is the normal {@link CollectSinkTableFactory.SuccessException}
   * terminator (the happy path), as opposed to one of the tolerated teardown-race symptoms.
   */
  private static boolean isSuccessException(Throwable e) {
    for (Throwable cur = e; cur != null; cur = cur.getCause()) {
      if (cur instanceof CollectSinkTableFactory.SuccessException) {
        return true;
      }
    }
    return false;
  }

  /**
   * Short description of {@code e}'s root cause, for logging which tolerated terminal failure fired.
   */
  private static String describeTerminalCause(Throwable e) {
    Throwable root = e;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return root.getClass().getSimpleName() + ": " + root.getMessage();
  }
}
