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

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestUtils;
import org.apache.hudi.utils.factory.CollectSinkTableFactory;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hudi.utils.TestData.assertRowsEquals;

/**
 * IT cases for Hoodie table source and sink.
 *
 * Note: should add more SQL cases when batch write is supported.
 */
public class HoodieDataSourceITCase extends AbstractTestBase {
  private TableEnvironment streamTableEnv;
  private TableEnvironment batchTableEnv;

  @BeforeEach
  void beforeEach() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    streamTableEnv = TableEnvironmentImpl.create(settings);
    streamTableEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
    streamTableEnv.getConfig().getConfiguration()
        .setString("execution.checkpointing.interval", "2s");

    settings = EnvironmentSettings.newInstance().inBatchMode().build();
    batchTableEnv = TableEnvironmentImpl.create(settings);
    batchTableEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
  }

  @TempDir
  File tempFile;

  @Test
  void testStreamWriteAndRead() throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.READ_AS_STREAMING.key(), "true");
    options.put(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    List<Row> rows = execSelectSql(streamTableEnv, "select * from t1", 10);
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);

    // insert another batch of data
    execInsertSql(streamTableEnv, insertInto);
    List<Row> rows2 = execSelectSql(streamTableEnv, "select * from t1", 10);
    assertRowsEquals(rows2, TestData.DATA_SET_SOURCE_INSERT);
  }

  @Test
  void testStreamReadAppendData() throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    String createSource2 = TestConfigurations.getFileSourceDDL("source2", "test_source2.data");
    streamTableEnv.executeSql(createSource);
    streamTableEnv.executeSql(createSource2);

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.READ_AS_STREAMING.key(), "true");
    options.put(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    String createHoodieTable = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    streamTableEnv.executeSql(createHoodieTable);
    String insertInto = "insert into t1 select * from source";
    // execute 2 times
    execInsertSql(streamTableEnv, insertInto);
    // remember the commit
    String specifiedCommit = TestUtils.getFirstCommit(tempFile.getAbsolutePath());
    // another update batch
    String insertInto2 = "insert into t1 select * from source2";
    execInsertSql(streamTableEnv, insertInto2);
    // now we consume starting from the oldest commit
    options.put(FlinkOptions.READ_STREAMING_START_COMMIT.key(), specifiedCommit);
    String createHoodieTable2 = TestConfigurations.getCreateHoodieTableDDL("t2", options);
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

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
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

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    // read optimized is supported for both MOR and COR table,
    // test MOR streaming write with compaction then reads as
    // query type 'read_optimized'.
    options.put(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    options.put(FlinkOptions.QUERY_TYPE.key(), FlinkOptions.QUERY_TYPE_READ_OPTIMIZED);
    options.put(FlinkOptions.COMPACTION_DELTA_COMMITS.key(), "1");
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    List<Row> rows = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
  }

  @ParameterizedTest
  @EnumSource(value = ExecMode.class)
  void testWriteAndRead(ExecMode execMode) {
    TableEnvironment tableEnv = execMode == ExecMode.BATCH ? batchTableEnv : streamTableEnv;
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),\n"
        + "('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),\n"
        + "('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par2'),\n"
        + "('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2'),\n"
        + "('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3'),\n"
        + "('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06','par3'),\n"
        + "('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07','par4'),\n"
        + "('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08','par4')";

    execInsertSql(tableEnv, insertInto);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);
    // apply filters
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where uuid > 'id5'").execute().collect());
    assertRowsEquals(result2, "["
        + "id6,Emma,20,1970-01-01T00:00:06,par3, "
        + "id7,Bob,44,1970-01-01T00:00:07,par4, "
        + "id8,Han,56,1970-01-01T00:00:08,par4]");
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  private enum ExecMode {
    BATCH, STREAM
  }

  private void execInsertSql(TableEnvironment tEnv, String insert) {
    TableResult tableResult = tEnv.executeSql(insert);
    // wait to finish
    try {
      tableResult.getJobClient().get()
          .getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
    } catch (InterruptedException | ExecutionException ex) {
      throw new RuntimeException(ex);
    }
  }

  private List<Row> execSelectSql(TableEnvironment tEnv, String select, long timeout) throws InterruptedException {
    tEnv.executeSql(TestConfigurations.getCollectSinkDDL("sink"));
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
