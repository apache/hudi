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

import org.apache.hudi.operator.FlinkOptions;
import org.apache.hudi.operator.utils.TestConfigurations;

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

import java.io.File;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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
  void testStreamWriteBatchRead() {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.READ_SCHEMA_FILE_PATH.key(),
        Objects.requireNonNull(Thread.currentThread()
            .getContextClassLoader().getResource("test_read_schema.avsc")).toString());
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    List<Row> rows = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    final String expected = "[id1,Danny,23,1970-01-01T00:00:01,par1, "
        + "id2,Stephen,33,1970-01-01T00:00:02,par1, "
        + "id3,Julian,53,1970-01-01T00:00:03,par2, "
        + "id4,Fabian,31,1970-01-01T00:00:04,par2, "
        + "id5,Sophia,18,1970-01-01T00:00:05,par3, "
        + "id6,Emma,20,1970-01-01T00:00:06,par3, "
        + "id7,Bob,44,1970-01-01T00:00:07,par4, "
        + "id8,Han,56,1970-01-01T00:00:08,par4]";
    assertRowsEquals(rows, expected);
  }

  @Test
  void testBatchWriteAndRead() {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.READ_SCHEMA_FILE_PATH.key(),
        Objects.requireNonNull(Thread.currentThread()
            .getContextClassLoader().getResource("test_read_schema.avsc")).toString());
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    batchTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),\n"
        + "('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),\n"
        + "('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par2'),\n"
        + "('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2'),\n"
        + "('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3'),\n"
        + "('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06','par3'),\n"
        + "('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07','par4'),\n"
        + "('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08','par4')";

    execInsertSql(batchTableEnv, insertInto);

    List<Row> rows = CollectionUtil.iterableToList(
        () -> batchTableEnv.sqlQuery("select * from t1").execute().collect());
    final String expected = "[id1,Danny,23,1970-01-01T00:00:01,par1, "
        + "id2,Stephen,33,1970-01-01T00:00:02,par1, "
        + "id3,Julian,53,1970-01-01T00:00:03,par2, "
        + "id4,Fabian,31,1970-01-01T00:00:04,par2, "
        + "id5,Sophia,18,1970-01-01T00:00:05,par3, "
        + "id6,Emma,20,1970-01-01T00:00:06,par3, "
        + "id7,Bob,44,1970-01-01T00:00:07,par4, "
        + "id8,Han,56,1970-01-01T00:00:08,par4]";
    assertRowsEquals(rows, expected);
  }

  /**
   * Sort the {@code rows} using field at index 0 and asserts
   * it equals with the expected string {@code expected}.
   *
   * @param rows     Actual result rows
   * @param expected Expected string of the sorted rows
   */
  private static void assertRowsEquals(List<Row> rows, String expected) {
    String rowsString = rows.stream()
        .sorted(Comparator.comparing(o -> o.getField(0).toString()))
        .collect(Collectors.toList()).toString();
    assertThat(rowsString, is(expected));
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
}
