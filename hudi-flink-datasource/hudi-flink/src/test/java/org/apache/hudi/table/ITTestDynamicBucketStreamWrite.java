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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestSQL;
import org.apache.hudi.utils.TestTableEnvs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.utils.TestConfigurations.sql;
import static org.apache.hudi.utils.TestData.assertRowsEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for Flink streaming writes with dynamic bucket index.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestDynamicBucketStreamWrite {
  private TableEnvironment streamTableEnv;
  private TableEnvironment batchTableEnv;

  @TempDir
  File tempFile;

  @BeforeEach
  void beforeEach() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    streamTableEnv = TableEnvironmentImpl.create(settings);
    streamTableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    Configuration execConf = streamTableEnv.getConfig().getConfiguration();
    execConf.setString("execution.checkpointing.interval", "2s");
    execConf.setString("restart-strategy", "fixed-delay");
    execConf.setString("restart-strategy.fixed-delay.attempts", "0");

    batchTableEnv = TestTableEnvs.getBatchTableEnv();
    batchTableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndBooleanTrueFalseParams")
  void testWriteAndBootstrapFromRecordIndex(HoodieTableType tableType, boolean partitioned) {
    streamTableEnv.executeSql(getTableDDL("t1", tableType, partitioned));

    final String insertInto1 = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),\n"
        + "('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),\n"
        + "('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par2'),\n"
        + "('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2')";
    execInsertSql(streamTableEnv, insertInto1);

    Map<String, String> initialFileIds = collectFileIds(streamTableEnv, "uuid in ('id1', 'id2')");
    assertThat(initialFileIds).containsOnlyKeys("id1", "id2");
    assertRecordIndexMetadataPartitionExists();

    final String insertInto2 = "insert into t1 values\n"
        + "('id1','Danny',24,TIMESTAMP '1970-01-01 00:00:05','par1'),\n"
        + "('id2','Stephen',34,TIMESTAMP '1970-01-01 00:00:06','par1'),\n"
        + "('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:07','par2')";
    execInsertSql(streamTableEnv, insertInto2);

    List<Row> result = execSelectSql(streamTableEnv, "select uuid, name, age, ts, `partition` from t1");
    assertRowsEquals(result, "["
        + "+I[id1, Danny, 24, 1970-01-01T00:00:05, par1], "
        + "+I[id2, Stephen, 34, 1970-01-01T00:00:06, par1], "
        + "+I[id3, Julian, 53, 1970-01-01T00:00:03, par2], "
        + "+I[id4, Fabian, 31, 1970-01-01T00:00:04, par2], "
        + "+I[id5, Sophia, 18, 1970-01-01T00:00:07, par2]]");

    Map<String, String> updatedFileIds = collectFileIds(streamTableEnv, "uuid in ('id1', 'id2')");
    assertEquals(initialFileIds, updatedFileIds);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testInsertOverwrite(HoodieTableType tableType) {
    batchTableEnv.executeSql(getTableDDL("t1", tableType));
    execInsertSql(batchTableEnv, TestSQL.INSERT_T1);
    String selectPhysicalColumns = "select uuid, name, age, ts, `partition` from t1";

    final String insertOverwritePartition = "insert overwrite t1 partition(`partition`='par1') values\n"
        + "('id1','Danny',24,TIMESTAMP '1970-01-01 00:00:01'),\n"
        + "('id2','Stephen',34,TIMESTAMP '1970-01-01 00:00:02')\n";
    execInsertSql(batchTableEnv, insertOverwritePartition);
    assertRowsEquals(execSelectSql(batchTableEnv, selectPhysicalColumns), TestData.DATA_SET_SOURCE_INSERT_OVERWRITE);

    execInsertSql(batchTableEnv, insertOverwritePartition);
    assertRowsEquals(execSelectSql(batchTableEnv, selectPhysicalColumns), TestData.DATA_SET_SOURCE_INSERT_OVERWRITE);

    final String insertOverwriteDynamicPartition = "insert overwrite t1 /*+ OPTIONS('write.partition.overwrite.mode'='dynamic') */ values\n"
        + "('id1','Danny',24,TIMESTAMP '1970-01-01 00:00:01', 'par1'),\n"
        + "('id2','Stephen',34,TIMESTAMP '1970-01-01 00:00:02', 'par2')\n";
    execInsertSql(batchTableEnv, insertOverwriteDynamicPartition);
    assertRowsEquals(execSelectSql(batchTableEnv, selectPhysicalColumns), TestData.DATA_SET_SOURCE_INSERT_OVERWRITE_DYNAMIC_PARTITION);

    execInsertSql(batchTableEnv, insertOverwriteDynamicPartition);
    assertRowsEquals(execSelectSql(batchTableEnv, selectPhysicalColumns), TestData.DATA_SET_SOURCE_INSERT_OVERWRITE_DYNAMIC_PARTITION);

    final String insertOverwriteTable = "insert overwrite t1 values\n"
        + "('id1','Danny',24,TIMESTAMP '1970-01-01 00:00:01', 'par1'),\n"
        + "('id2','Stephen',34,TIMESTAMP '1970-01-01 00:00:02', 'par2')\n";
    final String expected = "["
        + "+I[id1, Danny, 24, 1970-01-01T00:00:01, par1], "
        + "+I[id2, Stephen, 34, 1970-01-01T00:00:02, par2]]";
    execInsertSql(batchTableEnv, insertOverwriteTable);
    assertRowsEquals(execSelectSql(batchTableEnv, selectPhysicalColumns), expected);

    execInsertSql(batchTableEnv, insertOverwriteTable);
    assertRowsEquals(execSelectSql(batchTableEnv, selectPhysicalColumns), expected);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testBucketScalesUpWithContinuousWrites(HoodieTableType tableType) {
    streamTableEnv.executeSql(getTableDDL(
        "t1", tableType, Collections.singletonMap(HoodieCompactionConfig.COPY_ON_WRITE_INSERT_SPLIT_SIZE.key(), "1"), true));

    execInsertSql(streamTableEnv, "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par_scale'),\n"
        + "('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par_scale'),\n"
        + "('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par_scale'),\n"
        + "('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par_scale')");

    execInsertSql(streamTableEnv, "insert into t1 values\n"
        + "('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par_scale'),\n"
        + "('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06','par_scale'),\n"
        + "('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07','par_scale'),\n"
        + "('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08','par_scale')");

    Map<String, String> fileIdsByRecordKey = collectFileIds(streamTableEnv, "`partition` = 'par_scale'");
    assertThat(fileIdsByRecordKey).hasSize(8);
    Set<String> fileIds = new HashSet<>(fileIdsByRecordKey.values());
    assertThat(fileIds).hasSizeGreaterThan(2);
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

  private String getTableDDL(String tableName, HoodieTableType tableType) {
    return getTableDDL(tableName, tableType, Collections.emptyMap(), true);
  }

  private String getTableDDL(String tableName, HoodieTableType tableType, boolean partitioned) {
    return getTableDDL(tableName, tableType, Collections.emptyMap(), partitioned);
  }

  private String getTableDDL(String tableName, HoodieTableType tableType, Map<String, String> extraOptions, boolean partitioned) {
    TestConfigurations.Sql ddl = sql(tableName)
        .field("_hoodie_file_name STRING METADATA VIRTUAL")
        .field("uuid varchar(20)")
        .field("name varchar(10)")
        .field("age int")
        .field("ts timestamp(3)")
        .field("`partition` varchar(20)")
        .options(getDefaultKeys())
        .options(extraOptions)
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.RECORD_LEVEL_INDEX.name())
        .option(FlinkOptions.BUCKET_ASSIGN_TASKS, 2)
        .option(FlinkOptions.WRITE_TASKS, 2)
        .option(FlinkOptions.INDEX_WRITE_TASKS, 2);
    if (!partitioned) {
      ddl.noPartition();
    }
    return ddl.end();
  }

  private static Map<String, String> getDefaultKeys() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid");
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
    return conf.toMap();
  }

  private void assertRecordIndexMetadataPartitionExists() {
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    String mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(tempFile.getAbsolutePath());
    assertTrue(StreamerUtil.tableExists(mdtBasePath, hadoopConf),
        "Metadata table should exist for table with dynamic bucket index");
    assertTrue(StreamerUtil.partitionExists(mdtBasePath, MetadataPartitionType.RECORD_INDEX.getPartitionPath(), hadoopConf),
        "RECORD_INDEX partition should exist for table with dynamic bucket index");
  }

  private Map<String, String> collectFileIds(TableEnvironment tableEnv, String condition) {
    List<Row> rows = execSelectSql(tableEnv, "select uuid, _hoodie_file_name from t1 where " + condition);
    return rows.stream().collect(Collectors.toMap(
        row -> row.getField(0).toString(),
        row -> FSUtils.getFileId(row.getField(1).toString())));
  }

  private List<Row> execSelectSql(TableEnvironment tableEnv, String select) {
    return CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery(select).execute().collect());
  }

  private void execInsertSql(TableEnvironment tableEnv, String insert) {
    TableResult tableResult = tableEnv.executeSql(insert);
    try {
      tableResult.await();
    } catch (InterruptedException | ExecutionException ex) {
      throw new AssertionError("Failed to execute insert SQL: " + insert, ex);
    }
  }
}
