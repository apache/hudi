/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.utils.FlinkMiniCluster;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.AFTER;
import static org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.BEFORE;
import static org.apache.hudi.utils.TestConfigurations.ROW_TYPE_EVOLUTION_AFTER;
import static org.apache.hudi.utils.TestConfigurations.ROW_TYPE_EVOLUTION_BEFORE;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
@ExtendWith(FlinkMiniCluster.class)
public class ITTestSchemaEvolution {

  @TempDir File tempFile;
  private StreamTableEnvironment tEnv;

  @BeforeEach
  public void setUp() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
    tEnv = StreamTableEnvironment.create(env);
  }

  @Test
  public void testCopyOnWriteInputFormat() throws Exception {
    testSchemaEvolution(defaultTableOptions(tempFile.getAbsolutePath()));
  }

  @Test
  public void testMergeOnReadInputFormatBaseFileOnlyIterator() throws Exception {
    TableOptions tableOptions = defaultTableOptions(tempFile.getAbsolutePath())
        .withOption(FlinkOptions.READ_AS_STREAMING.key(), true)
        .withOption(FlinkOptions.READ_START_COMMIT.key(), FlinkOptions.START_COMMIT_EARLIEST);
    testSchemaEvolution(tableOptions);
  }

  @Test
  public void testMergeOnReadInputFormatBaseFileOnlyFilteringIterator() throws Exception {
    TableOptions tableOptions = defaultTableOptions(tempFile.getAbsolutePath())
        .withOption(FlinkOptions.READ_AS_STREAMING.key(), true)
        .withOption(FlinkOptions.READ_START_COMMIT.key(), 1);
    testSchemaEvolution(tableOptions);
  }

  @Test
  public void testMergeOnReadInputFormatLogFileOnlyIteratorGetLogFileIterator() throws Exception {
    TableOptions tableOptions = defaultTableOptions(tempFile.getAbsolutePath())
        .withOption(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    testSchemaEvolution(tableOptions);
  }

  @Test
  public void testMergeOnReadInputFormatLogFileOnlyIteratorGetUnMergedLogFileIterator() throws Exception {
    TableOptions tableOptions = defaultTableOptions(tempFile.getAbsolutePath())
        .withOption(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .withOption(FlinkOptions.READ_AS_STREAMING.key(), true)
        .withOption(FlinkOptions.READ_START_COMMIT.key(), FlinkOptions.START_COMMIT_EARLIEST)
        .withOption(FlinkOptions.CHANGELOG_ENABLED.key(), true);
    testSchemaEvolution(tableOptions, false, EXPECTED_UNMERGED_RESULT);
  }

  @Test
  public void testMergeOnReadInputFormatMergeIterator() throws Exception {
    TableOptions tableOptions = defaultTableOptions(tempFile.getAbsolutePath())
        .withOption(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .withOption(FlinkOptions.COMPACTION_DELTA_COMMITS.key(), 1);
    testSchemaEvolution(tableOptions, true);
  }

  @Test
  public void testMergeOnReadInputFormatSkipMergeIterator() throws Exception {
    TableOptions tableOptions = defaultTableOptions(tempFile.getAbsolutePath())
        .withOption(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .withOption(FlinkOptions.COMPACTION_DELTA_COMMITS.key(), 1)
        .withOption(FlinkOptions.MERGE_TYPE.key(), FlinkOptions.REALTIME_SKIP_MERGE);
    testSchemaEvolution(tableOptions, true, EXPECTED_UNMERGED_RESULT);
  }

  @Test
  public void testCompaction() throws Exception {
    TableOptions tableOptions = defaultTableOptions(tempFile.getAbsolutePath())
        .withOption(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        .withOption(FlinkOptions.COMPACTION_DELTA_COMMITS.key(), 1);
    testSchemaEvolution(tableOptions);
    try (HoodieFlinkWriteClient<?> writeClient = FlinkWriteClients.createWriteClient(tableOptions.toConfig())) {
      Option<String> compactionInstant = writeClient.scheduleCompaction(Option.empty());
      writeClient.compact(compactionInstant.get());
    }
    checkAnswerEvolved(EXPECTED_MERGED_RESULT.evolvedRows);
  }

  private void testSchemaEvolution(TableOptions tableOptions) throws Exception {
    testSchemaEvolution(tableOptions, false);
  }

  private void testSchemaEvolution(TableOptions tableOptions, boolean shouldCompact) throws Exception {
    testSchemaEvolution(tableOptions, shouldCompact, EXPECTED_MERGED_RESULT);
  }

  private void testSchemaEvolution(TableOptions tableOptions, boolean shouldCompact, ExpectedResult expectedResult) throws Exception {
    writeTableWithSchema1(tableOptions);
    changeTableSchema(tableOptions, shouldCompact);
    writeTableWithSchema2(tableOptions);
    checkAnswerEvolved(expectedResult.evolvedRows);
    checkAnswerCount(expectedResult.rowCount);
    checkAnswerWithMeta(tableOptions, expectedResult.rowsWithMeta);
  }

  private void writeTableWithSchema1(TableOptions tableOptions) throws ExecutionException, InterruptedException {
    //language=SQL
    tEnv.executeSql(""
        + "create table t1 ("
        + "  uuid string,"
        + "  name string,"
        + "  gender char,"
        + "  age int,"
        + "  ts timestamp,"
        + "  `partition` string"
        + ") partitioned by (`partition`) with (" + tableOptions + ")"
    );
    //language=SQL
    tEnv.executeSql(""
        + "insert into t1 select "
        + "  cast(uuid as string),"
        + "  cast(name as string),"
        + "  cast(gender as char),"
        + "  cast(age as int),"
        + "  cast(ts as timestamp),"
        + "  cast(`partition` as string) "
        + "from (values "
        + "  ('id1', 'Danny', 'M', 23, '2000-01-01 00:00:01', 'par1'),"
        + "  ('id2', 'Stephen', 'M', 33, '2000-01-01 00:00:02', 'par1'),"
        + "  ('id3', 'Julian', 'M', 53, '2000-01-01 00:00:03', 'par2'),"
        + "  ('id4', 'Fabian', 'M', 31, '2000-01-01 00:00:04', 'par2'),"
        + "  ('id5', 'Sophia', 'F', 18, '2000-01-01 00:00:05', 'par3'),"
        + "  ('id6', 'Emma', 'F', 20, '2000-01-01 00:00:06', 'par3'),"
        + "  ('id7', 'Bob', 'M', 44, '2000-01-01 00:00:07', 'par4'),"
        + "  ('id8', 'Han', 'M', 56, '2000-01-01 00:00:08', 'par4')"
        + ") as A(uuid, name, gender, age, ts, `partition`)"
    ).await();
  }

  private void changeTableSchema(TableOptions tableOptions, boolean shouldCompactBeforeSchemaChanges) throws IOException {
    try (HoodieFlinkWriteClient<?> writeClient = FlinkWriteClients.createWriteClient(tableOptions.toConfig())) {
      if (shouldCompactBeforeSchemaChanges) {
        Option<String> compactionInstant = writeClient.scheduleCompaction(Option.empty());
        writeClient.compact(compactionInstant.get());
      }
      Schema doubleType = SchemaBuilder.unionOf().nullType().and().doubleType().endUnion();
      Schema stringType = SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
      writeClient.addColumn("salary", doubleType, null, "name", AFTER);
      writeClient.deleteColumns("gender");
      writeClient.renameColumn("name", "first_name");
      writeClient.updateColumnType("age", Types.StringType.get());
      writeClient.addColumn("last_name", stringType, "empty allowed", "salary", BEFORE);
      writeClient.reOrderColPosition("age", "first_name", BEFORE);
    }
  }

  private void writeTableWithSchema2(TableOptions tableOptions) throws ExecutionException, InterruptedException {
    tableOptions.withOption(
        FlinkOptions.SOURCE_AVRO_SCHEMA.key(),
        AvroSchemaConverter.convertToSchema(ROW_TYPE_EVOLUTION_AFTER, "hoodie.t1.t1_record"));

    //language=SQL
    tEnv.executeSql("drop table t1");
    //language=SQL
    tEnv.executeSql(""
        + "create table t1 ("
        + "  uuid string,"
        + "  age string,"
        + "  first_name string,"
        + "  last_name string,"
        + "  salary double,"
        + "  ts timestamp,"
        + "  `partition` string"
        + ") partitioned by (`partition`) with (" + tableOptions + ")"
    );
    //language=SQL
    tEnv.executeSql(""
        + "insert into t1 select "
        + "  cast(uuid as string),"
        + "  cast(age as string),"
        + "  cast(first_name as string),"
        + "  cast(last_name as string),"
        + "  cast(salary as double),"
        + "  cast(ts as timestamp),"
        + "  cast(`partition` as string) "
        + "from (values "
        + "  ('id1', '23', 'Danny', '', 10000.1, '2000-01-01 00:00:01', 'par1'),"
        + "  ('id9', 'unknown', 'Alice', '', 90000.9, '2000-01-01 00:00:09', 'par1'),"
        + "  ('id3', '53', 'Julian', '', 30000.3, '2000-01-01 00:00:03', 'par2')"
        + ") as A(uuid, age, first_name, last_name, salary, ts, `partition`)"
    ).await();
  }

  private TableOptions defaultTableOptions(String tablePath) {
    return new TableOptions(
        FactoryUtil.CONNECTOR.key(), HoodieTableFactory.FACTORY_ID,
        FlinkOptions.PATH.key(), tablePath,
        FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_COPY_ON_WRITE,
        HoodieTableConfig.NAME.key(), "t1",
        FlinkOptions.READ_AS_STREAMING.key(), false,
        FlinkOptions.QUERY_TYPE.key(), FlinkOptions.QUERY_TYPE_SNAPSHOT,
        KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "uuid",
        KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition",
        KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), true,
        HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), ComplexAvroKeyGenerator.class.getName(),
        FlinkOptions.WRITE_BATCH_SIZE.key(), 0.000001, // each record triggers flush
        FlinkOptions.SOURCE_AVRO_SCHEMA.key(), AvroSchemaConverter.convertToSchema(ROW_TYPE_EVOLUTION_BEFORE),
        FlinkOptions.READ_TASKS.key(), 1,
        FlinkOptions.WRITE_TASKS.key(), 1,
        FlinkOptions.INDEX_BOOTSTRAP_TASKS.key(), 1,
        FlinkOptions.BUCKET_ASSIGN_TASKS.key(), 1,
        FlinkOptions.COMPACTION_TASKS.key(), 1,
        FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key(), false,
        HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), true);
  }

  private void checkAnswerEvolved(String... expectedResult) throws Exception {
    //language=SQL
    checkAnswer("select first_name, salary, age from t1", expectedResult);
  }

  private void checkAnswerCount(String... expectedResult) throws Exception {
    //language=SQL
    checkAnswer("select count(*) from t1", expectedResult);
  }

  private void checkAnswerWithMeta(TableOptions tableOptions, String... expectedResult) throws Exception {
    //language=SQL
    tEnv.executeSql("drop table t1");
    //language=SQL
    tEnv.executeSql(""
        + "create table t1 ("
        + "  `_hoodie_commit_time` string,"
        + "  `_hoodie_commit_seqno` string,"
        + "  `_hoodie_record_key` string,"
        + "  `_hoodie_partition_path` string,"
        + "  `_hoodie_file_name` string,"
        + "  uuid string,"
        + "  age string,"
        + "  first_name string,"
        + "  last_name string,"
        + "  salary double,"
        + "  ts timestamp,"
        + "  `partition` string"
        + ") partitioned by (`partition`) with (" + tableOptions + ")"
    );
    //language=SQL
    checkAnswer("select `_hoodie_record_key`, first_name, salary from t1", expectedResult);
  }

  private void checkAnswer(String query, String... expectedResult) throws Exception {
    TableResult actualResult = tEnv.executeSql(query);
    Set<String> expected = new HashSet<>(Arrays.asList(expectedResult));
    Set<String> actual = new HashSet<>(expected.size());
    try (CloseableIterator<Row> iterator = actualResult.collect()) {
      for (int i = 0; i < expected.size() && iterator.hasNext(); i++) {
        actual.add(iterator.next().toString());
      }
    }
    assertEquals(expected, actual);
  }

  private static final class TableOptions {
    private final Map<String, String> map = new HashMap<>();

    TableOptions(Object... options) {
      Preconditions.checkArgument(options.length % 2 == 0);
      for (int i = 0; i < options.length; i += 2) {
        withOption(options[i].toString(), options[i + 1]);
      }
    }

    TableOptions withOption(String optionName, Object optionValue) {
      if (StringUtils.isNullOrEmpty(optionName)) {
        throw new IllegalArgumentException("optionName must be presented");
      }
      map.put(optionName, optionValue.toString());
      return this;
    }

    Configuration toConfig() {
      return FlinkOptions.fromMap(map);
    }

    @Override
    public String toString() {
      return map.entrySet().stream()
          .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
          .collect(Collectors.joining(", "));
    }
  }

  private static final class ExpectedResult {
    final String[] evolvedRows;
    final String[] rowsWithMeta;
    final String[] rowCount;

    private ExpectedResult(String[] evolvedRows, String[] rowsWithMeta, String[] rowCount) {
      this.evolvedRows = evolvedRows;
      this.rowsWithMeta = rowsWithMeta;
      this.rowCount = rowCount;
    }
  }

  private static final ExpectedResult EXPECTED_MERGED_RESULT = new ExpectedResult(
      new String[] {
          "+I[Danny, 10000.1, 23]",
          "+I[Stephen, null, 33]",
          "+I[Julian, 30000.3, 53]",
          "+I[Fabian, null, 31]",
          "+I[Sophia, null, 18]",
          "+I[Emma, null, 20]",
          "+I[Bob, null, 44]",
          "+I[Han, null, 56]",
          "+I[Alice, 90000.9, unknown]",
      },
      new String[] {
          "+I[uuid:id1, Danny, 10000.1]",
          "+I[uuid:id2, Stephen, null]",
          "+I[uuid:id3, Julian, 30000.3]",
          "+I[uuid:id4, Fabian, null]",
          "+I[uuid:id5, Sophia, null]",
          "+I[uuid:id6, Emma, null]",
          "+I[uuid:id7, Bob, null]",
          "+I[uuid:id8, Han, null]",
          "+I[uuid:id9, Alice, 90000.9]",
      },
      new String[] {
          "+I[1]",
          "-U[1]",
          "+U[2]",
          "-U[2]",
          "+U[3]",
          "-U[3]",
          "+U[4]",
          "-U[4]",
          "+U[5]",
          "-U[5]",
          "+U[6]",
          "-U[6]",
          "+U[7]",
          "-U[7]",
          "+U[8]",
          "-U[8]",
          "+U[9]",
      }
  );

  private static final ExpectedResult EXPECTED_UNMERGED_RESULT = new ExpectedResult(
      new String[] {
          "+I[Danny, null, 23]",
          "+I[Stephen, null, 33]",
          "+I[Julian, null, 53]",
          "+I[Fabian, null, 31]",
          "+I[Sophia, null, 18]",
          "+I[Emma, null, 20]",
          "+I[Bob, null, 44]",
          "+I[Han, null, 56]",
          "+I[Alice, 90000.9, unknown]",
          "+I[Danny, 10000.1, 23]",
          "+I[Julian, 30000.3, 53]",
      },
      new String[] {
          "+I[uuid:id1, Danny, null]",
          "+I[uuid:id2, Stephen, null]",
          "+I[uuid:id3, Julian, null]",
          "+I[uuid:id4, Fabian, null]",
          "+I[uuid:id5, Sophia, null]",
          "+I[uuid:id6, Emma, null]",
          "+I[uuid:id7, Bob, null]",
          "+I[uuid:id8, Han, null]",
          "+I[uuid:id9, Alice, 90000.9]",
          "+I[uuid:id1, Danny, 10000.1]",
          "+I[uuid:id3, Julian, 30000.3]",
      },
      new String[] {
          "+I[1]",
          "-U[1]",
          "+U[2]",
          "-U[2]",
          "+U[3]",
          "-U[3]",
          "+U[4]",
          "-U[4]",
          "+U[5]",
          "-U[5]",
          "+U[6]",
          "-U[6]",
          "+U[7]",
          "-U[7]",
          "+U[8]",
          "-U[8]",
          "+U[9]",
          "-U[9]",
          "+U[10]",
          "-U[10]",
          "+U[11]",
      }
  );
}
