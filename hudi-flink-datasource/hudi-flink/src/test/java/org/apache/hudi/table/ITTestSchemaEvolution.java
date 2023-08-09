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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
        + "  f_struct row<f0 int, f1 string, drop_add string, change_type int>,"
        + "  f_map map<string, int>,"
        + "  f_array array<int>,"
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
        + "  cast(f_struct as row<f0 int, f1 string, drop_add string, change_type int>),"
        + "  cast(f_map as map<string, int>),"
        + "  cast(f_array as array<int>),"
        + "  cast(`partition` as string) "
        + "from (values "
        + "  ('id0', 'Indica', 'F', 12, '2000-01-01 00:00:00', cast(null as row<f0 int, f1 string, drop_add string, change_type int>), map['Indica', 1212], array[12], 'par0'),"
        + "  ('id1', 'Danny', 'M', 23, '2000-01-01 00:00:01', row(1, 's1', '', 1), map['Danny', 2323], array[23, 23], 'par1'),"
        + "  ('id2', 'Stephen', 'M', 33, '2000-01-01 00:00:02', row(2, 's2', '', 2), map['Stephen', 3333], array[33], 'par1'),"
        + "  ('id3', 'Julian', 'M', 53, '2000-01-01 00:00:03', row(3, 's3', '', 3), map['Julian', 5353], array[53, 53], 'par2'),"
        + "  ('id4', 'Fabian', 'M', 31, '2000-01-01 00:00:04', row(4, 's4', '', 4), map['Fabian', 3131], array[31], 'par2'),"
        + "  ('id5', 'Sophia', 'F', 18, '2000-01-01 00:00:05', row(5, 's5', '', 5), map['Sophia', 1818], array[18, 18], 'par3'),"
        + "  ('id6', 'Emma', 'F', 20, '2000-01-01 00:00:06', row(6, 's6', '', 6), map['Emma', 2020], array[20], 'par3'),"
        + "  ('id7', 'Bob', 'M', 44, '2000-01-01 00:00:07', row(7, 's7', '', 7), map['Bob', 4444], array[44, 44], 'par4'),"
        + "  ('id8', 'Han', 'M', 56, '2000-01-01 00:00:08', row(8, 's8', '', 8), map['Han', 5656], array[56, 56, 56], 'par4')"
        + ") as A(uuid, name, gender, age, ts, f_struct, f_map, f_array, `partition`)"
    ).await();
  }

  private void changeTableSchema(TableOptions tableOptions, boolean shouldCompactBeforeSchemaChanges) throws IOException {
    try (HoodieFlinkWriteClient<?> writeClient = FlinkWriteClients.createWriteClient(tableOptions.toConfig())) {
      if (shouldCompactBeforeSchemaChanges) {
        Option<String> compactionInstant = writeClient.scheduleCompaction(Option.empty());
        writeClient.compact(compactionInstant.get());
      }

      Schema intType = SchemaBuilder.unionOf().nullType().and().intType().endUnion();
      Schema doubleType = SchemaBuilder.unionOf().nullType().and().doubleType().endUnion();
      Schema stringType = SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
      Schema structType = SchemaBuilder.builder().record("new_row_col").fields()
              .name("f0").type("long").noDefault()
              .name("f1").type("string").noDefault().endRecord();
      Schema arrayType = Schema.createUnion(SchemaBuilder.builder().array().items(stringType), SchemaBuilder.builder().nullType());
      Schema mapType = Schema.createUnion(SchemaBuilder.builder().map().values(stringType), SchemaBuilder.builder().nullType());

      writeClient.addColumn("salary", doubleType, null, "name", AFTER);
      writeClient.deleteColumns("gender");
      writeClient.renameColumn("name", "first_name");
      writeClient.updateColumnType("age", Types.StringType.get());
      writeClient.addColumn("last_name", stringType, "empty allowed", "salary", BEFORE);
      writeClient.reOrderColPosition("age", "first_name", BEFORE);
      // add a field in the middle of the `f_struct` column
      writeClient.addColumn("f_struct.f2", intType, "add field in middle of struct", "f_struct.f0", AFTER);
      // add a field at the end of `f_struct` column
      writeClient.addColumn("f_struct.f3", stringType);

      // delete and add a field with the same name
      // reads should not return previously inserted datum of dropped field of the same name
      writeClient.deleteColumns("f_struct.drop_add");
      writeClient.addColumn("f_struct.drop_add", doubleType);

      // perform comprehensive evolution on complex types (struct, array, map) by promoting its primitive types
      writeClient.updateColumnType("f_struct.change_type", Types.LongType.get());
      writeClient.renameColumn("f_struct.change_type", "renamed_change_type");
      writeClient.updateColumnType("f_array.element", Types.DoubleType.get());
      writeClient.updateColumnType("f_map.value", Types.DoubleType.get());

      // perform comprehensive schema evolution on table by adding complex typed columns
      writeClient.addColumn("new_row_col", structType);
      writeClient.addColumn("new_array_col", arrayType);
      writeClient.addColumn("new_map_col", mapType);
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
        + "  f_struct row<f0 int, f2 int, f1 string, renamed_change_type bigint, f3 string, drop_add string>,"
        + "  f_map map<string, double>,"
        + "  f_array array<double>,"
        + "  new_row_col row<f0 bigint, f1 string>,"
        + "  new_array_col array<string>,"
        + "  new_map_col map<string, string>,"
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
        + "  cast(f_struct as row<f0 int, f2 int, f1 string, renamed_change_type bigint, f3 string, drop_add string>),"
        + "  cast(f_map as map<string, double>),"
        + "  cast(f_array as array<double>),"
        + "  cast(new_row_col as row<f0 bigint, f1 string>),"
        + "  cast(new_array_col as array<string>),"
        + "  cast(new_map_col as map<string, string>),"
        + "  cast(`partition` as string) "
        + "from (values "
        + "  ('id1', '23', 'Danny', '', 10000.1, '2000-01-01 00:00:01', row(1, 1, 's1', 11, 't1', 'drop_add1'), map['Danny', 2323.23], array[23, 23, 23], "
        + "  row(1, '1'), array['1'], Map['k1','v1'], 'par1'),"
        + "  ('id9', 'unknown', 'Alice', '', 90000.9, '2000-01-01 00:00:09', row(9, 9, 's9', 99, 't9', 'drop_add9'), map['Alice', 9999.99], array[9999, 9999], "
        + "  row(9, '9'), array['9'], Map['k9','v9'], 'par1'),"
        + "  ('id3', '53', 'Julian', '', 30000.3, '2000-01-01 00:00:03', row(3, 3, 's3', 33, 't3', 'drop_add3'), map['Julian', 5353.53], array[53], "
        + "  row(3, '3'), array['3'], Map['k3','v3'], 'par2')"
        + ") as A(uuid, age, first_name, last_name, salary, ts, f_struct, f_map, f_array, new_row_col, new_array_col, new_map_col, `partition`)"
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
        HoodieWriteConfig.EMBEDDED_TIMELINE_SERVER_REUSE_ENABLED.key(), false,
        HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), true);
  }

  private void checkAnswerEvolved(String... expectedResult) throws Exception {
    //language=SQL
    checkAnswer(""
        + "select "
        + "  first_name, "
        + "  salary, "
        + "  age, "
        + "  f_struct, "
        + "  f_map, "
        + "  f_array, "
        + "  new_row_col, "
        + "  new_array_col, "
        + "  new_map_col "
        + "from t1", expectedResult);
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
        + "  f_struct row<f0 int, f2 int, f1 string, renamed_change_type bigint, f3 string, drop_add string>,"
        + "  f_map map<string, double>,"
        + "  f_array array<double>,"
        + "  new_row_col row<f0 bigint, f1 string>,"
        + "  new_array_col array<string>,"
        + "  new_map_col map<string, string>,"
        + "  `partition` string"
        + ") partitioned by (`partition`) with (" + tableOptions + ")"
    );
    //language=SQL
    checkAnswer(""
        + "select "
        + "  `_hoodie_record_key`, "
        + "  first_name, "
        + "  salary, "
        + "  age, "
        + "  f_struct, "
        + "  f_map, "
        + "  f_array, "
        + "  new_row_col, "
        + "  new_array_col, "
        + "  new_map_col "
        + "from t1", expectedResult);
  }

  private void checkAnswer(String query, String... expectedResult) {
    TableResult actualResult = tEnv.executeSql(query);
    Set<String> expected = new HashSet<>(Arrays.asList(expectedResult));
    Set<String> actual = new HashSet<>();

    // create a runnable to handle reads (especially useful for streaming reads as they are unbounded)
    Runnable runnable = () -> {
      try (CloseableIterator<Row> iterator = actualResult.collect()) {
        while (iterator.hasNext()) {
          actual.add(iterator.next().toString());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future future = executor.submit(runnable);
    try {
      // allow result collector to run for a short period of time
      future.get(5, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      future.cancel(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      executor.shutdownNow();
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
          "+I[Indica, null, 12, null, {Indica=1212.0}, [12.0], null, null, null]",
          "+I[Danny, 10000.1, 23, +I[1, 1, s1, 11, t1, drop_add1], {Danny=2323.23}, [23.0, 23.0, 23.0], +I[1, 1], [1], {k1=v1}]",
          "+I[Stephen, null, 33, +I[2, null, s2, 2, null, null], {Stephen=3333.0}, [33.0], null, null, null]",
          "+I[Julian, 30000.3, 53, +I[3, 3, s3, 33, t3, drop_add3], {Julian=5353.53}, [53.0], +I[3, 3], [3], {k3=v3}]",
          "+I[Fabian, null, 31, +I[4, null, s4, 4, null, null], {Fabian=3131.0}, [31.0], null, null, null]",
          "+I[Sophia, null, 18, +I[5, null, s5, 5, null, null], {Sophia=1818.0}, [18.0, 18.0], null, null, null]",
          "+I[Emma, null, 20, +I[6, null, s6, 6, null, null], {Emma=2020.0}, [20.0], null, null, null]",
          "+I[Bob, null, 44, +I[7, null, s7, 7, null, null], {Bob=4444.0}, [44.0, 44.0], null, null, null]",
          "+I[Han, null, 56, +I[8, null, s8, 8, null, null], {Han=5656.0}, [56.0, 56.0, 56.0], null, null, null]",
          "+I[Alice, 90000.9, unknown, +I[9, 9, s9, 99, t9, drop_add9], {Alice=9999.99}, [9999.0, 9999.0], +I[9, 9], [9], {k9=v9}]",
      },
      new String[] {
          "+I[uuid:id0, Indica, null, 12, null, {Indica=1212.0}, [12.0], null, null, null]",
          "+I[uuid:id1, Danny, 10000.1, 23, +I[1, 1, s1, 11, t1, drop_add1], {Danny=2323.23}, [23.0, 23.0, 23.0], +I[1, 1], [1], {k1=v1}]",
          "+I[uuid:id2, Stephen, null, 33, +I[2, null, s2, 2, null, null], {Stephen=3333.0}, [33.0], null, null, null]",
          "+I[uuid:id3, Julian, 30000.3, 53, +I[3, 3, s3, 33, t3, drop_add3], {Julian=5353.53}, [53.0], +I[3, 3], [3], {k3=v3}]",
          "+I[uuid:id4, Fabian, null, 31, +I[4, null, s4, 4, null, null], {Fabian=3131.0}, [31.0], null, null, null]",
          "+I[uuid:id5, Sophia, null, 18, +I[5, null, s5, 5, null, null], {Sophia=1818.0}, [18.0, 18.0], null, null, null]",
          "+I[uuid:id6, Emma, null, 20, +I[6, null, s6, 6, null, null], {Emma=2020.0}, [20.0], null, null, null]",
          "+I[uuid:id7, Bob, null, 44, +I[7, null, s7, 7, null, null], {Bob=4444.0}, [44.0, 44.0], null, null, null]",
          "+I[uuid:id8, Han, null, 56, +I[8, null, s8, 8, null, null], {Han=5656.0}, [56.0, 56.0, 56.0], null, null, null]",
          "+I[uuid:id9, Alice, 90000.9, unknown, +I[9, 9, s9, 99, t9, drop_add9], {Alice=9999.99}, [9999.0, 9999.0], +I[9, 9], [9], {k9=v9}]",
      },
      new String[] {
          "+I[1]",
          "+U[2]",
          "+U[3]",
          "+U[4]",
          "+U[5]",
          "+U[6]",
          "+U[7]",
          "+U[8]",
          "+U[9]",
          "+U[10]",
          "-U[1]",
          "-U[2]",
          "-U[3]",
          "-U[4]",
          "-U[5]",
          "-U[6]",
          "-U[7]",
          "-U[8]",
          "-U[9]",
      }
  );

  private static final ExpectedResult EXPECTED_UNMERGED_RESULT = new ExpectedResult(
      new String[] {
          "+I[Indica, null, 12, null, {Indica=1212.0}, [12.0], null, null, null]",
          "+I[Danny, null, 23, +I[1, null, s1, 1, null, null], {Danny=2323.0}, [23.0, 23.0], null, null, null]",
          "+I[Stephen, null, 33, +I[2, null, s2, 2, null, null], {Stephen=3333.0}, [33.0], null, null, null]",
          "+I[Julian, null, 53, +I[3, null, s3, 3, null, null], {Julian=5353.0}, [53.0, 53.0], null, null, null]",
          "+I[Fabian, null, 31, +I[4, null, s4, 4, null, null], {Fabian=3131.0}, [31.0], null, null, null]",
          "+I[Sophia, null, 18, +I[5, null, s5, 5, null, null], {Sophia=1818.0}, [18.0, 18.0], null, null, null]",
          "+I[Emma, null, 20, +I[6, null, s6, 6, null, null], {Emma=2020.0}, [20.0], null, null, null]",
          "+I[Bob, null, 44, +I[7, null, s7, 7, null, null], {Bob=4444.0}, [44.0, 44.0], null, null, null]",
          "+I[Han, null, 56, +I[8, null, s8, 8, null, null], {Han=5656.0}, [56.0, 56.0, 56.0], null, null, null]",
          "+I[Alice, 90000.9, unknown, +I[9, 9, s9, 99, t9, drop_add9], {Alice=9999.99}, [9999.0, 9999.0], +I[9, 9], [9], {k9=v9}]",
          "+I[Danny, 10000.1, 23, +I[1, 1, s1, 11, t1, drop_add1], {Danny=2323.23}, [23.0, 23.0, 23.0], +I[1, 1], [1], {k1=v1}]",
          "+I[Julian, 30000.3, 53, +I[3, 3, s3, 33, t3, drop_add3], {Julian=5353.53}, [53.0], +I[3, 3], [3], {k3=v3}]",
      },
      new String[] {
          "+I[uuid:id0, Indica, null, 12, null, {Indica=1212.0}, [12.0], null, null, null]",
          "+I[uuid:id1, Danny, null, 23, +I[1, null, s1, 1, null, null], {Danny=2323.0}, [23.0, 23.0], null, null, null]",
          "+I[uuid:id2, Stephen, null, 33, +I[2, null, s2, 2, null, null], {Stephen=3333.0}, [33.0], null, null, null]",
          "+I[uuid:id3, Julian, null, 53, +I[3, null, s3, 3, null, null], {Julian=5353.0}, [53.0, 53.0], null, null, null]",
          "+I[uuid:id4, Fabian, null, 31, +I[4, null, s4, 4, null, null], {Fabian=3131.0}, [31.0], null, null, null]",
          "+I[uuid:id5, Sophia, null, 18, +I[5, null, s5, 5, null, null], {Sophia=1818.0}, [18.0, 18.0], null, null, null]",
          "+I[uuid:id6, Emma, null, 20, +I[6, null, s6, 6, null, null], {Emma=2020.0}, [20.0], null, null, null]",
          "+I[uuid:id7, Bob, null, 44, +I[7, null, s7, 7, null, null], {Bob=4444.0}, [44.0, 44.0], null, null, null]",
          "+I[uuid:id8, Han, null, 56, +I[8, null, s8, 8, null, null], {Han=5656.0}, [56.0, 56.0, 56.0], null, null, null]",
          "+I[uuid:id9, Alice, 90000.9, unknown, +I[9, 9, s9, 99, t9, drop_add9], {Alice=9999.99}, [9999.0, 9999.0], +I[9, 9], [9], {k9=v9}]",
          "+I[uuid:id1, Danny, 10000.1, 23, +I[1, 1, s1, 11, t1, drop_add1], {Danny=2323.23}, [23.0, 23.0, 23.0], +I[1, 1], [1], {k1=v1}]",
          "+I[uuid:id3, Julian, 30000.3, 53, +I[3, 3, s3, 33, t3, drop_add3], {Julian=5353.53}, [53.0], +I[3, 3], [3], {k3=v3}]",
      },
      new String[] {
          "+I[1]",
          "+U[2]",
          "+U[3]",
          "+U[4]",
          "+U[5]",
          "+U[6]",
          "+U[7]",
          "+U[8]",
          "+U[9]",
          "+U[10]",
          "-U[10]",
          "+U[11]",
          "-U[11]",
          "+U[12]",
          "-U[1]",
          "-U[2]",
          "-U[3]",
          "-U[4]",
          "-U[5]",
          "-U[6]",
          "-U[7]",
          "-U[8]",
          "-U[9]",
      }
  );
}
