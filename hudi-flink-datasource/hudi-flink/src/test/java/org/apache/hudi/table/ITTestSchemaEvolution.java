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

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.sink.compact.CompactOperator;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionCommitSink;
import org.apache.hudi.sink.compact.CompactionPlanSourceFunction;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.utils.FlinkMiniCluster;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
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

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.AFTER;
import static org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.BEFORE;
import static org.apache.hudi.utils.TestConfigurations.ROW_TYPE_EVOLUTION_AFTER;
import static org.apache.hudi.utils.TestConfigurations.ROW_TYPE_EVOLUTION_BEFORE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
@ExtendWith(FlinkMiniCluster.class)
public class ITTestSchemaEvolution {
  private static final Logger LOG = LoggerFactory.getLogger(ITTestSchemaEvolution.class);

  @TempDir File tempFile;
  private StreamTableEnvironment tEnv;
  private StreamExecutionEnvironment env;

  @BeforeEach
  public void setUp() {
    env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
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
        .withOption(FlinkOptions.READ_START_COMMIT.key(), FlinkOptions.START_COMMIT_EARLIEST)
        .withOption(FlinkOptions.READ_STREAMING_SKIP_COMPACT.key(), false);
    testSchemaEvolution(tableOptions);
  }

  @Test
  public void testMergeOnReadInputFormatBaseFileOnlyFilteringIterator() throws Exception {
    TableOptions tableOptions = defaultTableOptions(tempFile.getAbsolutePath())
        .withOption(FlinkOptions.READ_AS_STREAMING.key(), true)
        .withOption(FlinkOptions.READ_START_COMMIT.key(), 1)
        .withOption(FlinkOptions.READ_STREAMING_SKIP_COMPACT.key(), false);
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

    doCompact(tableOptions.toConfig());
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
        + "  f_row_map map<string, row<f0 int, f1 string, drop_add string, change_type int>>,"
        + "  f_row_array array<row<f0 int, f1 string, drop_add string, change_type int>>,"
        + "  `partition` string"
        + ") partitioned by (`partition`) with (" + tableOptions + ")"
    );
    // An explicit cast is performed for map-values to prevent implicit map.key strings from being truncated/extended based the last row's inferred schema
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
        + "  cast(f_row_map as map<string, row< f0 int, f1 string, drop_add string, change_type int>>),"
        + "  cast(f_row_array as array<row< f0 int, f1 string, drop_add string, change_type int>>),"
        + "  cast(`partition` as string) "
        + "from (values "
        + "  ('id0', 'Indica', 'F', 12, '2000-01-01 00:00:00', cast(null as row<f0 int, f1 string, drop_add string, change_type int>), map['Indica', 1212], array[12], "
        + "  cast(null as map<string, row<f0 int, f1 string, drop_add string, change_type int>>), array[row(0, 's0', '', 0)], 'par0'),"
        + "  ('id1', 'Danny', 'M', 23, '2000-01-01 00:00:01', row(1, 's1', '', 1), cast(map['Danny', 2323] as map<string, int>), array[23, 23], "
        + "  cast(map['Danny', row(1, 's1', '', 1)] as map<string, row<f0 int, f1 string, drop_add string, change_type int>>), array[row(1, 's1', '', 1)], 'par1'),"
        + "  ('id2', 'Stephen', 'M', 33, '2000-01-01 00:00:02', row(2, 's2', '', 2), cast(map['Stephen', 3333] as map<string, int>), array[33], "
        + "  cast(map['Stephen', row(2, 's2', '', 2)] as map<string, row<f0 int, f1 string, drop_add string, change_type int>>), array[row(2, 's2', '', 2)], 'par1'),"
        + "  ('id3', 'Julian', 'M', 53, '2000-01-01 00:00:03', row(3, 's3', '', 3), cast(map['Julian', 5353] as map<string, int>), array[53, 53], "
        + "  cast(map['Julian', row(3, 's3', '', 3)] as map<string, row<f0 int, f1 string, drop_add string, change_type int>>), array[row(3, 's3', '', 3)], 'par2'),"
        + "  ('id4', 'Fabian', 'M', 31, '2000-01-01 00:00:04', row(4, 's4', '', 4), cast(map['Fabian', 3131] as map<string, int>), array[31], "
        + "  cast(map['Fabian', row(4, 's4', '', 4)] as map<string, row<f0 int, f1 string, drop_add string, change_type int>>), array[row(4, 's4', '', 4)], 'par2'),"
        + "  ('id5', 'Sophia', 'F', 18, '2000-01-01 00:00:05', row(5, 's5', '', 5), cast(map['Sophia', 1818] as map<string, int>), array[18, 18], "
        + "  cast(map['Sophia', row(5, 's5', '', 5)] as map<string, row<f0 int, f1 string, drop_add string, change_type int>>), array[row(5, 's5', '', 5)], 'par3'),"
        + "  ('id6', 'Emma', 'F', 20, '2000-01-01 00:00:06', row(6, 's6', '', 6), cast(map['Emma', 2020] as map<string, int>), array[20], "
        + "  cast(map['Emma', row(6, 's6', '', 6)] as map<string, row<f0 int, f1 string, drop_add string, change_type int>>), array[row(6, 's6', '', 6)], 'par3'),"
        + "  ('id7', 'Bob', 'M', 44, '2000-01-01 00:00:07', row(7, 's7', '', 7), cast(map['Bob', 4444] as map<string, int>), array[44, 44], "
        + "  cast(map['Bob', row(7, 's7', '', 7)] as map<string, row<f0 int, f1 string, drop_add string, change_type int>>), array[row(7, 's7', '', 7)], 'par4'),"
        + "  ('id8', 'Han', 'M', 56, '2000-01-01 00:00:08', row(8, 's8', '', 8), cast(map['Han', 5656] as map<string, int>), array[56, 56, 56], "
        + "  cast(map['Han', row(8, 's8', '', 8)] as map<string, row<f0 int, f1 string, drop_add string, change_type int>>), array[row(8, 's8', '', 8)], 'par4')"
        + ") as A(uuid, name, gender, age, ts, f_struct, f_map, f_array, f_row_map, f_row_array, `partition`)"
    ).await();
  }

  private void changeTableSchema(TableOptions tableOptions, boolean shouldCompactBeforeSchemaChanges) throws IOException {
    Configuration conf = tableOptions.toConfig();
    try (HoodieFlinkWriteClient<?> writeClient = FlinkWriteClients.createWriteClient(conf)) {
      if (shouldCompactBeforeSchemaChanges) {
        doCompact(conf);
      }

      Schema intType = SchemaBuilder.unionOf().nullType().and().intType().endUnion();
      Schema longType = SchemaBuilder.unionOf().nullType().and().longType().endUnion();
      Schema doubleType = SchemaBuilder.unionOf().nullType().and().doubleType().endUnion();
      Schema stringType = SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
      Schema structType = SchemaBuilder.builder().record("new_row_col").fields()
              .name("f0").type(longType).noDefault()
              .name("f1").type(stringType).noDefault().endRecord();
      Schema arrayType = Schema.createUnion(SchemaBuilder.builder().array().items(stringType), SchemaBuilder.builder().nullType());
      Schema mapType = Schema.createUnion(SchemaBuilder.builder().map().values(stringType), SchemaBuilder.builder().nullType());

      writeClient.addColumn("salary", doubleType, null, "name", AFTER);
      writeClient.deleteColumns("gender");
      writeClient.renameColumn("name", "first_name");
      writeClient.updateColumnType("age", Types.StringType.get());
      writeClient.addColumn("last_name", stringType, "empty allowed", "salary", BEFORE);
      writeClient.reOrderColPosition("age", "first_name", BEFORE);
      // add a field in the middle of the `f_struct` and `f_row_map` columns
      writeClient.addColumn("f_struct.f2", intType, "add field in middle of struct", "f_struct.f0", AFTER);
      writeClient.addColumn("f_row_map.value.f2", intType, "add field in middle of struct", "f_row_map.value.f0", AFTER);
      // add a field at the end of `f_struct` and `f_row_map` column
      writeClient.addColumn("f_struct.f3", stringType);
      writeClient.addColumn("f_row_map.value.f3", stringType);

      // delete and add a field with the same name
      // reads should not return previously inserted datum of dropped field of the same name
      writeClient.deleteColumns("f_struct.drop_add");
      writeClient.addColumn("f_struct.drop_add", doubleType);
      writeClient.deleteColumns("f_row_map.value.drop_add");
      writeClient.addColumn("f_row_map.value.drop_add", doubleType);

      // perform comprehensive evolution on complex types (struct, array, map) by promoting its primitive types
      writeClient.updateColumnType("f_struct.change_type", Types.LongType.get());
      writeClient.renameColumn("f_struct.change_type", "renamed_change_type");
      writeClient.updateColumnType("f_row_map.value.change_type", Types.LongType.get());
      writeClient.renameColumn("f_row_map.value.change_type", "renamed_change_type");
      writeClient.updateColumnType("f_array.element", Types.DoubleType.get());
      writeClient.updateColumnType("f_map.value", Types.DoubleType.get());

      // perform comprehensive schema evolution on table by adding complex typed columns
      writeClient.addColumn("new_row_col", structType);
      writeClient.addColumn("new_array_col", arrayType);
      writeClient.addColumn("new_map_col", mapType);

      writeClient.reOrderColPosition("partition", "new_map_col", AFTER);

      // perform comprehensive evolution on a struct column by reordering field positions
      writeClient.updateColumnType("f_struct.f0", Types.DecimalType.get(20, 0));
      writeClient.reOrderColPosition("f_struct.f0", "f_struct.drop_add", AFTER);
      writeClient.updateColumnType("f_row_map.value.f0", Types.DecimalType.get(20, 0));
      writeClient.reOrderColPosition("f_row_map.value.f0", "f_row_map.value.drop_add", AFTER);
    } catch (Exception e) {
      throw new HoodieException(e);
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
        + "  f_struct row<f2 int, f1 string, renamed_change_type bigint, f3 string, drop_add string, f0 decimal(20, 0)>,"
        + "  f_map map<string, double>,"
        + "  f_array array<double>,"
        + "  f_row_map map<string, row<f2 int, f1 string, renamed_change_type bigint, f3 string, drop_add string, f0 decimal(20, 0)>>,"
        + "  f_row_array array<row<f0 int, f1 string, drop_add string, change_type int>>,"
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
        + "  cast(f_struct as row<f2 int, f1 string, renamed_change_type bigint, f3 string, drop_add string, f0 decimal(20, 0)>),"
        + "  cast(f_map as map<string, double>),"
        + "  cast(f_array as array<double>),"
        + "  cast(f_row_map as map<string, row<f2 int, f1 string, renamed_change_type bigint, f3 string, drop_add string, f0 decimal(20, 0)>>),"
        + "  cast(f_row_array as array<row<f0 int, f1 string, drop_add string, change_type int>>),"
        + "  cast(new_row_col as row<f0 bigint, f1 string>),"
        + "  cast(new_array_col as array<string>),"
        + "  cast(new_map_col as map<string, string>),"
        + "  cast(`partition` as string) "
        + "from (values "
        + "  ('id1', '23', 'Danny', '', 10000.1, '2000-01-01 00:00:01', row(1, 's1', 11, 't1', 'drop_add1', 1), cast(map['Danny', 2323.23] as map<string, double>), array[23, 23, 23], "
        + "  cast(map['Danny', row(1, 's1', 11, 't1', 'drop_add1', 1)] as map<string, row<f2 int, f1 string, renamed_change_type bigint, f3 string, drop_add string, f0 decimal(20, 0)>>), "
        + "  array[row(1, 's1', '', 1)], "
        + "  row(1, '1'), array['1'], Map['k1','v1'], 'par1'),"
        + "  ('id9', 'unknown', 'Alice', '', 90000.9, '2000-01-01 00:00:09', row(9, 's9', 99, 't9', 'drop_add9', 9), cast(map['Alice', 9999.99] as map<string, double>), array[9999, 9999], "
        + "  cast(map['Alice', row(9, 's9', 99, 't9', 'drop_add9', 9)] as map<string, row<f2 int, f1 string, renamed_change_type bigint, f3 string, drop_add string, f0 decimal(20, 0)>>), "
        + "  array[row(9, 's9', '', 9)], "
        + "  row(9, '9'), array['9'], Map['k9','v9'], 'par1'),"
        + "  ('id3', '53', 'Julian', '', 30000.3, '2000-01-01 00:00:03', row(3, 's3', 33, 't3', 'drop_add3', 3), cast(map['Julian', 5353.53] as map<string, double>), array[53], "
        + "  cast(map['Julian', row(3, 's3', 33, 't3', 'drop_add3', 3)] as map<string, row<f2 int, f1 string, renamed_change_type bigint, f3 string, drop_add string, f0 decimal(20, 0)>>), "
        + "  array[row(3, 's3', '', 3)], "
        + "  row(3, '3'), array['3'], Map['k3','v3'], 'par2')"
        + ") as A(uuid, age, first_name, last_name, salary, ts, f_struct, f_map, f_array, f_row_map, f_row_array, new_row_col, new_array_col, new_map_col, `partition`)"
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
        HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), true,
        HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true",
        HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key(), "false");
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
        + "  f_row_map, "
        + "  f_row_array, "
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
        + "  f_struct row<f2 int, f1 string, renamed_change_type bigint, f3 string, drop_add string, f0 decimal(20, 0)>,"
        + "  f_map map<string, double>,"
        + "  f_array array<double>,"
        + "  f_row_map map<string, row<f2 int, f1 string, renamed_change_type bigint, f3 string, drop_add string, f0 decimal(20, 0)>>,"
        + "  f_row_array array<row<f0 int, f1 string, drop_add string, change_type int>>,"
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
        + "  f_row_map, "
        + "  f_row_array, "
        + "  new_row_col, "
        + "  new_array_col, "
        + "  new_map_col "
        + "from t1", expectedResult);
  }

  private void doCompact(Configuration conf) throws Exception {
    // use sync compaction to ensure compaction finished.
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    try (HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf)) {
      HoodieFlinkTable<?> table = writeClient.getHoodieTable();

      Option<String> compactionInstantOpt = writeClient.scheduleCompaction(Option.empty());
      String compactionInstantTime = compactionInstantOpt.get();

      // generate compaction plan
      // should support configurable commit metadata
      HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
          table.getMetaClient(), compactionInstantTime);

      HoodieInstant instant = INSTANT_GENERATOR.getCompactionRequestedInstant(compactionInstantTime);
      // Mark instant as compaction inflight
      table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);

      env.addSource(new CompactionPlanSourceFunction(Collections.singletonList(Pair.of(compactionInstantTime, compactionPlan)), conf))
          .name("compaction_source")
          .uid("uid_compaction_source")
          .rebalance()
          .transform("compact_task",
              TypeInformation.of(CompactionCommitEvent.class),
              new CompactOperator(conf))
          .setParallelism(FlinkMiniCluster.DEFAULT_PARALLELISM)
          .addSink(new CompactionCommitSink(conf))
          .name("compaction_commit")
          .uid("uid_compaction_commit")
          .setParallelism(1);

      env.execute("flink_hudi_compaction");
      assertTrue(table.getMetaClient().reloadActiveTimeline().filterCompletedInstants().containsInstant(instant.requestedTime()));
    }
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

    for (String expectedItem : expected) {
      if (!actual.contains(expectedItem)) {
        LOG.info("Not in actual: {}", expectedItem);
      }
    }
    for (String actualItem : actual) {
      if (!expected.contains(actualItem)) {
        LOG.info("Not in expected: {}", actualItem);
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

  //TODO: null arrays have a single null row; array with null vs array with row will all null values
  private static final ExpectedResult EXPECTED_MERGED_RESULT = new ExpectedResult(
      new String[] {
          "+I[Indica, null, 12, null, {Indica=1212.0}, [12.0], null, [+I[0, s0, , 0]], null, null, null]",
          "+I[Danny, 10000.1, 23, +I[1, s1, 11, t1, drop_add1, 1], {Danny=2323.23}, [23.0, 23.0, 23.0], {Danny=+I[1, s1, 11, t1, drop_add1, 1]}, [+I[1, s1, , 1]], +I[1, 1], [1], {k1=v1}]",
          "+I[Stephen, null, 33, +I[null, s2, 2, null, null, 2], {Stephen=3333.0}, [33.0], {Stephen=+I[null, s2, 2, null, null, 2]}, [+I[2, s2, , 2]], null, null, null]",
          "+I[Julian, 30000.3, 53, +I[3, s3, 33, t3, drop_add3, 3], {Julian=5353.53}, [53.0], {Julian=+I[3, s3, 33, t3, drop_add3, 3]}, [+I[3, s3, , 3]], +I[3, 3], [3], {k3=v3}]",
          "+I[Fabian, null, 31, +I[null, s4, 4, null, null, 4], {Fabian=3131.0}, [31.0], {Fabian=+I[null, s4, 4, null, null, 4]}, [+I[4, s4, , 4]], null, null, null]",
          "+I[Sophia, null, 18, +I[null, s5, 5, null, null, 5], {Sophia=1818.0}, [18.0, 18.0], {Sophia=+I[null, s5, 5, null, null, 5]}, [+I[5, s5, , 5]], null, null, null]",
          "+I[Emma, null, 20, +I[null, s6, 6, null, null, 6], {Emma=2020.0}, [20.0], {Emma=+I[null, s6, 6, null, null, 6]}, [+I[6, s6, , 6]], null, null, null]",
          "+I[Bob, null, 44, +I[null, s7, 7, null, null, 7], {Bob=4444.0}, [44.0, 44.0], {Bob=+I[null, s7, 7, null, null, 7]}, [+I[7, s7, , 7]], null, null, null]",
          "+I[Han, null, 56, +I[null, s8, 8, null, null, 8], {Han=5656.0}, [56.0, 56.0, 56.0], {Han=+I[null, s8, 8, null, null, 8]}, [+I[8, s8, , 8]], null, null, null]",
          "+I[Alice, 90000.9, unknown, +I[9, s9, 99, t9, drop_add9, 9], {Alice=9999.99}, [9999.0, 9999.0], {Alice=+I[9, s9, 99, t9, drop_add9, 9]}, [+I[9, s9, , 9]], +I[9, 9], [9], {k9=v9}]",
      },
      new String[] {
          "+I[id0, Indica, null, 12, null, {Indica=1212.0}, [12.0], null, [+I[0, s0, , 0]], null, null, null]",
          "+I[id1, Danny, 10000.1, 23, +I[1, s1, 11, t1, drop_add1, 1], {Danny=2323.23}, [23.0, 23.0, 23.0], {Danny=+I[1, s1, 11, t1, drop_add1, 1]}, [+I[1, s1, , 1]], +I[1, 1], [1], {k1=v1}]",
          "+I[id2, Stephen, null, 33, +I[null, s2, 2, null, null, 2], {Stephen=3333.0}, [33.0], {Stephen=+I[null, s2, 2, null, null, 2]}, [+I[2, s2, , 2]], null, null, null]",
          "+I[id3, Julian, 30000.3, 53, +I[3, s3, 33, t3, drop_add3, 3], {Julian=5353.53}, [53.0], {Julian=+I[3, s3, 33, t3, drop_add3, 3]}, [+I[3, s3, , 3]], +I[3, 3], [3], {k3=v3}]",
          "+I[id4, Fabian, null, 31, +I[null, s4, 4, null, null, 4], {Fabian=3131.0}, [31.0], {Fabian=+I[null, s4, 4, null, null, 4]}, [+I[4, s4, , 4]], null, null, null]",
          "+I[id5, Sophia, null, 18, +I[null, s5, 5, null, null, 5], {Sophia=1818.0}, [18.0, 18.0], {Sophia=+I[null, s5, 5, null, null, 5]}, [+I[5, s5, , 5]], null, null, null]",
          "+I[id6, Emma, null, 20, +I[null, s6, 6, null, null, 6], {Emma=2020.0}, [20.0], {Emma=+I[null, s6, 6, null, null, 6]}, [+I[6, s6, , 6]], null, null, null]",
          "+I[id7, Bob, null, 44, +I[null, s7, 7, null, null, 7], {Bob=4444.0}, [44.0, 44.0], {Bob=+I[null, s7, 7, null, null, 7]}, [+I[7, s7, , 7]], null, null, null]",
          "+I[id8, Han, null, 56, +I[null, s8, 8, null, null, 8], {Han=5656.0}, [56.0, 56.0, 56.0], {Han=+I[null, s8, 8, null, null, 8]}, [+I[8, s8, , 8]], null, null, null]",
          "+I[id9, Alice, 90000.9, unknown, +I[9, s9, 99, t9, drop_add9, 9], {Alice=9999.99}, [9999.0, 9999.0], {Alice=+I[9, s9, 99, t9, drop_add9, 9]}, [+I[9, s9, , 9]], +I[9, 9], [9], {k9=v9}]"
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
          "+I[Indica, null, 12, null, {Indica=1212.0}, [12.0], null, [+I[0, s0, , 0]], null, null, null]",
          "+I[Danny, null, 23, +I[null, s1, 1, null, null, 1], {Danny=2323.0}, [23.0, 23.0], {Danny=+I[null, s1, 1, null, null, 1]}, [+I[1, s1, , 1]], null, null, null]",
          "+I[Stephen, null, 33, +I[null, s2, 2, null, null, 2], {Stephen=3333.0}, [33.0], {Stephen=+I[null, s2, 2, null, null, 2]}, [+I[2, s2, , 2]], null, null, null]",
          "+I[Julian, null, 53, +I[null, s3, 3, null, null, 3], {Julian=5353.0}, [53.0, 53.0], {Julian=+I[null, s3, 3, null, null, 3]}, [+I[3, s3, , 3]], null, null, null]",
          "+I[Fabian, null, 31, +I[null, s4, 4, null, null, 4], {Fabian=3131.0}, [31.0], {Fabian=+I[null, s4, 4, null, null, 4]}, [+I[4, s4, , 4]], null, null, null]",
          "+I[Sophia, null, 18, +I[null, s5, 5, null, null, 5], {Sophia=1818.0}, [18.0, 18.0], {Sophia=+I[null, s5, 5, null, null, 5]}, [+I[5, s5, , 5]], null, null, null]",
          "+I[Emma, null, 20, +I[null, s6, 6, null, null, 6], {Emma=2020.0}, [20.0], {Emma=+I[null, s6, 6, null, null, 6]}, [+I[6, s6, , 6]], null, null, null]",
          "+I[Bob, null, 44, +I[null, s7, 7, null, null, 7], {Bob=4444.0}, [44.0, 44.0], {Bob=+I[null, s7, 7, null, null, 7]}, [+I[7, s7, , 7]], null, null, null]",
          "+I[Han, null, 56, +I[null, s8, 8, null, null, 8], {Han=5656.0}, [56.0, 56.0, 56.0], {Han=+I[null, s8, 8, null, null, 8]}, [+I[8, s8, , 8]], null, null, null]",
          "+I[Alice, 90000.9, unknown, +I[9, s9, 99, t9, drop_add9, 9], {Alice=9999.99}, [9999.0, 9999.0], {Alice=+I[9, s9, 99, t9, drop_add9, 9]}, [+I[9, s9, , 9]], +I[9, 9], [9], {k9=v9}]",
          "+I[Danny, 10000.1, 23, +I[1, s1, 11, t1, drop_add1, 1], {Danny=2323.23}, [23.0, 23.0, 23.0], {Danny=+I[1, s1, 11, t1, drop_add1, 1]}, [+I[1, s1, , 1]], +I[1, 1], [1], {k1=v1}]",
          "+I[Julian, 30000.3, 53, +I[3, s3, 33, t3, drop_add3, 3], {Julian=5353.53}, [53.0], {Julian=+I[3, s3, 33, t3, drop_add3, 3]}, [+I[3, s3, , 3]], +I[3, 3], [3], {k3=v3}]",
      },
      new String[] {
          "+I[id0, Indica, null, 12, null, {Indica=1212.0}, [12.0], null, [+I[0, s0, , 0]], null, null, null]",
          "+I[id1, Danny, null, 23, +I[null, s1, 1, null, null, 1], {Danny=2323.0}, [23.0, 23.0], {Danny=+I[null, s1, 1, null, null, 1]}, [+I[1, s1, , 1]], null, null, null]",
          "+I[id2, Stephen, null, 33, +I[null, s2, 2, null, null, 2], {Stephen=3333.0}, [33.0], {Stephen=+I[null, s2, 2, null, null, 2]}, [+I[2, s2, , 2]], null, null, null]",
          "+I[id3, Julian, null, 53, +I[null, s3, 3, null, null, 3], {Julian=5353.0}, [53.0, 53.0], {Julian=+I[null, s3, 3, null, null, 3]}, [+I[3, s3, , 3]], null, null, null]",
          "+I[id4, Fabian, null, 31, +I[null, s4, 4, null, null, 4], {Fabian=3131.0}, [31.0], {Fabian=+I[null, s4, 4, null, null, 4]}, [+I[4, s4, , 4]], null, null, null]",
          "+I[id5, Sophia, null, 18, +I[null, s5, 5, null, null, 5], {Sophia=1818.0}, [18.0, 18.0], {Sophia=+I[null, s5, 5, null, null, 5]}, [+I[5, s5, , 5]], null, null, null]",
          "+I[id6, Emma, null, 20, +I[null, s6, 6, null, null, 6], {Emma=2020.0}, [20.0], {Emma=+I[null, s6, 6, null, null, 6]}, [+I[6, s6, , 6]], null, null, null]",
          "+I[id7, Bob, null, 44, +I[null, s7, 7, null, null, 7], {Bob=4444.0}, [44.0, 44.0], {Bob=+I[null, s7, 7, null, null, 7]}, [+I[7, s7, , 7]], null, null, null]",
          "+I[id8, Han, null, 56, +I[null, s8, 8, null, null, 8], {Han=5656.0}, [56.0, 56.0, 56.0], {Han=+I[null, s8, 8, null, null, 8]}, [+I[8, s8, , 8]], null, null, null]",
          "+I[id9, Alice, 90000.9, unknown, +I[9, s9, 99, t9, drop_add9, 9], {Alice=9999.99}, "
              + "[9999.0, 9999.0], {Alice=+I[9, s9, 99, t9, drop_add9, 9]}, [+I[9, s9, , 9]], +I[9, 9], [9], {k9=v9}]",
          "+I[id1, Danny, 10000.1, 23, +I[1, s1, 11, t1, drop_add1, 1], {Danny=2323.23}, [23.0, 23.0, 23.0], {Danny=+I[1, s1, 11, t1, drop_add1, 1]}, [+I[1, s1, , 1]], +I[1, 1], [1], {k1=v1}]",
          "+I[id3, Julian, 30000.3, 53, +I[3, s3, 33, t3, drop_add3, 3], {Julian=5353.53}, [53.0], {Julian=+I[3, s3, 33, t3, drop_add3, 3]}, [+I[3, s3, , 3]], +I[3, 3], [3], {k3=v3}]",
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
