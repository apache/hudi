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

package org.apache.hudi.sink;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.ExceptionUtil;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.MissingSchemaFieldException;
import org.apache.hudi.exception.SchemaBackwardsCompatibilityException;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.HoodiePipeline;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@ExtendWith(FlinkMiniCluster.class)
public class ITTestSchemaOnWrite extends TestLogger {
  @TempDir
  File tempFile;

  StreamExecutionEnvironment execEnv;

  @BeforeEach
  public void setup() {
    Configuration config = new Configuration();
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
    execEnv = StreamExecutionEnvironment.getExecutionEnvironment(config);
    execEnv.setParallelism(4);
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndBooleanTrueFalseParams")
  public void testWriteWithColumnMissing(HoodieTableType tableType, boolean allowColumnMissing) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.toURI().toString());
    options.put(FlinkOptions.TABLE_TYPE.key(), tableType.name());
    options.put(HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key(), "" + allowColumnMissing);

    DataStream<RowData> dataStream1 = execEnv.fromElements(
        createRowData("id1", "Alice", 25, Timestamp.valueOf("2025-10-10 00:00:01"), "par1"),
        createRowData("id2", "Lily", 21, Timestamp.valueOf("2025-10-10 00:00:01"), "par1"),
        createRowData("id3", "Julia", 15, Timestamp.valueOf("2025-10-10 00:00:01"), "par1")
    );
    HoodiePipeline.Builder builder1 = HoodiePipeline.builder("test_sink")
        .column("uuid string not null")
        .column("name string")
        .column("age int")
        .column("`ts` timestamp(3)")
        .column("`partition` string")
        .pk("uuid")
        .partition("partition")
        .options(options);
    builder1.sink(dataStream1, false);
    execEnv.execute("Api_Sink_Test");

    DataStream<RowData> dataStream2 = execEnv.fromElements(
        createRowData("id1", "Alice", Timestamp.valueOf("2025-10-10 00:00:08"), "par1"),
        createRowData("id2", "Lily", Timestamp.valueOf("2025-10-10 00:00:04"), "par1"),
        createRowData("id3", "Julia", Timestamp.valueOf("2025-10-10 00:00:07"), "par1")
    );
    HoodiePipeline.Builder builder2 = HoodiePipeline.builder("test_sink")
        .column("uuid string not null")
        .column("name string")
        .column("`ts` timestamp(3)")
        .column("`partition` string")
        .pk("uuid")
        .partition("partition")
        .options(options);
    if (allowColumnMissing) {
      builder2.sink(dataStream2, false);
      execEnv.execute("Api_Sink_Test");

      DataStream<RowData> rowDataDataStream = builder1.source(execEnv);
      List<RowData> result = new ArrayList<>();
      rowDataDataStream.executeAndCollect().forEachRemaining(result::add);
      TimeUnit.SECONDS.sleep(3); //sleep 3 second for collect data
      String expected = "["
          + "+I[id1, Alice, null, 2025-10-10T00:00:08, par1], "
          + "+I[id2, Lily, null, 2025-10-10T00:00:04, par1], "
          + "+I[id3, Julia, null, 2025-10-10T00:00:07, par1]"
          + "]";
      TestData.assertRowDataEquals(result, expected);

      // validate table schema, should contain `age` field, which is missing in the second insert.
      Schema tableSchema = getLatestTableSchema(options);
      Schema expectedSchema = AvroSchemaConverter.convertToSchema(
          TestConfigurations.ROW_DATA_TYPE_PK_NON_NULL.getLogicalType(), tableSchema.getFullName());
      Assertions.assertEquals(expectedSchema, tableSchema);
    } else {
      Exception ex = Assertions.assertThrows(HoodieException.class, () -> builder2.sink(dataStream2, false));
      Assertions.assertInstanceOf(MissingSchemaFieldException.class, ex.getCause());
    }
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testWriteWithAddingNewColumns(HoodieTableType tableType) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.toURI().toString());
    options.put(FlinkOptions.TABLE_TYPE.key(), tableType.name());

    DataStream<RowData> dataStream1 = execEnv.fromElements(
        createRowData("id1", "Alice", 25, Timestamp.valueOf("2025-10-10 00:00:01"), "par1"),
        createRowData("id2", "Lily", 21, Timestamp.valueOf("2025-10-10 00:00:01"), "par1"),
        createRowData("id3", "Julia", 15, Timestamp.valueOf("2025-10-10 00:00:01"), "par1")
    );
    HoodiePipeline.Builder builder1 = HoodiePipeline.builder("test_sink")
        .column("uuid string not null")
        .column("name string")
        .column("age int")
        .column("`ts` timestamp(3)")
        .column("`partition` string")
        .pk("uuid")
        .partition("partition")
        .options(options);
    builder1.sink(dataStream1, false);
    execEnv.execute("Api_Sink_Test1");

    DataStream<RowData> dataStream2 = execEnv.fromElements(
        createRowData("id1", "Alice", 26, 1000.1, Timestamp.valueOf("2025-10-10 00:00:08"), "par1"),
        createRowData("id2", "Lily", 22, 2000.2, Timestamp.valueOf("2025-10-10 00:00:04"), "par1"),
        createRowData("id4", "Mike", 16, 3000.3, Timestamp.valueOf("2025-10-10 00:00:07"), "par1")
    );
    HoodiePipeline.Builder builder2 = HoodiePipeline.builder("test_sink")
        .column("uuid string not null")
        .column("name string")
        .column("age int")
        .column("`salary` double")
        .column("`ts` timestamp(3)")
        .column("`partition` string")
        .pk("uuid")
        .partition("partition")
        .options(options);

    builder2.sink(dataStream2, false);
    execEnv.execute("Api_Sink_Test2");

    DataStream<RowData> rowDataDataStream = builder2.source(execEnv);
    List<RowData> result = new ArrayList<>();
    rowDataDataStream.executeAndCollect().forEachRemaining(result::add);
    TimeUnit.SECONDS.sleep(3); //sleep 3 second for collect data
    String expected = "["
        + "+I[id1, Alice, 26, 1000.1, 2025-10-10T00:00:08, par1], "
        + "+I[id2, Lily, 22, 2000.2, 2025-10-10T00:00:04, par1], "
        + "+I[id3, Julia, 15, null, 2025-10-10T00:00:01, par1], "
        + "+I[id4, Mike, 16, 3000.3, 2025-10-10T00:00:07, par1]"
        + "]";
    TestData.assertRowDataEquals(result, expected, TestConfigurations.ROW_DATA_TYPE_WIDER);

    DataType expectedTableSchema = DataTypes.ROW(
            DataTypes.FIELD("uuid", DataTypes.STRING().notNull()),
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("age", DataTypes.INT()),
            DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)),
            DataTypes.FIELD("partition", DataTypes.STRING()),
            DataTypes.FIELD("salary", DataTypes.DOUBLE()))
        .notNull();

    // validate the table schema, should contain the new added field `salary`
    Schema tableSchema = getLatestTableSchema(options);
    Schema expectedTableAvroSchema = AvroSchemaConverter.convertToSchema(expectedTableSchema.getLogicalType(), tableSchema.getFullName());
    Assertions.assertEquals(expectedTableAvroSchema, tableSchema);
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testWriteWithTypePromotion(HoodieTableType tableType) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.toURI().toString());
    options.put(FlinkOptions.TABLE_TYPE.key(), tableType.name());

    DataStream<RowData> dataStream1 = execEnv.fromElements(
        createRowData("id1", 1, 2, 3, 25L, 26L, 22.4f,
            DecimalData.fromBigDecimal(BigDecimal.valueOf(10.1), 10, 1),
            Timestamp.valueOf("2025-10-10 00:00:01"), "par1"),
        createRowData("id2", 1, 2, 3, 25L, 26L, 22.4f,
            DecimalData.fromBigDecimal(BigDecimal.valueOf(10.1), 10, 1),
            Timestamp.valueOf("2025-10-11 00:00:01"), "par1"),
        createRowData("id3", 1, 2, 3, 25L, 26L, 22.4f,
            DecimalData.fromBigDecimal(BigDecimal.valueOf(10.1), 10, 1),
            Timestamp.valueOf("2025-10-12 00:00:01"), "par1"));
    HoodiePipeline.Builder builder1 = HoodiePipeline.builder("test_sink")
        .column("uuid string not null")
        .column("f_int_1 int")
        .column("f_int_2 int")
        .column("f_int_3 int")
        .column("f_long_1 bigint")
        .column("f_long_2 bigint")
        .column("f_float_1 float")
        .column("f_decimal decimal(10, 1)")
        .column("`ts` timestamp(3)")
        .column("`partition` string")
        .pk("uuid")
        .partition("partition")
        .options(options);
    builder1.sink(dataStream1, false);
    execEnv.execute("Api_Sink_Test1");

    DataStream<RowData> dataStream2 = execEnv.fromElements(
        createRowData("id1", 1L, 2.2f, 3.3, 25.4f, 26.6, 22.8,
            DecimalData.fromBigDecimal(BigDecimal.valueOf(13.2), 11, 1),
            Timestamp.valueOf("2025-10-10 00:00:08"), "par1"),
        createRowData("id2", 1L, 2.2f, 3.3, 25.4f, 26.6, 22.8,
            DecimalData.fromBigDecimal(BigDecimal.valueOf(13.2), 11, 1),
            Timestamp.valueOf("2025-10-11 00:00:08"), "par1"));
    HoodiePipeline.Builder builder2 = HoodiePipeline.builder("test_sink")
        .column("uuid string not null")
        .column("f_int_1 bigint")
        .column("f_int_2 float")
        .column("f_int_3 double")
        .column("f_long_1 float")
        .column("f_long_2 double")
        .column("f_float_1 double")
        .column("f_decimal decimal(11, 1)")
        .column("`ts` timestamp(3)")
        .column("`partition` string")
        .pk("uuid")
        .partition("partition")
        .options(options);
    builder2.sink(dataStream2, false);
    execEnv.execute("Api_Sink_Test2");

    DataType expectedTableSchema = DataTypes.ROW(
            DataTypes.FIELD("uuid", DataTypes.STRING().notNull()),
            DataTypes.FIELD("f_int_1", DataTypes.BIGINT()),
            DataTypes.FIELD("f_int_2", DataTypes.FLOAT()),
            DataTypes.FIELD("f_int_3", DataTypes.DOUBLE()),
            DataTypes.FIELD("f_long_1", DataTypes.FLOAT()),
            DataTypes.FIELD("f_long_2", DataTypes.DOUBLE()),
            DataTypes.FIELD("f_float_1", DataTypes.DOUBLE()),
            DataTypes.FIELD("f_decimal", DataTypes.DECIMAL(11, 1)),
            DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)),
            DataTypes.FIELD("partition", DataTypes.STRING()))
        .notNull();

    DataStream<RowData> rowDataDataStream = builder2.source(execEnv);
    List<RowData> result = new ArrayList<>();
    rowDataDataStream.executeAndCollect().forEachRemaining(result::add);
    TimeUnit.SECONDS.sleep(3); //sleep 3 second for collect data
    String expected = "["
        + "+I[id1, 1, 2.2, 3.3, 25.4, 26.6, 22.8, 13.2, 2025-10-10T00:00:08, par1], "
        + "+I[id2, 1, 2.2, 3.3, 25.4, 26.6, 22.8, 13.2, 2025-10-11T00:00:08, par1], "
        + "+I[id3, 1, 2.0, 3.0, 25.0, 26.0, 22.4, 10.1, 2025-10-12T00:00:01, par1]"
        + "]";
    TestData.assertRowDataEquals(result, expected, expectedTableSchema);

    // validate the table schema, should contain the new added field `salary`
    Schema tableSchema = getLatestTableSchema(options);
    Schema expectedTableAvroSchema = AvroSchemaConverter.convertToSchema(expectedTableSchema.getLogicalType(), tableSchema.getFullName());
    Assertions.assertEquals(expectedTableAvroSchema, tableSchema);
  }

  @ParameterizedTest
  @MethodSource("unsupportedTypePromotion")
  public void testInvalidTypePromotion(String fromType, String toType, Object fromValue, Object toValue,
                                       Class<Exception> thrownException, Class<Exception> causeException) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.toURI().toString());

    DataStream<RowData> dataStream1 = execEnv.fromElements(
        createRowData("id1", fromValue, Timestamp.valueOf("2025-10-10 00:00:01"), "par1"));
    HoodiePipeline.Builder builder1 = HoodiePipeline.builder("test_sink")
        .column("uuid string not null")
        .column("f " + fromType)
        .column("`ts` timestamp(3)")
        .column("`partition` string")
        .pk("uuid")
        .partition("partition")
        .options(options);
    builder1.sink(dataStream1, false);
    execEnv.execute("Api_Sink_Test1");

    DataStream<RowData> dataStream2 = execEnv.fromElements(
        createRowData("id1", toValue, Timestamp.valueOf("2025-10-10 00:00:08"), "par1"));
    HoodiePipeline.Builder builder2 = HoodiePipeline.builder("test_sink")
        .column("uuid string not null")
        .column("f " + toType)
        .column("`ts` timestamp(3)")
        .column("`partition` string")
        .pk("uuid")
        .partition("partition")
        .options(options);

    Exception ex = Assertions.assertThrows(thrownException, () -> {
      builder2.sink(dataStream2, false);
      execEnv.execute("Api_Sink_Test2");
    });
    Assertions.assertInstanceOf(causeException, ExceptionUtil.getRootCause(ex));
  }

  private Schema getLatestTableSchema(Map<String, String> options) {
    Configuration conf = Configuration.fromMap(options);
    return StreamerUtil.getLatestTableSchema(conf.get(FlinkOptions.PATH), HadoopConfigurations.getHadoopConf(conf));
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
   * Return test params => (HoodieTableType, true/false).
   */
  private static Stream<Arguments> unsupportedTypePromotion() {
    Object[][] data =
        new Object[][] {
            {"INT", "DECIMAL(38, 8)", null, null, HoodieException.class, SchemaBackwardsCompatibilityException.class},
            {"BIGINT", "DECIMAL(38, 8)", null, null, HoodieException.class, SchemaBackwardsCompatibilityException.class},
            {"FLOAT", "DECIMAL(38, 8)", null, null, HoodieException.class, SchemaBackwardsCompatibilityException.class},
            {"DOUBLE", "DECIMAL(38, 8)", null, null, HoodieException.class, SchemaBackwardsCompatibilityException.class},
            {"DECIMAL(38, 8)", "STRING", DecimalData.fromBigDecimal(BigDecimal.valueOf(101.1), 38, 8), "hello", JobExecutionException.class, NumberFormatException.class},
            {"DATE", "TIMESTAMP(3)", Date.valueOf("2020-01-01"), Timestamp.valueOf("2020-01-01 00:00:01"), JobExecutionException.class, HoodieException.class},
        };
    return Stream.of(data).map(Arguments::of);
  }

  private static RowData createRowData(Object... values) {
    GenericRowData rowData = new GenericRowData(RowKind.INSERT, values.length);
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
      if (value == null) {
        rowData.setField(i, null);
      } else if (value instanceof Integer || value instanceof Long
          || value instanceof Float || value instanceof Double || value instanceof DecimalData) {
        rowData.setField(i, value);
      } else if (value instanceof String) {
        rowData.setField(i, StringData.fromString((String) value));
      } else if (value instanceof Timestamp) {
        rowData.setField(i, TimestampData.fromTimestamp((Timestamp) value));
      } else if (value instanceof Date) {
        rowData.setField(i, (int) ((Date) value).getTime());
      } else {
        throw new RuntimeException("unsupported type: " + value.getClass().getSimpleName());
      }
    }
    return rowData;
  }
}
