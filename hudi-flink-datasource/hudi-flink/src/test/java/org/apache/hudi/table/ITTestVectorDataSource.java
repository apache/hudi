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

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestTableEnvs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Array;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for Flink vector column read/write support.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestVectorDataSource {

  @TempDir
  Path tempDir;

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testVectorRoundTrip(HoodieTableType tableType) throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    String tablePath = tempDir.resolve("round_trip_" + tableType.name()).toUri().toString();
    createVectorTable(
        tableEnv,
        "vector_table",
        tablePath,
        tableType,
        "embedding:4,features:3,codes:4,nullable_embedding:2");

    execInsertSql(tableEnv,
        "INSERT INTO vector_table VALUES "
            + "('id1', "
            + " ARRAY[CAST(1.0 AS FLOAT), CAST(2.0 AS FLOAT), CAST(3.0 AS FLOAT), CAST(4.0 AS FLOAT)], "
            + " ARRAY[CAST(0.1 AS DOUBLE), CAST(0.2 AS DOUBLE), CAST(0.3 AS DOUBLE)], "
            + " ARRAY[CAST(1 AS TINYINT), CAST(2 AS TINYINT), CAST(3 AS TINYINT), CAST(4 AS TINYINT)], "
            + " ARRAY[CAST(9.0 AS FLOAT), CAST(9.5 AS FLOAT)], "
            + " 'label1', ARRAY['red', 'blue'], 1000), "
            + "('id2', "
            + " ARRAY[CAST(-1.0 AS FLOAT), CAST(-2.0 AS FLOAT), CAST(-3.0 AS FLOAT), CAST(-4.0 AS FLOAT)], "
            + " ARRAY[CAST(1.1 AS DOUBLE), CAST(1.2 AS DOUBLE), CAST(1.3 AS DOUBLE)], "
            + " ARRAY[CAST(-1 AS TINYINT), CAST(-2 AS TINYINT), CAST(-3 AS TINYINT), CAST(-4 AS TINYINT)], "
            + " CAST(NULL AS ARRAY<FLOAT>), "
            + " 'label2', ARRAY['green'], 2000)");

    assertStoredVectorSchema(tablePath, "embedding", 4, HoodieSchema.Vector.VectorElementType.FLOAT);
    assertStoredVectorSchema(tablePath, "features", 3, HoodieSchema.Vector.VectorElementType.DOUBLE);
    assertStoredVectorSchema(tablePath, "codes", 4, HoodieSchema.Vector.VectorElementType.INT8);
    assertStoredVectorSchema(tablePath, "nullable_embedding", 2, HoodieSchema.Vector.VectorElementType.FLOAT);

    List<Row> rows = collect(tableEnv,
        "SELECT id, embedding, features, codes, nullable_embedding, label, tags, ts FROM vector_table ORDER BY id");
    assertEquals(2, rows.size());

    Row first = rows.get(0);
    assertEquals("id1", first.getField(0));
    assertFloatArray(first.getField(1), new float[] {1.0f, 2.0f, 3.0f, 4.0f});
    assertDoubleArray(first.getField(2), new double[] {0.1d, 0.2d, 0.3d});
    assertByteArray(first.getField(3), new byte[] {1, 2, 3, 4});
    assertFloatArray(first.getField(4), new float[] {9.0f, 9.5f});
    assertEquals("label1", first.getField(5));
    assertObjectArray(first.getField(6), new Object[] {"red", "blue"});
    assertEquals(1000L, first.getField(7));

    Row second = rows.get(1);
    assertEquals("id2", second.getField(0));
    assertFloatArray(second.getField(1), new float[] {-1.0f, -2.0f, -3.0f, -4.0f});
    assertDoubleArray(second.getField(2), new double[] {1.1d, 1.2d, 1.3d});
    assertByteArray(second.getField(3), new byte[] {-1, -2, -3, -4});
    assertNull(second.getField(4));
    assertEquals("label2", second.getField(5));
    assertObjectArray(second.getField(6), new Object[] {"green"});
    assertEquals(2000L, second.getField(7));

    List<Row> nonVectorProjection = collect(tableEnv, "SELECT id, label, ts FROM vector_table ORDER BY id");
    assertEquals(Row.of("id1", "label1", 1000L), nonVectorProjection.get(0));
    assertEquals(Row.of("id2", "label2", 2000L), nonVectorProjection.get(1));

    List<Row> vectorProjection = collect(tableEnv, "SELECT id, embedding FROM vector_table WHERE id = 'id2'");
    assertEquals(1, vectorProjection.size());
    assertFloatArray(vectorProjection.get(0).getField(1), new float[] {-1.0f, -2.0f, -3.0f, -4.0f});
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testVectorAppendWriteRoundTrip(HoodieTableType tableType) throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    String tablePath = tempDir.resolve("append_write_" + tableType.name()).toUri().toString();
    createVectorTable(tableEnv, "vector_table", tablePath, tableType, "embedding:3,features:2,codes:3", "insert");

    execInsertSql(tableEnv,
        "INSERT INTO vector_table(id, embedding, features, codes, label, ts) VALUES "
            + "('id1', "
            + " ARRAY[CAST(1.0 AS FLOAT), CAST(1.5 AS FLOAT), CAST(2.0 AS FLOAT)], "
            + " ARRAY[CAST(0.1 AS DOUBLE), CAST(0.2 AS DOUBLE)], "
            + " ARRAY[CAST(1 AS TINYINT), CAST(2 AS TINYINT), CAST(3 AS TINYINT)], "
            + " 'append1', 1000), "
            + "('id2', "
            + " ARRAY[CAST(3.0 AS FLOAT), CAST(3.5 AS FLOAT), CAST(4.0 AS FLOAT)], "
            + " ARRAY[CAST(1.1 AS DOUBLE), CAST(1.2 AS DOUBLE)], "
            + " ARRAY[CAST(-1 AS TINYINT), CAST(-2 AS TINYINT), CAST(-3 AS TINYINT)], "
            + " 'append2', 2000)");

    assertStoredVectorSchema(tablePath, "embedding", 3, HoodieSchema.Vector.VectorElementType.FLOAT);
    assertStoredVectorSchema(tablePath, "features", 2, HoodieSchema.Vector.VectorElementType.DOUBLE);
    assertStoredVectorSchema(tablePath, "codes", 3, HoodieSchema.Vector.VectorElementType.INT8);

    List<Row> rows = collect(tableEnv, "SELECT id, embedding, features, codes, label, ts FROM vector_table ORDER BY id");
    assertEquals(2, rows.size());

    assertEquals("id1", rows.get(0).getField(0));
    assertFloatArray(rows.get(0).getField(1), new float[] {1.0f, 1.5f, 2.0f});
    assertDoubleArray(rows.get(0).getField(2), new double[] {0.1d, 0.2d});
    assertByteArray(rows.get(0).getField(3), new byte[] {1, 2, 3});
    assertEquals("append1", rows.get(0).getField(4));
    assertEquals(1000L, rows.get(0).getField(5));

    assertEquals("id2", rows.get(1).getField(0));
    assertFloatArray(rows.get(1).getField(1), new float[] {3.0f, 3.5f, 4.0f});
    assertDoubleArray(rows.get(1).getField(2), new double[] {1.1d, 1.2d});
    assertByteArray(rows.get(1).getField(3), new byte[] {-1, -2, -3});
    assertEquals("append2", rows.get(1).getField(4));
    assertEquals(2000L, rows.get(1).getField(5));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testVectorUpsert(HoodieTableType tableType) throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    String tablePath = tempDir.resolve("upsert_" + tableType.name()).toUri().toString();
    createVectorTable(tableEnv, "vector_table", tablePath, tableType, "embedding:2");

    execInsertSql(tableEnv,
        "INSERT INTO vector_table(id, embedding, label, ts) VALUES "
            + "('id1', ARRAY[CAST(1.0 AS FLOAT), CAST(1.1 AS FLOAT)], 'old1', 1), "
            + "('id2', ARRAY[CAST(2.0 AS FLOAT), CAST(2.2 AS FLOAT)], 'old2', 1)");
    execInsertSql(tableEnv,
        "INSERT INTO vector_table(id, embedding, label, ts) VALUES "
            + "('id1', ARRAY[CAST(9.0 AS FLOAT), CAST(9.9 AS FLOAT)], 'new1', 10), "
            + "('id3', ARRAY[CAST(3.0 AS FLOAT), CAST(3.3 AS FLOAT)], 'new3', 1)");

    List<Row> rows = collect(tableEnv, "SELECT id, embedding, label, ts FROM vector_table ORDER BY id");
    assertEquals(3, rows.size());

    assertEquals("id1", rows.get(0).getField(0));
    assertFloatArray(rows.get(0).getField(1), new float[] {9.0f, 9.9f});
    assertEquals("new1", rows.get(0).getField(2));
    assertEquals(10L, rows.get(0).getField(3));

    assertEquals("id2", rows.get(1).getField(0));
    assertFloatArray(rows.get(1).getField(1), new float[] {2.0f, 2.2f});
    assertEquals("old2", rows.get(1).getField(2));

    assertEquals("id3", rows.get(2).getField(0));
    assertFloatArray(rows.get(2).getField(1), new float[] {3.0f, 3.3f});
    assertEquals("new3", rows.get(2).getField(2));
  }

  @Test
  public void testVectorDimensionMismatchFails() {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    String tablePath = tempDir.resolve("dimension_mismatch").toUri().toString();
    createVectorTable(tableEnv, "vector_table", tablePath, HoodieTableType.COPY_ON_WRITE, "embedding:3");

    Exception exception = assertThrows(Exception.class, () -> execInsertSql(tableEnv,
        "INSERT INTO vector_table(id, embedding, label, ts) VALUES "
            + "('id1', ARRAY[CAST(1.0 AS FLOAT), CAST(2.0 AS FLOAT)], 'bad', 1)"));
    assertTrue(containsMessage(exception, "dimension mismatch"),
        "Expected dimension mismatch in exception chain, got: " + exception.getMessage());
  }

  private static void createVectorTable(
      TableEnvironment tableEnv,
      String tableName,
      String tablePath,
      HoodieTableType tableType,
      String vectorColumns) {
    createVectorTable(tableEnv, tableName, tablePath, tableType, vectorColumns, null);
  }

  private static void createVectorTable(
      TableEnvironment tableEnv,
      String tableName,
      String tablePath,
      HoodieTableType tableType,
      String vectorColumns,
      String writeOperation) {
    tableEnv.executeSql(
        "CREATE TABLE " + tableName + " ("
            + " id STRING,"
            + " embedding ARRAY<FLOAT>,"
            + " features ARRAY<DOUBLE>,"
            + " codes ARRAY<TINYINT>,"
            + " nullable_embedding ARRAY<FLOAT>,"
            + " label STRING,"
            + " tags ARRAY<STRING>,"
            + " ts BIGINT,"
            + " PRIMARY KEY (id) NOT ENFORCED"
            + ") WITH ("
            + " 'connector' = 'hudi',"
            + " 'path' = '" + tablePath + "',"
            + " '" + FlinkOptions.TABLE_TYPE.key() + "' = '" + tableType.name() + "',"
            + " '" + FlinkOptions.ORDERING_FIELDS.key() + "' = 'ts',"
            + " '" + FlinkOptions.VECTOR_COLUMNS.key() + "' = '" + vectorColumns + "',"
            + " '" + FlinkOptions.METADATA_ENABLED.key() + "' = 'false',"
            + " '" + FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key() + "' = 'false',"
            + " '" + FlinkOptions.COMPACTION_ASYNC_ENABLED.key() + "' = 'false',"
            + (writeOperation == null ? "" : " '" + FlinkOptions.OPERATION.key() + "' = '" + writeOperation + "',")
            + " '" + FlinkOptions.WRITE_TASKS.key() + "' = '1',"
            + " '" + FlinkOptions.READ_TASKS.key() + "' = '1'"
            + ")");
  }

  private static void execInsertSql(TableEnvironment tableEnv, String insert) throws ExecutionException, InterruptedException {
    TableResult tableResult = tableEnv.executeSql(insert);
    tableResult.await();
  }

  private static List<Row> collect(TableEnvironment tableEnv, String query) {
    return CollectionUtil.iteratorToList(tableEnv.executeSql(query).collect());
  }

  private static void assertStoredVectorSchema(
      String tablePath,
      String fieldName,
      int dimension,
      HoodieSchema.Vector.VectorElementType elementType) {
    Configuration conf = new Configuration();
    HoodieSchema tableSchema = StreamerUtil.getTableConfig(tablePath, HadoopConfigurations.getHadoopConf(conf))
        .flatMap(config -> config.getTableCreateSchema())
        .orElseThrow(() -> new AssertionError("Expected table create schema for " + tablePath));
    Option<HoodieSchemaField> fieldOpt = tableSchema.getField(fieldName);
    assertTrue(fieldOpt.isPresent(), "Expected field " + fieldName + " in table schema");
    HoodieSchema fieldSchema = fieldOpt.get().schema().getNonNullType();
    assertEquals(HoodieSchemaType.VECTOR, fieldSchema.getType());
    HoodieSchema.Vector vector = (HoodieSchema.Vector) fieldSchema;
    assertEquals(dimension, vector.getDimension());
    assertEquals(elementType, vector.getVectorElementType());
  }

  private static int arrayLength(Object value) {
    assertNotNull(value);
    if (value instanceof List) {
      return ((List<?>) value).size();
    }
    return Array.getLength(value);
  }

  private static Object arrayValue(Object value, int index) {
    if (value instanceof List) {
      return ((List<?>) value).get(index);
    }
    return Array.get(value, index);
  }

  private static void assertFloatArray(Object value, float[] expected) {
    assertEquals(expected.length, arrayLength(value));
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], ((Number) arrayValue(value, i)).floatValue(), 0.00001f);
    }
  }

  private static void assertDoubleArray(Object value, double[] expected) {
    assertEquals(expected.length, arrayLength(value));
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], ((Number) arrayValue(value, i)).doubleValue(), 0.0000001d);
    }
  }

  private static void assertByteArray(Object value, byte[] expected) {
    assertEquals(expected.length, arrayLength(value));
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], ((Number) arrayValue(value, i)).byteValue());
    }
  }

  private static void assertObjectArray(Object value, Object[] expected) {
    assertEquals(expected.length, arrayLength(value));
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], arrayValue(value, i));
    }
  }

  private static boolean containsMessage(Throwable throwable, String message) {
    return throwable != null && ((throwable.getMessage() != null && throwable.getMessage().contains(message))
        || containsMessage(throwable.getCause(), message));
  }
}
