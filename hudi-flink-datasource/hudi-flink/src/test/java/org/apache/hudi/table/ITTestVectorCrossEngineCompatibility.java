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
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestTableEnvs;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Array;
import java.nio.file.Path;
import java.util.List;

import static org.apache.hudi.utils.TestData.assertRowsEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test for cross-engine compatibility - verifying that Flink can read tables with VECTOR columns written by Spark.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestVectorCrossEngineCompatibility {
  @TempDir
  Path tempDir;

  private void createTable(TableEnvironment tableEnv, String tablePath, String tableType) throws Exception {
    // Create a Hudi table pointing to the Spark-written data
    // In Flink, VECTOR is represented as ARRAY<FLOAT/DOUBLE/TINYINT>
    String createTableDdl = String.format(
        "CREATE TABLE vector_table (\n"
            + "  id BIGINT,\n"
            + "  name STRING,\n"
            + "  embedding1 ARRAY<FLOAT>,\n"
            + "  embedding2 ARRAY<DOUBLE>,\n"
            + "  embedding3 ARRAY<TINYINT>,\n"
            + "  ts BIGINT,\n"
            + "  PRIMARY KEY (id) NOT ENFORCED\n"
            + ") WITH (\n"
            + "  'connector' = 'hudi',\n"
            + "  'path' = '%s',\n"
            + "  'table.type' = '%s',\n"
            + "  'ordering.fields' = 'ts'\n"
            + ");",
        tablePath, tableType);
    tableEnv.executeSql(createTableDdl);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testReadVectorColumnsFromTable(HoodieTableType tableType) throws Exception {
    Path tableTargetDir = tempDir.resolve("table");
    String resourceName = tableType == HoodieTableType.MERGE_ON_READ ? "vector_mor" : "vector_cow";
    HoodieTestUtils.extractZipToDirectory(String.format("vector_cross_engine_validation/%s.zip", resourceName), tableTargetDir, getClass());
    String tablePath = tableTargetDir.resolve(resourceName).toString();
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    createTable(tableEnv, tablePath, tableType.name());

    List<Row> rows = CollectionUtil.iteratorToList(tableEnv.executeSql("select * from vector_table order by id").collect());
    assertEquals(2, rows.size());
    assertEquals(1L, rows.get(0).getField(0));
    assertEquals("doc-1", rows.get(0).getField(1));
    assertEquals(1000L, rows.get(0).getField(5));
    assertEquals(2L, rows.get(1).getField(0));
    assertEquals("doc-2", rows.get(1).getField(1));
    assertEquals(2000L, rows.get(1).getField(5));
    for (Row row : rows) {
      assertFloatVector(row.getField(2), 0.1f);
      assertDoubleVector(row.getField(3), 0.2d);
      assertByteVector(row.getField(4), (byte) 3);
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testReadNonVectorColumnsFromCOWTable(HoodieTableType tableType) throws Exception {
    // Test that Flink can read non-VECTOR columns from a table with VECTOR columns
    Path tableTargetDir = tempDir.resolve("table");
    String resourceName = tableType == HoodieTableType.MERGE_ON_READ ? "vector_mor" : "vector_cow";
    HoodieTestUtils.extractZipToDirectory(String.format("vector_cross_engine_validation/%s.zip", resourceName), tableTargetDir, getClass());
    String tablePath = tableTargetDir.resolve(resourceName).toString();
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    createTable(tableEnv, tablePath, tableType.name());

    List<Row> rows = CollectionUtil.iteratorToList(tableEnv.executeSql("select id, name, ts from vector_table").collect());
    assertRowsEquals(rows, "[+I[1, doc-1, 1000], +I[2, doc-2, 2000]]");
  }

  private static int arrayLength(Object value) {
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

  private static void assertFloatVector(Object value, float expected) {
    assertEquals(128, arrayLength(value));
    for (int i = 0; i < 128; i++) {
      assertEquals(expected, ((Number) arrayValue(value, i)).floatValue(), 0.00001f);
    }
  }

  private static void assertDoubleVector(Object value, double expected) {
    assertEquals(128, arrayLength(value));
    for (int i = 0; i < 128; i++) {
      assertEquals(expected, ((Number) arrayValue(value, i)).doubleValue(), 0.0000001d);
    }
  }

  private static void assertByteVector(Object value, byte expected) {
    assertEquals(128, arrayLength(value));
    for (int i = 0; i < 128; i++) {
      assertEquals(expected, ((Number) arrayValue(value, i)).byteValue());
    }
  }
}
