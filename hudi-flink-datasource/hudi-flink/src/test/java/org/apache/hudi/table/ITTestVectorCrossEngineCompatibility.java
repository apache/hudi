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

import java.nio.file.Path;
import java.util.List;

import static org.apache.hudi.utils.TestData.assertRowsEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
  public void testValidationVectorColumnsFromCOWTable(HoodieTableType tableType) throws Exception {
    // Validate exception will be thrown when reading VECTOR columns from a table with VECTOR columns
    Path cowTargetDir = tempDir.resolve("table");
    String resourceName = tableType == HoodieTableType.MERGE_ON_READ ? "vector_mor" : "vector_cow";
    HoodieTestUtils.extractZipToDirectory(String.format("vector_cross_engine_validation/%s.zip", resourceName), cowTargetDir, getClass());
    String cowPath = cowTargetDir.resolve(resourceName).toString();
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    createTable(tableEnv, cowPath, tableType.name());

    // ValidationException expects to be thrown
    assertThrows(RuntimeException.class,
        () -> CollectionUtil.iteratorToList(tableEnv.executeSql("select * from vector_table").collect()),
        "Unexpected type exception. Primitive type: FIXED_LEN_BYTE_ARRAY. Field type: FLOAT. Field name: embedding1");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testReadNonVectorColumnsFromCOWTable(HoodieTableType tableType) throws Exception {
    // Test that Flink can read non-VECTOR columns from a table with VECTOR columns
    Path cowTargetDir = tempDir.resolve("table");
    String resourceName = tableType == HoodieTableType.MERGE_ON_READ ? "vector_mor" : "vector_cow";
    HoodieTestUtils.extractZipToDirectory(String.format("vector_cross_engine_validation/%s.zip", resourceName), cowTargetDir, getClass());
    String cowPath = cowTargetDir.resolve(resourceName).toString();
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    createTable(tableEnv, cowPath, tableType.name());

    List<Row> rows = CollectionUtil.iteratorToList(tableEnv.executeSql("select id, name, ts from vector_table").collect());
    assertRowsEquals(rows, "[+I[1, doc-1, 1000], +I[2, doc-2, 2000]]");
  }
}
