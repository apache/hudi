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

import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestTableEnvs;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integration test for cross-engine compatibility - verifying that Flink can read Variant tables written by Spark 4.0.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestVariantCrossEngineCompatibility {

  @TempDir
  Path tempDir;

  /**
   * Helper method to verify that Flink can read Spark 4.0 Variant tables.
   * Variant data is represented as ROW<value BYTES, metadata BYTES> in Flink.
   */
  private void verifyFlinkCanReadSparkVariantTable(String tablePath, String tableType, String testDescription) throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();

    // Create a Hudi table pointing to the Spark-written data
    // In Flink, Variant is represented as ROW<value BYTES, metadata BYTES>
    // NOTE: value is a reserved keyword
    String createTableDdl = String.format(
        "CREATE TABLE variant_table ("
            + "  id INT,"
            + "  name STRING,"
            + "  v ROW<`value` BYTES, metadata BYTES>,"
            + "  ts BIGINT,"
            + "  PRIMARY KEY (id) NOT ENFORCED"
            + ") WITH ("
            + "  'connector' = 'hudi',"
            + "  'path' = '%s',"
            + "  'table.type' = '%s'"
            + ")",
        tablePath, tableType);

    tableEnv.executeSql(createTableDdl);

    // Query the table to verify Flink can read the data
    TableResult result = tableEnv.executeSql("SELECT id, name, v, ts FROM variant_table ORDER BY id");
    List<Row> rows = CollectionUtil.iteratorToList(result.collect());

    // Verify we got the expected row (after Spark 4.0 delete operation, only 1 row remains)
    assertEquals(1, rows.size(), "Should have 1 row after delete operation in Spark 4.0 (" + testDescription + ")");

    Row row = rows.get(0);
    assertEquals(1, row.getField(0), "First column should be id=1");
    assertEquals("row1", row.getField(1), "Second column should be name=row1");
    assertEquals(1000L, row.getField(3), "Fourth column should be ts=1000");

    // Verify the variant column is readable as a ROW with binary fields
    Row variantRow = (Row) row.getField(2);
    assertNotNull(variantRow, "Variant column should not be null");

    byte[] valueBytes = (byte[]) variantRow.getField(0);
    byte[] metadataBytes = (byte[]) variantRow.getField(1);

    // Expected byte values from Spark 4.0 Variant representation: {"updated": true, "new_field": 123}
    byte[] expectedValueBytes = new byte[]{0x02, 0x02, 0x01, 0x00, 0x01, 0x00, 0x03, 0x04, 0x0C, 0x7B};
    byte[] expectedMetadataBytes = new byte[]{0x01, 0x02, 0x00, 0x07, 0x10, 0x75, 0x70, 0x64, 0x61,
        0x74, 0x65, 0x64, 0x6E, 0x65, 0x77, 0x5F, 0x66, 0x69, 0x65, 0x6C, 0x64};

    assertArrayEquals(expectedValueBytes, valueBytes,
        String.format("Variant value bytes mismatch (%s). Expected: %s, Got: %s",
            testDescription,
            Arrays.toString(StringUtils.encodeHex(expectedValueBytes)),
            Arrays.toString(StringUtils.encodeHex(valueBytes))));

    assertArrayEquals(expectedMetadataBytes, metadataBytes,
        String.format("Variant metadata bytes mismatch (%s). Expected: %s, Got: %s",
            testDescription,
            Arrays.toString(StringUtils.encodeHex(expectedMetadataBytes)),
            Arrays.toString(StringUtils.encodeHex(metadataBytes))));

    tableEnv.executeSql("DROP TABLE variant_table");
  }

  @Test
  public void testFlinkReadSparkVariantCOWTable() throws Exception {
    // Test that Flink can read a COW table with Variant data written by Spark 4.0
    Path cowTargetDir = tempDir.resolve("cow");
    HoodieTestUtils.extractZipToDirectory("variant_backward_compat/variant_cow.zip", cowTargetDir, getClass());
    String cowPath = cowTargetDir.resolve("variant_cow").toString();
    verifyFlinkCanReadSparkVariantTable(cowPath, "COPY_ON_WRITE", "COW table");
  }

  @Test
  public void testFlinkReadSparkVariantMORTableWithAvro() throws Exception {
    // Test that Flink can read a MOR table with AVRO record type and Variant data written by Spark 4.0
    Path morAvroTargetDir = tempDir.resolve("mor_avro");
    HoodieTestUtils.extractZipToDirectory("variant_backward_compat/variant_mor_avro.zip", morAvroTargetDir, getClass());
    String morAvroPath = morAvroTargetDir.resolve("variant_mor_avro").toString();
    verifyFlinkCanReadSparkVariantTable(morAvroPath, "MERGE_ON_READ", "MOR table with AVRO record type");
  }

  @Test
  public void testFlinkReadSparkVariantMORTableWithSpark() throws Exception {
    // Test that Flink can read a MOR table with SPARK record type and Variant data written by Spark 4.0
    Path morSparkTargetDir = tempDir.resolve("mor_spark");
    HoodieTestUtils.extractZipToDirectory("variant_backward_compat/variant_mor_spark.zip", morSparkTargetDir, getClass());
    String morSparkPath = morSparkTargetDir.resolve("variant_mor_spark").toString();
    verifyFlinkCanReadSparkVariantTable(morSparkPath, "MERGE_ON_READ", "MOR table with SPARK record type");
  }
}