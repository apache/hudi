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

import org.apache.hudi.adapter.DataTypeAdapter;
import org.apache.hudi.adapter.DataTypeAdapterTestUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestTableEnvs;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Integration test for cross-engine compatibility - verifying that Flink can read Variant tables
 * written by Spark 4.0, including via Table/SQL and {@code toDataStream}, and can upsert rows on
 * a writable copy of the Spark-produced COW fixture under {@code variant_backward_compat/} using
 * Flink SQL ({@code INSERT ... VALUES} and {@code INSERT ... SELECT}).
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestVariantCrossEngineCompatibility {

  private static boolean nativeVariantAvailable;

  @TempDir
  Path tempDir;

  @BeforeAll
  static void checkVariantSupport() {
    try {
      DataTypeAdapter.createVariantType();
      nativeVariantAvailable = true;
    } catch (UnsupportedOperationException e) {
      nativeVariantAvailable = false;
    }
  }

  static List<Object[]> sparkVariantBackwardCompatFixtures() {
    List<Object[]> list = new ArrayList<>();
    list.add(new Object[] {"variant_backward_compat/variant_cow.zip", "variant_cow", "COPY_ON_WRITE", "COW"});
    list.add(
        new Object[] {"variant_backward_compat/variant_mor_avro.zip", "variant_mor_avro",
            "MERGE_ON_READ", "MOR_AVRO"});
    list.add(
        new Object[] {"variant_backward_compat/variant_mor_spark.zip", "variant_mor_spark",
            "MERGE_ON_READ", "MOR_SPARK"});
    return list;
  }

  private static StreamTableEnvironment asStreamTableEnv(TableEnvironment env) {
    assertInstanceOf(
        StreamTableEnvironment.class,
        env,
        "Expected StreamTableEnvironment for DataStream assertions");
    return (StreamTableEnvironment) env;
  }

  /**
   * Helper method to verify that Flink can read Spark 4.0 Variant tables via Table/SQL.
   *
   * @return the {@link TableEnvironment} with {@code variant_table} still registered (caller drops)
   */
  private TableEnvironment verifyFlinkCanReadSparkVariantTable(
      String tablePath, String tableType, String testDescription) throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();

    String createTableDdl =
        String.format(
            "CREATE TABLE variant_table ("
                + "  id INT,"
                + "  name STRING,"
                + "  v VARIANT,"
                + "  ts BIGINT,"
                + "  PRIMARY KEY (id) NOT ENFORCED"
                + ") WITH ("
                + "  'connector' = 'hudi',"
                + "  'path' = '%s',"
                + "  'table.type' = '%s'"
                + ")",
            tablePath.replace("'", "''"),
            tableType);

    tableEnv.executeSql(createTableDdl);

    TableResult result = tableEnv.executeSql("SELECT id, name, v, ts FROM variant_table ORDER BY id");
    List<Row> rows = CollectionUtil.iteratorToList(result.collect());

    // Verify we got the expected row (after Spark 4.0 delete operation, only 1 row remains)
    assertEquals(
        1,
        rows.size(),
        "Should have 1 row after delete operation in Spark 4.0 (" + testDescription + ")");

    Row row = rows.get(0);
    assertEquals(1, row.getField(0), "First column should be id=1");
    assertEquals("row1", row.getField(1), "Second column should be name=row1");
    assertEquals(1000L, row.getField(3), "Fourth column should be ts=1000");

    assertExpectedSpark40VariantBytes(row.getField(2), testDescription);

    return tableEnv;
  }

  @ParameterizedTest(name = "{3}")
  @MethodSource("sparkVariantBackwardCompatFixtures")
  public void testFlinkSqlAndDataStreamReadSparkVariantTable(
      String zipResource,
      String rootDir,
      String hoodieTableType,
      String testDescription)
      throws Exception {
    Assumptions.assumeTrue(nativeVariantAvailable, "VARIANT requires Flink 2.1+");

    Path extractRoot = tempDir.resolve(testDescription);
    HoodieTestUtils.extractZipToDirectory(zipResource, extractRoot, getClass());
    String tablePath = extractRoot.resolve(rootDir).toString();

    TableEnvironment tEnv =
        verifyFlinkCanReadSparkVariantTable(tablePath, hoodieTableType, testDescription);

    StreamTableEnvironment sEnv = asStreamTableEnv(tEnv);
    List<Row> streamRows =
        CollectionUtil.iteratorToList(
            sEnv
                .toDataStream(
                    sEnv.sqlQuery("SELECT id, name, v, ts FROM variant_table ORDER BY id"),
                    Row.class)
                .executeAndCollect());

    assertEquals(
        1,
        streamRows.size(),
        "DataStream should see one row after Spark 4.0 delete (" + testDescription + ")");
    Row streamRow = streamRows.get(0);
    assertEquals(1, streamRow.getField(0), "First column should be id=1");
    assertEquals("row1", streamRow.getField(1), "Second column should be name=row1");
    assertEquals(1000L, streamRow.getField(3), "Fourth column should be ts=1000");
    assertExpectedSpark40VariantBytes(streamRow.getField(2), testDescription + " (DataStream)");

    tEnv.executeSql("DROP TABLE variant_table");
  }

  @Test
  public void testFlinkPrimaryKeyUpsertAppendsWithoutDisturbingExistingKey() throws Exception {
    Assumptions.assumeTrue(nativeVariantAvailable, "VARIANT requires Flink 2.1+");

    Path out = tempDir.resolve("cow_upsert_append");
    HoodieTestUtils.extractZipToDirectory("variant_backward_compat/variant_cow.zip", out, getClass());
    String cowPath = out.resolve("variant_cow").toString();

    TableEnvironment tEnv = TestTableEnvs.getBatchTableEnv();
    tEnv.executeSql(
        String.format(
            "CREATE TABLE variant_table ("
                + "  id INT,"
                + "  name STRING,"
                + "  v VARIANT,"
                + "  ts BIGINT,"
                + "  PRIMARY KEY (id) NOT ENFORCED"
                + ") WITH ("
                + "  'connector' = 'hudi',"
                + "  'path' = '%s',"
                + "  'table.type' = 'COPY_ON_WRITE'"
                + ")",
            cowPath.replace("'", "''")));

    tEnv.executeSql(
            "INSERT INTO variant_table VALUES ("
                + "99, CAST('from_flink' AS STRING), "
                + "PARSE_JSON('{\"engine\":\"flink\",\"n\":42}'), "
                + "CAST(9900 AS BIGINT))")
        .await();

    List<Row> rows =
        CollectionUtil.iteratorToList(
            tEnv.executeSql("SELECT id, name, v, ts FROM variant_table ORDER BY id").collect());
    assertEquals(2, rows.size(), "Should have Spark row plus Flink-inserted pk");

    assertEquals(1, rows.get(0).getField(0));
    assertEquals("row1", rows.get(0).getField(1));
    assertEquals(1000L, rows.get(0).getField(3));
    assertExpectedSpark40VariantBytes(rows.get(0).getField(2), "id=1 after Flink append pk=99");

    Row appended = rows.get(1);
    assertEquals(99, appended.getField(0));
    DataTypeAdapterTestUtils.assertAsBinaryVariant(appended.getField(2));
    String appendedJson =
        CollectionUtil.iteratorToList(
                tEnv.executeSql("SELECT CAST(v AS STRING) FROM variant_table WHERE id = 99").collect())
            .get(0)
            .getField(0)
            .toString();
    assertTrue(
        appendedJson.contains("flink") && appendedJson.contains("42"),
        "Inserted VARIANT stringify should expose engine+n; got: " + appendedJson);

    StreamTableEnvironment sEnv = asStreamTableEnv(tEnv);
    Row streamRow =
        CollectionUtil.iteratorToList(
                sEnv
                    .toDataStream(
                        sEnv.sqlQuery("SELECT id, v FROM variant_table WHERE id = 1"),
                        Row.class)
                    .executeAndCollect())
            .get(0);
    assertEquals(1, streamRow.getField(0));
    assertExpectedSpark40VariantBytes(streamRow.getField(1), "DataStream pk=1 after append");

    tEnv.executeSql("DROP TABLE variant_table");
  }

  @Test
  public void testFlinkPrimaryKeyUpsertOverwritesVariantsSqlMatchesDataStream() throws Exception {
    Assumptions.assumeTrue(nativeVariantAvailable, "VARIANT requires Flink 2.1+");

    Path out = tempDir.resolve("cow_pkey_overwrite");
    HoodieTestUtils.extractZipToDirectory("variant_backward_compat/variant_cow.zip", out, getClass());
    String cowPath = out.resolve("variant_cow").toString();

    TableEnvironment tEnv = TestTableEnvs.getBatchTableEnv();
    tEnv.executeSql(
        String.format(
            "CREATE TABLE variant_table ("
                + "  id INT,"
                + "  name STRING,"
                + "  v VARIANT,"
                + "  ts BIGINT,"
                + "  PRIMARY KEY (id) NOT ENFORCED"
                + ") WITH ("
                + "  'connector' = 'hudi',"
                + "  'path' = '%s',"
                + "  'table.type' = 'COPY_ON_WRITE'"
                + ")",
            cowPath.replace("'", "''")));

    tEnv.executeSql(
            "INSERT INTO variant_table VALUES ("
                + "1, CAST('row1' AS STRING), "
                + "PARSE_JSON('{\"flink_rewrite\":true}'), "
                + "CAST(5000 AS BIGINT))")
        .await();

    List<Row> rows =
        CollectionUtil.iteratorToList(
            tEnv.executeSql("SELECT id, name, v, ts FROM variant_table ORDER BY id").collect());
    assertEquals(1, rows.size());
    Row sqlRow = rows.get(0);
    assertEquals(1, sqlRow.getField(0));
    assertEquals("row1", sqlRow.getField(1));
    DataTypeAdapterTestUtils.assertAsBinaryVariant(sqlRow.getField(2));

    Object v = sqlRow.getField(2);
    String json =
        CollectionUtil.iteratorToList(
                tEnv.executeSql("SELECT CAST(v AS STRING) FROM variant_table WHERE id = 1").collect())
            .get(0)
            .getField(0)
            .toString();
    assertTrue(json.contains("flink_rewrite"), "Upsert VARIANT should stringify rewrite JSON; got: " + json);

    StreamTableEnvironment sEnv = asStreamTableEnv(tEnv);
    Row dsRow =
        CollectionUtil.iteratorToList(
                sEnv
                    .toDataStream(sEnv.sqlQuery("SELECT id, v FROM variant_table WHERE id = 1"), Row.class)
                    .executeAndCollect())
            .get(0);
    assertArrayEquals(DataTypeAdapter.getVariantMetadata(v), DataTypeAdapter.getVariantMetadata(dsRow.getField(1)));
    assertArrayEquals(DataTypeAdapter.getVariantValue(v), DataTypeAdapter.getVariantValue(dsRow.getField(1)));

    tEnv.executeSql("DROP TABLE variant_table");
  }

  @Test
  public void testFlinkPrimaryKeyUpsertOverwriteViaInsertSelectSql() throws Exception {
    Assumptions.assumeTrue(nativeVariantAvailable, "VARIANT requires Flink 2.1+");

    Path out = tempDir.resolve("cow_pkey_overwrite_insert_select");
    HoodieTestUtils.extractZipToDirectory("variant_backward_compat/variant_cow.zip", out, getClass());
    String cowPath = out.resolve("variant_cow").toString();

    TableEnvironment tEnv = TestTableEnvs.getBatchTableEnv();
    tEnv.executeSql(
        String.format(
            "CREATE TABLE variant_table ("
                + "  id INT,"
                + "  name STRING,"
                + "  v VARIANT,"
                + "  ts BIGINT,"
                + "  PRIMARY KEY (id) NOT ENFORCED"
                + ") WITH ("
                + "  'connector' = 'hudi',"
                + "  'path' = '%s',"
                + "  'table.type' = 'COPY_ON_WRITE'"
                + ")",
            cowPath.replace("'", "''")));

    tEnv.executeSql(
            "INSERT INTO variant_table "
                + "SELECT "
                + "1, "
                + "CAST('row1_flink_sql' AS STRING), "
                + "PARSE_JSON('{\"flink_sql_select\":true,\"n\":7}'), "
                + "CAST(6000 AS BIGINT)")
        .await();

    List<Row> rows =
        CollectionUtil.iteratorToList(
            tEnv.executeSql("SELECT id, name, v, ts FROM variant_table ORDER BY id").collect());
    assertEquals(1, rows.size(), "PK upsert via INSERT SELECT should still leave one row");
    Row row = rows.get(0);
    assertEquals(1, row.getField(0));
    assertEquals("row1_flink_sql", row.getField(1));
    assertEquals(6000L, row.getField(3));
    DataTypeAdapterTestUtils.assertAsBinaryVariant(row.getField(2));

    String json =
        CollectionUtil.iteratorToList(
                tEnv.executeSql("SELECT CAST(v AS STRING) FROM variant_table WHERE id = 1").collect())
            .get(0)
            .getField(0)
            .toString();
    assertTrue(
        json.contains("flink_sql_select") && json.contains("7"),
        "INSERT SELECT upsert VARIANT should stringify rewrite JSON; got: " + json);

    tEnv.executeSql("DROP TABLE variant_table");
  }

  private static void assertExpectedSpark40VariantBytes(Object variantObject, String testDescription) {
    assertNotNull(variantObject, "Variant column should not be null");
    DataTypeAdapterTestUtils.assertAsBinaryVariant(variantObject);

    // Expected byte values from Spark 4.0 Variant representation: {"updated": true, "new_field": 123}
    byte[] expectedValueBytes =
        new byte[]{0x02, 0x02, 0x01, 0x00, 0x01, 0x00, 0x03, 0x04, 0x0C, 0x7B};
    byte[] expectedMetadataBytes =
        new byte[]{0x01, 0x02, 0x00, 0x07, 0x10, 0x75, 0x70, 0x64, 0x61,
            0x74, 0x65, 0x64, 0x6E, 0x65, 0x77, 0x5F, 0x66, 0x69, 0x65, 0x6C, 0x64};

    assertArrayEquals(
        expectedValueBytes,
        DataTypeAdapter.getVariantValue(variantObject),
        String.format(
            "Variant value bytes mismatch (%s). Expected: %s, Got: %s",
            testDescription,
            Arrays.toString(StringUtils.encodeHex(expectedValueBytes)),
            Arrays.toString(StringUtils.encodeHex(DataTypeAdapter.getVariantValue(variantObject)))));

    assertArrayEquals(
        expectedMetadataBytes,
        DataTypeAdapter.getVariantMetadata(variantObject),
        String.format(
            "Variant metadata bytes mismatch (%s). Expected: %s, Got: %s",
            testDescription,
            Arrays.toString(StringUtils.encodeHex(expectedMetadataBytes)),
            Arrays.toString(StringUtils.encodeHex(DataTypeAdapter.getVariantMetadata(variantObject)))));
  }
}
