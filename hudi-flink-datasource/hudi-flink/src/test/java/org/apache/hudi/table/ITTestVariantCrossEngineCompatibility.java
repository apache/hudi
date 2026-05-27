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
 * Integration tests for Flink reading (and upserting) Apache Spark 4 VARIANT Hudi tables backed by
 * zipped golden layouts under {@code variant_backward_compat/*.zip} (see that directory's README).
 *
 * <p>Uses only existing Spark-produced fixtures; asserts Table/SQL and {@code toDataStream}
 * paths both surface the same VARIANT physical bytes as the Spark 4 golden row (id=1), and
 * exercises primary-key upserts on a writable copy of the COW fixture.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestVariantCrossEngineCompatibility {

  /**
   * Spark 4.0 golden VARIANT for id=1 after the delete flow in {@code TestVariantDataType} that
   * produced the zipped tables: {@code parse_json('{"updated": true, "new_field": 123}')}.
   */
  static final byte[] GOLDEN_VARIANT_VALUE_BYTES =
      new byte[]{0x02, 0x02, 0x01, 0x00, 0x01, 0x00, 0x03, 0x04, 0x0C, 0x7B};

  static final byte[] GOLDEN_VARIANT_METADATA_BYTES =
      new byte[]{0x01, 0x02, 0x00, 0x07, 0x10, 0x75, 0x70, 0x64, 0x61,
          0x74, 0x65, 0x64, 0x6E, 0x65, 0x77, 0x5F, 0x66, 0x69, 0x65, 0x6C, 0x64};

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

  /** Parameters: zip classpath resource, root dir inside zip, Hudi {@code table.type}, display name. */
  static List<Object[]> sparkGoldenFixtures() {
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

  /** SQL + {@code toDataStream} agree with Spark&nbsp;4 golden VARIANT bytes on id=1. */
  @ParameterizedTest(name = "{3}")
  @MethodSource("sparkGoldenFixtures")
  public void testFlinkSqlAndDataStreamReadSparkGoldenVariant(String zipResource, String rootDir,
      String hoodieTableType, String ignoredDisplayName)
      throws Exception {
    Assumptions.assumeTrue(nativeVariantAvailable, "VARIANT requires Flink 2.1+");

    Path extractRoot = tempDir.resolve(ignoredDisplayName);
    HoodieTestUtils.extractZipToDirectory(zipResource, extractRoot, getClass());
    String tablePath = extractRoot.resolve(rootDir).toString();

    TableEnvironment tEnv = TestTableEnvs.getBatchTableEnv();
    String ddl =
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
            hoodieTableType);

    tEnv.executeSql(ddl);

    List<Row> sqlRows =
        CollectionUtil.iteratorToList(
            tEnv.executeSql("SELECT id, name, v, ts FROM variant_table ORDER BY id").collect());

    assertEquals(1, sqlRows.size(),
        "Should have one row after Spark 4.0 delete workflow (" + ignoredDisplayName + ")");
    Row sqlRow = sqlRows.get(0);
    assertGoldenVariantRow(sqlRow, "SQL COLLECT (" + ignoredDisplayName + ")");

    StreamTableEnvironment sEnv = asStreamTableEnv(tEnv);
    List<Row> dsRows =
        CollectionUtil.iteratorToList(
            sEnv
                .toDataStream(
                    sEnv.sqlQuery("SELECT id, name, v, ts FROM variant_table ORDER BY id"),
                    Row.class)
                .executeAndCollect());

    assertEquals(1, dsRows.size(), "DataStream should see one logical row (" + ignoredDisplayName + ")");
    assertGoldenVariantRow(dsRows.get(0), "DataStream (" + ignoredDisplayName + ")");

    tEnv.executeSql("DROP TABLE variant_table");
  }

  @Test
  public void testFlinkPrimaryKeyUpsertAppendsWithoutDisturbingGoldenKey() throws Exception {
    Assumptions.assumeTrue(nativeVariantAvailable, "VARIANT requires Flink 2.1+");

    Path out = tempDir.resolve("cow_upsert_append");
    HoodieTestUtils.extractZipToDirectory("variant_backward_compat/variant_cow.zip", out, getClass());
    String cowPath = out.resolve("variant_cow").toString();

    TableEnvironment tEnv = TestTableEnvs.getBatchTableEnv();
    String ddl =
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
            cowPath.replace("'", "''"));
    tEnv.executeSql(ddl);

    tEnv.executeSql(
            "INSERT INTO variant_table VALUES ("
                + "99, CAST('from_flink' AS STRING), "
                + "CAST(PARSE_JSON('{\"engine\":\"flink\",\"n\":42}') AS VARIANT NOT NULL), "
                + "CAST(9900 AS BIGINT))")
        .await();

    List<Row> rows =
        CollectionUtil.iteratorToList(
            tEnv.executeSql("SELECT id, name, v, ts FROM variant_table ORDER BY id").collect());
    assertEquals(2, rows.size(), "Should have Spark golden row plus Flink-inserted pk");

    assertGoldenVariantRow(rows.get(0), "id=1 after Flink append pk=99");

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
    Row streamGolden =
        CollectionUtil.iteratorToList(
                sEnv
                    .toDataStream(
                        sEnv.sqlQuery("SELECT id, v FROM variant_table WHERE id = 1"),
                        Row.class)
                    .executeAndCollect())
            .get(0);
    assertEquals(1, streamGolden.getField(0));
    assertGoldenVariantBytes(streamGolden.getField(1), "DataStream pk=1 after append");

    tEnv.executeSql("DROP TABLE variant_table");
  }

  @Test
  public void testFlinkPrimaryKeyUpsertOverwritesVariantsSqlMatchesDataStream() throws Exception {
    Assumptions.assumeTrue(nativeVariantAvailable, "VARIANT requires Flink 2.1+");

    Path out = tempDir.resolve("cow_pkey_overwrite");
    HoodieTestUtils.extractZipToDirectory("variant_backward_compat/variant_cow.zip", out, getClass());
    String cowPath = out.resolve("variant_cow").toString();

    TableEnvironment tEnv = TestTableEnvs.getBatchTableEnv();
    String ddl =
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
            cowPath.replace("'", "''"));
    tEnv.executeSql(ddl);

    tEnv.executeSql(
            "INSERT INTO variant_table VALUES ("
                + "1, CAST('row1' AS STRING), "
                + "CAST(PARSE_JSON('{\"flink_rewrite\":true}') AS VARIANT NOT NULL), "
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

  private static void assertGoldenVariantRow(Row row, String desc) {
    assertEquals(1, row.getField(0), desc + " — id");
    assertEquals("row1", row.getField(1), desc + " — name");
    assertEquals(1000L, row.getField(3), desc + " — ts");
    assertGoldenVariantBytes(row.getField(2), desc);
  }

  private static void assertGoldenVariantBytes(Object variantObject, String desc) {
    assertNotNull(variantObject, desc);
    DataTypeAdapterTestUtils.assertAsBinaryVariant(variantObject);

    byte[] actualValue = DataTypeAdapter.getVariantValue(variantObject);
    byte[] actualMetadata = DataTypeAdapter.getVariantMetadata(variantObject);

    assertArrayEquals(
        GOLDEN_VARIANT_VALUE_BYTES,
        actualValue,
        String.format(
            "%s — value bytes mismatch. Expected hex %s, got %s",
            desc,
            Arrays.toString(StringUtils.encodeHex(GOLDEN_VARIANT_VALUE_BYTES)),
            Arrays.toString(StringUtils.encodeHex(actualValue))));

    assertArrayEquals(
        GOLDEN_VARIANT_METADATA_BYTES,
        actualMetadata,
        String.format(
            "%s — metadata bytes mismatch. Expected hex %s, got %s",
            desc,
            Arrays.toString(StringUtils.encodeHex(GOLDEN_VARIANT_METADATA_BYTES)),
            Arrays.toString(StringUtils.encodeHex(actualMetadata))));
  }
}
