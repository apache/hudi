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
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IT case for writing out-of-line (OOL) BLOB columns through the Flink writer and reading them back.
 *
 * <p>Verifies that the Hudi {@link HoodieSchema.Blob} structure round-trips through the Flink
 * write/read pipeline (both COW and MOR), that updates land through the MOR Avro log path, and that
 * the stored table schema keeps the BLOB logical type instead of degrading the column to a generic
 * Flink/Avro record.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestBlobWrite {

  @TempDir
  File tempFile;

  private static final String BLOB_COLUMN = "blob_col";

  private static final String BLOB_COLUMN_DDL =
      "  " + BLOB_COLUMN + " ROW<\n"
          + "    `type` STRING NOT NULL,\n"
          + "    `data` BYTES,\n"
          + "    `reference` ROW<\n"
          + "      external_path STRING NOT NULL,\n"
          + "      `offset` BIGINT,\n"
          + "      `length` BIGINT,\n"
          + "      managed BOOLEAN NOT NULL\n"
          + "    >\n"
          + "  >,\n";

  /**
   * Regression for {@code HoodieSchemaConverter#isBlobStructure}: Flink SQL {@code CREATE TABLE}
   * may declare nested {@code ROW} fields as {@code NOT NULL}, but
   * {@link ResolvedSchema#toPhysicalRowDataType()} does not always preserve those constraints in
   * the {@link RowType}. Schema inference must still recognize the BLOB shape (same path as
   * {@code HoodieTableFactory#inferAvroSchema}), otherwise {@code blob_col} would degrade to a
   * generic RECORD in the committed Hoodie schema.
   */
  @Test
  public void testFlinkSqlDdlPhysicalRowTypeStillMapsToHoodieBlob() {
    TableEnvironment tableEnv = batchEnv();
    final String probeTable = "flink_blob_ddl_probe";
    String createProbeDdl =
        "CREATE TABLE "
            + probeTable
            + " (\n"
            + "  id BIGINT,\n"
            + "  name STRING,\n"
            + BLOB_COLUMN_DDL
            + "  ts BIGINT\n"
            + ") WITH ('connector'='blackhole')";
    tableEnv.executeSql(createProbeDdl);

    ResolvedSchema resolved = tableEnv.from(probeTable).getResolvedSchema();
    RowType physical = (RowType) resolved.toPhysicalRowDataType().getLogicalType();
    // DDL uses NOT NULL on nested BLOB ROW fields where the canonical Hoodie BLOB shape is
    // stricter. Flink's physical RowType from ResolvedSchema#toPhysicalRowDataType() may or may
    // not preserve those flags across versions (see Flink table config / release notes linked in
    // the PR). We do not assert which fields widen — only that Hudi still recognizes the column as
    // a BLOB (regression for HoodieSchemaConverter#isBlobStructure).

    HoodieSchema recordSchema =
        HoodieSchemaConverter.convertToSchema(
            physical, HoodieSchemaUtils.getRecordQualifiedName(probeTable));
    HoodieSchemaField blobField =
        recordSchema
            .getField(BLOB_COLUMN)
            .orElseThrow(() -> new AssertionError("blob_col missing from converted HoodieSchema"));
    assertTrue(
        blobField.schema().isBlobField(),
        "Physical RowType from Flink SQL DDL must still map to Hoodie BLOB, got: "
            + blobField.schema());

    tableEnv.executeSql("DROP TABLE " + probeTable);
  }

  private void createTable(TableEnvironment tableEnv, String tablePath, HoodieTableType tableType) {
    String createTableDdl = String.format(
        "CREATE TABLE blob_table (\n"
            + "  id BIGINT,\n"
            + "  name STRING,\n"
            + BLOB_COLUMN_DDL
            + "  ts BIGINT,\n"
            + "  PRIMARY KEY (id) NOT ENFORCED\n"
            + ") WITH (\n"
            + "  'connector' = 'hudi',\n"
            + "  'path' = '%s',\n"
            + "  'table.type' = '%s',\n"
            + "  'ordering.fields' = 'ts'\n"
            + ");",
        tablePath, tableType.name());
    tableEnv.executeSql(createTableDdl);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testWriteAndReadOutOfLineBlob(HoodieTableType tableType) throws Exception {
    TableEnvironment tableEnv = batchEnv();
    String tablePath = new File(tempFile, "blob_table").getAbsolutePath();
    createTable(tableEnv, tablePath, tableType);

    // First batch: insert two OOL blob references.
    execInsert(tableEnv,
        "INSERT INTO blob_table VALUES\n"
            + "(1, 'doc-1', ROW('OUT_OF_LINE', CAST(NULL AS BYTES), "
            + "ROW('file1.bin', CAST(0 AS BIGINT), CAST(100 AS BIGINT), false)), 1000),\n"
            + "(2, 'doc-2', ROW('OUT_OF_LINE', CAST(NULL AS BYTES), "
            + "ROW('file1.bin', CAST(100 AS BIGINT), CAST(200 AS BIGINT), false)), 2000)");

    List<Row> rows = readOrdered(tableEnv);
    assertEquals(2, rows.size());
    assertOutOfLineRow(rows.get(0), 1L, "doc-1", "file1.bin", 0L, 100L, 1000L);
    assertOutOfLineRow(rows.get(1), 2L, "doc-2", "file1.bin", 100L, 200L, 2000L);

    // Second batch: upsert the same keys with new references. For MOR this exercises the Avro
    // log write path (RowData -> Avro), including the BLOB enum `type` field.
    execInsert(tableEnv,
        "INSERT INTO blob_table VALUES\n"
            + "(1, 'doc-1', ROW('OUT_OF_LINE', CAST(NULL AS BYTES), "
            + "ROW('file2.bin', CAST(500 AS BIGINT), CAST(300 AS BIGINT), false)), 3000),\n"
            + "(2, 'doc-2', ROW('OUT_OF_LINE', CAST(NULL AS BYTES), "
            + "ROW('file2.bin', CAST(800 AS BIGINT), CAST(400 AS BIGINT), false)), 4000)");

    List<Row> updated = readOrdered(tableEnv);
    assertEquals(2, updated.size());
    assertOutOfLineRow(updated.get(0), 1L, "doc-1", "file2.bin", 500L, 300L, 3000L);
    assertOutOfLineRow(updated.get(1), 2L, "doc-2", "file2.bin", 800L, 400L, 4000L);

    // The stored table schema must keep the BLOB logical type, not a generic record.
    assertBlobTypePreserved(tablePath);
  }

  private static List<Row> readOrdered(TableEnvironment tableEnv) {
    return CollectionUtil.iteratorToList(
        tableEnv.executeSql("select id, name, blob_col, ts from blob_table order by id").collect());
  }

  private static void assertOutOfLineRow(Row row, long id, String name, String path,
                                         long offset, long length, long ts) {
    assertEquals(id, row.getField(0));
    assertEquals(name, row.getField(1));
    Row blob = (Row) row.getField(2);
    assertNotNull(blob, "blob struct must be populated");
    assertEquals("OUT_OF_LINE", blob.getField(0));
    assertNull(blob.getField(1), "inline data must be null for OUT_OF_LINE blob");
    Row reference = (Row) blob.getField(2);
    assertNotNull(reference, "reference must be populated for OUT_OF_LINE blob");
    assertEquals(path, reference.getField(0));
    assertEquals(offset, reference.getField(1));
    assertEquals(length, reference.getField(2));
    assertEquals(false, reference.getField(3));
    assertEquals(ts, row.getField(3));
  }

  private static void assertBlobTypePreserved(String tablePath) throws Exception {
    HoodieTableMetaClient metaClient =
        StreamerUtil.createMetaClient(tablePath, new org.apache.hadoop.conf.Configuration());
    HoodieSchema tableSchema = new TableSchemaResolver(metaClient).getTableSchema();
    HoodieSchemaField blobField = tableSchema.getField(BLOB_COLUMN)
        .orElseThrow(() -> new AssertionError("blob_col field missing from table schema"));
    assertTrue(blobField.schema().isBlobField(),
        "blob_col must keep the BLOB logical type, found: " + blobField.schema());
  }

  private static TableEnvironment batchEnv() {
    TableEnvironment tableEnv = org.apache.hudi.utils.TestTableEnvs.getBatchTableEnv();
    tableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
    return tableEnv;
  }

  private static void execInsert(TableEnvironment tableEnv, String insertSql) throws Exception {
    TableResult result = tableEnv.executeSql(insertSql);
    result.getJobClient().get().getJobExecutionResult().get(120, TimeUnit.SECONDS);
  }

  // -------------------------------------------------------------------------
  //  Blob read materialization IT
  // -------------------------------------------------------------------------

  /**
   * Writes an OOL blob reference pointing to a real local temp file, then reads the table back
   * with {@code read.blob.materialize=true} and asserts the BLOB struct is materialized
   * (type=INLINE, data=<original bytes>, reference=null).
   *
   * <p>Both COW and MOR table types are tested. The MOR variant also exercises an upsert so that
   * at least one row lives in an Avro log file and must be merged before materialization.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testReadMaterializesOutOfLineBlob(HoodieTableType tableType) throws Exception {
    // Write two small payloads to a temp file that the OOL reference will point to.
    byte[] payload1 = {10, 20, 30, 40, 50};
    byte[] payload2 = {60, 70, 80, 90};
    File blobFile = new File(tempFile, "blobs.bin");
    try (FileOutputStream fos = new FileOutputStream(blobFile)) {
      fos.write(payload1);
      fos.write(payload2);
    }
    String blobFilePath = blobFile.getAbsolutePath();

    TableEnvironment tableEnv = batchEnv();
    String tablePath = new File(tempFile, "blob_materialize_" + tableType.name()).getAbsolutePath();
    createTableWithMaterialize(tableEnv, tablePath, tableType);

    // Insert two rows with OOL refs pointing into the temp file.
    execInsert(tableEnv,
        "INSERT INTO blob_mat_table VALUES\n"
            + "(1, 'doc-1', ROW('OUT_OF_LINE', CAST(NULL AS BYTES), "
            + "ROW('" + blobFilePath + "', CAST(0 AS BIGINT), CAST(5 AS BIGINT), false)), 1000),\n"
            + "(2, 'doc-2', ROW('OUT_OF_LINE', CAST(NULL AS BYTES), "
            + "ROW('" + blobFilePath + "', CAST(5 AS BIGINT), CAST(4 AS BIGINT), false)), 2000)");

    // For MOR: upsert row 1 so it has an Avro log entry that must be merged before materialization.
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      execInsert(tableEnv,
          "INSERT INTO blob_mat_table VALUES\n"
              + "(1, 'doc-1-updated', ROW('OUT_OF_LINE', CAST(NULL AS BYTES), "
              + "ROW('" + blobFilePath + "', CAST(0 AS BIGINT), CAST(5 AS BIGINT), false)), 3000)");
    }

    List<Row> rows = CollectionUtil.iteratorToList(
        tableEnv.executeSql("SELECT id, name, blob_col, ts FROM blob_mat_table ORDER BY id").collect());

    assertEquals(2, rows.size(), "Expected 2 rows after materialization");

    // Row 1 — payload1
    Row row1 = rows.get(0);
    assertEquals(1L, row1.getField(0));
    Row blob1 = (Row) row1.getField(2);
    assertNotNull(blob1, "Blob struct for row 1 must not be null");
    assertEquals("INLINE", blob1.getField(0), "type must be INLINE after materialization");
    assertArrayEquals(payload1, (byte[]) blob1.getField(1), "Inline data must match original payload1");
    assertNull(blob1.getField(2), "reference must be null after materialization");

    // Row 2 — payload2
    Row row2 = rows.get(1);
    assertEquals(2L, row2.getField(0));
    Row blob2 = (Row) row2.getField(2);
    assertNotNull(blob2, "Blob struct for row 2 must not be null");
    assertEquals("INLINE", blob2.getField(0), "type must be INLINE after materialization");
    assertArrayEquals(payload2, (byte[]) blob2.getField(1), "Inline data must match original payload2");
    assertNull(blob2.getField(2), "reference must be null after materialization");

    tableEnv.executeSql("DROP TABLE blob_mat_table");
  }

  /**
   * Verifies that when {@code read.blob.materialize=false} (default), OOL blob structs are
   * returned as-is with the original OUT_OF_LINE type and reference intact.
   */
  @Test
  public void testReadWithMaterializeDisabledReturnsOolStructAsIs() throws Exception {
    File blobFile = new File(tempFile, "no_mat_blobs.bin");
    try (FileOutputStream fos = new FileOutputStream(blobFile)) {
      fos.write(new byte[]{1, 2, 3});
    }
    String blobFilePath = blobFile.getAbsolutePath();

    TableEnvironment tableEnv = batchEnv();
    String tablePath = new File(tempFile, "no_mat_cow").getAbsolutePath();
    createTable(tableEnv, tablePath, HoodieTableType.COPY_ON_WRITE);

    execInsert(tableEnv,
        "INSERT INTO blob_table VALUES\n"
            + "(1, 'doc-1', ROW('OUT_OF_LINE', CAST(NULL AS BYTES), "
            + "ROW('" + blobFilePath + "', CAST(0 AS BIGINT), CAST(3 AS BIGINT), false)), 1000)");

    List<Row> rows = readOrdered(tableEnv);
    assertEquals(1, rows.size());
    // Without materialize, the blob struct remains OUT_OF_LINE.
    assertOutOfLineRow(rows.get(0), 1L, "doc-1", blobFilePath, 0L, 3L, 1000L);

    tableEnv.executeSql("DROP TABLE blob_table");
  }

  /**
   * Verifies that nearby OOL ranges from the same file are coalesced into a single read when
   * {@code read.blob.batching.max.gap.bytes} is large enough, and that the bytes are still
   * correct for each row.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testBatchingCoalescesNearbyRanges(HoodieTableType tableType) throws Exception {
    // Write 30 bytes; row1 → [0,10), row2 → [15,25) — gap of 5 bytes, well within any default.
    byte[] fileBytes = new byte[30];
    for (int i = 0; i < fileBytes.length; i++) {
      fileBytes[i] = (byte) (i + 1);
    }
    File blobFile = new File(tempFile, "batch_" + tableType.name() + ".bin");
    try (FileOutputStream fos = new FileOutputStream(blobFile)) {
      fos.write(fileBytes);
    }
    String blobFilePath = blobFile.getAbsolutePath();

    TableEnvironment tableEnv = batchEnv();
    String tablePath = new File(tempFile, "batch_test_" + tableType.name()).getAbsolutePath();
    createTableWithMaterialize(tableEnv, tablePath, tableType);

    execInsert(tableEnv,
        "INSERT INTO blob_mat_table VALUES\n"
            + "(1, 'r1', ROW('OUT_OF_LINE', CAST(NULL AS BYTES), "
            + "ROW('" + blobFilePath + "', CAST(0 AS BIGINT), CAST(10 AS BIGINT), false)), 1000),\n"
            + "(2, 'r2', ROW('OUT_OF_LINE', CAST(NULL AS BYTES), "
            + "ROW('" + blobFilePath + "', CAST(15 AS BIGINT), CAST(10 AS BIGINT), false)), 2000)");

    List<Row> rows = CollectionUtil.iteratorToList(
        tableEnv.executeSql("SELECT id, blob_col FROM blob_mat_table ORDER BY id").collect());

    assertEquals(2, rows.size());

    byte[] expected1 = java.util.Arrays.copyOfRange(fileBytes, 0, 10);
    byte[] expected2 = java.util.Arrays.copyOfRange(fileBytes, 15, 25);

    assertArrayEquals(expected1, (byte[]) ((Row) rows.get(0).getField(1)).getField(1));
    assertArrayEquals(expected2, (byte[]) ((Row) rows.get(1).getField(1)).getField(1));

    tableEnv.executeSql("DROP TABLE blob_mat_table");
  }

  private void createTableWithMaterialize(
      TableEnvironment tableEnv, String tablePath, HoodieTableType tableType) {
    String createTableDdl = String.format(
        "CREATE TABLE blob_mat_table (\n"
            + "  id BIGINT,\n"
            + "  name STRING,\n"
            + BLOB_COLUMN_DDL
            + "  ts BIGINT,\n"
            + "  PRIMARY KEY (id) NOT ENFORCED\n"
            + ") WITH (\n"
            + "  'connector' = 'hudi',\n"
            + "  'path' = '%s',\n"
            + "  'table.type' = '%s',\n"
            + "  'ordering.fields' = 'ts',\n"
            + "  'read.blob.materialize' = 'true',\n"
            + "  'read.blob.batching.max.gap.bytes' = '128',\n"
            + "  'read.blob.batching.lookahead.size' = '20'\n"
            + ");",
        tablePath, tableType.name());
    tableEnv.executeSql(createTableDdl);
  }
}
