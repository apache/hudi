/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.blob

import org.apache.hudi.blob.BlobTestHelpers._
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.exception.HoodieIOException
import org.apache.hudi.testutils.HoodieClientTestBase

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.Collections

/**
 * Tests for the read_blob() SQL function.
 *
 * This test suite verifies:
 * <ul>
 *   <li>Basic SQL integration with read_blob()</li>
 *   <li>Integration with WHERE clauses, JOINs</li>
 *   <li>Configuration parameter handling</li>
 *   <li>Error handling for invalid inputs</li>
 * </ul>
 */
class TestReadBlobSQL extends HoodieClientTestBase {

  @Test
  def testReadOutOfLineBlobOnHudiBackedTable(): Unit = {
    // Verifies read_blob()'s logical plan is task-serializable over a
    // HoodieFileIndex-backed relation (DataFrame write path).
    val extFile = createTestFile(tempDir, "basic.bin", 10000)
    val tablePath = s"$tempDir/hudi_blob_table"

    val rawDf = sparkSession.createDataFrame(Seq(
        (1, "rec1", extFile, 0L, 100L),
        (2, "rec2", extFile, 100L, 100L),
        (3, "rec3", extFile, 200L, 100L)
      )).toDF("id", "name", "external_path", "offset", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("external_path"), col("offset"),
          col("length")))
      .select("id", "name", "file_info")

    // Coerce to the canonical BlobType schema. blobStructCol produces a
    // non-null reference struct, but HoodieSparkSchemaConverters
    // .validateBlobStructure rejects that on write — it demands the
    // reference field be nullable. Rebuild the DataFrame on its RDD
    // against the canonical shape so .save() doesn't fail early.
    val canonicalSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("file_info", BlobType().asInstanceOf[StructType],
        nullable = true, blobMetadata)
    ))
    val df = sparkSession.createDataFrame(rawDf.rdd, canonicalSchema)

    df.write.format("hudi")
      .option("hoodie.table.name", "blob_test")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .mode("overwrite")
      .save(tablePath)

    // Hudi read pulls HoodieFileIndex into the plan that BatchedBlobReadExec
    // serializes to executors — the scenario the exec-node fix must handle.
    sparkSession.read.format("hudi").load(tablePath)
      .createOrReplaceTempView("hudi_blob_view")

    val result = sparkSession.sql("""
      SELECT id, read_blob(file_info) AS data
      FROM hudi_blob_view
      ORDER BY id
    """).collect()

    assertEquals(3, result.length)
    // Verify the bytes read from the external file match the recorded offsets.
    result.zipWithIndex.foreach { case (row, idx) =>
      assertEquals(idx + 1, row.getInt(0))
      val bytes = row.getAs[Array[Byte]]("data")
      assertEquals(100, bytes.length)
      assertBytesContent(bytes, expectedOffset = idx * 100)
    }
  }

  @Test
  def testBasicReadBlobSQL(): Unit = {
    val filePath = createTestFile(tempDir, "basic.bin", 10000)

    // Main DataFrame with blobStructCol
    val df = sparkSession.createDataFrame(Seq(
      (1, "record1", filePath, 0L, 100L),
      (3, "record3", filePath, 100L, 100L),
      (4, "record4", filePath, 200L, 100L)
    )).toDF("id", "name", "external_path", "offset", "length")
      .withColumn("file_info", blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "name", "file_info")

    // Ensure file_info is nullable in the schema
    val schema = StructType(df.schema.map {
      case StructField("file_info", dt, _, md) => StructField("file_info", dt, nullable = true, md)
      case other => other
    })
    val dfWithNullable = sparkSession.createDataFrame(df.rdd, schema)

    // DataFrame with a null blob value
    val nullRow = Row(2, "record2", null)
    val nullDf = sparkSession.createDataFrame(Collections.singletonList(nullRow), schema)

    // Union the null row
    val fullDf = dfWithNullable.unionByName(nullDf)
    fullDf.createOrReplaceTempView("test_table")

    // Use SQL with read_blob
    val result = sparkSession.sql("""
      SELECT id, name, read_blob(file_info) as data
      FROM test_table
      WHERE id <= 3
      ORDER BY id
    """)

    val rows = result.collect()
    assertEquals(3, rows.length)

    // Verify data is binary for non-null rows
    val data1 = rows(0).getAs[Array[Byte]]("data")
    assertEquals(100, data1.length)
    assertBytesContent(data1)

    // The null_blob row should have null data
    assertTrue(rows(1).isNullAt(2))

    val data3 = rows(2).getAs[Array[Byte]]("data")
    assertEquals(100, data3.length)
    assertBytesContent(data3, expectedOffset = 100)
  }

  @Test
  def testReadBlobWithJoin(): Unit = {
    val filePath = createTestFile(tempDir, "join.bin", 10000)

    // Create blob table
    val blobDF = sparkSession.createDataFrame(Seq(
      (1, filePath, 0L, 100L),
      (2, filePath, 100L, 100L)
    )).toDF("id", "external_path", "offset", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "file_info")

    blobDF.createOrReplaceTempView("blob_table")

    // Create metadata table
    val metaDF = sparkSession.createDataFrame(Seq(
      (1, "Alice"),
      (2, "Bob")
    )).toDF("id", "name")

    metaDF.createOrReplaceTempView("meta_table")

    // SQL with JOIN
    val result = sparkSession.sql("""
      SELECT m.id, m.name, read_blob(b.file_info) as data
      FROM meta_table m
      JOIN blob_table b ON m.id = b.id
      ORDER BY m.id
    """)

    val rows = result.collect()
    assertEquals(2, rows.length)
    assertEquals("Alice", rows(0).getAs[String]("name"))
    assertEquals(100, rows(0).getAs[Array[Byte]]("data").length)
    assertEquals("Bob", rows(1).getAs[String]("name"))
    assertEquals(100, rows(1).getAs[Array[Byte]]("data").length)

    // Verify data content
    val data1 = rows(0).getAs[Array[Byte]]("data")
    assertBytesContent(data1)
  }

  @Test
  def testReadBlobInSubquery(): Unit = {
    val filePath = createTestFile(tempDir, "subquery.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, "A", filePath, 0L, 100L),
      (2, "A", filePath, 100L, 100L),
      (3, "B", filePath, 200L, 100L)
    )).toDF("id", "category", "external_path", "offset", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "category", "file_info")

    df.createOrReplaceTempView("subquery_table")

    // SQL with subquery
    val result = sparkSession.sql("""
      SELECT * FROM (
        SELECT id, category, read_blob(file_info) as data
        FROM subquery_table
      ) WHERE category = 'A'
    """)

    val rows = result.collect()
    assertEquals(2, rows.length)
    rows.foreach { row =>
      assertEquals("A", row.getAs[String]("category"))
      assertEquals(100, row.getAs[Array[Byte]]("data").length)
    }
  }

  @Test
  def testConfigurationParameters(): Unit = {
    val filePath = createTestFile(tempDir, "config.bin", 50000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath, 0L, 100L),
      (2, filePath, 5000L, 100L),  // 4.9KB gap
      (3, filePath, 10000L, 100L)
    )).toDF("id", "external_path", "offset", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "file_info")

    df.createOrReplaceTempView("config_table")

    // Use withSparkConfig to automatically manage configuration
    withSparkConfig(sparkSession, Map(
      "hoodie.blob.batching.max.gap.bytes" -> "10000",
      "hoodie.blob.batching.lookahead.size" -> "100"
    )) {
      val result = sparkSession.sql("""
        SELECT id, read_blob(file_info) as data
        FROM config_table
      """)

      val rows = result.collect()
      assertEquals(3, rows.length)

      // Verify all reads completed successfully
      rows.foreach { row =>
        assertEquals(100, row.getAs[Array[Byte]]("data").length)
      }
    }
  }

  @Test
  def testMultipleReadBlobInSameQuery(): Unit = {
    val filePath1 = createTestFile(tempDir, "multi1.bin", 10000)
    val filePath2 = createTestFile(tempDir, "multi2.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath1, 0L, 50L, filePath2, 500L, 50L),
      (2, filePath1, 100L, 50L, filePath2, 600L, 50L)
    )).toDF("id", "external_path1", "offset1", "length1", "external_path2", "offset2", "length2")
      .withColumn("file_info1",
        blobStructCol("file_info1", col("external_path1"), col("offset1"), col("length1")))
      .withColumn("file_info2",
        blobStructCol("file_info2", col("external_path2"), col("offset2"), col("length2")))
      .select("id", "file_info1", "file_info2")

    df.createOrReplaceTempView("multi_table")

    // SQL with multiple read_blob calls
    val result = sparkSession.sql("""
      SELECT
        id,
        read_blob(file_info1) as data1,
        read_blob(file_info2) as data2
      FROM multi_table
    """)

    val rows = result.collect()
    assertEquals(2, rows.length)

    // Row 1: data1 = file1 at offset 0, data2 = file2 at offset 500
    val data1_row1 = rows(0).getAs[Array[Byte]]("data1")
    val data2_row1 = rows(0).getAs[Array[Byte]]("data2")
    assertEquals(50, data1_row1.length)
    assertEquals(50, data2_row1.length)
    assertBytesContent(data1_row1, expectedOffset = 0)
    assertBytesContent(data2_row1, expectedOffset = 500)

    // Row 2: data1 = file1 at offset 100, data2 = file2 at offset 600
    val data1_row2 = rows(1).getAs[Array[Byte]]("data1")
    val data2_row2 = rows(1).getAs[Array[Byte]]("data2")
    assertBytesContent(data1_row2, expectedOffset = 100)
    assertBytesContent(data2_row2, expectedOffset = 600)
  }

  @Test
  def testReadBlobWithEmptyResult(): Unit = {
    val filePath = createTestFile(tempDir, "empty.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath, 0L, 100L),
      (2, filePath, 100L, 100L)
    )).toDF("id", "external_path", "offset", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "file_info")

    df.createOrReplaceTempView("empty_table")

    // SQL that returns no rows
    val result = sparkSession.sql("""
      SELECT id, read_blob(file_info) as data
      FROM empty_table
      WHERE id > 100
    """)

    val rows = result.collect()
    assertEquals(0, rows.length)
  }

  @Test
  def testReadBlobMultipleFiles(): Unit = {
    val filePath1 = createTestFile(tempDir, "file1.bin", 10000)
    val filePath2 = createTestFile(tempDir, "file2.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath1, 0L, 100L),
      (2, filePath2, 0L, 100L),
      (3, filePath1, 100L, 100L),
      (4, filePath2, 100L, 100L)
    )).toDF("id", "external_path", "offset", "length")
    .withColumn("file_info",
      blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "file_info")

    df.createOrReplaceTempView("multi_file_table")

    // SQL reading from multiple files
    val result = sparkSession.sql("""
      SELECT id, read_blob(file_info) as data
      FROM multi_file_table
      ORDER BY id
    """)

    val rows = result.collect()
    assertEquals(4, rows.length)

    // Verify all data was read correctly
    rows.foreach { row =>
      assertEquals(100, row.getAs[Array[Byte]]("data").length)
    }
  }

  @Test
  def testReadBlobInWhereClause(): Unit = {
    val filePath = createTestFile(tempDir, "where.bin", 10000)
    val df = sparkSession.createDataFrame(Seq(
      (1, filePath, 0L, 50L),   // 50 bytes — filtered out
      (2, filePath, 100L, 100L), // 100 bytes — passes filter
      (3, filePath, 200L, 200L)  // 200 bytes — passes filter
    )).toDF("id", "external_path", "offset", "length")
      .withColumn("file_info", blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "file_info")
    df.createOrReplaceTempView("where_table")

    val result = sparkSession.sql("""
      SELECT id, read_blob(file_info) AS data
      FROM where_table
      WHERE length(read_blob(file_info)) > 50
      ORDER BY id
    """)

    val rows = result.collect()
    assertEquals(2, rows.length)

    // validate that rows with IDs 2 and 3 are returned
    assertEquals(2, rows(0).getInt(0))
    assertEquals(3, rows(1).getInt(0))
  }

  @Test
  def testReadBlobWithCaseWhen(): Unit = {
    val filePath = createTestFile(tempDir, "case.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, true, filePath, 0L, 100L),
      (2, false, filePath, 100L, 100L),
      (3, true, filePath, 200L, 100L)
    )).toDF("id", "should_resolve", "external_path", "offset", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "should_resolve", "file_info")

    df.createOrReplaceTempView("case_table")

    // SQL with CASE WHEN - note: this tests that the expression is handled
    // even in conditional contexts
    val result = sparkSession.sql("""
      SELECT
        id,
        should_resolve,
        CASE
          WHEN should_resolve THEN read_blob(file_info)
          ELSE NULL
        END as data
      FROM case_table
    """)

    val rows = result.collect()
    assertEquals(3, rows.length)

    // Row 1 should have data
    assertTrue(rows(0).getAs[Boolean]("should_resolve"))
    assertNotNull(rows(0).get(2))

    // Row 2 should have null
    assertFalse(rows(1).getAs[Boolean]("should_resolve"))
    assertTrue(rows(1).isNullAt(2))

    // Row 3 should have data
    assertTrue(rows(2).getAs[Boolean]("should_resolve"))
    assertNotNull(rows(2).get(2))
  }

  @Test
  def testReadBlobWithMissingFile(): Unit = {
    val missingPath = tempDir.resolve("does_not_exist.bin").toString
    val df = sparkSession.createDataFrame(Seq(
      (1, missingPath, 0L, 10L)
    )).toDF("id", "external_path", "offset", "length")
      .withColumn("file_info", blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "file_info")
    df.createOrReplaceTempView("missing_file_table")
    val thrown = assertThrows(classOf[Exception], () => {
      sparkSession.sql("SELECT id, read_blob(file_info) as data FROM missing_file_table").collect()
    })
    assertTrue(thrown.getCause.isInstanceOf[HoodieIOException])
  }

  @Test
  def testReadBlobOnNonBlobColumn(): Unit = {
    val df = sparkSession.createDataFrame(Seq(
      (1, "not_a_blob")
    )).toDF("id", "not_blob")
    df.createOrReplaceTempView("non_blob_table")
    val thrown = assertThrows(classOf[Exception], () => {
      sparkSession.sql("SELECT id, read_blob(not_blob) as data FROM non_blob_table").collect()
    })
    assertTrue(thrown.isInstanceOf[IllegalArgumentException])
    assertTrue(thrown.getMessage.contains("must be compatible with BlobType"))
  }

  @Test
  def testReadBlobInProjectAndFilter(): Unit = {
    val filePath = createTestFile(tempDir, "project_and_filter.bin", 10000)

    // DataFrame with blobStructCol
    val df = sparkSession.createDataFrame(Seq(
      (1, "record1", filePath, 0L, 100L),
      (2, "record2", filePath, 100L, 100L),
      (3, "record3", filePath, 200L, 100L)
    )).toDF("id", "name", "external_path", "offset", "length")
      .withColumn("file_info", blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "name", "file_info")

    df.createOrReplaceTempView("project_and_filter_table")

    // Query with read_blob in both SELECT and WHERE
    val result = sparkSession.sql("""
      SELECT id, name, read_blob(file_info) as data
      FROM project_and_filter_table
      WHERE length(read_blob(file_info)) = 100
      ORDER BY id
    """)

    val rows = result.collect()
    assertEquals(3, rows.length)
    rows.zipWithIndex.foreach { case (row, idx) =>
      assertEquals(100, row.getAs[Array[Byte]]("data").length)
      assertBytesContent(row.getAs[Array[Byte]]("data"), expectedOffset = idx * 100)
    }
  }
}
