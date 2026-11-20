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

package org.apache.spark.sql.hudi.dml.schema

import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType

import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Tag

import java.io.File

/** Whole and pruned reads of BLOB columns return the stored references and inline bytes. */
@Tag("functional")
class TestBlobPrunedProjection extends HoodieSparkSqlTestBase {

  private val functional = new org.scalatest.Tag("functional")

  private def withBlobTable(check: (String, String) => Unit): Unit = {
    withRecordType(Seq(HoodieRecordType.SPARK))(withTempDir { tmp =>
      val tablePath = new File(tmp, "hudi").getCanonicalPath
      val tableName = generateTableName
      spark.sql(s"""create table $tableName (id int, b blob, ts long) using hudi
                   |location '$tablePath' tblproperties (
                   |primaryKey = 'id', type = 'mor', preCombineField = 'ts',
                   |hoodie.index.type = 'INMEMORY', hoodie.compact.inline = 'false')""".stripMargin)
      spark.sql(s"""insert into $tableName values
                   |(1, named_struct('type', 'OUT_OF_LINE', 'data', cast(null as binary),
                   |'reference', named_struct('external_path', '/blob/first', 'offset', 0L,
                   |'length', 100L, 'managed', false)), 1000),
                   |(2, named_struct('type', 'OUT_OF_LINE', 'data', cast(null as binary),
                   |'reference', named_struct('external_path', '/blob/second', 'offset', 7L,
                   |'length', 60L, 'managed', false)), 1000),
                   |(3, named_struct('type', 'INLINE', 'data', cast(X'0a0b' as binary),
                   |'reference', cast(null as struct<external_path:string, offset:bigint,
                   |length:bigint, managed:boolean>)), 1000)""".stripMargin)
      val (_, view) = HoodieSparkSqlTestBase.getMetaClientAndFileSystemView(tablePath)
      try {
        val slices = view.getLatestFileSlices("").iterator()
        assert(slices.hasNext)
        while (slices.hasNext) {
          val slice = slices.next()
          assert(!slice.getBaseFile.isPresent && slice.hasLogFiles)
        }
      } finally {
        view.close()
      }
      check(tableName, tablePath)
    })
  }

  test("Whole BLOB reads preserve the stored values", functional) {
    withBlobTable { (tableName, _) =>
      withSQLConf("hoodie.file.group.reader.enabled" -> "true") {
        val rows = spark.sql(s"select id, b from $tableName order by id").collect()
        assert(rows.map(_.getInt(0)).toSeq == Seq(1, 2, 3))
        assert(rows(0).getStruct(1) == Row("OUT_OF_LINE", null, Row("/blob/first", 0L, 100L, false)))
        assert(rows(1).getStruct(1) == Row("OUT_OF_LINE", null, Row("/blob/second", 7L, 60L, false)))
        val inline = rows(2).getStruct(1)
        assert(inline.getString(0) == "INLINE")
        assert(inline.getAs[Array[Byte]](1).sameElements(Array[Byte](0x0a, 0x0b)))
        assert(inline.isNullAt(2))
      }
    }
  }

  test("Pruned BLOB reads preserve references and inline bytes", functional) {
    withBlobTable { (tableName, tablePath) =>
      for (vectorized <- Seq("true", "false")) {
        withSQLConf("hoodie.file.group.reader.enabled" -> "true",
          "spark.sql.parquet.enableVectorizedReader" -> vectorized,
          "spark.sql.optimizer.nestedSchemaPruning.enabled" -> "true") {
          val expected = Seq(Row(1, "/blob/first", 100L), Row(2, "/blob/second", 60L), Row(3, null, null))
          assert(spark.sql(s"select id, b.reference.external_path, b.reference.length from $tableName order by id")
            .collect().toSeq == expected)
          assert(spark.read.format("hudi").load(tablePath)
            .selectExpr("id", "b.reference.external_path", "b.reference.length").orderBy("id").collect().toSeq == expected)
          val bytes = spark.sql(s"select b.data from $tableName where id = 3").collect().head.getAs[Array[Byte]](0)
          assert(bytes.sameElements(Array[Byte](0x0a, 0x0b)))
        }
      }
    }
  }

  /**
   * Pruned reads over file slices that have a base file: a COW table (base files only) and a MOR table whose
   * update lands in a log file next to the base file. The MOR tests above use the in-memory index, so every
   * row lives in a log file. A slice with only a base file is read by Spark's Parquet reader, not by the file
   * group reader, so the COW reads and the MOR reads before the update pin that path; the MOR read after the
   * update (base file + log file) goes through the file group reader and covers the BLOB alignment there.
   */
  for (tableType <- Seq("cow", "mor")) {
    test(s"Pruned BLOB reads on a $tableType slice with a base file preserve references and inline bytes", functional) {
      withRecordType(Seq(HoodieRecordType.SPARK))(withTempDir { tmp =>
        val tablePath = new File(tmp, "hudi").getCanonicalPath
        val tableName = generateTableName
        spark.sql(s"""create table $tableName (id int, b blob, ts long) using hudi
                     |location '$tablePath' tblproperties (
                     |primaryKey = 'id', type = '$tableType', preCombineField = 'ts',
                     |hoodie.compact.inline = 'false')""".stripMargin)
        spark.sql(s"""insert into $tableName values
                     |(1, named_struct('type', 'OUT_OF_LINE', 'data', cast(null as binary),
                     |'reference', named_struct('external_path', '/blob/first', 'offset', 0L,
                     |'length', 100L, 'managed', false)), 1000),
                     |(3, named_struct('type', 'INLINE', 'data', cast(X'0a0b' as binary),
                     |'reference', cast(null as struct<external_path:string, offset:bigint,
                     |length:bigint, managed:boolean>)), 1000)""".stripMargin)
        assertSlices(tablePath, expectLogFiles = false)
        checkPrunedReads(tableName, tablePath, Seq(Row(1, "/blob/first", 100L), Row(3, null, null)))

        spark.sql(s"""update $tableName set b = named_struct('type', 'OUT_OF_LINE', 'data', cast(null as binary),
                     |'reference', named_struct('external_path', '/blob/updated', 'offset', 5L,
                     |'length', 40L, 'managed', false)), ts = 2000 where id = 1""".stripMargin)
        assertSlices(tablePath, expectLogFiles = tableType == "mor")
        checkPrunedReads(tableName, tablePath, Seq(Row(1, "/blob/updated", 40L), Row(3, null, null)))
      })
    }
  }

  /** Every latest file slice has a base file, and has log files exactly when `expectLogFiles`. */
  private def assertSlices(tablePath: String, expectLogFiles: Boolean): Unit = {
    val (_, view) = HoodieSparkSqlTestBase.getMetaClientAndFileSystemView(tablePath)
    try {
      val slices = view.getLatestFileSlices("").iterator()
      assert(slices.hasNext)
      while (slices.hasNext) {
        val slice = slices.next()
        assert(slice.getBaseFile.isPresent, s"slice without a base file: $slice")
        if (!expectLogFiles) {
          assert(!slice.hasLogFiles, s"unexpected log files: $slice")
        }
      }
      if (expectLogFiles) {
        val withLogs = view.getLatestFileSlices("").filter(s => s.hasLogFiles).count()
        assert(withLogs > 0, "expected a slice with log files")
      }
    } finally {
      view.close()
    }
  }

  private def checkPrunedReads(tableName: String, tablePath: String, expected: Seq[Row]): Unit = {
    for (vectorized <- Seq("true", "false")) {
      withSQLConf("hoodie.file.group.reader.enabled" -> "true",
        "spark.sql.parquet.enableVectorizedReader" -> vectorized,
        "spark.sql.optimizer.nestedSchemaPruning.enabled" -> "true") {
        assert(spark.sql(s"select id, b.reference.external_path, b.reference.length from $tableName order by id")
          .collect().toSeq == expected, s"vectorized=$vectorized")
        assert(spark.read.format("hudi").load(tablePath)
          .selectExpr("id", "b.reference.external_path", "b.reference.length").orderBy("id").collect().toSeq == expected,
          s"vectorized=$vectorized")
        val bytes = spark.sql(s"select b.data from $tableName where id = 3").collect().head.getAs[Array[Byte]](0)
        assert(bytes.sameElements(Array[Byte](0x0a, 0x0b)), s"vectorized=$vectorized")
        assert(spark.sql(s"select b.type from $tableName order by id").collect().map(_.getString(0)).toSeq
          == Seq("OUT_OF_LINE", "INLINE"), s"vectorized=$vectorized")
      }
    }
  }
}
