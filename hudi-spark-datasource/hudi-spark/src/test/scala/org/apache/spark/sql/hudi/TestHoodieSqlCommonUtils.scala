/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi

import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.scalatest.funsuite.AnyFunSuite

import java.net.URI
import java.util.TimeZone

/**
 * Unit coverage for the pure helper methods in [[HoodieSqlCommonUtils]] that are otherwise
 * only reached through heavier read/write code paths.
 */
class TestHoodieSqlCommonUtils extends AnyFunSuite {

  // Instant formatting is timezone sensitive; pin to UTC to keep assertions deterministic.
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  private def partitionedTable(partitionCols: Seq[String]): CatalogTable = {
    val fields = StructField("id", IntegerType) +: partitionCols.map(StructField(_, StringType))
    CatalogTable(
      identifier = TableIdentifier("t", Some("default")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = StructType(fields),
      provider = Some("hudi"),
      partitionColumnNames = partitionCols)
  }

  private val nonPartitionedTable: CatalogTable = CatalogTable(
    identifier = TableIdentifier("t0", Some("default")),
    tableType = CatalogTableType.MANAGED,
    storage = CatalogStorageFormat.empty,
    schema = StructType(Seq(StructField("id", IntegerType))),
    provider = Some("hudi"))

  test("formatQueryInstant normalizes supported time formats") {
    assertTrue(HoodieSqlCommonUtils.formatQueryInstant("2021-04-01").startsWith("20210401"))
    assertTrue(HoodieSqlCommonUtils.formatQueryInstant("2021-04-01 12:30:45").startsWith("20210401123045"))
    assertTrue(HoodieSqlCommonUtils.formatQueryInstant("2021-04-01T12:30:45").startsWith("20210401123045"))
    // 10-digit epoch seconds (2021-01-01T00:00:00Z) and 13-digit epoch millis.
    assertTrue(HoodieSqlCommonUtils.formatQueryInstant("1609459200").startsWith("20210101"))
    assertTrue(HoodieSqlCommonUtils.formatQueryInstant("1609459200000").startsWith("20210101"))
    intercept[IllegalArgumentException] {
      HoodieSqlCommonUtils.formatQueryInstant("abc")
    }
  }

  test("formatIncrementalInstant passes sentinels through and normalizes real instants") {
    assertEquals(IncrementalQueryAnalyzer.START_COMMIT_EARLIEST,
      HoodieSqlCommonUtils.formatIncrementalInstant(IncrementalQueryAnalyzer.START_COMMIT_EARLIEST))
    assertEquals("000", HoodieSqlCommonUtils.formatIncrementalInstant("000"))
    // A short, zero-prefixed numeric value is treated as a legacy instant and passed through.
    assertEquals("0000001", HoodieSqlCommonUtils.formatIncrementalInstant("0000001"))
    assertTrue(HoodieSqlCommonUtils.formatIncrementalInstant("2021-04-01").startsWith("20210401"))
  }

  test("partition style detectors classify partition paths") {
    val t = partitionedTable(Seq("dt"))

    assertTrue(HoodieSqlCommonUtils.isHiveStyledPartitioning(Seq("dt=2021-04-01"), t))
    assertFalse(HoodieSqlCommonUtils.isHiveStyledPartitioning(Seq("2021-04-01"), t))
    assertTrue(HoodieSqlCommonUtils.isHiveStyledPartitioning(Seq("anything"), nonPartitionedTable))

    assertTrue(HoodieSqlCommonUtils.isUrlEncodeEnabled(Seq("dt=2021-04-01"), t))
    assertFalse(HoodieSqlCommonUtils.isUrlEncodeEnabled(Seq("a/b"), t))
    assertFalse(HoodieSqlCommonUtils.isUrlEncodeEnabled(Seq("x"), nonPartitionedTable))

    assertTrue(HoodieSqlCommonUtils.isSlashSeparatedDatePartitioning(Seq("2021/04/01"), t))
    assertFalse(HoodieSqlCommonUtils.isSlashSeparatedDatePartitioning(Seq("dt=2021-04-01"), t))
    assertFalse(HoodieSqlCommonUtils.isSlashSeparatedDatePartitioning(Seq("2021/04/01"), nonPartitionedTable))
  }

  test("config helpers and meta field utilities") {
    val opts = Map("hoodie.a" -> "1", "spark.hoodie.b" -> "2", "other" -> "3")
    assertEquals(Map("hoodie.a" -> "1"), HoodieSqlCommonUtils.filterHoodieConfigs(opts))
    assertEquals(Map("hoodie.b" -> "2"), HoodieSqlCommonUtils.extractSparkPrefixedHoodieConfigs(opts))

    val base = StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
    val withMeta = HoodieSqlCommonUtils.addMetaFields(base)
    assertTrue(withMeta.fieldNames.contains("_hoodie_commit_time"))
    assertEquals(base.fields.length + 5, withMeta.fields.length)
    assertEquals(base, HoodieSqlCommonUtils.removeMetaFields(withMeta))
    assertTrue(HoodieSqlCommonUtils.isMetaField("_hoodie_commit_time"))
    assertFalse(HoodieSqlCommonUtils.isMetaField("id"))

    val attrs = Seq(
      AttributeReference("_hoodie_commit_time", StringType)(),
      AttributeReference("id", IntegerType)())
    assertEquals(Seq("id"), HoodieSqlCommonUtils.removeMetaFields(attrs).map(_.name))
  }

  test("normalizePartitionSpec normalizes keys and rejects invalid specs") {
    val resolver = caseInsensitiveResolution
    assertEquals(Map("dt" -> "2021"),
      HoodieSqlCommonUtils.normalizePartitionSpec(Map("DT" -> "2021"), Seq("dt"), "t", resolver))

    // Unknown partition column.
    intercept[HoodieAnalysisException] {
      HoodieSqlCommonUtils.normalizePartitionSpec(Map("bad" -> "x"), Seq("dt"), "t", resolver)
    }
    // Not all partition columns specified.
    intercept[HoodieAnalysisException] {
      HoodieSqlCommonUtils.normalizePartitionSpec(Map.empty[String, String], Seq("dt"), "t", resolver)
    }
    // Duplicate partition columns.
    intercept[HoodieAnalysisException] {
      HoodieSqlCommonUtils.normalizePartitionSpec(
        Map("dt" -> "a", "DT" -> "b"), Seq("dt", "DT"), "t", resolver)
    }
  }

  test("path qualification and column helpers") {
    val qualified = HoodieSqlCommonUtils.makePathQualified(new URI("/tmp/hudi_test_path"), new Configuration())
    assertTrue(qualified.startsWith("file:"))

    val resolver = caseInsensitiveResolution
    val schema = StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
    assertEquals("id", HoodieSqlCommonUtils.findColumnByName(schema, "ID", resolver).get.name)
    assertTrue(HoodieSqlCommonUtils.findColumnByName(schema, "missing", resolver).isEmpty)
    assertTrue(HoodieSqlCommonUtils.columnEqual(
      StructField("a", IntegerType), StructField("A", IntegerType), resolver))
    assertFalse(HoodieSqlCommonUtils.columnEqual(
      StructField("a", IntegerType), StructField("a", StringType), resolver))
  }
}
