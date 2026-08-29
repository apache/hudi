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

package org.apache.spark.sql.hudi.common

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.scalatest.funsuite.AnyFunSuite

import java.net.URI

/**
 * Unit coverage for the pure helper methods in [[HoodieSqlCommonUtils]] that are otherwise
 * only reached through heavier read/write code paths.
 */
class TestHoodieSqlCommonUtils extends AnyFunSuite {

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

  test("partition style detectors classify partition paths") {
    val t = partitionedTable(Seq("dt"))

    assertTrue(HoodieSqlCommonUtils.isHiveStyledPartitioning(Seq("dt=2021-04-01"), t))
    assertFalse(HoodieSqlCommonUtils.isHiveStyledPartitioning(Seq("2021-04-01"), t))
    assertTrue(HoodieSqlCommonUtils.isHiveStyledPartitioning(Seq("anything"), nonPartitionedTable))

    assertTrue(HoodieSqlCommonUtils.isUrlEncodeEnabled(Seq("dt=2021-04-01"), t))
    assertFalse(HoodieSqlCommonUtils.isUrlEncodeEnabled(Seq("a/b"), t))
    assertFalse(HoodieSqlCommonUtils.isUrlEncodeEnabled(Seq("x"), nonPartitionedTable))

    // Both detectors compare the slash-separated fragment count against the partition columns,
    // so a multi-column table only matches when every fragment is present.
    val t2 = partitionedTable(Seq("dt", "hh"))
    assertTrue(HoodieSqlCommonUtils.isHiveStyledPartitioning(Seq("dt=2021-04-01/hh=12"), t2))
    assertFalse(HoodieSqlCommonUtils.isHiveStyledPartitioning(Seq("dt=2021-04-01/12"), t2))
    assertTrue(HoodieSqlCommonUtils.isUrlEncodeEnabled(Seq("2021%2F04%2F01/12"), t2))
    assertFalse(HoodieSqlCommonUtils.isUrlEncodeEnabled(Seq("2021/04/01/12"), t2))
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
