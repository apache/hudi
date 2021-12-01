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

package org.apache.hudi

import org.apache.hudi.index.zorder.ZOrderingIndexHelper
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, Not}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hudi.DataSkippingUtils
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, VarcharType}
import org.apache.spark.sql.{Column, SparkSession}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import scala.collection.JavaConverters._

// NOTE: Only A, B columns are indexed
case class IndexRow(
  file: String,
  A_minValue: Long,
  A_maxValue: Long,
  A_num_nulls: Long,
  B_minValue: String = null,
  B_maxValue: String = null,
  B_num_nulls: Long = -1
)

class TestDataSkippingUtils extends HoodieClientTestBase {

  var spark: SparkSession = _

  @BeforeEach
  override def setUp(): Unit = {
    initSparkContexts()
    spark = sqlContext.sparkSession
  }

  val indexedCols = Seq("A", "B")
  val sourceTableSchema =
    StructType(
      Seq(
        StructField("A", LongType),
        StructField("B", StringType),
        StructField("C", VarcharType(32))
      )
    )

  val indexSchema =
    ZOrderingIndexHelper.composeIndexSchema(
      sourceTableSchema.fields.toSeq
        .filter(f => indexedCols.contains(f.name))
        .asJava
    )

  @ParameterizedTest
  @MethodSource(Array("testBaseLookupFilterExpressionsSource", "testAdvancedLookupFilterExpressionsSource"))
  def testLookupFilterExpressions(sourceExpr: String, input: Seq[IndexRow], output: Seq[String]): Unit = {
    val resolvedExpr: Expression = resolveFilterExpr(sourceExpr, sourceTableSchema)

    val lookupFilter = DataSkippingUtils.createZIndexLookupFilter(resolvedExpr, indexSchema)

    val spark2 = spark
    import spark2.implicits._

    val indexDf = spark.createDataset(input)

    val rows = indexDf.where(new Column(lookupFilter))
      .select("file")
      .collect()
      .map(_.getString(0))
      .toSeq

    assertEquals(output, rows)
  }

  @ParameterizedTest
  @MethodSource(Array("testStringsLookupFilterExpressionsSource"))
  def testStringsLookupFilterExpressions(sourceExpr: Expression, input: Seq[IndexRow], output: Seq[String]): Unit = {
    val resolvedExpr = resolveFilterExpr(sourceExpr, sourceTableSchema)
    val lookupFilter = DataSkippingUtils.createZIndexLookupFilter(resolvedExpr, indexSchema)

    val spark2 = spark
    import spark2.implicits._

    val indexDf = spark.createDataset(input)

    val rows = indexDf.where(new Column(lookupFilter))
      .select("file")
      .collect()
      .map(_.getString(0))
      .toSeq

    assertEquals(output, rows)
  }

  private def resolveFilterExpr(exprString: String, tableSchema: StructType): Expression = {
    val expr = spark.sessionState.sqlParser.parseExpression(exprString)
    resolveFilterExpr(expr, tableSchema)
  }

  private def resolveFilterExpr(expr: Expression, tableSchema: StructType): Expression = {
    val schemaFields = tableSchema.fields
    val resolvedExpr = spark.sessionState.analyzer.ResolveReferences(
      Filter(expr, LocalRelation(schemaFields.head, schemaFields.drop(1): _*))
    )
      .asInstanceOf[Filter].condition

    checkForUnresolvedRefs(resolvedExpr)
  }

  def checkForUnresolvedRefs(resolvedExpr: Expression): Expression =
    resolvedExpr match {
      case UnresolvedAttribute(_) => throw new IllegalStateException("unresolved attribute")
      case _ => resolvedExpr.mapChildren(e => checkForUnresolvedRefs(e))
    }
}

object TestDataSkippingUtils {
  def testStringsLookupFilterExpressionsSource(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      arguments(
        col("B").startsWith("abc").expr,
        Seq(
          IndexRow("file_1", 0, 0, 0, "aba", "adf", 1), // may contain strings starting w/ "abc"
          IndexRow("file_2", 0, 0, 0, "adf", "azy", 0),
          IndexRow("file_3", 0, 0, 0, "aaa", "aba", 0)
        ),
        Seq("file_1")),
      arguments(
        Not(col("B").startsWith("abc").expr),
        Seq(
          IndexRow("file_1", 0, 0, 0, "aba", "adf", 1), // may contain strings starting w/ "abc"
          IndexRow("file_2", 0, 0, 0, "adf", "azy", 0),
          IndexRow("file_3", 0, 0, 0, "aaa", "aba", 0),
          IndexRow("file_4", 0, 0, 0, "abc123", "abc345", 0) // all strings start w/ "abc"
        ),
        Seq("file_1", "file_2", "file_3"))
    )
  }

  def testBaseLookupFilterExpressionsSource(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      // TODO cases
      //    A = null
      arguments(
        "A = 0",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0)
        ),
        Seq("file_2")),
      arguments(
        "0 = A",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0)
        ),
        Seq("file_2")),
      arguments(
        "A != 0",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", 0, 0, 0) // Contains only 0s
        ),
        Seq("file_1", "file_2")),
      arguments(
        "0 != A",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", 0, 0, 0) // Contains only 0s
        ),
        Seq("file_1", "file_2")),
      arguments(
        "A < 0",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0)
        ),
        Seq("file_2", "file_3")),
      arguments(
        "0 > A",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0)
        ),
        Seq("file_2", "file_3")),
      arguments(
        "A > 0",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0)
        ),
        Seq("file_1", "file_2")),
      arguments(
        "0 < A",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0)
        ),
        Seq("file_1", "file_2")),
      arguments(
        "A <= -1",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0)
        ),
        Seq("file_2", "file_3")),
      arguments(
        "-1 >= A",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0)
        ),
        Seq("file_2", "file_3")),
      arguments(
        "A >= 1",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0)
        ),
        Seq("file_1", "file_2")),
      arguments(
        "1 <= A",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0)
        ),
        Seq("file_1", "file_2")),
      arguments(
        "A is null",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 1)
        ),
        Seq("file_2")),
      arguments(
        "A is not null",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 1)
        ),
        Seq("file_1")),
      arguments(
        "A in (0, 1)",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0)
        ),
        Seq("file_1", "file_2")),
      arguments(
        "A not in (0, 1)",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0),
          IndexRow("file_4", 0, 0, 0), // only contains 0
          IndexRow("file_5", 1, 1, 0) // only contains 1
        ),
        Seq("file_1", "file_2", "file_3"))
    )
  }

  def testAdvancedLookupFilterExpressionsSource(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      arguments(
        // Filter out all rows that contain either A = 0 OR A = 1
        "A != 0 AND A != 1",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0),
          IndexRow("file_4", 0, 0, 0), // only contains 0
          IndexRow("file_5", 1, 1, 0) // only contains 1
        ),
        Seq("file_1", "file_2", "file_3")),
      arguments(
        // This is an equivalent to the above expression
        "NOT(A = 0 OR A = 1)",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0),
          IndexRow("file_4", 0, 0, 0), // only contains 0
          IndexRow("file_5", 1, 1, 0) // only contains 1
        ),
        Seq("file_1", "file_2", "file_3")),

      arguments(
        // Filter out all rows that contain A = 0 AND B = 'abc'
      "A != 0 OR B != 'abc'",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0),
          IndexRow("file_4", 0, 0, 0, "abc", "abc", 0), // only contains A = 0, B = 'abc'
          IndexRow("file_5", 0, 0, 0, "abc", "abc", 0) // only contains A = 0, B = 'abc'
        ),
        Seq("file_1", "file_2", "file_3")),
      arguments(
        // This is an equivalent to the above expression
        "NOT(A = 0 AND B = 'abc')",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0),
          IndexRow("file_4", 0, 0, 0, "abc", "abc", 0), // only contains A = 0, B = 'abc'
          IndexRow("file_5", 0, 0, 0, "abc", "abc", 0) // only contains A = 0, B = 'abc'
        ),
        Seq("file_1", "file_2", "file_3")),

      arguments(
        // Queries contains expression involving non-indexed column C
        "A = 0 AND B = 'abc' AND C = '...'",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0),
          IndexRow("file_4", 0, 0, 0, "aaa", "xyz", 0) // might contain A = 0 AND B = 'abc'
        ),
        Seq("file_4")),

      arguments(
        // Queries contains expression involving non-indexed column C
        "A = 0 OR B = 'abc' OR C = '...'",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0),
          IndexRow("file_4", 0, 0, 0, "aaa", "xyz", 0) // might contain B = 'abc'
        ),
        Seq("file_1", "file_2", "file_3", "file_4"))
    )
  }
}
