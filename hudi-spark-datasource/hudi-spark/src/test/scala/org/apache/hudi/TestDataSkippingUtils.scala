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
import org.apache.spark.sql.catalyst.expressions.{Expression, Not}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hudi.DataSkippingUtils
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, SparkSession}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import scala.collection.JavaConverters._

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

  val sourceTableSchema =
    StructType(
      Seq(
        StructField("A", LongType),
        StructField("B", StringType)
      )
    )

  val indexSchema =
    ZOrderingIndexHelper.composeIndexSchema(sourceTableSchema.fields.toSeq.asJava)

  @ParameterizedTest
  @MethodSource(Array("testBaseLookupFilterExpressionsSource", "testAdvancedLookupFilterExpressionsSource"))
  def testLookupFilterExpressions(expr: String, input: Seq[IndexRow], output: Seq[String]): Unit = {
    val resolvedExpr: Expression = resolveFilterExpr(expr, sourceTableSchema)

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
  def testStringsLookupFilterExpressions(expr: Expression, input: Seq[IndexRow], output: Seq[String]): Unit = {
    val resolvedExpr = resolveFilterExpr(expr, sourceTableSchema)
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
    spark.sessionState.analyzer.ResolveReferences(
      Filter(expr, LocalRelation(schemaFields.head, schemaFields.drop(1): _*))
    )
      .asInstanceOf[Filter].condition
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
      // The only files we can filter, are the ones containing excluded values
      arguments(
        "A != 0 AND A != 1",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0),
          IndexRow("file_4", 0, 0, 0), // only contains 0
          IndexRow("file_5", 1, 1, 0) // only contains 1
        ),
        Seq("file_1", "file_2", "file_3")),
      // This is an equivalent to the above expression
      arguments(
        "NOT(A = 0 OR A = 1)",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0),
          IndexRow("file_4", 0, 0, 0), // only contains 0
          IndexRow("file_5", 1, 1, 0) // only contains 1
        ),
        Seq("file_1", "file_2", "file_3")),

      // The only files we can filter, are the ones containing excluded values
      arguments(
      "A != 0 OR B != 'abc'",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0),
          IndexRow("file_4", 0, 0, 0, "abc", "abc", 0), // only contains A = 0, B = 'abc'
          IndexRow("file_5", 0, 0, 0, "abc", "abc", 0) // only contains A = 0, B = 'abc'
        ),
        Seq("file_1", "file_2", "file_3")),
      // This is an equivalent to the above expression
      arguments(
        "NOT(A = 0 AND B = 'abc')",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0),
          IndexRow("file_3", -2, -1, 0),
          IndexRow("file_4", 0, 0, 0, "abc", "abc", 0), // only contains A = 0, B = 'abc'
          IndexRow("file_5", 0, 0, 0, "abc", "abc", 0) // only contains A = 0, B = 'abc'
        ),
        Seq("file_1", "file_2", "file_3"))
    )
  }
}
