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

import org.apache.hudi.ColumnStatsIndexSupport.composeIndexSchema
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.DummyExpressionHolder
import org.apache.spark.sql.catalyst.expressions.{Expression, InSet, Not}
import org.apache.spark.sql.catalyst.optimizer.OptimizeIn
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions.{col, lower}
import org.apache.spark.sql.hudi.DataSkippingUtils
import org.apache.spark.sql.internal.SQLConf.SESSION_LOCAL_TIMEZONE
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, HoodieCatalystExpressionUtils, Row, SparkSession}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.sql.Timestamp
import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet

// NOTE: Only A, B columns are indexed
case class IndexRow(fileName: String,
                    valueCount: Long = 1,

                    // Corresponding A column is LongType
                    A_minValue: Long = -1,
                    A_maxValue: Long = -1,
                    A_nullCount: Long = -1,

                    // Corresponding B column is StringType
                    B_minValue: String = null,
                    B_maxValue: String = null,
                    B_nullCount: Long = -1,

                    // Corresponding B column is TimestampType
                    C_minValue: Timestamp = null,
                    C_maxValue: Timestamp = null,
                    C_nullCount: Long = -1) {
  def toRow: Row = Row(productIterator.toSeq: _*)
}

class TestDataSkippingUtils extends HoodieClientTestBase with SparkAdapterSupport {

  val exprUtils: HoodieCatalystExpressionUtils = sparkAdapter.getCatalystExpressionUtils

  var spark: SparkSession = _

  @BeforeEach
  override def setUp(): Unit = {
    initSparkContexts()
    spark = sqlContext.sparkSession
  }

  val indexedCols: Seq[String] = Seq("A", "B", "C")
  val sourceTableSchema: StructType =
    StructType(
      Seq(
        StructField("A", LongType),
        StructField("B", StringType),
        StructField("C", TimestampType),
        StructField("D", VarcharType(32))
      )
    )

  val indexSchema: StructType = composeIndexSchema(indexedCols, sourceTableSchema)

  @ParameterizedTest
  @MethodSource(Array(
    "testBasicLookupFilterExpressionsSource",
    "testAdvancedLookupFilterExpressionsSource",
    "testCompositeFilterExpressionsSource"
  ))
  def testLookupFilterExpressions(sourceFilterExprStr: String, input: Seq[IndexRow], expectedOutput: Seq[String]): Unit = {
    // We have to fix the timezone to make sure all date-bound utilities output
    // is consistent with the fixtures
    spark.sqlContext.setConf(SESSION_LOCAL_TIMEZONE.key, "UTC")

    val resolvedFilterExpr: Expression = exprUtils.resolveExpr(spark, sourceFilterExprStr, sourceTableSchema)
    val optimizedExpr = optimize(resolvedFilterExpr)
    val rows: Seq[String] = applyFilterExpr(optimizedExpr, input)

    assertEquals(expectedOutput, rows)
  }

  @ParameterizedTest
  @MethodSource(Array(
    "testMiscLookupFilterExpressionsSource"
  ))
  def testMiscLookupFilterExpressions(filterExpr: Expression, input: Seq[IndexRow], expectedOutput: Seq[String]): Unit = {
    // We have to fix the timezone to make sure all date-bound utilities output
    // is consistent with the fixtures
    spark.sqlContext.setConf(SESSION_LOCAL_TIMEZONE.key, "UTC")

    val resolvedFilterExpr: Expression = exprUtils.resolveExpr(spark, filterExpr, sourceTableSchema)
    val rows: Seq[String] = applyFilterExpr(resolvedFilterExpr, input)

    assertEquals(expectedOutput, rows)
  }

  @ParameterizedTest
  @MethodSource(Array("testStringsLookupFilterExpressionsSource"))
  def testStringsLookupFilterExpressions(sourceExpr: Expression, input: Seq[IndexRow], output: Seq[String]): Unit = {
    val resolvedExpr = exprUtils.resolveExpr(spark, sourceExpr, sourceTableSchema)
    val lookupFilter = DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr(resolvedExpr, indexSchema)

    val spark2 = spark
    import spark2.implicits._

    val indexDf = spark.createDataset(input)

    val rows = indexDf.where(new Column(lookupFilter))
      .select("fileName")
      .collect()
      .map(_.getString(0))
      .toSeq

    assertEquals(output, rows)
  }


  private def optimize(expr: Expression): Expression = {
    val rules: Seq[Rule[LogicalPlan]] =
      OptimizeIn ::
        Nil

    val plan: LogicalPlan = DummyExpressionHolder(Seq(expr))

    rules.foldLeft(plan) {
      case (plan, rule) => rule.apply(plan)
    }.asInstanceOf[DummyExpressionHolder].exprs.head
  }

  private def applyFilterExpr(resolvedExpr: Expression, input: Seq[IndexRow]): Seq[String] = {
    val lookupFilter = DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr(resolvedExpr, indexSchema)

    val indexDf = spark.createDataFrame(input.map(_.toRow).asJava, indexSchema)

    indexDf.where(new Column(lookupFilter))
      .select("fileName")
      .collect()
      .map(_.getString(0))
      .toSeq
  }
}

object TestDataSkippingUtils {
  def testStringsLookupFilterExpressionsSource(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      arguments(
        col("B").startsWith("abc").expr,
        Seq(
          IndexRow("file_1", valueCount = 1, B_minValue = "aba", B_maxValue = "adf", B_nullCount = 1), // may contain strings starting w/ "abc"
          IndexRow("file_2", valueCount = 1, B_minValue = "adf", B_maxValue = "azy", B_nullCount = 0),
          IndexRow("file_3", valueCount = 1, B_minValue = "aaa", B_maxValue = "aba", B_nullCount = 0)
        ),
        Seq("file_1")),
      arguments(
        Not(col("B").startsWith("abc").expr),
        Seq(
          IndexRow("file_1", valueCount = 1, B_minValue = "aba", B_maxValue = "adf", B_nullCount = 1), // may contain strings starting w/ "abc"
          IndexRow("file_2", valueCount = 1, B_minValue = "adf", B_maxValue = "azy", B_nullCount = 0),
          IndexRow("file_3", valueCount = 1, B_minValue = "aaa", B_maxValue = "aba", B_nullCount = 0),
          IndexRow("file_4", valueCount = 1, B_minValue = "abc123", B_maxValue = "abc345", B_nullCount = 0) // all strings start w/ "abc"
        ),
        Seq("file_1", "file_2", "file_3")),
      arguments(
        // Composite expression
        Not(lower(col("B")).startsWith("abc").expr),
        Seq(
          IndexRow("file_1", valueCount = 1, B_minValue = "ABA", B_maxValue = "ADF", B_nullCount = 1), // may contain strings starting w/ "ABC" (after upper)
          IndexRow("file_2", valueCount = 1, B_minValue = "ADF", B_maxValue = "AZY", B_nullCount = 0),
          IndexRow("file_3", valueCount = 1, B_minValue = "AAA", B_maxValue = "ABA", B_nullCount = 0),
          IndexRow("file_4", valueCount = 1, B_minValue = "ABC123", B_maxValue = "ABC345", B_nullCount = 0) // all strings start w/ "ABC" (after upper)
        ),
        Seq("file_1", "file_2", "file_3"))
    )
  }

  def testMiscLookupFilterExpressionsSource(): java.util.stream.Stream[Arguments] = {
    // NOTE: Have to use [[Arrays.stream]], as Scala can't resolve properly 2 overloads for [[Stream.of]]
    //       (for single element)
    java.util.Arrays.stream(
      Array(
        arguments(
          InSet(UnresolvedAttribute("A"), HashSet(0, 1)),
          Seq(
            IndexRow("file_1", valueCount = 1, 1, 2, 0),
            IndexRow("file_2", valueCount = 1, -1, 1, 0),
            IndexRow("file_3", valueCount = 1, -2, -1, 0)
          ),
          Seq("file_1", "file_2"))
      )
    )
  }

  def testBasicLookupFilterExpressionsSource(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      // TODO cases
      //    A = null
      arguments(
        "A = 0",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0)
        ),
        Seq("file_2")),
      arguments(
        "0 = A",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0)
        ),
        Seq("file_2")),
      arguments(
        "A != 0",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, 0, 0, 0) // Contains only 0s
        ),
        Seq("file_1", "file_2")),
      arguments(
        "0 != A",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, 0, 0, 0) // Contains only 0s
        ),
        Seq("file_1", "file_2")),
      arguments(
        "A < 0",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, -2, -1, 0)
        ),
        Seq("file_2", "file_3")),
      arguments(
        "0 > A",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, -2, -1, 0)
        ),
        Seq("file_2", "file_3")),
      arguments(
        "A > 0",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, -2, -1, 0)
        ),
        Seq("file_1", "file_2")),
      arguments(
        "0 < A",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, -2, -1, 0)
        ),
        Seq("file_1", "file_2")),
      arguments(
        "A <= -1",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, -2, -1, 0)
        ),
        Seq("file_2", "file_3")),
      arguments(
        "-1 >= A",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, -2, -1, 0)
        ),
        Seq("file_2", "file_3")),
      arguments(
        "A >= 1",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, -2, -1, 0)
        ),
        Seq("file_1", "file_2")),
      arguments(
        "1 <= A",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, -2, -1, 0)
        ),
        Seq("file_1", "file_2")),
      arguments(
        "A is null",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 1)
        ),
        Seq("file_2")),
      arguments(
        "A is not null",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 2, -1, 1, 1) // might still contain non-null values (if nullCount < valueCount)
        ),
        Seq("file_1", "file_2")),
      arguments(
        "A is not null",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 1) // might NOT contain non-null values (nullCount == valueCount)
        ),
        Seq("file_1")),
      arguments(
        "A in (0, 1)",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, -2, -1, 0)
        ),
        Seq("file_1", "file_2")),
      arguments(
        s"B in (${(0 to 10).map(i => s"'a$i'").mkString(",")})",
        Seq(
          IndexRow("file_1", valueCount = 1, B_minValue = "a0", B_maxValue = "a10", B_nullCount = 0),
          IndexRow("file_2", valueCount = 1, B_minValue = "b0", B_maxValue = "b10", B_nullCount = 0),
          IndexRow("file_3", valueCount = 1, B_minValue = "a10", B_maxValue = "b20", B_nullCount = 0)
        ),
        Seq("file_1", "file_3")),
      arguments(
        "A not in (0, 1)",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, -2, -1, 0),
          IndexRow("file_4", valueCount = 1, 0, 0, 0), // only contains 0
          IndexRow("file_5", valueCount = 1, 1, 1, 0) // only contains 1
        ),
        Seq("file_1", "file_2", "file_3")),
      arguments(
        // Value expression containing expression, which isn't a literal
        "A = int('0')",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0)
        ),
        Seq("file_2")),
      arguments(
        // Value expression containing reference to the other attribute (column), fallback
        "A = D",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, -2, -1, 0)
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
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, -2, -1, 0),
          IndexRow("file_4", valueCount = 1, 0, 0, 0), // only contains 0
          IndexRow("file_5", valueCount = 1, 1, 1, 0) // only contains 1
        ),
        Seq("file_1", "file_2", "file_3")),
      arguments(
        // This is an equivalent to the above expression
        "NOT(A = 0 OR A = 1)",
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, -2, -1, 0),
          IndexRow("file_4", valueCount = 1, 0, 0, 0), // only contains 0
          IndexRow("file_5", valueCount = 1, 1, 1, 0) // only contains 1
        ),
        Seq("file_1", "file_2", "file_3")),

      arguments(
        // Filter out all rows that contain A = 0 AND B = 'abc'
        "A != 0 OR B != 'abc'",
        Seq(
          IndexRow("file_1", valueCount = 1, A_minValue = 1,  A_maxValue = 2,  A_nullCount = 0),
          IndexRow("file_2", valueCount = 1, A_minValue = -1, A_maxValue = 1,  A_nullCount = 0),
          IndexRow("file_3", valueCount = 1, A_minValue = -2, A_maxValue = -1, A_nullCount =  0),
          IndexRow("file_4", valueCount = 1, A_minValue = 0, A_maxValue = 0, A_nullCount = 0, B_minValue = "abc", B_maxValue = "abc", B_nullCount = 0), // only contains A = 0, B = 'abc'
          IndexRow("file_5", valueCount = 1, A_minValue = 0, A_maxValue = 0, A_nullCount = 0, B_minValue = "abc", B_maxValue = "abc", B_nullCount = 0) // only contains A = 0, B = 'abc'
        ),
        Seq("file_1", "file_2", "file_3")),
      arguments(
        // This is an equivalent to the above expression
        "NOT(A = 0 AND B = 'abc')",
        Seq(
          IndexRow("file_1", valueCount = 1, A_minValue = 1, A_maxValue = 2, A_nullCount = 0),
          IndexRow("file_2", valueCount = 1, A_minValue = -1, A_maxValue = 1, A_nullCount = 0),
          IndexRow("file_3", valueCount = 1, A_minValue = -2, A_maxValue = -1, A_nullCount = 0),
          IndexRow("file_4", valueCount = 1, A_minValue = 0, A_maxValue = 0, A_nullCount = 0, B_minValue = "abc", B_maxValue = "abc", B_nullCount = 0), // only contains A = 0, B = 'abc'
          IndexRow("file_5", valueCount = 1, A_minValue = 0, A_maxValue = 0, A_nullCount = 0, B_minValue = "abc", B_maxValue = "abc", B_nullCount = 0) // only contains A = 0, B = 'abc'
        ),
        Seq("file_1", "file_2", "file_3")),

      arguments(
        // Queries contains expression involving non-indexed column D
        "A = 0 AND B = 'abc' AND D IS NULL",
        Seq(
          IndexRow("file_1", valueCount = 1, A_minValue = 1, A_maxValue = 2, A_nullCount = 0),
          IndexRow("file_2", valueCount = 1, A_minValue = -1, A_maxValue = 1, A_nullCount = 0),
          IndexRow("file_3", valueCount = 1, A_minValue = -2, A_maxValue = -1, A_nullCount = 0),
          IndexRow("file_4", valueCount = 1, A_minValue = 0, A_maxValue = 0, A_nullCount = 0, B_minValue = "aaa", B_maxValue = "xyz", B_nullCount = 0) // might contain A = 0 AND B = 'abc'
        ),
        Seq("file_4")),

      arguments(
        // Queries contains expression involving non-indexed column D
        "A = 0 OR B = 'abc' OR D IS NULL",
        Seq(
          IndexRow("file_1", valueCount = 1, A_minValue = 1, A_maxValue = 2, A_nullCount = 0),
          IndexRow("file_2", valueCount = 1, A_minValue = -1, A_maxValue =  1, A_nullCount = 0),
          IndexRow("file_3", valueCount = 1, A_minValue = -2, A_maxValue =  -1, A_nullCount = 0),
          IndexRow("file_4", valueCount = 1, B_minValue = "aaa", B_maxValue = "xyz", B_nullCount = 0) // might contain B = 'abc'
        ),
        Seq("file_1", "file_2", "file_3", "file_4"))
    )
  }

  def testCompositeFilterExpressionsSource(): java.util.stream.Stream[Arguments] = {
    // NOTE: all timestamps in UTC
    java.util.stream.Stream.of(
      arguments(
        "date_format(C, 'MM/dd/yyyy') = '03/07/2022'",
        Seq(
          IndexRow("file_1", valueCount = 1,
            C_minValue = new Timestamp(1646711448000L), // 03/08/2022
            C_maxValue = new Timestamp(1646797848000L), // 03/09/2022
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 1,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646711448000L), // 03/08/2022
            C_nullCount = 0)
        ),
        Seq("file_2")),
      arguments(
        "'03/07/2022' = date_format(C, 'MM/dd/yyyy')",
        Seq(
          IndexRow("file_1", valueCount = 1,
            C_minValue = new Timestamp(1646711448000L), // 03/08/2022
            C_maxValue = new Timestamp(1646797848000L), // 03/09/2022
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 1,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646711448000L), // 03/08/2022
            C_nullCount = 0)
        ),
        Seq("file_2")),
      arguments(
        "'03/07/2022' != date_format(C, 'MM/dd/yyyy')",
        Seq(
          IndexRow("file_1", valueCount = 1,
            C_minValue = new Timestamp(1646711448000L), // 03/08/2022
            C_maxValue = new Timestamp(1646797848000L), // 03/09/2022
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 1,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646625048000L), // 03/07/2022
            C_nullCount = 0)
        ),
        Seq("file_1")),
      arguments(
        "date_format(C, 'MM/dd/yyyy') != '03/07/2022'",
        Seq(
          IndexRow("file_1", valueCount = 1,
            C_minValue = new Timestamp(1646711448000L), // 03/08/2022
            C_maxValue = new Timestamp(1646797848000L), // 03/09/2022
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 1,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646625048000L), // 03/07/2022
            C_nullCount = 0)
        ),
        Seq("file_1")),
      arguments(
        "date_format(C, 'MM/dd/yyyy') < '03/08/2022'",
        Seq(
          IndexRow("file_1", valueCount = 1,
            C_minValue = new Timestamp(1646711448000L), // 03/08/2022
            C_maxValue = new Timestamp(1646797848000L), // 03/09/2022
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 1,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646711448000L), // 03/08/2022
            C_nullCount = 0)
        ),
        Seq("file_2")),
      arguments(
        "'03/08/2022' > date_format(C, 'MM/dd/yyyy')",
        Seq(
          IndexRow("file_1", valueCount = 1,
            C_minValue = new Timestamp(1646711448000L), // 03/08/2022
            C_maxValue = new Timestamp(1646797848000L), // 03/09/2022
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 1,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646711448000L), // 03/08/2022
            C_nullCount = 0)
        ),
        Seq("file_2")),
      arguments(
        "'03/08/2022' < date_format(C, 'MM/dd/yyyy')",
        Seq(
          IndexRow("file_1", valueCount = 1,
            C_minValue = new Timestamp(1646711448000L), // 03/08/2022
            C_maxValue = new Timestamp(1646797848000L), // 03/09/2022
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 1,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646711448000L), // 03/08/2022
            C_nullCount = 0)
        ),
        Seq("file_1")),
      arguments(
        "date_format(C, 'MM/dd/yyyy') > '03/08/2022'",
        Seq(
          IndexRow("file_1", valueCount = 1,
            C_minValue = new Timestamp(1646711448000L), // 03/08/2022
            C_maxValue = new Timestamp(1646797848000L), // 03/09/2022
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 1,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646711448000L), // 03/08/2022
            C_nullCount = 0)
        ),
        Seq("file_1")),
      arguments(
        "date_format(C, 'MM/dd/yyyy') <= '03/07/2022'",
        Seq(
          IndexRow("file_1", valueCount = 1,
            C_minValue = new Timestamp(1646711448000L), // 03/08/2022
            C_maxValue = new Timestamp(1646797848000L), // 03/09/2022
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 1,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646711448000L), // 03/08/2022
            C_nullCount = 0)
        ),
        Seq("file_2")),
      arguments(
        "'03/07/2022' >= date_format(C, 'MM/dd/yyyy')",
        Seq(
          IndexRow("file_1", valueCount = 1,
            C_minValue = new Timestamp(1646711448000L), // 03/08/2022
            C_maxValue = new Timestamp(1646797848000L), // 03/09/2022
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 1,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646711448000L), // 03/08/2022
            C_nullCount = 0)
        ),
        Seq("file_2")),
      arguments(
        "'03/09/2022' <= date_format(C, 'MM/dd/yyyy')",
        Seq(
          IndexRow("file_1", valueCount = 1,
            C_minValue = new Timestamp(1646711448000L), // 03/08/2022
            C_maxValue = new Timestamp(1646797848000L), // 03/09/2022
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 1,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646711448000L), // 03/08/2022
            C_nullCount = 0)
        ),
        Seq("file_1")),
      arguments(
        "date_format(C, 'MM/dd/yyyy') >= '03/09/2022'",
        Seq(
          IndexRow("file_1", valueCount = 1,
            C_minValue = new Timestamp(1646711448000L), // 03/08/2022
            C_maxValue = new Timestamp(1646797848000L), // 03/09/2022
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 1,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646711448000L), // 03/08/2022
            C_nullCount = 0)
        ),
        Seq("file_1")),
      arguments(
        "date_format(C, 'MM/dd/yyyy') IN ('03/09/2022')",
        Seq(
          IndexRow("file_1", valueCount = 1,
            C_minValue = new Timestamp(1646711448000L), // 03/08/2022
            C_maxValue = new Timestamp(1646797848000L), // 03/09/2022
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 1,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646711448000L), // 03/08/2022
            C_nullCount = 0)
        ),
        Seq("file_1")),
      arguments(
        "date_format(C, 'MM/dd/yyyy') NOT IN ('03/07/2022')",
        Seq(
          IndexRow("file_1", valueCount = 1,
            C_minValue = new Timestamp(1646711448000L), // 03/08/2022
            C_maxValue = new Timestamp(1646797848000L), // 03/09/2022
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 1,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646625048000L), // 03/07/2022
            C_nullCount = 0)
        ),
        Seq("file_1")),
      arguments(
        // Should be identical to the one above
        "date_format(to_timestamp(B, 'yyyy-MM-dd'), 'MM/dd/yyyy') NOT IN ('03/06/2022')",
        Seq(
          IndexRow("file_1", valueCount = 1,
            B_minValue = "2022-03-07", // 03/07/2022
            B_maxValue = "2022-03-08", // 03/08/2022
            B_nullCount = 0),
          IndexRow("file_2", valueCount = 1,
            B_minValue = "2022-03-06", // 03/06/2022
            B_maxValue = "2022-03-06", // 03/06/2022
            B_nullCount = 0)
        ),
        Seq("file_1"))

    )
  }
}
