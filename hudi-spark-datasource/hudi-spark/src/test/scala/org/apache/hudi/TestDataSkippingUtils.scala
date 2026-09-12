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
import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.testutils.{HoodieDummyExpressionHolder, HoodieSparkClientTestBase}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.HoodieCatalystExpressionUtils.resolveExpr
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.DummyExpressionHolder
import org.apache.spark.sql.catalyst.expressions.{Expression, InSet, Not, ParseToDate, ParseToTimestamp}
import org.apache.spark.sql.catalyst.optimizer.OptimizeIn
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions.{col, lower}
import org.apache.spark.sql.hudi.DataSkippingUtils
import org.apache.spark.sql.internal.SQLConf.{ANSI_ENABLED, SESSION_LOCAL_TIMEZONE}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.junit.jupiter.params.provider.Arguments.arguments

import java.sql.Timestamp

import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet

// NOTE: Only A, B columns are indexed
case class IndexRow(fileName: String,
                    valueCount: Long = 1,

                    // Corresponding A column is LongType
                    A_minValue: Long = -1,
                    A_maxValue: Long = -1,
                    A_nullCount: java.lang.Long = null,

                    // Corresponding B column is StringType
                    B_minValue: String = null,
                    B_maxValue: String = null,
                    B_nullCount: java.lang.Long = null,

                    // Corresponding B column is TimestampType
                    C_minValue: Timestamp = null,
                    C_maxValue: Timestamp = null,
                    C_nullCount: java.lang.Long = null) {
  def toRow: Row = Row(productIterator.toSeq: _*)
}

class TestDataSkippingUtils extends HoodieSparkClientTestBase with SparkAdapterSupport {

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

  val (indexSchema: StructType, targetIndexedColumns:  Seq[String]) = composeIndexSchema(indexedCols, indexedCols, sourceTableSchema)

  @ParameterizedTest
  @MethodSource(Array(
    "testBasicLookupFilterExpressionsSource",
    "testAdvancedLookupFilterExpressionsSource",
    "testCompositeFilterExpressionsSource",
    "testSupportedAndUnsupportedDataSkippingColumnsSource"
  ))
  def testLookupFilterExpressions(sourceFilterExprStr: String, input: Seq[IndexRow], expectedOutput: Seq[String]): Unit = {
    // We have to fix the timezone to make sure all date-bound utilities output
    // is consistent with the fixtures
    spark.sqlContext.setConf(SESSION_LOCAL_TIMEZONE.key, "UTC")

    val resolvedFilterExpr: Expression = resolveExpr(spark, sourceFilterExprStr, sourceTableSchema)
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

    val resolvedFilterExpr: Expression = resolveExpr(spark, filterExpr, sourceTableSchema)
    val rows: Seq[String] = applyFilterExpr(resolvedFilterExpr, input)

    assertEquals(expectedOutput, rows)
  }

  @ParameterizedTest
  @MethodSource(Array("testStringsLookupFilterExpressionsSource"))
  def testStringsLookupFilterExpressions(sourceExpr: Expression, input: Seq[IndexRow], output: Seq[String]): Unit = {
    val resolvedExpr = resolveExpr(spark, sourceExpr, sourceTableSchema)
    val lookupFilter = DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr(resolvedExpr, indexedCols = indexedCols)

    val sparkB = spark
    import sparkB.implicits._

    val indexDf = spark.createDataset(input)

    val rows = indexDf.where(sparkAdapter.createColumnFromExpression(lookupFilter))
      .select("fileName")
      .collect()
      .map(_.getString(0))
      .toSeq

    assertEquals(output, rows)
  }


  @ParameterizedTest
  @MethodSource(Array("testDateParsingFilterExpressionsAfterReplaceExpressionsSource"))
  def testLookupFilterExpressionsAfterReplaceExpressions(sourceFilterExprStr: String,
                                                         input: Seq[IndexRow],
                                                         expectedOutput: Seq[String]): Unit = {
    // We have to fix the timezone to make sure all date-bound utilities output
    // is consistent with the fixtures
    spark.sqlContext.setConf(SESSION_LOCAL_TIMEZONE.key, "UTC")

    // Pin ANSI off so the replaced GetTimestamp carries failOnError = false on every profile
    // (ANSI is the default on Spark 4); the ANSI-on behavior is covered by
    // testDateParsingLookupUnderAnsiMode
    val previousAnsi = spark.sqlContext.getConf(ANSI_ENABLED.key)
    spark.sqlContext.setConf(ANSI_ENABLED.key, "false")
    try {
      val resolvedFilterExpr: Expression = resolveExpr(spark, sourceFilterExprStr, sourceTableSchema)
      val optimizedFilterExpr = fullyOptimize(resolvedFilterExpr)

      // On the real read path the optimizer's FinishAnalysis batch (ReplaceExpressions) rewrites
      // RuntimeReplaceable to_date/to_timestamp before any filter reaches Hudi's file pruning, so
      // the translation must be exercised against the replaced shape
      assertTrue(
        optimizedFilterExpr.collectFirst {
          case p: ParseToDate => p
          case p: ParseToTimestamp => p
        }.isEmpty,
        s"Expected ReplaceExpressions to rewrite RuntimeReplaceable date parsing nodes, got: $optimizedFilterExpr")

      val rows: Seq[String] = applyFilterExpr(optimizedFilterExpr, input)

      assertEquals(expectedOutput, rows)
    } finally {
      spark.sqlContext.setConf(ANSI_ENABLED.key, previousAnsi)
    }
  }

  @Test
  def testDateParsingLookupUnderAnsiMode(): Unit = {
    spark.sqlContext.setConf(SESSION_LOCAL_TIMEZONE.key, "UTC")

    // Under ANSI mode the replaced GetTimestamp carries failOnError = true: probing string
    // min/max stats with a throwing parse could fail queries whose files are pruned away by
    // other predicates, or whose stats hold unparseable values like 'zzz'. The whitelist arm
    // must therefore refuse to match, translation must fall back to keeping every file, and
    // the lookup must not throw
    val input = Seq(
      IndexRow("file_mixed", valueCount = 2,
        B_minValue = "2022-03-07",
        B_maxValue = "zzz",
        B_nullCount = 0)
    )
    val previousAnsi = spark.sqlContext.getConf(ANSI_ENABLED.key)
    spark.sqlContext.setConf(ANSI_ENABLED.key, "true")
    try {
      val resolvedFilterExpr: Expression =
        resolveExpr(spark, "to_timestamp(B, 'yyyy-MM-dd') > '2022-03-06 12:00:00'", sourceTableSchema)
      val optimizedFilterExpr = fullyOptimize(resolvedFilterExpr)
      assertEquals(Seq("file_mixed"), applyFilterExpr(optimizedFilterExpr, input))
    } finally {
      spark.sqlContext.setConf(ANSI_ENABLED.key, previousAnsi)
    }
  }

  private def fullyOptimize(expr: Expression): Expression = {
    val holder = HoodieDummyExpressionHolder(Seq(expr), expr.references.toSeq)
    spark.sessionState.optimizer.execute(holder)
      .asInstanceOf[HoodieDummyExpressionHolder]
      .exprs.head
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
    val lookupFilter = DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr(resolvedExpr, indexedCols = indexedCols)

    val indexDf = spark.createDataFrame(input.map(_.toRow).asJava, indexSchema)

    indexDf.where(sparkAdapter.createColumnFromExpression(lookupFilter))
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
        sparkAdapter.getExpressionFromColumn(col("B").startsWith("abc")),
        Seq(
          IndexRow("file_1", valueCount = 1, B_minValue = "aba", B_maxValue = "adf", B_nullCount = 1), // may contain strings starting w/ "abc"
          IndexRow("file_2", valueCount = 1, B_minValue = "adf", B_maxValue = "azy", B_nullCount = 0),
          IndexRow("file_3", valueCount = 1, B_minValue = "aaa", B_maxValue = "aba", B_nullCount = 0)
        ),
        Seq("file_1")),
      arguments(
        Not(sparkAdapter.getExpressionFromColumn(col("B").startsWith("abc"))),
        Seq(
          IndexRow("file_1", valueCount = 1, B_minValue = "aba", B_maxValue = "adf", B_nullCount = 1), // may contain strings starting w/ "abc"
          IndexRow("file_2", valueCount = 1, B_minValue = "adf", B_maxValue = "azy", B_nullCount = 0),
          IndexRow("file_3", valueCount = 1, B_minValue = "aaa", B_maxValue = "aba", B_nullCount = 0),
          IndexRow("file_4", valueCount = 1, B_minValue = "abc123", B_maxValue = "abc345", B_nullCount = 0) // all strings start w/ "abc"
        ),
        Seq("file_1", "file_2", "file_3")),
      arguments(
        // Composite expression
        Not(sparkAdapter.getExpressionFromColumn(lower(col("B")).startsWith("abc"))),
        Seq(
          IndexRow("file_1", valueCount = 1, B_minValue = "ABA", B_maxValue = "ADF", B_nullCount = 1), // may contain strings starting w/ "ABC" (after upper)
          IndexRow("file_2", valueCount = 1, B_minValue = "ADF", B_maxValue = "AZY", B_nullCount = 0),
          IndexRow("file_3", valueCount = 1, B_minValue = "AAA", B_maxValue = "ABA", B_nullCount = 0),
          IndexRow("file_4", valueCount = 1, B_minValue = "ABC123", B_maxValue = "ABC345", B_nullCount = 0) // all strings start w/ "ABC" (after upper)
        ),
        Seq("file_1", "file_2", "file_3"))
    )
  }

  def testSupportedAndUnsupportedDataSkippingColumnsSource(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      arguments(
        "A = 1 and B is not null",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 0, A_maxValue = 1, A_nullCount = 0, B_minValue = null, B_maxValue = null, B_nullCount = null),
          IndexRow("file_2", valueCount = 2, A_minValue = 1, A_maxValue = 2, A_nullCount = 0, B_minValue = null, B_maxValue = null, B_nullCount = null),
          IndexRow("file_3", valueCount = 2, A_minValue = 2, A_maxValue = 3, A_nullCount = 0, B_minValue = null, B_maxValue = null, B_nullCount = null)
        ),
        Seq("file_1", "file_2")
      ),
      arguments(
        "B = 1 and B is not null",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 0, A_maxValue = 1, A_nullCount = 0, B_minValue = null, B_maxValue = null, B_nullCount = null),
          IndexRow("file_2", valueCount = 2, A_minValue = 1, A_maxValue = 2, A_nullCount = 0, B_minValue = null, B_maxValue = null, B_nullCount = null),
          IndexRow("file_3", valueCount = 2, A_minValue = 2, A_maxValue = 3, A_nullCount = 0, B_minValue = null, B_maxValue = null, B_nullCount = null)
        ),
        Seq("file_1", "file_2", "file_3")
      ),
      arguments(
        "A = 1 and A is not null and B is not null and B > 2",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 0, A_maxValue = 1, A_nullCount = 0, B_minValue = null, B_maxValue = null, B_nullCount = null),
          IndexRow("file_2", valueCount = 2, A_minValue = 1, A_maxValue = 2, A_nullCount = 0, B_minValue = null, B_maxValue = null, B_nullCount = null),
          IndexRow("file_3", valueCount = 2, A_minValue = 2, A_maxValue = 3, A_nullCount = 0, B_minValue = null, B_maxValue = null, B_nullCount = null)
        ),
        Seq("file_1", "file_2")
      ),
      // NOTE: file_1 holds no stats for B (for ex, B was added by schema evolution after it was written),
      //       hence it could hold nulls for B and could NOT be pruned
      arguments(
        "B is null",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 0, A_maxValue = 1, A_nullCount = 0, B_minValue = null, B_maxValue = null, B_nullCount = null),
          IndexRow("file_2", valueCount = 2, A_minValue = 1, A_maxValue = 2, A_nullCount = 0, B_minValue = "a", B_maxValue = "b", B_nullCount = 0),
          IndexRow("file_3", valueCount = 2, A_minValue = 2, A_maxValue = 3, A_nullCount = 0, B_minValue = "a", B_maxValue = "b", B_nullCount = 1)
        ),
        Seq("file_1", "file_3")
      ),
      arguments(
        "B <=> null",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 0, A_maxValue = 1, A_nullCount = 0, B_minValue = null, B_maxValue = null, B_nullCount = null),
          IndexRow("file_2", valueCount = 2, A_minValue = 1, A_maxValue = 2, A_nullCount = 0, B_minValue = "a", B_maxValue = "b", B_nullCount = 0),
          IndexRow("file_3", valueCount = 2, A_minValue = 2, A_maxValue = 3, A_nullCount = 0, B_minValue = "a", B_maxValue = "b", B_nullCount = 1)
        ),
        Seq("file_1", "file_3")
      ),
      arguments(
        "A = 1 and B is null",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 0, A_maxValue = 1, A_nullCount = 0, B_minValue = null, B_maxValue = null, B_nullCount = null),
          IndexRow("file_2", valueCount = 2, A_minValue = 2, A_maxValue = 3, A_nullCount = 0, B_minValue = null, B_maxValue = null, B_nullCount = null),
          IndexRow("file_3", valueCount = 2, A_minValue = 1, A_maxValue = 2, A_nullCount = 0, B_minValue = "a", B_maxValue = "b", B_nullCount = 0)
        ),
        Seq("file_1")
      )
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
        Seq("file_1"))
      // NOTE: The analysis-time to_timestamp(B, fmt) composite that used to live here moved to
      //       testDateParsingFilterExpressionsAfterReplaceExpressionsSource: on the real read
      //       path the optimizer always rewrites ParseToTimestamp before Hudi sees the filter,
      //       so only the replaced shape is worth pinning
    )
  }

  def testDateParsingFilterExpressionsAfterReplaceExpressionsSource(): java.util.stream.Stream[Arguments] = {
    // NOTE: All timestamps in UTC. Column B holds 'yyyy-MM-dd' formatted strings, for which
    //       lexicographic ordering coincides with chronological ordering
    val input = Seq(
      IndexRow("file_1", valueCount = 1,
        B_minValue = "2022-03-07",
        B_maxValue = "2022-03-08",
        B_nullCount = 0),
      IndexRow("file_2", valueCount = 1,
        B_minValue = "2022-03-06",
        B_maxValue = "2022-03-06",
        B_nullCount = 0)
    )
    // US-format strings: the lexicographic min/max ('03/06/2022' / '12/31/2021') do NOT
    // correspond to the chronologically smallest/largest values
    val inputUsFormat = Seq(
      IndexRow("file_us", valueCount = 2,
        B_minValue = "03/06/2022",
        B_maxValue = "12/31/2021",
        B_nullCount = 0)
    )
    // Variable-width year-first strings: '2022-11-5' < '2022-3-6' lexicographically even though
    // it is the chronologically larger value
    val inputVariableWidth = Seq(
      IndexRow("file_slop", valueCount = 2,
        B_minValue = "2022-11-5",
        B_maxValue = "2022-3-6",
        B_nullCount = 0)
    )
    // The lexicographic max 'zzz' does not parse; the file still holds a parseable matching value
    val inputUnparseableMax = Seq(
      IndexRow("file_mixed", valueCount = 2,
        B_minValue = "2022-03-07",
        B_maxValue = "zzz",
        B_nullCount = 0)
    )
    java.util.stream.Stream.of(
      // to_date(B) is replaced with Cast(B as date), which the pre-existing Cast whitelist arm
      // already handled (kept as a regression pin; does not exercise the GetTimestamp arm)
      arguments("to_date(B) > '2022-03-06'", input, Seq("file_1")),
      // to_date(B, fmt) is replaced with Cast(GetTimestamp(B, fmt) as date), but the optimizer
      // then folds the outer cast-to-date into the comparison bound, so the matcher sees a bare
      // GetTimestamp here; see the date_add case below for a shape that retains the cast
      arguments("to_date(B, 'yyyy-MM-dd') > '2022-03-06'", input, Seq("file_1")),
      arguments("to_date(B, 'yyyy-MM-dd') = '2022-03-08'", input, Seq("file_1")),
      // to_timestamp(B) is replaced with Cast(B as timestamp), which the pre-existing Cast
      // whitelist arm already handled (regression pin, same as to_date(B) above)
      arguments("to_timestamp(B) > '2022-03-06 12:00:00'", input, Seq("file_1")),
      // to_timestamp(B, fmt) is replaced with a bare GetTimestamp(B, fmt)
      arguments("to_timestamp(B, 'yyyy-MM-dd') > '2022-03-06 12:00:00'", input, Seq("file_1")),
      // The only optimized shape that retains Cast(GetTimestamp(...) as date): the matcher must
      // recurse through date_add, the cast and the timestamp parse back to B
      arguments("date_add(to_date(B, 'yyyy-MM-dd'), 1) > '2022-03-07'", input, Seq("file_1")),
      // Same composite as in testCompositeFilterExpressionsSource, but under the real optimizer
      // ReplaceExpressions rewrites to_timestamp and OptimizeIn turns the single-element NOT IN
      // into Not(EqualTo(...))
      arguments("date_format(to_timestamp(B, 'yyyy-MM-dd'), 'MM/dd/yyyy') NOT IN ('03/06/2022')",
        input, Seq("file_1")),
      // Non-order-preserving format: the whitelist arm must reject 'MM/dd/yyyy' and translation
      // must fall back to keeping every file (safe, not smart). Pruning on the lexicographic
      // min/max of US-format strings would wrongly drop file_us, which contains 2022-03-06
      arguments("to_date(B, 'MM/dd/yyyy') = '2022-03-06'", inputUsFormat, Seq("file_us")),
      // Variable-width year-first format: equally order-breaking, equally rejected
      arguments("to_date(B, 'yyyy-M-d') = '2022-11-05'", inputVariableWidth, Seq("file_slop")),
      // Unparseable stat value: GetTimestamp('zzz', 'yyyy-MM-dd') is null, and the null-tolerant
      // bounds (Coalesce(bound, true)) must keep the file -- it holds 2022-03-07, which matches
      arguments("to_timestamp(B, 'yyyy-MM-dd') > '2022-03-06 12:00:00'", inputUnparseableMax,
        Seq("file_mixed"))
    )
  }
}
