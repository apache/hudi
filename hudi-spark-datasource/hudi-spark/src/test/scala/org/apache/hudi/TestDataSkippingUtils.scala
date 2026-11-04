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
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieIndexDefinition
import org.apache.hudi.testutils.{HoodieDummyExpressionHolder, HoodieSparkClientTestBase}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.HoodieCatalystExpressionUtils.resolveExpr
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.DummyExpressionHolder
import org.apache.spark.sql.catalyst.expressions.{And, Expression, GetStructField, InSet, Not, ParseToDate, ParseToTimestamp}
import org.apache.spark.sql.catalyst.optimizer.OptimizeIn
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions.{col, date_format, lit, lower, upper}
import org.apache.spark.sql.hudi.DataSkippingUtils
import org.apache.spark.sql.hudi.dataskipping.LocalDateReversalGuard
import org.apache.spark.sql.internal.SQLConf.{ANSI_ENABLED, SESSION_LOCAL_TIMEZONE}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource, ValueSource}
import org.junit.jupiter.params.provider.Arguments.arguments

import java.sql.Timestamp
import java.time.{DayOfWeek, Instant, LocalTime, Month, ZoneId, ZoneOffset}
import java.time.zone.{ZoneOffsetTransition, ZoneOffsetTransitionRule, ZoneRules}
import java.time.zone.ZoneOffsetTransitionRule.TimeDefinition
import java.util.Collections

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
  @MethodSource(Array("missingColumnStatsPredicates"))
  def testCandidateFilesWithMissingColumnStats(predicate: String, expected: Set[String], separateFilters: Boolean): Unit = {
    val input = Seq(
      IndexRow("missing", A_minValue = 25, A_maxValue = 25, A_nullCount = 0),
      IndexRow("all_null", A_minValue = 25, A_maxValue = 25, A_nullCount = 0, B_nullCount = 1),
      IndexRow("matching", A_minValue = 25, A_maxValue = 25, A_nullCount = 0,
        B_minValue = "alice", B_maxValue = "alice", B_nullCount = 0),
      IndexRow("nonmatching", A_minValue = 25, A_maxValue = 25, A_nullCount = 0,
        B_minValue = "aaa", B_maxValue = "aaa", B_nullCount = 0)
    )
    // C is indexed globally but not referenced by this query, so it is absent from the projection.
    val indexDf = spark.createDataFrame(input.map(_.toRow).asJava, indexSchema)
      .drop("C_minValue", "C_maxValue", "C_nullCount")
    val definition = HoodieIndexDefinition.newBuilder().withIndexName("column_stats")
      .withIndexType("column_stats").withSourceFields(indexedCols.asJava).build()
    val resolvedPredicate = resolveExpr(spark, predicate, sourceTableSchema)
    val filters = resolvedPredicate match {
      case And(left, right) if separateFilters => Seq(left, right)
      case other => Seq(other)
    }
    val support = new ColumnStatsIndexSupport(spark, sourceTableSchema, null,
      HoodieMetadataConfig.newBuilder().build(), null) {
      def candidates: Set[String] = getCandidateFiles(indexDf, filters,
        input.map(_.fileName).toSet + "unindexed", _ => indexedCols,
        indexDefinitionOpt = Some(definition))
    }
    assertEquals(expected + "unindexed", support.candidates)
  }

  @ParameterizedTest
  @MethodSource(Array("dottedColumnStatsPredicates"))
  def testCandidateFilesWithDottedColumnStats(predicate: String, expected: Set[String],
                                            separateFilters: Boolean, structNested: Boolean): Unit = {
    val nestedField = if (structNested) {
      StructField("profile", StructType(Seq(StructField("name", StringType))))
    } else {
      StructField("profile.name", StringType)
    }
    val nestedSchema = StructType(Seq(StructField("A", LongType), nestedField, StructField("C", TimestampType)))
    // Literal dotted source attributes have a quoted canonical index name, including the backticks.
    val indexedName = if (structNested) "profile.name" else "`profile.name`"
    val nestedIndexedCols = Seq("A", indexedName, "C")
    val nestedIndexSchema = StructType(indexSchema.fields.map { field =>
      if (field.name.startsWith("B_")) field.copy(name = indexedName + field.name.stripPrefix("B")) else field
    })
    val input = Seq(
      IndexRow("missing", A_minValue = 25, A_maxValue = 25, A_nullCount = 0),
      IndexRow("missing_pruned_by_A", A_minValue = -1, A_maxValue = -1, A_nullCount = 0),
      IndexRow("all_null", A_minValue = 25, A_maxValue = 25, A_nullCount = 0, B_nullCount = 1),
      IndexRow("matching", A_minValue = 25, A_maxValue = 25, A_nullCount = 0,
        B_minValue = "alice", B_maxValue = "alice", B_nullCount = 0),
      IndexRow("nonmatching", A_minValue = 25, A_maxValue = 25, A_nullCount = 0,
        B_minValue = "aaa", B_maxValue = "aaa", B_nullCount = 0))
    // The index stores nested paths as literal top-level names containing dots.
    val indexDf = spark.createDataFrame(spark.sparkContext.parallelize(input.map(_.toRow), 1), nestedIndexSchema)
      .drop("C_minValue", "C_maxValue", "C_nullCount")
    assertTrue(indexDf.columns.contains(s"${indexedName}_nullCount"))

    // Analyze the raw source DataFrame to retain the actual Catalyst shape of both field references.
    val sourceDf = spark.createDataFrame(java.util.Collections.emptyList[Row](), nestedSchema)
    def analyzePredicate(sql: String): Expression = sourceDf.where(sql).queryExecution.analyzed
      .asInstanceOf[Filter].condition
    val resolvedPredicate = analyzePredicate(predicate)
    assertEquals(structNested, resolvedPredicate.exists(_.isInstanceOf[GetStructField]))
    if (!structNested) {
      val nestedLookup = DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr(
        analyzePredicate("`profile.name` = 'alice'"), indexedCols = nestedIndexedCols)
      assertEquals(Set(Seq(s"${indexedName}_minValue"), Seq(s"${indexedName}_maxValue")),
        nestedLookup.collect { case attribute: UnresolvedAttribute => attribute.nameParts }.toSet,
        "Dotted predicates must translate to literal statistics names instead of silently disabling pruning")
    }
    val filters = resolvedPredicate match {
      case And(left, right) if separateFilters => Seq(left, right)
      case other => Seq(other)
    }
    val definition = HoodieIndexDefinition.newBuilder().withIndexName("column_stats")
      .withIndexType("column_stats").withSourceFields(nestedIndexedCols.asJava).build()
    val support = new ColumnStatsIndexSupport(spark, nestedSchema, null,
      HoodieMetadataConfig.newBuilder().build(), null) {
      def candidates: Set[String] = getCandidateFiles(indexDf, filters,
        input.map(_.fileName).toSet + "unindexed", _ => nestedIndexedCols,
        indexDefinitionOpt = Some(definition))
    }
    val jobGroup = s"nested-column-stats-$predicate-$separateFilters"
    spark.sparkContext.setJobGroup(jobGroup, "Nested column statistics must prune file candidates")
    try {
      assertEquals(expected + "unindexed", support.candidates)
      assertTrue(spark.sparkContext.statusTracker.getJobIdsForGroup(jobGroup).nonEmpty,
        "Candidate pruning must execute against the index")
    } finally {
      spark.sparkContext.clearJobGroup()
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testUnsupportedPredicatesDoNotScanIndex(separateFilters: Boolean): Unit = {
    val input = Seq(IndexRow("indexed"))
    val indexDf = spark.createDataFrame(spark.sparkContext.parallelize(input.map(_.toRow), 1), indexSchema)
    val definition = HoodieIndexDefinition.newBuilder().withIndexName("column_stats")
      .withIndexType("column_stats").withSourceFields(indexedCols.asJava).build()
    val predicate = resolveExpr(spark, "A % 2 = 1 AND length(B) > 2", sourceTableSchema)
    val filters = predicate match {
      case And(left, right) if separateFilters => Seq(left, right)
      case other => Seq(other)
    }
    val expected = Set("indexed", "unindexed")
    val support = new ColumnStatsIndexSupport(spark, sourceTableSchema, null,
      HoodieMetadataConfig.newBuilder().build(), null) {
      def candidates: Set[String] = getCandidateFiles(indexDf, filters, expected, _ => indexedCols,
        indexDefinitionOpt = Some(definition))
    }
    val jobGroup = s"unsupported-column-stats-predicates-$separateFilters"
    spark.sparkContext.setJobGroup(jobGroup, "Unsupported predicates must not scan the index")
    try {
      assertEquals(expected, support.candidates)
      assertEquals(0, spark.sparkContext.statusTracker.getJobIdsForGroup(jobGroup).length)
    } finally {
      spark.sparkContext.clearJobGroup()
    }
  }

  @ParameterizedTest
  @MethodSource(Array(
    "testBasicLookupFilterExpressionsSource",
    "testAdvancedLookupFilterExpressionsSource",
    "testCompositeFilterExpressionsSource",
    "testSupportedAndUnsupportedDataSkippingColumnsSource",
    "testNonOrderPreservingTransformationsSource",
    "testNegativeFactorSource"
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

  @Test
  def testCollationChangingCastDoesNotPrune(): Unit = {
    assumeTrue(HoodieSparkUtils.gteqSpark4_0, "String collations only exist on Spark 4.x")
    // Stats are in UTF8_BINARY order ("B" < "a"), but the filter compares in UTF8_LCASE order
    // ("a" < "B"): re-applying the cast to the bounds gives "B" <= "a" = false and would wrongly
    // prune file_1, which holds "a". Must fall back to always-true, keeping the file
    testLookupFilterExpressions(
      "CAST(B AS STRING COLLATE UTF8_LCASE) = 'a'",
      Seq(IndexRow("file_1", valueCount = 2, B_minValue = "B", B_maxValue = "a", B_nullCount = 0)),
      Seq("file_1"))
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

  @Test
  def testSessionZoneConversionsAcrossDstFallBack(): Unit = {
    spark.sqlContext.setConf(SESSION_LOCAL_TIMEZONE.key, "America/New_York")
    try {
      // 05:30Z formats as 01:30 EDT but the later 06:10Z formats as 01:10 EST, so the string max
      // is smaller than the min. Re-applying the cast over min/max stats would wrongly prune
      // file_1 even though its 01:30 row matches; must fall back to always-true, keeping the file
      val input = Seq(
        IndexRow("file_1", valueCount = 2,
          // epoch seconds of C's bounds
          A_minValue = TestDataSkippingUtils.NyFallBackEdt0130.getTime / 1000,
          A_maxValue = TestDataSkippingUtils.NyFallBackEst0110.getTime / 1000,
          A_nullCount = 0,
          C_minValue = TestDataSkippingUtils.NyFallBackEdt0130,
          C_maxValue = TestDataSkippingUtils.NyFallBackEst0110,
          C_nullCount = 0))
      val resolvedFilterExpr = resolveExpr(spark, "CAST(C AS STRING) > '2026-11-01 01:20:00'", sourceTableSchema)

      assertEquals(Seq("file_1"), applyFilterExpr(resolvedFilterExpr, input))

      // Converting to timestamp_ntz takes the same local wall-clock time, so it re-orders the same way
      if (HoodieSparkUtils.gteqSpark3_4) {
        val ntzFilterExpr = resolveExpr(spark,
          "CAST(C AS TIMESTAMP_NTZ) > TIMESTAMP_NTZ '2026-11-01 01:20:00'", sourceTableSchema)

        assertEquals(Seq("file_1"), applyFilterExpr(ntzFilterExpr, input))
      }

      // date_format and from_unixtime render the same local time, so a pattern with the time of day
      // re-orders the same way
      assertEquals(Seq("file_1"), applyFilterExpr(
        resolveExpr(spark, "date_format(C, 'yyyy-MM-dd HH:mm') > '2026-11-01 01:20'", sourceTableSchema), input))
      assertEquals(Seq("file_1"), applyFilterExpr(
        resolveExpr(spark, "from_unixtime(A, 'yyyy-MM-dd HH:mm') > '2026-11-01 01:20'", sourceTableSchema), input))

      // A date-only pattern renders the local date, which never runs backwards in New York, so it still
      // translates and prunes
      val earlierDay = IndexRow("file_2", valueCount = 2,
        C_minValue = new Timestamp(1793361600000L), // 2026-10-30 12:00Z
        C_maxValue = new Timestamp(1793365200000L), // 2026-10-30 13:00Z
        C_nullCount = 0)
      assertEquals(Seq("file_1"), applyFilterExpr(
        resolveExpr(spark, "date_format(C, 'yyyy-MM-dd') = '2026-11-01'", sourceTableSchema), input :+ earlierDay))
    } finally {
      spark.sqlContext.setConf(SESSION_LOCAL_TIMEZONE.key, "UTC")
    }
  }

  @Test
  def testLocalDateAcrossMidnightFallBack(): Unit = {
    spark.sqlContext.setConf(SESSION_LOCAL_TIMEZONE.key, "America/St_Johns")
    try {
      // At 02:31Z on 2009-11-01 the clocks went from 00:01 NDT back to 23:01 NST of the previous day, so 02:30:30Z
      // renders as 2009-11-01 but 02:31Z as 2009-10-31. Re-applying a date rendering over min/max stats puts the max
      // below the min and would wrongly prune file_1 for either date; must keep the file
      val acrossFallBack = IndexRow("file_1", valueCount = 2,
        A_minValue = 1257042630L, A_maxValue = 1257042660L, A_nullCount = 0, // epoch seconds of C's bounds
        C_minValue = new Timestamp(1257042630000L), // 2009-11-01 02:30:30Z
        C_maxValue = new Timestamp(1257042660000L), // 2009-11-01 02:31:00Z, the first instant after the fall-back
        C_nullCount = 0)
      // Files that do not span the fall-back are still pruned
      val afterFallBack = IndexRow("file_2", valueCount = 2,
        A_minValue = 1257042660L, A_maxValue = 1257043200L, A_nullCount = 0,
        C_minValue = new Timestamp(1257042660000L), // 2009-11-01 02:31:00Z, renders as 2009-10-31
        C_maxValue = new Timestamp(1257043200000L), // 2009-11-01 02:40:00Z, renders as 2009-10-31
        C_nullCount = 0)
      val earlierDay = IndexRow("file_3", valueCount = 2,
        A_minValue = 1256904000L, A_maxValue = 1256907600L, A_nullCount = 0,
        C_minValue = new Timestamp(1256904000000L), // 2009-10-30 12:00Z
        C_maxValue = new Timestamp(1256907600000L), // 2009-10-30 13:00Z
        C_nullCount = 0)
      Seq(
        "date_format(C, 'yyyy-MM-dd') = '2009-10-31'" -> Seq("file_1", "file_2"),
        "date_format(C, 'yyyy-MM-dd') = '2009-11-01'" -> Seq("file_1"),
        "from_unixtime(A, 'yyyy-MM-dd') = '2009-10-31'" -> Seq("file_1", "file_2"),
        "CAST(C AS DATE) IN (DATE'2009-10-29', DATE'2009-10-31')" -> Seq("file_1", "file_2"),
        // Not optimized here, so to_date is still a RuntimeReplaceable rather than the cast it stands for
        "to_date(C) = DATE'2009-10-31'" -> Seq("file_1", "file_2"),
        "date_add(CAST(C AS DATE), 1) = DATE'2009-11-01'" -> Seq("file_1", "file_2"),
        "date_format(C, 'yyyy-MM-dd') = '2009-11-01' AND A > 0" -> Seq("file_1"),
        // Only the leaves are guarded, so another conjunct still prunes a file spanning the fall-back
        "date_format(C, 'yyyy-MM-dd') = '2009-11-01' AND A < 0" -> Seq()
      ).foreach { case (filter, expected) =>
        assertEquals(expected, applyFilterExpr(resolveExpr(spark, filter, sourceTableSchema),
          Seq(acrossFallBack, afterFallBack, earlierDay)), filter)
      }

      // The same date at both ends does not rule out a row of the previous date in between
      val aroundFallBack = IndexRow("file_4", valueCount = 3,
        C_minValue = new Timestamp(1257042630000L), // 2009-11-01 02:30:30Z, renders as 2009-11-01
        C_maxValue = new Timestamp(1257046800000L), // 2009-11-01 03:40:00Z, renders as 2009-11-01
        C_nullCount = 0)
      assertEquals(Seq("file_4"), applyFilterExpr(
        resolveExpr(spark, "NOT (date_format(C, 'yyyy-MM-dd') = '2009-11-01')", sourceTableSchema), Seq(aroundFallBack)))
    } finally {
      spark.sqlContext.setConf(SESSION_LOCAL_TIMEZONE.key, "UTC")
    }
  }

  @Test
  def testLocalDateGuardIgnoresBoundsFailingUnderAnsi(): Unit = {
    val ansi = spark.sqlContext.getConf(ANSI_ENABLED.key)
    spark.sqlContext.setConf(SESSION_LOCAL_TIMEZONE.key, "America/Phoenix")
    spark.sqlContext.setConf(ANSI_ENABLED.key, "true")
    try {
      // America/Phoenix ran its date backwards in 1944, so the guard renders both bounds, while ">=" only renders the
      // max. Under ANSI the min '' cannot be cast to a timestamp, which must not fail the lookup: file_1 is still
      // pruned on its max
      val input = Seq(IndexRow("file_1", valueCount = 2,
        B_minValue = "", B_maxValue = "2023-12-31 10:00:00", B_nullCount = 0))
      assertEquals(Seq(), applyFilterExpr(
        resolveExpr(spark, "date_format(CAST(B AS TIMESTAMP), 'yyyy-MM-dd') >= '2024-06-01'", sourceTableSchema), input))
    } finally {
      spark.sqlContext.setConf(ANSI_ENABLED.key, ansi)
      spark.sqlContext.setConf(SESSION_LOCAL_TIMEZONE.key, "UTC")
    }
  }

  @Test
  def testLocalDateReversals(): Unit = {
    def reversals(zoneId: String): Option[Seq[Instant]] = LocalDateReversalGuard.localDateReversals(ZoneId.of(zoneId).getRules)

    // America/St_Johns fell back across midnight every year from 1987 to 2010
    val stJohns = reversals("America/St_Johns").get
    assertTrue(stJohns.contains(Instant.parse("2009-11-01T02:31:00Z")))
    assertEquals(Instant.parse("2010-11-07T02:31:00Z"), stJohns.last)
    // Falling back at midnight itself (Asia/Beirut, or Pacific/Apia repeating a whole day in 1892) repeats the previous
    // date without running backwards, and a fall-back within the day (America/New_York) does not change the date
    Seq("Asia/Beirut", "Pacific/Apia", "America/New_York", "UTC").foreach { zoneId =>
      assertEquals(Some(Seq.empty), reversals(zoneId), zoneId)
    }

    // A current rule falling back from 00:30 to 23:30 runs the date backwards every year, without bound
    val (standard, summer) = (ZoneOffset.ofHours(-4), ZoneOffset.ofHours(-3))
    val rules = ZoneRules.of(standard, standard,
      Collections.emptyList[ZoneOffsetTransition](), Collections.emptyList[ZoneOffsetTransition](), Seq(
        ZoneOffsetTransitionRule.of(Month.MARCH, 8, DayOfWeek.SUNDAY, LocalTime.of(2, 0), false, TimeDefinition.WALL,
          standard, standard, summer),
        ZoneOffsetTransitionRule.of(Month.NOVEMBER, 1, DayOfWeek.SUNDAY, LocalTime.of(0, 30), false, TimeDefinition.WALL,
          standard, summer, standard)).asJava)
    assertEquals(None, LocalDateReversalGuard.localDateReversals(rules))
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
  @MethodSource(Array("testDateTimeFilterExpressionsSource", "testIntegralOverflowSource"))
  def testLookupThroughFullOptimizer(sourceFilterExprStr: String,
                                     input: Seq[IndexRow],
                                     expectedOutput: Seq[String]): Unit = {
    // We have to fix the timezone to make sure all date-bound utilities output
    // is consistent with the fixtures
    spark.sqlContext.setConf(SESSION_LOCAL_TIMEZONE.key, "UTC")

    // ANSI flips the parse expressions to throw on unparseable input, and integral and decimal
    // arithmetic to throw on overflow, except `shiftleft`, which wraps around in either mode (ANSI is
    // the default on Spark 4): the outcome must be the same either way, and the lookup must not throw
    Seq("false", "true").foreach { ansi =>
      withSqlConf(ANSI_ENABLED.key -> ansi) {
        assertEquals(expectedOutput, lookupThroughFullOptimizer(sourceFilterExprStr, input), s"ANSI $ansi")
      }
    }
  }

  @Test
  def testDateTimeLookupInDstZone(): Unit = {
    // America/Los_Angeles springs forward at 2022-03-13 02:00
    withSqlConf(SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      // '02:30' does not exist and parses as 03:30, past the max '03:05', so pruning on the parsed
      // bounds would drop file_gap, which holds '03:00:00'. Must keep the file
      val gap = Seq(IndexRow("file_gap", valueCount = 3,
        B_minValue = "2022-03-13 02:30:00", B_maxValue = "2022-03-13 03:05:00", B_nullCount = 0))
      assertEquals(Seq("file_gap"), lookupThroughFullOptimizer(
        "unix_timestamp(B, 'yyyy-MM-dd HH:mm:ss') = 1647165600", gap)) // 2022-03-13 03:00:00 PDT
      assertEquals(Seq("file_gap"), lookupThroughFullOptimizer(
        "to_unix_timestamp(B, 'yyyy-MM-dd HH:mm:ss') = 1647165600", gap))
      assertEquals(Seq("file_gap"), lookupThroughFullOptimizer(
        "to_timestamp(B, 'yyyy-MM-dd HH:mm:ss') = '2022-03-13 03:00:00'", gap))

      // A date-only parse does not depend on the time of day, so it still prunes in this zone
      val dates = Seq(
        IndexRow("file_1", valueCount = 2, B_minValue = "2022-03-13", B_maxValue = "2022-03-14", B_nullCount = 0),
        IndexRow("file_2", valueCount = 2, B_minValue = "2022-03-10", B_maxValue = "2022-03-11", B_nullCount = 0))
      assertEquals(Seq("file_1"), lookupThroughFullOptimizer("to_date(B, 'yyyy-MM-dd') >= '2022-03-12'", dates))
      assertEquals(Seq("file_1"), lookupThroughFullOptimizer(
        "date_format(to_timestamp(B, 'yyyy-MM-dd'), 'yyyy-MM-dd') >= '2022-03-12'", dates))
    }
  }

  /**
   * Runs `filter` through the full session optimizer before translating it, as on the read path: the
   * optimizer's ReplaceExpressions rewrites RuntimeReplaceable nodes (for ex, to_date and
   * to_timestamp into GetTimestamp) before any filter reaches Hudi's file pruning.
   */
  private def lookupThroughFullOptimizer(filter: String, input: Seq[IndexRow]): Seq[String] = {
    val optimizedFilterExpr = fullyOptimize(resolveExpr(spark, filter, sourceTableSchema))
    assertTrue(
      optimizedFilterExpr.collectFirst {
        case p: ParseToDate => p
        case p: ParseToTimestamp => p
      }.isEmpty,
      s"Expected ReplaceExpressions to rewrite RuntimeReplaceable date parsing nodes, got: $optimizedFilterExpr")
    applyFilterExpr(optimizedFilterExpr, input)
  }

  private def withSqlConf[T](conf: (String, String))(body: => T): T = {
    val (key, value) = conf
    val previous = spark.conf.getOption(key)
    spark.conf.set(key, value)
    try {
      body
    } finally {
      previous match {
        case Some(v) => spark.conf.set(key, v)
        case None => spark.conf.unset(key)
      }
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
  // New York local time runs backwards across the DST fall-back: 05:30Z is 01:30 EDT, but the later
  // 06:10Z is 01:10 EST
  private val NyFallBackEdt0130 = new Timestamp(1793511000000L) // 2026-11-01 05:30Z
  private val NyFallBackEst0110 = new Timestamp(1793513400000L) // 2026-11-01 06:10Z

  def dottedColumnStatsPredicates(): java.util.stream.Stream[Arguments] = {
    val cases = Seq(
      ("A >= 0 AND `profile.name` = 'alice'", Set("missing", "matching"), false),
      ("A >= 0 AND NOT (`profile.name` = 'aaa')", Set("missing", "matching"), false),
      ("A > 100 AND `profile.name` = 'alice'", Set.empty[String], false),
      // GetStructField is currently unsupported, but its independent complete conjunct can still prune.
      ("A >= 0 AND profile.name = 'alice'", Set("missing", "all_null", "matching", "nonmatching"), true),
      ("A > 100 AND profile.name = 'alice'", Set.empty[String], true))
    cases.flatMap { case (predicate, expected, structNested) =>
      Seq(false, true).map(separate => arguments(predicate, expected, Boolean.box(separate), Boolean.box(structNested)))
    }.asJava.stream()
  }

  def missingColumnStatsPredicates(): java.util.stream.Stream[Arguments] = {
    val matching = Set("missing", "matching")
    val cases = Seq(
      "A >= 0 AND B = 'alice'" -> matching,
      "A >= 0 AND B > 'aaa'" -> matching,
      "A >= 0 AND B IN ('alice', 'ann')" -> matching,
      "A >= 0 AND NOT (B = 'aaa')" -> matching,
      "A >= 0 AND (B = 'alice' OR B = 'ann')" -> matching,
      "A > 100 AND B = 'alice'" -> Set.empty[String],
      "A > 100 AND NOT (B = 'alice')" -> Set.empty[String],
      "A > 100 AND length(B) > 2" -> Set.empty[String],
      "length(B) > 2 AND A > 100" -> Set.empty[String],
      "A > 100 OR B = 'alice'" -> matching,
      "NOT (A > 100 OR B = 'aaa')" -> matching,
      "NOT (A > 100 AND B = 'alice')" -> Set("missing", "matching", "nonmatching", "all_null")
    )
    cases.flatMap { case (predicate, expected) =>
      Seq(false, true).map(separate => arguments(predicate, expected, Boolean.box(separate)))
    }.asJava.stream()
  }

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
        // NOTE: file_1 may hold "B" (sorting between "ABC" and "abc"), whose lower-case form "b" doesn't
        //       start w/ "abc", even though lower(B_minValue) = lower(B_maxValue) = "abc" does
        Not(sparkAdapter.getExpressionFromColumn(lower(col("B")).startsWith("abc"))),
        Seq(
          IndexRow("file_1", valueCount = 3, B_minValue = "ABC", B_maxValue = "abc", B_nullCount = 0)
        ),
        Seq("file_1")),
      arguments(
        // NOTE: file_1 holds "Zebra", whose lower-case form is the queried "zebra", even though
        //       lower(B_maxValue) = "apple" sorts before it
        sparkAdapter.getExpressionFromColumn(lower(col("B")) === "zebra"),
        Seq(
          IndexRow("file_1", valueCount = 2, B_minValue = "Zebra", B_maxValue = "apple", B_nullCount = 0)
        ),
        Seq("file_1")),
      arguments(
        // NOTE: file_1 holds "apple", whose upper-case form is the queried "APPLE", even though
        //       upper(B_minValue) = "ZEBRA" sorts after it
        sparkAdapter.getExpressionFromColumn(upper(col("B")) === "APPLE"),
        Seq(
          IndexRow("file_1", valueCount = 2, B_minValue = "Zebra", B_maxValue = "apple", B_nullCount = 0)
        ),
        Seq("file_1"))
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
    java.util.stream.Stream.of(
      arguments(
        InSet(UnresolvedAttribute("A"), HashSet(0, 1)),
        Seq(
          IndexRow("file_1", valueCount = 1, 1, 2, 0),
          IndexRow("file_2", valueCount = 1, -1, 1, 0),
          IndexRow("file_3", valueCount = 1, -2, -1, 0)
        ),
        Seq("file_1", "file_2")),
      arguments(
        // Composite expression
        // NOTE: date_format(C, 'yyyy-MM-dd') preserves the ordering of C, so this filter is translated and
        //       file_1, whose dates all fall in 03/2022, is pruned
        Not(sparkAdapter.getExpressionFromColumn(date_format(col("C"), "yyyy-MM-dd").startsWith("2022-03"))),
        Seq(
          IndexRow("file_1", valueCount = 2,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646797848000L), // 03/09/2022
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 2,
            C_minValue = new Timestamp(1646711448000L), // 03/08/2022
            C_maxValue = new Timestamp(1648785048000L), // 04/01/2022
            C_nullCount = 0)
        ),
        Seq("file_2")),
      arguments(
        // An infinite factor turns the zero min into NaN (0 * Infinity), which sorts above every
        // number, so the translated bound A_minValue * Infinity <= Infinity is false and would
        // wrongly prune file_1, although A = 1 gives Infinity <= Infinity. Built with lit() since
        // SQL has no infinite double literal. Must fall back to always-true, keeping the file
        sparkAdapter.getExpressionFromColumn(
          col("A").cast(DoubleType) * lit(Double.PositiveInfinity) <= lit(Double.PositiveInfinity)),
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 0, A_maxValue = 1, A_nullCount = 0)
        ),
        Seq("file_1"))
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

  def testNegativeFactorSource(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      // Multiplying by a negative constant reverses ordering: the translated bound
      // A_minValue * -1.0 < 5.0 is 10 < 5 and would wrongly prune file_1, although A = 0 gives
      // 0 < 5. Must fall back to always-true, keeping the file
      arguments(
        "CAST(A AS DOUBLE) * -1.0D < 5.0D",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = -10, A_maxValue = 0, A_nullCount = 0)
        ),
        Seq("file_1")),
      // Dividing by a negative constant reverses ordering the same way
      arguments(
        "CAST(A AS DOUBLE) / -2.0D < 5.0D",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = -20, A_maxValue = 0, A_nullCount = 0)
        ),
        Seq("file_1")),
      // Adding a term that references the column can carry a negative factor past the Multiply
      // check: A + A * -2 = -A, so the translated bound A_minValue + A_minValue * -2 < -5 is
      // 0 < -5 and would wrongly prune file_1, although A = 10 gives -10 < -5
      arguments(
        "A + A * -2 < -5",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 0, A_maxValue = 10, A_nullCount = 0)
        ),
        Seq("file_1")),
      // Multiplying and dividing by a positive constant still translate and prune
      arguments(
        "CAST(A AS DOUBLE) * 2.0D < 5.0D",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 1, A_maxValue = 2, A_nullCount = 0),
          IndexRow("file_2", valueCount = 2, A_minValue = 3, A_maxValue = 4, A_nullCount = 0)
        ),
        Seq("file_1")),
      arguments(
        "CAST(A AS DOUBLE) / 2.0D > 50.0D",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 1, A_maxValue = 100, A_nullCount = 0),
          IndexRow("file_2", valueCount = 2, A_minValue = 101, A_maxValue = 200, A_nullCount = 0)
        ),
        Seq("file_2"))
    )
  }

  def testCompositeFilterExpressionsSource(): java.util.stream.Stream[Arguments] = {
    // NOTE: all timestamps in UTC
    java.util.stream.Stream.of(
      arguments(
        "date_format(C, 'yyyy-MM-dd') = '2022-03-07'",
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
        "'2022-03-07' = date_format(C, 'yyyy-MM-dd')",
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
        "'2022-03-07' != date_format(C, 'yyyy-MM-dd')",
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
        "date_format(C, 'yyyy-MM-dd') != '2022-03-07'",
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
        "date_format(C, 'yyyy-MM-dd') < '2022-03-08'",
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
        "'2022-03-08' > date_format(C, 'yyyy-MM-dd')",
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
        "'2022-03-08' < date_format(C, 'yyyy-MM-dd')",
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
        "date_format(C, 'yyyy-MM-dd') > '2022-03-08'",
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
        "date_format(C, 'yyyy-MM-dd') <= '2022-03-07'",
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
        "'2022-03-07' >= date_format(C, 'yyyy-MM-dd')",
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
        "'2022-03-09' <= date_format(C, 'yyyy-MM-dd')",
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
        "date_format(C, 'yyyy-MM-dd') >= '2022-03-09'",
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
        "date_format(C, 'yyyy-MM-dd') IN ('2022-03-09')",
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
        "date_format(C, 'yyyy-MM-dd') NOT IN ('2022-03-07')",
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
        // datediff(end, start) preserves the ordering of its end date, hence is translated
        "datediff(CAST(C AS DATE), DATE '2022-03-07') >= 2",
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
        // NOTE: datediff(end, start) reverses the ordering of its start date, so it isn't translated:
        //       file_1 holds 03/08/2022 (1 day before the end date) though its min is 2 days before it
        "datediff(DATE '2022-03-09', CAST(C AS DATE)) <= 1",
        Seq(
          IndexRow("file_1", valueCount = 1,
            C_minValue = new Timestamp(1646625048000L), // 03/07/2022
            C_maxValue = new Timestamp(1646711448000L), // 03/08/2022
            C_nullCount = 0)
        ),
        Seq("file_1"))
      // NOTE: Parsing (to_date / to_timestamp with a format) is pinned in
      //       testDateTimeFilterExpressionsSource instead: on the real read path the optimizer
      //       always rewrites ParseToDate / ParseToTimestamp before Hudi sees the filter
    )
  }

  def testDateTimeFilterExpressionsSource(): java.util.stream.Stream[Arguments] = {
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
    // Column B holding 'yyyy-MM-dd HH:mm:ss' strings, the default format of unix_timestamp
    val inputDateTime = Seq(
      IndexRow("file_1", valueCount = 2,
        B_minValue = "2022-03-07 00:00:00",
        B_maxValue = "2022-03-08 23:59:59",
        B_nullCount = 0),
      IndexRow("file_2", valueCount = 2,
        B_minValue = "2022-03-06 00:00:00",
        B_maxValue = "2022-03-06 23:59:59",
        B_nullCount = 0)
    )
    val inputTimestamps = Seq(
      IndexRow("file_1", valueCount = 2,
        C_minValue = new Timestamp(1646711448000L), // 2022-03-08 03:50:48
        C_maxValue = new Timestamp(1646797848000L), // 2022-03-09 03:50:48
        C_nullCount = 0),
      IndexRow("file_2", valueCount = 2,
        C_minValue = new Timestamp(1646625048000L), // 2022-03-07 03:50:48
        C_maxValue = new Timestamp(1646711448000L), // 2022-03-08 03:50:48
        C_nullCount = 0)
    )
    // Column A holding seconds since the epoch. Long.MaxValue seconds overflow microseconds and
    // format as '1969-12-31', below the min: file_overflow holds 2022-03-07 but would be pruned
    val inputSeconds = Seq(
      IndexRow("file_1", valueCount = 2, A_minValue = 1646611200L, A_maxValue = 1646697599L, A_nullCount = 0),
      IndexRow("file_2", valueCount = 2, A_minValue = 1646697600L, A_maxValue = 1646783999L, A_nullCount = 0),
      IndexRow("file_overflow", valueCount = 3, A_minValue = 0L, A_maxValue = Long.MaxValue, A_nullCount = 0)
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
    // The max '2022-3-6' starts with a digit but does not parse with 'yyyy-MM-dd' (it only parses
    // leniently, so the Spark 3 EXCEPTION parser policy throws on it, as ANSI does); the file still
    // holds, for ex, '2022-03-07'
    val inputSloppyMax = Seq(
      IndexRow("file_sloppy", valueCount = 3,
        B_minValue = "2022-03-01",
        B_maxValue = "2022-3-6",
        B_nullCount = 0)
    )
    // The parser matches the quoted 'T' in any case, so '2022-03-05t09:00:00' parses (09:00) but
    // sorts after '2022-03-05T23:00:00' (23:00): the bounds are 23:00 and 09:00
    val inputIsoCase = Seq(
      IndexRow("file_iso", valueCount = 2,
        B_minValue = "2022-03-05T23:00:00",
        B_maxValue = "2022-03-05t09:00:00",
        B_nullCount = 0)
    )
    java.util.stream.Stream.of(
      // to_date(B) is replaced with Cast(B as date), which the Cast rule refuses (string to date is not
      // order-preserving, #20513), so every file is kept (regression pin; does not exercise GetTimestamp)
      arguments("to_date(B) > '2022-03-06'", input, Seq("file_1", "file_2")),
      // to_date(B, fmt) is replaced with Cast(GetTimestamp(B, fmt) as date), but the optimizer
      // then folds the outer cast-to-date into the comparison bound, so the rule sees a bare
      // GetTimestamp here; see the date_add case below for a shape that retains the cast
      arguments("to_date(B, 'yyyy-MM-dd') > '2022-03-06'", input, Seq("file_1")),
      arguments("to_date(B, 'yyyy-MM-dd') = '2022-03-08'", input, Seq("file_1")),
      // to_timestamp(B) is replaced with Cast(B as timestamp), refused by the Cast rule as well
      // (regression pin, same as to_date(B) above)
      arguments("to_timestamp(B) > '2022-03-06 12:00:00'", input, Seq("file_1", "file_2")),
      // to_timestamp(B, fmt) is replaced with a bare GetTimestamp(B, fmt)
      arguments("to_timestamp(B, 'yyyy-MM-dd') > '2022-03-06 12:00:00'", input, Seq("file_1")),
      // The only optimized shape that retains Cast(GetTimestamp(...) as date): the range walks
      // through date_add, the cast and the timestamp parse back to B
      arguments("date_add(to_date(B, 'yyyy-MM-dd'), 1) > '2022-03-07'", input, Seq("file_1")),
      // unix_timestamp and to_unix_timestamp parse the same way, so they prune the same way
      arguments("unix_timestamp(B, 'yyyy-MM-dd') > 1646568000", input, Seq("file_1")), // 2022-03-06 12:00
      arguments("to_unix_timestamp(B, 'yyyy-MM-dd') > 1646568000", input, Seq("file_1")),
      arguments("unix_timestamp(B) > 1646611200", inputDateTime, Seq("file_1")), // 2022-03-07 00:00
      arguments("to_unix_timestamp(B) > 1646611200", inputDateTime, Seq("file_1")),
      // Formatting a parsed string: under the real optimizer ReplaceExpressions rewrites
      // to_timestamp, and OptimizeIn turns the single-element NOT IN into Not(EqualTo(...))
      arguments("date_format(to_timestamp(B, 'yyyy-MM-dd'), 'yyyy/MM/dd') NOT IN ('2022/03/06')",
        input, Seq("file_1")),
      arguments("date_format(C, 'yyyy-MM-dd HH:mm:ss') > '2022-03-08 12:00:00'", inputTimestamps, Seq("file_1")),
      // Seconds outside years 0001 to 9999 do not format to sortable strings: file_overflow is kept
      arguments("from_unixtime(A, 'yyyy-MM-dd') = '2022-03-07'", inputSeconds, Seq("file_1", "file_overflow")),
      // Non-order-preserving formats: translation must fall back to keeping every file (safe, not
      // smart). Pruning on the lexicographic min/max of US-format strings would wrongly drop
      // file_us, which contains 2022-03-06
      arguments("to_date(B, 'MM/dd/yyyy') = '2022-03-06'", inputUsFormat, Seq("file_us")),
      arguments("unix_timestamp(B, 'MM/dd/yyyy') = 1646524800", inputUsFormat, Seq("file_us")), // 2022-03-06
      // Variable-width year-first format: equally order-breaking, equally rejected
      arguments("to_date(B, 'yyyy-M-d') = '2022-11-05'", inputVariableWidth, Seq("file_slop")),
      arguments("date_format(C, 'MM/dd/yyyy') = '03/07/2022'", inputTimestamps, Seq("file_1", "file_2")),
      // A quoted letter is format-only: parsing with it keeps file_iso, whose 23:00 row matches...
      arguments("to_timestamp(B, \"yyyy-MM-dd'T'HH:mm:ss\") > '2022-03-05 12:00:00'", inputIsoCase, Seq("file_iso")),
      // ... while formatting with it still prunes, since date_format always writes 'T'
      arguments("date_format(C, \"yyyy-MM-dd'T'HH:mm:ss\") > '2022-03-08T12:00:00'", inputTimestamps, Seq("file_1")),
      // Unparseable stat values: their bound is NULL, which must keep the file (and not throw), as it
      // holds 2022-03-07, which matches
      arguments("to_timestamp(B, 'yyyy-MM-dd') > '2022-03-06 12:00:00'", inputUnparseableMax,
        Seq("file_mixed")),
      arguments("to_date(B, 'yyyy-MM-dd') > '2022-03-06'", inputSloppyMax, Seq("file_sloppy")),
      arguments("unix_timestamp(B, 'yyyy-MM-dd') > 1646568000", inputSloppyMax, Seq("file_sloppy"))
    )
  }

  def testNonOrderPreservingTransformationsSource(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      // Issue #19445 repro: cast(4294967297L as int) wraps around to 1 in non-ANSI mode, so
      // re-applying the cast over min/max stats would wrongly prune both files; a narrowing
      // cast must instead fall back to always-true, keeping every file
      arguments(
        "CAST(A AS INT) > 100",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 1, A_maxValue = 100, A_nullCount = 0),
          IndexRow("file_2", valueCount = 3, A_minValue = 1, A_maxValue = 4294967297L, A_nullCount = 0)
        ),
        Seq("file_1", "file_2")),
      // Casting to boolean is A != 0, so min -5 and max 5 both cast to true; re-applying the cast
      // over min/max stats would wrongly prune file_1 even though its 0 row matches. Must fall
      // back to always-true, keeping the file
      arguments(
        "CAST(A AS BOOLEAN) = false",
        Seq(
          IndexRow("file_1", valueCount = 3, A_minValue = -5, A_maxValue = 5, A_nullCount = 0)
        ),
        Seq("file_1")),
      // A widening (up) cast still translates and prunes
      arguments(
        "CAST(A AS DOUBLE) > 100.0D",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 1, A_maxValue = 100, A_nullCount = 0),
          IndexRow("file_2", valueCount = 2, A_minValue = 101, A_maxValue = 200, A_nullCount = 0)
        ),
        Seq("file_2")),
      // A decimal column compared with a double is cast to double, which still translates and prunes
      arguments(
        "CAST(A AS DECIMAL(20, 0)) > 100.5D",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 1, A_maxValue = 100, A_nullCount = 0),
          IndexRow("file_2", valueCount = 2, A_minValue = 101, A_maxValue = 200, A_nullCount = 0)
        ),
        Seq("file_2")),
      // A timestamp casts to smallint through its epoch seconds, so 65537s wraps around to 1 in
      // non-ANSI mode; re-applying the cast over min/max stats would wrongly prune file_1 even
      // though a row at 200s matches. Must fall back to always-true, keeping the file
      arguments(
        "CAST(C AS SMALLINT) > 100",
        Seq(
          IndexRow("file_1", valueCount = 3, C_minValue = new Timestamp(1000L), C_maxValue = new Timestamp(65537000L), C_nullCount = 0)
        ),
        Seq("file_1")),
      // Casting a timestamp to bigint still translates and prunes
      arguments(
        "CAST(C AS BIGINT) > 100",
        Seq(
          IndexRow("file_1", valueCount = 2, C_minValue = new Timestamp(1000L), C_maxValue = new Timestamp(50000L), C_nullCount = 0),
          IndexRow("file_2", valueCount = 2, C_minValue = new Timestamp(101000L), C_maxValue = new Timestamp(200000L), C_nullCount = 0)
        ),
        Seq("file_2")),
      // New York local time runs backwards across the DST fall-back: 05:30Z is 01:30 EDT, but the later
      // 06:10Z is 01:10 EST. Re-applying from_utc_timestamp over min/max stats would wrongly prune
      // file_1 even though its 05:30Z row matches; must fall back to always-true, keeping the file
      arguments(
        "from_utc_timestamp(C, 'America/New_York') > TIMESTAMP '2026-11-01 01:20:00'",
        Seq(
          IndexRow("file_1", valueCount = 2,
            C_minValue = NyFallBackEdt0130,
            C_maxValue = NyFallBackEst0110,
            C_nullCount = 0)
        ),
        Seq("file_1")),
      // to_utc_timestamp reads 02:30Z as New York wall-clock time, which falls in the DST spring-forward
      // gap, so it shifts to 07:30Z, past the 07:00Z of the later 03:00Z. Re-applying it over min/max stats
      // would wrongly prune file_1 even though its 02:30Z row matches; must fall back to always-true,
      // keeping the file
      arguments(
        "to_utc_timestamp(C, 'America/New_York') > TIMESTAMP '2026-03-08 07:15:00'",
        Seq(
          IndexRow("file_1", valueCount = 2,
            C_minValue = new Timestamp(1772937000000L), // 2026-03-08 02:30Z
            C_maxValue = new Timestamp(1772938800000L), // 2026-03-08 03:00Z
            C_nullCount = 0)
        ),
        Seq("file_1")),
      // A fixed-offset zone shifts every timestamp by the same amount, so it still translates and prunes
      arguments(
        "from_utc_timestamp(C, '+08:00') > TIMESTAMP '2026-11-01 13:20:00'",
        Seq(
          IndexRow("file_1", valueCount = 2,
            C_minValue = NyFallBackEdt0130,
            C_maxValue = NyFallBackEst0110,
            C_nullCount = 0),
          IndexRow("file_2", valueCount = 2,
            C_minValue = new Timestamp(1793491200000L), // 2026-11-01 00:00Z
            C_maxValue = new Timestamp(1793494800000L), // 2026-11-01 01:00Z
            C_nullCount = 0)
        ),
        Seq("file_1"))
    )
  }

  def testIntegralOverflowSource(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      // Issue #20460 repros: integral arithmetic wraps around on overflow in non-ANSI mode, so
      // re-applying it over min/max stats maps the max below the min and would wrongly prune
      // file_1, whose first row matches (51 * 2 = 102, 0 + 1 = 1, 1 << 1 = 2). The file may
      // overflow, so its bounds are unknown and it is kept
      arguments(
        "A * 2L > 100",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 51, A_maxValue = Long.MaxValue, A_nullCount = 0)
        ),
        Seq("file_1")),
      arguments(
        "A + 1 > 0",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 0, A_maxValue = Long.MaxValue, A_nullCount = 0)
        ),
        Seq("file_1")),
      arguments(
        "shiftleft(A, 1) > 0",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 1, A_maxValue = Long.MaxValue, A_nullCount = 0)
        ),
        Seq("file_1")),
      // Integral arithmetic still prunes files that cannot overflow. At the limit (file_1) the
      // bounds are exact and prune; one past it (file_2) a row wraps around in non-ANSI mode and
      // matches (for ex, Long.MaxValue + 1 = Long.MinValue < 0), so the file is kept
      arguments(
        "A + 1 < 0",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = Long.MaxValue - 10, A_maxValue = Long.MaxValue - 1, A_nullCount = 0),
          IndexRow("file_2", valueCount = 2, A_minValue = Long.MaxValue - 10, A_maxValue = Long.MaxValue, A_nullCount = 0)
        ),
        Seq("file_2")),
      arguments(
        "A + -1 > 0",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = Long.MinValue + 1, A_maxValue = Long.MinValue + 10, A_nullCount = 0),
          IndexRow("file_2", valueCount = 2, A_minValue = Long.MinValue, A_maxValue = Long.MinValue + 10, A_nullCount = 0)
        ),
        Seq("file_2")),
      // Long.MinValue / 3 does not divide evenly, so the min limit rounds towards zero
      arguments(
        "A * 3L < 0",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 1, A_maxValue = Long.MaxValue / 3, A_nullCount = 0),
          IndexRow("file_2", valueCount = 2, A_minValue = 1, A_maxValue = Long.MaxValue / 3 + 1, A_nullCount = 0)
        ),
        Seq("file_2")),
      arguments(
        "A * 3L > 0",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = Long.MinValue / 3, A_maxValue = -1, A_nullCount = 0),
          IndexRow("file_2", valueCount = 2, A_minValue = Long.MinValue / 3 - 1, A_maxValue = -1, A_nullCount = 0)
        ),
        Seq("file_2")),
      // A bigint shifts by the low 6 bits of the amount, and 33 pins that width: a 5-bit mask would
      // shift by 1. One past the limit (file_2), 2^30 << 33 wraps around to Long.MinValue < 0
      arguments(
        "shiftleft(A, 33) < 0",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 1, A_maxValue = Long.MaxValue >> 33, A_nullCount = 0),
          IndexRow("file_2", valueCount = 2, A_minValue = 1, A_maxValue = (Long.MaxValue >> 33) + 1, A_nullCount = 0)
        ),
        Seq("file_2")),
      // Floating-point addition saturates to infinity instead, so it still translates and prunes
      arguments(
        "CAST(A AS DOUBLE) + 1.0D > 101.0D",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 1, A_maxValue = 100, A_nullCount = 0),
          IndexRow("file_2", valueCount = 2, A_minValue = 101, A_maxValue = 200, A_nullCount = 0)
        ),
        Seq("file_2")),
      // Decimal arithmetic overflows into NULL instead (and throws in ANSI mode, which TryEval turns
      // into NULL), so its steps are partial: an overflowing bound is NULL and keeps the file. The
      // quotient below is decimal(38, 6), with 32 integer digits: the min maps to 1e18 and fits, the
      // max (about 9.2e36) overflows, and dropping the file would lose the matching row 1
      arguments(
        "CAST(A AS DECIMAL(38,0)) / 0.000000000000000001BD > 0",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 1, A_maxValue = Long.MaxValue, A_nullCount = 0)
        ),
        Seq("file_1")),
      // A file whose bounds fit still prunes (file_1, max maps to 1e20 <= 1e21), while file_2 overflows
      arguments(
        "CAST(A AS DECIMAL(38,0)) / 0.000000000000000001BD > 1000000000000000000000BD",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 1, A_maxValue = 100, A_nullCount = 0),
          IndexRow("file_2", valueCount = 2, A_minValue = 1, A_maxValue = Long.MaxValue, A_nullCount = 0)
        ),
        Seq("file_2")),
      // The product is bounded to decimal(38, 0), so the max (about 9.2e38) overflows
      arguments(
        "CAST(A AS DECIMAL(38,0)) * 100000000000000000000BD > 0",
        Seq(
          IndexRow("file_1", valueCount = 2, A_minValue = 1, A_maxValue = Long.MaxValue, A_nullCount = 0)
        ),
        Seq("file_1"))
    )
  }
}
