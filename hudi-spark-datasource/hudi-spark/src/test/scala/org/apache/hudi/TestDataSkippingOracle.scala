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

import org.apache.hudi.HoodieFileIndex.collectReferencedColumns
import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.TestDataSkippingOracle._
import org.apache.hudi.avro.model.{HoodieMetadataColumnStats, HoodieMetadataRecord}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.{HoodieData, HoodieListData}
import org.apache.hudi.common.model.{FileSlice, HoodieBaseFile, HoodieIndexDefinition, HoodieIndexMetadata, HoodieRecord}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.util.{FileFormatUtils, HoodieStorageUtils, Option => HOption, ParquetUtils}
import org.apache.hudi.core.read.BaseHoodieTableFileIndex.PartitionPath
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.{HoodieIndexVersion, HoodieMetadataPayload}
import org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.HoodieCatalystExpressionUtils.resolveExpr
import org.apache.spark.sql.catalyst.expressions.{And, BinaryComparison, Cast, Expression, FromUTCTimestamp, LessThan, Literal, TimeZoneAwareExpression, ToUTCTimestamp}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.hudi.ColumnStatsExpressionUtils.AllowedTransformationExpression
import org.apache.spark.sql.hudi.DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{BeforeEach, DynamicTest, TestFactory}
import org.junit.jupiter.api.Assertions.{assertTrue, fail}
import org.junit.jupiter.api.function.Executable
import org.mockito.Mockito.{mock, when}

import java.math.{BigDecimal => JBigDecimal}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.sql.{Date, Timestamp}
import java.time.Instant

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random
import scala.util.control.NonFatal

object TestDataSkippingOracle {

  // ---------------------------------------------------------------------------------------------
  // Bug families. Every family is one test per index path in `paths`, failing while any observed
  // signature belongs to it, so that a fix turns its own test green. Signatures are
  // `<csi|psi>:<transform id>/<shape>`, `<csi|psi>:compound:<op>/<reason>` or `<csi|psi>:missing_stats/leaf`;
  // in the globs `*` matches anything. A signature belongs to the first family it matches: transforms that
  // break ordering come before LIKE, which owns LIKE over transforms that only break on DST or extreme
  // years. Compositions belong to the family of their inner transform.
  // ---------------------------------------------------------------------------------------------

  case class BugFamily(id: String, paths: Set[String], globs: Seq[String], issue: Int, description: String) {
    def link: String = s"https://github.com/apache/hudi/issues/$issue"
    def displayName(path: IndexPath): String = s"${path.id}: $description (#$issue)"
  }

  private val BOTH = Set("csi", "psi")
  private val CSI_ONLY = Set("csi")

  val BUG_FAMILIES: Seq[BugFamily] = Seq(
    BugFamily("negative_factor", BOTH, Seq("*:multiply_neg/*", "*:divide_neg/*", "*:add_pos(multiply_neg)/*",
      "*:log10(multiply_neg)/*"), 19445, "multiply or divide by a negative constant"),
    BugFamily("narrowing_cast", CSI_ONLY, Seq("*:cast:*>tinyint/*", "*:cast:*>smallint/*", "*:cast:bigint>int/*",
      "*:cast(cast:bigint>int)>bigint/*"), 19445, "narrowing casts that wrap"),
    BugFamily("decimal_cast", CSI_ONLY, Seq("*:cast:*>decimal(*"), 19445, "decimal casts that overflow into NULL bounds"),
    BugFamily("string_to_datetime", CSI_ONLY, Seq("*:cast:string>date/*", "*:cast:string>timestamp/*", "*:to_date/*",
      "*:to_timestamp/*"), 19481, "string to date or timestamp casts, to_date and to_timestamp"),
    BugFamily("non_sortable_format", CSI_ONLY, Seq("*:date_format:dd-MM-yyyy/*", "*:date_format:MM/dd/yyyy/*",
      "*:date_format:EEE/*", "*:from_unixtime:dd-MM-yyyy/*"), 19481, "non-sortable formats in date_format and from_unixtime"),
    // unparseable strings throw on Spark 3 and give NULL bounds on Spark 4
    BugFamily("to_unix_timestamp", BOTH, Seq("*:to_unix_timestamp:*"), 19481, "to_unix_timestamp of strings"),
    BugFamily("bitwise_or", BOTH, Seq("*:bitor/*"), 20296, "BitwiseOr with a constant"),
    BugFamily("add_overflow", BOTH, Seq("*:add_pos/*", "*:add_neg/*", "*:cast(add_pos)>bigint/*"), 20296,
      "Add overflow without ANSI"),
    BugFamily("shiftleft_overflow", BOTH, Seq("*:shiftleft/*"), 20296, "ShiftLeft overflow"),
    BugFamily("multiply_overflow", BOTH, Seq("*:multiply_pos/*"), 20296, "Multiply overflow with a positive factor"),
    BugFamily("log_null_bound", CSI_ONLY, Seq("*:ln/*", "*:log10/*", "*:log1p/*", "*:log2/*"), 20296,
      "NULL bounds of ln, log10, log1p and log2"),
    BugFamily("like_prefix", BOTH, Seq("*:*/like"), 20430, "LIKE 'prefix%' translated as an equality on the prefix"),
    BugFamily("dst_fall_back", BOTH, Seq("*:date_format:yyyy-MM-dd HH:mm/*", "*:cast:timestamp>string/*",
      "*:from_utc_timestamp:America/New_York/*", "*:date_format(from_utc_timestamp:America/New_York):yyyy-MM-dd HH:mm/*",
      // a timestamp rendered as a string only breaks on DST, whatever the collation of the string
      "*:cast:timestamp>string collate */*"),
      20296, "DST fall-back in date_format HH:mm, cast(ts as string) and from_utc_timestamp"),
    BugFamily("collation_cast", CSI_ONLY, Seq("*:cast:*>string collate */*"), 19445,
      "casts to a string of another collation (Spark 4)"),
    BugFamily("to_utc_timestamp_gap", CSI_ONLY, Seq("*:to_utc_timestamp:America/New_York/*"), 20296,
      "to_utc_timestamp over the spring-forward gap"),
    BugFamily("from_unixtime_years", CSI_ONLY, Seq("*:from_unixtime:yyyy-MM-dd HH:mm:ss/*"), 20296,
      "from_unixtime with years outside 0000-9999"),
    BugFamily("missing_stats_compound", CSI_ONLY, Seq("*:compound:*/missing_stats"), 20296,
      "missing stats in AND, OR and NOT compounds"),
    BugFamily("missing_partition_stats", Set("psi"), Seq("psi:missing_stats/leaf"), 20296,
      "partitions with missing partition stats"))

  def familyOf(signature: String): Option[BugFamily] =
    BUG_FAMILIES.find(_.globs.exists(glob => signature.matches(glob.split("\\*", -1).map(java.util.regex.Pattern.quote).mkString(".*"))))

  // ---------------------------------------------------------------------------------------------
  // Budget and dimensions
  // ---------------------------------------------------------------------------------------------

  val SEED = 20296L
  // all but the last dataset use the edge profile; only the first one keeps complete stats
  val NUM_DATASETS = 3
  val FILES_PER_DATASET: (Int, Int) = (24, 32)
  val ROWS_PER_FILE: (Int, Int) = (2, 5)
  val NUM_PARTITIONS = 8
  val ORACLE_BATCH = 64
  val FOCUS_PROB = 0.7
  val BROKEN_FOCUS_PROB = 0.6
  val MAX_DEPTH = 3
  val SWEEP_FILES = 14
  val ALL_NULL_PROB = 0.07
  val NULL_PROB = 0.15
  val MISSING_COLUMN_STATS_PROB = 0.12
  val MISSING_FILE_STATS_PROB = 0.06
  val MISSING_PARTITION_STATS_PROB = 0.1
  // index version of the stats of every dataset
  val INDEX_VERSIONS: Seq[HoodieIndexVersion] = Seq(HoodieIndexVersion.V2, HoodieIndexVersion.V2, HoodieIndexVersion.V1)

  case class Dim(ansi: Boolean, timeZone: String)

  // Files are written once in UTC and queried under arbitrary session confs
  val DIMS: Seq[Dim] = for {
    ansi <- Seq(false, true)
    tz <- Seq("UTC", "America/New_York", "Asia/Kolkata")
  } yield Dim(ansi, tz)

  // ---------------------------------------------------------------------------------------------
  // Schema and value pools
  // ---------------------------------------------------------------------------------------------

  case class OracleColumn(name: String, dataType: DataType, sqlType: String)

  val COLUMNS: Seq[OracleColumn] = Seq(
    OracleColumn("i", IntegerType, "INT"),
    OracleColumn("l", LongType, "BIGINT"),
    OracleColumn("sh", ShortType, "SMALLINT"),
    OracleColumn("d", DoubleType, "DOUBLE"),
    OracleColumn("dcm", DecimalType(10, 2), "DECIMAL(10,2)"),
    OracleColumn("s", StringType, "STRING"),
    // date-like strings: padded, unpadded, unparseable
    OracleColumn("sd", StringType, "STRING"),
    OracleColumn("dt", DateType, "DATE"),
    OracleColumn("ts", TimestampType, "TIMESTAMP"))

  val INDEXED_COLUMNS: Seq[String] = COLUMNS.map(_.name)

  // fid identifies the file and is never referenced by predicates
  val TABLE_SCHEMA: StructType = StructType(StructField("fid", IntegerType, nullable = false) +:
    COLUMNS.map(c => StructField(c.name, c.dataType)))

  val HOODIE_SCHEMA: HoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(TABLE_SCHEMA, "hoodie.oracle_record")

  val COLUMN_SCHEMAS: Map[String, HoodieSchema] = INDEXED_COLUMNS.map(c => c -> HOODIE_SCHEMA.getField(c).get().schema()).toMap

  sealed trait Profile
  // edge values, including ones that make ANSI evaluation throw
  case object Edge extends Profile
  // no overflowing or unparseable values, so that ANSI runs get to evaluate most predicates
  case object Clean extends Profile

  private def ts(instant: String): Timestamp = Timestamp.from(Instant.parse(instant))

  // NOTE: NaN is left out on purpose: writer-side NaN stats handling is tracked separately (#18792)
  def edgeValues(column: String, profile: Profile): Seq[Any] = (column, profile) match {
    case ("i", Edge) => Seq(Int.MinValue, Int.MinValue + 1, -1000, -3, -2, -1, 0, 1, 2, 3, 5, 7, 100, 1000, Int.MaxValue - 1, Int.MaxValue)
    case ("i", Clean) => Seq(-1000, -3, -2, -1, 0, 1, 2, 3, 5, 7, 100, 1000)
    case ("l", Edge) => Seq(Long.MinValue, Long.MinValue + 1, -86400L, -1L, 0L, 1L, 2L, 3L, Int.MaxValue.toLong, Int.MaxValue + 1L,
      1773000000L, 1793500000L, 1793502000L, 1793506000L, 253402300799L, 253402300800L, Long.MaxValue - 1, Long.MaxValue)
    case ("l", Clean) => Seq(-86400L, -1L, 0L, 1L, 2L, 3L, 1773000000L, 1793500000L, 1793502000L, 1793506000L)
    case ("sh", Edge) => Seq(Short.MinValue, (Short.MinValue + 1).toShort, -1.toShort, 0.toShort, 1.toShort, 2.toShort, 300.toShort,
      (Short.MaxValue - 1).toShort, Short.MaxValue)
    case ("sh", Clean) => Seq(-300.toShort, -1.toShort, 0.toShort, 1.toShort, 2.toShort, 300.toShort)
    case ("d", Edge) => Seq(-1e300, -1e10, -2.5, -1.0, -0.5, -0.0, 0.0, 0.5, 1.0, 2.0, math.E, 10.0, 1e10, 1e300)
    case ("d", Clean) => Seq(-2.5, -1.0, -0.5, -0.0, 0.0, 0.5, 1.0, 2.0, math.E, 10.0)
    case ("dcm", Edge) => Seq("-99999999.99", "-100.00", "-1.00", "-0.05", "-0.01", "0.00", "0.01", "0.05", "0.95", "1.00", "1.04",
      "1.05", "9.95", "99.94", "99.95", "100.00", "99999999.99").map(new JBigDecimal(_))
    case ("dcm", Clean) => Seq("-1.00", "-0.05", "-0.01", "0.00", "0.01", "0.05", "0.95", "1.00", "1.04", "1.05", "9.95").map(new JBigDecimal(_))
    // shared prefixes, mixed case, 2/3/4-byte UTF-8 (U+FF21 sorts before U+1F600 in UTF-8 but after it in UTF-16)
    case ("s", _) => Seq("", "-1", "10", "9", "A", "APPLE", "Zebra", "a", "ab", "abc", "abc1", "abc9", "abd", "apple", "b", "zebra",
      "\u00e9t\u00e9", "\u4e2d\u6587", "\uff21", "\ud83d\ude00")
    case ("sd", Edge) => Seq("", "2023-12-31", "2024-01-15", "2024-02-01", "2024-02-1", "2024-1-15", "2024-10-1", "2024-12-31",
      "2024-13-01", "2024-2-1", "20240201", "2025-01-01", "2026-03-08 02:30:00", "2026-11-01 01:30:00", "abc")
    case ("sd", Clean) => Seq("2023-12-31", "2024-01-15", "2024-02-01", "2024-1-15", "2024-10-1", "2024-12-31", "2024-2-1", "2025-01-01")
    case ("dt", _) => Seq("1900-01-01", "1969-12-31", "1970-01-01", "2023-12-31", "2024-01-01", "2024-01-31", "2024-02-01", "2024-02-29",
      "2024-03-01", "2024-12-31", "2025-01-01", "2026-02-20", "2026-03-08", "2026-03-15", "2026-11-01", "2026-12-31").map(Date.valueOf)
    // America/New_York spring-forward (2026-03-08 07:00Z) and fall-back (2026-11-01 06:00Z), midnight UTC,
    // Asia/Kolkata midnight (18:30Z), epoch, pre-1970
    case ("ts", _) => Seq("1960-06-01T00:00:00Z", "1969-12-31T23:00:00Z", "1970-01-01T00:00:00Z", "1970-01-01T00:00:00.000001Z",
      "2026-03-08T01:59:59Z", "2026-03-08T02:30:00Z", "2026-03-08T03:00:00Z", "2026-03-08T06:59:59Z", "2026-03-08T07:00:00Z",
      "2026-03-08T07:30:00Z", "2026-03-14T23:59:59Z", "2026-03-15T00:00:00Z", "2026-03-15T00:00:01Z", "2026-03-15T10:00:00Z",
      "2026-03-15T18:29:59Z", "2026-03-15T18:30:00Z", "2026-11-01T04:59:59Z", "2026-11-01T05:30:00Z", "2026-11-01T05:40:00Z",
      "2026-11-01T05:50:00Z", "2026-11-01T05:59:59Z", "2026-11-01T06:00:00Z", "2026-11-01T06:10:00Z", "2026-11-01T06:20:00Z",
      "2026-11-01T06:30:00Z", "2026-11-01T07:00:00Z").map(ts)
  }

  private def randomValue(column: String, rnd: Random): Any = column match {
    case "i" => rnd.nextInt(41) - 20
    case "l" => if (rnd.nextBoolean()) (rnd.nextInt(41) - 20).toLong else 1793500000L + rnd.nextInt(20000)
    case "sh" => (rnd.nextInt(41) - 20).toShort
    case "d" => math.round((rnd.nextDouble() * 20 - 10) * 1000) / 1000.0
    case "dcm" => JBigDecimal.valueOf(rnd.nextInt(2001) - 1000L, 2)
    case "s" => (0 to rnd.nextInt(4)).map(_ => pick(rnd, Seq("a", "b", "c", "1", "9", "Z"))).mkString
    case "sd" =>
      def part(v: Int): String = if (rnd.nextBoolean()) f"$v%02d" else v.toString
      s"${2023 + rnd.nextInt(3)}-${part(1 + rnd.nextInt(12))}-${part(1 + rnd.nextInt(28))}"
    case "dt" => Date.valueOf(java.time.LocalDate.of(2024, 1, 1).plusDays(rnd.nextInt(1100)))
    case "ts" => Timestamp.from(Instant.parse("2026-01-01T00:00:00Z").plusSeconds(rnd.nextInt(365 * 86400)).plusNanos(rnd.nextInt(1000) * 1000L))
  }

  private def poolFor(column: OracleColumn, profile: Profile, rnd: Random): Seq[Any] =
    (edgeValues(column.name, profile) ++ Seq.fill(8)(randomValue(column.name, rnd))).distinct.sorted(ordering(column.dataType))

  def ordering(dataType: DataType): Ordering[Any] = dataType match {
    case IntegerType => Ordering.by[Any, Int](_.asInstanceOf[Int])
    case LongType => Ordering.by[Any, Long](_.asInstanceOf[Long])
    case ShortType => Ordering.by[Any, Short](_.asInstanceOf[Short])
    case DoubleType => Ordering.by[Any, Double](_.asInstanceOf[Double])
    case _: DecimalType => Ordering.fromLessThan[Any]((a, b) => a.asInstanceOf[JBigDecimal].compareTo(b.asInstanceOf[JBigDecimal]) < 0)
    // Spark (and parquet) compare strings by their unsigned UTF-8 bytes
    case StringType => Ordering.fromLessThan[Any]((a, b) => compareUtf8(a.toString, b.toString) < 0)
    // collated strings (Spark 4 only) sort by their collation, so Spark does the comparison
    case collated: StringType => Ordering.fromLessThan[Any]((a, b) => LessThan(Literal.create(a, collated), Literal.create(b, collated)).eval() == true)
    case ByteType => Ordering.by[Any, Byte](_.asInstanceOf[Byte])
    case FloatType => Ordering.by[Any, Float](_.asInstanceOf[Float])
    case BooleanType => Ordering.by[Any, Boolean](_.asInstanceOf[Boolean])
    case DateType => Ordering.by[Any, Long](_.asInstanceOf[Date].getTime)
    case TimestampType => Ordering.by[Any, Long](v => micros(v.asInstanceOf[Timestamp]))
  }

  private def compareUtf8(a: String, b: String): Int = {
    val (x, y) = (a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8))
    x.zip(y).collectFirst { case (p, q) if p != q => (p & 0xff) - (q & 0xff) }.getOrElse(x.length - y.length)
  }

  private def micros(t: Timestamp): Long = {
    val instant = t.toInstant
    instant.getEpochSecond * 1000000L + instant.getNano / 1000
  }

  def display(value: Any): String = value match {
    case null => "null"
    case v: Timestamp => v.toInstant.toString
    case v: String => s"'$v'"
    case v => v.toString
  }

  def sqlLiteral(value: Any, dataType: DataType): String = (value, dataType) match {
    case (null, _) => "NULL"
    case (v: Int, _) => v.toString
    case (v: Long, _) => s"${v}L"
    case (v: Short, _) => s"${v}S"
    case (v: Double, _) => s"CAST('$v' AS DOUBLE)"
    case (v: JBigDecimal, dt: DecimalType) => s"CAST('${v.toPlainString}' AS DECIMAL(${dt.precision},${dt.scale}))"
    case (v: String, _) => "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"
    case (v: Date, _) => s"DATE'$v'"
    case (v: Timestamp, _) => s"TIMESTAMP_MICROS(${micros(v)}L)"
  }

  private def perturb(rnd: Random, value: Any): Any = {
    val up = rnd.nextBoolean()
    value match {
      case v: Int => if (up) { if (v == Int.MaxValue) v else v + 1 } else { if (v == Int.MinValue) v else v - 1 }
      case v: Long => if (up) { if (v == Long.MaxValue) v else v + 1 } else { if (v == Long.MinValue) v else v - 1 }
      case v: Short => if (up) { if (v == Short.MaxValue) v else (v + 1).toShort } else { if (v == Short.MinValue) v else (v - 1).toShort }
      case v: Double => if (up) v + 0.5 else v - 0.5
      case v: JBigDecimal =>
        val next = if (up) v.add(new JBigDecimal("0.01")) else v.subtract(new JBigDecimal("0.01"))
        if (next.abs().compareTo(new JBigDecimal("99999999.99")) > 0) v else next
      case v: String => if (v.nonEmpty && up) v.substring(0, v.length - 1) else v + pick(rnd, Seq("0", "z", "-1"))
      case v: Date => Date.valueOf(v.toLocalDate.plusDays(if (up) 1 else -1))
      case v: Timestamp => Timestamp.from(v.toInstant.plusSeconds(pick(rnd, Seq(1L, 1800L, 3600L)) * (if (up) 1 else -1)))
    }
  }

  def pick[T](rnd: Random, values: Seq[T]): T = values(rnd.nextInt(values.size))

  // ---------------------------------------------------------------------------------------------
  // Transform templates. Every arm of OrderPreservingTransformation (BaseHoodieCatalystExpressionUtils
  // and unapplyOrderPreservingDateParsing) is exercised with order-preserving and order-breaking
  // parameters; `control` templates are not whitelisted and must never prune.
  // ---------------------------------------------------------------------------------------------

  case class Template(id: String, column: String, fn: String => String, control: Boolean = false)

  val DATE_FORMATS: Seq[String] = Seq("yyyy-MM-dd", "yyyy-MM-dd HH:mm", "yyyyMMdd", "dd-MM-yyyy", "MM/dd/yyyy", "EEE")
  val ZONES: Seq[String] = Seq("America/New_York", "Asia/Kolkata", "UTC")
  // collated strings only exist on Spark 4: UTF8_LCASE folds case, UNICODE sorts by ICU rules
  val CAST_TARGETS: Seq[String] = Seq("TINYINT", "SMALLINT", "INT", "BIGINT", "DOUBLE", "DECIMAL(10,2)", "DECIMAL(3,1)",
    "DECIMAL(20,4)", "STRING", "DATE", "TIMESTAMP") ++
    (if (HoodieSparkUtils.gteqSpark4_0) Seq("STRING COLLATE UTF8_LCASE", "STRING COLLATE UNICODE") else Seq.empty)
  val NUMERIC_COLUMNS: Seq[String] = Seq("i", "l", "sh", "d", "dcm")

  val TEMPLATES: Seq[Template] = {
    val b = Seq.newBuilder[Template]
    def t(id: String, column: String, control: Boolean = false)(fn: String => String): Unit = b += Template(id, column, fn, control)

    COLUMNS.foreach(c => t("identity", c.name)(x => x))
    // date/time
    for (c <- Seq("ts", "dt"); f <- DATE_FORMATS) t(s"date_format:$f", c)(x => s"date_format($x, '$f')")
    t("date_add_pos", "dt")(x => s"date_add($x, 1)")
    t("date_add_neg", "dt")(x => s"date_add($x, -3)")
    t("date_add_pos", "ts")(x => s"date_add($x, 1)")
    t("date_sub", "dt")(x => s"date_sub($x, 2)")
    t("datediff_end", "dt")(x => s"datediff($x, DATE'2024-01-01')")
    t("datediff_end", "ts")(x => s"datediff($x, DATE'2026-01-01')")
    t("datediff_start", "dt", control = true)(x => s"datediff(DATE'2024-01-01', $x)")
    for (c <- Seq("l", "i"); f <- Seq("yyyy-MM-dd HH:mm:ss", "dd-MM-yyyy")) t(s"from_unixtime:$f", c)(x => s"from_unixtime($x, '$f')")
    for (z <- ZONES) {
      t(s"from_utc_timestamp:$z", "ts")(x => s"from_utc_timestamp($x, '$z')")
      t(s"to_utc_timestamp:$z", "ts")(x => s"to_utc_timestamp($x, '$z')")
    }
    t("to_unix_timestamp:yyyy-M-d", "sd")(x => s"to_unix_timestamp($x, 'yyyy-M-d')")
    t("to_unix_timestamp:yyyy-MM-dd", "sd")(x => s"to_unix_timestamp($x, 'yyyy-MM-dd')")
    t("to_unix_timestamp", "ts")(x => s"to_unix_timestamp($x)")
    t("to_unix_timestamp", "dt")(x => s"to_unix_timestamp($x)")
    t("unix_timestamp:yyyy-M-d", "sd", control = true)(x => s"unix_timestamp($x, 'yyyy-M-d')")
    t("to_date", "sd")(x => s"to_date($x)")
    t("to_date:yyyy-M-d", "sd")(x => s"to_date($x, 'yyyy-M-d')")
    t("to_date", "ts")(x => s"to_date($x)")
    t("to_timestamp", "sd")(x => s"to_timestamp($x)")
    t("to_timestamp:yyyy-M-d", "sd")(x => s"to_timestamp($x, 'yyyy-M-d')")
    // strings
    for (n <- Seq(1, 3); c <- Seq("s", "sd")) t(s"left:$n", c)(x => s"left($x, $n)")
    t("substring:1,2", "s")(x => s"substring($x, 1, 2)")
    t("substring:2,2", "s", control = true)(x => s"substring($x, 2, 2)")
    t("lower", "s")(x => s"lower($x)")
    t("upper", "s")(x => s"upper($x)")
    // math
    for (c <- NUMERIC_COLUMNS) {
      t("add_pos", c)(x => s"($x) + 1")
      t("add_pos", c)(x => s"1 + ($x)")
      t("add_neg", c)(x => s"($x) + -1")
      t("multiply_pos", c)(x => s"($x) * 2")
      t("multiply_pos", c)(x => s"2 * ($x)")
      t("multiply_neg", c)(x => s"($x) * -1")
      t("multiply_neg", c)(x => s"-3 * ($x)")
      t("multiply_zero", c)(x => s"($x) * 0")
      t("divide_pos", c)(x => s"($x) / 2")
      t("divide_neg", c)(x => s"($x) / -1")
    }
    t("add_pos", "i")(x => s"($x) + 2147483647")
    for (c <- Seq("i", "l", "sh")) {
      t("bitor", c)(x => s"($x) | 2")
      t("bitor", c)(x => s"1 | ($x)")
      t("shiftleft", c)(x => s"shiftleft($x, 1)")
      t("shiftright", c)(x => s"shiftright($x, 1)")
    }
    for (c <- Seq("d", "i"); fn <- Seq("exp", "expm1", "ln", "log10", "log1p", "log2")) t(fn, c)(x => s"$fn($x)")
    // casts across all pairs of the schema types
    for (c <- COLUMNS; target <- CAST_TARGETS if target != c.sqlType) {
      t(s"cast:${c.sqlType.toLowerCase}>${target.toLowerCase}", c.name)(x => s"CAST($x AS $target)")
    }
    // depth-2 compositions
    t("cast(date_add_pos)>string", "dt")(x => s"CAST(date_add($x, 1) AS STRING)")
    t("date_format(date_add_pos):yyyy-MM-dd", "dt")(x => s"date_format(date_add($x, 1), 'yyyy-MM-dd')")
    t("cast(add_pos)>bigint", "i")(x => s"CAST(($x) + 1 AS BIGINT)")
    t("add_pos(multiply_neg)", "i")(x => s"(($x) * -1) + 1")
    t("left:7(cast>string)", "dt")(x => s"left(CAST($x AS STRING), 7)")
    t("cast(cast:bigint>int)>bigint", "l")(x => s"CAST(CAST($x AS INT) AS BIGINT)")
    t("date_format(from_utc_timestamp:America/New_York):yyyy-MM-dd HH:mm", "ts")(x =>
      s"date_format(from_utc_timestamp($x, 'America/New_York'), 'yyyy-MM-dd HH:mm')")
    t("log10(multiply_neg)", "d")(x => s"log10(($x) * -1)")
    // controls
    t("abs", "i", control = true)(x => s"abs($x)")
    t("abs", "d", control = true)(x => s"abs($x)")
    t("negate", "i", control = true)(x => s"-($x)")
    t("mod2", "i", control = true)(x => s"($x) % 2")
    b.result()
  }

  val WHITELIST_ARMS: Seq[String] = Seq("DateFormatClass", "DateAdd", "DateSub", "DateDiff", "FromUnixTime", "FromUTCTimestamp",
    "ToUnixTimestamp", "ToUTCTimestamp", "Left", "Add", "Multiply", "Divide", "BitwiseOr", "Exp", "Expm1", "Log", "Log10", "Log1p",
    "Log2", "ShiftLeft", "ShiftRight", "Cast", "AttributeReference", "ParseToDate", "ParseToTimestamp")

  val COMPARISON_SHAPES: Seq[String] = Seq("eq", "eq_r", "lt", "lt_r", "le", "le_r", "gt", "gt_r", "ge", "ge_r", "ne", "in", "inset",
    "not_in", "isnull", "isnotnull")
  // only translated for a plain column
  val IDENTITY_SHAPES: Seq[String] = Seq("ne_r", "nseq", "nseq_null")
  val STRING_SHAPES: Seq[String] = Seq("like", "not_like")

  case class ResolvedTemplate(template: Template, resolved: Expression) {
    val key: String = template.fn(template.column)
    // whether the result depends on time zone rules (session time zone or an explicit one)
    val zoned: Boolean = resolved.find {
      case c: Cast => Cast.needsTimeZone(c.child.dataType, c.dataType)
      case _: TimeZoneAwareExpression | _: FromUTCTimestamp | _: ToUTCTimestamp => true
      case _ => false
    }.isDefined
    val shapes: Seq[String] = COMPARISON_SHAPES ++
      (if (template.id == "identity") IDENTITY_SHAPES else Seq.empty) ++
      (if (resolved.dataType == StringType) STRING_SHAPES else Seq.empty)
  }

  // ---------------------------------------------------------------------------------------------
  // Predicates
  // ---------------------------------------------------------------------------------------------

  sealed trait Pred {
    def sql: String
    def op: String
    // leaves with the polarity they have once NOT is pushed down
    def leaves(negated: Boolean): Seq[(LeafPred, Boolean)]
    def templateIds: Seq[String] = leaves(negated = false).map(_._1.template.id).distinct
  }

  case class LeafPred(template: Template, shape: String, sql: String) extends Pred {
    override def op: String = "leaf"
    override def leaves(negated: Boolean): Seq[(LeafPred, Boolean)] = Seq((this, negated))
  }

  case class AndPred(left: Pred, right: Pred) extends Pred {
    override def sql: String = s"(${left.sql}) AND (${right.sql})"
    override def op: String = "and"
    override def leaves(negated: Boolean): Seq[(LeafPred, Boolean)] = left.leaves(negated) ++ right.leaves(negated)
  }

  case class OrPred(left: Pred, right: Pred) extends Pred {
    override def sql: String = s"(${left.sql}) OR (${right.sql})"
    override def op: String = "or"
    override def leaves(negated: Boolean): Seq[(LeafPred, Boolean)] = left.leaves(negated) ++ right.leaves(negated)
  }

  case class NotPred(child: Pred) extends Pred {
    override def sql: String = s"NOT (${child.sql})"
    override def op: String = "not"
    override def leaves(negated: Boolean): Seq[(LeafPred, Boolean)] = child.leaves(!negated)
  }

  def leafSql(leaf: LeafPred, negated: Boolean): String = if (negated) s"NOT (${leaf.sql})" else leaf.sql

  def splitConjuncts(e: Expression): Seq[Expression] = e match {
    case And(l, r) => splitConjuncts(l) ++ splitConjuncts(r)
    case other => Seq(other)
  }

  // ---------------------------------------------------------------------------------------------
  // Index paths, outcomes and results
  // ---------------------------------------------------------------------------------------------

  /**
   * Leaves per template and dim: transforms that do not depend on time zones behave the same under
   * every session time zone, so their budget goes to the UTC dims and zoned ones get more elsewhere;
   * plain columns, the most common filters, get more everywhere
   */
  def leavesPerDim(path: IndexPath, rt: ResolvedTemplate, dim: Dim): Int = {
    val base = if (rt.zoned) path.leavesPerTemplatePerDim + (if (dim.timeZone == "UTC") 0 else 2)
      else if (dim.timeZone == "UTC") path.leavesPerTemplatePerDim else path.leavesPerTemplatePerOtherZone
    if (rt.template.id == "identity") base + 2 else base
  }

  /**
   * @param minEvaluated floor on the predicates evaluated, and minPruning on those that pruned anything,
   *                     so that a degenerate run can not pass
   */
  sealed abstract class IndexPath(val id: String, val leavesPerTemplatePerDim: Int, val leavesPerTemplatePerOtherZone: Int,
                                  val compoundsPerDim: Int, val minEvaluated: Int, val minPruning: Int) {
    def unitsOf(ds: OracleDataset, fids: Set[Int]): Set[Int]
  }

  case object CSI extends IndexPath("csi", leavesPerTemplatePerDim = 3, leavesPerTemplatePerOtherZone = 1, compoundsPerDim = 70,
    minEvaluated = 2000, minPruning = 1500) {
    override def unitsOf(ds: OracleDataset, fids: Set[Int]): Set[Int] = fids
  }

  // NOTE: Partition stats pruning ignores filters holding unary expressions (casts, NOT, unary math, ...),
  //       and every evaluation persists the index view, hence a smaller budget
  case object PSI extends IndexPath("psi", leavesPerTemplatePerDim = 1, leavesPerTemplatePerOtherZone = 0, compoundsPerDim = 30,
    minEvaluated = 700, minPruning = 150) {
    override def unitsOf(ds: OracleDataset, fids: Set[Int]): Set[Int] = fids.map(ds.partitionOf)
  }

  case class OracleFile(fid: Int, partition: String, fileName: String, path: String,
                        stats: Seq[HoodieColumnRangeMetadata[Comparable[_]]])

  /**
   * @param matched    units (files or partitions) holding at least one row for which the predicate is TRUE
   * @param candidates units kept by data skipping, None if nothing was pruned
   * @param failed     whether candidate computation threw
   */
  case class Outcome(matched: Set[Int], candidates: Option[Set[Int]], filters: Seq[Expression], failed: Boolean) {
    def violations: Set[Int] = candidates.map(c => matched -- c).getOrElse(Set.empty)
  }

  case class Counterexample(signature: String, dim: Dim, ds: OracleDataset, pred: Pred, unit: Int,
                            filters: Seq[Expression]) {
    // smaller is better: fewer leaves, fewer raw rows, shorter SQL
    def size: (Int, Int, Int) = {
      val unitRows = ds.rows.count(r => if (signature.startsWith(PSI.id)) ds.partitionOf(r.getInt(0)) == unit else r.getInt(0) == unit)
      (pred.leaves(negated = false).size, unitRows, pred.sql.length)
    }
  }

  class Results(path: IndexPath) {
    var evaluated = 0
    var pruning = 0
    val skipped: mutable.Map[String, Int] = mutable.TreeMap[String, Int]().withDefaultValue(0)
    val failures: mutable.Map[String, Int] = mutable.TreeMap[String, Int]().withDefaultValue(0)
    private val violationCounts = mutable.TreeMap[String, Int]().withDefaultValue(0)
    private val smallest = mutable.HashMap[String, Counterexample]()

    def record(ce: Counterexample): Unit = {
      violationCounts(ce.signature) += 1
      smallest.get(ce.signature) match {
        case Some(prev) if Ordering[(Int, Int, Int)].lteq(prev.size, ce.size) =>
        case _ => smallest(ce.signature) = ce
      }
    }

    def printSummary(datasets: Seq[OracleDataset]): Unit = {
      val tag = s"[oracle:${path.id}]"
      datasets.foreach(ds => println(s"$tag dataset #${ds.idx}(${ds.profile}, index ${ds.indexVersion}): ${ds.files.size} files, " +
        s"${ds.rows.size} rows, unindexed files ${ds.unindexedFiles.toSeq.sorted.mkString("[", ",", "]")}, " +
        s"unindexed partitions ${ds.unindexedPartitions.toSeq.sorted.mkString("[", ",", "]")}"))
      println(s"$tag evaluated=$evaluated pruned_something=$pruning skipped=${skipped.mkString(", ")}")
      println(s"$tag candidate computation failures (${if (path == CSI) "fallback to no pruning" else "query would fail"}): " +
        s"${failures.values.sum} ${failures.toSeq.sortBy(-_._2).take(15).map { case (k, v) => s"$k=$v" }.mkString(", ")}")
      println(s"$tag ${violationCounts.size} violation signatures (${violationCounts.values.sum} violating predicate/unit pairs):")
      violationCounts.foreach { case (signature, count) =>
        println(f"    $signature%-72s $count%5d  ${familyOf(signature).filter(_.paths.contains(path.id)).map(_.id).getOrElse("UNATTRIBUTED")}")
      }
    }

    def sanityFailure(armProblems: Seq[String]): Option[String] = {
      val problems = armProblems ++
        (if (evaluated < path.minEvaluated) Seq(s"only $evaluated predicates evaluated, expected at least ${path.minEvaluated}") else Nil) ++
        (if (pruning < path.minPruning) Seq(s"only $pruning predicates pruned anything, expected at least ${path.minPruning}") else Nil)
      if (problems.isEmpty) None else Some(problems.mkString("; "))
    }

    def familyFailure(family: BugFamily, test: TestDataSkippingOracle): Option[String] =
      failure(violationCounts.keys.filter(s => familyOf(s).contains(family)).toSeq, s"see ${family.link}", test)

    // signatures of no family, or of a family not expected on this path
    def unattributedFailure(test: TestDataSkippingOracle): Option[String] =
      failure(violationCounts.keys.filterNot(s => familyOf(s).exists(_.paths.contains(path.id))).toSeq,
        "add the signatures to a bug family of TestDataSkippingOracle.BUG_FAMILIES", test)

    private def failure(signatures: Seq[String], hint: String, test: TestDataSkippingOracle): Option[String] = {
      if (signatures.isEmpty) {
        None
      } else {
        val pairs = signatures.map(violationCounts).sum
        val listed = signatures.sortBy(s => -violationCounts(s)).take(8).map(s => s"$s (${violationCounts(s)})").mkString(", ")
        Some(s"Data skipping dropped matching ${if (path == CSI) "files" else "partitions"} in $pairs predicate/unit pairs " +
          s"over ${signatures.size} signatures ($hint): $listed${if (signatures.size > 8) ", ..." else ""}\n" +
          s"Smallest counterexample:\n${test.describe(signatures.map(smallest).minBy(_.size))}")
      }
    }
  }

  class OracleDataset(spark: SparkSession,
                      val idx: Int,
                      val profile: Profile,
                      val indexVersion: HoodieIndexVersion,
                      val files: Seq[OracleFile],
                      val partitions: Seq[String],
                      val pools: Map[String, Seq[Any]],
                      val rows: Seq[Row],
                      val parquetDf: DataFrame,
                      val rawDf: DataFrame,
                      val unindexedFiles: Set[Int],
                      val missingFileStats: Map[Int, Set[String]],
                      val unindexedPartitions: Set[Int],
                      val missingPartitionStats: Map[Int, Set[String]]) {

    val fidByFileName: Map[String, Int] = files.map(f => f.fileName -> f.fid).toMap
    val values: Map[String, Seq[Any]] = COLUMNS.map(c => c.name -> rows.map(_.getAs[Any](c.name)).filter(_ != null).distinct).toMap
    // distinct non-null values of every column, per file
    val fileValues: Map[String, Seq[Seq[Any]]] = COLUMNS.map(c => c.name -> files.map(f =>
      rows.filter(_.getInt(0) == f.fid).map(_.getAs[Any](c.name)).filter(_ != null).distinct)).toMap
    val fileIndex: HoodieFileIndex = mock(classOf[HoodieFileIndex])
    val fileSlices: Seq[(Option[PartitionPath], Seq[FileSlice])] = partitions.map { p =>
      (Option(new PartitionPath(p, Array.empty[AnyRef])), files.filter(_.partition == p).map { f =>
        val slice = new FileSlice(p, "20260101000000000", f.fileName.split("_")(0))
        slice.setBaseFile(new HoodieBaseFile(f.path))
        slice
      })
    }

    def partitionOf(fid: Int): Int = partitions.indexOf(files(fid).partition)

    def unitCount(path: IndexPath): Int = if (path == CSI) files.size else partitions.size

    def missingColumns(path: IndexPath, unit: Int): Set[String] = path match {
      case CSI => if (unindexedFiles.contains(unit)) INDEXED_COLUMNS.toSet else missingFileStats(unit)
      case PSI => if (unindexedPartitions.contains(unit)) INDEXED_COLUMNS.toSet else missingPartitionStats(unit)
    }

    private def columnStatsRecords(complete: Boolean): Seq[HoodieMetadataColumnStats] = files.flatMap { f =>
      if (!complete && unindexedFiles.contains(f.fid)) {
        Seq.empty
      } else {
        val kept = f.stats.filter(s => complete || !missingFileStats(f.fid).contains(s.getColumnName))
        HoodieMetadataPayload.createColumnStatsRecords(f.partition, kept.asJava, false).iterator().asScala.map(toColumnStats).toSeq
      }
    }

    // Aggregated the way the writer builds partition stats from file stats
    private def partitionStatsRecords(complete: Boolean): Seq[HoodieMetadataColumnStats] = partitions.indices.flatMap { p =>
      if (!complete && unindexedPartitions.contains(p)) {
        Seq.empty
      } else {
        val ranges = INDEXED_COLUMNS
          .filter(c => complete || !missingPartitionStats(p).contains(c))
          .map { c =>
            val fileRanges = files.filter(_.partition == partitions(p)).flatMap(_.stats.filter(_.getColumnName == c))
            FileFormatUtils.getColumnRangeInPartition[String](partitions(p), c,
              fileRanges.asInstanceOf[Seq[HoodieColumnRangeMetadata[String]]].asJava, COLUMN_SCHEMAS.asJava, indexVersion)
              .asInstanceOf[HoodieColumnRangeMetadata[Comparable[_]]]
          }
        HoodieMetadataPayload.createPartitionStatsRecords(partitions(p), ranges.asJava, false, true, HOption.empty[String]())
          .iterator().asScala.map(toColumnStats).toSeq
      }
    }

    private val csiSupports = mutable.HashMap[Boolean, OracleColumnStatsIndexSupport]()
    private val psiSupports = mutable.HashMap[Boolean, OraclePartitionStatsIndexSupport]()

    def csi(complete: Boolean): OracleColumnStatsIndexSupport = csiSupports.getOrElseUpdate(complete,
      new OracleColumnStatsIndexSupport(spark, mockMetaClient(indexVersion), () => columnStatsRecords(complete)))

    def psi(complete: Boolean): OraclePartitionStatsIndexSupport = psiSupports.getOrElseUpdate(complete,
      new OraclePartitionStatsIndexSupport(spark, mockMetaClient(indexVersion), () => partitionStatsRecords(complete)))

    def unitOf(path: IndexPath, name: String): Int = if (path == CSI) fidByFileName(name) else partitions.indexOf(name)

    def statsDf(path: IndexPath, columns: Seq[String], complete: Boolean): DataFrame =
      if (path == CSI) csi(complete).loadTransposed(columns, shouldReadInMemory = true)(identity)
      else psi(complete).loadTransposed(columns, shouldReadInMemory = true)(identity)

    def statsRow(path: IndexPath, unit: Int, columns: Seq[String], complete: Boolean): Option[Row] =
      statsDf(path, columns, complete).collect().find(r => unitOf(path, r.getString(0)) == unit)
  }

  def mockMetaClient(indexVersion: HoodieIndexVersion): HoodieTableMetaClient = {
    val metaClient = mock(classOf[HoodieTableMetaClient])
    val tableConfig = mock(classOf[HoodieTableConfig])
    val definition = HoodieIndexDefinition.newBuilder()
      .withIndexName(PARTITION_NAME_COLUMN_STATS).withIndexType(PARTITION_NAME_COLUMN_STATS)
      .withVersion(indexVersion).withSourceFields(INDEXED_COLUMNS.asJava).build()
    when(metaClient.getIndexMetadata).thenReturn(HOption.of(new HoodieIndexMetadata(Map(PARTITION_NAME_COLUMN_STATS -> definition).asJava)))
    when(metaClient.getIndexForMetadataPartition(PARTITION_NAME_COLUMN_STATS)).thenReturn(HOption.of(definition))
    when(metaClient.getTableConfig).thenReturn(tableConfig)
    when(tableConfig.getTableInitialVersion).thenReturn(HoodieTableVersion.NINE)
    metaClient
  }

  /**
   * Outcome of the oracle for one index path: the failure message, if any, of the sanity test, of every
   * bug family and of the unattributed violations
   */
  case class OracleRun(sanity: Option[String], families: Map[String, Option[String]], unattributed: Option[String])

  // NOTE: Surefire reruns failing tests (rerunFailingTestsCount), which re-invokes the test factories; the
  //       oracle is deterministic, so reruns within the JVM reuse its outcome instead of running it again
  private val ORACLE_RUNS = mutable.HashMap[String, OracleRun]()

  private val METADATA_CONFIG = HoodieMetadataConfig.newBuilder().enable(true).build()

  private def toColumnStats(record: HoodieRecord[_]): HoodieMetadataColumnStats =
    record.getData.asInstanceOf[HoodieMetadataPayload].getInsertValue(null, null).get()
      .asInstanceOf[HoodieMetadataRecord].getColumnStatsMetadata

  /**
   * Feeds the generated records to the production transpose; transposed views are memoized per set of
   * target columns (as with allowCaching) and served as in-memory relations
   */
  trait OracleIndexSupport extends ColumnStatsIndexSupport {
    protected def session: SparkSession

    protected def records: () => Seq[HoodieMetadataColumnStats]

    private val memo = mutable.HashMap[Seq[String], (Seq[Row], StructType)]()

    override def isIndexAvailable: Boolean = true

    override protected def shouldReadInMemory(fileIndex: HoodieFileIndex, columns: Seq[String], threshold: Integer): Boolean = true

    override def loadColumnStatsIndexRecords(targetColumns: Seq[String], prunedPartitions: Option[Set[String]],
                                             shouldReadInMemory: Boolean): HoodieData[HoodieMetadataColumnStats] =
      HoodieListData.eager(records().filter(r => targetColumns.contains(r.getColumnName)).asJava)

    override def loadTransposed[T](targetColumns: Seq[String], shouldReadInMemory: Boolean, prunedPartitions: Option[Set[String]],
                                   prunedFileNamesOpt: Option[Set[String]])(block: DataFrame => T): T = {
      val (rows, schema) = memo.getOrElseUpdate(targetColumns,
        super.loadTransposed(targetColumns, shouldReadInMemory, prunedPartitions, prunedFileNamesOpt)(df => (df.collect().toSeq, df.schema)))
      block(sparkAdapter.getUnsafeUtils.createDataFrameFromRows(session, rows, schema))
    }
  }

  class OracleColumnStatsIndexSupport(protected val session: SparkSession, metaClient: HoodieTableMetaClient,
                                      protected val records: () => Seq[HoodieMetadataColumnStats])
    extends ColumnStatsIndexSupport(session, TABLE_SCHEMA, HOODIE_SCHEMA, METADATA_CONFIG, metaClient) with OracleIndexSupport

  class OraclePartitionStatsIndexSupport(protected val session: SparkSession, metaClient: HoodieTableMetaClient,
                                         protected val records: () => Seq[HoodieMetadataColumnStats])
    extends PartitionStatsIndexSupport(session, TABLE_SCHEMA, HOODIE_SCHEMA, METADATA_CONFIG, metaClient) with OracleIndexSupport
}

/**
 * Property test for data skipping in which the raw data is the reference answer: for every generated
 * predicate, every file (column stats) or partition (partition stats) holding a row for which Spark
 * evaluates the predicate to TRUE must survive the production pruning path.
 *
 * Files are real parquet files, stats are read back from their footers and turned into metadata table
 * records the way the writer does, and candidates are computed through
 * [[ColumnStatsIndexSupport.computeCandidateFileNames]] and [[PartitionStatsIndexSupport.prunePartitions]],
 * fed with the conjuncts of the optimized filter, as a query would.
 *
 * Violations are reported by a signature built from the generator ids of [[TestDataSkippingOracle.TEMPLATES]]
 * (stable across Spark versions) and grouped into [[TestDataSkippingOracle.BUG_FAMILIES]]: the oracle runs once
 * per index path, and every family is a test that fails while any of its signatures is observed.
 */
class TestDataSkippingOracle extends HoodieSparkClientTestBase {

  private var spark: SparkSession = _

  @BeforeEach
  override def setUp(): Unit = {
    initSparkContexts()
    spark = sqlContext.sparkSession
  }

  @TestFactory
  def testColumnStatsIndexOracle(): java.util.stream.Stream[DynamicTest] = oracleTests(CSI)

  @TestFactory
  def testPartitionStatsIndexOracle(): java.util.stream.Stream[DynamicTest] = oracleTests(PSI)

  // ---------------------------------------------------------------------------------------------
  // Driver
  // ---------------------------------------------------------------------------------------------

  private def oracleTests(path: IndexPath): java.util.stream.Stream[DynamicTest] = {
    val run = ORACLE_RUNS.getOrElseUpdate(path.id, runOracle(path))
    // NOTE: Surefire reports dynamic tests by index, so the message repeats the display name
    def test(name: String, failure: Option[String]): DynamicTest =
      DynamicTest.dynamicTest(name, new Executable {
        override def execute(): Unit = failure.foreach(f => fail[Unit](s"[$name] $f"))
      })
    (Seq(test(s"${path.id}: harness sanity", run.sanity)) ++
      BUG_FAMILIES.filter(_.paths.contains(path.id)).map(f => test(f.displayName(path), run.families(f.id))) :+
      test(s"${path.id}: unattributed violations", run.unattributed)).asJava.stream()
  }

  private def runOracle(path: IndexPath): OracleRun = {
    val startNanos = System.nanoTime()
    val datasets = (0 until NUM_DATASETS).map(idx => writeDataset(idx, if (idx < NUM_DATASETS - 1) Edge else Clean))
    val templates = resolveTemplates()
    val results = new Results(path)
    val armProblems = checkArmCoverage(templates, datasets.head, print = path == CSI)

    DIMS.zipWithIndex.foreach { case (dim, dimIdx) =>
      withDim(dim) {
        val predicates = generatePredicates(path, dimIdx, templates, datasets)
        // leaves of compounds are evaluated on their own when attributing violations
        predicates.groupBy(_._1).foreach { case (ds, preds) =>
          val sqls = preds.flatMap { case (_, pred) =>
            pred.sql +: (if (pred.op == "leaf") Seq.empty else pred.leaves(negated = false).map { case (l, n) => leafSql(l, n) })
          }
          prefetchOracle(dim, ds, sqls.distinct)
        }
        predicates.foreach { case (ds, pred) => check(path, dim, ds, pred, results) }
      }
    }

    results.printSummary(datasets)
    val run = OracleRun(results.sanityFailure(armProblems),
      BUG_FAMILIES.map(f => f.id -> results.familyFailure(f, this)).toMap, results.unattributedFailure(this))
    println(f"[oracle:${path.id}] finished in ${(System.nanoTime() - startNanos) / 1e9}%.1fs")
    run
  }

  private def check(path: IndexPath, dim: Dim, ds: OracleDataset, pred: Pred, results: Results): Unit = {
    evaluate(path, dim, ds, complete = false, pred.sql) match {
      case Left(reason) => results.skipped(reason) += 1
      case Right(outcome) =>
        results.evaluated += 1
        if (outcome.failed) {
          results.failures(pred.templateIds.mkString("+")) += 1
        }
        if (outcome.candidates.exists(_.size < ds.unitCount(path))) {
          results.pruning += 1
        }
        outcome.violations.foreach { unit =>
          val signature = s"${path.id}:${attribute(path, dim, ds, pred, unit)}"
          results.record(Counterexample(signature, dim, ds, pred, unit, outcome.filters))
        }
    }
  }

  /**
   * Attributes a violation on `unit` to a generator signature: a compound predicate is attributed to
   * the first of its leaves (with the polarity it has inside the tree) that violates on its own, with
   * the simulated or with complete stats, and to the compound otherwise. A violation that disappears
   * once all stats are present is reported as a missing-stats one.
   */
  private def attribute(path: IndexPath, dim: Dim, ds: OracleDataset, pred: Pred, unit: Int): String = {
    def violates(complete: Boolean, sql: String): Boolean =
      evaluate(path, dim, ds, complete, sql).exists(_.violations.contains(unit))

    def leafSignature(leaf: LeafPred, negated: Boolean): String = {
      if (!violates(complete = true, leafSql(leaf, negated))) {
        "missing_stats/leaf"
      } else {
        val nullBound = if (nullBoundLeaves(path, dim, ds, leaf, unit).nonEmpty) "[null_bound]" else ""
        // NOT of a negated shape is the positive one
        val shape = if (!negated) leaf.shape
          else if (leaf.shape.startsWith("not_")) leaf.shape.stripPrefix("not_")
          else s"not_${leaf.shape}"
        s"${leaf.template.id}/$shape$nullBound"
      }
    }

    pred match {
      case leaf: LeafPred => leafSignature(leaf, negated = false)
      case _ =>
        val leaves = pred.leaves(negated = false)
        // a leaf may only violate with complete stats when the missing ones keep the file out of its own lookup
        leaves.find { case (leaf, negated) => violates(complete = false, leafSql(leaf, negated)) }
          .orElse(leaves.find { case (leaf, negated) => violates(complete = true, leafSql(leaf, negated)) })
          .map { case (leaf, negated) => leafSignature(leaf, negated) }
          .getOrElse {
            if (!violates(complete = true, pred.sql)) {
              s"compound:${pred.op}/missing_stats"
            } else if (nullBoundLeaves(path, dim, ds, pred, unit).nonEmpty) {
              s"compound:${pred.op}/null_bound"
            } else {
              s"compound:${pred.op}/other"
            }
          }
    }
  }

  private val planCache = mutable.HashMap[(Int, Dim, String), Either[String, Seq[Expression]]]()
  private val oracleCache = mutable.HashMap[(Int, Dim, String), Either[String, Set[Int]]]()
  private val evalCache = mutable.HashMap[(Int, String, Boolean, Dim, String), Either[String, Outcome]]()

  /**
   * Evaluates `sql` (session confs of `dim` must be in effect) against the raw rows (oracle) and the
   * production pruning path; returns the skip reason if Spark itself cannot plan or evaluate it.
   */
  private def evaluate(path: IndexPath, dim: Dim, ds: OracleDataset, complete: Boolean, sql: String): Either[String, Outcome] =
    evalCache.getOrElseUpdate((ds.idx, path.id, complete, dim, sql), {
      for {
        filters <- plan(dim, ds, sql)
        fids <- oracleCache.getOrElseUpdate((ds.idx, dim, sql), try {
          Right(oracle(ds, Seq(sql)).head)
        } catch {
          case NonFatal(_) => Left("raw_eval_error")
        })
      } yield {
        val (candidates, failed) = path match {
          case CSI => computeFileCandidates(ds, complete, filters)
          case PSI => computePartitionCandidates(ds, complete, filters)
        }
        Outcome(path.unitsOf(ds, fids), candidates, filters, failed)
      }
    })

  private def plan(dim: Dim, ds: OracleDataset, sql: String): Either[String, Seq[Expression]] =
    planCache.getOrElseUpdate((ds.idx, dim, sql), optimizedFilters(ds.parquetDf, sql))

  /**
   * Evaluates the predicates that Spark can plan as one projection over the raw rows, bisecting the
   * batch when the evaluation of some predicate throws (for ex, ANSI overflows)
   */
  private def prefetchOracle(dim: Dim, ds: OracleDataset, sqls: Seq[String]): Unit = {
    def run(batch: Seq[String]): Unit = {
      try {
        batch.zip(oracle(ds, batch)).foreach { case (sql, fids) => oracleCache((ds.idx, dim, sql)) = Right(fids) }
      } catch {
        case NonFatal(_) if batch.size > 1 =>
          val (left, right) = batch.splitAt(batch.size / 2)
          run(left)
          run(right)
        case NonFatal(_) => oracleCache((ds.idx, dim, batch.head)) = Left("raw_eval_error")
      }
    }
    sqls.filter(sql => plan(dim, ds, sql).isRight && !oracleCache.contains((ds.idx, dim, sql))).grouped(ORACLE_BATCH).foreach(run)
  }

  // fids holding a row for which the predicate is TRUE, with Spark three-valued semantics
  private def oracle(ds: OracleDataset, sqls: Seq[String]): Seq[Set[Int]] = {
    val matches = ds.rawDf.select(col("fid") +: sqls.map(expr): _*).collect()
    sqls.indices.map(i => matches.filter(r => !r.isNullAt(i + 1) && r.getBoolean(i + 1)).map(_.getInt(0)).toSet)
  }

  private def optimizedFilters(df: DataFrame, sql: String): Either[String, Seq[Expression]] = {
    try {
      df.where(sql).queryExecution.optimizedPlan.collectFirst { case f: Filter => f.condition } match {
        case Some(condition) => Right(splitConjuncts(condition))
        case None => Left("folded_away")
      }
    } catch {
      case NonFatal(_) => Left("plan_error")
    }
  }

  // Mirrors the Fallback mode of SparkBaseIndexSupport#computeCandidateIsStrict: a failure disables pruning
  private def computeFileCandidates(ds: OracleDataset, complete: Boolean, filters: Seq[Expression]): (Option[Set[Int]], Boolean) = {
    try {
      val referencedColumns = collectReferencedColumns(spark, filters, TABLE_SCHEMA)
      val names = ds.csi(complete).computeCandidateFileNames(ds.fileIndex, filters, referencedColumns, ds.fileSlices,
        shouldPushDownFilesFilter = false)
      (names.map(_.map(ds.fidByFileName)), false)
    } catch {
      case NonFatal(_) => (None, true)
    }
  }

  // NOTE: Production calls prunePartitions outside of any fallback, so a failure here fails the query
  private def computePartitionCandidates(ds: OracleDataset, complete: Boolean, filters: Seq[Expression]): (Option[Set[Int]], Boolean) = {
    try {
      (ds.psi(complete).prunePartitions(ds.fileIndex, filters).map(_.map(ds.partitions.indexOf(_))), false)
    } catch {
      case NonFatal(_) => (None, true)
    }
  }

  private def withDim[T](dim: Dim)(f: => T): T = {
    val keys = Seq(SQLConf.ANSI_ENABLED.key, SQLConf.SESSION_LOCAL_TIMEZONE.key)
    val saved = keys.map(k => k -> spark.conf.getOption(k))
    spark.conf.set(SQLConf.ANSI_ENABLED.key, dim.ansi.toString)
    spark.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, dim.timeZone)
    try {
      f
    } finally {
      saved.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None) => spark.conf.unset(k)
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Predicate generation
  // ---------------------------------------------------------------------------------------------

  private def resolveTemplates(): Seq[ResolvedTemplate] = withDim(Dim(ansi = false, "UTC")) {
    TEMPLATES.flatMap { t =>
      try {
        Some(ResolvedTemplate(t, resolveExpr(spark, t.fn(t.column), TABLE_SCHEMA)))
      } catch {
        // invalid combinations (for ex, casts Spark does not allow) are dropped
        case NonFatal(_) => None
      }
    }
  }

  private val brokenUnitsCache = mutable.HashMap[(String, Int, String), Map[String, Seq[Seq[Any]]]]()

  /**
   * Per template, the column values of every unit (file or partition) on which Spark shows the transform
   * not to preserve order: results out of order, or NULL for some but not all non-null inputs. Literals
   * are aimed at those units; whether rows get lost is still decided by the oracle. Computed with ANSI
   * off, under which the transforms either agree with ANSI or throw.
   */
  private def brokenUnits(path: IndexPath, ds: OracleDataset, timeZone: String,
                          templates: Seq[ResolvedTemplate]): Map[String, Seq[Seq[Any]]] =
    brokenUnitsCache.getOrElseUpdate((path.id, ds.idx, timeZone), withDim(Dim(ansi = false, timeZone)) {
      def run(batch: Seq[ResolvedTemplate]): Seq[(ResolvedTemplate, Seq[(Int, Any, Any)])] = {
        try {
          val rows = ds.rawDf.select(col("fid") +: batch.flatMap(rt => Seq(col(rt.template.column), expr(rt.key))): _*).collect()
          batch.zipWithIndex.map { case (rt, i) => rt -> rows.toSeq.map(r => (r.getInt(0), r.get(1 + 2 * i), r.get(2 + 2 * i))) }
        } catch {
          case NonFatal(_) if batch.size > 1 =>
            val (left, right) = batch.splitAt(batch.size / 2)
            run(left) ++ run(right)
          case NonFatal(_) => Seq.empty
        }
      }
      run(templates).map { case (rt, values) =>
        val inputOrdering = ordering(COLUMNS.find(_.name == rt.template.column).get.dataType)
        val outputOrdering = ordering(rt.resolved.dataType)
        rt.key -> values.groupBy(v => path.unitsOf(ds, Set(v._1)).head).toSeq.sortBy(_._1).flatMap { case (_, unitValues) =>
          val pairs = unitValues.collect { case (_, in, out) if in != null => (in, out) }.distinct.sortBy(_._1)(inputOrdering)
          val broken = pairs.zip(pairs.drop(1)).exists { case ((_, a), (_, b)) =>
            (a == null) != (b == null) || (a != null && outputOrdering.gt(a, b))
          }
          if (broken) Some(pairs.map(_._1)) else None
        }
      }.filter(_._2.nonEmpty).toMap
    })

  private def generatePredicates(path: IndexPath, dimIdx: Int, templates: Seq[ResolvedTemplate],
                                 datasets: Seq[OracleDataset]): Seq[(OracleDataset, Pred)] = {
    val leaves = for {
      rt <- templates
      k <- 0 until leavesPerDim(path, rt, DIMS(dimIdx))
    } yield {
      val rnd = new Random(SEED + 1000003L * dimIdx + 7919L * rt.key.hashCode + k)
      val withBroken = datasets.filter(ds => brokenUnits(path, ds, DIMS(dimIdx).timeZone, templates).contains(rt.key))
      val ds = if (withBroken.nonEmpty && rnd.nextDouble() < BROKEN_FOCUS_PROB) pick(rnd, withBroken) else pick(rnd, datasets)
      val broken = brokenUnits(path, ds, DIMS(dimIdx).timeZone, templates).getOrElse(rt.key, Seq.empty)
      // cycle through the shapes across dims so that every template meets most of them
      val shape = rt.shapes(Math.floorMod(rt.key.hashCode + dimIdx * 7 + k, rt.shapes.size))
      (ds, buildLeaf(rnd, ds, rt, shape, anchor = None, broken))
    }
    val compoundTemplates = templates.filterNot(_.template.control)
    val compounds = (0 until path.compoundsPerDim).flatMap { k =>
      val rnd = new Random(SEED + 1000003L * dimIdx + 104729L * k)
      // mostly on the datasets simulating missing stats
      val ds = if (rnd.nextDouble() < 0.8) pick(rnd, datasets.tail) else datasets.head
      // anchoring the literals on one row keeps conjunctions satisfiable
      val anchor = ds.rows(rnd.nextInt(ds.rows.size))
      Iterator.continually(buildTree(rnd, ds, compoundTemplates, anchor, mutable.Set[String](), MAX_DEPTH))
        .take(5).find(_.leaves(negated = false).size >= 2).map(tree => (ds, tree))
    }
    leaves ++ compounds
  }

  private def buildTree(rnd: Random, ds: OracleDataset, templates: Seq[ResolvedTemplate], anchor: Row,
                        usedColumns: mutable.Set[String], depth: Int): Pred = {
    def leaf(): Pred = {
      val available = templates.filterNot(rt => usedColumns.contains(rt.template.column))
      val rt = pick(rnd, if (available.nonEmpty) available else templates)
      usedColumns += rt.template.column
      buildLeaf(rnd, ds, rt, pick(rnd, rt.shapes), Some(anchor))
    }

    if (depth == 0 || (depth < MAX_DEPTH && rnd.nextDouble() < 0.35)) {
      leaf()
    } else {
      rnd.nextInt(10) match {
        case k if k < 5 => AndPred(buildTree(rnd, ds, templates, anchor, usedColumns, depth - 1),
          buildTree(rnd, ds, templates, anchor, usedColumns, depth - 1))
        case k if k < 8 => OrPred(buildTree(rnd, ds, templates, anchor, usedColumns, depth - 1),
          buildTree(rnd, ds, templates, anchor, usedColumns, depth - 1))
        case _ => NotPred(buildTree(rnd, ds, templates, anchor, usedColumns, depth - 1))
      }
    }
  }

  /**
   * The literal is the same transform applied to a (possibly perturbed) constant drawn from the data,
   * written as a foldable expression that the optimizer turns into a literal, so that predicates hit rows.
   */
  private def buildLeaf(rnd: Random, ds: OracleDataset, rt: ResolvedTemplate, shape: String, anchor: Option[Row],
                        broken: Seq[Seq[Any]] = Seq.empty): LeafPred = {
    val t = rt.template
    val column = COLUMNS.find(_.name == t.column).get
    var anchored = anchor.flatMap(r => Option(r.getAs[Any](column.name)))
    // most literals focus on one unit: its values and the pool values just outside its range
    val focus = if (broken.nonEmpty && rnd.nextDouble() < BROKEN_FOCUS_PROB) broken
      else if (rnd.nextDouble() < FOCUS_PROB) ds.fileValues(column.name).filter(_.size > 1)
      else Seq.empty
    val focusValues = if (focus.nonEmpty) pick(rnd, focus) else Seq.empty
    def lit(): String = {
      val value = anchored.getOrElse(pickConstant(rnd, ds, column, focusValues))
      anchored = None
      t.fn(sqlLiteral(value, column.dataType))
    }
    def lits(n: Int): String = (0 until n).map(_ => lit()).mkString(", ")

    val f = s"(${t.fn(column.name)})"
    val sql = shape match {
      case "eq" => s"$f = (${lit()})"
      case "eq_r" => s"(${lit()}) = $f"
      case "lt" => s"$f < (${lit()})"
      case "lt_r" => s"(${lit()}) > $f"
      case "le" => s"$f <= (${lit()})"
      case "le_r" => s"(${lit()}) >= $f"
      case "gt" => s"$f > (${lit()})"
      case "gt_r" => s"(${lit()}) < $f"
      case "ge" => s"$f >= (${lit()})"
      case "ge_r" => s"(${lit()}) <= $f"
      case "ne" => s"$f != (${lit()})"
      case "ne_r" => s"(${lit()}) != $f"
      case "nseq" => s"$f <=> (${lit()})"
      case "nseq_null" => s"$f <=> NULL"
      case "in" => s"$f IN (${lits(3)})"
      // above spark.sql.optimizer.inSetConversionThreshold, so that OptimizeIn produces InSet
      case "inset" => s"$f IN (${lits(12)})"
      case "not_in" => s"$f NOT IN (${lits(3)})"
      case "isnull" => s"$f IS NULL"
      case "isnotnull" => s"$f IS NOT NULL"
      case "like" => s"$f LIKE concat(substring(${lit()}, 1, ${1 + rnd.nextInt(4)}), '%')"
      case "not_like" => s"$f NOT LIKE concat(substring(${lit()}, 1, ${1 + rnd.nextInt(4)}), '%')"
    }
    LeafPred(t, shape, sql)
  }

  private def pickConstant(rnd: Random, ds: OracleDataset, column: OracleColumn, focusValues: Seq[Any]): Any = {
    val present = if (focusValues.nonEmpty) focusValues else ds.values(column.name)
    val pool = ds.pools(column.name)
    val r = rnd.nextDouble()
    if (present.isEmpty || r < 0.1) {
      pick(rnd, pool)
    } else if (r < 0.55) {
      pick(rnd, present)
    } else if (r < 0.85) {
      val idx = pool.indexOf(pick(rnd, present)) + pick(rnd, Seq(-2, -1, 1, 2))
      pool(math.max(0, math.min(pool.size - 1, idx)))
    } else {
      perturb(rnd, pick(rnd, present))
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Data, stats and index support wiring
  // ---------------------------------------------------------------------------------------------

  private def writeDataset(idx: Int, profile: Profile): OracleDataset = {
    val rnd = new Random(SEED + 31L * idx)
    val pools = COLUMNS.map(c => c.name -> poolFor(c, profile, rnd)).toMap
    val numFiles = FILES_PER_DATASET._1 + rnd.nextInt(FILES_PER_DATASET._2 - FILES_PER_DATASET._1 + 1)
    val sweepOffsets = COLUMNS.map(c => c.name -> new Random(SEED + c.name.hashCode).nextInt(1000)).toMap
    val generated = (0 until numFiles).flatMap { fid =>
      val sweep = fid < SWEEP_FILES
      val numRows = ROWS_PER_FILE._1 + rnd.nextInt(ROWS_PER_FILE._2 - ROWS_PER_FILE._1 + 1)
      val columnValues = COLUMNS.map { c =>
        val pool = pools(c.name)
        val edges = edgeValues(c.name, profile).sorted(ordering(c.dataType))
        val r = rnd.nextDouble()
        if (r < ALL_NULL_PROB) {
          Seq.fill(numRows)(null)
        } else if (sweep) {
          // the sweep files of all datasets hold every pair of adjacent edge values, hence straddle
          // every transition the edge values are placed around (DST, overflow, padding, ...)
          val start = (sweepOffsets(c.name) + idx * SWEEP_FILES + fid) % (edges.size - 1)
          (0 until numRows).map(row => if (row >= 2 && rnd.nextDouble() < NULL_PROB) null else edges(start + (row + fid) % 2))
        } else {
          // narrow windows keep file ranges tight enough for pruning to kick in
          val source = if (r < 0.5) edges else pool
          val size = 2 + rnd.nextInt(3)
          val window = if (r < 0.87) source.slice(rnd.nextInt(math.max(1, source.size - size + 1)), Int.MaxValue).take(size) else pool
          Seq.fill(numRows)(if (rnd.nextDouble() < NULL_PROB) null else pick(rnd, window))
        }
      }
      (0 until numRows).map(r => Row.fromSeq(Seq(fid) ++ columnValues.map(_(r) match {
        case v: Short => v.toInt
        case v => v
      }) ++ Seq(fid)))
    }

    val baseDir = Files.createTempDirectory(tempDir, s"oracle_$idx")
    val stagingDir = baseDir.resolve("staging").toString
    // NOTE: SMALLINT is written as plain INT32, like the Avro write path does: V2 stats can not be read
    //       from footers of INT(16)-annotated columns (LogicalTypeParquetAdapter rejects the annotation)
    val writeSchema = StructType(TABLE_SCHEMA.map(f => if (f.dataType == ShortType) f.copy(dataType = IntegerType) else f))
      .add(StructField("_fdir", IntegerType))
    withDim(Dim(ansi = false, "UTC")) {
      val conf = "spark.sql.parquet.outputTimestampType"
      val saved = spark.conf.getOption(conf)
      // the writer stores timestamps as micros
      spark.conf.set(conf, "TIMESTAMP_MICROS")
      try {
        spark.createDataFrame(generated.asJava, writeSchema).repartition(col("_fdir"))
          .write.partitionBy("_fdir").parquet(stagingDir)
      } finally {
        saved.map(spark.conf.set(conf, _)).getOrElse(spark.conf.unset(conf))
      }
    }

    val partitions = (0 until NUM_PARTITIONS).map(p => s"p$p")
    val storage = HoodieStorageUtils.getStorage(new StoragePath(baseDir.toString),
      HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration))
    val parquetUtils = new ParquetUtils()
    val files = (0 until numFiles).map { fid =>
      val partition = partitions(fid * NUM_PARTITIONS / numFiles)
      val fileName = f"$idx%08d-0000-0000-0000-$fid%012d-0_0-0-0_20260101000000000.parquet"
      val target = baseDir.resolve(partition).resolve(fileName)
      Files.createDirectories(target.getParent)
      val written = Files.list(Paths.get(stagingDir, s"_fdir=$fid")).iterator().asScala
        .filter(_.getFileName.toString.endsWith(".parquet")).toList
      assertTrue(written.size == 1, s"Expected exactly one parquet file for file id $fid")
      Files.move(written.head, target)
      val stats = parquetUtils.readColumnStatsFromMetadata(storage, new StoragePath(target.toString),
        INDEXED_COLUMNS.asJava, INDEX_VERSIONS(idx)).asScala.toSeq
      OracleFile(fid, partition, fileName, target.toString, stats)
    }

    val parquetDf = spark.read.schema(TABLE_SCHEMA).parquet(files.map(_.path): _*)
    val rows = withDim(Dim(ansi = false, "UTC"))(parquetDf.collect().toSeq)
    val rawDf = spark.createDataFrame(rows.asJava, TABLE_SCHEMA)

    // Missing stats, ground truth per (file, column) and (partition, column); the first dataset keeps
    // complete stats so that missing ones never mask a transform bug there
    def missing(prob: Double): Boolean = idx > 0 && rnd.nextDouble() < prob
    val unindexedFiles = files.map(_.fid).filter(_ => missing(MISSING_FILE_STATS_PROB)).toSet
    val missingFileStats = files.map(f => f.fid -> INDEXED_COLUMNS.filter(_ => missing(MISSING_COLUMN_STATS_PROB)).toSet).toMap
    val unindexedPartitions = partitions.indices.filter(_ => missing(MISSING_PARTITION_STATS_PROB)).toSet
    val missingPartitionStats = partitions.indices.map(p =>
      p -> INDEXED_COLUMNS.filter(_ => missing(MISSING_COLUMN_STATS_PROB)).toSet).toMap

    new OracleDataset(spark, idx, profile, INDEX_VERSIONS(idx), files, partitions, pools, rows, parquetDf, rawDf,
      unindexedFiles, missingFileStats, unindexedPartitions, missingPartitionStats)
  }

  // ---------------------------------------------------------------------------------------------
  // Reporting helpers
  // ---------------------------------------------------------------------------------------------

  private val nullBoundCache = mutable.HashMap[(String, Int, Dim, String), Set[Int]]()

  /**
   * Leaves of `pred` whose transform evaluates to NULL on the actual (not all-NULL) min or max stat of
   * `unit`, as opposed to stats that are missing altogether
   */
  private[hudi] def nullBoundLeaves(path: IndexPath, dim: Dim, ds: OracleDataset, pred: Pred, unit: Int): Seq[String] = {
    pred.leaves(negated = false).map(_._1.template).distinct.filter { t =>
      nullBoundCache.getOrElseUpdate((path.id, ds.idx, dim, t.fn(t.column)), {
        val (minCol, maxCol) = (s"${t.column}_minValue", s"${t.column}_maxValue")
        try {
          ds.statsDf(path, Seq(t.column), complete = true)
            .where(col(minCol).isNotNull && col(maxCol).isNotNull)
            .where(expr(t.fn(minCol)).isNull || expr(t.fn(maxCol)).isNull)
            .select(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME).collect().map(r => ds.unitOf(path, r.getString(0))).toSet
        } catch {
          case NonFatal(_) => Set.empty[Int]
        }
      }).contains(unit)
    }.map(t => t.fn(t.column))
  }

  private[hudi] def describe(ce: Counterexample): String = withDim(ce.dim) {
    val ds = ce.ds
    val path = if (ce.signature.startsWith(CSI.id)) CSI else PSI
    val referenced = collectReferencedColumns(spark, ce.filters, TABLE_SCHEMA)
    val fids = if (path == CSI) Seq(ce.unit) else ds.files.filter(f => ds.partitionOf(f.fid) == ce.unit).map(_.fid)
    val rows = ds.rows.filter(r => fids.contains(r.getInt(0)))
      .map(r => ("fid" +: referenced).map(c => s"$c=${display(r.getAs[Any](c))}").mkString("[", ", ", "]"))
    val indexFilter = try {
      ce.filters.map(translateIntoColumnStatsIndexFilterExpr(_, indexedCols = INDEXED_COLUMNS)).reduce(And).sql
    } catch {
      case NonFatal(e) => s"<translation failed: ${e.getMessage}>"
    }
    val stats = ds.statsRow(path, ce.unit, referenced, complete = false)
      .map(r => r.schema.fieldNames.zip(r.toSeq).map { case (k, v) => s"$k=${display(v)}" }.mkString(", "))
      .getOrElse("<not indexed>")
    val unitName = if (path == CSI) s"file ${ce.unit} (${ds.files(ce.unit).partition})" else s"partition ${ds.partitions(ce.unit)}"
    val missing = ds.missingColumns(path, ce.unit).intersect(referenced.toSet).toSeq.sorted
    val nullBound = nullBoundLeaves(path, ce.dim, ds, ce.pred, ce.unit)
    s"""[${ce.signature}]
       |    dims        : ansi=${ce.dim.ansi} tz=${ce.dim.timeZone} dataset=${ds.idx}(${ds.profile}, ${ds.indexVersion}) $unitName
       |    predicate   : ${ce.pred.sql}
       |    optimized   : ${ce.filters.map(_.sql).mkString(" AND ")}
       |    index filter: $indexFilter
       |    raw rows    : ${rows.mkString(" ")}
       |    stats       : $stats
       |    tags        : missing_stats=${if (missing.isEmpty) "none" else missing.mkString(",")} """.stripMargin +
      s"null_bound=${if (nullBound.isEmpty) "none" else nullBound.mkString(",")}"
  }

  /**
   * Prints, for every arm of the order-preserving whitelist (BaseHoodieCatalystExpressionUtils), which
   * templates exercise it and whether the optimized expression actually reaches the matcher on this Spark
   * version (RuntimeReplaceable expressions are rewritten before Hudi sees them)
   */
  private def checkArmCoverage(templates: Seq[ResolvedTemplate], ds: OracleDataset, print: Boolean): Seq[String] =
    withDim(Dim(ansi = false, "UTC")) {
      val exercised = mutable.LinkedHashMap[String, mutable.Buffer[String]]()
      val reached = mutable.LinkedHashMap[String, mutable.Buffer[String]]()
      templates.foreach { rt =>
        val column = COLUMNS.find(_.name == rt.template.column).get
        val label = rt.template.fn(column.name)
        rt.resolved.foreach(e => if (WHITELIST_ARMS.contains(e.getClass.getSimpleName)) {
          exercised.getOrElseUpdate(e.getClass.getSimpleName, mutable.Buffer()) += label
        })
        val value = ds.values(column.name).headOption.getOrElse(ds.pools(column.name).head)
        optimizedFilters(ds.parquetDf, s"(${rt.template.fn(column.name)}) = (${rt.template.fn(sqlLiteral(value, column.dataType))})")
          .toOption.toSeq.flatten
          .collect { case c: BinaryComparison => c.children.filter(_.references.nonEmpty) }
          .flatten
          .filter(side => AllowedTransformationExpression.unapply(side).isDefined)
          .foreach(_.foreach(e => if (WHITELIST_ARMS.contains(e.getClass.getSimpleName)) {
            reached.getOrElseUpdate(e.getClass.getSimpleName, mutable.Buffer()) += label
          }))
      }
      if (print) {
        println(s"[oracle] whitelist arm coverage (${templates.size} templates resolved):")
        WHITELIST_ARMS.foreach { arm =>
          val ex = exercised.get(arm).map(_.distinct).getOrElse(Seq.empty)
          val re = reached.get(arm).map(_.distinct).getOrElse(Seq.empty)
          val examples = (if (re.nonEmpty) re else ex).take(2).mkString("; ")
          println(f"    $arm%-18s exercised=${ex.size}%3d reached_matcher=${re.size}%3d e.g. $examples")
        }
      }
      val unexercised = WHITELIST_ARMS.filterNot(exercised.contains)
      if (unexercised.isEmpty) Seq.empty else Seq(s"whitelist arms not exercised by any template: ${unexercised.mkString(", ")}")
    }
}
