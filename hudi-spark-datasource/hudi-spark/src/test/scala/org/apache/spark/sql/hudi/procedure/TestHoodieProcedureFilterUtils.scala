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

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.HoodieSparkUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.command.procedures.HoodieProcedureFilterUtils
import org.apache.spark.sql.types._

import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Timestamp}

import scala.collection.JavaConverters._

/**
 * Direct unit tests for [[HoodieProcedureFilterUtils]] which evaluates SQL filter
 * expressions against procedure output rows. Covers primitive/date/decimal/complex
 * type conversion, the function-resolution table, numeric type coercion, validation
 * and error handling.
 */
class TestHoodieProcedureFilterUtils extends HoodieSparkProcedureTestBase {

  private def schemaOf(fields: (String, DataType)*): StructType =
    StructType(fields.map { case (n, dt) => StructField(n, dt, nullable = true) })

  private def keep(rows: Seq[Row], expr: String, schema: StructType): Seq[Row] =
    HoodieProcedureFilterUtils.evaluateFilter(rows, expr, schema, spark)

  private def validate(expr: String, schema: StructType = scalarSchema): Either[String, Unit] =
    HoodieProcedureFilterUtils.validateFilterExpression(expr, schema, spark)

  // Not the scalarRows factory: only id and ts matter to the widening tests that use it.
  private def tsRow(id: Int, ts: Long): Row =
    Row(id, s"n$id", 10.0d * id, ts, true, -id,
      Date.valueOf("2024-01-01"), Timestamp.valueOf("2024-01-01 00:00:00"))

  // A rich scalar schema reused across the function tests.
  private val scalarSchema = schemaOf(
    "id" -> IntegerType,
    "name" -> StringType,
    "price" -> DoubleType,
    "ts" -> LongType,
    "flag" -> BooleanType,
    "neg" -> IntegerType,
    "d" -> DateType,
    "t" -> TimestampType)

  private val scalarRows = Seq(
    Row(1, "a1", 10.0d, 1000L, true, -5, Date.valueOf("2024-03-15"), Timestamp.valueOf("2024-03-15 12:30:00")),
    Row(2, "b2", 20.0d, 2000L, false, -7, Date.valueOf("2023-01-02"), Timestamp.valueOf("2023-01-02 05:00:00")))

  test("evaluateFilter returns all rows for null / blank filter") {
    assertResult(scalarRows)(keep(scalarRows, null, scalarSchema))
    assertResult(scalarRows)(keep(scalarRows, "", scalarSchema))
    assertResult(scalarRows)(keep(scalarRows, "   ", scalarSchema))
  }

  test("evaluateFilter handles comparison operators and boolean columns") {
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "id = 1", scalarSchema))
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "id > 1", scalarSchema))
    assertResult(2)(keep(scalarRows, "id >= 1", scalarSchema).length)
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "id < 2", scalarSchema))
    assertResult(2)(keep(scalarRows, "id <= 2", scalarSchema).length)
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "id != 1", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "name = 'a1'", scalarSchema))
    // A plain 15.0 decimal literal is coerced with the double column.
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "price > 15.0", scalarSchema))
    assertResult(Right(()))(validate("price > 15.0"))
    // Bare boolean column evaluates to a Boolean result directly.
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "flag", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "flag = true", scalarSchema))
  }

  test("evaluateFilter widens Long columns and integer literals") {
    // Exercises applyTypeCoercion for every comparison operator (Long boundRef vs Int literal).
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "ts = 1000", scalarSchema))
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "ts > 1500", scalarSchema))
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "ts >= 2000", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "ts < 2000", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "ts <= 1000", scalarSchema))
    // The integer literal is widened rather than the column narrowed, so a Long past the Int
    // range keeps its value instead of wrapping to -1294967296 and matching "ts < 2000".
    val bigRow = tsRow(3, 3000000000L)
    val withBigRow = scalarRows :+ bigRow
    assertResult(Seq(scalarRows(1), bigRow))(keep(withBigRow, "ts > 1500", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(withBigRow, "ts < 2000", scalarSchema))
    // Coercion applies symmetrically when the literal is on the left.
    assertResult(Seq(scalarRows(1), bigRow))(keep(withBigRow, "1500 < ts", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(withBigRow, "1000 = ts", scalarSchema))
    // IN and <=> widen through the same path as the binary comparisons.
    assertResult(scalarRows)(keep(withBigRow, "ts IN (1000, 2000)", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(withBigRow, "ts <=> 1000", scalarSchema))
    // An IN list of mixed integral widths widens to the single common type.
    assertResult(scalarRows)(keep(withBigRow, "ts IN (1000, 2000L)", scalarSchema))
    // Every filterable procedure calls validateFilterExpression before evaluateFilter, and each of
    // these shapes was rejected there before the operands widened.
    assertResult(Right(()))(validate("1500 < ts"))
    assertResult(Right(()))(validate("ts IN (1000, 2000)"))
    assertResult(Right(()))(validate("ts <=> 1000"))
    // A non-numeric operand still bails out of the widening and stays unresolved.
    assert(validate("id IN (1, 'x')").isLeft)
    // A null operand widens with the numeric ones, the plan Spark builds for the same filter.
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "ts IN (1000, null)", scalarSchema))
    assertResult(Right(()))(validate("ts IN (1000, null)"))
    assertResult(Seq.empty)(keep(scalarRows, "ts <=> null", scalarSchema))
    assertResult(Right(()))(validate("ts <=> null"))
    // A null operand takes its peers' type whether or not that type is numeric.
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "name IN ('a1', null)", scalarSchema))
    assertResult(Right(()))(validate("name IN ('a1', null)"))
    assertResult(Seq.empty)(keep(scalarRows, "name <=> null", scalarSchema))
    assertResult(Right(()))(validate("name <=> null"))
  }

  test("evaluateFilter coerces numeric column and literal type pairs") {
    val schema = schemaOf(
      "f" -> FloatType,
      "sh" -> ShortType,
      "by" -> ByteType,
      "dec" -> DecimalType(10, 2))
    // Two rows, so an assertion that matches everything is distinguishable from one that
    // coerces correctly.
    val rows = Seq(
      Row(2.5f, 3.toShort, 4.toByte, new JBigDecimal("3.00")),
      Row(0.5f, 9.toShort, 9.toByte, new JBigDecimal("0.50")))
    val matched = Seq(rows.head)

    // A literal whose parsed type already matches the column type evaluates correctly.
    assertResult(matched)(keep(rows, "f > 1.0f", schema))
    // A decimal literal of another precision or scale widens to the common decimal type, or the
    // comparison stays unresolved and validateFilterExpression rejects the filter.
    assertResult(matched)(keep(rows, "dec > 1.00", schema))
    assertResult(matched)(keep(rows, "dec > 1.5", schema))

    // Mismatched numeric types are widened to Spark's common type.
    assertResult(matched)(keep(rows, "sh = 3", schema))
    assertResult(matched)(keep(rows, "by = 4", schema))
    assertResult(matched)(keep(rows, "f > 1.0d", schema))
    assertResult(matched)(keep(rows, "dec > 1", schema))
    // Reversed operands widen the same way.
    assertResult(matched)(keep(rows, "3 = sh", schema))
    // 130 narrowed to a Byte is -126, so narrowing the literal would keep neither row.
    assertResult(rows)(keep(rows, "by < 130", schema))
    // The same pairs have to pass validation, which every filterable procedure runs first.
    assertResult(Right(()))(validate("sh = 3", schema))
    assertResult(Right(()))(validate("dec > 1", schema))
    assertResult(Right(()))(validate("dec > 1.00", schema))
  }

  test("evaluateFilter surfaces an ANSI error instead of dropping the row") {
    // An overflowing Long addition wraps to a negative without ANSI and raises with it, exactly
    // as it does in a query. The raise has to reach the caller: swallowing it into "no match"
    // would report an empty result for a filter the query answers with an error.
    val rows = Seq(tsRow(1, 1000L))
    withSQLConf("spark.sql.ansi.enabled" -> "false") {
      // 1000 + Long.MaxValue wraps to a negative, so the row genuinely fails "> 0".
      assertResult(Seq.empty)(keep(rows, "ts + 9223372036854775807 > 0", scalarSchema))
    }
    withSQLConf("spark.sql.ansi.enabled" -> "true") {
      intercept[ArithmeticException] {
        keep(rows, "ts + 9223372036854775807 > 0", scalarSchema)
      }
    }
    // An ANSI cast of a malformed string raises SparkNumberFormatException, not an
    // ArithmeticException, and has to reach the caller the same way.
    withSQLConf("spark.sql.ansi.enabled" -> "false") {
      assertResult(Seq.empty)(keep(scalarRows, "int(name) > 1", scalarSchema))
    }
    withSQLConf("spark.sql.ansi.enabled" -> "true") {
      intercept[NumberFormatException] {
        keep(scalarRows, "int(name) > 1", scalarSchema)
      }
    }
  }

  test("evaluateFilter follows ANSI mode when a decimal widening overflows") {
    val schema = schemaOf("big" -> DecimalType(38, 0), "frac" -> DecimalType(38, 18))
    val rows = Seq(Row(new JBigDecimal("1" + "0" * 30), new JBigDecimal("1.5")))
    // On Spark 3 the widening picks DECIMAL(38,18), and the precision-38 clamp leaves it 20
    // integral digits, too few for this 31-digit value, so the cast overflows and the ANSI mode
    // decides what happens. Spark 4 casts the fractional operand down instead, planning the same
    // filter as `big > cast(frac as decimal(38,0))`, so nothing overflows and the row survives in
    // either mode.
    if (!HoodieSparkUtils.gteqSpark4_0) {
      withSQLConf("spark.sql.ansi.enabled" -> "false") {
        // The cast yields null and the row drops.
        assertResult(Seq.empty)(keep(rows, "big > frac", schema))
      }
      withSQLConf("spark.sql.ansi.enabled" -> "true") {
        intercept[ArithmeticException] {
          keep(rows, "big > frac", schema)
        }
      }
    } else {
      for (ansi <- Seq("false", "true")) {
        withSQLConf("spark.sql.ansi.enabled" -> ansi) {
          assertResult(rows)(keep(rows, "big > frac", schema))
          assertResult(Right(()))(validate("big > frac", schema))
        }
      }
    }
  }

  test("evaluateFilter widens with the coercion rules of the active ANSI mode") {
    // The two coercion objects disagree on BIGINT with FLOAT: AnsiTypeCoercion widens to DOUBLE,
    // TypeCoercion follows numericPrecedence to FLOAT. 16777217 is the first Long that a Float
    // cannot hold, so it survives the widening under ANSI and rounds down to 16777216.0f without
    // it. A filter has to widen the way the same comparison would in a query.
    val rows = Seq(tsRow(1, 16777217L))
    withSQLConf("spark.sql.ansi.enabled" -> "true") {
      assertResult(rows)(keep(rows, "ts > 16777216.0f", scalarSchema))
    }
    withSQLConf("spark.sql.ansi.enabled" -> "false") {
      assertResult(Seq.empty)(keep(rows, "ts > 16777216.0f", scalarSchema))
      // The rounded-down Long still clears a smaller Float, so the widening runs either way.
      assertResult(rows)(keep(rows, "ts > 16777215.0f", scalarSchema))
    }
    // Column against column splits the same way, and only the widening reaches it.
    val pairSchema = schemaOf("l" -> LongType, "f" -> FloatType)
    val pairRows = Seq(Row(16777217L, 16777216.0f))
    withSQLConf("spark.sql.ansi.enabled" -> "true") {
      assertResult(pairRows)(keep(pairRows, "l > f", pairSchema))
    }
    withSQLConf("spark.sql.ansi.enabled" -> "false") {
      assertResult(Seq.empty)(keep(pairRows, "l > f", pairSchema))
    }
  }

  test("evaluateFilter widens arithmetic and coalesce operands") {
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "ts + 1 > 1500", scalarSchema))
    assertResult(Right(()))(validate("ts + 1 > 1500"))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "coalesce(ts, 0) = 1000", scalarSchema))
    assertResult(Right(()))(validate("coalesce(ts, 0) = 1000"))
    // Divide accepts only Double or Decimal, so an integral pair has to become Double rather
    // than a wider integral. Otherwise "ts / 2" stays unresolved while "price / 2" resolves.
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "ts / 2 > 500", scalarSchema))
    assertResult(Right(()))(validate("ts / 2 > 500"))
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "price / 2 > 5", scalarSchema))
    assertResult(Right(()))(validate("price / 2 > 5"))
    // Spark's rule admits a null operand on either side, so these resolve and evaluate to null
    // rather than being rejected as an unsupported filter expression.
    assertResult(Seq.empty)(keep(scalarRows, "ts / null > 0", scalarSchema))
    assertResult(Right(()))(validate("ts / null > 0"))
    assertResult(Seq.empty)(keep(scalarRows, "null / ts > 0", scalarSchema))
    assertResult(Right(()))(validate("null / ts > 0"))
    // The remaining arithmetic operators go through the same widening.
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "ts % 3 = 1", scalarSchema))
    assertResult(Right(()))(validate("ts % 3 = 1"))
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "ts - 1 > 1500", scalarSchema))
    assertResult(Right(()))(validate("ts - 1 > 1500"))
    // IntegralDivide accepts only Long or Decimal and never widens its operands against each
    // other, so an Int pair has to be promoted a side at a time the way Spark's rule does.
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "ts div 3 > 500", scalarSchema))
    assertResult(Right(()))(validate("ts div 3 > 500"))
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "id div 2 > 0", scalarSchema))
    assertResult(Right(()))(validate("id div 2 > 0"))
    // Coalesce widens across more than two children, and across widths as well as kinds.
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "coalesce(ts, id, 0) = 1000", scalarSchema))
    assertResult(Right(()))(validate("coalesce(ts, id, 0) = 1000"))
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "coalesce(ts, price) > 1500", scalarSchema))
    assertResult(Right(()))(validate("coalesce(ts, price) > 1500"))
    // div is the operator this coercion newly reaches, so check it against Spark itself.
    val df = spark.createDataFrame(scalarRows.asJava, scalarSchema)
    Seq("id div 2 > 0", "ts div 3 > 500").foreach { filter =>
      withClue(s"filter=$filter: ") {
        assertResult(df.filter(filter).collect().toSeq)(keep(scalarRows, filter, scalarSchema))
      }
    }
  }

  test("evaluateFilter matches Spark for mixed decimal arithmetic") {
    val schema = schemaOf("dec" -> DecimalType(38, 18), "i" -> IntegerType, "f" -> FloatType)
    val rows = Seq(
      Row(new JBigDecimal("0.0000001"), 1, 0.5f),
      Row(new JBigDecimal("-2.5"), 2, 0.25f))
    val filters = Seq(
      "dec + 1 > 0", "1 + dec > 0", "dec * 1 > 0", "1L * dec > 0",
      "dec + i > 0", "i * dec > 0", "dec / 2 > 0", "2 / dec > 0",
      "dec + 0.5f > 0", "0.5f + dec > 0", "dec * f > 0", "f / dec > 0",
      "dec + 0.5d > 0", "0.5d / dec > 0",
      "dec / null > 0", "null / dec > 0", "dec + null > 0", "null * dec > 0",
      // DECIMAL(38,18) * DECIMAL(2,1) gives a scale-16 product that still holds 0.0000001, which
      // widening both operands to DECIMAL(38,18) first would round away to zero.
      "dec * 1.0 > 0.0")
    for (ansi <- Seq("false", "true"); minimumPrecision <- Seq("false", "true")) {
      withSQLConf("spark.sql.ansi.enabled" -> ansi,
        "spark.sql.legacy.literal.pickMinimumPrecision" -> minimumPrecision) {
        val df = spark.createDataFrame(rows.asJava, schema)
        filters.foreach { filter =>
          withClue(s"filter=$filter, ansi=$ansi, minimumPrecision=$minimumPrecision: ") {
            assertResult(Right(()))(validate(filter, schema))
            assertResult(df.filter(filter).collect().toSeq)(keep(rows, filter, schema))
          }
        }
      }
    }
  }

  test("evaluateFilter matches Spark for high-precision decimal comparisons") {
    val schema = schemaOf("ts" -> LongType, "dec" -> DecimalType(38, 30))
    val rows = Seq(
      Row(3000000000L, new JBigDecimal("0.00000000000000000000000000001")),
      Row(0L, new JBigDecimal("0")),
      Row(-3000000000L, new JBigDecimal("-0.00000000000000000000000000001")))
    val tiny = "0.000000000000000000000000000001"
    // A decimal literal past the Long range is folded to a constant by DecimalPrecision, so the
    // comparison never reaches the widening.
    val huge = "99999999999999999999.0"
    val filters = Seq(
      s"ts > $tiny", s"ts >= $tiny", s"ts < $tiny", s"ts <= $tiny",
      s"$tiny < ts", s"$tiny <= ts", s"$tiny > ts", s"$tiny >= ts",
      s"ts > $huge", s"ts < $huge", s"$huge < ts",
      "dec > 0", "0 < dec", "dec = 0", "dec <=> 0", "dec <= 0")
    // spark.sql.legacy.decimal.retainFractionDigitsOnTruncate is undefined before Spark 4.
    val retainFractionSettings = if (HoodieSparkUtils.gteqSpark4_0) Seq("false", "true") else Seq("false")
    for (ansi <- Seq("false", "true"); minimumPrecision <- Seq("false", "true");
         retainFraction <- retainFractionSettings) {
      withSQLConf("spark.sql.ansi.enabled" -> ansi,
        "spark.sql.legacy.literal.pickMinimumPrecision" -> minimumPrecision,
        "spark.sql.legacy.decimal.retainFractionDigitsOnTruncate" -> retainFraction) {
        val df = spark.createDataFrame(rows.asJava, schema)
        filters.foreach { filter =>
          withClue(s"filter=$filter, ansi=$ansi, minimumPrecision=$minimumPrecision, retainFraction=$retainFraction: ") {
            assertResult(Right(()))(validate(filter, schema))
            assertResult(df.filter(filter).collect().toSeq)(keep(rows, filter, schema))
          }
        }
      }
    }
  }

  test("evaluateFilter binds quoted column names") {
    // show_column_stats_overlap, the second procedure named in #19632, outputs columns like
    // "Average overlap" and "50% overlap".
    val schema = schemaOf("Average overlap" -> DoubleType, "50% overlap" -> IntegerType)
    val rows = Seq(Row(0.75d, 10), Row(0.25d, 20))
    assertResult(Seq(rows.head))(keep(rows, "`Average overlap` > 0.5", schema))
    assertResult(Right(()))(validate("`Average overlap` > 0.5", schema))
    assertResult(Seq(rows(1)))(keep(rows, "`50% overlap` > 15", schema))
  }

  test("evaluateFilter silently drops rows for expressions it cannot resolve") {
    assertResult(Seq.empty)(keep(scalarRows, "concat(name, 'x') = 'a1x'", scalarSchema))
    assertResult(Seq.empty)(keep(scalarRows, "instr(name, 'a') = 1", scalarSchema))
    assertResult(Seq.empty)(keep(scalarRows, "if(name = 'a1', true, false)", scalarSchema))
    assertResult(Seq(scalarRows.head))(
      keep(scalarRows, "case when name = 'a1' then true else false end", scalarSchema))
    // Or short-circuits on the resolved side, which is what the unresolved-operand guard preserves.
    assertResult(Seq(scalarRows.head))(
      keep(scalarRows, "id = 1 OR concat(name, 'x') = 'a1x'", scalarSchema))
  }

  test("evaluateFilter handles AND / OR / NOT / IN / BETWEEN") {
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "id = 1 AND flag = true", scalarSchema))
    assertResult(2)(keep(scalarRows, "id = 1 OR id = 2", scalarSchema).length)
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "NOT (id = 1)", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "id IN (1, 3)", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "name IN ('a1', 'x')", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "id BETWEEN 1 AND 1", scalarSchema))
  }

  test("evaluateFilter resolves string functions") {
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "upper(name) = 'A1'", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "lower(name) = 'a1'", scalarSchema))
    assertResult(2)(keep(scalarRows, "length(name) = 2", scalarSchema).length)
    assertResult(2)(keep(scalarRows, "len(name) = 2", scalarSchema).length)
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "trim(name) = 'a1'", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "ltrim(name) = 'a1'", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "rtrim(name) = 'a1'", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "substr(name, 1, 1) = 'a'", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "substring(name, 1, 1) = 'a'", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "name LIKE 'a%'", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "regexp_like(name, '^a')", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "regexp_extract(name, '([a-z]+)', 1) = 'a'", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "coalesce(name, 'x') = 'a1'", scalarSchema))
  }

  test("evaluateFilter resolves numeric and cast functions") {
    // Literals are typed to match each function's result type (round/double yield double,
    // ceil/floor/long yield long); the widening above would cover a mismatch either way.
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "abs(neg) = 5", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "round(price) = 10.0d", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "round(price, 1) = 10.0d", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "ceil(price) = 10L", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "floor(price) = 10L", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "int(price) = 10", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "long(id) = 1L", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "double(id) = 1.0d", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "string(id) = '1'", scalarSchema))
  }

  test("evaluateFilter resolves date functions") {
    // These operate on the date column and need no session time zone, so they evaluate directly.
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "year(d) = 2024", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "month(d) = 3", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "day(d) = 15", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "dayofmonth(d) = 15", scalarSchema))
    assertResult(2)(keep(scalarRows, "datediff(d, d) = 0", scalarSchema).length)
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "datediff(d, date'2024-03-14') = 1", scalarSchema))
  }

  test("evaluateFilter cannot evaluate time-zone-aware timestamp functions") {
    // The util binds and evaluates expressions without running Spark's analyzer, so time-zone-aware
    // expressions never receive a resolved time zone and fail to evaluate; the filter then treats the
    // row as a non-match rather than throwing. This documents that hour()/date_format() on a timestamp
    // are unsupported here (unlike the date functions above, which are time-zone independent).
    assertResult(Seq.empty)(keep(scalarRows, "hour(t) = 12", scalarSchema))
    assertResult(Seq.empty)(keep(scalarRows, "date_format(t, 'yyyy') = '2024'", scalarSchema))
  }

  test("evaluateFilter maps a string result of true / false to a boolean decision") {
    // string(flag) yields the literal strings "true"/"false", exercising the string->boolean branch.
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "string(flag)", scalarSchema))
    // The mapping is for direct callers: Spark rejects a string filter condition, so a filterable
    // procedure never gets past validation with one.
    assert(validate("string(flag)").isLeft)
  }

  test("evaluateFilter treats non-boolean-valued expressions as no-match") {
    // A bare string/int column produces a value that is neither Boolean nor "true"/"false".
    assertResult(Seq.empty)(keep(scalarRows, "name", scalarSchema))
    assertResult(Seq.empty)(keep(scalarRows, "id", scalarSchema))
  }

  test("evaluateFilter handles null values and IS [NOT] NULL") {
    val schema = schemaOf("id" -> IntegerType, "name" -> StringType)
    val rows = Seq(Row(1, "a1"), Row(2, null))
    assertResult(Seq(rows(1)))(keep(rows, "isnull(name)", schema))
    assertResult(Seq(rows.head))(keep(rows, "isnotnull(name)", schema))
    assertResult(Seq(rows(1)))(keep(rows, "name IS NULL", schema))
    assertResult(Seq(rows.head))(keep(rows, "name IS NOT NULL", schema))
    // A null-valued bare expression resolves to a false decision.
    assertResult(Seq.empty)(keep(Seq(rows(1)), "name", schema))
  }

  test("evaluateFilter converts map columns and resolves map functions") {
    val schema = schemaOf("id" -> IntegerType,
      "mScala" -> MapType(StringType, IntegerType),
      "mJava" -> MapType(StringType, IntegerType))
    val javaMap = Map("a" -> 1, "b" -> 2).asJava
    val rows = Seq(Row(1, Map("a" -> 1, "b" -> 2), javaMap))
    assertResult(rows)(keep(rows, "size(mScala) = 2", schema))
    assertResult(rows)(keep(rows, "size(mJava) = 2", schema))
    assertResult(rows)(keep(rows, "array_contains(map_keys(mScala), 'a')", schema))
    assertResult(rows)(keep(rows, "array_contains(map_values(mScala), 1)", schema))
    assertResult(Seq.empty)(keep(rows, "size(mScala) = 5", schema))
  }

  test("evaluateFilter converts struct columns and resolves IS [NOT] NULL on them") {
    val schema = schemaOf("id" -> IntegerType,
      "st" -> StructType(Seq(StructField("x", IntegerType), StructField("y", StringType))))
    val rows = Seq(Row(1, Row(10, "p")), Row(2, null))
    assertResult(Seq(rows.head))(keep(rows, "isnotnull(st)", schema))
    assertResult(Seq(rows(1)))(keep(rows, "isnull(st)", schema))
  }

  test("evaluateFilter converts array / decimal / binary / uuid / java-time columns without error") {
    val schema = schemaOf(
      "id" -> IntegerType,
      "arrScala" -> ArrayType(IntegerType),
      "arrList" -> ArrayType(IntegerType),
      "arrArray" -> ArrayType(IntegerType),
      "dec" -> DecimalType(10, 2),
      "decScala" -> DecimalType(10, 2),
      "bin" -> BinaryType,
      "uuidCol" -> StringType,
      "inst" -> TimestampType,
      "ld" -> DateType,
      "ldt" -> TimestampType)
    val row = Row(
      1,
      Seq(1, 2, 3),
      List(1, 2, 3).map(Int.box).asJava,
      Array(1, 2, 3),
      new JBigDecimal("12.50"),
      scala.math.BigDecimal("34.75"),
      Array[Byte](1, 2, 3),
      java.util.UUID.randomUUID(),
      java.time.Instant.parse("2024-03-15T12:30:00Z"),
      java.time.LocalDate.of(2024, 3, 15),
      java.time.LocalDateTime.of(2024, 3, 15, 12, 30, 0))
    val rows = Seq(row)
    // Filtering on the scalar column converts every field of the row, exercising each
    // complex-type conversion branch; the row is retained.
    assertResult(rows)(keep(rows, "id = 1", schema))
    // Decimal / binary / uuid / java-time values survive conversion and are non-null.
    assertResult(rows)(keep(rows, "isnotnull(dec) AND isnotnull(decScala)", schema))
    assertResult(rows)(keep(rows, "isnotnull(bin) AND isnotnull(uuidCol)", schema))
    assertResult(rows)(keep(rows, "isnotnull(inst) AND isnotnull(ld) AND isnotnull(ldt)", schema))
    // Known limitation: array values are converted to a plain Array instead of Catalyst ArrayData,
    // so every array predicate (even isnotnull) fails to evaluate and drops the row instead of
    // matching. Pinned here so a fix flips these assertions; see #19633.
    assertResult(Seq.empty)(keep(rows, "size(arrScala) >= 0", schema))
    assertResult(Seq.empty)(keep(rows, "isnotnull(arrScala)", schema))
  }

  test("evaluateFilter throws IllegalArgumentException on unparseable expression") {
    val e = intercept[IllegalArgumentException] {
      keep(scalarRows, "id >< 1", scalarSchema)
    }
    assert(e.getMessage.contains("Failed to parse or evaluate filter expression"))
  }

  test("validateFilterExpression accepts valid references and rejects unknown ones") {
    assertResult(Right(()))(
      validate("id > 1 AND name = 'a1'"))
    assertResult(Right(()))(
      validate("ts >= 0 AND ts BETWEEN 0 AND 999999"))
    assertResult(Right(()))(
      validate(null))
    assertResult(Right(()))(
      validate("   "))

    val invalidCol = validate("missing_col > 1")
    assert(invalidCol.isLeft)
    val invalidColMsg = invalidCol.fold(identity, _ => "")
    assert(invalidColMsg.contains("Invalid column references"))
    assert(invalidColMsg.contains("missing_col"))

    val parseError = validate("id >< 1")
    assert(parseError.isLeft)
    assert(parseError.fold(identity, _ => "").contains("Invalid filter expression"))
  }

  test("validateFilterExpression rejects expressions the evaluator cannot resolve") {
    val unknown = validate("concat(name, 'x') = 'a1x' OR instr(name, 'a') = 1")
    assert(unknown.left.exists(_.contains("Unsupported functions: concat, instr")))

    assert(validate("if(name = 'a1', true, false)").isLeft)
    assert(validate("substring(name, 2)").isLeft)
    assert(validate("id = 1 OR concat(name, 'x') = 'a1x'").left.exists(_.contains("Unsupported functions: concat")))
    assert(validate("hour(t) = 12").isLeft)
    assert(validate("date_format(t, 'yyyy') = '2024'").isLeft)
    assert(validate("any_value(id) = 1").isLeft)
    assert(validate("id = (select 1)").isLeft)
    // Spark rejects any non-boolean filter condition. Without this these resolve and report zero
    // matching rows instead of the error the same query raises.
    assert(validate("ts + 1").left.exists(_.contains("boolean")))
    assert(validate("name").left.exists(_.contains("boolean")))

    assertResult(Right(()))(validate("upper(name) = 'A1'"))
  }
}
