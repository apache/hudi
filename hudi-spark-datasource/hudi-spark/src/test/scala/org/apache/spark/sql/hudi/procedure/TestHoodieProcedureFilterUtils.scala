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

import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.command.procedures.HoodieProcedureFilterUtils
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}

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
    // The literal must match the column type, so use an explicit double literal here; the plain
    // 15.0 (decimal) form is pinned in the numeric-coercion test below.
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "price > 15.0d", scalarSchema))
    // Bare boolean column evaluates to a Boolean result directly.
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "flag", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "flag = true", scalarSchema))
  }

  test("evaluateFilter coerces Long columns against integer literals") {
    // Exercises applyTypeCoercion for every comparison operator (Long boundRef vs Int literal).
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "ts = 1000", scalarSchema))
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "ts > 1500", scalarSchema))
    assertResult(Seq(scalarRows(1)))(keep(scalarRows, "ts >= 2000", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "ts < 2000", scalarSchema))
    assertResult(Seq(scalarRows.head))(keep(scalarRows, "ts <= 1000", scalarSchema))
    // Known limitation: the coercion narrows the Long column to Int instead of widening the Int
    // literal, so a Long value beyond Int range never matches (wrong results under non-ANSI Spark,
    // swallowed overflow error under ANSI). Pinned here so a fix flips this assertion; see #19632.
    val bigRow = Seq(Row(3, "c3", 30.0d, 3000000000L, true, -9,
      Date.valueOf("2024-03-16"), Timestamp.valueOf("2024-03-16 12:30:00")))
    assertResult(Seq.empty)(keep(bigRow, "ts > 2000", scalarSchema))
    // Known limitation: the coercion only matches column-on-left, so a literal-on-left comparison
    // never coerces and drops every row instead of mirroring the equivalent column-on-left filter.
    // Pinned here so a fix flips these assertions; see #19632.
    assertResult(Seq.empty)(keep(scalarRows, "1500 < ts", scalarSchema))
    assertResult(Seq.empty)(keep(scalarRows, "1000 = ts", scalarSchema))
  }

  test("evaluateFilter does not coerce other numeric column/literal type pairs") {
    // Known limitation: applyTypeCoercion only special-cases a Long column against an Int literal.
    // Every other numeric column/literal pair is left alone, so the mismatched comparison fails to
    // evaluate; the per-row Try swallows the failure and drops the row. The filter therefore
    // returns no rows instead of erroring on the type mismatch.
    // Pinned here so a fix flips the Seq.empty assertions; see #19632.
    val schema = schemaOf(
      "f" -> FloatType,
      "sh" -> ShortType,
      "by" -> ByteType,
      "dec" -> DecimalType(10, 2))
    val rows = Seq(Row(2.5f, 3.toShort, 4.toByte, new java.math.BigDecimal("3.00")))

    // Literals whose parsed type already matches the column type evaluate correctly.
    assertResult(rows)(keep(rows, "f > 1.0f", schema))
    assertResult(rows)(keep(rows, "dec > 1.00", schema))

    // Mismatched literal types no-match even though the values would satisfy the predicate.
    assertResult(Seq.empty)(keep(rows, "sh = 3", schema))
    assertResult(Seq.empty)(keep(rows, "by = 4", schema))
    assertResult(Seq.empty)(keep(rows, "f > 1.0d", schema))
    assertResult(Seq.empty)(keep(rows, "dec > 1", schema))
    // Same gap for a double column: a plain 15.0 parses as decimal, not double.
    assertResult(Seq.empty)(keep(scalarRows, "price > 15.0", scalarSchema))
  }

  test("evaluateFilter silently drops rows for expressions it cannot resolve") {
    assertResult(Seq.empty)(keep(scalarRows, "concat(name, 'x') = 'a1x'", scalarSchema))
    assertResult(Seq.empty)(keep(scalarRows, "instr(name, 'a') = 1", scalarSchema))
    assertResult(Seq.empty)(keep(scalarRows, "if(name = 'a1', true, false)", scalarSchema))
    assertResult(Seq(scalarRows.head))(
      keep(scalarRows, "case when name = 'a1' then true else false end", scalarSchema))
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
    // The util only special-cases a long column vs an integer literal; every other numeric
    // comparison relies on the literal already matching the expression's result type. So the
    // literals below are typed to match: round/double yield double, ceil/floor/long yield long.
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
    import scala.collection.JavaConverters._
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
    import scala.collection.JavaConverters._
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
      new java.math.BigDecimal("12.50"),
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
    assert(validate("id = 1 OR concat(name, 'x') = 'a1x'").isLeft)
    assert(validate("hour(t) = 12").isLeft)
    assert(validate("date_format(t, 'yyyy') = '2024'").isLeft)
    assert(validate("any_value(id) = 1").isLeft)
    assert(validate("id = (select 1)").isLeft)

    assertResult(Right(()))(validate("upper(name) = 'A1'"))
  }
}
