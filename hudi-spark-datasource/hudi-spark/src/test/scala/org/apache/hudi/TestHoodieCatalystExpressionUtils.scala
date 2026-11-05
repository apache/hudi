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

import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, BitwiseOr, Cast, DateAdd, DateDiff, DateFormatClass, DateSub, Divide, Exp, Expression, FromUnixTime, FromUTCTimestamp, GetTimestamp, Literal, Log, Lower, Multiply, ParseToDate, ParseToTimestamp, Rand, ShiftLeft, Sqrt, ToUnixTimestamp, ToUTCTimestamp, UnixTimestamp, Upper}
import org.apache.spark.sql.hudi.dataskipping.{ColumnStatsSource, TermRange}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test

/**
 * Branch coverage for the order-preserving rule table in
 * [[org.apache.spark.sql.hudi.dataskipping.RangeRules]]. This is what lets data skipping map a
 * transformed column reference back to its source attribute, so each case is pinned to the exact
 * source [[AttributeReference]] it must recover, and non-order-preserving shapes must not match.
 */
class TestHoodieCatalystExpressionUtils extends SparkAdapterSupport {

  private val intAttr = AttributeReference("i", IntegerType)()
  private val strAttr = AttributeReference("s", StringType)()
  private val dblAttr = AttributeReference("d", DoubleType)()
  private val dateAttr = AttributeReference("dt", DateType)()
  private val longAttr = AttributeReference("l", LongType)()
  private val decAttr = AttributeReference("dec", DecimalType(10, 2))()
  private val tsAttr = AttributeReference("ts", TimestampType)()
  private val boolAttr = AttributeReference("b", BooleanType)()
  private val binAttr = AttributeReference("bin", BinaryType)()
  // TimestampNTZType is private[spark] on Spark 3.3, where TIMESTAMP_NTZ is unsupported, so it is
  // parsed from DDL and only built on Spark 3.4+.
  private lazy val ntzAttr = AttributeReference("ntz", DataType.fromDDL("timestamp_ntz"))()

  /**
   * The column `expr` is ranged through, found by the production range walk ([[TermRange.derive]])
   * with one of its columns indexed at a time.
   */
  private def matched(expr: Expression): Option[AttributeReference] =
    expr.collect { case attrRef: AttributeReference => attrRef }.distinct
      .find(attrRef => TermRange.derive(expr, ColumnStatsSource(Seq(attrRef.name))).isDefined)

  @Test
  def testIdentityAttributeMatches(): Unit = {
    assertEquals(Some(intAttr), matched(intAttr))
  }

  @Test
  def testArithmeticTransformationsPreserveOrdering(): Unit = {
    assertEquals(Some(intAttr), matched(Add(intAttr, Literal(1))))
    assertEquals(Some(intAttr), matched(Add(Literal(1), intAttr)))
    assertEquals(Some(intAttr), matched(Multiply(intAttr, Literal(2))))
    assertEquals(Some(intAttr), matched(Multiply(Literal(2), intAttr)))
    assertEquals(Some(intAttr), matched(Divide(intAttr, Literal(2))))
    assertEquals(Some(intAttr), matched(BitwiseOr(intAttr, Literal(1))))
    assertEquals(Some(intAttr), matched(BitwiseOr(Literal(1), intAttr)))
    assertEquals(Some(intAttr), matched(ShiftLeft(intAttr, Literal(1))))
  }

  @Test
  def testMultiplyDivideRequirePositiveLiteralFactor(): Unit = {
    // Negative factors reverse ordering.
    assertEquals(None, matched(Multiply(dblAttr, Literal(-1.0d))))
    assertEquals(None, matched(Multiply(Literal(-1), intAttr)))
    assertEquals(None, matched(Divide(dblAttr, Literal(Decimal(-2), DecimalType(1, 0)))))
    // Zero collapses ordering and makes division undefined.
    assertEquals(None, matched(Multiply(intAttr, Literal(0L))))
    assertEquals(None, matched(Divide(dblAttr, Literal(0.0d))))
    // NaN fails the positive check; typed null literals carry a null value.
    assertEquals(None, matched(Multiply(dblAttr, Literal(Double.NaN))))
    // Infinite factors can yield NaN (0 * Infinity, Infinity / Infinity).
    assertEquals(None, matched(Multiply(dblAttr, Literal(Double.PositiveInfinity))))
    assertEquals(None, matched(Multiply(Literal(Float.PositiveInfinity), dblAttr)))
    assertEquals(None, matched(Divide(dblAttr, Literal(Double.PositiveInfinity))))
    assertEquals(None, matched(Multiply(intAttr, Literal(null, IntegerType))))
    // Non-literal factors are not matched, even when reference-free or the column itself.
    assertEquals(None, matched(Multiply(intAttr, Add(Literal(1), Literal(1)))))
    assertEquals(None, matched(Multiply(intAttr, intAttr)))
    // Strictly positive literals of any numeric type match in the supported operand positions.
    assertEquals(Some(dblAttr), matched(Multiply(dblAttr, Literal(2.5d))))
    assertEquals(Some(longAttr), matched(Multiply(Literal(3L), longAttr)))
    assertEquals(Some(dblAttr), matched(Divide(dblAttr, Literal(Decimal(2), DecimalType(1, 0)))))
  }

  @Test
  def testAddRequiresConstantOtherOperand(): Unit = {
    // A + A * -2 = -A reverses ordering, so an operand referencing the column must not match.
    assertEquals(None, matched(Add(dblAttr, Multiply(dblAttr, Literal(-2.0d)))))
    assertEquals(None, matched(Add(Multiply(dblAttr, Literal(-2.0d)), dblAttr)))
    assertEquals(None, matched(Add(intAttr, intAttr)))
    // Data skipping re-evaluates the operand against the min/max bounds, so it must be a constant.
    assertEquals(None, matched(Add(dblAttr, Rand(Literal(0L)))))
    assertEquals(None, matched(Add(Rand(Literal(0L)), dblAttr)))
  }

  @Test
  def testUnaryMathTransformationsPreserveOrdering(): Unit = {
    assertEquals(Some(dblAttr), matched(Exp(dblAttr)))
    assertEquals(Some(dblAttr), matched(Log(dblAttr)))
  }

  @Test
  def testDateTransformationsPreserveOrdering(): Unit = {
    assertEquals(Some(dateAttr), matched(DateAdd(dateAttr, Literal(1))))
    assertEquals(Some(dateAttr), matched(DateSub(dateAttr, Literal(1))))
    // DateDiff(end, start) = end - start preserves the ordering of its end date.
    assertEquals(Some(dateAttr), matched(DateDiff(dateAttr, Literal(java.sql.Date.valueOf("2022-03-07")))))
  }

  @Test
  def testUtcTimestampConversionsRequireFixedOffsetZone(): Unit = {
    // A zone with DST runs local time backwards across a fall-back and shifts the spring-forward gap.
    assertEquals(None, matched(FromUTCTimestamp(tsAttr, Literal("America/New_York"))))
    assertEquals(None, matched(ToUTCTimestamp(tsAttr, Literal("America/New_York"))))
    // A fixed-offset zone shifts every timestamp by the same amount.
    assertEquals(Some(tsAttr), matched(FromUTCTimestamp(tsAttr, Literal("+08:00"))))
    assertEquals(Some(tsAttr), matched(ToUTCTimestamp(tsAttr, Literal("UTC"))))
    // Zones are resolved the way Spark evaluates them: Etc/GMT+5 is a fixed -05:00, and +8:00 is +08:00.
    assertEquals(Some(tsAttr), matched(FromUTCTimestamp(tsAttr, Literal("Etc/GMT+5"))))
    assertEquals(Some(tsAttr), matched(ToUTCTimestamp(tsAttr, Literal("+8:00"))))
    // Non-literal, null and unknown zones are not matched.
    assertEquals(None, matched(FromUTCTimestamp(tsAttr, strAttr)))
    assertEquals(None, matched(FromUTCTimestamp(tsAttr, Literal(null, StringType))))
    assertEquals(None, matched(ToUTCTimestamp(tsAttr, Literal("Not/AZone"))))
    // A zone with historical offset changes but no DST is not fixed-offset either; GMT+8 is.
    assertEquals(None, matched(FromUTCTimestamp(tsAttr, Literal("Asia/Kolkata"))))
    assertEquals(Some(tsAttr), matched(ToUTCTimestamp(tsAttr, Literal("GMT+8"))))
  }

  @Test
  def testDateParsingTransformationsPreserveOrdering(): Unit = {
    // to_date / to_timestamp reach data skipping as their replacement: GetTimestamp, whose arity
    // differs across Spark versions, so DateTimeFormatRule matches it by type. The ParseTo* 2-arg
    // auxiliary constructors are stable on all profiles, letting this pin that match everywhere
    def toTimestamp(input: Expression, format: String): Expression =
      new ParseToTimestamp(input, Literal(format)).withTimeZone("UTC").asInstanceOf[ParseToTimestamp].replacement
    def toDate(input: Expression, format: String): Expression =
      new ParseToDate(input, Literal(format)).withTimeZone("UTC").asInstanceOf[ParseToDate].replacement

    assertTrue(toTimestamp(strAttr, "yyyy-MM-dd").isInstanceOf[GetTimestamp])
    assertEquals(Some(strAttr), matched(toTimestamp(strAttr, "yyyy-MM-dd HH:mm:ss")))
    // to_date wraps GetTimestamp in a cast to date; the range walks through both
    assertEquals(Some(strAttr), matched(toDate(strAttr, "yyyy-MM-dd")))
    // The pre-optimizer shapes are not matched: production filters are optimized first
    assertEquals(None, matched(new ParseToDate(strAttr, Literal("yyyy-MM-dd"))))
    // Lexicographic order of 'MM/dd/yyyy' or unpadded strings is not chronological
    assertEquals(None, matched(toTimestamp(strAttr, "MM/dd/yyyy")))
    assertEquals(None, matched(toDate(strAttr, "yyyy-M-d")))
    // Converting a timestamp or date does not use the format
    assertEquals(Some(tsAttr), matched(toTimestamp(tsAttr, "MM/dd/yyyy")))
    assertEquals(Some(dateAttr), matched(toTimestamp(dateAttr, "MM/dd/yyyy")))

    // unix_timestamp and to_unix_timestamp take the same guards (parity, #19481)
    assertEquals(Some(strAttr), matched(UnixTimestamp(strAttr, Literal("yyyy-MM-dd"), Some("UTC"))))
    assertEquals(Some(strAttr), matched(ToUnixTimestamp(strAttr, Literal("yyyy-MM-dd"), Some("UTC"))))
    assertEquals(None, matched(UnixTimestamp(strAttr, Literal("MM/dd/yyyy"), Some("UTC"))))
    assertEquals(None, matched(ToUnixTimestamp(strAttr, Literal("MM/dd/yyyy"), Some("UTC"))))
    // A time-of-day parse is refused in a zone with a spring-forward gap, a date-only one is not
    assertEquals(None, matched(UnixTimestamp(strAttr, Literal("yyyy-MM-dd HH:mm:ss"), Some("America/Los_Angeles"))))
    assertEquals(Some(strAttr), matched(UnixTimestamp(strAttr, Literal("yyyy-MM-dd"), Some("America/Los_Angeles"))))
    // No time zone, no step
    assertEquals(None, matched(UnixTimestamp(strAttr, Literal("yyyy-MM-dd"), None)))
    assertEquals(Some(tsAttr), matched(UnixTimestamp(tsAttr, Literal("MM/dd/yyyy"), Some("UTC"))))
  }

  @Test
  def testDateFormattingPreservesOrdering(): Unit = {
    assertEquals(Some(tsAttr), matched(DateFormatClass(tsAttr, Literal("yyyy-MM-dd HH:mm"), Some("UTC"))))
    assertEquals(None, matched(DateFormatClass(tsAttr, Literal("MM/dd/yyyy"), Some("UTC"))))
    assertEquals(Some(longAttr), matched(FromUnixTime(longAttr, Literal("yyyy-MM-dd HH:mm:ss"), Some("UTC"))))
    assertEquals(None, matched(FromUnixTime(longAttr, Literal("dd-MM-yyyy"), Some("UTC"))))
  }

  @Test
  def testTimeOfDayPatternsRequireFixedOffsetSessionZone(): Unit = {
    val newYork = Some("America/New_York")
    // A time-of-day or offset field reorders values across a DST fall-back of the session zone.
    assertEquals(None, matched(DateFormatClass(tsAttr, Literal("yyyy-MM-dd HH:mm"), newYork)))
    assertEquals(None, matched(FromUnixTime(longAttr, Literal("yyyy-MM-dd HH:mm:ss"), newYork)))
    assertEquals(None, matched(DateFormatClass(tsAttr, Literal("yyyy-MM-dd XXX"), newYork)))
    // So does every pattern letter that is not a date field, each on its own: hours, am-pm, day period, minutes,
    // seconds, fractions, zone names and offsets.
    "aBhKkHmsSAnNVvzOXxZ".foreach { letter =>
      assertEquals(None, matched(DateFormatClass(tsAttr, Literal(s"yyyy-MM-dd $letter"), newYork)), s"letter $letter")
    }
    // A field outside the quotes or after an escaped quote is still a field.
    assertEquals(None, matched(DateFormatClass(tsAttr, Literal("yyyy-MM-dd 'at' HH 'h'"), newYork)))
    assertEquals(None, matched(DateFormatClass(tsAttr, Literal("yyyy''HH"), newYork)))
    // A date-only pattern renders the local date, which never runs backwards in New York; neither quoted text nor
    // a non-ASCII character is a field.
    assertEquals(Some(tsAttr), matched(DateFormatClass(tsAttr, Literal("yyyy-MM-dd"), newYork)))
    assertEquals(Some(longAttr), matched(FromUnixTime(longAttr, Literal("yyyy-MM-dd 'at' 'H'"), newYork)))
    assertEquals(Some(tsAttr), matched(DateFormatClass(tsAttr, Literal("yyyy\u5e74MM\u6708dd\u65e5"), newYork)))
    // Any pattern keeps the ordering in a session zone whose offset never changes.
    assertEquals(Some(tsAttr), matched(DateFormatClass(tsAttr, Literal("yyyy-MM-dd HH:mm"), Some("UTC"))))
    assertEquals(Some(longAttr), matched(FromUnixTime(longAttr, Literal("yyyy-MM-dd HH:mm:ss"), Some("+08:00"))))
    // PST resolves to America/Los_Angeles; Kolkata has no DST but historical offset changes.
    assertEquals(None, matched(DateFormatClass(tsAttr, Literal("yyyy-MM-dd HH:mm"), Some("PST"))))
    assertEquals(None, matched(DateFormatClass(tsAttr, Literal("yyyy-MM-dd HH:mm"), Some("Asia/Kolkata"))))
    // A non-literal or null pattern, an unknown zone or an unresolved session zone is not matched.
    assertEquals(None, matched(DateFormatClass(tsAttr, strAttr, newYork)))
    assertEquals(None, matched(DateFormatClass(tsAttr, Literal(null, StringType), newYork)))
    assertEquals(None, matched(DateFormatClass(tsAttr, Literal("yyyy-MM-dd HH:mm"), Some("Not/AZone"))))
    assertEquals(None, matched(DateFormatClass(tsAttr, Literal("yyyy-MM-dd HH:mm"), None)))
  }

  @Test
  def testUpCastPreservesOrderingButNumericToStringDoesNot(): Unit = {
    // Widening a numeric column preserves ordering, so the source attribute is recovered.
    assertEquals(Some(intAttr), matched(Cast(intAttr, LongType)))
    // Casting a numeric column to string can reorder values, so it must not match.
    assertEquals(None, matched(Cast(intAttr, StringType)))
    // A narrowing numeric cast wraps around in non-ANSI mode and does not preserve ordering.
    assertEquals(None, matched(Cast(longAttr, IntegerType)))
  }

  @Test
  def testNumericCastOrderingFollowsUpCastRules(): Unit = {
    // Widening precedence casts preserve ordering, including lossy-but-monotonic long to float.
    assertEquals(Some(intAttr), matched(Cast(intAttr, DoubleType)))
    assertEquals(Some(longAttr), matched(Cast(longAttr, FloatType)))
    // Decimal widening is an up-cast; narrowing precision overflows into NULL.
    assertEquals(Some(decAttr), matched(Cast(decAttr, DecimalType(12, 2))))
    assertEquals(None, matched(Cast(decAttr, DecimalType(8, 2))))
    // Decimal to float/double rounds monotonically and is allowed explicitly.
    assertEquals(Some(decAttr), matched(Cast(decAttr, DoubleType)))
    assertEquals(Some(decAttr), matched(Cast(decAttr, FloatType)))
    // Cast.canUpCast rejects double to float even though rounding is weakly monotonic; the
    // conservative answer only costs pruning opportunity.
    assertEquals(None, matched(Cast(dblAttr, FloatType)))
  }

  @Test
  def testTimestampToNumericCastOrdering(): Unit = {
    // Epoch seconds fit bigint and floating-point targets.
    assertEquals(Some(tsAttr), matched(Cast(tsAttr, LongType)))
    assertEquals(Some(tsAttr), matched(Cast(tsAttr, DoubleType)))
    assertEquals(Some(tsAttr), matched(Cast(tsAttr, FloatType)))
    // Narrower integral targets wrap around and decimals overflow into NULL.
    assertEquals(None, matched(Cast(tsAttr, IntegerType)))
    assertEquals(None, matched(Cast(tsAttr, ShortType)))
    assertEquals(None, matched(Cast(tsAttr, ByteType)))
    assertEquals(None, matched(Cast(tsAttr, DecimalType(10, 2))))
  }

  @Test
  def testTimestampToStringAndCastsToBooleanDoNotPreserveOrdering(): Unit = {
    // Timestamps format in the session time zone, which repeats local time across a DST fall-back.
    assertEquals(None, matched(Cast(tsAttr, StringType)))
    // Casting to boolean collapses every non-zero or "true"-like value to true.
    assertEquals(None, matched(Cast(intAttr, BooleanType)))
    assertEquals(None, matched(Cast(dblAttr, BooleanType)))
    assertEquals(None, matched(Cast(strAttr, BooleanType)))
    assertEquals(None, matched(Cast(tsAttr, BooleanType)))
    // Identity boolean cast and boolean to numeric (false < true, 0 < 1) keep the ordering.
    assertEquals(Some(boolAttr), matched(Cast(boolAttr, BooleanType)))
    assertEquals(Some(boolAttr), matched(Cast(boolAttr, IntegerType)))
  }

  @Test
  def testCollationChangingStringCastsDoNotPreserveOrdering(): Unit = {
    assumeTrue(HoodieSparkUtils.gteqSpark4_0, "String collations only exist on Spark 4.x")
    // StringType("UTF8_LCASE") is a Spark 4 API, so it is obtained reflectively to keep this
    // file compiling against the Spark 3 profiles.
    val lcase = StringType.getClass.getMethod("apply", classOf[String])
      .invoke(StringType, "UTF8_LCASE").asInstanceOf[DataType]
    val collatedAttr = AttributeReference("cs", lcase)()
    // Changing collation changes the sort order, in either direction.
    assertEquals(None, matched(Cast(collatedAttr, StringType)))
    assertEquals(None, matched(Cast(strAttr, lcase)))
    // Collated strings and numbers re-order each other like default strings do.
    assertEquals(None, matched(Cast(collatedAttr, IntegerType)))
    assertEquals(None, matched(Cast(intAttr, lcase)))
    // Any other source cast to a collated string is rejected too, binary bounds being in byte order.
    assertEquals(None, matched(Cast(binAttr, lcase)))
    assertEquals(None, matched(Cast(dateAttr, lcase)))
    // A collated string cast to any non-string type is rejected, for ex binary compares bytes.
    assertEquals(None, matched(Cast(collatedAttr, BinaryType)))
    assertEquals(None, matched(Cast(collatedAttr, DateType)))
    // The default string still casts to binary, which keeps its byte order.
    assertEquals(Some(strAttr), matched(Cast(strAttr, BinaryType)))
    // A collation-preserving cast keeps the ordering.
    assertEquals(Some(collatedAttr), matched(Cast(collatedAttr, lcase)))
  }

  @Test
  def testCastsOutsideTheAllowlistDoNotPreserveOrdering(): Unit = {
    // Dates and booleans only cast to the explicitly allowed targets; DECIMAL(1, 1) cannot hold true.
    assertEquals(None, matched(Cast(dateAttr, StringType)))
    assertEquals(None, matched(Cast(dateAttr, IntegerType)))
    assertEquals(None, matched(Cast(boolAttr, DecimalType(1, 1))))
    // A date maps to the start of its day, and truncating a timestamp to its date is non-decreasing.
    assertEquals(Some(dateAttr), matched(Cast(dateAttr, TimestampType)))
    assertEquals(Some(tsAttr), matched(Cast(tsAttr, DateType)))
    // The default string and binary both compare by bytes.
    assertEquals(Some(binAttr), matched(Cast(binAttr, StringType)))
    // Boolean to bigint keeps false < true as 0 < 1, and an identity cast is always allowed.
    assertEquals(Some(boolAttr), matched(Cast(boolAttr, LongType)))
    assertEquals(Some(intAttr), matched(Cast(intAttr, IntegerType)))
    // Unpadded strings sort differently from what they parse to ("2022-1-5" > "2022-01-10").
    assertEquals(None, matched(Cast(strAttr, DateType)))
    assertEquals(None, matched(Cast(strAttr, TimestampType)))
    if (HoodieSparkUtils.gteqSpark3_4) {
      // Converting between timestamp and timestamp_ntz shifts local times in the DST spring-forward gap.
      assertEquals(None, matched(Cast(tsAttr, ntzAttr.dataType)))
      assertEquals(None, matched(Cast(ntzAttr, TimestampType)))
      assertEquals(None, matched(Cast(strAttr, ntzAttr.dataType)))
      // Dates map to the start of the day and back without a time zone shift.
      assertEquals(Some(dateAttr), matched(Cast(dateAttr, ntzAttr.dataType)))
      assertEquals(Some(ntzAttr), matched(Cast(ntzAttr, DateType)))
    }
  }

  @Test
  def testNestedComposition(): Unit = {
    assertEquals(Some(intAttr), matched(Add(Multiply(intAttr, Literal(2)), Literal(3))))
  }

  @Test
  def testNonOrderPreservingExpressionsDoNotMatch(): Unit = {
    // DateDiff(end, start) = end - start reverses the ordering of its start date.
    assertEquals(None, matched(DateDiff(Literal(java.sql.Date.valueOf("2022-03-09")), dateAttr)))
    // A bare literal carries no attribute.
    assertEquals(None, matched(Literal(5)))
    // No attribute on either operand.
    assertEquals(None, matched(Add(Literal(1), Literal(2))))
    // Sqrt is not one of the whitelisted order-preserving transformations.
    assertEquals(None, matched(Sqrt(dblAttr)))
    // Case folding breaks the binary ordering of strings: "Zebra" < "apple", but lower("Zebra") > "apple".
    assertEquals(None, matched(Lower(strAttr)))
    assertEquals(None, matched(Upper(strAttr)))
  }
}
