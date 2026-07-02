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
package org.apache.spark.sql.parser

import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}
import org.apache.hudi.spark.sql.parser.{HoodieSqlBaseBaseVisitor, HoodieSqlBaseParser}
import org.apache.hudi.spark.sql.parser.HoodieSqlBaseParser._

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode, TerminalNode}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{First, Last}
import org.apache.spark.sql.catalyst.parser.{EnhancedLogicalPlan, ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.parser.ParserUtils.{checkDuplicateClauses, checkDuplicateKeys, entry, escapedIdentifier, operationNotAllowed, source, string, stringWithoutUnescape, validate, withOrigin}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{truncatedString, CharVarcharUtils, DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.BucketSpecHelper
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.expressions.{ApplyTransform, BucketTransform, DaysTransform, Expression => V2Expression, FieldReference, HoursTransform, IdentityTransform, LiteralValue, MonthsTransform, Transform, YearsTransform}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.Utils.isTesting

import javax.xml.bind.DatatypeConverter

import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

/**
 * The AstBuilder for the extended Hudi SQL statements: CREATE TABLE with Hudi-specific
 * column types (BLOB, VECTOR) and the index DDL statements. Any other statement falls
 * through the grammar's passThrough rule and is parsed by the delegate Spark parser.
 */
class HoodieSpark4_2ExtendedSqlAstBuilder(conf: SQLConf, delegate: ParserInterface)
  extends HoodieSqlBaseBaseVisitor[AnyRef] with Logging {

  protected def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  /**
   * Override the default behavior for all visit methods. This will only return a non-null result
   * when the context has only one child. This is done because there is no generic method to
   * combine the results of the context children. In all other cases null is returned.
   */
  override def visitChildren(node: RuleNode): AnyRef = {
    if (node.getChildCount == 1) {
      node.getChild(0).accept(this)
    } else {
      null
    }
  }

  // ============== The following code is fork from org.apache.spark.sql.catalyst.parser.AstBuilder
  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  /**
   * Create a Sequence of Strings for a parenthesis enclosed alias list.
   */
  override def visitIdentifierList(ctx: IdentifierListContext): Seq[String] = withOrigin(ctx) {
    visitIdentifierSeq(ctx.identifierSeq)
  }

  /**
   * Create a Sequence of Strings for an identifier list.
   */
  override def visitIdentifierSeq(ctx: IdentifierSeqContext): Seq[String] = withOrigin(ctx) {
    ctx.ident.asScala.map(_.getText).toSeq
  }

  /* ********************************************************************************************
   * Table Identifier parsing
   * ******************************************************************************************** */

  /**
   * Create a [[TableIdentifier]] from a 'tableName' or 'databaseName'.'tableName' pattern.
   */
  override def visitTableIdentifier(
                                     ctx: TableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    TableIdentifier(ctx.table.getText, Option(ctx.db).map(_.getText))
  }

  /**
   * Create a multi-part identifier.
   */
  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] =
    withOrigin(ctx) {
      ctx.parts.asScala.map(_.getText).toSeq
    }

  /* ********************************************************************************************
   * Expression parsing
   * ******************************************************************************************** */

  /**
   * Create a typed Literal expression. A typed literal has the following SQL syntax:
   * {{{
   *   [TYPE] '[VALUE]'
   * }}}
   * Currently Date, Timestamp, Interval and Binary typed literals are supported.
   */
  override def visitTypeConstructor(ctx: TypeConstructorContext): Literal = withOrigin(ctx) {
    val value = string(ctx.STRING)
    val valueType = ctx.identifier.getText.toUpperCase(Locale.ROOT)

    def toLiteral[T](f: UTF8String => Option[T], t: DataType): Literal = {
      f(UTF8String.fromString(value)).map(Literal(_, t)).getOrElse {
        throw new ParseException(s"Cannot parse the $valueType value: $value", ctx)
      }
    }

    def constructTimestampLTZLiteral(value: String): Literal = {
      val zoneId = getZoneId(conf.sessionLocalTimeZone)
      val specialTs = convertSpecialTimestamp(value, zoneId).map(Literal(_, TimestampType))
      specialTs.getOrElse(toLiteral(stringToTimestamp(_, zoneId), TimestampType))
    }

    try {
      valueType match {
        case "DATE" =>
          val zoneId = getZoneId(conf.sessionLocalTimeZone)
          val specialDate = convertSpecialDate(value, zoneId).map(Literal(_, DateType))
          specialDate.getOrElse(toLiteral(stringToDate, DateType))
        // SPARK-36227: Remove TimestampNTZ type support in Spark 3.2 with minimal code changes.
        case "TIMESTAMP_NTZ" if isTesting =>
          convertSpecialTimestampNTZ(value, getZoneId(conf.sessionLocalTimeZone))
            .map(Literal(_, TimestampNTZType))
            .getOrElse(toLiteral(stringToTimestampWithoutTimeZone, TimestampNTZType))
        case "TIMESTAMP_LTZ" if isTesting =>
          constructTimestampLTZLiteral(value)
        case "TIMESTAMP" =>
          SQLConf.get.timestampType match {
            case TimestampNTZType =>
              convertSpecialTimestampNTZ(value, getZoneId(conf.sessionLocalTimeZone))
                .map(Literal(_, TimestampNTZType))
                .getOrElse {
                  val containsTimeZonePart =
                    DateTimeUtils.parseTimestampString(UTF8String.fromString(value))._2.isDefined
                  // If the input string contains time zone part, return a timestamp with local time
                  // zone literal.
                  if (containsTimeZonePart) {
                    constructTimestampLTZLiteral(value)
                  } else {
                    toLiteral(stringToTimestampWithoutTimeZone, TimestampNTZType)
                  }
                }

            case TimestampType =>
              constructTimestampLTZLiteral(value)
          }

        case "INTERVAL" =>
          val interval = try {
            IntervalUtils.stringToInterval(UTF8String.fromString(value))
          } catch {
            case e: IllegalArgumentException =>
              val ex = new ParseException(s"Cannot parse the INTERVAL value: $value", ctx)
              ex.setStackTrace(e.getStackTrace)
              throw ex
          }
          if (!conf.legacyIntervalEnabled) {
            val units = value
              .split("\\s")
              .map(_.toLowerCase(Locale.ROOT).stripSuffix("s"))
              .filter(s => s != "interval" && s.matches("[a-z]+"))
            constructMultiUnitsIntervalLiteral(ctx, interval, units)
          } else {
            Literal(interval, CalendarIntervalType)
          }
        case "X" =>
          val padding = if (value.length % 2 != 0) "0" else ""
          Literal(DatatypeConverter.parseHexBinary(padding + value))
        case other =>
          throw new ParseException(s"Literals of type '$other' are currently not supported.", ctx)
      }
    } catch {
      case e: IllegalArgumentException =>
        val message = Option(e.getMessage).getOrElse(s"Exception parsing $valueType")
        throw new ParseException(message, ctx)
    }
  }

  /**
   * Create a NULL literal expression.
   */
  override def visitNullLiteral(ctx: NullLiteralContext): Literal = withOrigin(ctx) {
    Literal(null)
  }

  /**
   * Create a Boolean literal expression.
   */
  override def visitBooleanLiteral(ctx: BooleanLiteralContext): Literal = withOrigin(ctx) {
    if (ctx.getText.toBoolean) {
      Literal.TrueLiteral
    } else {
      Literal.FalseLiteral
    }
  }

  /**
   * Create an integral literal expression. The code selects the most narrow integral type
   * possible, either a BigDecimal, a Long or an Integer is returned.
   */
  override def visitIntegerLiteral(ctx: IntegerLiteralContext): Literal = withOrigin(ctx) {
    BigDecimal(ctx.getText) match {
      case v if v.isValidInt =>
        Literal(v.intValue)
      case v if v.isValidLong =>
        Literal(v.longValue)
      case v => Literal(v.underlying())
    }
  }

  /**
   * Create a decimal literal for a regular decimal number.
   */
  override def visitDecimalLiteral(ctx: DecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  /**
   * Create a decimal literal for a regular decimal number or a scientific decimal number.
   */
  override def visitLegacyDecimalLiteral(
                                          ctx: LegacyDecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  /**
   * Create a double literal for number with an exponent, e.g. 1E-30
   */
  override def visitExponentLiteral(ctx: ExponentLiteralContext): Literal = {
    numericLiteral(ctx, ctx.getText, /* exponent values don't have a suffix */
      Double.MinValue, Double.MaxValue, DoubleType.simpleString)(_.toDouble)
  }

  /** Create a numeric literal expression. */
  private def numericLiteral(
                              ctx: NumberContext,
                              rawStrippedQualifier: String,
                              minValue: BigDecimal,
                              maxValue: BigDecimal,
                              typeName: String)(converter: String => Any): Literal = withOrigin(ctx) {
    try {
      val rawBigDecimal = BigDecimal(rawStrippedQualifier)
      if (rawBigDecimal < minValue || rawBigDecimal > maxValue) {
        throw new ParseException(s"Numeric literal $rawStrippedQualifier does not " +
          s"fit in range [$minValue, $maxValue] for type $typeName", ctx)
      }
      Literal(converter(rawStrippedQualifier))
    } catch {
      case e: NumberFormatException =>
        throw new ParseException(e.getMessage, ctx)
    }
  }

  /**
   * Create a Byte Literal expression.
   */
  override def visitTinyIntLiteral(ctx: TinyIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Byte.MinValue, Byte.MaxValue, ByteType.simpleString)(_.toByte)
  }

  /**
   * Create a Short Literal expression.
   */
  override def visitSmallIntLiteral(ctx: SmallIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Short.MinValue, Short.MaxValue, ShortType.simpleString)(_.toShort)
  }

  /**
   * Create a Long Literal expression.
   */
  override def visitBigIntLiteral(ctx: BigIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Long.MinValue, Long.MaxValue, LongType.simpleString)(_.toLong)
  }

  /**
   * Create a Float Literal expression.
   */
  override def visitFloatLiteral(ctx: FloatLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Float.MinValue, Float.MaxValue, FloatType.simpleString)(_.toFloat)
  }

  /**
   * Create a Double Literal expression.
   */
  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Double.MinValue, Double.MaxValue, DoubleType.simpleString)(_.toDouble)
  }

  /**
   * Create a BigDecimal Literal expression.
   */
  override def visitBigDecimalLiteral(ctx: BigDecimalLiteralContext): Literal = {
    val raw = ctx.getText.substring(0, ctx.getText.length - 2)
    try {
      Literal(BigDecimal(raw).underlying())
    } catch {
      case e: AnalysisException =>
        throw new ParseException(e.message, ctx)
    }
  }

  /**
   * Create a String literal expression.
   */
  override def visitStringLiteral(ctx: StringLiteralContext): Literal = withOrigin(ctx) {
    Literal(createString(ctx))
  }

  /**
   * Create a String from a string literal context. This supports multiple consecutive string
   * literals, these are concatenated, for example this expression "'hello' 'world'" will be
   * converted into "helloworld".
   *
   * Special characters can be escaped by using Hive/C-style escaping.
   */
  private def createString(ctx: StringLiteralContext): String = {
    if (conf.escapedStringLiterals) {
      ctx.STRING().asScala.map(x => stringWithoutUnescape(x.getSymbol)).mkString
    } else {
      ctx.STRING().asScala.map(string).mkString
    }
  }

  /**
   * Construct an [[Literal]] from [[CalendarInterval]] and
   * units represented as a [[Seq]] of [[String]].
   */
  private def constructMultiUnitsIntervalLiteral(
                                                  ctx: ParserRuleContext,
                                                  calendarInterval: CalendarInterval,
                                                  units: Seq[String]): Literal = {
    var yearMonthFields = Set.empty[Byte]
    var dayTimeFields = Set.empty[Byte]
    for (unit <- units) {
      if (YearMonthIntervalType.stringToField.contains(unit)) {
        yearMonthFields += YearMonthIntervalType.stringToField(unit)
      } else if (DayTimeIntervalType.stringToField.contains(unit)) {
        dayTimeFields += DayTimeIntervalType.stringToField(unit)
      } else if (unit == "week") {
        dayTimeFields += DayTimeIntervalType.DAY
      } else {
        assert(unit == "millisecond" || unit == "microsecond")
        dayTimeFields += DayTimeIntervalType.SECOND
      }
    }
    if (yearMonthFields.nonEmpty) {
      if (dayTimeFields.nonEmpty) {
        val literalStr = source(ctx)
        throw new ParseException(s"Cannot mix year-month and day-time fields: $literalStr", ctx)
      }
      Literal(
        calendarInterval.months,
        YearMonthIntervalType(yearMonthFields.min, yearMonthFields.max)
      )
    } else {
      Literal(
        IntervalUtils.getDuration(calendarInterval, TimeUnit.MICROSECONDS),
        DayTimeIntervalType(dayTimeFields.min, dayTimeFields.max))
    }
  }

  /**
   * Create a [[CalendarInterval]] or ANSI interval literal expression.
   * Two syntaxes are supported:
   * - multiple unit value pairs, for instance: interval 2 months 2 days.
   * - from-to unit, for instance: interval '1-2' year to month.
   */
  override def visitInterval(ctx: IntervalContext): Literal = withOrigin(ctx) {
    val calendarInterval = parseIntervalLiteral(ctx)
    if (ctx.errorCapturingUnitToUnitInterval != null && !conf.legacyIntervalEnabled) {
      // Check the `to` unit to distinguish year-month and day-time intervals because
      // `CalendarInterval` doesn't have enough info. For instance, new CalendarInterval(0, 0, 0)
      // can be derived from INTERVAL '0-0' YEAR TO MONTH as well as from
      // INTERVAL '0 00:00:00' DAY TO SECOND.
      val fromUnit =
      ctx.errorCapturingUnitToUnitInterval.body.from.getText.toLowerCase(Locale.ROOT)
      val toUnit = ctx.errorCapturingUnitToUnitInterval.body.to.getText.toLowerCase(Locale.ROOT)
      if (toUnit == "month") {
        assert(calendarInterval.days == 0 && calendarInterval.microseconds == 0)
        val start = YearMonthIntervalType.stringToField(fromUnit)
        Literal(calendarInterval.months, YearMonthIntervalType(start, YearMonthIntervalType.MONTH))
      } else {
        assert(calendarInterval.months == 0)
        val micros = IntervalUtils.getDuration(calendarInterval, TimeUnit.MICROSECONDS)
        val start = DayTimeIntervalType.stringToField(fromUnit)
        val end = DayTimeIntervalType.stringToField(toUnit)
        Literal(micros, DayTimeIntervalType(start, end))
      }
    } else if (ctx.errorCapturingMultiUnitsInterval != null && !conf.legacyIntervalEnabled) {
      val units =
        ctx.errorCapturingMultiUnitsInterval.body.unit.asScala.map(
          _.getText.toLowerCase(Locale.ROOT).stripSuffix("s")).toSeq
      constructMultiUnitsIntervalLiteral(ctx, calendarInterval, units)
    } else {
      Literal(calendarInterval, CalendarIntervalType)
    }
  }

  /**
   * Create a [[CalendarInterval]] object
   */
  protected def parseIntervalLiteral(ctx: IntervalContext): CalendarInterval = withOrigin(ctx) {
    if (ctx.errorCapturingMultiUnitsInterval != null) {
      val innerCtx = ctx.errorCapturingMultiUnitsInterval
      if (innerCtx.unitToUnitInterval != null) {
        throw new ParseException("Can only have a single from-to unit in the interval literal syntax", innerCtx.unitToUnitInterval)
      }
      visitMultiUnitsInterval(innerCtx.multiUnitsInterval)
    } else if (ctx.errorCapturingUnitToUnitInterval != null) {
      val innerCtx = ctx.errorCapturingUnitToUnitInterval
      if (innerCtx.error1 != null || innerCtx.error2 != null) {
        val errorCtx = if (innerCtx.error1 != null) innerCtx.error1 else innerCtx.error2
        throw new ParseException("Can only have a single from-to unit in the interval literal syntax", errorCtx)
      }
      visitUnitToUnitInterval(innerCtx.body)
    } else {
      throw new ParseException("at least one time unit should be given for interval literal", ctx)
    }
  }

  /**
   * Creates a [[CalendarInterval]] with multiple unit value pairs, e.g. 1 YEAR 2 DAYS.
   */
  override def visitMultiUnitsInterval(ctx: MultiUnitsIntervalContext): CalendarInterval = {
    withOrigin(ctx) {
      val units = ctx.unit.asScala
      val values = ctx.intervalValue().asScala
      try {
        assert(units.length == values.length)
        val kvs = units.indices.map { i =>
          val u = units(i).getText
          val v = if (values(i).STRING() != null) {
            val value = string(values(i).STRING())
            // SPARK-32840: For invalid cases, e.g. INTERVAL '1 day 2' hour,
            // INTERVAL 'interval 1' day, we need to check ahead before they are concatenated with
            // units and become valid ones, e.g. '1 day 2 hour'.
            // Ideally, we only ensure the value parts don't contain any units here.
            if (value.exists(Character.isLetter)) {
              throw new ParseException("Can only use numbers in the interval value part for" +
                s" multiple unit value pairs interval form, but got invalid value: $value", ctx)
            }
            if (values(i).MINUS() == null) {
              value
            } else {
              value.startsWith("-") match {
                case true => value.replaceFirst("-", "")
                case false => s"-$value"
              }
            }
          } else {
            values(i).getText
          }
          UTF8String.fromString(" " + v + " " + u)
        }
        IntervalUtils.stringToInterval(UTF8String.concat(kvs: _*))
      } catch {
        case i: IllegalArgumentException =>
          val e = new ParseException(i.getMessage, ctx)
          e.setStackTrace(i.getStackTrace)
          throw e
      }
    }
  }

  /**
   * Creates a [[CalendarInterval]] with from-to unit, e.g. '2-1' YEAR TO MONTH.
   */
  override def visitUnitToUnitInterval(ctx: UnitToUnitIntervalContext): CalendarInterval = {
    withOrigin(ctx) {
      val value = Option(ctx.intervalValue.STRING).map(string).map { interval =>
        if (ctx.intervalValue().MINUS() == null) {
          interval
        } else {
          interval.startsWith("-") match {
            case true => interval.replaceFirst("-", "")
            case false => s"-$interval"
          }
        }
      }.getOrElse {
        throw new ParseException("The value of from-to unit must be a string", ctx.intervalValue)
      }
      try {
        val from = ctx.from.getText.toLowerCase(Locale.ROOT)
        val to = ctx.to.getText.toLowerCase(Locale.ROOT)
        (from, to) match {
          case ("year", "month") =>
            IntervalUtils.fromYearMonthString(value)
          case ("day", "hour") | ("day", "minute") | ("day", "second") | ("hour", "minute") |
               ("hour", "second") | ("minute", "second") =>
            IntervalUtils.fromDayTimeString(value,
              DayTimeIntervalType.stringToField(from), DayTimeIntervalType.stringToField(to))
          case _ =>
            throw new ParseException(s"Intervals FROM $from TO $to are not supported.", ctx)
        }
      } catch {
        // Handle Exceptions thrown by CalendarInterval
        case e: IllegalArgumentException =>
          val pe = new ParseException(e.getMessage, ctx)
          pe.setStackTrace(e.getStackTrace)
          throw pe
      }
    }
  }

  /* ********************************************************************************************
   * DataType parsing
   * ******************************************************************************************** */

  /**
   * Resolve/create a primitive type.
   */
  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = withOrigin(ctx) {
    val dataType = ctx.typeName.getText.toLowerCase(Locale.ROOT)
    (dataType, ctx.INTEGER_VALUE().asScala.toList) match {
      case ("boolean", Nil) => BooleanType
      case ("tinyint" | "byte", Nil) => ByteType
      case ("smallint" | "short", Nil) => ShortType
      case ("int" | "integer", Nil) => IntegerType
      case ("bigint" | "long", Nil) => LongType
      case ("float" | "real", Nil) => FloatType
      case ("double", Nil) => DoubleType
      case ("date", Nil) => DateType
      case ("timestamp", Nil) => SQLConf.get.timestampType
      // SPARK-36227: Remove TimestampNTZ type support in Spark 3.2 with minimal code changes.
      case ("timestamp_ntz", Nil) if isTesting => TimestampNTZType
      case ("timestamp_ltz", Nil) if isTesting => TimestampType
      case ("string", Nil) => StringType
      case ("character" | "char", length :: Nil) => CharType(length.getText.toInt)
      case ("varchar", length :: Nil) => VarcharType(length.getText.toInt)
      case ("binary", Nil) => BinaryType
      case ("blob", Nil) => BlobType()
      case ("vector", _ :: _) =>
        // Delegate validation to HoodieSchema.parseTypeDescriptor which handles dimension
        // range checks, element type validation, and canonical normalization.
        val vectorSchema = try {
          HoodieSchema.parseTypeDescriptor(ctx.getText).asInstanceOf[HoodieSchema.Vector]
        } catch {
          case e: IllegalArgumentException =>
            throw new ParseException(s"Invalid VECTOR type: ${e.getMessage}", ctx)
        }
        val sparkElemType = vectorSchema.getVectorElementType match {
          case HoodieSchema.Vector.VectorElementType.FLOAT => FloatType
          case HoodieSchema.Vector.VectorElementType.DOUBLE => DoubleType
          case HoodieSchema.Vector.VectorElementType.INT8 => ByteType
        }
        ArrayType(sparkElemType, containsNull = false)
      case ("decimal" | "dec" | "numeric", Nil) => DecimalType.USER_DEFAULT
      case ("decimal" | "dec" | "numeric", precision :: Nil) =>
        DecimalType(precision.getText.toInt, 0)
      case ("decimal" | "dec" | "numeric", precision :: scale :: Nil) =>
        DecimalType(precision.getText.toInt, scale.getText.toInt)
      case ("void", Nil) => NullType
      case ("interval", Nil) => CalendarIntervalType
      case (dt, params) =>
        val dtStr = if (params.nonEmpty) s"$dt(${params.mkString(",")})" else dt
        throw new ParseException(s"DataType $dtStr is not supported.", ctx)
    }
  }

  override def visitYearMonthIntervalDataType(ctx: YearMonthIntervalDataTypeContext): DataType = {
    val startStr = ctx.from.getText.toLowerCase(Locale.ROOT)
    val start = YearMonthIntervalType.stringToField(startStr)
    if (ctx.to != null) {
      val endStr = ctx.to.getText.toLowerCase(Locale.ROOT)
      val end = YearMonthIntervalType.stringToField(endStr)
      if (end <= start) {
        throw new ParseException(s"Intervals FROM $startStr TO $endStr are not supported.", ctx)
      }
      YearMonthIntervalType(start, end)
    } else {
      YearMonthIntervalType(start)
    }
  }

  override def visitDayTimeIntervalDataType(ctx: DayTimeIntervalDataTypeContext): DataType = {
    val startStr = ctx.from.getText.toLowerCase(Locale.ROOT)
    val start = DayTimeIntervalType.stringToField(startStr)
    if (ctx.to != null) {
      val endStr = ctx.to.getText.toLowerCase(Locale.ROOT)
      val end = DayTimeIntervalType.stringToField(endStr)
      if (end <= start) {
        throw new ParseException(s"Intervals FROM $startStr TO $endStr are not supported.", ctx)
      }
      DayTimeIntervalType(start, end)
    } else {
      DayTimeIntervalType(start)
    }
  }

  /**
   * Create a complex DataType. Arrays, Maps and Structures are supported.
   */
  override def visitComplexDataType(ctx: ComplexDataTypeContext): DataType = withOrigin(ctx) {
    ctx.complex.getType match {
      case HoodieSqlBaseParser.ARRAY =>
        ArrayType(typedVisit(ctx.dataType(0)))
      case HoodieSqlBaseParser.MAP =>
        MapType(typedVisit(ctx.dataType(0)), typedVisit(ctx.dataType(1)))
      case HoodieSqlBaseParser.STRUCT =>
        StructType(Option(ctx.complexColTypeList).toSeq.flatMap(visitComplexColTypeList))
    }
  }

  /**
   * Create top level table schema.
   */
  protected def createSchema(ctx: ColTypeListContext): StructType = {
    StructType(Option(ctx).toSeq.flatMap(visitColTypeList))
  }

  /**
   * Create a [[StructType]] from a number of column definitions.
   */
  override def visitColTypeList(ctx: ColTypeListContext): Seq[StructField] = withOrigin(ctx) {
    ctx.colType().asScala.map(visitColType).toSeq
  }

  /**
   * Create a top level [[StructField]] from a column definition.
   */
  override def visitColType(ctx: ColTypeContext): StructField = withOrigin(ctx) {
    import ctx._

    val builder = new MetadataBuilder
    // Add comment to metadata
    Option(commentSpec()).map(visitCommentSpec).foreach {
      builder.putString("comment", _)
    }

    val dataType = typedVisit[DataType](ctx.dataType)

    addMetadataForType(ctx.dataType(), builder)

    StructField(
      name = colName.getText,
      dataType = dataType,
      nullable = NULL == null,
      metadata = builder.build())
  }

  private def addMetadataForType(dataType: HoodieSqlBaseParser.DataTypeContext, builder: MetadataBuilder): Unit = {
    val typeText = dataType.getText
    val upperTypeText = typeText.toUpperCase(Locale.ROOT)
    if (upperTypeText == HoodieSchemaType.BLOB.name()) {
      builder.putString(HoodieSchema.TYPE_METADATA_FIELD, HoodieSchemaType.BLOB.name())
    } else if (upperTypeText.startsWith("VECTOR(")) {
      // Normalize to canonical form (e.g. "VECTOR(128,FLOAT)" -> "VECTOR(128)")
      val vectorSchema = HoodieSchema.parseTypeDescriptor(typeText).asInstanceOf[HoodieSchema.Vector]
      builder.putString(HoodieSchema.TYPE_METADATA_FIELD, vectorSchema.toTypeDescriptor)
    }
  }

  /**
   * Create a [[StructType]] from a sequence of [[StructField]]s.
   */
  protected def createStructType(ctx: ComplexColTypeListContext): StructType = {
    StructType(Option(ctx).toSeq.flatMap(visitComplexColTypeList))
  }

  /**
   * Create a [[StructType]] from a number of column definitions.
   */
  override def visitComplexColTypeList(
                                        ctx: ComplexColTypeListContext): Seq[StructField] = withOrigin(ctx) {
    ctx.complexColType().asScala.map(visitComplexColType).toSeq
  }

  /**
   * Create a [[StructField]] from a column definition.
   */
  override def visitComplexColType(ctx: ComplexColTypeContext): StructField = withOrigin(ctx) {
    import ctx._
    val builder = new MetadataBuilder
    // Add comment to metadata
    Option(commentSpec()).map(visitCommentSpec).foreach {
      builder.putString("comment", _)
    }
    addMetadataForType(ctx.dataType(), builder)

    StructField(
      name = identifier.getText,
      dataType = typedVisit(dataType()),
      nullable = NULL == null,
      metadata = builder.build())
  }

  /**
   * Create a location string.
   */
  override def visitLocationSpec(ctx: LocationSpecContext): String = withOrigin(ctx) {
    string(ctx.STRING)
  }

  /**
   * Create an optional location string.
   */
  protected def visitLocationSpecList(ctx: java.util.List[LocationSpecContext]): Option[String] = {
    ctx.asScala.headOption.map(visitLocationSpec)
  }

  /**
   * Create a comment string.
   */
  override def visitCommentSpec(ctx: CommentSpecContext): String = withOrigin(ctx) {
    string(ctx.STRING)
  }

  /**
   * Create an optional comment string.
   */
  protected def visitCommentSpecList(ctx: java.util.List[CommentSpecContext]): Option[String] = {
    ctx.asScala.headOption.map(visitCommentSpec)
  }

  /**
   * Create a [[BucketSpec]].
   */
  override def visitBucketSpec(ctx: BucketSpecContext): BucketSpec = withOrigin(ctx) {
    BucketSpec(
      ctx.INTEGER_VALUE.getText.toInt,
      visitIdentifierList(ctx.identifierList),
      Option(ctx.orderedIdentifierList)
        .toSeq
        .flatMap(_.orderedIdentifier.asScala)
        .map { orderedIdCtx =>
          Option(orderedIdCtx.ordering).map(_.getText).foreach { dir =>
            if (dir.toLowerCase(Locale.ROOT) != "asc") {
              operationNotAllowed(s"Column ordering must be ASC, was '$dir'", ctx)
            }
          }

          orderedIdCtx.ident.getText
        })
  }

  /**
   * Convert a table property list into a key-value map.
   * This should be called through [[visitPropertyKeyValues]] or [[visitPropertyKeys]].
   */
  override def visitTablePropertyList(
                                       ctx: TablePropertyListContext): Map[String, String] = withOrigin(ctx) {
    val properties = ctx.tableProperty.asScala.map { property =>
      val key = visitTablePropertyKey(property.key)
      val value = visitTablePropertyValue(property.value)
      key -> value
    }
    // Check for duplicate property names.
    checkDuplicateKeys(properties.toSeq, ctx)
    properties.toMap
  }

  /**
   * Parse a key-value map from a [[TablePropertyListContext]], assuming all values are specified.
   */
  def visitPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String] = {
    val props = visitTablePropertyList(ctx)
    val badKeys = props.collect { case (key, null) => key }
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values must be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props
  }

  /**
   * Parse a list of keys from a [[TablePropertyListContext]], assuming no values are specified.
   */
  def visitPropertyKeys(ctx: TablePropertyListContext): Seq[String] = {
    val props = visitTablePropertyList(ctx)
    val badKeys = props.filter { case (_, v) => v != null }.keys
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values should not be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props.keys.toSeq
  }

  /**
   * A table property key can either be String or a collection of dot separated elements. This
   * function extracts the property key based on whether its a string literal or a table property
   * identifier.
   */
  override def visitTablePropertyKey(key: TablePropertyKeyContext): String = {
    if (key.STRING != null) {
      string(key.STRING)
    } else {
      key.getText
    }
  }

  /**
   * A table property value can be String, Integer, Boolean or Decimal. This function extracts
   * the property value based on whether its a string, integer, boolean or decimal literal.
   */
  override def visitTablePropertyValue(value: TablePropertyValueContext): String = {
    if (value == null) {
      null
    } else if (value.STRING != null) {
      string(value.STRING)
    } else if (value.booleanValue != null) {
      value.getText.toLowerCase(Locale.ROOT)
    } else {
      value.getText
    }
  }

  /**
   * Type to keep track of a table header: (identifier, isTemporary, ifNotExists, isExternal).
   */
  type TableHeader = (Seq[String], Boolean, Boolean, Boolean)

  /**
   * Type to keep track of table clauses:
   * - partition transforms
   * - partition columns
   * - bucketSpec
   * - properties
   * - options
   * - location
   * - comment
   * - serde
   *
   * Note: Partition transforms are based on existing table schema definition. It can be simple
   * column names, or functions like `year(date_col)`. Partition columns are column names with data
   * types like `i INT`, which should be appended to the existing table schema.
   */
  type TableClauses = (
    Seq[Transform], Seq[StructField], Option[BucketSpec], Map[String, String],
      Map[String, String], Option[String], Option[String], Option[SerdeInfo])

  /**
   * Validate a create table statement and return the [[TableIdentifier]].
   */
  override def visitCreateTableHeader(
                                       ctx: CreateTableHeaderContext): TableHeader = withOrigin(ctx) {
    val temporary = ctx.TEMPORARY != null
    val ifNotExists = ctx.EXISTS != null
    if (temporary && ifNotExists) {
      operationNotAllowed("CREATE TEMPORARY TABLE ... IF NOT EXISTS", ctx)
    }
    val multipartIdentifier = ctx.multipartIdentifier.parts.asScala.map(_.getText).toSeq
    (multipartIdentifier, temporary, ifNotExists, ctx.EXTERNAL != null)
  }

  /**
   * Parse a qualified name to a multipart name.
   */
  override def visitQualifiedName(ctx: QualifiedNameContext): Seq[String] = withOrigin(ctx) {
    ctx.identifier.asScala.map(_.getText).toSeq
  }

  /**
   * Parse a list of transforms or columns.
   */
  override def visitPartitionFieldList(
                                        ctx: PartitionFieldListContext): (Seq[Transform], Seq[StructField]) = withOrigin(ctx) {
    val (transforms, columns) = ctx.fields.asScala.map {
      case transform: PartitionTransformContext =>
        (Some(visitPartitionTransform(transform)), None)
      case field: PartitionColumnContext =>
        (None, Some(visitColType(field.colType)))
    }.unzip

    (transforms.flatten.toSeq, columns.flatten.toSeq)
  }

  override def visitPartitionTransform(
                                        ctx: PartitionTransformContext): Transform = withOrigin(ctx) {
    def getFieldReference(
                           ctx: ApplyTransformContext,
                           arg: V2Expression): FieldReference = {
      lazy val name: String = ctx.identifier.getText
      arg match {
        case ref: FieldReference =>
          ref
        case nonRef =>
          throw new ParseException(s"Expected a column reference for transform $name: $nonRef.describe", ctx)
      }
    }

    def getSingleFieldReference(
                                 ctx: ApplyTransformContext,
                                 arguments: Seq[V2Expression]): FieldReference = {
      lazy val name: String = ctx.identifier.getText
      if (arguments.size > 1) {
        throw new ParseException(s"Too many arguments for transform $name", ctx)
      } else if (arguments.isEmpty) {
        throw

          new ParseException(s"Not enough arguments for transform $name", ctx)
      } else {
        getFieldReference(ctx, arguments.head)
      }
    }

    ctx.transform match {
      case identityCtx: IdentityTransformContext =>
        IdentityTransform(FieldReference(typedVisit[Seq[String]](identityCtx.qualifiedName)))

      case applyCtx: ApplyTransformContext =>
        val arguments = applyCtx.argument.asScala.map(visitTransformArgument).toSeq

        applyCtx.identifier.getText match {
          case "bucket" =>
            val numBuckets: Int = arguments.head match {
              case LiteralValue(shortValue, ShortType) =>
                shortValue.asInstanceOf[Short].toInt
              case LiteralValue(intValue, IntegerType) =>
                intValue.asInstanceOf[Int]
              case LiteralValue(longValue, LongType) =>
                longValue.asInstanceOf[Long].toInt
              case lit =>
                throw new ParseException(s"Invalid number of buckets: ${lit.describe}", applyCtx)
            }

            val fields = arguments.tail.map(arg => getFieldReference(applyCtx, arg))

            BucketTransform(LiteralValue(numBuckets, IntegerType), fields)

          case "years" =>
            YearsTransform(getSingleFieldReference(applyCtx, arguments))

          case "months" =>
            MonthsTransform(getSingleFieldReference(applyCtx, arguments))

          case "days" =>
            DaysTransform(getSingleFieldReference(applyCtx, arguments))

          case "hours" =>
            HoursTransform(getSingleFieldReference(applyCtx, arguments))

          case name =>
            ApplyTransform(name, arguments)
        }
    }
  }

  /**
   * Parse an argument to a transform. An argument may be a field reference (qualified name) or
   * a value literal.
   */
  override def visitTransformArgument(ctx: TransformArgumentContext): V2Expression = {
    withOrigin(ctx) {
      val reference = Option(ctx.qualifiedName)
        .map(typedVisit[Seq[String]])
        .map(FieldReference(_))
      val literal = Option(ctx.constant)
        .map(typedVisit[Literal])
        .map(lit => LiteralValue(lit.value, lit.dataType))
      reference.orElse(literal)
        .getOrElse(throw new ParseException("Invalid transform argument", ctx))
    }
  }

  def cleanTableProperties(
                            ctx: ParserRuleContext, properties: Map[String, String]): Map[String, String] = {
    import TableCatalog._
    val legacyOn = conf.getConf(SQLConf.LEGACY_PROPERTY_NON_RESERVED)
    properties.filter {
      case (PROP_PROVIDER, _) if !legacyOn =>
        throw new ParseException(s"$PROP_PROVIDER is a reserved table property, please use the USING clause to specify it.", ctx)
      case (PROP_PROVIDER, _) => false
      case (PROP_LOCATION, _) if !legacyOn =>
        throw new ParseException(s"$PROP_LOCATION is a reserved table property, please use the LOCATION clause to specify it.", ctx)
      case (PROP_LOCATION, _) => false
      case (PROP_OWNER, _) if !legacyOn =>
        throw new ParseException(s"$PROP_OWNER is a reserved table property, it will be set to the current user.", ctx)
      case (PROP_OWNER, _) => false
      case _ => true
    }
  }

  def cleanTableOptions(
                         ctx: ParserRuleContext,
                         options: Map[String, String],
                         location: Option[String]): (Map[String, String], Option[String]) = {
    var path = location
    val filtered = cleanTableProperties(ctx, options).filter {
      case (k, v) if k.equalsIgnoreCase("path") && path.nonEmpty =>
        throw new ParseException(s"Duplicated table paths found: '${path.get}' and '$v'. LOCATION" +
          s" and the case insensitive key 'path' in OPTIONS are all used to indicate the custom" +
          s" table path, you can only specify one of them.", ctx)
      case (k, v) if k.equalsIgnoreCase("path") =>
        path = Some(v)
        false
      case _ => true
    }
    (filtered, path)
  }

  /**
   * Create a [[SerdeInfo]] for creating tables.
   *
   * Format: STORED AS (name | INPUTFORMAT input_format OUTPUTFORMAT output_format)
   */
  override def visitCreateFileFormat(ctx: CreateFileFormatContext): SerdeInfo = withOrigin(ctx) {
    (ctx.fileFormat, ctx.storageHandler) match {
      // Expected format: INPUTFORMAT input_format OUTPUTFORMAT output_format
      case (c: TableFileFormatContext, null) =>
        SerdeInfo(formatClasses = Some(FormatClasses(string(c.inFmt), string(c.outFmt))))
      // Expected format: SEQUENCEFILE | TEXTFILE | RCFILE | ORC | PARQUET | AVRO
      case (c: GenericFileFormatContext, null) =>
        SerdeInfo(storedAs = Some(c.identifier.getText))
      case (null, storageHandler) =>
        operationNotAllowed("STORED BY", ctx)
      case _ =>
        throw new ParseException("Expected either STORED AS or STORED BY, not both", ctx)
    }
  }

  /**
   * Create a [[SerdeInfo]] used for creating tables.
   *
   * Example format:
   * {{{
   *   SERDE serde_name [WITH SERDEPROPERTIES (k1=v1, k2=v2, ...)]
   * }}}
   *
   * OR
   *
   * {{{
   *   DELIMITED [FIELDS TERMINATED BY char [ESCAPED BY char]]
   *   [COLLECTION ITEMS TERMINATED BY char]
   *   [MAP KEYS TERMINATED BY char]
   *   [LINES TERMINATED BY char]
   *   [NULL DEFINED AS char]
   * }}}
   */
  def visitRowFormat(ctx: RowFormatContext): SerdeInfo = withOrigin(ctx) {
    ctx match {
      case serde: RowFormatSerdeContext => visitRowFormatSerde(serde)
      case delimited: RowFormatDelimitedContext => visitRowFormatDelimited(delimited)
    }
  }

  /**
   * Create SERDE row format name and properties pair.
   */
  override def visitRowFormatSerde(ctx: RowFormatSerdeContext): SerdeInfo = withOrigin(ctx) {
    import ctx._
    SerdeInfo(
      serde = Some(string(name)),
      serdeProperties = Option(tablePropertyList).map(visitPropertyKeyValues).getOrElse(Map.empty))
  }

  /**
   * Create a delimited row format properties object.
   */
  override def visitRowFormatDelimited(
                                        ctx: RowFormatDelimitedContext): SerdeInfo = withOrigin(ctx) {
    // Collect the entries if any.
    def entry(key: String, value: Token): Seq[(String, String)] = {
      Option(value).toSeq.map(x => key -> string(x))
    }

    // TODO we need proper support for the NULL format.
    val entries =
      entry("field.delim", ctx.fieldsTerminatedBy) ++
        entry("serialization.format", ctx.fieldsTerminatedBy) ++
        entry("escape.delim", ctx.escapedBy) ++
        // The following typo is inherited from Hive...
        entry("colelction.delim", ctx.collectionItemsTerminatedBy) ++
        entry("mapkey.delim", ctx.keysTerminatedBy) ++
        Option(ctx.linesSeparatedBy).toSeq.map { token =>
          val value = string(token)
          validate(
            value == "\n",
            s"LINES TERMINATED BY only supports newline '\\n' right now: $value",
            ctx)
          "line.delim" -> value
        }
    SerdeInfo(serdeProperties = entries.toMap)
  }

  /**
   * Throw a [[ParseException]] if the user specified incompatible SerDes through ROW FORMAT
   * and STORED AS.
   *
   * The following are allowed. Anything else is not:
   * ROW FORMAT SERDE ... STORED AS [SEQUENCEFILE | RCFILE | TEXTFILE]
   * ROW FORMAT DELIMITED ... STORED AS TEXTFILE
   * ROW FORMAT ... STORED AS INPUTFORMAT ... OUTPUTFORMAT ...
   */
  protected def validateRowFormatFileFormat(
                                             rowFormatCtx: RowFormatContext,
                                             createFileFormatCtx: CreateFileFormatContext,
                                             parentCtx: ParserRuleContext): Unit = {
    if (!(rowFormatCtx == null || createFileFormatCtx == null)) {
      (rowFormatCtx, createFileFormatCtx.fileFormat) match {
        case (_, ffTable: TableFileFormatContext) => // OK
        case (rfSerde: RowFormatSerdeContext, ffGeneric: GenericFileFormatContext) =>
          ffGeneric.identifier.getText.toLowerCase(Locale.ROOT) match {
            case ("sequencefile" | "textfile" | "rcfile") => // OK
            case fmt =>
              operationNotAllowed(
                s"ROW FORMAT SERDE is incompatible with format '$fmt', which also specifies a serde",
                parentCtx)
          }
        case (rfDelimited: RowFormatDelimitedContext, ffGeneric: GenericFileFormatContext) =>
          ffGeneric.identifier.getText.toLowerCase(Locale.ROOT) match {
            case "textfile" => // OK
            case fmt => operationNotAllowed(
              s"ROW FORMAT DELIMITED is only compatible with 'textfile', not '$fmt'", parentCtx)
          }
        case _ =>
          // should never happen
          def str(ctx: ParserRuleContext): String = {
            (0 until ctx.getChildCount).map { i => ctx.getChild(i).getText }.mkString(" ")
          }

          operationNotAllowed(
            s"Unexpected combination of ${str(rowFormatCtx)} and ${str(createFileFormatCtx)}",
            parentCtx)
      }
    }
  }

  protected def validateRowFormatFileFormat(
                                             rowFormatCtx: Seq[RowFormatContext],
                                             createFileFormatCtx: Seq[CreateFileFormatContext],
                                             parentCtx: ParserRuleContext): Unit = {
    if (rowFormatCtx.size == 1 && createFileFormatCtx.size == 1) {
      validateRowFormatFileFormat(rowFormatCtx.head, createFileFormatCtx.head, parentCtx)
    }
  }

  override def visitCreateTableClauses(ctx: CreateTableClausesContext): TableClauses = {
    checkDuplicateClauses(ctx.TBLPROPERTIES, "TBLPROPERTIES", ctx)
    checkDuplicateClauses(ctx.OPTIONS, "OPTIONS", ctx)
    checkDuplicateClauses(ctx.PARTITIONED, "PARTITIONED BY", ctx)
    checkDuplicateClauses(ctx.createFileFormat, "STORED AS/BY", ctx)
    checkDuplicateClauses(ctx.rowFormat, "ROW FORMAT", ctx)
    checkDuplicateClauses(ctx.commentSpec(), "COMMENT", ctx)
    checkDuplicateClauses(ctx.bucketSpec(), "CLUSTERED BY", ctx)
    checkDuplicateClauses(ctx.locationSpec, "LOCATION", ctx)

    if (ctx.skewSpec.size > 0) {
      operationNotAllowed("CREATE TABLE ... SKEWED BY", ctx)
    }

    val (partTransforms, partCols) =
      Option(ctx.partitioning).map(visitPartitionFieldList).getOrElse((Nil, Nil))
    val bucketSpec = ctx.bucketSpec().asScala.headOption.map(visitBucketSpec)
    val properties = Option(ctx.tableProps).map(visitPropertyKeyValues).getOrElse(Map.empty)
    val cleanedProperties = cleanTableProperties(ctx, properties)
    val options = Option(ctx.options).map(visitPropertyKeyValues).getOrElse(Map.empty)
    val location = visitLocationSpecList(ctx.locationSpec())
    val (cleanedOptions, newLocation) = cleanTableOptions(ctx, options, location)
    val comment = visitCommentSpecList(ctx.commentSpec())
    val serdeInfo =
      getSerdeInfo(ctx.rowFormat.asScala.toSeq, ctx.createFileFormat.asScala.toSeq, ctx)
    (partTransforms, partCols, bucketSpec, cleanedProperties, cleanedOptions, newLocation, comment,
      serdeInfo)
  }

  protected def getSerdeInfo(
                              rowFormatCtx: Seq[RowFormatContext],
                              createFileFormatCtx: Seq[CreateFileFormatContext],
                              ctx: ParserRuleContext): Option[SerdeInfo] = {
    validateRowFormatFileFormat(rowFormatCtx, createFileFormatCtx, ctx)
    val rowFormatSerdeInfo = rowFormatCtx.map(visitRowFormat)
    val fileFormatSerdeInfo = createFileFormatCtx.map(visitCreateFileFormat)
    (fileFormatSerdeInfo ++ rowFormatSerdeInfo).reduceLeftOption((l, r) => l.merge(r))
  }

  private def partitionExpressions(
                                    partTransforms: Seq[Transform],
                                    partCols: Seq[StructField],
                                    ctx: ParserRuleContext): Seq[Transform] = {
    if (partTransforms.nonEmpty) {
      if (partCols.nonEmpty) {
        val references = partTransforms.map(_.describe()).mkString(", ")
        val columns = partCols
          .map(field => s"${field.name} ${field.dataType.simpleString}")
          .mkString(", ")
        operationNotAllowed(
          s"""PARTITION BY: Cannot mix partition expressions and partition columns:
             |Expressions: $references
             |Columns: $columns""".stripMargin, ctx)

      }
      partTransforms
    } else {
      // columns were added to create the schema. convert to column references
      partCols.map { column =>
        IdentityTransform(FieldReference(Seq(column.name)))
      }
    }
  }

  /**
   * Create a table, returning a [[CreateTable]] logical plan.
   *
   * Expected format:
   * {{{
   *   CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db_name.]table_name
   *   [USING table_provider]
   *   create_table_clauses;
   *
   *   create_table_clauses (order insensitive):
   *     [PARTITIONED BY (partition_fields)]
   *     [OPTIONS table_property_list]
   *     [ROW FORMAT row_format]
   *     [STORED AS file_format]
   *     [CLUSTERED BY (col_name, col_name, ...)
   *       [SORTED BY (col_name [ASC|DESC], ...)]
   *       INTO num_buckets BUCKETS
   *     ]
   *     [LOCATION path]
   *     [COMMENT table_comment]
   *     [TBLPROPERTIES (property_name=property_value, ...)]
   *
   *   partition_fields:
   *     col_name, transform(col_name), transform(constant, col_name), ... |
   *     col_name data_type [NOT NULL] [COMMENT col_comment], ...
   * }}}
   */
  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = withOrigin(ctx) {
    val (table, temp, ifNotExists, external) = visitCreateTableHeader(ctx.createTableHeader)

    val columns = Option(ctx.colTypeList()).map(visitColTypeList).getOrElse(Nil)
    val provider = Option(ctx.tableProvider).map(_.multipartIdentifier.getText)
    val (partTransforms, partCols, bucketSpec, properties, options, location, comment, serdeInfo) =
      visitCreateTableClauses(ctx.createTableClauses())

    if (provider.isDefined && serdeInfo.isDefined) {
      operationNotAllowed(s"CREATE TABLE ... USING ... ${serdeInfo.get.describe}", ctx)
    }

    if (temp) {
      operationNotAllowed(
        "CREATE TEMPORARY TABLE ..., use CREATE TEMPORARY VIEW instead", ctx)
    }

    // partition transforms for BucketSpec was moved inside parser
    // https://issues.apache.org/jira/browse/SPARK-37923
    val partitioning =
    partitionExpressions(partTransforms, partCols, ctx) ++ bucketSpec.map(_.asTransform)
    val tableSpec = TableSpec(properties, provider, options, location, comment,
      Option.empty, serdeInfo, external)

    // Note: table schema includes both the table columns list and the partition columns
    // with data type.
    val schema = StructType(columns ++ partCols)
    CreateTable(
      UnresolvedIdentifier(table),
      schema.map(ColumnDefinition.fromV1Column(_, delegate)), partitioning, tableSpec, ignoreIfExists = ifNotExists)
  }

  /**
   * Create an index, returning a [[CreateIndex]] logical plan.
   * For example:
   * {{{
   * CREATE INDEX index_name ON [TABLE] table_name [USING index_type] (column_index_property_list)
   *   [OPTIONS indexPropertyList]
   *   column_index_property_list: column_name [OPTIONS(indexPropertyList)]  [ ,  . . . ]
   *   indexPropertyList: index_property_name [= index_property_value] [ ,  . . . ]
   * }}}
   */
  override def visitCreateIndex(ctx: CreateIndexContext): LogicalPlan = withOrigin(ctx) {
    val (indexName, indexType) = if (ctx.identifier.size() == 1) {
      (ctx.identifier(0).getText, "")
    } else {
      (ctx.identifier(0).getText, ctx.identifier(1).getText)
    }

    val columns = ctx.columns.multipartIdentifierProperty.asScala
      .map(_.multipartIdentifier).map(typedVisit[Seq[String]]).toSeq
    val columnsProperties = ctx.columns.multipartIdentifierProperty.asScala
      .map(x => (Option(x.options).map(visitPropertyKeyValues).getOrElse(Map.empty))).toSeq
    val options = Option(ctx.indexOptions).map(visitPropertyKeyValues).getOrElse(Map.empty)

    CreateIndex(
      UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier())),
      indexName,
      indexType,
      ctx.EXISTS != null,
      columns.map(UnresolvedFieldName).zip(columnsProperties),
      options)
  }

  /**
   * Drop an index, returning a [[DropIndex]] logical plan.
   * For example:
   * {{{
   *   DROP INDEX [IF EXISTS] index_name ON [TABLE] table_name
   * }}}
   */
  override def visitDropIndex(ctx: DropIndexContext): LogicalPlan = withOrigin(ctx) {
    val indexName = ctx.identifier.getText
    DropIndex(
      UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier())),
      indexName,
      ctx.EXISTS != null)
  }

  /**
   * Show indexes, returning a [[HoodieShowIndexes]] logical plan.
   * For example:
   * {{{
   *   SHOW INDEXES (FROM | IN) [TABLE] table_name
   * }}}
   */
  override def visitShowIndexes(ctx: ShowIndexesContext): LogicalPlan = withOrigin(ctx) {
    HoodieShowIndexes(UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier())))
  }

  /**
   * Refresh index, returning a [[RefreshIndex]] logical plan
   * For example:
   * {{{
   *   REFRESH INDEX index_name ON [TABLE] table_name
   * }}}
   */
  override def visitRefreshIndex(ctx: RefreshIndexContext): LogicalPlan = withOrigin(ctx) {
    RefreshIndex(UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier())), ctx.identifier.getText)
  }

  /**
   * Convert a property list into a key-value map.
   * This should be called through [[visitPropertyKeyValues]] or [[visitPropertyKeys]].
   */
  override def visitPropertyList(ctx: PropertyListContext): Map[String, String] = withOrigin(ctx) {
    val properties = ctx.property.asScala.map { property =>
      val key = visitPropertyKey(property.key)
      val value = visitPropertyValue(property.value)
      key -> value
    }
    // Check for duplicate property names.
    checkDuplicateKeys(properties.toSeq, ctx)
    properties.toMap
  }

  /**
   * Parse a key-value map from a [[PropertyListContext]], assuming all values are specified.
   */
  def visitPropertyKeyValues(ctx: PropertyListContext): Map[String, String] = {
    val props = visitPropertyList(ctx)
    val badKeys = props.collect { case (key, null) => key }
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values must be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props
  }

  /**
   * Parse a list of keys from a [[PropertyListContext]], assuming no values are specified.
   */
  def visitPropertyKeys(ctx: PropertyListContext): Seq[String] = {
    val props = visitPropertyList(ctx)
    val badKeys = props.filter { case (_, v) => v != null }.keys
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values should not be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props.keys.toSeq
  }

  /**
   * A property key can either be String or a collection of dot separated elements. This
   * function extracts the property key based on whether its a string literal or a property
   * identifier.
   */
  override def visitPropertyKey(key: PropertyKeyContext): String = {
    if (key.STRING != null) {
      string(key.STRING)
    } else {
      key.getText
    }
  }

  /**
   * A property value can be String, Integer, Boolean or Decimal. This function extracts
   * the property value based on whether its a string, integer, boolean or decimal literal.
   */
  override def visitPropertyValue(value: PropertyValueContext): String = {
    if (value == null) {
      null
    } else if (value.STRING != null) {
      string(value.STRING)
    } else if (value.booleanValue != null) {
      value.getText.toLowerCase(Locale.ROOT)
    } else {
      value.getText
    }
  }
}
