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

package org.apache.spark.sql.hudi.dataskipping

import org.apache.hudi.common.model.HoodieIndexDefinition
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.core.index.expression.HoodieExpressionIndex.{DAYS_OPTION, FORMAT_OPTION, IDENTITY_TRANSFORM, LENGTH_OPTION, PATTERN_OPTION, POSITION_OPTION, REGEX_GROUP_INDEX_OPTION, REPLACEMENT_OPTION, TIME_ZONE_OPTION, TRIM_STRING_OPTION}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, DateAdd, DateFormatClass, DateSub, DayOfMonth, Expression, FromUnixTime, Hour, Length, Literal, Lower, Month, ParseToDate, ParseToTimestamp, RegExpExtract, RegExpReplace, StringSplit, StringTrim, StringTrimLeft, StringTrimRight, Substring, TimeZoneAwareExpression, UnixTimestamp, Upper, Year}
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, DateType, TimestampType}

import scala.collection.JavaConverters._

/**
 * The expression an expression index holds stats for: `function(sourceColumn)` with the index's options, as
 * `ExpressionIndexSparkFunctions` computes it on write.
 *
 * A filter term reads the index only if [[matches]] says it is this expression: the same function, the same
 * options and the same column. Any other term, including the bare column of a non-identity index, has no
 * range in the index, so [[ExpressionIndexSource]] returns none and the file is kept.
 *
 * A term whose value depends on the session time zone (for ex `hour` over a timestamp, or the cast Spark adds to
 * parse a string) also has to be evaluated in the zone the index was computed in. An index without a recorded
 * zone, created before the zone was recorded, matches no such term.
 */
case class IndexedExpression(function: String, sourceColumn: String, options: Map[String, String]) {

  def matches(term: Expression): Boolean = term.resolved && callOf.lift(term).exists { call =>
    call.function == function && call.optionsMatch && isSource(call)
  } && inIndexTimeZone(term)

  /** A term read as the call the writer made: which function, on which input, with which options. */
  private case class Call(function: String, input: Expression, inputType: AbstractDataType, optionsMatch: Boolean = true)

  private def callOf: PartialFunction[Expression, Call] = {
    case e: AttributeReference => Call(IDENTITY_TRANSFORM, e, AnyDataType)

    case e: Year => Call("year", e.child, e.inputTypes.head)
    case e: Month => Call("month", e.child, e.inputTypes.head)
    case e: DayOfMonth => Call("day", e.child, e.inputTypes.head)
    case e: Hour => Call("hour", e.child, e.inputTypes.head)
    case e: DateFormatClass =>
      Call("date_format", e.left, e.inputTypes.head, literalEquals(e.right, options.get(FORMAT_OPTION)))
    case e: FromUnixTime =>
      Call("from_unixtime", e.sec, e.inputTypes.head, literalEquals(e.format, Some(formatOrDefault)))
    case e: UnixTimestamp =>
      Call("unix_timestamp", e.timeExp, e.inputTypes.head, literalEquals(e.format, Some(formatOrDefault)))
    case e: ParseToDate => Call("to_date", e.left, e.inputTypes.head, optionalLiteralEquals(e.format, FORMAT_OPTION))
    case e: ParseToTimestamp =>
      Call("to_timestamp", e.left, e.inputTypes.head, optionalLiteralEquals(e.format, FORMAT_OPTION))
    // NOTE: The optimizer replaces a format-less `ParseToDate` / `ParseToTimestamp` by this cast
    case e: Cast if e.dataType == DateType => Call("to_date", e.child, AnyDataType, optionalLiteralEquals(None, FORMAT_OPTION))
    case e: Cast if e.dataType == TimestampType =>
      Call("to_timestamp", e.child, AnyDataType, optionalLiteralEquals(None, FORMAT_OPTION))
    case e: DateAdd => Call("date_add", e.startDate, e.inputTypes.head, literalEquals(e.days, options.get(DAYS_OPTION)))
    case e: DateSub => Call("date_sub", e.startDate, e.inputTypes.head, literalEquals(e.days, options.get(DAYS_OPTION)))

    case e: Upper => Call("upper", e.child, e.inputTypes.head)
    case e: Lower => Call("lower", e.child, e.inputTypes.head)
    case e: Length => Call("length", e.child, e.inputTypes.head)
    case e: Substring =>
      Call("substring", e.str, e.inputTypes.head,
        literalEquals(e.pos, options.get(POSITION_OPTION)) && literalEquals(e.len, options.get(LENGTH_OPTION)))
    case e: StringTrim => Call("trim", e.srcStr, e.inputTypes.head, optionalLiteralEquals(e.trimStr, TRIM_STRING_OPTION))
    case e: StringTrimLeft => Call("ltrim", e.srcStr, e.inputTypes.head, optionalLiteralEquals(e.trimStr, TRIM_STRING_OPTION))
    case e: StringTrimRight => Call("rtrim", e.srcStr, e.inputTypes.head, optionalLiteralEquals(e.trimStr, TRIM_STRING_OPTION))
    case e: RegExpReplace =>
      Call("regexp_replace", e.subject, e.inputTypes.head, literalEquals(e.pos, Some("1")) &&
        literalEquals(e.regexp, options.get(PATTERN_OPTION)) && literalEquals(e.rep, options.get(REPLACEMENT_OPTION)))
    case e: RegExpExtract =>
      Call("regexp_extract", e.subject, e.inputTypes.head,
        literalEquals(e.regexp, options.get(PATTERN_OPTION)) && literalEquals(e.idx, options.get(REGEX_GROUP_INDEX_OPTION)))
    case e: StringSplit =>
      Call("split", e.str, e.inputTypes.head,
        literalEquals(e.limit, Some("-1")) && literalEquals(e.regex, options.get(PATTERN_OPTION)))
  }

  /** Whether every zone dependent part of the term is evaluated in the index's zone. */
  private def inIndexTimeZone(term: Expression): Boolean = {
    val termZones = term.collect {
      case cast: Cast if Cast.needsTimeZone(cast.child.dataType, cast.dataType) => cast.timeZoneId
      case e: TimeZoneAwareExpression if !e.isInstanceOf[Cast] => e.timeZoneId
    }
    termZones.isEmpty || options.get(TIME_ZONE_OPTION).exists { indexZone =>
      termZones.forall(_.exists(TimeZones.sameZone(_, indexZone)))
    }
  }

  private def isSourceColumn(column: AttributeReference): Boolean = column.name == sourceColumn

  /**
   * The call's input: the column, or the cast Spark adds when the function does not take the column's type as is. The
   * writer applied the same function to the same column type, so it went through the same cast. Any other cast is the
   * query's own, and the index holds no stats for it.
   */
  private def isSource(call: Call): Boolean = call.input match {
    case column: AttributeReference => isSourceColumn(column)
    case cast: Cast => cast.child match {
      case column: AttributeReference =>
        isSourceColumn(column) && !call.inputType.acceptsType(column.dataType) &&
          cast.dataType == call.inputType.defaultConcreteType
      case _ => false
    }
    case _ => false
  }

  private def formatOrDefault: String = options.getOrElse(FORMAT_OPTION, TimestampFormatter.defaultPattern())

  private def literalEquals(e: Expression, expected: Option[String]): Boolean = e match {
    case Literal(value, _) if value != null => expected.contains(value.toString)
    case _ => false
  }

  /** An optional argument, such as a format or a trim string, is absent exactly when the option is unset or empty. */
  private def optionalLiteralEquals(e: Option[Expression], option: String): Boolean = e match {
    case None => StringUtils.isNullOrEmpty(options.getOrElse(option, null))
    case Some(argument) => literalEquals(argument, options.get(option))
  }
}

object IndexedExpression {

  def apply(definition: HoodieIndexDefinition): IndexedExpression =
    IndexedExpression(definition.getIndexFunction, definition.getSourceFields.get(0), definition.getIndexOptions.asScala.toMap)
}
