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

  def matches(term: Expression): Boolean = term.resolved && sameFunction(term) && inIndexTimeZone(term)

  private def sameFunction(term: Expression): Boolean = term match {
    case column: AttributeReference => function == IDENTITY_TRANSFORM && isSourceColumn(column)

    case e: Year => function == "year" && isSource(e.child)
    case e: Month => function == "month" && isSource(e.child)
    case e: DayOfMonth => function == "day" && isSource(e.child)
    case e: Hour => function == "hour" && isSource(e.child)
    case e: DateFormatClass =>
      function == "date_format" && isSource(e.left) && literalEquals(e.right, options.get(FORMAT_OPTION))
    case e: FromUnixTime =>
      function == "from_unixtime" && isSource(e.sec) && literalEquals(e.format, Some(formatOrDefault))
    case e: UnixTimestamp =>
      function == "unix_timestamp" && isSource(e.timeExp) && literalEquals(e.format, Some(formatOrDefault))
    case e: ParseToDate => function == "to_date" && isSource(e.left) && optionalLiteralEquals(e.format, FORMAT_OPTION)
    case e: ParseToTimestamp =>
      function == "to_timestamp" && isSource(e.left) && optionalLiteralEquals(e.format, FORMAT_OPTION)
    case e: DateAdd => function == "date_add" && isSource(e.startDate) && literalEquals(e.days, options.get(DAYS_OPTION))
    case e: DateSub => function == "date_sub" && isSource(e.startDate) && literalEquals(e.days, options.get(DAYS_OPTION))

    case e: Upper => function == "upper" && isSource(e.child)
    case e: Lower => function == "lower" && isSource(e.child)
    case e: Length => function == "length" && isSource(e.child)
    case e: Substring =>
      function == "substring" && isSource(e.str) &&
        literalEquals(e.pos, options.get(POSITION_OPTION)) && literalEquals(e.len, options.get(LENGTH_OPTION))
    case e: StringTrim => function == "trim" && isSource(e.srcStr) && optionalLiteralEquals(e.trimStr, TRIM_STRING_OPTION)
    case e: StringTrimLeft => function == "ltrim" && isSource(e.srcStr) && optionalLiteralEquals(e.trimStr, TRIM_STRING_OPTION)
    case e: StringTrimRight =>
      function == "rtrim" && isSource(e.srcStr) && optionalLiteralEquals(e.trimStr, TRIM_STRING_OPTION)
    case e: RegExpReplace =>
      function == "regexp_replace" && isSource(e.subject) && literalEquals(e.pos, Some("1")) &&
        literalEquals(e.regexp, options.get(PATTERN_OPTION)) && literalEquals(e.rep, options.get(REPLACEMENT_OPTION))
    case e: RegExpExtract =>
      function == "regexp_extract" && isSource(e.subject) &&
        literalEquals(e.regexp, options.get(PATTERN_OPTION)) && literalEquals(e.idx, options.get(REGEX_GROUP_INDEX_OPTION))
    case e: StringSplit =>
      function == "split" && isSource(e.str) && literalEquals(e.limit, Some("-1")) &&
        literalEquals(e.regex, options.get(PATTERN_OPTION))

    case _ => false
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
   * The function's input: the column, or the cast Spark adds to the function's input type. The writer applied the
   * same function to the same column type, so it went through the same cast.
   */
  private def isSource(e: Expression): Boolean = e match {
    case column: AttributeReference => isSourceColumn(column)
    case cast: Cast => cast.child match {
      case column: AttributeReference => isSourceColumn(column)
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
