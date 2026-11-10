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
import org.apache.hudi.core.index.expression.HoodieExpressionIndex.{DAYS_OPTION, FORMAT_OPTION, IDENTITY_TRANSFORM, LENGTH_OPTION, PATTERN_OPTION, POSITION_OPTION, REGEX_GROUP_INDEX_OPTION, REPLACEMENT_OPTION, TRIM_STRING_OPTION}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, DateAdd, DateFormatClass, DateSub, DayOfMonth, Expression, FromUnixTime, Hour, Length, Literal, Lower, Month, ParseToDate, ParseToTimestamp, RegExpExtract, RegExpReplace, StringSplit, StringTrim, StringTrimLeft, StringTrimRight, Substring, UnixTimestamp, Upper, Year}
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.types.{BinaryType, DataType, DateType, LongType, StringType, TimestampNTZType, TimestampType}

import scala.collection.JavaConverters._

/**
 * The expression an expression index holds stats for: `function(sourceColumn)` with the index's options, as
 * `ExpressionIndexSparkFunctions` computes it on write.
 *
 * A filter term reads the index only if [[matches]] says it is this expression: the same function, the same
 * options and the same column. Any other term, including the bare column of a non-identity index, has no
 * range in the index, so [[ExpressionIndexSource]] returns none and the file is kept.
 */
case class IndexedExpression(function: String, sourceColumn: String, options: Map[String, String]) {

  def matches(term: Expression): Boolean = term.resolved && (term match {
    case column: AttributeReference => function == IDENTITY_TRANSFORM && isSourceColumn(column)

    case e: Year => function == "year" && isSource(e.child, DateType)
    case e: Month => function == "month" && isSource(e.child, DateType)
    case e: DayOfMonth => function == "day" && isSource(e.child, DateType)
    case e: Hour => function == "hour" && isSource(e.child, TimestampType)
    case e: DateFormatClass =>
      function == "date_format" && isSource(e.left, TimestampType) && literalEquals(e.right, options.get(FORMAT_OPTION))
    case e: FromUnixTime =>
      function == "from_unixtime" && isSource(e.sec, LongType) && literalEquals(e.format, Some(formatOrDefault))
    case e: UnixTimestamp =>
      function == "unix_timestamp" && isParseSource(e.timeExp) && literalEquals(e.format, Some(formatOrDefault))
    case e: ParseToDate => function == "to_date" && isParseSource(e.left) && optionalLiteralEquals(e.format, FORMAT_OPTION)
    case e: ParseToTimestamp =>
      function == "to_timestamp" && isParseSource(e.left) && optionalLiteralEquals(e.format, FORMAT_OPTION)
    // NOTE: The optimizer replaces a format-less `ParseToDate` / `ParseToTimestamp` by this cast
    case e: Cast if e.dataType == DateType => function == "to_date" && isFormatLessParseOf(e.child)
    case e: Cast if e.dataType == TimestampType => function == "to_timestamp" && isFormatLessParseOf(e.child)
    case e: DateAdd => function == "date_add" && isSource(e.startDate, DateType) && literalEquals(e.days, options.get(DAYS_OPTION))
    case e: DateSub => function == "date_sub" && isSource(e.startDate, DateType) && literalEquals(e.days, options.get(DAYS_OPTION))

    case e: Upper => function == "upper" && isSource(e.child, StringType)
    case e: Lower => function == "lower" && isSource(e.child, StringType)
    case e: Length => function == "length" && isSource(e.child, StringType, BinaryType)
    case e: Substring =>
      function == "substring" && isSource(e.str, StringType, BinaryType) &&
        literalEquals(e.pos, options.get(POSITION_OPTION)) && literalEquals(e.len, options.get(LENGTH_OPTION))
    case e: StringTrim =>
      function == "trim" && isSource(e.srcStr, StringType) && optionalLiteralEquals(e.trimStr, TRIM_STRING_OPTION)
    case e: StringTrimLeft =>
      function == "ltrim" && isSource(e.srcStr, StringType) && optionalLiteralEquals(e.trimStr, TRIM_STRING_OPTION)
    case e: StringTrimRight =>
      function == "rtrim" && isSource(e.srcStr, StringType) && optionalLiteralEquals(e.trimStr, TRIM_STRING_OPTION)
    case e: RegExpReplace =>
      function == "regexp_replace" && isSource(e.subject, StringType) && literalEquals(e.pos, Some("1")) &&
        literalEquals(e.regexp, options.get(PATTERN_OPTION)) && literalEquals(e.rep, options.get(REPLACEMENT_OPTION))
    case e: RegExpExtract =>
      function == "regexp_extract" && isSource(e.subject, StringType) &&
        literalEquals(e.regexp, options.get(PATTERN_OPTION)) && literalEquals(e.idx, options.get(REGEX_GROUP_INDEX_OPTION))
    case e: StringSplit =>
      function == "split" && isSource(e.str, StringType) && literalEquals(e.limit, Some("-1")) &&
        literalEquals(e.regex, options.get(PATTERN_OPTION))

    case _ => false
  })

  private def isSourceColumn(column: AttributeReference): Boolean = column.name == sourceColumn

  /**
   * The function's input: the column, or the cast Spark adds to `coercedType` when the function does not accept the
   * column's type. The writer applied the same function to the same column type, so it went through the same cast. Any
   * other cast is the query's own, and the index holds no stats for it.
   *
   * @param acceptedTypes other types the function takes as is: Spark adds no cast to a column of one of these
   */
  private def isSource(e: Expression, coercedType: DataType, acceptedTypes: DataType*): Boolean = e match {
    case column: AttributeReference => isSourceColumn(column)
    case cast: Cast if cast.dataType == coercedType => cast.child match {
      case column: AttributeReference => isSourceColumn(column) && !acceptedTypes.contains(column.dataType)
      case _ => false
    }
    case _ => false
  }

  /** The input of a function that parses strings and takes dates and timestamps as is. */
  private def isParseSource(e: Expression): Boolean = isSource(e, StringType, DateType, TimestampType, TimestampNTZType)

  private def isFormatLessParseOf(e: Expression): Boolean = e match {
    case column: AttributeReference => isSourceColumn(column) && optionalLiteralEquals(None, FORMAT_OPTION)
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
