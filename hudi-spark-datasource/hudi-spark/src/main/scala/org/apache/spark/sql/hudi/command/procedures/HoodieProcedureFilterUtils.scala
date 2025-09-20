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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Utility object for filtering procedure results using SQL expressions.
 *
 * Supports all Spark SQL data types including:
 * - Primitive types: Boolean, Byte, Short, Int, Long, Float, Double, String, Binary
 * - Date/Time types: Date, Timestamp, Instant, LocalDate, LocalDateTime
 * - Decimal types: BigDecimal with precision/scale
 * - Complex types: Array, Map, Struct (Row)
 * - Nested combinations of all above types
 */
object HoodieProcedureFilterUtils {

  /**
   * Evaluates a SQL filter expression against a sequence of rows.
   *
   * @param rows             The rows to filter
   * @param filterExpression SQL expression string
   * @param schema           The schema of the rows
   * @param sparkSession     Spark session for expression parsing
   * @return Filtered rows that match the expression
   */
  def evaluateFilter(rows: Seq[Row], filterExpression: String, schema: StructType, sparkSession: SparkSession): Seq[Row] = {

    if (filterExpression == null || filterExpression.trim.isEmpty) {
      rows
    } else {
      Try {
        val parsedExpr = sparkSession.sessionState.sqlParser.parseExpression(filterExpression)

        rows.filter { row =>
          evaluateExpressionOnRow(parsedExpr, row, schema)
        }
      } match {
        case Success(filteredRows) => filteredRows
        case Failure(exception) =>
          throw new IllegalArgumentException(
            s"Failed to parse or evaluate filter expression '$filterExpression': ${exception.getMessage}",
            exception
          )
      }
    }
  }

  private def evaluateExpressionOnRow(expression: Expression, row: Row, schema: StructType): Boolean = {

    val internalRow = convertRowToInternalRow(row, schema)

    Try {
      // First pass: bind attributes
      val attributeBound = expression.transform {
        case attr: org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute =>
          try {
            val fieldIndex = schema.fieldIndex(attr.name)
            val field = schema.fields(fieldIndex)
            org.apache.spark.sql.catalyst.expressions.BoundReference(fieldIndex, field.dataType, field.nullable)
          } catch {
            case _: IllegalArgumentException => attr
          }
      }

      // Second pass: resolve functions
      val functionResolved = attributeBound.transform {
        case unresolvedFunc: org.apache.spark.sql.catalyst.analysis.UnresolvedFunction =>
          unresolvedFunc.nameParts.head.toLowerCase match {
            case "upper" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Upper(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "lower" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Lower(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "length" | "len" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Length(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "trim" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.StringTrim(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "ltrim" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.StringTrimLeft(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "rtrim" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.StringTrimRight(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "substring" | "substr" =>
              if (unresolvedFunc.arguments.length == 3) {
                org.apache.spark.sql.catalyst.expressions.Substring(
                  unresolvedFunc.arguments(0),
                  unresolvedFunc.arguments(1),
                  unresolvedFunc.arguments(2)
                )
              } else {
                unresolvedFunc
              }
            case "abs" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Abs(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "round" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Round(unresolvedFunc.arguments.head, org.apache.spark.sql.catalyst.expressions.Literal(0))
              } else if (unresolvedFunc.arguments.length == 2) {
                org.apache.spark.sql.catalyst.expressions.Round(unresolvedFunc.arguments(0), unresolvedFunc.arguments(1))
              } else {
                unresolvedFunc
              }
            case "ceil" | "ceiling" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Ceil(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "floor" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Floor(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "year" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Year(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "month" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Month(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "day" | "dayofmonth" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.DayOfMonth(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "hour" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Hour(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "size" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Size(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "map_keys" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.MapKeys(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "map_values" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.MapValues(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "array_contains" =>
              if (unresolvedFunc.arguments.length == 2) {
                org.apache.spark.sql.catalyst.expressions.ArrayContains(
                  unresolvedFunc.arguments.head,
                  unresolvedFunc.arguments(1)
                )
              } else {
                unresolvedFunc
              }
            case "array_size" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Size(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "sort_array" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.SortArray(
                  unresolvedFunc.arguments.head,
                  org.apache.spark.sql.catalyst.expressions.Literal(true)
                )
              } else if (unresolvedFunc.arguments.length == 2) {
                org.apache.spark.sql.catalyst.expressions.SortArray(
                  unresolvedFunc.arguments.head,
                  unresolvedFunc.arguments(1)
                )
              } else {
                unresolvedFunc
              }
            case "like" =>
              if (unresolvedFunc.arguments.length == 2) {
                org.apache.spark.sql.catalyst.expressions.Like(
                  unresolvedFunc.arguments.head,
                  unresolvedFunc.arguments(1),
                  '\\'
                )
              } else {
                unresolvedFunc
              }
            case "rlike" | "regexp_like" =>
              if (unresolvedFunc.arguments.length == 2) {
                org.apache.spark.sql.catalyst.expressions.RLike(
                  unresolvedFunc.arguments.head,
                  unresolvedFunc.arguments(1)
                )
              } else {
                unresolvedFunc
              }
            case "regexp_extract" =>
              if (unresolvedFunc.arguments.length == 3) {
                org.apache.spark.sql.catalyst.expressions.RegExpExtract(
                  unresolvedFunc.arguments.head,
                  unresolvedFunc.arguments(1),
                  unresolvedFunc.arguments(2)
                )
              } else {
                unresolvedFunc
              }
            case "date_format" =>
              if (unresolvedFunc.arguments.length == 2) {
                org.apache.spark.sql.catalyst.expressions.DateFormatClass(
                  unresolvedFunc.arguments.head,
                  unresolvedFunc.arguments(1)
                )
              } else {
                unresolvedFunc
              }
            case "datediff" =>
              if (unresolvedFunc.arguments.length == 2) {
                org.apache.spark.sql.catalyst.expressions.DateDiff(
                  unresolvedFunc.arguments.head,
                  unresolvedFunc.arguments(1)
                )
              } else {
                unresolvedFunc
              }
            case "isnull" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.IsNull(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "isnotnull" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.IsNotNull(unresolvedFunc.arguments.head)
              } else {
                unresolvedFunc
              }
            case "coalesce" =>
              if (unresolvedFunc.arguments.nonEmpty) {
                org.apache.spark.sql.catalyst.expressions.Coalesce(unresolvedFunc.arguments)
              } else {
                unresolvedFunc
              }
            case "string" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Cast(
                  unresolvedFunc.arguments.head,
                  org.apache.spark.sql.types.StringType
                )
              } else {
                unresolvedFunc
              }
            case "int" | "integer" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Cast(
                  unresolvedFunc.arguments.head,
                  org.apache.spark.sql.types.IntegerType
                )
              } else {
                unresolvedFunc
              }
            case "long" | "bigint" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Cast(
                  unresolvedFunc.arguments.head,
                  org.apache.spark.sql.types.LongType
                )
              } else {
                unresolvedFunc
              }
            case "double" =>
              if (unresolvedFunc.arguments.length == 1) {
                org.apache.spark.sql.catalyst.expressions.Cast(
                  unresolvedFunc.arguments.head,
                  org.apache.spark.sql.types.DoubleType
                )
              } else {
                unresolvedFunc
              }
            case "between" =>
              // This is needed for Spark 4 to properly parse BETWEEN expression
              if (unresolvedFunc.arguments.length == 3) {
                // Convert BETWEEN to >= AND <=
                // between(expr, lower, upper) -> (expr >= lower) AND (expr <= upper)
                val expr = unresolvedFunc.arguments(0)
                val lower = unresolvedFunc.arguments(1)
                val upper = unresolvedFunc.arguments(2)
                org.apache.spark.sql.catalyst.expressions.And(
                  org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual(expr, lower),
                  org.apache.spark.sql.catalyst.expressions.LessThanOrEqual(expr, upper)
                )
              } else {
                unresolvedFunc
              }
            case _ => unresolvedFunc
          }
      }

      // Third pass: handle type coercion for numeric comparisons
      val boundExpr = functionResolved.transformUp {
        case eq: org.apache.spark.sql.catalyst.expressions.EqualTo =>
          applyTypeCoercion(eq.left, eq.right, org.apache.spark.sql.catalyst.expressions.EqualTo.apply, eq)
        case gt: org.apache.spark.sql.catalyst.expressions.GreaterThan =>
          applyTypeCoercion(gt.left, gt.right, org.apache.spark.sql.catalyst.expressions.GreaterThan.apply, gt)
        case gte: org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual =>
          applyTypeCoercion(gte.left, gte.right, org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual.apply, gte)
        case lt: org.apache.spark.sql.catalyst.expressions.LessThan =>
          applyTypeCoercion(lt.left, lt.right, org.apache.spark.sql.catalyst.expressions.LessThan.apply, lt)
        case lte: org.apache.spark.sql.catalyst.expressions.LessThanOrEqual =>
          applyTypeCoercion(lte.left, lte.right, org.apache.spark.sql.catalyst.expressions.LessThanOrEqual.apply, lte)
      }
      val result = boundExpr.eval(internalRow)

      result match {
        case null => false
        case boolean: Boolean => boolean
        case other =>
          other.toString.toLowerCase match {
            case "true" => true
            case "false" => false
            case _ => false
          }
      }
    } match {
      case Success(result) => result
      case Failure(_) => false
    }
  }

  private def convertRowToInternalRow(row: Row, schema: StructType): GenericInternalRow = {
    val values = schema.fields.zipWithIndex.map { case (field, index) =>
      if (row.isNullAt(index)) {
        null
      } else {
        convertValueToInternal(row.get(index), field.dataType)
      }
    }
    new GenericInternalRow(values)
  }

  private def convertValueToInternal(value: Any, dataType: DataType): Any = {
    import org.apache.spark.sql.types._

    value match {
      case null => null
      case s: String => UTF8String.fromString(s)
      case ts: java.sql.Timestamp => DateTimeUtils.fromJavaTimestamp(ts)
      case date: java.sql.Date => DateTimeUtils.fromJavaDate(date)
      case instant: java.time.Instant => DateTimeUtils.instantToMicros(instant)
      case localDate: java.time.LocalDate => DateTimeUtils.localDateToDays(localDate)
      case localDateTime: java.time.LocalDateTime => DateTimeUtils.localDateTimeToMicros(localDateTime)
      case byte: Byte => byte
      case short: Short => short
      case int: Int => int
      case long: Long => long
      case float: Float => float
      case double: Double => double
      case decimal: java.math.BigDecimal =>
        org.apache.spark.sql.types.Decimal(decimal, dataType.asInstanceOf[DecimalType].precision, dataType.asInstanceOf[DecimalType].scale)
      case decimal: scala.math.BigDecimal =>
        org.apache.spark.sql.types.Decimal(decimal, dataType.asInstanceOf[DecimalType].precision, dataType.asInstanceOf[DecimalType].scale)
      case bool: Boolean => bool
      case bytes: Array[Byte] => bytes
      case array: Array[_] =>
        val arrayType = dataType.asInstanceOf[ArrayType]
        array.map(convertValueToInternal(_, arrayType.elementType))
      case list: java.util.List[_] =>
        val arrayType = dataType.asInstanceOf[ArrayType]
        list.asScala.map(convertValueToInternal(_, arrayType.elementType)).toArray
      case seq: Seq[_] =>
        val arrayType = dataType.asInstanceOf[ArrayType]
        seq.map(convertValueToInternal(_, arrayType.elementType)).toArray
      case map: java.util.Map[_, _] =>
        val mapType = dataType.asInstanceOf[MapType]
        val convertedKeys = map.asScala.keys.map(convertValueToInternal(_, mapType.keyType)).toArray
        val convertedValues = map.asScala.values.map(convertValueToInternal(_, mapType.valueType)).toArray
        org.apache.spark.sql.catalyst.util.ArrayBasedMapData(convertedKeys, convertedValues)
      case map: scala.collection.Map[_, _] =>
        val mapType = dataType.asInstanceOf[MapType]
        val convertedKeys = map.keys.map(convertValueToInternal(_, mapType.keyType)).toArray
        val convertedValues = map.values.map(convertValueToInternal(_, mapType.valueType)).toArray
        org.apache.spark.sql.catalyst.util.ArrayBasedMapData(convertedKeys, convertedValues)
      case row: org.apache.spark.sql.Row =>
        val structType = dataType.asInstanceOf[StructType]
        val values = structType.fields.zipWithIndex.map { case (field, index) =>
          if (row.isNullAt(index)) {
            null
          } else {
            convertValueToInternal(row.get(index), field.dataType)
          }
        }
        new GenericInternalRow(values)
      case utf8: UTF8String => utf8
      case internalRow: org.apache.spark.sql.catalyst.InternalRow => internalRow
      case mapData: org.apache.spark.sql.catalyst.util.MapData => mapData
      case arrayData: org.apache.spark.sql.catalyst.util.ArrayData => arrayData
      case decimal: org.apache.spark.sql.types.Decimal => decimal
      case uuid: java.util.UUID => UTF8String.fromString(uuid.toString)
      case other => other
    }
  }

  def validateFilterExpression(filterExpression: String, schema: StructType, sparkSession: SparkSession): Either[String, Unit] = {

    if (filterExpression == null || filterExpression.trim.isEmpty) {
      Right(())
    } else {
      Try {
        val parsedExpr = sparkSession.sessionState.sqlParser.parseExpression(filterExpression)
        val columnNames = schema.fieldNames.toSet
        val referencedColumns = extractColumnReferences(parsedExpr)
        val invalidColumns = referencedColumns -- columnNames

        if (invalidColumns.nonEmpty) {
          Left(s"Invalid column references: ${invalidColumns.mkString(", ")}. Available columns: ${columnNames.mkString(", ")}")
        } else {
          Right(())
        }
      } match {
        case Success(result) => result
        case Failure(exception) => Left(s"Invalid filter expression: ${exception.getMessage}")
      }
    }
  }

  private def extractColumnReferences(expression: Expression): Set[String] = {
    import org.apache.spark.sql.catalyst.expressions._

    expression match {
      case attr: AttributeReference => Set(attr.name)
      case unresolved: UnresolvedAttribute => Set(unresolved.name)
      case _ => expression.children.flatMap(extractColumnReferences).toSet
    }
  }

  private def applyTypeCoercion[T <: org.apache.spark.sql.catalyst.expressions.Expression](
                                                                                            left: org.apache.spark.sql.catalyst.expressions.Expression,
                                                                                            right: org.apache.spark.sql.catalyst.expressions.Expression,
                                                                                            constructor: (org.apache.spark.sql.catalyst.expressions.Expression, org.apache.spark.sql.catalyst.expressions.Expression) => T,
                                                                                            original: T): T = {
    (left, right) match {
      case (boundRef: org.apache.spark.sql.catalyst.expressions.BoundReference, literal: org.apache.spark.sql.catalyst.expressions.Literal)
        if boundRef.dataType == org.apache.spark.sql.types.LongType && literal.dataType == org.apache.spark.sql.types.IntegerType =>
        val castExpr = org.apache.spark.sql.catalyst.expressions.Cast(boundRef, org.apache.spark.sql.types.IntegerType)
        constructor(castExpr, literal)
      case _ => original
    }
  }
}

