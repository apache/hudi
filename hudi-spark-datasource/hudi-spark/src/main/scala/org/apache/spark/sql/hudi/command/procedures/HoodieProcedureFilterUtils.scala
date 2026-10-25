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
import org.apache.spark.sql.catalyst.analysis.{AnsiTypeCoercion, DecimalPrecision, TypeCoercion, UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{BinaryArithmetic, BinaryComparison, Cast, Coalesce, Divide, EqualNullSafe, Expression, GenericInternalRow, In, IntegralDivide, Unevaluable}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DecimalType, DoubleType, IntegerType, LongType, NullType, NumericType, ShortType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.time.DateTimeException
import java.util.Locale

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

        // Binding and resolution depend only on the schema, so run the three passes once for the
        // whole batch instead of per row.
        val boundExpr = bindAndResolveExpression(parsedExpr, schema)
        rows.filter(row => evaluateExpressionOnRow(boundExpr, row, schema))
      } match {
        case Success(filteredRows) => filteredRows
        // Surface an overflowing ANSI cast or arithmetic, or an ANSI cast of a malformed string,
        // with Spark's own exception rather than restating it as a filter-expression problem: the
        // expression is fine, the data does not fit.
        case Failure(e @ (_: ArithmeticException | _: NumberFormatException | _: DateTimeException)) => throw e
        case Failure(exception) =>
          throw new IllegalArgumentException(
            s"Failed to parse or evaluate filter expression '$filterExpression': ${exception.getMessage}",
            exception
          )
      }
    }
  }

  private def bindAndResolveExpression(expression: Expression, schema: StructType): Expression = {
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
          unresolvedFunc.nameParts.head.toLowerCase(Locale.ROOT) match {
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
    functionResolved.transformUp {
      case eq: org.apache.spark.sql.catalyst.expressions.EqualTo =>
        applyTypeCoercion(eq)
      case gt: org.apache.spark.sql.catalyst.expressions.GreaterThan =>
        applyTypeCoercion(gt)
      case gte: org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual =>
        applyTypeCoercion(gte)
      case lt: org.apache.spark.sql.catalyst.expressions.LessThan =>
        applyTypeCoercion(lt)
      case lte: org.apache.spark.sql.catalyst.expressions.LessThanOrEqual =>
        applyTypeCoercion(lte)
      case eqns: EqualNullSafe =>
        applyTypeCoercion(eqns)
      case in: In =>
        applyInTypeCoercion(in)
      // Divide and IntegralDivide are BinaryArithmetic but accept only Double or Decimal, and only
      // Long or Decimal, respectively, so each needs its own target type and has to be matched
      // before the general arithmetic case below.
      case divide: Divide =>
        applyDivideTypeCoercion(divide)
      case idiv: IntegralDivide =>
        applyIntegralDivideTypeCoercion(idiv)
      case arith: BinaryArithmetic =>
        applyArithmeticTypeCoercion(arith)
      case coalesce: Coalesce =>
        applyCoalesceTypeCoercion(coalesce)
    }
  }

  private def evaluateExpressionOnRow(boundExpr: Expression, row: Row, schema: StructType): Boolean = {

    val internalRow = convertRowToInternalRow(row, schema)

    Try {
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
      // Spark raises SparkArithmeticException for an overflowing ANSI cast or arithmetic, and
      // SparkNumberFormatException or SparkDateTimeException for an ANSI cast of a malformed
      // string; each extends the matching JDK type. Swallowing one would silently drop a row the
      // same query keeps, so let it out and let the caller fail the way the equivalent query does.
      case Failure(e @ (_: ArithmeticException | _: NumberFormatException | _: DateTimeException)) => throw e
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
        val resolvedExpr = bindAndResolveExpression(parsedExpr, schema)
        val unsupportedFunctions = extractFunctionReferences(resolvedExpr)
        val unsupportedExpressions = resolvedExpr.collect {
          case expression: Unevaluable
            if !expression.isInstanceOf[UnresolvedAttribute]
              && !expression.isInstanceOf[UnresolvedFunction] => expression.prettyName
        }.toSet

        if (invalidColumns.nonEmpty) {
          Left(s"Invalid column references: ${invalidColumns.mkString(", ")}. Available columns: ${columnNames.mkString(", ")}")
        } else if (unsupportedFunctions.nonEmpty) {
          Left(s"Unsupported functions: ${unsupportedFunctions.toSeq.sorted.mkString(", ")}")
        } else if (!resolvedExpr.resolved || unsupportedExpressions.nonEmpty) {
          val names = unsupportedExpressions.toSeq.sorted
          val detail = if (names.nonEmpty) s": ${names.mkString(", ")}" else ""
          Left(s"Unsupported filter expression$detail")
        } else if (resolvedExpr.dataType != BooleanType) {
          // Spark rejects any non-boolean filter condition, string included, with
          // DATATYPE_MISMATCH.FILTER_NOT_BOOLEAN. Without this a resolvable expression such as
          // "ts + 1" would report zero matching rows instead of the error the same query raises.
          Left(s"Filter expression must be boolean, got ${resolvedExpr.dataType.simpleString}")
        } else {
          Right(())
        }
      } match {
        case Success(result) => result
        case Failure(exception) => Left(s"Invalid filter expression: ${exception.getMessage}")
      }
    }
  }

  private def extractFunctionReferences(expression: Expression): Set[String] = expression match {
    case unresolved: UnresolvedFunction =>
      Set(unresolved.nameParts.mkString(".")) ++ unresolved.children.flatMap(extractFunctionReferences)
    case _ => expression.children.flatMap(extractFunctionReferences).toSet
  }

  private def extractColumnReferences(expression: Expression): Set[String] = {
    import org.apache.spark.sql.catalyst.expressions._

    expression match {
      case attr: AttributeReference => Set(attr.name)
      case unresolved: UnresolvedAttribute => Set(unresolved.name)
      case _ => expression.children.flatMap(extractColumnReferences).toSet
    }
  }

  private def applyTypeCoercion(original: BinaryComparison): Expression = {
    if (!original.childrenResolved) {
      original
    } else {
      // Spark can replace an integral/decimal-literal inequality with an integral comparison,
      // avoiding a lossy cast of the column. It also gives integral literals minimum decimal
      // precision before finding the common comparison type.
      val promoted = DecimalPrecision.transform.applyOrElse(original, identity[Expression])
      // Mixed decimal/integral promotion creates two decimal operands. Apply the decimal-pair
      // rule next, just as a subsequent analyzer iteration would.
      val comparison = DecimalPrecision.transform.applyOrElse(promoted, identity[Expression])
      comparison match {
        case binary: BinaryComparison =>
          widenOperands(Seq(binary.left, binary.right))
            .map(binary.withNewChildren).getOrElse(binary)
        case other => other
      }
    }
  }

  private def applyInTypeCoercion(in: In): Expression = {
    widenOperands(in.value +: in.list) match {
      case Some(widened) => In(widened.head, widened.tail)
      case _ => in
    }
  }

  /**
   * Arithmetic keeps its decimal operands exactly as they are. Unlike a comparison,
   * BinaryArithmetic.checkInputDataTypes accepts two decimals of different precision and scale and
   * derives the result type from them, so widening to a common type changes the answer rather than
   * enabling it: DECIMAL(38,18) * DECIMAL(2,1) yields a scale-16 product, while casting both to
   * DECIMAL(38,18) first drives the product to scale 6 and rounds 0.0000001 away to zero.
   *
   * Spark's DecimalPrecision rule promotes integral operands without changing the existing
   * decimal's type, including minimum precision for integral literals. It also promotes decimals
   * mixed with floating-point operands to Double. Null operands take the other operand's type.
   */
  private def applyArithmeticTypeCoercion(arith: BinaryArithmetic): Expression = {
    val operands = Seq(arith.left, arith.right)
    if (operands.exists(!_.resolved) || !operands.forall(operand => isNumericOrNull(operand.dataType))) {
      arith
    } else {
      val decimalOperands = operands.exists(_.dataType.isInstanceOf[DecimalType])
      val promoted = if (decimalOperands) {
        DecimalPrecision.transform.applyOrElse(arith, identity[Expression])
      } else {
        arith
      }
      promoted match {
        case binary: BinaryArithmetic =>
          val children = Seq(binary.left, binary.right)
          val widened = if (children.forall(_.dataType.isInstanceOf[DecimalType])) {
            binary
          } else {
            widenOperands(children)
              .map(binary.withNewChildren).getOrElse(binary)
          }
          // Spark 3.3 wraps decimal arithmetic in CheckOverflow after operand promotion.
          // Later versions calculate the result precision within BinaryArithmetic itself.
          if (decimalOperands) DecimalPrecision.transform.applyOrElse(widened, identity[Expression]) else widened
        case other => other
      }
    }
  }

  /**
   * Divide only accepts Double or Decimal, so widening its operands to their common numeric type
   * leaves an integral pair unresolved and "ts / 2 > 500" rejected while "price / 2 > 5" works.
   * Mirror the analyzer's Division rule instead: leave a pair that involves a decimal to the same
   * widening as the other arithmetic, and promote everything else to Double. Like Spark, that
   * includes a null operand, so "ts / null" resolves and evaluates to null rather than failing
   * validation. The rule is unchanged between the two majors this builds against, only relocated:
   *
   * 3.5.5 object Division, in
   * https://github.com/apache/spark/blob/v3.5.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TypeCoercion.scala
   * 4.1.1
   * https://github.com/apache/spark/blob/v4.1.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/DivisionTypeCoercion.scala
   */
  private def applyDivideTypeCoercion(divide: Divide): Expression = {
    val operands = Seq(divide.left, divide.right)
    if (operands.exists(!_.resolved) || !operands.forall(operand => isNumericOrNull(operand.dataType))) {
      divide
    } else if (operands.exists(_.dataType.isInstanceOf[DecimalType])) {
      applyArithmeticTypeCoercion(divide)
    } else {
      divide.withNewChildren(operands.map(operand => castTo(operand, DoubleType)))
    }
  }

  /**
   * IntegralDivide accepts only Long or Decimal, and its operands are never widened against each
   * other, so a same-typed Int pair leaves "id div 2" unresolved while "ts div 2" resolves. Mirror
   * the analyzer's IntegralDivision rule, which promotes each narrower integral operand on its own
   * before the arithmetic widening runs.
   */
  private def applyIntegralDivideTypeCoercion(divide: IntegralDivide): Expression = {
    if (!divide.childrenResolved) {
      divide
    } else {
      val promoted = divide.withNewChildren(Seq(divide.left, divide.right).map { operand =>
        operand.dataType match {
          case ByteType | ShortType | IntegerType => castTo(operand, LongType)
          case _ => operand
        }
      })
      promoted match {
        case arith: BinaryArithmetic => applyArithmeticTypeCoercion(arith)
        case other => other
      }
    }
  }

  /** Spark's own guard for the numeric coercion rules, which admit a null literal. */
  private def isNumericOrNull(dataType: DataType): Boolean =
    dataType.isInstanceOf[NumericType] || dataType.isInstanceOf[NullType]

  private def applyCoalesceTypeCoercion(coalesce: Coalesce): Expression = {
    widenOperands(coalesce.children) match {
      case Some(widened) => Coalesce(widened)
      case _ => coalesce
    }
  }

  /**
   * Widens comparison operands of differing numeric types to their common wider type, so that
   * e.g. a LongType column compares against an IntegerType literal on the widened Long rather
   * than narrowing the column. A NullType operand takes the type of its peers whatever that type
   * is, the way Spark plans `ts IN (1000, null)` and `name IN ('a1', null)`. Returns None when the
   * operands need no widening or cannot be widened, in which case the caller keeps the expression
   * untouched.
   *
   * Numeric conversion can still lose precision:
   *  - Large integers may round when converted to Float or Double. For example, Long 16777217
   *    becomes Float 16777216.
   *  - Large integers may overflow when converted to a decimal with insufficient space before the
   *    decimal point. For example, DECIMAL(38,20) allows only 18 digits before the decimal point,
   *    so the 19-digit Long 9000000000000000000 does not fit.
   *
   * Rounding can change comparison results, silently, exactly as it does in a query. Overflow
   * follows the session's ANSI mode the way Spark's own Cast does: without ANSI the cast yields
   * null and the row is filtered out, with ANSI it raises and the failure reaches the caller.
   */
  private def widenOperands(operands: Seq[Expression]): Option[Seq[Expression]] = {
    if (operands.exists(!_.resolved)) {
      // dataType throws on an unresolved operand. Leaving it untouched lets validateFilterExpression
      // report its own message ("Invalid column references", "Unsupported functions") instead of an
      // UnresolvedException, and keeps Or/And short-circuiting intact at eval time.
      None
    } else {
      val operandTypes = operands.map(_.dataType)
      val nonNullTypes = operandTypes.filterNot(_.isInstanceOf[NullType]).distinct
      if (operandTypes.distinct.length == 1) {
        None
      } else if (nonNullTypes.length == 1) {
        // Only nulls differ from a single peer type, so the nulls take that type whether or not it
        // is numeric. Nothing else needs widening.
        Some(operands.map(operand => castTo(operand, nonNullTypes.head)))
      } else if (!operandTypes.forall(isNumericOrNull)) {
        None
      } else {
        findWiderNumericType(operandTypes)
          .map(widerType => operands.map(operand => castTo(operand, widerType)))
      }
    }
  }

  /**
   * Mirrors the analyzer's choice of coercion rules, so that a filter widens the way the same
   * comparison would in a SQL query. The two disagree: for BIGINT with FLOAT, AnsiTypeCoercion
   * gives DOUBLE while TypeCoercion follows numericPrecedence and gives FLOAT. Spark 4 defaults
   * to ANSI mode, Spark 3 does not.
   *
   * Reads SQLConf.get rather than the SparkSession because Cast takes its eval mode from that same
   * thread-local at construction, and the two-argument Cast(child, dataType) is the only form that
   * is portable across Spark 3.3 to 4.x (3.3 takes ansiEnabled, 3.4+ takes evalMode).
   *
   * Decimal pairs go through Spark's own precision rules, which likewise only moved between the
   * majors: 3.5.5 analysis/DecimalPrecision.scala, 4.1.1 analysis/DecimalPrecisionTypeCoercion
   * .scala. Parity for a comparison whose common precision would exceed 38 is not settled here;
   * see HUDI #19860.
   */
  private def findWiderNumericType(types: Seq[DataType]): Option[DataType] = {
    if (SQLConf.get.ansiEnabled) {
      AnsiTypeCoercion.findWiderCommonType(types)
    } else {
      TypeCoercion.findWiderCommonType(types)
    }
  }

  private def castTo(expression: Expression, dataType: DataType): Expression = {
    if (expression.dataType == dataType) expression else Cast(expression, dataType)
  }
}
