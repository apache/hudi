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

import scala.util.{Failure, Success, Try}

object HoodieProcedureFilterUtils {

  def evaluateFilter(
                      rows: Seq[Row],
                      filterExpression: String,
                      schema: StructType,
                      sparkSession: SparkSession
                    ): Seq[Row] = {

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

  private def evaluateExpressionOnRow(
                                       expression: Expression,
                                       row: Row,
                                       schema: StructType
                                     ): Boolean = {

    val internalRow = convertRowToInternalRow(row, schema)

    val boundExpr = try {
      expression.transform {
        case attr: org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute =>
          schema.fieldIndex(attr.name) match {
            case fieldIndex =>
              val field = schema.fields(fieldIndex)
              org.apache.spark.sql.catalyst.expressions.BoundReference(fieldIndex, field.dataType, field.nullable)
          }
      }
    } catch {
      case _: Exception => expression
    }

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
      case s: java.sql.Timestamp => DateTimeUtils.fromJavaTimestamp(s)
      case s: java.sql.Date => DateTimeUtils.fromJavaDate(s)
      case array: Array[_] =>
        array.map(convertValueToInternal(_, dataType.asInstanceOf[ArrayType].elementType))
      case other => other
    }
  }

  def validateFilterExpression(
                                filterExpression: String,
                                schema: StructType,
                                sparkSession: SparkSession
                              ): Either[String, Unit] = {

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
}
