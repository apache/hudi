/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.hudi.{HoodieCDCFileIndex, SparkAdapterSupport, SparkHoodieTableFileIndex}

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Contains, EndsWith, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, NamedExpression, Not, Or, StartsWith}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.{HoodieFormatTrait, ParquetFileFormat}
import org.apache.spark.sql.types.{BooleanType, StructType}

import scala.util.Try

object FileFormatUtilsForFileGroupReader extends SparkAdapterSupport {

  def applyNewFileFormatChanges(scanOperation: LogicalPlan, logicalRelation: LogicalPlan, fs: HadoopFsRelation): LogicalPlan = {
    val ff = fs.fileFormat.asInstanceOf[ParquetFileFormat with HoodieFormatTrait]
    ff.isProjected = true
    val tableSchema = fs.location match {
      case index: HoodieCDCFileIndex => index.cdcRelation.schema
      case index: SparkHoodieTableFileIndex => index.schema
    }
    val resolvedSchema = logicalRelation.resolve(tableSchema, fs.sparkSession.sessionState.analyzer.resolver)
    val unfilteredPlan = if (!fs.partitionSchema.fields.isEmpty && sparkAdapter.getCatalystPlanUtils.produceSameOutput(scanOperation, logicalRelation)) {
      Project(resolvedSchema, scanOperation)
    } else {
      scanOperation
    }
    applyFiltersToPlan(unfilteredPlan, tableSchema, resolvedSchema, ff.getRequiredFilters)
  }

  /**
   * adapted from https://github.com/apache/spark/blob/20df062d85e80422a55afae80ddbf2060f26516c/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/StructFilters.scala
   */
  def applyFiltersToPlan(plan: LogicalPlan, tableSchema: StructType, resolvedSchema: Seq[Attribute], filters: Seq[org.apache.spark.sql.sources.Filter]): LogicalPlan = {

    def filterToExpression(
                            filter: sources.Filter,
                            toRef: String => Option[NamedExpression]): Option[Expression] = {
      def zipAttributeAndValue(name: String, value: Any): Option[(NamedExpression, Literal)] = {
        zip(toRef(name), toLiteral(value))
      }

      def translate(filter: sources.Filter): Option[Expression] = filter match {
        case sources.And(left, right) =>
          zip(translate(left), translate(right)).map(And.tupled)
        case sources.Or(left, right) =>
          zip(translate(left), translate(right)).map(Or.tupled)
        case sources.Not(child) =>
          translate(child).map(Not)
        case sources.EqualTo(attribute, value) =>
          zipAttributeAndValue(attribute, value).map(EqualTo.tupled)
        case sources.EqualNullSafe(attribute, value) =>
          zipAttributeAndValue(attribute, value).map(EqualNullSafe.tupled)
        case sources.IsNull(attribute) =>
          toRef(attribute).map(IsNull)
        case sources.IsNotNull(attribute) =>
          toRef(attribute).map(IsNotNull)
        case sources.In(attribute, values) =>
          val literals = values.toSeq.flatMap(toLiteral)
          if (literals.length == values.length) {
            toRef(attribute).map(In(_, literals))
          } else {
            None
          }
        case sources.GreaterThan(attribute, value) =>
          zipAttributeAndValue(attribute, value).map(GreaterThan.tupled)
        case sources.GreaterThanOrEqual(attribute, value) =>
          zipAttributeAndValue(attribute, value).map(GreaterThanOrEqual.tupled)
        case sources.LessThan(attribute, value) =>
          zipAttributeAndValue(attribute, value).map(LessThan.tupled)
        case sources.LessThanOrEqual(attribute, value) =>
          zipAttributeAndValue(attribute, value).map(LessThanOrEqual.tupled)
        case sources.StringContains(attribute, value) =>
          zipAttributeAndValue(attribute, value).map(Contains.tupled)
        case sources.StringStartsWith(attribute, value) =>
          zipAttributeAndValue(attribute, value).map(StartsWith.tupled)
        case sources.StringEndsWith(attribute, value) =>
          zipAttributeAndValue(attribute, value).map(EndsWith.tupled)
      case sources.AlwaysTrue() =>
        Some(Literal(true, BooleanType))
      case sources.AlwaysFalse() =>
        Some(Literal(false, BooleanType))
      }

      translate(filter)
    }

    def zip[A, B](a: Option[A], b: Option[B]): Option[(A, B)] = {
      a.zip(b).headOption
    }

    def toLiteral(value: Any): Option[Literal] = {
      Try(Literal(value)).toOption
    }

    def toRef(attr: String): Option[NamedExpression] = {
      tableSchema.getFieldIndex(attr).map { index =>
        resolvedSchema(index)
      }
    }

    val expressionFilters = filters.map(f => filterToExpression(f, n => toRef(n)).get)
    if (expressionFilters.nonEmpty) {
      Filter(expressionFilters.reduceLeft(And), plan)
    } else {
      plan
    }
  }

  def createStreamingDataFrame(sqlContext: SQLContext, relation: HadoopFsRelation, requiredSchema: StructType): DataFrame = {
    val logicalRelation = LogicalRelation(relation, isStreaming = true)
    val resolvedSchema = logicalRelation.resolve(requiredSchema, sqlContext.sparkSession.sessionState.analyzer.resolver)
    Dataset.ofRows(sqlContext.sparkSession, applyFiltersToPlan(logicalRelation, requiredSchema, resolvedSchema,
      relation.fileFormat.asInstanceOf[HoodieFormatTrait].getRequiredFilters))
  }
}
