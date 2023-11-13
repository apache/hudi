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

package org.apache.spark.sql

import org.apache.hudi.SparkAdapterSupport
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableOutputResolver
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, Contains, EndsWith, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, NamedExpression, Not, Or, ProjectionOverSchema, StartsWith}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Filter, InsertIntoStatement, Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.execution.command.{CreateTableLikeCommand, ExplainCommand}
import org.apache.spark.sql.execution.{ExtendedMode, SimpleMode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, StructType}

import scala.util.Try

trait HoodieSpark3CatalystPlanUtils extends HoodieCatalystPlansUtils {

  /**
   * Instantiates [[ProjectionOverSchema]] utility
   */
  def projectOverSchema(schema: StructType, output: AttributeSet): ProjectionOverSchema

  /**
   * Un-applies [[ResolvedTable]] that had its signature changed in Spark 3.2
   */
  def unapplyResolvedTable(plan: LogicalPlan): Option[(TableCatalog, Identifier, Table)]

  def resolveOutputColumns(tableName: String,
                           expected: Seq[Attribute],
                           query: LogicalPlan,
                           byName: Boolean,
                           conf: SQLConf): LogicalPlan =
    TableOutputResolver.resolveOutputColumns(tableName, expected, query, byName, conf)

  override def createExplainCommand(plan: LogicalPlan, extended: Boolean): LogicalPlan =
    ExplainCommand(plan, mode = if (extended) ExtendedMode else SimpleMode)

  override def createJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType): Join = {
    Join(left, right, joinType, None, JoinHint.NONE)
  }

  override def unapplyInsertIntoStatement(plan: LogicalPlan): Option[(LogicalPlan, Map[String, Option[String]], LogicalPlan, Boolean, Boolean)] = {
    plan match {
      case insert: InsertIntoStatement =>
        Some((insert.table, insert.partitionSpec, insert.query, insert.overwrite, insert.ifPartitionNotExists))
      case _ =>
        None
    }
  }


  override def unapplyCreateTableLikeCommand(plan: LogicalPlan): Option[(TableIdentifier, TableIdentifier, CatalogStorageFormat, Option[String], Map[String, String], Boolean)] = {
    plan match {
      case CreateTableLikeCommand(targetTable, sourceTable, fileFormat, provider, properties, ifNotExists) =>
        Some(targetTable, sourceTable, fileFormat, provider, properties, ifNotExists)
      case _ => None
    }
  }

  def rebaseInsertIntoStatement(iis: LogicalPlan, targetTable: LogicalPlan, query: LogicalPlan): LogicalPlan =
    iis.asInstanceOf[InsertIntoStatement].copy(table = targetTable, query = query)

  override def createMITJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType, condition: Option[Expression], hint: String): LogicalPlan = {
    Join(left, right, joinType, condition, JoinHint.NONE)
  }
}

object HoodieSpark3CatalystPlanUtils extends SparkAdapterSupport {

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

    def toRef(attr: String, structSchema: StructType, attrSchema: Seq[Attribute]): Option[NamedExpression] = {
      structSchema.getFieldIndex(attr).map { index =>
        attrSchema(index)
      }
    }

    filters.foldLeft(plan)((p, f) => {
      Filter(filterToExpression(f, n => toRef(n, tableSchema, resolvedSchema)).get, p)
    })
  }

  /**
   * This is an extractor to accommodate for [[ResolvedTable]] signature change in Spark 3.2
   */
  object MatchResolvedTable {
    def unapply(plan: LogicalPlan): Option[(TableCatalog, Identifier, Table)] =
      sparkAdapter.getCatalystPlanUtils match {
        case spark3Utils: HoodieSpark3CatalystPlanUtils => spark3Utils.unapplyResolvedTable(plan)
        case _ => None
      }
  }
}
