/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.merge

import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, ExtractValue, GetStructField, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.util.SchemaUtils

import scala.collection.mutable

/**
  * Trait that represents a WHEN clause in MERGE. It extends [[Expression]]
  * so that Catalyst can find all the expresions in the clause implementations
  */
sealed trait HudiMergeClause extends Expression with Unevaluable {
  // condition of the clause
  def condition: Option[Expression]

  def actions: Seq[Expression]

  def isStarAction: Boolean

  def resolvedActions: Seq[HudiMergeAction] = {
    actions.map(_.asInstanceOf[HudiMergeAction])
  }

  override def foldable: Boolean = false

  override def nullable: Boolean = false

  override def dataType: DataType = null

  override def children: Seq[Expression] = condition.toSeq ++ actions

}

/** Represents the clause WHEN MATCHED clause in MERGE. */
sealed trait HudiMatchedClause extends HudiMergeClause

/** Represents the clause WHEN MATCHED THEN UPDATE in MERGE. */
case class HudiMergeUpdateClause(condition: Option[Expression], actions: Seq[Expression], IsStarAction: Boolean = false) extends HudiMatchedClause {
  override def isStarAction: Boolean = IsStarAction
}

/** Represents the clause WHEN MATCHED THEN DELETE in MERGE. */
case class HudiMergeDeleteClause(condition: Option[Expression])
  extends HudiMatchedClause {

  def this(condition: Option[Expression], actions: Seq[Expression], IsStarAction: Boolean = false) = this(condition)

  override def isStarAction: Boolean = false

  override def actions: Seq[Expression] = Seq.empty
}

/** Represents the clause WHEN NOT MATCHED THEN INSERT in MERGE. */
case class HudiMergeInsertClause(condition: Option[Expression], actions: Seq[Expression], IsStarActions: Boolean = false)
  extends HudiMergeClause {

  override def isStarAction: Boolean = IsStarActions

}

case class HudiMergeIntoTable(
    target: LogicalPlan,
    source: LogicalPlan,
    joinConditon: Expression,
    matchedClauses: Seq[HudiMatchedClause],
    noMatchedClauses: Seq[HudiMergeInsertClause],
    finalSchema: StructType = null) extends Command with SupportsSubquery {

  override def children: Seq[LogicalPlan] = Seq(target, source)

  override def output: Seq[Attribute] = Seq.empty
}

/**
  * Represents an action in MErGE's UPDATE or INSERT clause where a target columns is assigned the value of an expression
  * @param targetColNameParts The name parts of the target column. This is a sequence to support nested fields as targets
  * @param expr Expression to generate the value of the target column.
  */
case class HudiMergeAction(targetColNameParts: Seq[String], expr: Expression)
  extends UnaryExpression with Unevaluable {

  override def child: Expression = expr

  override def foldable: Boolean = false

  override def dataType: DataType = expr.dataType

  override def sql: String = s"${targetColNameParts.mkString("`", "`.`", "`")} = ${expr.sql}"

  override def toString(): String = s"$prettyName ( $sql )"
}

/**  utils function for merge operation */
object HudiMergeIntoUtils {
  /** LogicalPlan to help resolve the given expression */
  case class FakeLogicalPlan(expr: Expression, children: Seq[LogicalPlan]) extends LogicalPlan {
    override def output: Seq[Attribute] = Nil
  }

  def tryResolveSubquery(spark: SparkSession, plan: LogicalPlan): LogicalPlan = {
    spark.sessionState.analyzer.ResolveSubquery(spark.sessionState.analyzer.execute(plan))
  }

  def tryResolveReferences(spark: SparkSession)
                          (expr: Expression, plan: LogicalPlan): Expression = {
    val newPlan = FakeLogicalPlan(expr, plan.children)
    spark.sessionState.analyzer.execute(newPlan) match {
      case FakeLogicalPlan(resolveExpr, _) =>
        // scalastyle:off
        resolveExpr
        // scalastyle:on
      case _ =>
        throw new AnalysisException(s"cannot resolve expression ${expr}")
    }
  }

  def convertToActions(assignments: Seq[Assignment]): Seq[Expression] = {
    assignments.map {
      case Assignment(key: Attribute, expr) => HudiMergeAction(Seq(key.name), expr)
      case other =>
        throw new AnalysisException(s"Unexpected assignment key: ${other}")
    }
  }

  /**
    * Extracts name from a resolved expression referring to a nested or non-nested column
    */
  def getTargetColNameParts(resolvedTargetCol: Expression): Seq[String] = {
    resolvedTargetCol match {
      case attr: Attribute => Seq(attr.name)
      case Alias(c, _) => getTargetColNameParts(c)
      case GetStructField(c, _, Some(name)) => getTargetColNameParts(c) :+ name
      case ex: ExtractValue =>
        throw new AnalysisException(s"convert reference to name failed, Updating nested fields is not support for StructType: ${ex}")
      case other =>
        throw new AnalysisException(s"convert reference to name failed, Found unsupported expression: ${other}")
    }
  }

  def resolveReferences(merge: HudiMergeIntoTable, conf: SQLConf)
                       (resolveExpr: (Expression, LogicalPlan) => Expression): HudiMergeIntoTable = {
    val HudiMergeIntoTable(target, source, joinCondition, matchedClauses, notMatchedClause, _) = merge

    val fakeSourcePlan = Project(source.output, source)
    val fakeTargetPlan = Project(target.output, target)

    val finalSchema = {
      val migratedSchema = mutable.ListBuffer[StructField]()
      target.schema.foreach(migratedSchema.append(_))
      source.schema.filterNot { col =>
        target.schema.exists(targetCol => target.conf.resolver(targetCol.name, col.name))
      }.foreach(migratedSchema.append(_))
      StructType(migratedSchema)
    }

    def resolveOrFail(expr: Expression, plan: LogicalPlan): Expression = {
      val resolvedExpr = resolveExpr(expr, plan)
      resolvedExpr.flatMap(_.references).filter(!_.resolved).foreach { a =>
        val cols = "columns" + plan.children.flatMap(_.output).map(_.sql).mkString(", ")
        throw new AnalysisException(s"cannot resolve ${a.sql} fro give ${cols}")
      }
      resolvedExpr
    }

    def resolveClause[T <: HudiMergeClause](clause: T, planToResolveAction: LogicalPlan): T = {
      val resolvedActions: Seq[HudiMergeAction] = clause.actions.flatMap { action =>
        action match {
          case HudiMergeAction(colNameParts, expr) =>
            // extract nested field
            val unresolvedAttribute = UnresolvedAttribute(colNameParts)
            val resolvedNameParts = getTargetColNameParts(
              try {
                resolveOrFail(unresolvedAttribute, fakeTargetPlan)
              } catch {
                case _: AnalysisException => resolveOrFail(unresolvedAttribute, fakeSourcePlan)
                case _ => throw new AnalysisException(s"cannot solve col ${colNameParts.toString()} for target table ${fakeTargetPlan.toString()}")
              }
            )
            Seq(HudiMergeAction(resolvedNameParts, expr))
          case _ =>
            throw new AnalysisException("find unexpected merge actions")
        }
      }
      val resolveCondition = clause.condition.map(resolveOrFail(_, planToResolveAction))
      clause.makeCopy(Array(resolveCondition, resolvedActions, clause.isStarAction.asInstanceOf[AnyRef])).asInstanceOf[T]
    }

    val resolvedMatchedClauses = matchedClauses.map {
      resolveClause(_, merge)
    }

    val resolvedNotMatchedClause = notMatchedClause.map {
      resolveClause(_, fakeSourcePlan)
    }
    HudiMergeIntoTable(target, source, joinCondition, resolvedMatchedClauses, resolvedNotMatchedClause, finalSchema)
  }

  /**
   *  do normalize for table partitionSpec
   */
  def normalizePartitionSpec[T](
      partitionSpec: Map[String, T],
      partColNames: Seq[String],
      tblName: String,
      resolver: Resolver): Map[String, T] = {
    val normalizedPartSpec = partitionSpec.toSeq.map { case (key, value) =>
      val normalizedKey = partColNames.find(resolver(_, key)).getOrElse {
        throw new AnalysisException(s"${key} is not a valid partition column in table ${tblName}")
      }
      normalizedKey -> value
    }

    SchemaUtils.checkColumnNameDuplication(
      normalizedPartSpec.map(_._1), "in the partition schema", resolver)

    normalizedPartSpec.toMap
  }

}
