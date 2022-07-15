package org.apache.spark.sql

import org.apache.hudi.spark3.internal.ReflectUtil
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Expression, Like}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

object HoodieSpark3CatalystPlanUtils extends HoodieCatalystPlansUtils {

  override def toTableIdentifier(aliasId: AliasIdentifier): TableIdentifier = {
    aliasId match {
      case AliasIdentifier(name, Seq(database)) =>
        TableIdentifier(name, Some(database))
      case AliasIdentifier(name, Seq(_, database)) =>
        TableIdentifier(name, Some(database))
      case AliasIdentifier(name, Seq()) =>
        TableIdentifier(name, None)
      case _ => throw new IllegalArgumentException(s"Cannot cast $aliasId to TableIdentifier")
    }
  }

  override def toTableIdentifier(relation: UnresolvedRelation): TableIdentifier = {
    relation.multipartIdentifier.asTableIdentifier
  }

  override def createJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType): Join = {
    Join(left, right, joinType, None, JoinHint.NONE)
  }

  override def isInsertInto(plan: LogicalPlan): Boolean = {
    plan.isInstanceOf[InsertIntoStatement]
  }

  override def getInsertIntoChildren(plan: LogicalPlan):
  Option[(LogicalPlan, Map[String, Option[String]], LogicalPlan, Boolean, Boolean)] = {
    plan match {
      case insert: InsertIntoStatement =>
        Some((insert.table, insert.partitionSpec, insert.query, insert.overwrite, insert.ifPartitionNotExists))
      case _ =>
        None
    }
  }

  override def createInsertInto(table: LogicalPlan, partition: Map[String, Option[String]],
                                query: LogicalPlan, overwrite: Boolean, ifPartitionNotExists: Boolean): LogicalPlan = {
    ReflectUtil.createInsertInto(table, partition, Seq.empty[String], query, overwrite, ifPartitionNotExists)
  }

  override def createLike(left: Expression, right: Expression): Expression = {
    new Like(left, right)
  }

  /**
   * if the logical plan is a TimeTravelRelation LogicalPlan.
   */
  override def isRelationTimeTravel(plan: LogicalPlan): Boolean = {
    false
  }

  /**
   * Get the member of the TimeTravelRelation LogicalPlan.
   */
  override def getRelationTimeTravel(plan: LogicalPlan): Option[(LogicalPlan, Option[Expression], Option[String])] = {
    throw new IllegalStateException(s"Should not call getRelationTimeTravel for spark3.1.x")
  }

}
