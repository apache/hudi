package org.apache.spark.sql

import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Expression, Like}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, Join, LogicalPlan}

object HoodieSpark2CatalystPlanUtils extends HoodieCatalystPlansUtils {

  override def toTableIdentifier(aliasId: AliasIdentifier): TableIdentifier = {
    TableIdentifier(aliasId.identifier, aliasId.database)
  }

  override def toTableIdentifier(relation: UnresolvedRelation): TableIdentifier = {
    relation.tableIdentifier
  }

  override def createJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType): Join = {
    Join(left, right, joinType, None)
  }

  override def isInsertInto(plan: LogicalPlan): Boolean = {
    plan.isInstanceOf[InsertIntoTable]
  }

  override def getInsertIntoChildren(plan: LogicalPlan):
  Option[(LogicalPlan, Map[String, Option[String]], LogicalPlan, Boolean, Boolean)] = {
    plan match {
      case InsertIntoTable(table, partition, query, overwrite, ifPartitionNotExists) =>
        Some((table, partition, query, overwrite, ifPartitionNotExists))
      case _=> None
    }
  }

  override def createInsertInto(table: LogicalPlan, partition: Map[String, Option[String]],
                                query: LogicalPlan, overwrite: Boolean, ifPartitionNotExists: Boolean): LogicalPlan = {
    InsertIntoTable(table, partition, query, overwrite, ifPartitionNotExists)
  }

  override def createLike(left: Expression, right: Expression): Expression = {
    Like(left, right)
  }

  override def isRelationTimeTravel(plan: LogicalPlan): Boolean = {
    false
  }

  override def getRelationTimeTravel(plan: LogicalPlan): Option[(LogicalPlan, Option[Expression], Option[String])] = {
    throw new IllegalStateException(s"Should not call getRelationTimeTravel for spark2")
  }
}
