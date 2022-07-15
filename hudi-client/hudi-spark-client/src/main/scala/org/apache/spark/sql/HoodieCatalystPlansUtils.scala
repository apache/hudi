package org.apache.spark.sql

import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}

trait HoodieCatalystPlansUtils {

  /**
   * Convert a AliasIdentifier to TableIdentifier.
   */
  def toTableIdentifier(aliasId: AliasIdentifier): TableIdentifier

  /**
   * Convert a UnresolvedRelation to TableIdentifier.
   */
  def toTableIdentifier(relation: UnresolvedRelation): TableIdentifier

  /**
   * Create Join logical plan.
   */
  def createJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType): Join

  /**
   * Test if the logical plan is a Insert Into LogicalPlan.
   */
  def isInsertInto(plan: LogicalPlan): Boolean

  /**
   * Get the member of the Insert Into LogicalPlan.
   */
  def getInsertIntoChildren(plan: LogicalPlan):
    Option[(LogicalPlan, Map[String, Option[String]], LogicalPlan, Boolean, Boolean)]

  /**
   * if the logical plan is a TimeTravelRelation LogicalPlan.
   */
  def isRelationTimeTravel(plan: LogicalPlan): Boolean

  /**
   * Get the member of the TimeTravelRelation LogicalPlan.
   */
  def getRelationTimeTravel(plan: LogicalPlan): Option[(LogicalPlan, Option[Expression], Option[String])]

  /**
   * Create a Insert Into LogicalPlan.
   */
  def createInsertInto(table: LogicalPlan, partition: Map[String, Option[String]],
                       query: LogicalPlan, overwrite: Boolean, ifPartitionNotExists: Boolean): LogicalPlan

  /**
   * Create Like expression.
   */
  def createLike(left: Expression, right: Expression): Expression

}
