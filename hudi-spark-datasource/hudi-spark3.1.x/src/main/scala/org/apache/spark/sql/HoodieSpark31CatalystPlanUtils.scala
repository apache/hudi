package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object HoodieSpark31CatalystPlanUtils extends HoodieSpark3CatalystPlanUtils {

  override def isRelationTimeTravel(plan: LogicalPlan): Boolean = false

  override def getRelationTimeTravel(plan: LogicalPlan): Option[(LogicalPlan, Option[Expression], Option[String])] = {
    throw new IllegalStateException(s"Should not call getRelationTimeTravel for Spark <= 3.2.x")
  }
}
