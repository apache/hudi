package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, TimeTravelRelation}

object HoodieSpark32CatalystPlanUtils extends HoodieSpark3CatalystPlanUtils {

  override def isRelationTimeTravel(plan: LogicalPlan): Boolean = {
    plan.isInstanceOf[TimeTravelRelation]
  }

  override def getRelationTimeTravel(plan: LogicalPlan): Option[(LogicalPlan, Option[Expression], Option[String])] = {
    plan match {
      case timeTravel: TimeTravelRelation =>
        Some((timeTravel.table, timeTravel.timestamp, timeTravel.version))
      case _ =>
        None
    }
  }
}
