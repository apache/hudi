package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, Predicate, PredicateHelper}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy

private trait HoodieSpark3CatalystExpressionUtils extends HoodieCatalystExpressionUtils
  with PredicateHelper {

  override def normalizeExprs(exprs: Seq[Expression], attributes: Seq[Attribute]): Seq[Expression] =
    DataSourceStrategy.normalizeExprs(exprs, attributes)

  override def extractPredicatesWithinOutputSet(condition: Expression,
                                                outputSet: AttributeSet): Option[Expression] =
    super[PredicateHelper].extractPredicatesWithinOutputSet(condition, outputSet)
}
