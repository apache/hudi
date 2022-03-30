package org.apache.spark.sql

import org.apache.spark.sql.types.{DataType, DecimalType, NumericType, StringType}

// TODO unify w/ DataTypeUtils
object HoodieSparkTypeUtils {

  /**
   * Returns whether this DecimalType is wider than `other`. If yes, it means `other`
   * can be casted into `this` safely without losing any precision or range.
   */
  def isWiderThan(one: DecimalType, another: DecimalType) =
    one.isWiderThan(another)

  /**
   * Checks whether casting expression of [[from]] [[DataType]] to [[to]] [[DataType]] will
   * preserve ordering of the elements
   */
  def isCastPreservingOrdering(from: DataType, to: DataType): Boolean =
    (from, to) match {
      // NOTE: In the casting rules defined by Spark, only casting from String to Numeric
      // (and vice versa) are the only casts that might break the ordering of the elements after casting
      case (StringType, _: NumericType) => false
      case (_: NumericType, StringType) => false

      case _ => true
    }
}
