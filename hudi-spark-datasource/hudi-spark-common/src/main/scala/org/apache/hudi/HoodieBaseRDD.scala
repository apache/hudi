package org.apache.hudi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
 * Base class for all of the custom Hudi's RDD implementations
 *
 * NOTE: It enforces, for ex, that all of the RDDs implement [[compute]] method returning
 *       [[InternalRow]] to avoid superfluous ser/de
 */
abstract class HoodieBaseRDD(@transient sc: SparkContext)
  extends RDD[InternalRow](sc, Nil) {

  def compute(split: Partition, context: TaskContext): Iterator[InternalRow]

}
