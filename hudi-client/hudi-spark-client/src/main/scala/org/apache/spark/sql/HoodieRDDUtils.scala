package org.apache.spark.sql

import org.apache.hudi.SparkAdapterSupport
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.{InterruptibleIterator, TaskContext}

import java.util.Comparator

/**
 * Suite of utilities helping in handling [[RDD]]
 */
object HoodieRDDUtils extends SparkAdapterSupport {

  /**
   * Sorts records of the provided [[RDD]] by their keys using [[Comparator]] w/in individual [[RDD]]'s partitions
   * (ie no shuffling is performed)
   */
  def sortWithinPartitions[K, V](rdd: RDD[(K, V)], c: Comparator[K]): RDD[(K, V)] = {
    // NOTE: We leverage implicit [[Comparator]] to [[Ordering]] conversion here
    implicit val implicitComp: Comparator[K] = c
    val ordering = implicitly[Ordering[K]]

    rdd.mapPartitions(iter => {
      val ctx = TaskContext.get()
      val sorter = new ExternalSorter[K, V, V](ctx, None, None, Some(ordering))
      new InterruptibleIterator(ctx,
        sparkAdapter.insertInto(ctx, iter, sorter).asInstanceOf[Iterator[(K, V)]])
    }, preservesPartitioning = true)
  }

}
