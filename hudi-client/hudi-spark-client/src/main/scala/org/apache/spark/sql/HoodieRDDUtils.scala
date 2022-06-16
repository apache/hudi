package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.{InterruptibleIterator, TaskContext}

import java.util.Comparator

/**
 * Suite of utilities helping in handling [[RDD]]
 */
object HoodieRDDUtils {

  /**
   * Sorts records of the provided [[RDD]] by their keys using [[Comparator]] w/in individual [[RDD]]'s partitions
   * (ie no shuffling is performed)
   */
  def sortWithinPartitions[K, V](rdd: RDD[(K, V)], c: Comparator[K]): RDD[(K, V)] = {
    // NOTE: We leverage implicit [[Comparator]] to [[Ordering]] conversion here
    implicit val implicitComp: Comparator[K] = c
    val ordering = implicitly[Ordering[K]]

    rdd.mapPartitions(iter => {
      val context = TaskContext.get()
      val sorter = new ExternalSorter[K, V, V](context, None, None, Some(ordering))
      new InterruptibleIterator(context,
        sorter.insertAllAndUpdateMetrics(iter).asInstanceOf[Iterator[(K, V)]])
    }, preservesPartitioning = true)
  }

}
