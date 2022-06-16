package org.apache.spark.sql

import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}

import java.util.Comparator

/**
 * Suite of utilities helping in handling [[JavaRDD]]
 */
object HoodieJavaRDDUtils {

  /**
   * [[HoodieRDDUtils.sortWithinPartitions]] counterpart transforming [[JavaRDD]]s
   */
  def sortWithinPartitions[K, V](rdd: JavaPairRDD[K, V], c: Comparator[K]): JavaPairRDD[K, V] = {
    JavaPairRDD.fromRDD(HoodieRDDUtils.sortWithinPartitions(rdd.rdd, c))
  }

}
