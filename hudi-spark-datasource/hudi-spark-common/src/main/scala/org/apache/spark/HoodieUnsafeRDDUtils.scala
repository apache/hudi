package org.apache.spark

import org.apache.hudi.HoodieUnsafeRDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.MutablePair

/**
 * Suite of utilities helping in handling instances of [[HoodieUnsafeRDD]]
 */
object HoodieUnsafeRDDUtils {

  /**
   * Canonical implementation of the [[RDD#collect]] for [[HoodieUnsafeRDD]], returning a properly
   * copied [[Array]] of [[InternalRow]]s
   */
  def collect(rdd: HoodieUnsafeRDD): Array[InternalRow] = {
    rdd.mapPartitionsInternal { iter =>
      // NOTE: We're leveraging [[MutablePair]] here to avoid unnecessary allocations, since
      //       a) iteration is performed lazily and b) iteration is single-threaded (w/in partition)
      val pair = new MutablePair[InternalRow, Null]()
      iter.map(row => pair.update(row.copy(), null))
    }
      .map(p => p._1)
      .collect()
  }

}
