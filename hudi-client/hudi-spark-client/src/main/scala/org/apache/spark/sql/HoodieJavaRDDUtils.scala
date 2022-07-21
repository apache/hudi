package org.apache.spark.sql

import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}

import java.util.Comparator
import scala.reflect.ClassTag

/**
 * Suite of utilities helping in handling [[JavaRDD]]
 */
object HoodieJavaRDDUtils {

  /**
   * [[HoodieRDDUtils.sortWithinPartitions]] counterpart transforming [[JavaRDD]]s
   */
  def sortWithinPartitions[K, V](rdd: JavaPairRDD[K, V], c: Comparator[K]): JavaPairRDD[K, V] = {
    implicit val classTagK: ClassTag[K] = fakeClassTag
    implicit val classTagV: ClassTag[V] = fakeClassTag
    JavaPairRDD.fromRDD(HoodieRDDUtils.sortWithinPartitions(rdd.rdd, c))
  }

}
