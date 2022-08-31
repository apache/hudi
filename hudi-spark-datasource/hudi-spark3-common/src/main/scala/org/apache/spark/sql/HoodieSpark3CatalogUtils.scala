package org.apache.spark.sql

import org.apache.hudi.SparkAdapterSupport
import org.apache.spark.sql.connector.expressions.{NamedReference, Transform}

trait HoodieSpark3CatalogUtils extends HoodieCatalogUtils {

  /**
   * Decomposes [[org.apache.spark.sql.connector.expressions.BucketTransform]] extracting its
   * arguments to accommodate for API changes in Spark 3.3 returning:
   *
   * <ol>
   *   <li>Number of the buckets</li>
   *   <li>Seq of references (to be bucketed by)</li>
   *   <li>Seq of sorted references</li>
   * </ol>
   */
  def unapplyBucketTransform(t: Transform): Option[(Int, Seq[NamedReference], Seq[NamedReference])]

}

object HoodieSpark3CatalogUtils extends SparkAdapterSupport {

  object MatchBucketTransform {
    def unapply(t: Transform): Option[(Int, Seq[NamedReference], Seq[NamedReference])] =
      sparkAdapter.getCatalogUtils.asInstanceOf[HoodieSpark3CatalogUtils]
        .unapplyBucketTransform(t)
  }

}