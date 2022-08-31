package org.apache.spark.sql

import org.apache.spark.sql.connector.expressions.{BucketTransform, NamedReference, Transform}

object HoodieSpark33CatalogUtils extends HoodieSpark3CatalogUtils {

  override def unapplyBucketTransform(t: Transform): Option[(Int, Seq[NamedReference], Seq[NamedReference])] =
    t match {
      case BucketTransform(numBuckets, refs, sortedRefs) => Some(numBuckets, refs, sortedRefs)
      case _ => None
    }

}
