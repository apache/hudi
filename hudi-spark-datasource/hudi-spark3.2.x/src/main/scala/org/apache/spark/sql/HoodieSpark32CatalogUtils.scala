package org.apache.spark.sql
import org.apache.spark.sql.connector.expressions.{BucketTransform, NamedReference, Transform}

object HoodieSpark32CatalogUtils extends HoodieSpark3CatalogUtils {

  override def unapplyBucketTransform(t: Transform): Option[(Int, Seq[NamedReference], Seq[NamedReference])] =
    t match {
      case BucketTransform(numBuckets, ref) => Some(numBuckets, Seq(ref), Seq.empty)
      case _ => None
    }

}
