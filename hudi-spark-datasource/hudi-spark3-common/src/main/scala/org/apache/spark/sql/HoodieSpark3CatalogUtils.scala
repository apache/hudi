/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
