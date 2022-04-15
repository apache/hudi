/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
