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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.{InterruptibleIterator, TaskContext}

import java.util.Comparator

trait HoodieRDDUtils {

  /**
   * Insert all records, updates related task metrics, and return a completion iterator
   * over all the data written to this [[ExternalSorter]], aggregated by our aggregator.
   *
   * On task completion (success, failure, or cancellation), it releases resources by
   * calling `stop()`.
   *
   * NOTE: This method is an [[ExternalSorter#insertAllAndUpdateMetrics]] back-ported to Spark 2.4
   */
  def insertInto[K, V, C](ctx: TaskContext, records: Iterator[Product2[K, V]], sorter: ExternalSorter[K, V, C]): Iterator[Product2[K, C]]

  /**
   * Create instance of [[HoodieFileScanRDD]]
   * SPARK-37273 FileScanRDD constructor changed in SPARK 3.3
   */
  def createHoodieFileScanRDD(sparkSession: SparkSession,
                              readFunction: PartitionedFile => Iterator[InternalRow],
                              filePartitions: Seq[FilePartition],
                              readDataSchema: StructType,
                              metadataColumns: Seq[AttributeReference] = Seq.empty): FileScanRDD

}

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
        sparkAdapter.getRDDUtils.insertInto(ctx, iter, sorter).asInstanceOf[Iterator[(K, V)]])
    }, preservesPartitioning = true)
  }

}
