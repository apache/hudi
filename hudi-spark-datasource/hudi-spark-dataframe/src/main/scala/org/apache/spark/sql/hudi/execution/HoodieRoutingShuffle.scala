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

package org.apache.spark.sql.hudi.execution

import org.apache.spark.{MapOutputTrackerMaster, Partition, Partitioner, ShuffleDependency, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.ThreadUtils

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

/**
 * Routes update buckets and insert buckets into disjoint reducer ranges: update buckets must
 * never be split across write tasks (one merge handle owns a whole file group), while insert
 * reducers are safe to bin-pack because insert runs may open as many files as needed.
 */
class BucketRangePartitioner(updateReducers: Int,
                             insertReducers: Int,
                             insertBucketPrefix: String) extends Partitioner {
  override def numPartitions: Int = updateReducers + insertReducers

  override def getPartition(key: Any): Int = {
    val bucket = key.asInstanceOf[String]
    if (bucket.startsWith(insertBucketPrefix)) {
      updateReducers + nonNegativeMod(bucket.hashCode, insertReducers)
    } else {
      nonNegativeMod(bucket.hashCode, updateReducers)
    }
  }

  private def nonNegativeMod(hash: Int, mod: Int): Int = ((hash % mod) + mod) % mod
}

/** One write task's slice of the shuffle: a contiguous map-output range of one reducer. */
case class ShuffleBin(reducerId: Int, startMapIndex: Int, endMapIndex: Int)

private class RoutingShuffleBinPartition(override val index: Int, val bin: ShuffleBin) extends Partition

/**
 * Reduce side of the profiled routing shuffle: one RDD partition per planned bin, each reading
 * its bin's map-output range through the shuffle manager's map-range reader (the same public
 * seam AQE uses for skew splits), sorted by the routing-bucket key via the dependency's key
 * ordering so a write task sees each bucket as one contiguous run. Declaring the dependency
 * keeps lineage intact: lost shuffle files re-run only the affected map tasks.
 */
class HoodieRoutingShuffledRdd(@transient sc: SparkContext,
                               dependency: ShuffleDependency[String, InternalRow, InternalRow],
                               bins: Array[ShuffleBin])
  extends RDD[(String, InternalRow)](sc, Seq(dependency)) {

  override protected def getPartitions: Array[Partition] =
    bins.zipWithIndex.map { case (bin, i) => new RoutingShuffleBinPartition(i, bin) }

  override def compute(split: Partition, context: TaskContext): Iterator[(String, InternalRow)] = {
    val bin = split.asInstanceOf[RoutingShuffleBinPartition].bin
    val reader = SparkEnv.get.shuffleManager.getReader[String, InternalRow](
      dependency.shuffleHandle, bin.startMapIndex, bin.endMapIndex, bin.reducerId, bin.reducerId + 1,
      context, context.taskMetrics().createTempShuffleReadMetrics())
    reader.read().map(pair => (pair._1, pair._2))
  }
}

case class RoutingShuffleResult(rows: RDD[(String, InternalRow)], totalShuffleBytes: Long, binCount: Int)

/**
 * The shuffle-stats profiler mechanism: materialize only the map stage of the routing shuffle,
 * read per-block byte sizes from the map output tracker (free metadata every shuffle produces,
 * accurate to the tracker's size quantization), and plan write tasks from measured sizes on the
 * driver. Update reducers pass through whole; insert reducers are bin-packed into contiguous
 * map ranges targeting the given byte size.
 */
object HoodieRoutingShuffle extends Logging {

  def execute(pairs: RDD[(String, InternalRow)],
              updateReducers: Int,
              insertReducers: Int,
              insertBucketPrefix: String,
              binSizeBytes: Long): RoutingShuffleResult = {
    val sc = pairs.sparkContext
    val partitioner = new BucketRangePartitioner(updateReducers, insertReducers, insertBucketPrefix)
    val dependency = new ShuffleDependency[String, InternalRow, InternalRow](
      pairs, partitioner, SparkEnv.get.serializer, Option(implicitly[Ordering[String]]))

    val stats = ThreadUtils.awaitResult(sc.submitMapStage(dependency), Duration.Inf)
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val blockSizes: Array[Array[Long]] = tracker.shuffleStatuses(dependency.shuffleId)
      .withMapStatuses { statuses =>
        statuses.map(status => Array.tabulate(partitioner.numPartitions)(r => status.getSizeForBlock(r)))
      }
    val numMaps = blockSizes.length

    val bins = new ArrayBuffer[ShuffleBin]()
    (0 until updateReducers).foreach { reducer =>
      if (blockSizes.exists(mapSizes => mapSizes(reducer) > 0)) {
        bins += ShuffleBin(reducer, 0, numMaps)
      }
    }
    (updateReducers until partitioner.numPartitions).foreach { reducer =>
      var start = 0
      var accumulated = 0L
      (0 until numMaps).foreach { mapIndex =>
        accumulated += blockSizes(mapIndex)(reducer)
        if (accumulated >= binSizeBytes && mapIndex + 1 < numMaps) {
          bins += ShuffleBin(reducer, start, mapIndex + 1)
          start = mapIndex + 1
          accumulated = 0L
        }
      }
      if (accumulated > 0) {
        bins += ShuffleBin(reducer, start, numMaps)
      }
    }

    val totalBytes = stats.bytesByPartitionId.sum
    log.info(s"Routing shuffle planned ${bins.size} write tasks from $numMaps map outputs, "
      + s"$totalBytes shuffle bytes, bin target $binSizeBytes bytes")
    RoutingShuffleResult(new HoodieRoutingShuffledRdd(sc, dependency, bins.toArray), totalBytes, bins.size)
  }
}
