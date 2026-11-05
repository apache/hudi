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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.StringUtils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import scala.Tuple2;

/**
 * Shared sorting operations for LSM RDD bulk-insert partitioners.
 *
 * <p>LSM base files must be ordered by partition path and record key using their UTF-8 byte
 * representation. This helper exposes two variants of that ordering:
 *
 * <ul>
 *   <li>{@link #keyByPartitionAndRecordKey(JavaRDD)} attaches the LSM sort key to records that
 *       will subsequently be repartitioned and sorted by a caller.</li>
 *   <li>{@link #sortWithinPartitions(JavaRDD)} sorts records without changing which Spark
 *       partition they belong to. This is used when a bulk-insert partitioner has already
 *       established the desired record distribution.</li>
 * </ul>
 */
final class LSMBulkInsertRecordSorter {

  /** Comparator for the externally visible (partition path, record key) LSM sort key. */
  static final Comparator<Tuple2<String, String>> KEY_COMPARATOR =
      (Comparator<Tuple2<String, String>> & Serializable) (left, right) ->
          comparePartitionAndRecordKeys(left._1, left._2, right._1, right._2);

  private LSMBulkInsertRecordSorter() {
  }

  /**
   * Keys records by the LSM physical ordering columns.
   *
   * <p>The returned pair RDD is intended for callers that need to choose their own Spark
   * partitioner and invoke {@code repartitionAndSortWithinPartitions}.
   */
  static <T> JavaPairRDD<Tuple2<String, String>, HoodieRecord<T>> keyByPartitionAndRecordKey(
      JavaRDD<HoodieRecord<T>> records) {
    return records.mapToPair(record -> new Tuple2<>(
        new Tuple2<>(record.getPartitionPath(), record.getRecordKey()), record));
  }

  /**
   * Sorts each existing Spark partition by the LSM physical ordering without changing record
   * distribution between partitions.
   *
   * <p>This follows the existing {@link RDDPartitionSortPartitioner} execution model: each Spark
   * partition is materialized into a list and sorted locally without introducing another shuffle.
   * Memory usage is therefore proportional to the largest input partition.
   */
  static <T> JavaRDD<HoodieRecord<T>> sortWithinPartitions(
      JavaRDD<HoodieRecord<T>> records) {
    return records.mapPartitions(iterator -> {
      List<HoodieRecord<T>> recordList = new ArrayList<>();
      iterator.forEachRemaining(recordList::add);
      recordList.sort((left, right) -> comparePartitionAndRecordKeys(
          left.getPartitionPath(), left.getRecordKey(),
          right.getPartitionPath(), right.getRecordKey()));
      return recordList.iterator();
    });
  }

  private static int comparePartitionAndRecordKeys(
      String leftPartitionPath, String leftRecordKey,
      String rightPartitionPath, String rightRecordKey) {
    int partitionComparison = StringUtils.compareUtf8Bytes(leftPartitionPath, rightPartitionPath);
    return partitionComparison != 0
        ? partitionComparison
        : StringUtils.compareUtf8Bytes(leftRecordKey, rightRecordKey);
  }
}
