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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.HoodieJavaRDDUtils;
import scala.Tuple2;

import java.util.Comparator;

import static org.apache.hudi.execution.bulkinsert.BulkInsertSortMode.PARTITION_SORT;

/**
 * A built-in partitioner that does local sorting for each RDD partition
 * after coalescing it to specified number of partitions.
 * Corresponds to the {@link BulkInsertSortMode#PARTITION_SORT} mode.
 *
 * @param <T> HoodieRecordPayload type
 */
public class RDDPartitionSortPartitioner<T> extends SparkBulkInsertPartitionerBase<JavaRDD<HoodieRecord<T>>> {

  private final boolean shouldPopulateMetaFields;

  public RDDPartitionSortPartitioner(HoodieWriteConfig config) {
    this.shouldPopulateMetaFields = config.populateMetaFields();
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records,
                                                     int targetPartitionNumHint) {
    if (!shouldPopulateMetaFields) {
      throw new HoodieException(PARTITION_SORT.name() + " mode requires meta-fields to be enabled");
    }

    JavaPairRDD<Pair<String, String>, HoodieRecord<T>> kvPairsRDD = tryCoalesce(records, targetPartitionNumHint)
        .mapToPair(record -> new Tuple2<>(Pair.of(record.getPartitionPath(), record.getRecordKey()), record));

    // NOTE: [[JavaRDD]] doesn't expose an API to do the sorting w/o (re-)shuffling, as such
    //       we're relying on our own sequence to achieve that
    return HoodieJavaRDDUtils.sortWithinPartitions(kvPairsRDD, Comparator.naturalOrder()).values();
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
