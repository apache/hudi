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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.Duration;
import java.time.Instant;
import scala.Tuple2;

public class WriteHelper<T extends HoodieRecordPayload<T>> {

  public static <T extends HoodieRecordPayload<T>> HoodieWriteMetadata write(String instantTime,
                                                                             JavaRDD<HoodieRecord<T>> inputRecordsRDD, JavaSparkContext jsc,
                                                                             HoodieTable<T> table, boolean shouldCombine,
                                                                             int shuffleParallelism, CommitActionExecutor<T> executor, boolean performTagging) {
    try {
      // De-dupe/merge if needed
      JavaRDD<HoodieRecord<T>> dedupedRecords =
          combineOnCondition(shouldCombine, inputRecordsRDD, shuffleParallelism, table);

      Instant lookupBegin = Instant.now();
      JavaRDD<HoodieRecord<T>> taggedRecords = dedupedRecords;
      if (performTagging) {
        // perform index loop up to get existing location of records
        taggedRecords = tag(dedupedRecords, jsc, table);
      }
      Duration indexLookupDuration = Duration.between(lookupBegin, Instant.now());

      HoodieWriteMetadata result = executor.execute(taggedRecords);
      result.setIndexLookupDuration(indexLookupDuration);
      return result;
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to upsert for commit time " + instantTime, e);
    }
  }

  private static <T extends HoodieRecordPayload<T>> JavaRDD<HoodieRecord<T>> tag(
      JavaRDD<HoodieRecord<T>> dedupedRecords, JavaSparkContext jsc, HoodieTable<T> table) {
    // perform index loop up to get existing location of records
    return table.getIndex().tagLocation(dedupedRecords, jsc, table);
  }

  public static <T extends HoodieRecordPayload<T>> JavaRDD<HoodieRecord<T>> combineOnCondition(
      boolean condition, JavaRDD<HoodieRecord<T>> records, int parallelism, HoodieTable<T> table) {
    return condition ? deduplicateRecords(records, table, parallelism) : records;
  }

  /**
   * Deduplicate Hoodie records, using the given deduplication function.
   *
   * @param records hoodieRecords to deduplicate
   * @param parallelism parallelism or partitions to be used while reducing/deduplicating
   * @return RDD of HoodieRecord already be deduplicated
   */
  public static <T extends HoodieRecordPayload<T>> JavaRDD<HoodieRecord<T>> deduplicateRecords(
      JavaRDD<HoodieRecord<T>> records, HoodieTable<T> table, int parallelism) {
    return deduplicateRecords(records, table.getIndex(), parallelism);
  }

  public static <T extends HoodieRecordPayload<T>> JavaRDD<HoodieRecord<T>> deduplicateRecords(
      JavaRDD<HoodieRecord<T>> records, HoodieIndex<T> index, int parallelism) {
    boolean isIndexingGlobal = index.isGlobal();
    return records.mapToPair(record -> {
      HoodieKey hoodieKey = record.getKey();
      // If index used is global, then records are expected to differ in their partitionPath
      Object key = isIndexingGlobal ? hoodieKey.getRecordKey() : hoodieKey;
      return new Tuple2<>(key, record);
    }).reduceByKey((rec1, rec2) -> {
      @SuppressWarnings("unchecked")
      T reducedData = (T) rec1.getData().preCombine(rec2.getData());
      // we cannot allow the user to change the key or partitionPath, since that will affect
      // everything
      // so pick it from one of the records.
      return new HoodieRecord<T>(rec1.getKey(), reducedData);
    }, parallelism).map(Tuple2::_2);
  }
}
