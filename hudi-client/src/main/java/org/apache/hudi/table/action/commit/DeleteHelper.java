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

import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;

import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.Duration;
import java.time.Instant;

/**
 * Helper class to perform delete keys on hoodie table.
 * @param <T>
 */
public class DeleteHelper<T extends HoodieRecordPayload<T>> {

  /**
   * Deduplicate Hoodie records, using the given deduplication function.
   *
   * @param keys RDD of HoodieKey to deduplicate
   * @return RDD of HoodieKey already be deduplicated
   */
  private static  <T extends HoodieRecordPayload<T>> JavaRDD<HoodieKey> deduplicateKeys(JavaRDD<HoodieKey> keys,
      HoodieTable<T> table) {
    boolean isIndexingGlobal = table.getIndex().isGlobal();
    if (isIndexingGlobal) {
      return keys.keyBy(HoodieKey::getRecordKey)
          .reduceByKey((key1, key2) -> key1)
          .values();
    } else {
      return keys.distinct();
    }
  }

  public static <T extends HoodieRecordPayload<T>> HoodieWriteMetadata execute(String instantTime,
                                                                               JavaRDD<HoodieKey> keys, JavaSparkContext jsc, HoodieWriteConfig config, HoodieTable<T> table,
                                                                               CommitActionExecutor<T> deleteExecutor) {
    try {
      HoodieWriteMetadata result = null;
      // De-dupe/merge if needed
      JavaRDD<HoodieKey> dedupedKeys = config.shouldCombineBeforeDelete() ? deduplicateKeys(keys, table) : keys;

      JavaRDD<HoodieRecord<T>> dedupedRecords =
          dedupedKeys.map(key -> new HoodieRecord(key, new EmptyHoodieRecordPayload()));
      Instant beginTag = Instant.now();
      // perform index loop up to get existing location of records
      JavaRDD<HoodieRecord<T>> taggedRecords =
          ((HoodieTable<T>)table).getIndex().tagLocation(dedupedRecords, jsc, (HoodieTable<T>)table);
      Duration tagLocationDuration = Duration.between(beginTag, Instant.now());

      // filter out non existant keys/records
      JavaRDD<HoodieRecord<T>> taggedValidRecords = taggedRecords.filter(HoodieRecord::isCurrentLocationKnown);
      if (!taggedValidRecords.isEmpty()) {
        result = deleteExecutor.execute(taggedValidRecords);
        result.setIndexLookupDuration(tagLocationDuration);
      } else {
        // if entire set of keys are non existent
        deleteExecutor.saveWorkloadProfileMetadataToInflight(new WorkloadProfile(jsc.emptyRDD()), instantTime);
        result = new HoodieWriteMetadata();
        result.setWriteStatuses(jsc.emptyRDD());
        deleteExecutor.commitOnAutoCommit(result);
      }
      return result;
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to delete for commit time " + instantTime, e);
    }
  }
}
