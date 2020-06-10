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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.table.BaseHoodieTable;
import org.apache.hudi.table.SparkWorkloadProfile;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.Duration;
import java.time.Instant;

/**
 * A spark implementation of {@link BaseDeleteHelper}.
 *
 * @param <T>
 */
public class SparkDeleteHelper<T extends HoodieRecordPayload> extends BaseDeleteHelper<T, JavaRDD<HoodieRecord<T>>,
    JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> {
  private SparkDeleteHelper() {
  }

  private static class DeleteHelperHolder {
    private static final SparkDeleteHelper SPARK_DELETE_HELPER = new SparkDeleteHelper();
  }

  public static SparkDeleteHelper newInstance() {
    return DeleteHelperHolder.SPARK_DELETE_HELPER;
  }

  @Override
  public JavaRDD<HoodieKey> deduplicateKeys(JavaRDD<HoodieKey> keys,
                                            BaseHoodieTable<T, JavaRDD<HoodieRecord<T>>,
                                                JavaRDD<HoodieKey>,
                                                JavaRDD<WriteStatus>,
                                                JavaPairRDD<HoodieKey,
                                                    Option<Pair<String, String>>>> table) {
    boolean isIndexingGlobal = table.getIndex().isGlobal();
    if (isIndexingGlobal) {
      return keys.keyBy(HoodieKey::getRecordKey)
          .reduceByKey((key1, key2) -> key1)
          .values();
    } else {
      return keys.distinct();
    }
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> execute(String instantTime,
                                                           JavaRDD<HoodieKey> keys,
                                                           HoodieEngineContext context,
                                                           HoodieWriteConfig config,
                                                           BaseHoodieTable<T, JavaRDD<HoodieRecord<T>>,
                                                               JavaRDD<HoodieKey>,
                                                               JavaRDD<WriteStatus>,
                                                               JavaPairRDD<HoodieKey,
                                                                   Option<Pair<String, String>>>> table,
                                                           BaseCommitActionExecutor<T, JavaRDD<HoodieRecord<T>>,
                                                               JavaRDD<HoodieKey>,
                                                               JavaRDD<WriteStatus>,
                                                               JavaPairRDD<HoodieKey,
                                                                   Option<Pair<String, String>>>> deleteExecutor) {
    try {
      HoodieWriteMetadata result = null;
      // De-dupe/merge if needed
      JavaRDD<HoodieKey> dedupedKeys = config.shouldCombineBeforeDelete() ? deduplicateKeys(keys, table) : keys;

      JavaRDD<HoodieRecord<T>> dedupedRecords =
          dedupedKeys.map(key -> new HoodieRecord(key, new EmptyHoodieRecordPayload()));
      Instant beginTag = Instant.now();
      // perform index loop up to get existing location of records
      JavaRDD<HoodieRecord<T>> taggedRecords =
          table.getIndex().tagLocation(dedupedRecords, context, table);
      Duration tagLocationDuration = Duration.between(beginTag, Instant.now());

      // filter out non existant keys/records
      JavaRDD<HoodieRecord<T>> taggedValidRecords = taggedRecords.filter(HoodieRecord::isCurrentLocationKnown);
      if (!taggedValidRecords.isEmpty()) {
        result = deleteExecutor.execute(taggedValidRecords);
        result.setIndexLookupDuration(tagLocationDuration);
      } else {
        JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
        // if entire set of keys are non existent
        deleteExecutor.saveWorkloadProfileMetadataToInflight(new SparkWorkloadProfile(jsc.emptyRDD()), instantTime);
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
