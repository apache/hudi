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
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;

/**
 * A spark implementation of {@link AbstractDeleteHelper}.
 *
 * @param <T>
 */
@SuppressWarnings("checkstyle:LineLength")
public class SparkDeleteHelper<T extends HoodieRecordPayload,R> extends
    AbstractDeleteHelper<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, R> {
  private SparkDeleteHelper() {
  }

  private static class DeleteHelperHolder {
    private static final SparkDeleteHelper SPARK_DELETE_HELPER = new SparkDeleteHelper();
  }

  public static SparkDeleteHelper newInstance() {
    return DeleteHelperHolder.SPARK_DELETE_HELPER;
  }

  @Override
  public JavaRDD<HoodieKey> deduplicateKeys(JavaRDD<HoodieKey> keys, HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table, int parallelism) {
    boolean isIndexingGlobal = table.getIndex().isGlobal();
    if (isIndexingGlobal) {
      return keys.keyBy(HoodieKey::getRecordKey)
          .reduceByKey((key1, key2) -> key1, parallelism)
          .values();
    } else {
      return keys.distinct(parallelism);
    }
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> execute(String instantTime,
                                                           JavaRDD<HoodieKey> keys,
                                                           HoodieEngineContext context,
                                                           HoodieWriteConfig config,
                                                           HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table,
                                                           BaseCommitActionExecutor<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, R> deleteExecutor) {
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);

    try {
      HoodieWriteMetadata result = null;
      JavaRDD<HoodieKey> dedupedKeys = keys;
      final int parallelism = config.getDeleteShuffleParallelism();
      if (config.shouldCombineBeforeDelete()) {
        // De-dupe/merge if needed
        dedupedKeys = deduplicateKeys(keys, table, parallelism);
      } else if (!keys.partitions().isEmpty()) {
        dedupedKeys = keys.repartition(parallelism);
      }

      JavaRDD<HoodieRecord<T>> dedupedRecords =
          dedupedKeys.map(key -> new HoodieRecord(key, new EmptyHoodieRecordPayload()));
      Instant beginTag = Instant.now();
      // perform index loop up to get existing location of records
      JavaRDD<HoodieRecord<T>> taggedRecords = HoodieJavaRDD.getJavaRDD(
          table.getIndex().tagLocation(HoodieJavaRDD.of(dedupedRecords), context, table));
      Duration tagLocationDuration = Duration.between(beginTag, Instant.now());

      // filter out non existent keys/records
      JavaRDD<HoodieRecord<T>> taggedValidRecords = taggedRecords.filter(HoodieRecord::isCurrentLocationKnown);
      if (!taggedValidRecords.isEmpty()) {
        result = deleteExecutor.execute(taggedValidRecords);
        result.setIndexLookupDuration(tagLocationDuration);
      } else {
        // if entire set of keys are non existent
        deleteExecutor.saveWorkloadProfileMetadataToInflight(new WorkloadProfile(Pair.of(new HashMap<>(), new WorkloadStat())), instantTime);
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
