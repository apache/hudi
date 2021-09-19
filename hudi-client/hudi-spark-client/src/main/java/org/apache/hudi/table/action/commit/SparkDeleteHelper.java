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

import org.apache.hudi.SparkHoodieRDDData;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieData;
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
 * A spark implementation of {@link BaseDeleteHelper}.
 *
 * @param <T>
 */
@SuppressWarnings("checkstyle:LineLength")
public class SparkDeleteHelper<T extends HoodieRecordPayload<T>> extends BaseDeleteHelper<T> {
  private SparkDeleteHelper() {
  }

  private static class DeleteHelperHolder {
    private static final SparkDeleteHelper SPARK_DELETE_HELPER = new SparkDeleteHelper();
  }

  public static SparkDeleteHelper newInstance() {
    return SparkDeleteHelper.DeleteHelperHolder.SPARK_DELETE_HELPER;
  }

  @Override
  public SparkHoodieRDDData<HoodieKey> deduplicateKeys(
      HoodieData<HoodieKey> keys,
      HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table,
      int parallelism) {
    JavaRDD<HoodieKey> keysRdd = ((SparkHoodieRDDData<HoodieKey>) keys).get();
    boolean isIndexingGlobal = table.getIndex().isGlobal();
    if (isIndexingGlobal) {
      return SparkHoodieRDDData.of(keysRdd.keyBy(HoodieKey::getRecordKey)
          .reduceByKey((key1, key2) -> key1, parallelism)
          .values());
    } else {
      return SparkHoodieRDDData.of(keysRdd.distinct(parallelism));
    }
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute(
      String instantTime, HoodieData<HoodieKey> keys, HoodieEngineContext context,
      HoodieWriteConfig config, HoodieTable table,
      BaseCommitHelper<T> commitHelper) {
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);

    try {
      HoodieWriteMetadata<HoodieData<WriteStatus>> result = null;
      SparkHoodieRDDData<HoodieKey> dedupedKeys = (SparkHoodieRDDData<HoodieKey>) keys;
      final int parallelism = config.getDeleteShuffleParallelism();
      if (config.shouldCombineBeforeDelete()) {
        // De-dupe/merge if needed
        dedupedKeys = deduplicateKeys(keys, table, parallelism);
      } else {
        JavaRDD<HoodieKey> keysRdd = ((SparkHoodieRDDData<HoodieKey>) keys).get();
        if (!keysRdd.partitions().isEmpty()) {
          dedupedKeys = SparkHoodieRDDData.of(keysRdd.repartition(parallelism));
        }
      }

      JavaRDD<HoodieRecord<T>> dedupedRecords =
          dedupedKeys.get().map(key -> new HoodieRecord(key, new EmptyHoodieRecordPayload()));
      Instant beginTag = Instant.now();
      // perform index loop up to get existing location of records
      HoodieData<HoodieRecord<T>> taggedRecords = SparkHoodieRDDData.of((JavaRDD<HoodieRecord<T>>)
          table.getIndex().tagLocation(dedupedRecords, context, table));
      Duration tagLocationDuration = Duration.between(beginTag, Instant.now());

      // filter out non existent keys/records
      HoodieData<HoodieRecord<T>> taggedValidRecords = SparkHoodieRDDData.of(
          ((SparkHoodieRDDData<HoodieRecord<T>>) taggedRecords).get().filter(HoodieRecord::isCurrentLocationKnown));
      if (!taggedValidRecords.isEmpty()) {
        result = commitHelper.execute(taggedValidRecords);
        result.setIndexLookupDuration(tagLocationDuration);
      } else {
        // if entire set of keys are non existent
        commitHelper.saveWorkloadProfileMetadataToInflight(new WorkloadProfile(
            Pair.of(new HashMap<>(), new WorkloadStat())), instantTime);
        result = new HoodieWriteMetadata<>();
        result.setWriteStatuses(SparkHoodieRDDData.of(jsc.emptyRDD()));
        commitHelper.commitOnAutoCommit(result);
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
