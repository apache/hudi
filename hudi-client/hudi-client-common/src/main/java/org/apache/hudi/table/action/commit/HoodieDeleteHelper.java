/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  
 */

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;

/**
 * A spark implementation of {@link BaseDeleteHelper}.
 *
 * @param <T>
 */
@SuppressWarnings("checkstyle:LineLength")
public class HoodieDeleteHelper<T extends HoodieRecordPayload, R> extends
    BaseDeleteHelper<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>, R> {
  private HoodieDeleteHelper() {
  }

  private static class DeleteHelperHolder {
    private static final HoodieDeleteHelper HOODIE_DELETE_HELPER = new HoodieDeleteHelper<>();
  }

  public static HoodieDeleteHelper newInstance() {
    return DeleteHelperHolder.HOODIE_DELETE_HELPER;
  }

  @Override
  public HoodieData<HoodieKey> deduplicateKeys(HoodieData<HoodieKey> keys, HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table, int parallelism) {
    boolean isIndexingGlobal = table.getIndex().isGlobal();
    if (isIndexingGlobal) {
      return keys.distinctWithKey(HoodieKey::getRecordKey, parallelism);
    } else {
      return keys.distinct(parallelism);
    }
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute(String instantTime,
                                                              HoodieData<HoodieKey> keys,
                                                              HoodieEngineContext context,
                                                              HoodieWriteConfig config,
                                                              HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table,
                                                              BaseCommitActionExecutor<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>, R> deleteExecutor) {
    try {
      HoodieData<HoodieKey> dedupedKeys = keys;
      final int parallelism = Math.max(config.getInt(HoodieWriteConfig.DELETE_PARALLELISM_VALUE), 1);
      if (config.getBoolean(HoodieWriteConfig.COMBINE_BEFORE_DELETE)) {
        // De-dupe/merge if needed
        dedupedKeys = deduplicateKeys(keys, table, parallelism);
      } else if (!keys.isEmpty()) {
        dedupedKeys = keys.repartition(parallelism);
      }

      HoodieData<HoodieRecord<T>> dedupedRecords =
          dedupedKeys.map(key -> new HoodieAvroRecord(key, new EmptyHoodieRecordPayload()));
      Instant beginTag = Instant.now();
      // perform index loop up to get existing location of records
      HoodieData<HoodieRecord<T>> taggedRecords = table.getIndex().tagLocation(dedupedRecords, context, table);
      Duration tagLocationDuration = Duration.between(beginTag, Instant.now());

      // filter out non existent keys/records
      HoodieData<HoodieRecord<T>> taggedValidRecords = taggedRecords.filter(HoodieRecord::isCurrentLocationKnown);
      HoodieWriteMetadata<HoodieData<WriteStatus>> result;
      if (!taggedValidRecords.isEmpty()) {
        result = deleteExecutor.execute(taggedValidRecords);
        result.setIndexLookupDuration(tagLocationDuration);
      } else {
        // if entire set of keys are non existent
        deleteExecutor.saveWorkloadProfileMetadataToInflight(new WorkloadProfile(Pair.of(new HashMap<>(), new WorkloadStat())), instantTime);
        result = new HoodieWriteMetadata<>();
        result.setWriteStatuses(context.emptyHoodieData());
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
