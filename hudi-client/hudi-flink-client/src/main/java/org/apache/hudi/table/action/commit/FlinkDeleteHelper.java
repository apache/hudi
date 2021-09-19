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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.data.HoodieListData;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:LineLength")
public class FlinkDeleteHelper<T extends HoodieRecordPayload<T>> extends BaseDeleteHelper<T> {

  private FlinkDeleteHelper() {
  }

  private static class DeleteHelperHolder {
    private static final FlinkDeleteHelper FLINK_DELETE_HELPER = new FlinkDeleteHelper();
  }

  public static FlinkDeleteHelper newInstance() {
    return DeleteHelperHolder.FLINK_DELETE_HELPER;
  }

  @Override
  public HoodieData<HoodieKey> deduplicateKeys(
      HoodieData<HoodieKey> keys,
      HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table,
      int parallelism) {
    List<HoodieKey> keysList = ((HoodieListData<HoodieKey>) keys).get();
    boolean isIndexingGlobal = table.getIndex().isGlobal();
    if (isIndexingGlobal) {
      HashSet<String> recordKeys = keysList.stream().map(HoodieKey::getRecordKey)
          .collect(Collectors.toCollection(HashSet::new));
      List<HoodieKey> deduplicatedKeys = new LinkedList<>();
      keysList.forEach(x -> {
        if (recordKeys.contains(x.getRecordKey())) {
          deduplicatedKeys.add(x);
        }
      });
      return HoodieListData.of(deduplicatedKeys);
    } else {
      HashSet<HoodieKey> set = new HashSet<>(keysList);
      keysList.clear();
      keysList.addAll(set);
      return keys;
    }
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute(
      String instantTime, HoodieData<HoodieKey> keys, HoodieEngineContext context,
      HoodieWriteConfig config, HoodieTable table,
      BaseCommitHelper<T> commitHelper) {
    try {
      HoodieWriteMetadata<HoodieData<WriteStatus>> result = null;
      HoodieData<HoodieKey> dedupedKeys = keys;
      final int parallelism = config.getDeleteShuffleParallelism();
      if (config.shouldCombineBeforeDelete()) {
        // De-dupe/merge if needed
        dedupedKeys = deduplicateKeys(keys, table, parallelism);
      }

      List<HoodieRecord> dedupedRecords = FlinkCommitHelper.getList(dedupedKeys).stream().map(
          key -> new HoodieRecord(key, new EmptyHoodieRecordPayload())).collect(Collectors.toList());
      Instant beginTag = Instant.now();
      // perform index look up to get existing location of records
      HoodieData<HoodieRecord<T>> taggedRecords = HoodieListData.of((List<HoodieRecord<T>>)
          table.getIndex().tagLocation(dedupedRecords, context, table));
      Duration tagLocationDuration = Duration.between(beginTag, Instant.now());

      // filter out non existent keys/records
      List<HoodieRecord<T>> taggedValidRecords =
          FlinkCommitHelper.getList(taggedRecords).stream()
              .filter(HoodieRecord::isCurrentLocationKnown).collect(Collectors.toList());
      if (!taggedValidRecords.isEmpty()) {
        result = commitHelper.execute(HoodieListData.of(taggedValidRecords));
        result.setIndexLookupDuration(tagLocationDuration);
      } else {
        // if entire set of keys are non existent
        commitHelper.saveWorkloadProfileMetadataToInflight(
            new WorkloadProfile(Pair.of(new HashMap<>(), new WorkloadStat())), instantTime);
        result = new HoodieWriteMetadata<>();
        result.setWriteStatuses(HoodieListData.of(Collections.EMPTY_LIST));
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
