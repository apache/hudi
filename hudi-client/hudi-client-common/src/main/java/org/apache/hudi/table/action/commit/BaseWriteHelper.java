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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.time.Duration;
import java.time.Instant;

public abstract class BaseWriteHelper<T extends HoodieRecordPayload<T>> {

  public HoodieWriteMetadata<HoodieData<WriteStatus>> write(
      String instantTime, HoodieData<HoodieRecord<T>> inputRecords, HoodieEngineContext context,
      HoodieTable table, boolean shouldCombine, int shuffleParallelism,
      BaseCommitHelper<T> commitHelper, boolean performTagging) {
    try {
      // De-dupe/merge if needed
      HoodieData<HoodieRecord<T>> dedupedRecords =
          combineOnCondition(shouldCombine, inputRecords, shuffleParallelism, table);

      Instant lookupBegin = Instant.now();
      HoodieData<HoodieRecord<T>> taggedRecords = dedupedRecords;
      if (performTagging) {
        // perform index loop up to get existing location of records
        taggedRecords = tag(dedupedRecords, context, table);
      }
      Duration indexLookupDuration = Duration.between(lookupBegin, Instant.now());

      HoodieWriteMetadata<HoodieData<WriteStatus>> result = commitHelper.execute(taggedRecords);
      result.setIndexLookupDuration(indexLookupDuration);
      return result;
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to upsert for commit time " + instantTime, e);
    }
  }

  protected abstract HoodieData<HoodieRecord<T>> tag(
      HoodieData<HoodieRecord<T>> dedupedRecords, HoodieEngineContext context, HoodieTable table);

  public HoodieData<HoodieRecord<T>> combineOnCondition(
      boolean condition, HoodieData<HoodieRecord<T>> records, int parallelism, HoodieTable table) {
    return condition ? deduplicateRecords(records, table, parallelism) : records;
  }

  /**
   * Deduplicate Hoodie records, using the given deduplication function.
   *
   * @param records     hoodieRecords to deduplicate
   * @param parallelism parallelism or partitions to be used while reducing/deduplicating
   * @return Collection of HoodieRecord already be deduplicated
   */
  public HoodieData<HoodieRecord<T>> deduplicateRecords(
      HoodieData<HoodieRecord<T>> records, HoodieTable table, int parallelism) {
    return deduplicateRecords(records, table.getIndex(), parallelism);
  }

  public abstract HoodieData<HoodieRecord<T>> deduplicateRecords(
      HoodieData<HoodieRecord<T>> records, HoodieIndex index, int parallelism);
}
