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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import static org.apache.hudi.config.HoodieWriteConfig.WRITE_STATUS_STORAGE_LEVEL_VALUE;

/**
 * Compacts a hoodie table with merge on read storage. Computes all possible compactions,
 * passes it through a CompactionFilter and executes all the compactions and writes a new version of base files and make
 * a normal commit
 */
@SuppressWarnings("checkstyle:LineLength")
public class HoodieSparkMergeOnReadTableCompactor<T>
    extends HoodieCompactor<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> {

  @Override
  public void preCompact(
      HoodieTable table, HoodieTimeline pendingCompactionTimeline, WriteOperationType operationType, String instantTime) {
    HoodieInstant requestedCompactionInstantTime = WriteOperationType.COMPACT.equals(operationType)
        ? table.getInstantGenerator().getCompactionRequestedInstant(instantTime)
        : table.getInstantGenerator().getLogCompactionRequestedInstant(instantTime);
    if (!pendingCompactionTimeline.containsInstant(requestedCompactionInstantTime)) {
      throw new IllegalStateException(
          "No Compaction request available at " + requestedCompactionInstantTime.requestedTime() + " to run compaction");
    }
  }

  @Override
  public void maybePersist(HoodieData<WriteStatus> writeStatus, HoodieEngineContext context, HoodieWriteConfig config, String instantTime) {
    writeStatus.persist(config.getString(WRITE_STATUS_STORAGE_LEVEL_VALUE), context, HoodieDataCacheKey.of(config.getBasePath(), instantTime));
  }

  @Override
  protected HoodieRecord.HoodieRecordType getEngineRecordType() {
    return HoodieRecord.HoodieRecordType.SPARK;
  }
}
