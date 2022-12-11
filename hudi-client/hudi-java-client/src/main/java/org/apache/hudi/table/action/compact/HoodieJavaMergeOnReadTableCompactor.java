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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import java.util.List;

/**
 * Compacts a hoodie table with merge on read storage in Java engine. Computes all possible
 * compactions, passes it through a CompactionFilter and executes all the compactions and
 * writes a new version of base files and make a normal commit.
 */
public class HoodieJavaMergeOnReadTableCompactor<T extends HoodieRecordPayload>
    extends HoodieCompactor<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {

  @Override
  public void preCompact(
      HoodieTable table, HoodieTimeline pendingCompactionTimeline, WriteOperationType operationType, String instantTime) {
    if (WriteOperationType.LOG_COMPACT.equals(operationType)) {
      throw new UnsupportedOperationException("Log compaction is not supported for this execution engine.");
    }
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(instantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      table.rollbackInflightCompaction(inflightInstant);
      table.getMetaClient().reloadActiveTimeline();
    }
  }

  @Override
  public void maybePersist(HoodieData<WriteStatus> writeStatus, HoodieWriteConfig config) {
    // No OP
  }
}
