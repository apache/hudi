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
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieCompactionHandler;
import org.apache.hudi.table.HoodieTable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public class CompactionExecutionHelper<T extends HoodieRecordPayload, I, K, O> implements Serializable {

  protected void transitionRequestedToInflight(HoodieTable table, String compactionInstantTime) {
    HoodieActiveTimeline timeline = table.getActiveTimeline();
    HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    // Mark instant as compaction inflight
    timeline.transitionCompactionRequestedToInflight(instant);
  }

  protected String instantTimeToUseForScanning(String compactionInstantTime, String maxInstantTime) {
    return maxInstantTime;
  }

  protected boolean shouldPreserveCommitMetadata() {
    return false;
  }

  protected Iterator<List<WriteStatus>> writeFileAndGetWriteStats(HoodieCompactionHandler compactionHandler,
                                                                  CompactionOperation operation,
                                                                  String instantTime,
                                                                  HoodieMergedLogRecordScanner scanner,
                                                                  Option<HoodieBaseFile> oldDataFileOpt) throws IOException {
    Iterator<List<WriteStatus>> result;
    // If the dataFile is present, perform updates else perform inserts into a new base file.
    if (oldDataFileOpt.isPresent()) {
      result = compactionHandler.handleUpdate(instantTime, operation.getPartitionPath(),
          operation.getFileId(), scanner.getRecords(),
          oldDataFileOpt.get());
    } else {
      result = compactionHandler.handleInsert(instantTime, operation.getPartitionPath(), operation.getFileId(),
          scanner.getRecords());
    }
    return result;
  }

  protected boolean useScanV2(HoodieWriteConfig writeConfig) {
    return writeConfig.useScanV2ForLogRecordReader();
  }

}
