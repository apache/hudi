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
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieCompactionHandler;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Strategy class to execute log compaction operations.
 */
public class LogCompactionExecutionHelper<T extends HoodieRecordPayload, I, K, O>
    extends CompactionExecutionHelper<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(LogCompactionExecutionHelper.class);

  @Override
  protected void transitionRequestedToInflight(HoodieTable table, String logCompactionInstantTime) {
    HoodieActiveTimeline timeline = table.getActiveTimeline();
    HoodieInstant instant = HoodieTimeline.getLogCompactionRequestedInstant(logCompactionInstantTime);
    // Mark instant as compaction inflight
    timeline.transitionLogCompactionRequestedToInflight(instant);
  }

  protected String instantTimeToUseForScanning(String logCompactionInstantTime, String maxInstantTime) {
    return logCompactionInstantTime;
  }

  protected boolean shouldPreserveCommitMetadata() {
    return true;
  }

  @Override
  protected Iterator<List<WriteStatus>> writeFileAndGetWriteStats(HoodieCompactionHandler compactionHandler,
                                                                  CompactionOperation operation,
                                                                  String instantTime,
                                                                  HoodieMergedLogRecordScanner scanner,
                                                                  Option<HoodieBaseFile> oldDataFileOpt) throws IOException {
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.COMPACTED_BLOCK_TIMES,
        StringUtils.join(scanner.getValidBlockInstants(), ","));
    // Compacting is very similar to applying updates to existing file
    return compactionHandler.handleInsertsForLogCompaction(instantTime, operation.getPartitionPath(),
        operation.getFileId(), scanner.getRecords(), header);
  }

  @Override
  protected boolean enableOptimizedLogBlockScan(HoodieWriteConfig writeConfig) {
    return true;
  }
}
