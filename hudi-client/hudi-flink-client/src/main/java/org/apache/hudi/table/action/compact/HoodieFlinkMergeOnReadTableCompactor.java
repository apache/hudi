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
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Compacts a hoodie table with merge on read storage. Computes all possible compactions,
 * passes it through a CompactionFilter and executes all the compactions and writes a new version of base files and make
 * a normal commit.
 *
 * <p>Note: the compaction logic is invoked through the flink pipeline.
 */
@SuppressWarnings("checkstyle:LineLength")
public class HoodieFlinkMergeOnReadTableCompactor<T>
    extends HoodieCompactor<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieFlinkMergeOnReadTableCompactor.class);

  @Override
  public void preCompact(
      HoodieTable table, HoodieTimeline pendingCompactionTimeline, WriteOperationType operationType, String instantTime) {
    InstantGenerator instantGenerator = table.getInstantGenerator();
    HoodieInstant inflightInstant = WriteOperationType.COMPACT.equals(operationType)
        ? instantGenerator.getCompactionInflightInstant(instantTime)
        : instantGenerator.getLogCompactionInflightInstant(instantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      TransactionManager transactionManager = table.getTxnManager();
      table.rollbackInflightCompaction(inflightInstant, transactionManager);
      table.getMetaClient().reloadActiveTimeline();
    }
  }

  public List<WriteStatus> compact(HoodieWriteConfig writeConfig,
                                   CompactionOperation operation,
                                   String instantTime,
                                   TaskContextSupplier taskContextSupplier,
                                   HoodieReaderContext<?> readerContext,
                                   HoodieTable table) throws IOException {
    String maxInstantTime = getMaxInstantTime(table.getMetaClient());
    LOG.info("Compact using file group reader based compaction, operation: {}.", operation);
    return compact(
        writeConfig,
        operation,
        instantTime,
        readerContext,
        table,
        maxInstantTime,
        taskContextSupplier);
  }

  @Override
  public void maybePersist(HoodieData<WriteStatus> writeStatus, HoodieEngineContext context, HoodieWriteConfig config, String instantTime) {
    // No OP
  }

  @Override
  protected HoodieRecord.HoodieRecordType getEngineRecordType() {
    return HoodieRecord.HoodieRecordType.FLINK;
  }
}
