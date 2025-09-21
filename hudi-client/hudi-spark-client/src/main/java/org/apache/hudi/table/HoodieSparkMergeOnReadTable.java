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

package org.apache.hudi.table;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.io.HoodieAppendHandle;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.hudi.table.action.bootstrap.SparkBootstrapDeltaCommitActionExecutor;
import org.apache.hudi.table.action.compact.HoodieSparkMergeOnReadTableCompactor;
import org.apache.hudi.table.action.compact.RunCompactionActionExecutor;
import org.apache.hudi.table.action.compact.ScheduleCompactionActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkBulkInsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkBulkInsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkDeleteDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkDeletePreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkInsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkInsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkUpsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkUpsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.restore.MergeOnReadRestoreActionExecutor;
import org.apache.hudi.table.action.rollback.BaseRollbackPlanActionExecutor;
import org.apache.hudi.table.action.rollback.MergeOnReadRollbackActionExecutor;
import org.apache.hudi.table.action.rollback.RestorePlanActionExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.deleteMetadataTable;

/**
 * Implementation of a more real-time Hoodie Table the provides tradeoffs on read and write cost/amplification.
 *
 * <p>
 * INSERTS - Same as HoodieCopyOnWriteTable - Produce new files, block aligned to desired size (or) Merge with the
 * smallest existing file, to expand it
 * </p>
 * <p>
 * UPDATES - Appends the changes to a rolling log file maintained per file Id. Compaction merges the log file into the
 * base file.
 * </p>
 * <p>
 * WARNING - MOR table type does not support nested rollbacks, every rollback must be followed by an attempted commit
 * action
 * </p>
 */
public class HoodieSparkMergeOnReadTable<T> extends HoodieSparkCopyOnWriteTable<T> implements HoodieCompactionHandler<T> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkMergeOnReadTable.class);

  HoodieSparkMergeOnReadTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient, Option<TransactionManager> txnManager) {
    super(config, context, metaClient, txnManager);
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> upsert(HoodieEngineContext context, String instantTime, HoodieData<HoodieRecord<T>> records) {
    return new SparkUpsertDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> insert(HoodieEngineContext context, String instantTime, HoodieData<HoodieRecord<T>> records) {
    return new SparkInsertDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> bulkInsert(HoodieEngineContext context, String instantTime, HoodieData<HoodieRecord<T>> records,
      Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    return new SparkBulkInsertDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config,
        this, instantTime, records, userDefinedBulkInsertPartitioner).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> delete(HoodieEngineContext context, String instantTime, HoodieData<HoodieKey> keys) {
    return new SparkDeleteDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, keys).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> deletePrepped(HoodieEngineContext context, String instantTime, HoodieData<HoodieRecord<T>> preppedRecords) {
    return new SparkDeletePreppedDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> upsertPrepped(HoodieEngineContext context, String instantTime,
      HoodieData<HoodieRecord<T>> preppedRecords) {
    return new SparkUpsertPreppedDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> insertPrepped(HoodieEngineContext context, String instantTime,
      HoodieData<HoodieRecord<T>> preppedRecords) {
    return new SparkInsertPreppedDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> bulkInsertPrepped(HoodieEngineContext context, String instantTime,
      HoodieData<HoodieRecord<T>> preppedRecords,  Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    return new SparkBulkInsertPreppedDeltaCommitActionExecutor((HoodieSparkEngineContext) context, config,
        this, instantTime, preppedRecords, userDefinedBulkInsertPartitioner).execute();
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    ScheduleCompactionActionExecutor scheduleCompactionExecutor = new ScheduleCompactionActionExecutor(
        context, config, this, instantTime, extraMetadata, WriteOperationType.COMPACT);
    return scheduleCompactionExecutor.execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> compact(
      HoodieEngineContext context, String compactionInstantTime) {
    RunCompactionActionExecutor<T> compactionExecutor = new RunCompactionActionExecutor<>(
        context, config, this, compactionInstantTime, new HoodieSparkMergeOnReadTableCompactor<>(),
        WriteOperationType.COMPACT);
    return compactionExecutor.execute();
  }

  @Override
  public HoodieBootstrapWriteMetadata<HoodieData<WriteStatus>> bootstrap(HoodieEngineContext context, Option<Map<String, String>> extraMetadata) {
    return new SparkBootstrapDeltaCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, extraMetadata).execute();
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleLogCompaction(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    ScheduleCompactionActionExecutor scheduleLogCompactionExecutor = new ScheduleCompactionActionExecutor(
        context, config, this, instantTime, extraMetadata, WriteOperationType.LOG_COMPACT);
    return scheduleLogCompactionExecutor.execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> logCompact(
      HoodieEngineContext context, String logCompactionInstantTime) {
    RunCompactionActionExecutor logCompactionExecutor = new RunCompactionActionExecutor(context, config, this,
        logCompactionInstantTime, new HoodieSparkMergeOnReadTableCompactor<>(), WriteOperationType.LOG_COMPACT);
    return logCompactionExecutor.execute();
  }

  @Override
  public void rollbackBootstrap(HoodieEngineContext context, String instantTime) {
    // Delete metadata table to rollback a failed bootstrap. re-attempt of bootstrap will re-initialize the mdt.
    try {
      LOG.info("Deleting metadata table because we are rolling back failed bootstrap. ");
      deleteMetadataTable(config.getBasePath(), context);
    } catch (HoodieMetadataException e) {
      throw new HoodieException("Failed to delete metadata table.", e);
    }

    new RestorePlanActionExecutor<>(context, config, this, instantTime, HoodieTimeline.INIT_INSTANT_TS).execute();
    new MergeOnReadRestoreActionExecutor<>(context, config, this, instantTime, HoodieTimeline.INIT_INSTANT_TS).execute();
  }

  @Override
  public Option<HoodieRollbackPlan> scheduleRollback(HoodieEngineContext context,
                                                     String instantTime,
                                                     HoodieInstant instantToRollback, boolean skipTimelinePublish, boolean shouldRollbackUsingMarkers,
                                                     boolean isRestore) {
    return new BaseRollbackPlanActionExecutor<>(context, config, this, instantTime, instantToRollback, skipTimelinePublish,
        shouldRollbackUsingMarkers, isRestore).execute();
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsertsForLogCompaction(String instantTime, String partitionPath, String fileId,
                                                          Map<String, HoodieRecord<?>> recordMap,
                                                          Map<HoodieLogBlock.HeaderMetadataType, String> header) {
    HoodieAppendHandle appendHandle = new HoodieAppendHandle(config, instantTime, this,
        partitionPath, fileId, recordMap.values().iterator(), taskContextSupplier, header);
    appendHandle.write(recordMap);
    List<WriteStatus> writeStatuses = appendHandle.close();
    return Collections.singletonList(writeStatuses).iterator();
  }

  @Override
  public HoodieRollbackMetadata rollback(HoodieEngineContext context,
                                         String rollbackInstantTime,
                                         HoodieInstant commitInstant,
                                         boolean deleteInstants,
                                         boolean skipLocking) {
    return new MergeOnReadRollbackActionExecutor<>(context, config, this, rollbackInstantTime, commitInstant, deleteInstants, skipLocking).execute();
  }

  @Override
  public HoodieRestoreMetadata restore(HoodieEngineContext context, String restoreInstantTimestamp, String savepointToRestoreTimestamp) {
    return new MergeOnReadRestoreActionExecutor<>(context, config, this, restoreInstantTimestamp, savepointToRestoreTimestamp).execute();
  }

  @Override
  public void finalizeWrite(HoodieEngineContext context, String instantTs, List<HoodieWriteStat> stats)
      throws HoodieIOException {
    // delegate to base class for MOR tables
    super.finalizeWrite(context, instantTs, stats);
  }
}
