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
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.FlinkAppendHandle;
import org.apache.hudi.io.HoodieAppendHandle;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.delta.FlinkUpsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.commit.delta.FlinkUpsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.compact.HoodieFlinkMergeOnReadTableCompactor;
import org.apache.hudi.table.action.compact.RunCompactionActionExecutor;
import org.apache.hudi.table.action.compact.ScheduleCompactionActionExecutor;
import org.apache.hudi.table.action.rollback.BaseRollbackPlanActionExecutor;
import org.apache.hudi.table.action.rollback.MergeOnReadRollbackActionExecutor;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Flink MERGE_ON_READ table.
 */
public class HoodieFlinkMergeOnReadTable<T>
    extends HoodieFlinkCopyOnWriteTable<T> {

  HoodieFlinkMergeOnReadTable(
      HoodieWriteConfig config,
      HoodieEngineContext context,
      HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> upsert(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      BucketInfo bucketInfo,
      String instantTime,
      Iterator<HoodieRecord<T>> records) {
    ValidationUtils.checkArgument(writeHandle instanceof FlinkAppendHandle,
        "MOR RowData handle should always be a FlinkAppendHandle");
    return new FlinkUpsertDeltaCommitActionExecutor<>(context, writeHandle, bucketInfo, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> upsertPrepped(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      BucketInfo bucketInfo,
      String instantTime,
      List<HoodieRecord<T>> preppedRecords) {
    ValidationUtils.checkArgument(writeHandle instanceof FlinkAppendHandle,
        "MOR write handle should always be a FlinkAppendHandle");
    FlinkAppendHandle<?, ?, ?, ?> appendHandle = (FlinkAppendHandle<?, ?, ?, ?>) writeHandle;
    return new FlinkUpsertPreppedDeltaCommitActionExecutor<>(context, appendHandle, bucketInfo, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insert(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      BucketInfo bucketInfo,
      String instantTime,
      Iterator<HoodieRecord<T>> hoodieRecords) {
    if (writeHandle instanceof FlinkAppendHandle) {
      FlinkAppendHandle<?, ?, ?, ?> appendHandle = (FlinkAppendHandle<?, ?, ?, ?>) writeHandle;
      return new FlinkUpsertDeltaCommitActionExecutor<>(context, appendHandle, bucketInfo, config, this, instantTime, hoodieRecords).execute();
    } else {
      return super.insert(context, writeHandle, bucketInfo, instantTime, hoodieRecords);
    }
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(
      HoodieEngineContext context,
      String instantTime,
      Option<Map<String, String>> extraMetadata) {
    ScheduleCompactionActionExecutor scheduleCompactionExecutor = new ScheduleCompactionActionExecutor(
        context, config, this, instantTime, extraMetadata, WriteOperationType.COMPACT);
    return scheduleCompactionExecutor.execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> compact(
      HoodieEngineContext context, String compactionInstantTime) {
    RunCompactionActionExecutor compactionExecutor = new RunCompactionActionExecutor(
        context, config, this, compactionInstantTime, new HoodieFlinkMergeOnReadTableCompactor(),
        new HoodieFlinkCopyOnWriteTable(config, context, getMetaClient()), WriteOperationType.COMPACT);
    return convertMetadata(compactionExecutor.execute());
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleLogCompaction(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    ScheduleCompactionActionExecutor scheduleLogCompactionExecutor = new ScheduleCompactionActionExecutor(
        context, config, this, instantTime, extraMetadata, WriteOperationType.LOG_COMPACT);
    return scheduleLogCompactionExecutor.execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> logCompact(
      HoodieEngineContext context, String logCompactionInstantTime) {
    RunCompactionActionExecutor logCompactionExecutor = new RunCompactionActionExecutor(context, config, this,
        logCompactionInstantTime, new HoodieFlinkMergeOnReadTableCompactor<>(), this, WriteOperationType.LOG_COMPACT);
    return convertMetadata(logCompactionExecutor.execute());
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
  public Option<HoodieRollbackPlan> scheduleRollback(HoodieEngineContext context, String instantTime, HoodieInstant instantToRollback,
                                                     boolean skipTimelinePublish, boolean shouldRollbackUsingMarkers, boolean isRestore) {
    return new BaseRollbackPlanActionExecutor(context, config, this, instantTime, instantToRollback, skipTimelinePublish,
        shouldRollbackUsingMarkers, isRestore).execute();
  }

  @Override
  public HoodieRollbackMetadata rollback(HoodieEngineContext context, String rollbackInstantTime, HoodieInstant commitInstant,
                                         boolean deleteInstants, boolean skipLocking) {
    return new MergeOnReadRollbackActionExecutor(context, config, this, rollbackInstantTime, commitInstant, deleteInstants,
        skipLocking).execute();
  }
}

