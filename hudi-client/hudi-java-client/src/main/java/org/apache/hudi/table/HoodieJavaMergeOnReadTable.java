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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.HoodieJavaMergeOnReadTableCompactor;
import org.apache.hudi.table.action.compact.RunCompactionActionExecutor;
import org.apache.hudi.table.action.compact.ScheduleCompactionActionExecutor;
import org.apache.hudi.table.action.deltacommit.JavaBulkInsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.JavaBulkInsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.JavaDeleteDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.JavaDeletePreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.JavaInsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.JavaUpsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.JavaUpsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.restore.MergeOnReadRestoreActionExecutor;
import org.apache.hudi.table.action.rollback.MergeOnReadRollbackActionExecutor;

import java.util.List;
import java.util.Map;

public class HoodieJavaMergeOnReadTable<T> extends HoodieJavaCopyOnWriteTable<T> {
  protected HoodieJavaMergeOnReadTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> upsert(HoodieEngineContext context, String instantTime, List<HoodieRecord<T>> hoodieRecords) {
    return new JavaUpsertDeltaCommitActionExecutor<>((HoodieJavaEngineContext) context, config, this, instantTime, hoodieRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insert(HoodieEngineContext context, String instantTime, List<HoodieRecord<T>> hoodieRecords) {
    return new JavaInsertDeltaCommitActionExecutor<>((HoodieJavaEngineContext) context, config, this, instantTime, hoodieRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> upsertPrepped(HoodieEngineContext context,
                                                              String instantTime,
                                                              List<HoodieRecord<T>> preppedRecords) {
    return new JavaUpsertPreppedDeltaCommitActionExecutor<>((HoodieJavaEngineContext) context, config,
        this, instantTime, preppedRecords).execute();

  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> delete(HoodieEngineContext context, String instantTime, List<HoodieKey> keys) {
    return new JavaDeleteDeltaCommitActionExecutor<>(context, config, this, instantTime, keys).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> deletePrepped(HoodieEngineContext context, String instantTime, List<HoodieRecord<T>> preppedRecords) {
    return new JavaDeletePreppedDeltaCommitActionExecutor<>((HoodieJavaEngineContext) context, config,
        this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> bulkInsert(HoodieEngineContext context, String instantTime, List<HoodieRecord<T>> hoodieRecords, Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    return new JavaBulkInsertDeltaCommitActionExecutor((HoodieJavaEngineContext) context, config,
        this, instantTime, hoodieRecords, bulkInsertPartitioner).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> bulkInsertPrepped(HoodieEngineContext context,
                                                                  String instantTime,
                                                                  List<HoodieRecord<T>> preppedRecords,
                                                                  Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    return new JavaBulkInsertPreppedDeltaCommitActionExecutor((HoodieJavaEngineContext) context, config,
        this, instantTime, preppedRecords, bulkInsertPartitioner).execute();
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    ScheduleCompactionActionExecutor scheduleCompactionExecutor = new ScheduleCompactionActionExecutor(
        context, config, this, instantTime, extraMetadata, WriteOperationType.COMPACT);
    return scheduleCompactionExecutor.execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> compact(
      HoodieEngineContext context, String compactionInstantTime) {
    RunCompactionActionExecutor compactionExecutor = new RunCompactionActionExecutor(
        context, config, this, compactionInstantTime, new HoodieJavaMergeOnReadTableCompactor(),
        new HoodieJavaCopyOnWriteTable(config, context, getMetaClient()), WriteOperationType.COMPACT);
    return convertMetadata(compactionExecutor.execute());
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleLogCompaction(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    ScheduleCompactionActionExecutor scheduleLogCompactionExecutor = new ScheduleCompactionActionExecutor(
        context, config, this, instantTime, extraMetadata, WriteOperationType.LOG_COMPACT);
    return scheduleLogCompactionExecutor.execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> logCompact(HoodieEngineContext context, String logCompactionInstantTime) {
    RunCompactionActionExecutor logCompactionExecutor = new RunCompactionActionExecutor(context, config, this,
        logCompactionInstantTime, new HoodieJavaMergeOnReadTableCompactor<>(), this, WriteOperationType.LOG_COMPACT);
    return convertMetadata(logCompactionExecutor.execute());
  }

  @Override
  public HoodieRollbackMetadata rollback(HoodieEngineContext context, String rollbackInstantTime, HoodieInstant commitInstant, boolean deleteInstants, boolean skipLocking) {
    return new MergeOnReadRollbackActionExecutor<>(
        context, config, this, rollbackInstantTime, commitInstant, deleteInstants, skipLocking).execute();
  }

  @Override
  public HoodieRestoreMetadata restore(HoodieEngineContext context, String restoreInstantTimestamp, String savepointToRestoreTimestamp) {
    return new MergeOnReadRestoreActionExecutor<>(
        context, config, this, restoreInstantTimestamp, savepointToRestoreTimestamp).execute();
  }
}
