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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieListData;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.hudi.table.action.clean.JavaCleanActionExecutor;
import org.apache.hudi.table.action.clean.JavaScheduleCleanActionExecutor;
import org.apache.hudi.table.action.commit.BulkInsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.BulkInsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.DeleteCommitActionExecutor;
import org.apache.hudi.table.action.commit.InsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.InsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.JavaBulkInsertHelper;
import org.apache.hudi.table.action.commit.JavaCommitHelper;
import org.apache.hudi.table.action.commit.JavaDeleteHelper;
import org.apache.hudi.table.action.commit.JavaInsertOverwriteCommitHelper;
import org.apache.hudi.table.action.commit.JavaInsertOverwriteTableCommitHelper;
import org.apache.hudi.table.action.commit.JavaWriteHelper;
import org.apache.hudi.table.action.commit.UpsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.UpsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.restore.JavaCopyOnWriteRestoreActionExecutor;
import org.apache.hudi.table.action.rollback.BaseRollbackPlanActionExecutor;
import org.apache.hudi.table.action.rollback.CopyOnWriteRollbackActionExecutor;
import org.apache.hudi.table.action.savepoint.SavepointActionExecutor;

import java.util.List;
import java.util.Map;

public class HoodieJavaCopyOnWriteTable<T extends HoodieRecordPayload<T>> extends HoodieJavaTable<T> {
  protected HoodieJavaCopyOnWriteTable(HoodieWriteConfig config,
                                       HoodieEngineContext context,
                                       HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> upsert(HoodieEngineContext context,
                                                       String instantTime,
                                                       List<HoodieRecord<T>> records) {
    return convertMetadata(new UpsertCommitActionExecutor<T>(
        context, config, this, instantTime, HoodieListData.of(records),
        new JavaCommitHelper<>(context, config, this, instantTime, WriteOperationType.UPSERT),
        JavaWriteHelper.newInstance()).execute());
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insert(HoodieEngineContext context,
                                                       String instantTime,
                                                       List<HoodieRecord<T>> records) {
    return convertMetadata(new InsertCommitActionExecutor<T>(
        context, config, this, instantTime, WriteOperationType.INSERT, HoodieListData.of(records),
        new JavaCommitHelper<>(context, config, this, instantTime, WriteOperationType.INSERT),
        JavaWriteHelper.newInstance()).execute());
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> bulkInsert(HoodieEngineContext context,
                                                           String instantTime,
                                                           List<HoodieRecord<T>> records,
                                                           Option<BulkInsertPartitioner<List<HoodieRecord<T>>>> bulkInsertPartitioner) {
    return convertMetadata(new BulkInsertCommitActionExecutor<T>(
        context, config, this, instantTime, HoodieListData.of(records),
        convertBulkInsertPartitioner(bulkInsertPartitioner),
        new JavaCommitHelper<>(context, config, this, instantTime, WriteOperationType.BULK_INSERT),
        JavaBulkInsertHelper.newInstance()).execute());
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> delete(HoodieEngineContext context,
                                                       String instantTime,
                                                       List<HoodieKey> keys) {
    return convertMetadata(new DeleteCommitActionExecutor<T>(
        context, config, this, instantTime, HoodieListData.of(keys),
        new JavaCommitHelper<>(context, config, this, instantTime, WriteOperationType.DELETE),
        JavaDeleteHelper.newInstance()).execute());
  }

  @Override
  public HoodieWriteMetadata deletePartitions(HoodieEngineContext context, String instantTime, List<String> partitions) {
    throw new HoodieNotSupportedException("Delete partitions is not supported yet");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> upsertPrepped(HoodieEngineContext context,
                                                              String instantTime,
                                                              List<HoodieRecord<T>> preppedRecords) {
    return convertMetadata(new UpsertPreppedCommitActionExecutor<>(
        context, config, this, instantTime, HoodieListData.of(preppedRecords),
        new JavaCommitHelper<>(
            context, config, this, instantTime, WriteOperationType.UPSERT_PREPPED)).execute());
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insertPrepped(HoodieEngineContext context,
                                                              String instantTime,
                                                              List<HoodieRecord<T>> preppedRecords) {
    return convertMetadata(new InsertPreppedCommitActionExecutor<>(
        context, config, this, instantTime, HoodieListData.of(preppedRecords),
        new JavaCommitHelper<>(
            context, config, this, instantTime, WriteOperationType.INSERT_PREPPED)).execute());
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> bulkInsertPrepped(HoodieEngineContext context,
                                                                  String instantTime,
                                                                  List<HoodieRecord<T>> preppedRecords,
                                                                  Option<BulkInsertPartitioner<List<HoodieRecord<T>>>> bulkInsertPartitioner) {
    return convertMetadata(new BulkInsertPreppedCommitActionExecutor<T>(
        context, config, this, instantTime, HoodieListData.of(preppedRecords),
        convertBulkInsertPartitioner(bulkInsertPartitioner),
        new JavaCommitHelper<>(context, config, this, instantTime, WriteOperationType.BULK_INSERT_PREPPED),
        JavaBulkInsertHelper.newInstance()).execute());
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insertOverwrite(HoodieEngineContext context,
                                                                String instantTime,
                                                                List<HoodieRecord<T>> records) {
    return convertMetadata(new InsertCommitActionExecutor<T>(
        context, config, this, instantTime, WriteOperationType.INSERT_OVERWRITE,
        HoodieListData.of(records),
        new JavaInsertOverwriteCommitHelper<>(
            context, config, this, instantTime, WriteOperationType.INSERT_OVERWRITE),
        JavaWriteHelper.newInstance()).execute());
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insertOverwriteTable(HoodieEngineContext context,
                                                                     String instantTime,
                                                                     List<HoodieRecord<T>> records) {
    return convertMetadata(new InsertCommitActionExecutor<T>(
        context, config, this, instantTime, WriteOperationType.INSERT_OVERWRITE_TABLE,
        HoodieListData.of(records),
        new JavaInsertOverwriteTableCommitHelper<>(
            context, config, this, instantTime, WriteOperationType.INSERT_OVERWRITE_TABLE),
        JavaWriteHelper.newInstance()).execute());
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context,
                                                         String instantTime,
                                                         Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("ScheduleCompaction is not supported yet");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> compact(HoodieEngineContext context,
                                                        String compactionInstantTime) {
    throw new HoodieNotSupportedException("Compact is not supported yet");
  }

  @Override
  public Option<HoodieClusteringPlan> scheduleClustering(final HoodieEngineContext context, final String instantTime, final Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("Clustering is not supported yet");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> cluster(final HoodieEngineContext context, final String clusteringInstantTime) {
    throw new HoodieNotSupportedException("Clustering is not supported yet");
  }

  @Override
  public HoodieBootstrapWriteMetadata<List<WriteStatus>> bootstrap(HoodieEngineContext context,
                                                                   Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("Bootstrap is not supported yet");
  }

  @Override
  public void rollbackBootstrap(HoodieEngineContext context,
                                String instantTime) {
    throw new HoodieNotSupportedException("RollbackBootstrap is not supported yet");
  }

  @Override
  public Option<HoodieRollbackPlan> scheduleRollback(HoodieEngineContext context, String instantTime, HoodieInstant instantToRollback,
                                                     boolean skipTimelinePublish) {
    return new BaseRollbackPlanActionExecutor(context, config, this, instantTime, instantToRollback, skipTimelinePublish).execute();
  }

  @Override
  public Option<HoodieCleanerPlan> scheduleCleaning(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    return new JavaScheduleCleanActionExecutor<>(context, config, this, instantTime, extraMetadata).execute();
  }

  @Override
  public HoodieCleanMetadata clean(HoodieEngineContext context,
                                   String cleanInstantTime) {
    return new JavaCleanActionExecutor(context, config, this, cleanInstantTime).execute();
  }

  @Override
  public HoodieRollbackMetadata rollback(HoodieEngineContext context,
                                         String rollbackInstantTime,
                                         HoodieInstant commitInstant,
                                         boolean deleteInstants) {
    return new CopyOnWriteRollbackActionExecutor(
        context, config, this, rollbackInstantTime, commitInstant, deleteInstants).execute();
  }

  @Override
  public HoodieSavepointMetadata savepoint(HoodieEngineContext context,
                                           String instantToSavepoint,
                                           String user,
                                           String comment) {
    return new SavepointActionExecutor(
        context, config, this, instantToSavepoint, user, comment).execute();
  }

  @Override
  public HoodieRestoreMetadata restore(HoodieEngineContext context,
                                       String restoreInstantTime,
                                       String instantToRestore) {
    return new JavaCopyOnWriteRestoreActionExecutor((HoodieJavaEngineContext) context,
        config, this, restoreInstantTime, instantToRestore).execute();
  }
}
