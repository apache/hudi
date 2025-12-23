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
import org.apache.hudi.avro.model.HoodieIndexCommitMetadata;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.HoodieMergeHandleFactory;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.hudi.table.action.clean.CleanActionExecutor;
import org.apache.hudi.table.action.clean.CleanPlanActionExecutor;
import org.apache.hudi.table.action.cluster.ClusteringPlanActionExecutor;
import org.apache.hudi.table.action.cluster.JavaExecuteClusteringCommitActionExecutor;
import org.apache.hudi.table.action.commit.JavaBulkInsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.JavaBulkInsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.JavaDeleteCommitActionExecutor;
import org.apache.hudi.table.action.commit.JavaDeletePreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.JavaInsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.JavaInsertOverwriteCommitActionExecutor;
import org.apache.hudi.table.action.commit.JavaInsertOverwriteTableCommitActionExecutor;
import org.apache.hudi.table.action.commit.JavaInsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.JavaUpsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.JavaUpsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.index.RunIndexActionExecutor;
import org.apache.hudi.table.action.index.ScheduleIndexActionExecutor;
import org.apache.hudi.table.action.restore.CopyOnWriteRestoreActionExecutor;
import org.apache.hudi.table.action.rollback.BaseRollbackPlanActionExecutor;
import org.apache.hudi.table.action.rollback.CopyOnWriteRollbackActionExecutor;
import org.apache.hudi.table.action.rollback.RestorePlanActionExecutor;
import org.apache.hudi.table.action.savepoint.SavepointActionExecutor;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
public class HoodieJavaCopyOnWriteTable<T>
    extends HoodieJavaTable<T> implements HoodieCompactionHandler<T> {

  protected HoodieJavaCopyOnWriteTable(HoodieWriteConfig config,
                                       HoodieEngineContext context,
                                       HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> upsert(HoodieEngineContext context,
                                                       String instantTime,
                                                       List<HoodieRecord<T>> records) {
    return new JavaUpsertCommitActionExecutor<>(context, config,
        this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insert(HoodieEngineContext context,
                                                       String instantTime,
                                                       List<HoodieRecord<T>> records) {
    return new JavaInsertCommitActionExecutor<>(context, config,
        this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> bulkInsert(HoodieEngineContext context,
                                                           String instantTime,
                                                           List<HoodieRecord<T>> records,
                                                           Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    return new JavaBulkInsertCommitActionExecutor((HoodieJavaEngineContext) context, config,
        this, instantTime, records, bulkInsertPartitioner).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> delete(HoodieEngineContext context,
                                                       String instantTime,
                                                       List<HoodieKey> keys) {
    return new JavaDeleteCommitActionExecutor<>(context, config, this, instantTime, keys).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> deletePrepped(HoodieEngineContext context, String instantTime, List<HoodieRecord<T>> preppedRecords) {
    return new JavaDeletePreppedCommitActionExecutor<>((HoodieJavaEngineContext) context, config,
        this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata deletePartitions(HoodieEngineContext context, String instantTime, List<String> partitions) {
    throw new HoodieNotSupportedException("Delete partitions is not supported yet");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> upsertPrepped(HoodieEngineContext context,
                                                              String instantTime,
                                                              List<HoodieRecord<T>> preppedRecords) {
    return new JavaUpsertPreppedCommitActionExecutor<>((HoodieJavaEngineContext) context, config,
        this, instantTime, preppedRecords).execute();

  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insertPrepped(HoodieEngineContext context,
                                                              String instantTime,
                                                              List<HoodieRecord<T>> preppedRecords) {
    return new JavaInsertPreppedCommitActionExecutor<>((HoodieJavaEngineContext) context, config,
        this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> bulkInsertPrepped(HoodieEngineContext context,
                                                                  String instantTime,
                                                                  List<HoodieRecord<T>> preppedRecords,
                                                                  Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    return new JavaBulkInsertPreppedCommitActionExecutor((HoodieJavaEngineContext) context, config,
        this, instantTime, preppedRecords, bulkInsertPartitioner).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insertOverwrite(HoodieEngineContext context,
                                                                String instantTime,
                                                                List<HoodieRecord<T>> records) {
    return new JavaInsertOverwriteCommitActionExecutor(
        context, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insertOverwriteTable(HoodieEngineContext context,
                                                                     String instantTime,
                                                                     List<HoodieRecord<T>> records) {
    return new JavaInsertOverwriteTableCommitActionExecutor(
        context, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> managePartitionTTL(HoodieEngineContext context, String instantTime) {
    throw new HoodieNotSupportedException("Manage partition ttl is not supported yet");
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context,
                                                         String instantTime,
                                                         Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("ScheduleCompaction is not supported on a CopyOnWrite table");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> compact(HoodieEngineContext context,
                                                        String compactionInstantTime) {
    throw new HoodieNotSupportedException("Compaction is not supported on a CopyOnWrite table");
  }

  @Override
  public Option<HoodieClusteringPlan> scheduleClustering(final HoodieEngineContext context, final String instantTime, final Option<Map<String, String>> extraMetadata) {
    return new ClusteringPlanActionExecutor<>(context, config, this, instantTime, extraMetadata).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> cluster(final HoodieEngineContext context, final String clusteringInstantTime) {
    return new JavaExecuteClusteringCommitActionExecutor<>(context, config, this, clusteringInstantTime).execute();
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
                                                     boolean skipTimelinePublish, boolean shouldRollbackUsingMarkers, boolean isRestore) {
    return new BaseRollbackPlanActionExecutor(context, config, this, instantTime, instantToRollback, skipTimelinePublish,
        shouldRollbackUsingMarkers, isRestore).execute();
  }

  @Override
  public Option<HoodieCleanerPlan> createCleanerPlan(HoodieEngineContext context, Option<Map<String, String>> extraMetadata) {
    return new CleanPlanActionExecutor<>(context, config, this, extraMetadata).execute();
  }

  @Override
  public HoodieCleanMetadata clean(HoodieEngineContext context,
                                   String cleanInstantTime) {
    return new CleanActionExecutor(context, config, this, cleanInstantTime).execute();
  }

  @Override
  public HoodieRollbackMetadata rollback(HoodieEngineContext context,
                                         String rollbackInstantTime,
                                         HoodieInstant commitInstant,
                                         boolean deleteInstants,
                                         boolean skipLocking) {
    return new CopyOnWriteRollbackActionExecutor(
        context, config, this, rollbackInstantTime, commitInstant, deleteInstants, skipLocking).execute();
  }

  @Override
  public Option<HoodieIndexPlan> scheduleIndexing(HoodieEngineContext context, String indexInstantTime, List<MetadataPartitionType> partitionsToIndex, List<String> partitionPaths) {
    return new ScheduleIndexActionExecutor<>(context, config, this, indexInstantTime, partitionsToIndex, partitionPaths).execute();
  }

  @Override
  public Option<HoodieIndexCommitMetadata> index(HoodieEngineContext context, String indexInstantTime) {
    return new RunIndexActionExecutor<>(context, config, this, indexInstantTime).execute();
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
  public Option<HoodieRestorePlan> scheduleRestore(HoodieEngineContext context, String restoreInstantTimestamp, String savepointToRestoreTimestamp) {
    return new RestorePlanActionExecutor(context, config, this, restoreInstantTimestamp, savepointToRestoreTimestamp).execute();
  }

  @Override
  public HoodieRestoreMetadata restore(HoodieEngineContext context,
                                       String restoreInstantTimestamp,
                                       String savepointToRestoreTimestamp) {
    return new CopyOnWriteRestoreActionExecutor(
        context, config, this, restoreInstantTimestamp, savepointToRestoreTimestamp).execute();
  }

  @Override
  public Iterator<List<WriteStatus>> handleUpdate(
      String instantTime, String partitionPath, String fileId,
      Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile oldDataFile)
      throws IOException {
    // these are updates
    HoodieMergeHandle mergeHandle = getUpdateHandle(instantTime, partitionPath, fileId, keyToNewRecords, oldDataFile);
    return IOUtils.runMerge(mergeHandle, instantTime, fileId);
  }

  protected HoodieMergeHandle getUpdateHandle(String instantTime, String partitionPath, String fileId,
                                              Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile dataFileToBeMerged) {
    Option<BaseKeyGenerator> keyGeneratorOpt = Option.empty();
    if (!config.populateMetaFields()) {
      try {
        keyGeneratorOpt = Option.of((BaseKeyGenerator) HoodieAvroKeyGeneratorFactory.createKeyGenerator(config.getProps()));
      } catch (IOException e) {
        throw new HoodieIOException("Only BaseKeyGenerator (or any key generator that extends from BaseKeyGenerator) are supported when meta "
            + "columns are disabled. Please choose the right key generator if you wish to disable meta fields.", e);
      }
    }
    return HoodieMergeHandleFactory.create(config, instantTime, this, keyToNewRecords, partitionPath, fileId,
        dataFileToBeMerged, taskContextSupplier, keyGeneratorOpt);
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsert(
      String instantTime, String partitionPath, String fileId,
      Map<String, HoodieRecord<?>> recordMap) {
    HoodieCreateHandle<?, ?, ?, ?> createHandle =
        new HoodieCreateHandle(config, instantTime, this, partitionPath, fileId, recordMap, taskContextSupplier);
    createHandle.write();
    return Collections.singletonList(createHandle.close()).iterator();
  }
}
