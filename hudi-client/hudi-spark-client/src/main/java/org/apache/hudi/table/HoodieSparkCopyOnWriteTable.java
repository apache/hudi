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
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.utils.SparkPartitionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.HoodieMergeHandleFactory;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.hudi.table.action.bootstrap.SparkBootstrapCommitActionExecutor;
import org.apache.hudi.table.action.clean.CleanActionExecutor;
import org.apache.hudi.table.action.clean.CleanPlanActionExecutor;
import org.apache.hudi.table.action.cluster.ClusteringPlanActionExecutor;
import org.apache.hudi.table.action.cluster.SparkExecuteClusteringCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkBulkInsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkBulkInsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkDeleteCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkDeletePartitionCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkDeletePreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkInsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkInsertOverwriteCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkInsertOverwriteTableCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkInsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkPartitionTTLActionExecutor;
import org.apache.hudi.table.action.commit.SparkUpsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkUpsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.index.RunIndexActionExecutor;
import org.apache.hudi.table.action.index.ScheduleIndexActionExecutor;
import org.apache.hudi.table.action.restore.CopyOnWriteRestoreActionExecutor;
import org.apache.hudi.table.action.rollback.BaseRollbackPlanActionExecutor;
import org.apache.hudi.table.action.rollback.CopyOnWriteRollbackActionExecutor;
import org.apache.hudi.table.action.rollback.RestorePlanActionExecutor;
import org.apache.hudi.table.action.savepoint.SavepointActionExecutor;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.deleteMetadataTable;

/**
 * Implementation of a very heavily read-optimized Hoodie Table where, all data is stored in base files, with
 * zero read amplification.
 * <p>
 * INSERTS - Produce new files, block aligned to desired size (or) Merge with the smallest existing file, to expand it
 * <p>
 * UPDATES - Produce a new version of the file, just replacing the updated records with new values
 */
public class HoodieSparkCopyOnWriteTable<T>
    extends HoodieSparkTable<T> implements HoodieCompactionHandler<T> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkCopyOnWriteTable.class);

  public HoodieSparkCopyOnWriteTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> upsert(HoodieEngineContext context, String instantTime, HoodieData<HoodieRecord<T>> records) {
    return new SparkUpsertCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> insert(HoodieEngineContext context, String instantTime, HoodieData<HoodieRecord<T>> records) {
    return new SparkInsertCommitActionExecutor<>((HoodieSparkEngineContext)context, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> bulkInsert(HoodieEngineContext context, String instantTime, HoodieData<HoodieRecord<T>> records,
      Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    return new SparkBulkInsertCommitActionExecutor<>((HoodieSparkEngineContext) context, config,
        this, instantTime, records, userDefinedBulkInsertPartitioner).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> delete(HoodieEngineContext context, String instantTime, HoodieData<HoodieKey> keys) {
    return new SparkDeleteCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, keys).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> deletePrepped(HoodieEngineContext context, String instantTime, HoodieData<HoodieRecord<T>> preppedRecords) {
    return new SparkDeletePreppedCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> deletePartitions(HoodieEngineContext context, String instantTime, List<String> partitions) {
    return new SparkDeletePartitionCommitActionExecutor<>(context, config, this, instantTime, partitions).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> upsertPrepped(HoodieEngineContext context, String instantTime,
      HoodieData<HoodieRecord<T>> preppedRecords) {
    return new SparkUpsertPreppedCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> insertPrepped(HoodieEngineContext context, String instantTime,
      HoodieData<HoodieRecord<T>> preppedRecords) {
    return new SparkInsertPreppedCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> bulkInsertPrepped(HoodieEngineContext context, String instantTime,
      HoodieData<HoodieRecord<T>> preppedRecords,  Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    return new SparkBulkInsertPreppedCommitActionExecutor((HoodieSparkEngineContext) context, config,
        this, instantTime, preppedRecords, userDefinedBulkInsertPartitioner).execute();
  }

  @Override
  public HoodieWriteMetadata insertOverwrite(HoodieEngineContext context, String instantTime, HoodieData<HoodieRecord<T>> records) {
    return new SparkInsertOverwriteCommitActionExecutor(context, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> insertOverwriteTable(HoodieEngineContext context, String instantTime, HoodieData<HoodieRecord<T>> records) {
    return new SparkInsertOverwriteTableCommitActionExecutor(context, config, this, instantTime, records).execute();
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("Compaction is not supported on a CopyOnWrite table");
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> compact(
      HoodieEngineContext context, String compactionInstantTime) {
    throw new HoodieNotSupportedException("Compaction is not supported on a CopyOnWrite table");
  }

  @Override
  public Option<HoodieClusteringPlan> scheduleClustering(HoodieEngineContext context,
                                                         String instantTime,
                                                         Option<Map<String, String>> extraMetadata) {
    return new ClusteringPlanActionExecutor<>(context, config,this, instantTime, extraMetadata).execute();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> cluster(HoodieEngineContext context,
                                                           String clusteringInstantTime) {
    return new SparkExecuteClusteringCommitActionExecutor<>(context, config, this, clusteringInstantTime).execute();
  }

  @Override
  public HoodieBootstrapWriteMetadata<HoodieData<WriteStatus>> bootstrap(HoodieEngineContext context, Option<Map<String, String>> extraMetadata) {
    return new SparkBootstrapCommitActionExecutor((HoodieSparkEngineContext) context, config, this, extraMetadata).execute();
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
    new CopyOnWriteRestoreActionExecutor<>(context, config, this, instantTime, HoodieTimeline.INIT_INSTANT_TS).execute();
  }

  @Override
  public Option<HoodieCleanerPlan> createCleanerPlan(HoodieEngineContext context, Option<Map<String, String>> extraMetadata) {
    return new CleanPlanActionExecutor<>(context, config, this, extraMetadata).execute();
  }

  @Override
  public Option<HoodieRollbackPlan> scheduleRollback(HoodieEngineContext context,
                                                     String instantTime,
                                                     HoodieInstant instantToRollback, boolean skipTimelinePublish, boolean shouldRollbackUsingMarkers,
                                                     boolean isRestore) {
    return new BaseRollbackPlanActionExecutor<>(context, config, this, instantTime, instantToRollback, skipTimelinePublish,
        shouldRollbackUsingMarkers, isRestore).execute();
  }

  /**
   * Delete expired partition by config
   * @param context HoodieEngineContext
   * @param instantTime Instant Time for the action
   * @return HoodieWriteMetadata
   */
  public HoodieWriteMetadata<HoodieData<WriteStatus>> managePartitionTTL(HoodieEngineContext context, String instantTime) {
    return new SparkPartitionTTLActionExecutor<>(context, config, this, instantTime).execute();
  }

  @Override
  public Iterator<List<WriteStatus>> handleUpdate(
      String instantTime, String partitionPath, String fileId,
      Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile oldDataFile) throws IOException {
    // these are updates
    HoodieMergeHandle mergeHandle = getUpdateHandle(instantTime, partitionPath, fileId, keyToNewRecords, oldDataFile);
    return IOUtils.runMerge(mergeHandle, instantTime, fileId);
  }

  protected HoodieMergeHandle getUpdateHandle(String instantTime, String partitionPath, String fileId,
                                              Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile dataFileToBeMerged) {
    Option<BaseKeyGenerator> keyGeneratorOpt = Option.empty();
    if (!config.populateMetaFields()) {
      try {
        keyGeneratorOpt = Option.of((BaseKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(TypedProperties.copy(config.getProps())));
      } catch (Exception e) {
        throw new HoodieException("Only BaseKeyGenerator (or any key generator that extends from BaseKeyGenerator) are supported when meta "
            + "columns are disabled. Please choose the right key generator if you wish to disable meta fields.", e);
      }
    }
    HoodieMergeHandle mergeHandle = HoodieMergeHandleFactory.create(config, instantTime, this, keyToNewRecords, partitionPath, fileId,
        dataFileToBeMerged, taskContextSupplier, keyGeneratorOpt);
    if (mergeHandle.getOldFilePath() != null && mergeHandle.baseFileForMerge().getBootstrapBaseFile().isPresent()) {
      Option<String[]> partitionFields = getMetaClient().getTableConfig().getPartitionFields();
      Object[] partitionValues = SparkPartitionUtils.getPartitionFieldVals(partitionFields, mergeHandle.getPartitionPath(),
          getMetaClient().getTableConfig().getBootstrapBasePath().get(),
          mergeHandle.getWriterSchema(), (Configuration) getStorageConf().unwrap());
      mergeHandle.setPartitionFields(partitionFields);
      mergeHandle.setPartitionValues(partitionValues);
    }
    return mergeHandle;
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

  @Override
  public HoodieCleanMetadata clean(HoodieEngineContext context, String cleanInstantTime) {
    return new CleanActionExecutor<>(context, config, this, cleanInstantTime).execute();
  }

  @Override
  public HoodieRollbackMetadata rollback(HoodieEngineContext context, String rollbackInstantTime, HoodieInstant commitInstant,
                                         boolean deleteInstants, boolean skipLocking) {
    return new CopyOnWriteRollbackActionExecutor<>(context, config, this, rollbackInstantTime, commitInstant,
        deleteInstants, skipLocking).execute();
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
  public HoodieSavepointMetadata savepoint(HoodieEngineContext context, String instantToSavepoint, String completionTime, String user, String comment) {
    return new SavepointActionExecutor<>(context, config, this, instantToSavepoint, completionTime, user, comment).execute();
  }

  @Override
  public HoodieRestoreMetadata restore(HoodieEngineContext context, String restoreInstantTimestamp, String savepointToRestoreTimestamp) {
    return new CopyOnWriteRestoreActionExecutor<>(context, config, this, restoreInstantTimestamp, savepointToRestoreTimestamp).execute();
  }

  @Override
  public Option<HoodieRestorePlan> scheduleRestore(HoodieEngineContext context, String restoreInstantTimestamp, String savepointToRestoreTimestamp) {
    return new RestorePlanActionExecutor<>(context, config, this, restoreInstantTimestamp, savepointToRestoreTimestamp).execute();
  }

}
