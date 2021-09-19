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

import org.apache.hudi.SparkHoodieRDDData;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.bootstrap.BootstrapWriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.HoodieSortedMergeHandle;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.BootstrapCommitActionExecutor;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.hudi.table.action.bootstrap.SparkBootstrapCommitHelper;
import org.apache.hudi.table.action.bootstrap.SparkBootstrapHelper;
import org.apache.hudi.table.action.clean.SparkCleanActionExecutor;
import org.apache.hudi.table.action.clean.SparkCleanPlanActionExecutor;
import org.apache.hudi.table.action.cluster.ClusteringPlanActionExecutor;
import org.apache.hudi.table.action.cluster.SparkClusteringCommitHelper;
import org.apache.hudi.table.action.cluster.SparkClusteringHelper;
import org.apache.hudi.table.action.cluster.SparkClusteringPlanHelper;
import org.apache.hudi.table.action.commit.BulkInsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.BulkInsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.DeleteCommitActionExecutor;
import org.apache.hudi.table.action.commit.DeletePartitionCommitActionExecutor;
import org.apache.hudi.table.action.commit.ExecuteClusteringCommitActionExecutor;
import org.apache.hudi.table.action.commit.InsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.InsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkBulkInsertHelper;
import org.apache.hudi.table.action.commit.SparkCommitHelper;
import org.apache.hudi.table.action.commit.SparkDeleteHelper;
import org.apache.hudi.table.action.commit.SparkDeletePartitionHelper;
import org.apache.hudi.table.action.commit.SparkInsertOverwriteCommitHelper;
import org.apache.hudi.table.action.commit.SparkInsertOverwriteTableCommitHelper;
import org.apache.hudi.table.action.commit.SparkMergeHelper;
import org.apache.hudi.table.action.commit.SparkWriteHelper;
import org.apache.hudi.table.action.commit.UpsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.UpsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.restore.SparkCopyOnWriteRestoreActionExecutor;
import org.apache.hudi.table.action.rollback.BaseRollbackPlanActionExecutor;
import org.apache.hudi.table.action.rollback.CopyOnWriteRollbackActionExecutor;
import org.apache.hudi.table.action.savepoint.SavepointActionExecutor;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of a very heavily read-optimized Hoodie Table where, all data is stored in base files, with
 * zero read amplification.
 * <p>
 * INSERTS - Produce new files, block aligned to desired size (or) Merge with the smallest existing file, to expand it
 * <p>
 * UPDATES - Produce a new version of the file, just replacing the updated records with new values
 */
public class HoodieSparkCopyOnWriteTable<T extends HoodieRecordPayload<T>> extends HoodieSparkTable<T> {

  private static final Logger LOG = LogManager.getLogger(HoodieSparkCopyOnWriteTable.class);

  public HoodieSparkCopyOnWriteTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> upsert(HoodieEngineContext context, String instantTime, JavaRDD<HoodieRecord<T>> records) {
    return convertMetadata(new UpsertCommitActionExecutor<T>(
        context, config, this, instantTime, SparkHoodieRDDData.of(records),
        new SparkCommitHelper<>(context, config, this, instantTime, WriteOperationType.UPSERT),
        SparkWriteHelper.newInstance()).execute());
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> insert(HoodieEngineContext context, String instantTime, JavaRDD<HoodieRecord<T>> records) {
    return convertMetadata(new InsertCommitActionExecutor<T>(
        context, config, this, instantTime, WriteOperationType.INSERT, SparkHoodieRDDData.of(records),
        new SparkCommitHelper<>(context, config, this, instantTime, WriteOperationType.INSERT),
        SparkWriteHelper.newInstance()).execute());
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> bulkInsert(HoodieEngineContext context, String instantTime, JavaRDD<HoodieRecord<T>> records,
      Option<BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>>> userDefinedBulkInsertPartitioner) {
    return convertMetadata(new BulkInsertCommitActionExecutor<T>(
        context, config, this, instantTime, SparkHoodieRDDData.of(records),
        convertBulkInsertPartitioner(userDefinedBulkInsertPartitioner),
        new SparkCommitHelper<>(context, config, this, instantTime, WriteOperationType.BULK_INSERT_PREPPED),
        SparkBulkInsertHelper.newInstance()).execute());
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> delete(HoodieEngineContext context, String instantTime, JavaRDD<HoodieKey> keys) {
    return convertMetadata(new DeleteCommitActionExecutor<T>(
        context, config, this, instantTime, SparkHoodieRDDData.of(keys),
        new SparkCommitHelper<>(context, config, this, instantTime, WriteOperationType.DELETE),
        SparkDeleteHelper.newInstance()).execute());
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> deletePartitions(HoodieEngineContext context, String instantTime, List<String> partitions) {
    return convertMetadata(new DeletePartitionCommitActionExecutor<T>(
        context, config, this, instantTime, WriteOperationType.DELETE_PARTITION, partitions,
        new SparkInsertOverwriteCommitHelper<>(
            context, config, this, instantTime, WriteOperationType.DELETE_PARTITION),
        SparkDeletePartitionHelper.newInstance()).execute());
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> upsertPrepped(HoodieEngineContext context, String instantTime,
      JavaRDD<HoodieRecord<T>> preppedRecords) {
    return convertMetadata(new UpsertPreppedCommitActionExecutor<>(
        context, config, this, instantTime, SparkHoodieRDDData.of(preppedRecords),
        new SparkCommitHelper<>(
            context, config, this, instantTime, WriteOperationType.UPSERT_PREPPED)).execute());
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> insertPrepped(HoodieEngineContext context, String instantTime,
      JavaRDD<HoodieRecord<T>> preppedRecords) {
    return convertMetadata(new InsertPreppedCommitActionExecutor<T>(
        context, config, this, instantTime, SparkHoodieRDDData.of(preppedRecords),
        new SparkCommitHelper<>(
            context, config, this, instantTime, WriteOperationType.INSERT_PREPPED)).execute());
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> bulkInsertPrepped(HoodieEngineContext context, String instantTime,
      JavaRDD<HoodieRecord<T>> preppedRecords,  Option<BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>>> userDefinedBulkInsertPartitioner) {
    return convertMetadata(new BulkInsertPreppedCommitActionExecutor<T>(
        context, config, this, instantTime, SparkHoodieRDDData.of(preppedRecords),
        convertBulkInsertPartitioner(userDefinedBulkInsertPartitioner),
        new SparkCommitHelper<>(context, config, this, instantTime, WriteOperationType.BULK_INSERT_PREPPED),
        SparkBulkInsertHelper.newInstance()).execute());
  }

  @Override
  public HoodieWriteMetadata insertOverwrite(HoodieEngineContext context, String instantTime, JavaRDD<HoodieRecord<T>> records) {
    return convertMetadata(new InsertCommitActionExecutor<T>(
        context, config, this, instantTime, WriteOperationType.INSERT_OVERWRITE,
        SparkHoodieRDDData.of(records),
        new SparkInsertOverwriteCommitHelper<>(
            context, config, this, instantTime, WriteOperationType.INSERT_OVERWRITE),
        SparkWriteHelper.newInstance()).execute());
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> insertOverwriteTable(HoodieEngineContext context, String instantTime, JavaRDD<HoodieRecord<T>> records) {
    return convertMetadata(new InsertCommitActionExecutor<T>(
        context, config, this, instantTime, WriteOperationType.INSERT_OVERWRITE_TABLE,
        SparkHoodieRDDData.of(records),
        new SparkInsertOverwriteTableCommitHelper<>(
            context, config, this, instantTime, WriteOperationType.INSERT_OVERWRITE_TABLE),
        SparkWriteHelper.newInstance()).execute());
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("Compaction is not supported on a CopyOnWrite table");
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> compact(HoodieEngineContext context, String compactionInstantTime) {
    throw new HoodieNotSupportedException("Compaction is not supported on a CopyOnWrite table");
  }

  @Override
  public Option<HoodieClusteringPlan> scheduleClustering(HoodieEngineContext context,
                                                         String instantTime,
                                                         Option<Map<String, String>> extraMetadata) {
    return new ClusteringPlanActionExecutor<>(
        context, config, this, instantTime, extraMetadata, SparkClusteringPlanHelper.newInstance()).execute();
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> cluster(HoodieEngineContext context,
                                                           String clusteringInstantTime) {
    return convertMetadata(new ExecuteClusteringCommitActionExecutor<T>(
        context, config, this, clusteringInstantTime,
        new SparkClusteringCommitHelper<>((HoodieSparkEngineContext) context, config, this, clusteringInstantTime),
        SparkClusteringHelper.newInstance()).execute());
  }

  @Override
  public HoodieBootstrapWriteMetadata<JavaRDD<WriteStatus>> bootstrap(HoodieEngineContext context, Option<Map<String, String>> extraMetadata) {
    HoodieWriteConfig bootstrapConfig = new HoodieWriteConfig.Builder().withProps(config.getProps())
        .withAutoCommit(true).withWriteStatusClass(BootstrapWriteStatus.class)
        .withBulkInsertParallelism(config.getBootstrapParallelism())
        .build();
    return convertBootstrapMetadata(new BootstrapCommitActionExecutor<T>(
        context, bootstrapConfig, this, extraMetadata,
        new SparkBootstrapCommitHelper<>(context, bootstrapConfig, this, extraMetadata),
        SparkBootstrapHelper.newInstance()).execute());
  }

  @Override
  public void rollbackBootstrap(HoodieEngineContext context, String instantTime) {
    new SparkCopyOnWriteRestoreActionExecutor((HoodieSparkEngineContext) context, config, this, instantTime, HoodieTimeline.INIT_INSTANT_TS).execute();
  }

  @Override
  public Option<HoodieCleanerPlan> scheduleCleaning(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    return new SparkCleanPlanActionExecutor<>(context, config,this, instantTime, extraMetadata).execute();
  }

  @Override
  public Option<HoodieRollbackPlan> scheduleRollback(HoodieEngineContext context,
                                                              String instantTime,
                                                              HoodieInstant instantToRollback, boolean skipTimelinePublish) {
    return new BaseRollbackPlanActionExecutor<>(context, config, this, instantTime, instantToRollback, skipTimelinePublish).execute();
  }

  public Iterator<List<WriteStatus>> handleUpdate(String instantTime, String partitionPath, String fileId,
      Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile oldDataFile) throws IOException {
    // these are updates
    HoodieMergeHandle upsertHandle = getUpdateHandle(instantTime, partitionPath, fileId, keyToNewRecords, oldDataFile);
    return handleUpdateInternal(upsertHandle, instantTime, fileId);
  }

  protected Iterator<List<WriteStatus>> handleUpdateInternal(HoodieMergeHandle<?,?,?,?> upsertHandle, String instantTime,
      String fileId) throws IOException {
    if (upsertHandle.getOldFilePath() == null) {
      throw new HoodieUpsertException(
          "Error in finding the old file path at commit " + instantTime + " for fileId: " + fileId);
    } else {
      SparkMergeHelper.newInstance().runMerge(this, upsertHandle);
    }

    // TODO(vc): This needs to be revisited
    if (upsertHandle.getPartitionPath() == null) {
      LOG.info("Upsert Handle has partition path as null " + upsertHandle.getOldFilePath() + ", "
          + upsertHandle.writeStatuses());
    }

    return Collections.singletonList(upsertHandle.writeStatuses()).iterator();
  }

  protected HoodieMergeHandle getUpdateHandle(String instantTime, String partitionPath, String fileId,
      Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile dataFileToBeMerged) {
    Option<BaseKeyGenerator> keyGeneratorOpt = Option.empty();
    if (!config.populateMetaFields()) {
      try {
        keyGeneratorOpt = Option.of((BaseKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(new TypedProperties(config.getProps())));
      } catch (IOException e) {
        throw new HoodieIOException("Only BaseKeyGenerator (or any key generator that extends from BaseKeyGenerator) are supported when meta "
            + "columns are disabled. Please choose the right key generator if you wish to disable meta fields.", e);
      }
    }
    if (requireSortedRecords()) {
      return new HoodieSortedMergeHandle<>(config, instantTime, this, keyToNewRecords, partitionPath, fileId,
          dataFileToBeMerged, taskContextSupplier, keyGeneratorOpt);
    } else {
      return new HoodieMergeHandle(config, instantTime, this, keyToNewRecords, partitionPath, fileId,
          dataFileToBeMerged, taskContextSupplier, keyGeneratorOpt);
    }
  }

  public Iterator<List<WriteStatus>> handleInsert(String instantTime, String partitionPath, String fileId,
      Map<String, HoodieRecord<? extends HoodieRecordPayload>> recordMap) {
    HoodieCreateHandle<?,?,?,?> createHandle =
        new HoodieCreateHandle(config, instantTime, this, partitionPath, fileId, recordMap, taskContextSupplier);
    createHandle.write();
    return Collections.singletonList(createHandle.close()).iterator();
  }

  @Override
  public HoodieCleanMetadata clean(HoodieEngineContext context, String cleanInstantTime) {
    return new SparkCleanActionExecutor((HoodieSparkEngineContext)context, config, this, cleanInstantTime).execute();
  }

  @Override
  public HoodieRollbackMetadata rollback(HoodieEngineContext context, String rollbackInstantTime, HoodieInstant commitInstant, boolean deleteInstants) {
    return new CopyOnWriteRollbackActionExecutor((HoodieSparkEngineContext) context, config, this, rollbackInstantTime, commitInstant, deleteInstants).execute();
  }

  @Override
  public HoodieSavepointMetadata savepoint(HoodieEngineContext context, String instantToSavepoint, String user, String comment) {
    return new SavepointActionExecutor(context, config, this, instantToSavepoint, user, comment).execute();
  }

  @Override
  public HoodieRestoreMetadata restore(HoodieEngineContext context, String restoreInstantTime, String instantToRestore) {
    return new SparkCopyOnWriteRestoreActionExecutor((HoodieSparkEngineContext) context, config, this, restoreInstantTime, instantToRestore).execute();
  }

}
