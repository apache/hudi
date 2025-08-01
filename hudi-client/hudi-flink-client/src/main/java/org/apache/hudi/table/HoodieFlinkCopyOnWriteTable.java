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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieMergeHandleFactory;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.hudi.table.action.clean.CleanActionExecutor;
import org.apache.hudi.table.action.clean.CleanPlanActionExecutor;
import org.apache.hudi.table.action.cluster.ClusteringPlanActionExecutor;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.FlinkAutoCommitActionExecutor;
import org.apache.hudi.table.action.commit.FlinkBulkInsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.FlinkDeletePartitionCommitActionExecutor;
import org.apache.hudi.table.action.commit.FlinkDeletePreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.FlinkInsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.FlinkInsertOverwriteCommitActionExecutor;
import org.apache.hudi.table.action.commit.FlinkInsertOverwriteTableCommitActionExecutor;
import org.apache.hudi.table.action.commit.FlinkInsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.FlinkPartitionTTLActionExecutor;
import org.apache.hudi.table.action.commit.FlinkUpsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.FlinkUpsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.rollback.BaseRollbackPlanActionExecutor;
import org.apache.hudi.table.action.rollback.CopyOnWriteRollbackActionExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class HoodieFlinkCopyOnWriteTable<T>
    extends HoodieFlinkTable<T> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieFlinkCopyOnWriteTable.class);

  public HoodieFlinkCopyOnWriteTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  /**
   * Upsert a batch of RowData into Hoodie table at the supplied instantTime.
   *
   * @param context     HoodieEngineContext
   * @param writeHandle The write handle
   * @param bucketInfo  The bucket info for the flushing data bucket
   * @param instantTime Instant Time for the action
   * @param records     hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  @Override
  public HoodieWriteMetadata<List<WriteStatus>> upsert(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      BucketInfo bucketInfo,
      String instantTime,
      Iterator<HoodieRecord<T>> records) {
    return new FlinkUpsertCommitActionExecutor<>(context, writeHandle, bucketInfo, config, this, instantTime, records).execute();
  }

  /**
   * Insert a batch of new records into Hoodie table at the supplied instantTime.
   *
   * <p>Specifies the write handle explicitly in order to have fine-grained control with
   * the underneath file.
   *
   * @param context     HoodieEngineContext
   * @param writeHandle The write handle
   * @param bucketInfo  The bucket info for the flushing data bucket
   * @param instantTime Instant Time for the action
   * @param records     hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public HoodieWriteMetadata<List<WriteStatus>> insert(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      BucketInfo bucketInfo,
      String instantTime,
      Iterator<HoodieRecord<T>> records) {
    return new FlinkInsertCommitActionExecutor<>(context, writeHandle, bucketInfo, config, this, instantTime, records).execute();
  }

  /**
   * Delete the given prepared records from the Hoodie table, at the supplied instantTime.
   *
   * <p>This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * <p>Specifies the write handle explicitly in order to have fine-grained control with
   * the underneath file.
   *
   * @param context        {@link HoodieEngineContext}
   * @param writeHandle    The write handle
   * @param bucketInfo     The bucket info for the flushing data bucket
   * @param instantTime    Instant Time for the action
   * @param preppedRecords Hoodie records to delete
   * @return {@link HoodieWriteMetadata}
   */
  public HoodieWriteMetadata<List<WriteStatus>> deletePrepped(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      BucketInfo bucketInfo,
      String instantTime,
      List<HoodieRecord<T>> preppedRecords) {
    return new FlinkDeletePreppedCommitActionExecutor<>(context, writeHandle, bucketInfo, config, this, instantTime, preppedRecords).execute();
  }

  /**
   * Upserts the given prepared records into the Hoodie table, at the supplied instantTime.
   *
   * <p>This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * <p>Specifies the write handle explicitly in order to have fine-grained control with
   * the underneath file.
   *
   * @param context        HoodieEngineContext
   * @param writeHandle    The write handle
   * @param bucketInfo     The bucket info for the flushing data bucket
   * @param instantTime    Instant Time for the action
   * @param preppedRecords Hoodie records to upsert
   * @return HoodieWriteMetadata
   */
  public HoodieWriteMetadata<List<WriteStatus>> upsertPrepped(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      BucketInfo bucketInfo,
      String instantTime,
      List<HoodieRecord<T>> preppedRecords) {
    return new FlinkUpsertPreppedCommitActionExecutor<>(context, writeHandle, bucketInfo, config, this, instantTime, preppedRecords).execute();
  }

  /**
   * Inserts the given prepared records into the Hoodie table, at the supplied instantTime.
   *
   * <p>This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * <p>Specifies the write handle explicitly in order to have fine-grained control with
   * the underneath file.
   *
   * @param context        HoodieEngineContext
   * @param writeHandle    The write handle
   * @param bucketInfo     The bucket info for the flushing data bucket
   * @param instantTime    Instant Time for the action
   * @param preppedRecords Hoodie records to insert
   * @return HoodieWriteMetadata
   */
  public HoodieWriteMetadata<List<WriteStatus>> insertPrepped(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      BucketInfo bucketInfo,
      String instantTime,
      List<HoodieRecord<T>> preppedRecords) {
    return new FlinkInsertPreppedCommitActionExecutor<>(context, writeHandle, bucketInfo, config, this, instantTime, preppedRecords).execute();
  }

  /**
   * Bulk inserts the given prepared records into the Hoodie table, at the supplied instantTime.
   *
   * <p>This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * <p>Specifies the write handle explicitly in order to have fine-grained control with
   * the underneath file.
   *
   * @param context        HoodieEngineContext
   * @param writeHandle    The write handle
   * @param bucketInfo     The bucket info for the flushing data bucket
   * @param instantTime    Instant Time for the action
   * @param preppedRecords Hoodie records to bulk_insert
   * @return HoodieWriteMetadata
   */
  public HoodieWriteMetadata<List<WriteStatus>> bulkInsertPrepped(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      BucketInfo bucketInfo,
      String instantTime,
      List<HoodieRecord<T>> preppedRecords) {
    return new FlinkBulkInsertPreppedCommitActionExecutor<>(context, writeHandle, bucketInfo, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insertOverwrite(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      BucketInfo bucketInfo,
      String instantTime,
      Iterator<HoodieRecord<T>> records) {
    return new FlinkInsertOverwriteCommitActionExecutor(context, writeHandle, bucketInfo, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insertOverwriteTable(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      BucketInfo bucketInfo,
      String instantTime,
      Iterator<HoodieRecord<T>> records) {
    return new FlinkInsertOverwriteTableCommitActionExecutor(context, writeHandle, bucketInfo, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> upsert(HoodieEngineContext context, String instantTime, List<HoodieRecord<T>> records) {
    throw new HoodieNotSupportedException("This method should not be invoked");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insert(HoodieEngineContext context, String instantTime, List<HoodieRecord<T>> records) {
    throw new HoodieNotSupportedException("This method should not be invoked");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> bulkInsert(HoodieEngineContext context,
                                                           String instantTime,
                                                           List<HoodieRecord<T>> records,
                                                           Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    throw new HoodieNotSupportedException("BulkInsert is not supported yet");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> delete(HoodieEngineContext context, String instantTime, List<HoodieKey> keys) {
    throw new HoodieNotSupportedException("This method should not be invoked");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> deletePrepped(HoodieEngineContext context, String instantTime, List<HoodieRecord<T>> preppedRecords) {
    throw new HoodieNotSupportedException("This method should not be invoked");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> deletePartitions(HoodieEngineContext context, String instantTime, List<String> partitions) {
    return new FlinkAutoCommitActionExecutor(new FlinkDeletePartitionCommitActionExecutor<>(context, config, this, instantTime, partitions)).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> upsertPrepped(HoodieEngineContext context, String instantTime, List<HoodieRecord<T>> preppedRecords) {
    throw new HoodieNotSupportedException("This method should not be invoked");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insertPrepped(HoodieEngineContext context, String instantTime, List<HoodieRecord<T>> preppedRecords) {
    throw new HoodieNotSupportedException("This method should not be invoked");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> bulkInsertPrepped(HoodieEngineContext context,
                                                                  String instantTime,
                                                                  List<HoodieRecord<T>> preppedRecords,
                                                                  Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    throw new HoodieNotSupportedException("BulkInsertPrepped is not supported yet");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insertOverwrite(HoodieEngineContext context, String instantTime, List<HoodieRecord<T>> records) {
    throw new HoodieNotSupportedException("This method should not be invoked");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> insertOverwriteTable(HoodieEngineContext context, String instantTime, List<HoodieRecord<T>> records) {
    throw new HoodieNotSupportedException("This method should not be invoked");
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("Compaction is not supported on a CopyOnWrite table");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> compact(
      HoodieEngineContext context, String compactionInstantTime) {
    throw new HoodieNotSupportedException("Compaction is not supported on a CopyOnWrite table");
  }

  @Override
  public Option<HoodieClusteringPlan> scheduleClustering(final HoodieEngineContext context, final String instantTime, final Option<Map<String, String>> extraMetadata) {
    return new ClusteringPlanActionExecutor<>(context, config, this, instantTime, extraMetadata).execute();
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> cluster(final HoodieEngineContext context, final String clusteringInstantTime) {
    throw new HoodieNotSupportedException("Clustering is not supported on a Flink CopyOnWrite table");
  }

  @Override
  public HoodieBootstrapWriteMetadata<List<WriteStatus>> bootstrap(HoodieEngineContext context, Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("Bootstrap is not supported yet");
  }

  @Override
  public void rollbackBootstrap(HoodieEngineContext context, String instantTime) {
    throw new HoodieNotSupportedException("Bootstrap is not supported yet");
  }

  /**
   * @param context       HoodieEngineContext
   * @param extraMetadata additional metadata to write into plan
   * @return
   */
  @Override
  public Option<HoodieCleanerPlan> createCleanerPlan(HoodieEngineContext context, Option<Map<String, String>> extraMetadata) {
    return new CleanPlanActionExecutor(context, config, this, extraMetadata).execute();
  }

  @Override
  public Option<HoodieRollbackPlan> scheduleRollback(HoodieEngineContext context, String instantTime, HoodieInstant instantToRollback,
                                                     boolean skipTimelinePublish, boolean shouldRollbackUsingMarkers, boolean isRestore) {
    return new BaseRollbackPlanActionExecutor(context, config, this, instantTime, instantToRollback, skipTimelinePublish,
        shouldRollbackUsingMarkers, isRestore).execute();
  }

  @Override
  public HoodieCleanMetadata clean(HoodieEngineContext context, String cleanInstantTime) {
    return new CleanActionExecutor(context, config, this, cleanInstantTime).execute();
  }

  @Override
  public HoodieRollbackMetadata rollback(HoodieEngineContext context, String rollbackInstantTime, HoodieInstant commitInstant,
                                         boolean deleteInstants, boolean skipLocking) {
    return new CopyOnWriteRollbackActionExecutor(context, config, this, rollbackInstantTime, commitInstant, deleteInstants, skipLocking).execute();
  }

  @Override
  public Option<HoodieIndexPlan> scheduleIndexing(HoodieEngineContext context, String indexInstantTime, List<MetadataPartitionType> partitionsToIndex, List<String> partitionPaths) {
    throw new HoodieNotSupportedException("Metadata indexing is not supported for a Flink table yet.");
  }

  @Override
  public Option<HoodieIndexCommitMetadata> index(HoodieEngineContext context, String indexInstantTime) {
    throw new HoodieNotSupportedException("Metadata indexing is not supported for a Flink table yet.");
  }

  @Override
  public HoodieSavepointMetadata savepoint(HoodieEngineContext context, String instantToSavepoint, String user, String comment) {
    throw new HoodieNotSupportedException("Savepoint is not supported yet");
  }

  @Override
  public Option<HoodieRestorePlan> scheduleRestore(HoodieEngineContext context, String restoreInstantTimestamp, String savepointToRestoreTimestamp) {
    throw new HoodieNotSupportedException("Restore is not supported yet");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> managePartitionTTL(HoodieEngineContext context, String instantTime) {
    return new FlinkPartitionTTLActionExecutor(context, config, this, instantTime).execute();
  }

  @Override
  public HoodieRestoreMetadata restore(HoodieEngineContext context, String restoreInstantTimestamp, String savepointToRestoreTimestamp) {
    throw new HoodieNotSupportedException("Savepoint and restore is not supported yet");
  }

  // -------------------------------------------------------------------------
  //  Used for compaction
  // -------------------------------------------------------------------------

  @Override
  public Iterator<List<WriteStatus>> handleUpdate(
      String instantTime, String partitionPath, String fileId,
      Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile oldDataFile) throws IOException {
    // always using avro record merger for legacy compaction since log scanner do not support rowdata reading yet.
    config.setRecordMergerClass(HoodieAvroRecordMerger.class.getName());
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
