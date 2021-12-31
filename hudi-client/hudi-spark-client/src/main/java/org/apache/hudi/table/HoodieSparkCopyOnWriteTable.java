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

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.columnstats.ColumnStatsIndexHelper;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.HoodieSortedMergeHandle;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.hudi.table.action.bootstrap.SparkBootstrapCommitActionExecutor;
import org.apache.hudi.table.action.clean.CleanActionExecutor;
import org.apache.hudi.table.action.clean.CleanPlanActionExecutor;
import org.apache.hudi.table.action.cluster.SparkClusteringPlanActionExecutor;
import org.apache.hudi.table.action.cluster.SparkExecuteClusteringCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkBulkInsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkBulkInsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkDeleteCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkDeletePartitionCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkInsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkInsertOverwriteCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkInsertOverwriteTableCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkInsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkMergeHelper;
import org.apache.hudi.table.action.commit.SparkUpsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkUpsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.restore.CopyOnWriteRestoreActionExecutor;
import org.apache.hudi.table.action.rollback.BaseRollbackPlanActionExecutor;
import org.apache.hudi.table.action.rollback.CopyOnWriteRollbackActionExecutor;
import org.apache.hudi.table.action.savepoint.SavepointActionExecutor;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of a very heavily read-optimized Hoodie Table where, all data is stored in base files, with
 * zero read amplification.
 * <p>
 * INSERTS - Produce new files, block aligned to desired size (or) Merge with the smallest existing file, to expand it
 * <p>
 * UPDATES - Produce a new version of the file, just replacing the updated records with new values
 */
public class HoodieSparkCopyOnWriteTable<T extends HoodieRecordPayload>
    extends HoodieSparkTable<T> implements HoodieCompactionHandler<T> {

  private static final Logger LOG = LogManager.getLogger(HoodieSparkCopyOnWriteTable.class);

  public HoodieSparkCopyOnWriteTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  @Override
  public boolean isTableServiceAction(String actionType) {
    return !actionType.equals(HoodieTimeline.COMMIT_ACTION);
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> upsert(HoodieEngineContext context, String instantTime, JavaRDD<HoodieRecord<T>> records) {
    return new SparkUpsertCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> insert(HoodieEngineContext context, String instantTime, JavaRDD<HoodieRecord<T>> records) {
    return new SparkInsertCommitActionExecutor<>((HoodieSparkEngineContext)context, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> bulkInsert(HoodieEngineContext context, String instantTime, JavaRDD<HoodieRecord<T>> records,
      Option<BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>>> userDefinedBulkInsertPartitioner) {
    return new SparkBulkInsertCommitActionExecutor((HoodieSparkEngineContext) context, config,
        this, instantTime, records, userDefinedBulkInsertPartitioner).execute();
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> delete(HoodieEngineContext context, String instantTime, JavaRDD<HoodieKey> keys) {
    return new SparkDeleteCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, keys).execute();
  }

  @Override
  public HoodieWriteMetadata deletePartitions(HoodieEngineContext context, String instantTime, List<String> partitions) {
    return new SparkDeletePartitionCommitActionExecutor(context, config, this, instantTime, partitions).execute();
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> upsertPrepped(HoodieEngineContext context, String instantTime,
      JavaRDD<HoodieRecord<T>> preppedRecords) {
    return new SparkUpsertPreppedCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> insertPrepped(HoodieEngineContext context, String instantTime,
      JavaRDD<HoodieRecord<T>> preppedRecords) {
    return new SparkInsertPreppedCommitActionExecutor<>((HoodieSparkEngineContext) context, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> bulkInsertPrepped(HoodieEngineContext context, String instantTime,
      JavaRDD<HoodieRecord<T>> preppedRecords,  Option<BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>>> userDefinedBulkInsertPartitioner) {
    return new SparkBulkInsertPreppedCommitActionExecutor((HoodieSparkEngineContext) context, config,
        this, instantTime, preppedRecords, userDefinedBulkInsertPartitioner).execute();
  }

  @Override
  public HoodieWriteMetadata insertOverwrite(HoodieEngineContext context, String instantTime, JavaRDD<HoodieRecord<T>> records) {
    return new SparkInsertOverwriteCommitActionExecutor(context, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> insertOverwriteTable(HoodieEngineContext context, String instantTime, JavaRDD<HoodieRecord<T>> records) {
    return new SparkInsertOverwriteTableCommitActionExecutor(context, config, this, instantTime, records).execute();
  }

  @Override
  public void updateMetadataIndexes(@Nonnull HoodieEngineContext context, @Nonnull List<HoodieWriteStat> stats, @Nonnull String instantTime) throws Exception {
    updateColumnsStatsIndex(context, stats, instantTime);
  }

  private void updateColumnsStatsIndex(
      @Nonnull HoodieEngineContext context,
      @Nonnull List<HoodieWriteStat> updatedFilesStats,
      @Nonnull String instantTime
  ) throws Exception {
    String sortColsList = config.getClusteringSortColumns();
    String basePath = metaClient.getBasePath();
    String indexPath = metaClient.getColumnStatsIndexPath();

    List<String> completedCommits =
        metaClient.getCommitsTimeline()
            .filterCompletedInstants()
            .getInstants()
            .map(HoodieInstant::getTimestamp)
            .collect(Collectors.toList());

    List<String> touchedFiles =
        updatedFilesStats.stream()
            .map(s -> new Path(basePath, s.getPath()).toString())
            .collect(Collectors.toList());

    if (touchedFiles.isEmpty() || StringUtils.isNullOrEmpty(sortColsList) || StringUtils.isNullOrEmpty(indexPath)) {
      return;
    }

    LOG.info(String.format("Updating column-statistics index table (%s)", indexPath));

    List<String> sortCols = Arrays.stream(sortColsList.split(","))
        .map(String::trim)
        .collect(Collectors.toList());

    HoodieSparkEngineContext sparkEngineContext = (HoodieSparkEngineContext)context;

    // Fetch table schema to appropriately construct col-stats index schema
    Schema tableWriteSchema =
        HoodieAvroUtils.createHoodieWriteSchema(
            new TableSchemaResolver(metaClient).getTableAvroSchemaWithoutMetadataFields()
        );

    ColumnStatsIndexHelper.updateColumnStatsIndexFor(
        sparkEngineContext.getSqlContext().sparkSession(),
        AvroConversionUtils.convertAvroSchemaToStructType(tableWriteSchema),
        touchedFiles,
        sortCols,
        indexPath,
        instantTime,
        completedCommits
    );

    LOG.info(String.format("Successfully updated column-statistics index at instant (%s)", instantTime));
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("Compaction is not supported on a CopyOnWrite table");
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> compact(
      HoodieEngineContext context, String compactionInstantTime) {
    throw new HoodieNotSupportedException("Compaction is not supported on a CopyOnWrite table");
  }

  @Override
  public Option<HoodieClusteringPlan> scheduleClustering(HoodieEngineContext context,
                                                         String instantTime,
                                                         Option<Map<String, String>> extraMetadata) {
    return new SparkClusteringPlanActionExecutor<>(context, config,this, instantTime, extraMetadata).execute();
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> cluster(HoodieEngineContext context,
                                                           String clusteringInstantTime) {
    return new SparkExecuteClusteringCommitActionExecutor<>(context, config, this, clusteringInstantTime).execute();
  }

  @Override
  public HoodieBootstrapWriteMetadata<JavaRDD<WriteStatus>> bootstrap(HoodieEngineContext context, Option<Map<String, String>> extraMetadata) {
    return new SparkBootstrapCommitActionExecutor((HoodieSparkEngineContext) context, config, this, extraMetadata).execute();
  }

  @Override
  public void rollbackBootstrap(HoodieEngineContext context, String instantTime) {
    new CopyOnWriteRestoreActionExecutor(context, config, this, instantTime, HoodieTimeline.INIT_INSTANT_TS).execute();
  }

  @Override
  public Option<HoodieCleanerPlan> scheduleCleaning(HoodieEngineContext context, String instantTime, Option<Map<String, String>> extraMetadata) {
    return new CleanPlanActionExecutor<>(context, config, this, instantTime, extraMetadata).execute();
  }

  @Override
  public Option<HoodieRollbackPlan> scheduleRollback(HoodieEngineContext context,
                                                     String instantTime,
                                                     HoodieInstant instantToRollback, boolean skipTimelinePublish, boolean shouldRollbackUsingMarkers) {
    return new BaseRollbackPlanActionExecutor<>(context, config, this, instantTime, instantToRollback, skipTimelinePublish,
        shouldRollbackUsingMarkers).execute();
  }

  @Override
  public Iterator<List<WriteStatus>> handleUpdate(
      String instantTime, String partitionPath, String fileId,
      Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile oldDataFile) throws IOException {
    // these are updates
    HoodieMergeHandle upsertHandle = getUpdateHandle(instantTime, partitionPath, fileId, keyToNewRecords, oldDataFile);
    return handleUpdateInternal(upsertHandle, instantTime, fileId);
  }

  protected Iterator<List<WriteStatus>> handleUpdateInternal(HoodieMergeHandle<?, ?, ?, ?> upsertHandle, String instantTime,
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

  @Override
  public Iterator<List<WriteStatus>> handleInsert(
      String instantTime, String partitionPath, String fileId,
      Map<String, HoodieRecord<? extends HoodieRecordPayload>> recordMap) {
    HoodieCreateHandle<?, ?, ?, ?> createHandle =
        new HoodieCreateHandle(config, instantTime, this, partitionPath, fileId, recordMap, taskContextSupplier);
    createHandle.write();
    return Collections.singletonList(createHandle.close()).iterator();
  }

  @Override
  public HoodieCleanMetadata clean(HoodieEngineContext context, String cleanInstantTime, boolean skipLocking) {
    return new CleanActionExecutor(context, config, this, cleanInstantTime, skipLocking).execute();
  }

  @Override
  public HoodieRollbackMetadata rollback(HoodieEngineContext context, String rollbackInstantTime, HoodieInstant commitInstant,
                                         boolean deleteInstants, boolean skipLocking) {
    return new CopyOnWriteRollbackActionExecutor((HoodieSparkEngineContext) context, config, this, rollbackInstantTime, commitInstant,
        deleteInstants, skipLocking).execute();
  }

  @Override
  public HoodieSavepointMetadata savepoint(HoodieEngineContext context, String instantToSavepoint, String user, String comment) {
    return new SavepointActionExecutor(context, config, this, instantToSavepoint, user, comment).execute();
  }

  @Override
  public HoodieRestoreMetadata restore(HoodieEngineContext context, String restoreInstantTime, String instantToRestore) {
    return new CopyOnWriteRestoreActionExecutor(context, config, this, restoreInstantTime, instantToRestore).execute();
  }

}
