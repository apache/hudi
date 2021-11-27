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

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.fs.ConsistencyGuard.FileVisibility;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FailSafeConsistencyGuard;
import org.apache.hudi.common.fs.OptimisticConsistencyGuard;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Abstract implementation of a HoodieTable.
 *
 * @param <T> Sub type of HoodieRecordPayload
 * @param <I> Type of inputs
 * @param <K> Type of keys
 * @param <O> Type of outputs
 */
public abstract class HoodieTable<T extends HoodieRecordPayload, I, K, O> implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieTable.class);

  protected final HoodieWriteConfig config;
  protected final HoodieTableMetaClient metaClient;
  protected final HoodieIndex<T, ?, ?, ?> index;
  private SerializableConfiguration hadoopConfiguration;
  protected final TaskContextSupplier taskContextSupplier;
  private final HoodieTableMetadata metadata;

  private transient FileSystemViewManager viewManager;
  protected final transient HoodieEngineContext context;

  protected HoodieTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    this.config = config;
    this.hadoopConfiguration = context.getHadoopConf();
    this.context = context;

    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().fromProperties(config.getMetadataConfig().getProps())
        .build();
    this.metadata = HoodieTableMetadata.create(context, metadataConfig, config.getBasePath(),
        FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue());

    this.viewManager = FileSystemViewManager.createViewManager(context, config.getMetadataConfig(), config.getViewStorageConfig(), config.getCommonConfig(), () -> metadata);
    this.metaClient = metaClient;
    this.index = getIndex(config, context);
    this.taskContextSupplier = context.getTaskContextSupplier();
  }

  protected abstract HoodieIndex<T, ?, ?, ?> getIndex(HoodieWriteConfig config, HoodieEngineContext context);

  private synchronized FileSystemViewManager getViewManager() {
    if (null == viewManager) {
      viewManager = FileSystemViewManager.createViewManager(getContext(), config.getMetadataConfig(), config.getViewStorageConfig(), config.getCommonConfig(), () -> metadata);
    }
    return viewManager;
  }

  /**
   * Upsert a batch of new records into Hoodie table at the supplied instantTime.
   * @param context    HoodieEngineContext
   * @param instantTime Instant Time for the action
   * @param records  hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata<O> upsert(HoodieEngineContext context, String instantTime,
      I records);

  /**
   * Insert a batch of new records into Hoodie table at the supplied instantTime.
   * @param context    HoodieEngineContext
   * @param instantTime Instant Time for the action
   * @param records  hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata<O> insert(HoodieEngineContext context, String instantTime,
      I records);

  /**
   * Bulk Insert a batch of new records into Hoodie table at the supplied instantTime.
   * @param context    HoodieEngineContext
   * @param instantTime Instant Time for the action
   * @param records  hoodieRecords to upsert
   * @param bulkInsertPartitioner User Defined Partitioner
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata<O> bulkInsert(HoodieEngineContext context, String instantTime,
      I records, Option<BulkInsertPartitioner<I>> bulkInsertPartitioner);

  /**
   * Deletes a list of {@link HoodieKey}s from the Hoodie table, at the supplied instantTime {@link HoodieKey}s will be
   * de-duped and non existent keys will be removed before deleting.
   *
   * @param context    HoodieEngineContext
   * @param instantTime Instant Time for the action
   * @param keys   {@link List} of {@link HoodieKey}s to be deleted
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata<O> delete(HoodieEngineContext context, String instantTime, K keys);

  /**
   * Deletes all data of partitions.
   * @param context    HoodieEngineContext
   * @param instantTime Instant Time for the action
   * @param partitions   {@link List} of partition to be deleted
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata deletePartitions(HoodieEngineContext context, String instantTime, List<String> partitions);

  /**
   * Upserts the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   * @param context    HoodieEngineContext
   * @param instantTime Instant Time for the action
   * @param preppedRecords  hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata<O> upsertPrepped(HoodieEngineContext context, String instantTime,
      I preppedRecords);

  /**
   * Inserts the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   * @param context    HoodieEngineContext
   * @param instantTime Instant Time for the action
   * @param preppedRecords  hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata<O> insertPrepped(HoodieEngineContext context, String instantTime,
      I preppedRecords);

  /**
   * Bulk Insert the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   * @param context    HoodieEngineContext
   * @param instantTime Instant Time for the action
   * @param preppedRecords  hoodieRecords to upsert
   * @param bulkInsertPartitioner User Defined Partitioner
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata<O> bulkInsertPrepped(HoodieEngineContext context, String instantTime,
      I preppedRecords,  Option<BulkInsertPartitioner<I>> bulkInsertPartitioner);

  /**
   * Replaces all the existing records and inserts the specified new records into Hoodie table at the supplied instantTime,
   * for the partition paths contained in input records.
   *
   * @param context HoodieEngineContext
   * @param instantTime Instant time for the replace action
   * @param records input records
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata<O> insertOverwrite(HoodieEngineContext context, String instantTime, I records);

  /**
   * Delete all the existing records of the Hoodie table and inserts the specified new records into Hoodie table at the supplied instantTime,
   * for the partition paths contained in input records.
   *
   * @param context HoodieEngineContext
   * @param instantTime Instant time for the replace action
   * @param records input records
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata<O> insertOverwriteTable(HoodieEngineContext context, String instantTime, I records);

  /**
   * Updates Metadata Indexes (like Z-Index)
   * TODO rebase onto metadata table (post RFC-27)
   *
   * @param context instance of {@link HoodieEngineContext}
   * @param instantTime instant of the carried operation triggering the update
   */
  public abstract void updateMetadataIndexes(
      @Nonnull HoodieEngineContext context,
      @Nonnull List<HoodieWriteStat> stats,
      @Nonnull String instantTime
  ) throws Exception;

  public HoodieWriteConfig getConfig() {
    return config;
  }

  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }

  public Configuration getHadoopConf() {
    return metaClient.getHadoopConf();
  }

  /**
   * Get the view of the file system for this table.
   */
  public TableFileSystemView getFileSystemView() {
    return new HoodieTableFileSystemView(metaClient, getCompletedCommitsTimeline());
  }

  /**
   * Get the base file only view of the file system for this table.
   */
  public BaseFileOnlyView getBaseFileOnlyView() {
    return getViewManager().getFileSystemView(metaClient);
  }

  /**
   * Get the full view of the file system for this table.
   */
  public SliceView getSliceView() {
    return getViewManager().getFileSystemView(metaClient);
  }

  /**
   * Get complete view of the file system for this table with ability to force sync.
   */
  public SyncableFileSystemView getHoodieView() {
    return getViewManager().getFileSystemView(metaClient);
  }

  /**
   * Get only the completed (no-inflights) commit + deltacommit timeline.
   */
  public HoodieTimeline getCompletedCommitsTimeline() {
    return metaClient.getCommitsTimeline().filterCompletedInstants();
  }

  /**
   * Get only the completed (no-inflights) commit timeline.
   */
  public HoodieTimeline getCompletedCommitTimeline() {
    return metaClient.getCommitTimeline().filterCompletedInstants();
  }

  /**
   * Get only the inflights (no-completed) commit timeline.
   */
  public HoodieTimeline getPendingCommitTimeline() {
    return metaClient.getCommitsTimeline().filterPendingExcludingCompaction();
  }

  /**
   * Get only the completed (no-inflights) clean timeline.
   */
  public HoodieTimeline getCompletedCleanTimeline() {
    return getActiveTimeline().getCleanerTimeline().filterCompletedInstants();
  }

  /**
   * Get clean timeline.
   */
  public HoodieTimeline getCleanTimeline() {
    return getActiveTimeline().getCleanerTimeline();
  }

  /**
   * Get rollback timeline.
   */
  public HoodieTimeline getRollbackTimeline() {
    return getActiveTimeline().getRollbackTimeline();
  }

  /**
   * Get only the completed (no-inflights) savepoint timeline.
   */
  public HoodieTimeline getCompletedSavepointTimeline() {
    return getActiveTimeline().getSavePointTimeline().filterCompletedInstants();
  }

  /**
   * Get the list of savepoints in this table.
   */
  public List<String> getSavepoints() {
    return getCompletedSavepointTimeline().getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
  }

  public HoodieActiveTimeline getActiveTimeline() {
    return metaClient.getActiveTimeline();
  }

  /**
   * Return the index.
   */
  public HoodieIndex<T, ?, ?, ?> getIndex() {
    return index;
  }

  /**
   * Schedule compaction for the instant time.
   *
   * @param context HoodieEngineContext
   * @param instantTime Instant Time for scheduling compaction
   * @param extraMetadata additional metadata to write into plan
   * @return
   */
  public abstract Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context,
                                                                  String instantTime,
                                                                  Option<Map<String, String>> extraMetadata);

  /**
   * Run Compaction on the table. Compaction arranges the data so that it is optimized for data access.
   *
   * @param context               HoodieEngineContext
   * @param compactionInstantTime Instant Time
   */
  public abstract HoodieWriteMetadata<O> compact(HoodieEngineContext context,
                                                 String compactionInstantTime);

  /**
   * Schedule clustering for the instant time.
   *
   * @param context HoodieEngineContext
   * @param instantTime Instant Time for scheduling clustering
   * @param extraMetadata additional metadata to write into plan
   * @return HoodieClusteringPlan, if there is enough data for clustering.
   */
  public abstract Option<HoodieClusteringPlan> scheduleClustering(HoodieEngineContext context,
                                                                  String instantTime,
                                                                  Option<Map<String, String>> extraMetadata);

  /**
   * Execute Clustering on the table. Clustering re-arranges the data so that it is optimized for data access.
   *
   * @param context HoodieEngineContext
   * @param clusteringInstantTime Instant Time
   */
  public abstract HoodieWriteMetadata<O> cluster(HoodieEngineContext context, String clusteringInstantTime);

  /**
   * Perform metadata/full bootstrap of a Hudi table.
   * @param context HoodieEngineContext
   * @param extraMetadata Additional Metadata for storing in commit file.
   * @return HoodieBootstrapWriteMetadata
   */
  public abstract HoodieBootstrapWriteMetadata<O> bootstrap(HoodieEngineContext context, Option<Map<String, String>> extraMetadata);

  /**
   * Perform rollback of bootstrap of a Hudi table.
   * @param context HoodieEngineContext
   */
  public abstract void rollbackBootstrap(HoodieEngineContext context, String instantTime);


  /**
   * Schedule cleaning for the instant time.
   *
   * @param context HoodieEngineContext
   * @param instantTime Instant Time for scheduling cleaning
   * @param extraMetadata additional metadata to write into plan
   * @return HoodieCleanerPlan, if there is anything to clean.
   */
  public abstract Option<HoodieCleanerPlan> scheduleCleaning(HoodieEngineContext context,
                                                             String instantTime,
                                                             Option<Map<String, String>> extraMetadata);

  /**
   * Executes a new clean action.
   *
   * @return information on cleaned file slices
   */
  public abstract HoodieCleanMetadata clean(HoodieEngineContext context, String cleanInstantTime, boolean skipLocking);

  /**
   * Schedule rollback for the instant time.
   *
   * @param context HoodieEngineContext
   * @param instantTime Instant Time for scheduling rollback
   * @param instantToRollback instant to be rolled back
   * @param shouldRollbackUsingMarkers uses marker based rollback strategy when set to true. uses list based rollback when false.
   * @return HoodieRollbackPlan containing info on rollback.
   */
  public abstract Option<HoodieRollbackPlan> scheduleRollback(HoodieEngineContext context,
                                                              String instantTime,
                                                              HoodieInstant instantToRollback,
                                                              boolean skipTimelinePublish, boolean shouldRollbackUsingMarkers);
  
  /**
   * Rollback the (inflight/committed) record changes with the given commit time.
   * <pre>
   *   Three steps:
   *   (1) Atomically unpublish this commit
   *   (2) clean indexing data
   *   (3) clean new generated parquet files.
   *   (4) Finally delete .commit or .inflight file, if deleteInstants = true
   * </pre>
   */
  public abstract HoodieRollbackMetadata rollback(HoodieEngineContext context,
                                                  String rollbackInstantTime,
                                                  HoodieInstant commitInstant,
                                                  boolean deleteInstants,
                                                  boolean skipLocking);

  /**
   * Create a savepoint at the specified instant, so that the table can be restored
   * to this point-in-timeline later if needed.
   */
  public abstract HoodieSavepointMetadata savepoint(HoodieEngineContext context,
                                                    String instantToSavepoint,
                                                    String user,
                                                    String comment);

  /**
   * Restore the table to the given instant. Note that this is a admin table recovery operation
   * that would cause any running queries that are accessing file slices written after the instant to fail.
   */
  public abstract HoodieRestoreMetadata restore(HoodieEngineContext context,
                                                String restoreInstantTime,
                                                String instantToRestore);

  /**
   * Rollback failed compactions. Inflight rollbacks for compactions revert the .inflight file
   * to the .requested file.
   *
   * @param inflightInstant Inflight Compaction Instant
   */
  public void rollbackInflightCompaction(HoodieInstant inflightInstant) {
    String commitTime = HoodieActiveTimeline.createNewInstantTime();
    scheduleRollback(context, commitTime, inflightInstant, false, config.shouldRollbackUsingMarkers());
    rollback(context, commitTime, inflightInstant, false, false);
    getActiveTimeline().revertCompactionInflightToRequested(inflightInstant);
  }

  /**
   * Finalize the written data onto storage. Perform any final cleanups.
   *
   * @param context HoodieEngineContext
   * @param stats   List of HoodieWriteStats
   * @throws HoodieIOException if some paths can't be finalized on storage
   */
  public void finalizeWrite(HoodieEngineContext context, String instantTs, List<HoodieWriteStat> stats) throws HoodieIOException {
    reconcileAgainstMarkers(context, instantTs, stats, config.getConsistencyGuardConfig().isConsistencyCheckEnabled());
  }

  private void deleteInvalidFilesByPartitions(HoodieEngineContext context, Map<String, List<Pair<String, String>>> invalidFilesByPartition) {
    // Now delete partially written files
    context.setJobStatus(this.getClass().getSimpleName(), "Delete invalid files generated during the write operation");
    context.map(new ArrayList<>(invalidFilesByPartition.values()), partitionWithFileList -> {
      final FileSystem fileSystem = metaClient.getFs();
      LOG.info("Deleting invalid data files=" + partitionWithFileList);
      if (partitionWithFileList.isEmpty()) {
        return true;
      }
      // Delete
      partitionWithFileList.stream().map(Pair::getValue).forEach(file -> {
        try {
          fileSystem.delete(new Path(file), false);
        } catch (IOException e) {
          throw new HoodieIOException(e.getMessage(), e);
        }
      });

      return true;
    }, config.getFinalizeWriteParallelism());
  }

  /**
   * Returns the possible invalid data file name with given marker files.
   */
  protected Set<String> getInvalidDataPaths(WriteMarkers markers) throws IOException {
    return markers.createdAndMergedDataPaths(context, config.getFinalizeWriteParallelism());
  }

  /**
   * Reconciles WriteStats and marker files to detect and safely delete duplicate data files created because of Spark
   * retries.
   *
   * @param context HoodieEngineContext
   * @param instantTs Instant Timestamp
   * @param stats Hoodie Write Stat
   * @param consistencyCheckEnabled Consistency Check Enabled
   * @throws HoodieIOException
   */
  protected void reconcileAgainstMarkers(HoodieEngineContext context,
                                         String instantTs,
                                         List<HoodieWriteStat> stats,
                                         boolean consistencyCheckEnabled) throws HoodieIOException {
    try {
      // Reconcile marker and data files with WriteStats so that partially written data-files due to failed
      // (but succeeded on retry) tasks are removed.
      String basePath = getMetaClient().getBasePath();
      WriteMarkers markers = WriteMarkersFactory.get(config.getMarkersType(), this, instantTs);

      if (!markers.doesMarkerDirExist()) {
        // can happen if it was an empty write say.
        return;
      }

      // we are not including log appends here, since they are already fail-safe.
      Set<String> invalidDataPaths = getInvalidDataPaths(markers);
      Set<String> validDataPaths = stats.stream()
          .map(HoodieWriteStat::getPath)
          .filter(p -> p.endsWith(this.getBaseFileExtension()))
          .collect(Collectors.toSet());

      // Contains list of partially created files. These needs to be cleaned up.
      invalidDataPaths.removeAll(validDataPaths);

      if (!invalidDataPaths.isEmpty()) {
        LOG.info("Removing duplicate data files created due to spark retries before committing. Paths=" + invalidDataPaths);
        Map<String, List<Pair<String, String>>> invalidPathsByPartition = invalidDataPaths.stream()
            .map(dp -> Pair.of(new Path(basePath, dp).getParent().toString(), new Path(basePath, dp).toString()))
            .collect(Collectors.groupingBy(Pair::getKey));

        // Ensure all files in delete list is actually present. This is mandatory for an eventually consistent FS.
        // Otherwise, we may miss deleting such files. If files are not found even after retries, fail the commit
        if (consistencyCheckEnabled) {
          // This will either ensure all files to be deleted are present.
          waitForAllFiles(context, invalidPathsByPartition, FileVisibility.APPEAR);
        }

        // Now delete partially written files
        context.setJobStatus(this.getClass().getSimpleName(), "Delete all partially written files");
        deleteInvalidFilesByPartitions(context, invalidPathsByPartition);

        // Now ensure the deleted files disappear
        if (consistencyCheckEnabled) {
          // This will either ensure all files to be deleted are absent.
          waitForAllFiles(context, invalidPathsByPartition, FileVisibility.DISAPPEAR);
        }
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Ensures all files passed either appear or disappear.
   *
   * @param context HoodieEngineContext
   * @param groupByPartition Files grouped by partition
   * @param visibility Appear/Disappear
   */
  private void waitForAllFiles(HoodieEngineContext context, Map<String, List<Pair<String, String>>> groupByPartition, FileVisibility visibility) {
    // This will either ensure all files to be deleted are present.
    context.setJobStatus(this.getClass().getSimpleName(), "Wait for all files to appear/disappear");
    boolean checkPassed =
        context.map(new ArrayList<>(groupByPartition.entrySet()), partitionWithFileList -> waitForCondition(partitionWithFileList.getKey(),
            partitionWithFileList.getValue().stream(), visibility), config.getFinalizeWriteParallelism())
            .stream().allMatch(x -> x);
    if (!checkPassed) {
      throw new HoodieIOException("Consistency check failed to ensure all files " + visibility);
    }
  }

  private boolean waitForCondition(String partitionPath, Stream<Pair<String, String>> partitionFilePaths, FileVisibility visibility) {
    final FileSystem fileSystem = metaClient.getRawFs();
    List<String> fileList = partitionFilePaths.map(Pair::getValue).collect(Collectors.toList());
    try {
      getConsistencyGuard(fileSystem, config.getConsistencyGuardConfig()).waitTill(partitionPath, fileList, visibility);
    } catch (IOException | TimeoutException ioe) {
      LOG.error("Got exception while waiting for files to show up", ioe);
      return false;
    }
    return true;
  }

  /**
   * Instantiate {@link ConsistencyGuard} based on configs.
   * <p>
   * Default consistencyGuard class is {@link OptimisticConsistencyGuard}.
   */
  public static ConsistencyGuard getConsistencyGuard(FileSystem fs, ConsistencyGuardConfig consistencyGuardConfig) throws IOException {
    try {
      return consistencyGuardConfig.shouldEnableOptimisticConsistencyGuard()
          ? new OptimisticConsistencyGuard(fs, consistencyGuardConfig) : new FailSafeConsistencyGuard(fs, consistencyGuardConfig);
    } catch (Throwable e) {
      throw new IOException("Could not load ConsistencyGuard ", e);
    }
  }

  public TaskContextSupplier getTaskContextSupplier() {
    return taskContextSupplier;
  }

  /**
   * Ensure that the current writerSchema is compatible with the latest schema of this dataset.
   *
   * When inserting/updating data, we read records using the last used schema and convert them to the
   * GenericRecords with writerSchema. Hence, we need to ensure that this conversion can take place without errors.
   */
  private void validateSchema() throws HoodieUpsertException, HoodieInsertException {

    if (!config.getAvroSchemaValidate() || getActiveTimeline().getCommitsTimeline().filterCompletedInstants().empty()) {
      // Check not required
      return;
    }

    Schema tableSchema;
    Schema writerSchema;
    boolean isValid;
    try {
      TableSchemaResolver schemaUtil = new TableSchemaResolver(getMetaClient());
      writerSchema = HoodieAvroUtils.createHoodieWriteSchema(config.getSchema());
      tableSchema = HoodieAvroUtils.createHoodieWriteSchema(schemaUtil.getTableAvroSchemaWithoutMetadataFields());
      isValid = TableSchemaResolver.isSchemaCompatible(tableSchema, writerSchema);
    } catch (Exception e) {
      throw new HoodieException("Failed to read schema/check compatibility for base path " + metaClient.getBasePath(), e);
    }

    if (!isValid) {
      throw new HoodieException("Failed schema compatibility check for writerSchema :" + writerSchema
          + ", table schema :" + tableSchema + ", base path :" + metaClient.getBasePath());
    }
  }

  public void validateUpsertSchema() throws HoodieUpsertException {
    try {
      validateSchema();
    } catch (HoodieException e) {
      throw new HoodieUpsertException("Failed upsert schema compatibility check.", e);
    }
  }

  public void validateInsertSchema() throws HoodieInsertException {
    try {
      validateSchema();
    } catch (HoodieException e) {
      throw new HoodieInsertException("Failed insert schema compability check.", e);
    }
  }

  public HoodieFileFormat getBaseFileFormat() {
    return metaClient.getTableConfig().getBaseFileFormat();
  }

  public HoodieFileFormat getLogFileFormat() {
    return metaClient.getTableConfig().getLogFileFormat();
  }

  public HoodieLogBlockType getLogDataBlockFormat() {
    switch (getBaseFileFormat()) {
      case PARQUET:
      case ORC:
        return HoodieLogBlockType.AVRO_DATA_BLOCK;
      case HFILE:
        return HoodieLogBlockType.HFILE_DATA_BLOCK;
      default:
        throw new HoodieException("Base file format " + getBaseFileFormat()
            + " does not have associated log block format");
    }
  }

  public String getBaseFileExtension() {
    return getBaseFileFormat().getFileExtension();
  }

  public boolean requireSortedRecords() {
    return getBaseFileFormat() == HoodieFileFormat.HFILE;
  }

  public HoodieEngineContext getContext() {
    // This is to handle scenarios where this is called at the executor tasks which do not have access
    // to engine context, and it ends up being null (as its not serializable and marked transient here).
    return context == null ? new HoodieLocalEngineContext(hadoopConfiguration.get()) : context;
  }

  /**
   * Get Table metadata writer.
   *
   * @param triggeringInstantTimestamp - The instant that is triggering this metadata write
   * @return instance of {@link HoodieTableMetadataWriter
   */
  public final Option<HoodieTableMetadataWriter> getMetadataWriter(String triggeringInstantTimestamp) {
    return getMetadataWriter(triggeringInstantTimestamp, Option.empty());
  }

  /**
   * Check if action type is a table service.
   * @param actionType action type of interest.
   * @return true if action represents a table service. false otherwise.
   */
  public abstract boolean isTableServiceAction(String actionType);

  /**
   * Get Table metadata writer.
   * <p>
   * Note:
   * Get the metadata writer for the conf. If the metadata table doesn't exist,
   * this wil trigger the creation of the table and the initial bootstrapping.
   * Since this call is under the transaction lock, other concurrent writers
   * are blocked from doing the similar initial metadata table creation and
   * the bootstrapping.
   *
   * @param triggeringInstantTimestamp - The instant that is triggering this metadata write
   * @return instance of {@link HoodieTableMetadataWriter}
   */
  public <T extends SpecificRecordBase> Option<HoodieTableMetadataWriter> getMetadataWriter(String triggeringInstantTimestamp,
                                                                                            Option<T> actionMetadata) {
    // Each engine is expected to override this and
    // provide the actual metadata writer, if enabled.
    return Option.empty();
  }

}
