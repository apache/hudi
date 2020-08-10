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

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.fs.ConsistencyGuard.FileVisibility;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FailSafeConsistencyGuard;
import org.apache.hudi.common.fs.OptimisticConsistencyGuard;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewManager;
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
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
 */
public abstract class HoodieTable<T extends HoodieRecordPayload> implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieTable.class);

  protected final HoodieWriteConfig config;
  protected final HoodieTableMetaClient metaClient;
  protected final HoodieIndex<T> index;

  private SerializableConfiguration hadoopConfiguration;
  private transient FileSystemViewManager viewManager;

  protected final SparkTaskContextSupplier sparkTaskContextSupplier = new SparkTaskContextSupplier();

  protected HoodieTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
    this.config = config;
    this.hadoopConfiguration = new SerializableConfiguration(hadoopConf);
    this.viewManager = FileSystemViewManager.createViewManager(new SerializableConfiguration(hadoopConf),
        config.getViewStorageConfig());
    this.metaClient = metaClient;
    this.index = HoodieIndex.createIndex(config);
  }

  private synchronized FileSystemViewManager getViewManager() {
    if (null == viewManager) {
      viewManager = FileSystemViewManager.createViewManager(hadoopConfiguration, config.getViewStorageConfig());
    }
    return viewManager;
  }

  public static <T extends HoodieRecordPayload> HoodieTable<T> create(HoodieWriteConfig config, Configuration hadoopConf) {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(
        hadoopConf,
        config.getBasePath(),
        true,
        config.getConsistencyGuardConfig(),
        Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion()))
    );
    return HoodieTable.create(metaClient, config, hadoopConf);
  }

  public static <T extends HoodieRecordPayload> HoodieTable<T> create(HoodieTableMetaClient metaClient,
                                                                      HoodieWriteConfig config,
                                                                      Configuration hadoopConf) {
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        return new HoodieCopyOnWriteTable<>(config, hadoopConf, metaClient);
      case MERGE_ON_READ:
        return new HoodieMergeOnReadTable<>(config, hadoopConf, metaClient);
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
  }

  /**
   * Upsert a batch of new records into Hoodie table at the supplied instantTime.
   * @param jsc    Java Spark Context jsc
   * @param instantTime Instant Time for the action
   * @param records  JavaRDD of hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata upsert(JavaSparkContext jsc, String instantTime,
      JavaRDD<HoodieRecord<T>> records);

  /**
   * Insert a batch of new records into Hoodie table at the supplied instantTime.
   * @param jsc    Java Spark Context jsc
   * @param instantTime Instant Time for the action
   * @param records  JavaRDD of hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata insert(JavaSparkContext jsc, String instantTime,
      JavaRDD<HoodieRecord<T>> records);

  /**
   * Bulk Insert a batch of new records into Hoodie table at the supplied instantTime.
   * @param jsc    Java Spark Context jsc
   * @param instantTime Instant Time for the action
   * @param records  JavaRDD of hoodieRecords to upsert
   * @param bulkInsertPartitioner User Defined Partitioner
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata bulkInsert(JavaSparkContext jsc, String instantTime,
      JavaRDD<HoodieRecord<T>> records, Option<BulkInsertPartitioner> bulkInsertPartitioner);

  /**
   * Deletes a list of {@link HoodieKey}s from the Hoodie table, at the supplied instantTime {@link HoodieKey}s will be
   * de-duped and non existent keys will be removed before deleting.
   *
   * @param jsc    Java Spark Context jsc
   * @param instantTime Instant Time for the action
   * @param keys   {@link List} of {@link HoodieKey}s to be deleted
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata delete(JavaSparkContext jsc, String instantTime, JavaRDD<HoodieKey> keys);

  /**
   * Upserts the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   * @param jsc    Java Spark Context jsc
   * @param instantTime Instant Time for the action
   * @param preppedRecords  JavaRDD of hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata upsertPrepped(JavaSparkContext jsc, String instantTime,
      JavaRDD<HoodieRecord<T>> preppedRecords);

  /**
   * Inserts the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   * @param jsc    Java Spark Context jsc
   * @param instantTime Instant Time for the action
   * @param preppedRecords  JavaRDD of hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata insertPrepped(JavaSparkContext jsc, String instantTime,
      JavaRDD<HoodieRecord<T>> preppedRecords);

  /**
   * Bulk Insert the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   * @param jsc    Java Spark Context jsc
   * @param instantTime Instant Time for the action
   * @param preppedRecords  JavaRDD of hoodieRecords to upsert
   * @param bulkInsertPartitioner User Defined Partitioner
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata bulkInsertPrepped(JavaSparkContext jsc, String instantTime,
      JavaRDD<HoodieRecord<T>> preppedRecords,  Option<BulkInsertPartitioner> bulkInsertPartitioner);

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
  public HoodieIndex<T> getIndex() {
    return index;
  }

  /**
   * Schedule compaction for the instant time.
   *
   * @param jsc Spark Context
   * @param instantTime Instant Time for scheduling compaction
   * @param extraMetadata additional metadata to write into plan
   * @return
   */
  public abstract Option<HoodieCompactionPlan> scheduleCompaction(JavaSparkContext jsc,
                                                                  String instantTime,
                                                                  Option<Map<String, String>> extraMetadata);

  /**
   * Run Compaction on the table. Compaction arranges the data so that it is optimized for data access.
   *
   * @param jsc Spark Context
   * @param compactionInstantTime Instant Time
   */
  public abstract HoodieWriteMetadata compact(JavaSparkContext jsc,
                                              String compactionInstantTime);

  /**
   * Perform metadata/full bootstrap of a Hudi table.
   * @param jsc JavaSparkContext
   * @param extraMetadata Additional Metadata for storing in commit file.
   * @return HoodieBootstrapWriteMetadata
   */
  public abstract HoodieBootstrapWriteMetadata bootstrap(JavaSparkContext jsc, Option<Map<String, String>> extraMetadata);

  /**
   * Perform rollback of bootstrap of a Hudi table.
   * @param jsc JavaSparkContext
   */
  public abstract void rollbackBootstrap(JavaSparkContext jsc, String instantTime);

  /**
   * Executes a new clean action.
   *
   * @return information on cleaned file slices
   */
  public abstract HoodieCleanMetadata clean(JavaSparkContext jsc, String cleanInstantTime);

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
  public abstract HoodieRollbackMetadata rollback(JavaSparkContext jsc,
                                                  String rollbackInstantTime,
                                                  HoodieInstant commitInstant,
                                                  boolean deleteInstants);

  /**
   * Create a savepoint at the specified instant, so that the table can be restored
   * to this point-in-timeline later if needed.
   */
  public abstract HoodieSavepointMetadata savepoint(JavaSparkContext jsc,
                                                    String instantToSavepoint,
                                                    String user,
                                                    String comment);

  /**
   * Restore the table to the given instant. Note that this is a admin table recovery operation
   * that would cause any running queries that are accessing file slices written after the instant to fail.
   */
  public abstract HoodieRestoreMetadata restore(JavaSparkContext jsc,
                                                String restoreInstantTime,
                                                String instantToRestore);

  /**
   * Finalize the written data onto storage. Perform any final cleanups.
   *
   * @param jsc Spark Context
   * @param stats List of HoodieWriteStats
   * @throws HoodieIOException if some paths can't be finalized on storage
   */
  public void finalizeWrite(JavaSparkContext jsc, String instantTs, List<HoodieWriteStat> stats) throws HoodieIOException {
    reconcileAgainstMarkers(jsc, instantTs, stats, config.getConsistencyGuardConfig().isConsistencyCheckEnabled());
  }

  private void deleteInvalidFilesByPartitions(JavaSparkContext jsc, Map<String, List<Pair<String, String>>> invalidFilesByPartition) {
    // Now delete partially written files
    jsc.parallelize(new ArrayList<>(invalidFilesByPartition.values()), config.getFinalizeWriteParallelism())
        .map(partitionWithFileList -> {
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
        }).collect();
  }

  /**
   * Reconciles WriteStats and marker files to detect and safely delete duplicate data files created because of Spark
   * retries.
   *
   * @param jsc Spark Context
   * @param instantTs Instant Timestamp
   * @param stats Hoodie Write Stat
   * @param consistencyCheckEnabled Consistency Check Enabled
   * @throws HoodieIOException
   */
  protected void reconcileAgainstMarkers(JavaSparkContext jsc,
                                         String instantTs,
                                         List<HoodieWriteStat> stats,
                                         boolean consistencyCheckEnabled) throws HoodieIOException {
    try {
      // Reconcile marker and data files with WriteStats so that partially written data-files due to failed
      // (but succeeded on retry) tasks are removed.
      String basePath = getMetaClient().getBasePath();
      MarkerFiles markers = new MarkerFiles(this, instantTs);

      if (!markers.doesMarkerDirExist()) {
        // can happen if it was an empty write say.
        return;
      }

      // we are not including log appends here, since they are already fail-safe.
      Set<String> invalidDataPaths = markers.createdAndMergedDataPaths(jsc, config.getFinalizeWriteParallelism());
      Set<String> validDataPaths = stats.stream()
          .map(HoodieWriteStat::getPath)
          .filter(p -> p.endsWith(this.getBaseFileExtension()))
          .collect(Collectors.toSet());

      // Contains list of partially created files. These needs to be cleaned up.
      invalidDataPaths.removeAll(validDataPaths);

      if (!invalidDataPaths.isEmpty()) {
        LOG.info("Removing duplicate data files created due to spark retries before committing. Paths=" + invalidDataPaths);
        Map<String, List<Pair<String, String>>> invalidPathsByPartition = invalidDataPaths.stream()
            .map(dp -> Pair.of(new Path(dp).getParent().toString(), new Path(basePath, dp).toString()))
            .collect(Collectors.groupingBy(Pair::getKey));

        // Ensure all files in delete list is actually present. This is mandatory for an eventually consistent FS.
        // Otherwise, we may miss deleting such files. If files are not found even after retries, fail the commit
        if (consistencyCheckEnabled) {
          // This will either ensure all files to be deleted are present.
          waitForAllFiles(jsc, invalidPathsByPartition, FileVisibility.APPEAR);
        }

        // Now delete partially written files
        jsc.setJobGroup(this.getClass().getSimpleName(), "Delete all partially written files");
        deleteInvalidFilesByPartitions(jsc, invalidPathsByPartition);

        // Now ensure the deleted files disappear
        if (consistencyCheckEnabled) {
          // This will either ensure all files to be deleted are absent.
          waitForAllFiles(jsc, invalidPathsByPartition, FileVisibility.DISAPPEAR);
        }
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Ensures all files passed either appear or disappear.
   *
   * @param jsc JavaSparkContext
   * @param groupByPartition Files grouped by partition
   * @param visibility Appear/Disappear
   */
  private void waitForAllFiles(JavaSparkContext jsc, Map<String, List<Pair<String, String>>> groupByPartition, FileVisibility visibility) {
    // This will either ensure all files to be deleted are present.
    jsc.setJobGroup(this.getClass().getSimpleName(), "Wait for all files to appear/disappear");
    boolean checkPassed =
        jsc.parallelize(new ArrayList<>(groupByPartition.entrySet()), config.getFinalizeWriteParallelism())
            .map(partitionWithFileList -> waitForCondition(partitionWithFileList.getKey(),
                partitionWithFileList.getValue().stream(), visibility))
            .collect().stream().allMatch(x -> x);
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

  public SparkTaskContextSupplier getSparkTaskContextSupplier() {
    return sparkTaskContextSupplier;
  }

  /**
   * Ensure that the current writerSchema is compatible with the latest schema of this dataset.
   *
   * When inserting/updating data, we read records using the last used schema and convert them to the
   * GenericRecords with writerSchema. Hence, we need to ensure that this conversion can take place without errors.
   *
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
        return HoodieLogBlockType.AVRO_DATA_BLOCK;
      default:
        throw new HoodieException("Base file format " + getBaseFileFormat()
            + " does not have associated log block format");
    }
  }

  public String getBaseFileExtension() {
    return getBaseFileFormat().getFileExtension();
  }
}
