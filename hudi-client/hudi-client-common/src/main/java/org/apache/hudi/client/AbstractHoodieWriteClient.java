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

package org.apache.hudi.client;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.callback.HoodieWriteCommitCallback;
import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage;
import org.apache.hudi.callback.util.HoodieCommitCallbackFactory;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.common.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRestoreException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.exception.HoodieSavepointException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.HoodieTimelineArchiveLog;
import org.apache.hudi.table.MarkerFiles;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.savepoint.SavepointHelpers;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Abstract Write Client providing functionality for performing commit, index updates and rollback
 * Reused for regular write operations like upsert/insert/bulk-insert.. as well as bootstrap
 *
 * @param <T> Sub type of HoodieRecordPayload
 * @param <I> Type of inputs
 * @param <K> Type of keys
 * @param <O> Type of outputs
 */
public abstract class AbstractHoodieWriteClient<T extends HoodieRecordPayload, I, K, O> extends AbstractHoodieClient {

  protected static final String LOOKUP_STR = "lookup";
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(AbstractHoodieWriteClient.class);

  protected final transient HoodieMetrics metrics;
  private final transient HoodieIndex<T, I, K, O> index;

  protected transient Timer.Context writeTimer = null;
  protected transient Timer.Context compactionTimer;
  protected transient Timer.Context clusteringTimer;

  private transient WriteOperationType operationType;
  private transient HoodieWriteCommitCallback commitCallback;
  protected final boolean rollbackPending;
  protected transient AsyncCleanerService asyncCleanerService;

  /**
   * Create a write client, without cleaning up failed/inflight commits.
   *
   * @param context HoodieEngineContext
   * @param clientConfig instance of HoodieWriteConfig
   */
  public AbstractHoodieWriteClient(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    this(context, clientConfig, false);
  }

  /**
   * Create a write client, with new hudi index.
   *
   * @param context HoodieEngineContext
   * @param writeConfig instance of HoodieWriteConfig
   * @param rollbackPending whether need to cleanup pending commits
   */
  public AbstractHoodieWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig, boolean rollbackPending) {
    this(context, writeConfig, rollbackPending, Option.empty());
  }

  /**
   * Create a write client, allows to specify all parameters.
   *
   * @param context         HoodieEngineContext
   * @param writeConfig instance of HoodieWriteConfig
   * @param rollbackPending whether need to cleanup pending commits
   * @param timelineService Timeline Service that runs as part of write client.
   */
  public AbstractHoodieWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig, boolean rollbackPending,
                                   Option<EmbeddedTimelineService> timelineService) {
    super(context, writeConfig, timelineService);
    this.metrics = new HoodieMetrics(config, config.getTableName());
    this.rollbackPending = rollbackPending;
    this.index = createIndex(writeConfig);
  }

  protected abstract HoodieIndex<T, I, K, O> createIndex(HoodieWriteConfig writeConfig);

  public void setOperationType(WriteOperationType operationType) {
    this.operationType = operationType;
  }

  public WriteOperationType getOperationType() {
    return this.operationType;
  }

  /**
   * Commit changes performed at the given instantTime marker.
   */
  public boolean commit(String instantTime, O writeStatuses) {
    return commit(instantTime, writeStatuses, Option.empty());
  }

  /**
   *
   * Commit changes performed at the given instantTime marker.
   */
  public boolean commit(String instantTime, O writeStatuses, Option<Map<String, String>> extraMetadata) {
    HoodieTableMetaClient metaClient = createMetaClient(false);
    String actionType = metaClient.getCommitActionType();
    return commit(instantTime, writeStatuses, extraMetadata, actionType, Collections.emptyMap());
  }

  public abstract boolean commit(String instantTime, O writeStatuses, Option<Map<String, String>> extraMetadata,
                                 String commitActionType, Map<String, List<String>> partitionToReplacedFileIds);

  public boolean commitStats(String instantTime, List<HoodieWriteStat> stats, Option<Map<String, String>> extraMetadata,
                             String commitActionType) {
    return commitStats(instantTime, stats, extraMetadata, commitActionType, Collections.emptyMap());
  }

  public boolean commitStats(String instantTime, List<HoodieWriteStat> stats, Option<Map<String, String>> extraMetadata,
                             String commitActionType, Map<String, List<String>> partitionToReplaceFileIds) {
    LOG.info("Committing " + instantTime + " action " + commitActionType);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable table = createTable(config, hadoopConf);

    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieCommitMetadata metadata = CommitUtils.buildMetadata(stats, partitionToReplaceFileIds, extraMetadata, operationType, config.getSchema(), commitActionType);
    // Finalize write
    finalizeWrite(table, instantTime, stats);

    try {
      activeTimeline.saveAsComplete(new HoodieInstant(true, commitActionType, instantTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
      postCommit(table, metadata, instantTime, extraMetadata);
      emitCommitMetrics(instantTime, metadata, commitActionType);
      LOG.info("Committed " + instantTime);
    } catch (IOException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + instantTime,
          e);
    }

    // callback if needed.
    if (config.writeCommitCallbackOn()) {
      if (null == commitCallback) {
        commitCallback = HoodieCommitCallbackFactory.create(config);
      }
      commitCallback.call(new HoodieWriteCommitCallbackMessage(instantTime, config.getTableName(), config.getBasePath()));
    }
    return true;
  }

  protected abstract HoodieTable<T, I, K, O> createTable(HoodieWriteConfig config, Configuration hadoopConf);

  void emitCommitMetrics(String instantTime, HoodieCommitMetadata metadata, String actionType) {
    try {

      if (writeTimer != null) {
        long durationInMs = metrics.getDurationInMs(writeTimer.stop());
        metrics.updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(instantTime).getTime(), durationInMs,
            metadata, actionType);
        writeTimer = null;
      }
    } catch (ParseException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + instantTime
          + "Instant time is not of valid format", e);
    }
  }

  /**
   * Filter out HoodieRecords that already exists in the output folder. This is useful in deduplication.
   *
   * @param hoodieRecords Input Hoodie records.
   * @return A subset of hoodieRecords, with existing records filtered out.
   */
  public abstract I filterExists(I hoodieRecords);

  /**
   * Main API to run bootstrap to hudi.
   */
  public void bootstrap(Option<Map<String, String>> extraMetadata) {
    if (rollbackPending) {
      rollBackInflightBootstrap();
    }
    HoodieTable<T, I, K, O> table = getTableAndInitCtx(WriteOperationType.UPSERT, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS);
    table.bootstrap(context, extraMetadata);
  }

  /**
   * Main API to rollback pending bootstrap.
   */
  protected void rollBackInflightBootstrap() {
    LOG.info("Rolling back pending bootstrap if present");
    HoodieTable<T, I, K, O> table = createTable(config, hadoopConf);
    HoodieTimeline inflightTimeline = table.getMetaClient().getCommitsTimeline().filterPendingExcludingCompaction();
    Option<String> instant = Option.fromJavaOptional(
        inflightTimeline.getReverseOrderedInstants().map(HoodieInstant::getTimestamp).findFirst());
    if (instant.isPresent() && HoodieTimeline.compareTimestamps(instant.get(), HoodieTimeline.LESSER_THAN_OR_EQUALS,
        HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS)) {
      LOG.info("Found pending bootstrap instants. Rolling them back");
      table.rollbackBootstrap(context, HoodieActiveTimeline.createNewInstantTime());
      LOG.info("Finished rolling back pending bootstrap");
    }

  }

  /**
   * Upsert a batch of new records into Hoodie table at the supplied instantTime.
   *
   * @param records hoodieRecords to upsert
   * @param instantTime Instant time of the commit
   * @return WriteStatus to inspect errors and counts
   */
  public abstract O upsert(I records, final String instantTime);

  /**
   * Upserts the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * @param preppedRecords Prepared HoodieRecords to upsert
   * @param instantTime Instant time of the commit
   * @return Collection of WriteStatus to inspect errors and counts
   */
  public abstract O upsertPreppedRecords(I preppedRecords, final String instantTime);

  /**
   * Inserts the given HoodieRecords, into the table. This API is intended to be used for normal writes.
   * <p>
   * This implementation skips the index check and is able to leverage benefits such as small file handling/blocking
   * alignment, as with upsert(), by profiling the workload
   *
   * @param records HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return Collection of WriteStatus to inspect errors and counts
   */
  public abstract O insert(I records, final String instantTime);

  /**
   * Inserts the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation skips the index check, skips de-duping and is able to leverage benefits such as small file
   * handling/blocking alignment, as with insert(), by profiling the workload. The prepared HoodieRecords should be
   * de-duped if needed.
   *
   * @param preppedRecords HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return Collection of WriteStatus to inspect errors and counts
   */
  public abstract O insertPreppedRecords(I preppedRecords, final String instantTime);

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk loads into a Hoodie
   * table for the very first time (e.g: converting an existing table to Hoodie).
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and attempts to control
   * the numbers of files with less memory compared to the {@link AbstractHoodieWriteClient#insert(I, String)}
   *
   * @param records HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return Collection of WriteStatus to inspect errors and counts
   */
  public abstract O bulkInsert(I records, final String instantTime);

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk loads into a Hoodie
   * table for the very first time (e.g: converting an existing table to Hoodie).
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and attempts to control
   * the numbers of files with less memory compared to the {@link AbstractHoodieWriteClient#insert(I, String)}. Optionally
   * it allows users to specify their own partitioner. If specified then it will be used for repartitioning records. See
   * {@link BulkInsertPartitioner}.
   *
   * @param records HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @param userDefinedBulkInsertPartitioner If specified then it will be used to partition input records before they are inserted
   * into hoodie.
   * @return Collection of WriteStatus to inspect errors and counts
   */
  public abstract O bulkInsert(I records, final String instantTime,
                               Option<BulkInsertPartitioner<I>> userDefinedBulkInsertPartitioner);


  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk loads into a Hoodie
   * table for the very first time (e.g: converting an existing table to Hoodie). The input records should contain no
   * duplicates if needed.
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and attempts to control
   * the numbers of files with less memory compared to the {@link AbstractHoodieWriteClient#insert(I, String)}. Optionally
   * it allows users to specify their own partitioner. If specified then it will be used for repartitioning records. See
   * {@link BulkInsertPartitioner}.
   *
   * @param preppedRecords HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @param bulkInsertPartitioner If specified then it will be used to partition input records before they are inserted
   * into hoodie.
   * @return Collection of WriteStatus to inspect errors and counts
   */
  public abstract O bulkInsertPreppedRecords(I preppedRecords, final String instantTime,
                                             Option<BulkInsertPartitioner<I>> bulkInsertPartitioner);

  /**
   * Deletes a list of {@link HoodieKey}s from the Hoodie table, at the supplied instantTime {@link HoodieKey}s will be
   * de-duped and non existent keys will be removed before deleting.
   *
   * @param keys {@link List} of {@link HoodieKey}s to be deleted
   * @param instantTime Commit time handle
   * @return Collection of WriteStatus to inspect errors and counts
   */
  public abstract O delete(K keys, final String instantTime);

  /**
   * Common method containing steps to be performed after write (upsert/insert/..) operations including auto-commit.
   * @param result  Commit Action Result
   * @param instantTime Instant Time
   * @param hoodieTable Hoodie Table
   * @return Write Status
   */
  protected abstract O postWrite(HoodieWriteMetadata<O> result, String instantTime, HoodieTable<T, I, K, O> hoodieTable);

  /**
   * Post Commit Hook. Derived classes use this method to perform post-commit processing
   *
   * @param table         table to commit on
   * @param metadata      Commit Metadata corresponding to committed instant
   * @param instantTime   Instant Time
   * @param extraMetadata Additional Metadata passed by user
   */
  protected void postCommit(HoodieTable<T, I, K, O> table, HoodieCommitMetadata metadata, String instantTime, Option<Map<String, String>> extraMetadata) {
    try {

      // Delete the marker directory for the instant.
      new MarkerFiles(table, instantTime).quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());

      // Do an inline compaction if enabled
      if (config.isInlineCompaction()) {
        runAnyPendingCompactions(table);
        metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "true");
        inlineCompact(extraMetadata);
      } else {
        metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "false");
      }

      // Do an inline clustering if enabled
      if (config.isInlineClustering()) {
        runAnyPendingClustering(table);
        metadata.addMetadata(HoodieClusteringConfig.INLINE_CLUSTERING_PROP, "true");
        inlineCluster(extraMetadata);
      } else {
        metadata.addMetadata(HoodieClusteringConfig.INLINE_CLUSTERING_PROP, "false");
      }

      // We cannot have unbounded commit files. Archive commits if we have to archive
      HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(config, table);
      archiveLog.archiveIfRequired(context);
      autoCleanOnCommit(instantTime);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  protected void runAnyPendingCompactions(HoodieTable<T, I, K, O> table) {
    table.getActiveTimeline().getCommitsAndCompactionTimeline().filterPendingCompactionTimeline().getInstants()
        .forEach(instant -> {
          LOG.info("Running previously failed inflight compaction at instant " + instant);
          compact(instant.getTimestamp(), true);
        });
  }

  protected void runAnyPendingClustering(HoodieTable<T, I, K, O> table) {
    table.getActiveTimeline().filterPendingReplaceTimeline().getInstants().forEach(instant -> {
      Option<Pair<HoodieInstant, HoodieClusteringPlan>> instantPlan = ClusteringUtils.getClusteringPlan(table.getMetaClient(), instant);
      if (instantPlan.isPresent()) {
        LOG.info("Running pending clustering at instant " + instantPlan.get().getLeft());
        cluster(instant.getTimestamp(), true);
      }
    });
  }

  /**
   * Handle auto clean during commit.
   *
   * @param instantTime
   */
  protected void autoCleanOnCommit(String instantTime) {
    if (config.isAutoClean()) {
      // Call clean to cleanup if there is anything to cleanup after the commit,
      if (config.isAsyncClean()) {
        LOG.info("Cleaner has been spawned already. Waiting for it to finish");
        AsyncCleanerService.waitForCompletion(asyncCleanerService);
        LOG.info("Cleaner has finished");
      } else {
        LOG.info("Auto cleaning is enabled. Running cleaner now");
        clean(instantTime);
      }
    }
  }

  /**
   * Create a savepoint based on the latest commit action on the timeline.
   *
   * @param user - User creating the savepoint
   * @param comment - Comment for the savepoint
   */
  public void savepoint(String user, String comment) {
    HoodieTable<T, I, K, O> table = createTable(config, hadoopConf);
    if (table.getCompletedCommitsTimeline().empty()) {
      throw new HoodieSavepointException("Could not savepoint. Commit timeline is empty");
    }

    String latestCommit = table.getCompletedCommitsTimeline().lastInstant().get().getTimestamp();
    LOG.info("Savepointing latest commit " + latestCommit);
    savepoint(latestCommit, user, comment);
  }

  /**
   * Savepoint a specific commit instant time. Latest version of data files as of the passed in instantTime
   * will be referenced in the savepoint and will never be cleaned. The savepointed commit will never be rolledback or archived.
   * <p>
   * This gives an option to rollback the state to the savepoint anytime. Savepoint needs to be manually created and
   * deleted.
   * <p>
   * Savepoint should be on a commit that could not have been cleaned.
   *
   * @param instantTime - commit that should be savepointed
   * @param user - User creating the savepoint
   * @param comment - Comment for the savepoint
   */
  public void savepoint(String instantTime, String user, String comment) {
    HoodieTable<T, I, K, O> table = createTable(config, hadoopConf);
    table.savepoint(context, instantTime, user, comment);
  }

  /**
   * Delete a savepoint that was created. Once the savepoint is deleted, the commit can be rolledback and cleaner may
   * clean up data files.
   *
   * @param savepointTime - delete the savepoint
   * @return true if the savepoint was deleted successfully
   */
  public void deleteSavepoint(String savepointTime) {
    HoodieTable<T, I, K, O> table = createTable(config, hadoopConf);
    SavepointHelpers.deleteSavepoint(table, savepointTime);
  }

  /**
   * Restore the data to the savepoint.
   *
   * WARNING: This rolls back recent commits and deleted data files and also pending compactions after savepoint time.
   * Queries accessing the files will mostly fail. This is expected to be a manual operation and no concurrent write or
   * compaction is expected to be running
   *
   * @param savepointTime - savepoint time to rollback to
   * @return true if the savepoint was restored to successfully
   */
  public void restoreToSavepoint(String savepointTime) {
    HoodieTable<T, I, K, O> table = createTable(config, hadoopConf);
    SavepointHelpers.validateSavepointPresence(table, savepointTime);
    restoreToInstant(savepointTime);
    SavepointHelpers.validateSavepointRestore(table, savepointTime);
  }

  /**
   * Rollback the inflight record changes with the given commit time.
   *
   * @param commitInstantTime Instant time of the commit
   * @throws HoodieRollbackException if rollback cannot be performed successfully
   */
  public boolean rollback(final String commitInstantTime) throws HoodieRollbackException {
    LOG.info("Begin rollback of instant " + commitInstantTime);
    final String rollbackInstantTime = HoodieActiveTimeline.createNewInstantTime();
    final Timer.Context timerContext = this.metrics.getRollbackCtx();
    try {
      HoodieTable<T, I, K, O> table = createTable(config, hadoopConf);
      Option<HoodieInstant> commitInstantOpt = Option.fromJavaOptional(table.getActiveTimeline().getCommitsTimeline().getInstants()
          .filter(instant -> HoodieActiveTimeline.EQUALS.test(instant.getTimestamp(), commitInstantTime))
          .findFirst());
      if (commitInstantOpt.isPresent()) {
        HoodieRollbackMetadata rollbackMetadata = table.rollback(context, rollbackInstantTime, commitInstantOpt.get(), true);
        if (timerContext != null) {
          long durationInMs = metrics.getDurationInMs(timerContext.stop());
          metrics.updateRollbackMetrics(durationInMs, rollbackMetadata.getTotalFilesDeleted());
        }
        return true;
      } else {
        LOG.warn("Cannot find instant " + commitInstantTime + " in the timeline, for rollback");
        return false;
      }
    } catch (Exception e) {
      throw new HoodieRollbackException("Failed to rollback " + config.getBasePath() + " commits " + commitInstantTime, e);
    }
  }

  /**
   * NOTE : This action requires all writers (ingest and compact) to a table to be stopped before proceeding. Revert
   * the (inflight/committed) record changes for all commits after the provided instant time.
   *
   * @param instantTime Instant time to which restoration is requested
   */
  public HoodieRestoreMetadata restoreToInstant(final String instantTime) throws HoodieRestoreException {
    LOG.info("Begin restore to instant " + instantTime);
    final String restoreInstantTime = HoodieActiveTimeline.createNewInstantTime();
    Timer.Context timerContext = metrics.getRollbackCtx();
    try {
      HoodieTable<T, I, K, O> table = createTable(config, hadoopConf);
      HoodieRestoreMetadata restoreMetadata = table.restore(context, restoreInstantTime, instantTime);
      if (timerContext != null) {
        final long durationInMs = metrics.getDurationInMs(timerContext.stop());
        final long totalFilesDeleted = restoreMetadata.getHoodieRestoreMetadata().values().stream()
            .flatMap(Collection::stream)
            .mapToLong(HoodieRollbackMetadata::getTotalFilesDeleted)
            .sum();
        metrics.updateRollbackMetrics(durationInMs, totalFilesDeleted);
      }
      return restoreMetadata;
    } catch (Exception e) {
      throw new HoodieRestoreException("Failed to restore to " + instantTime, e);
    }
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   */
  public HoodieCleanMetadata clean(String cleanInstantTime) throws HoodieIOException {
    LOG.info("Cleaner started");
    final Timer.Context timerContext = metrics.getCleanCtx();
    HoodieCleanMetadata metadata = createTable(config, hadoopConf).clean(context, cleanInstantTime);
    if (timerContext != null && metadata != null) {
      long durationMs = metrics.getDurationInMs(timerContext.stop());
      metrics.updateCleanMetrics(durationMs, metadata.getTotalFilesDeleted());
      LOG.info("Cleaned " + metadata.getTotalFilesDeleted() + " files"
          + " Earliest Retained Instant :" + metadata.getEarliestCommitToRetain()
          + " cleanerElapsedMs" + durationMs);
    }
    return metadata;
  }

  public HoodieCleanMetadata clean() {
    return clean(HoodieActiveTimeline.createNewInstantTime());
  }

  /**
   * Provides a new commit time for a write operation (insert/update/delete).
   */
  public String startCommit() {
    // NOTE : Need to ensure that rollback is done before a new commit is started
    if (rollbackPending) {
      // Only rollback pending commit/delta-commits. Do not touch compaction commits
      rollbackPendingCommits();
    }
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    startCommitWithTime(instantTime);
    return instantTime;
  }

  /**
   * Provides a new commit time for a write operation (insert/update/delete).
   *
   * @param instantTime Instant time to be generated
   */
  public void startCommitWithTime(String instantTime) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    startCommitWithTime(instantTime, metaClient.getCommitActionType(), metaClient);
  }

  /**
   * Completes a new commit time for a write operation (insert/update/delete) with specified action.
   */
  public void startCommitWithTime(String instantTime, String actionType) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    startCommitWithTime(instantTime, actionType, metaClient);
  }

  /**
   * Completes a new commit time for a write operation (insert/update/delete) with specified action.
   */
  private void startCommitWithTime(String instantTime, String actionType, HoodieTableMetaClient metaClient) {
    // NOTE : Need to ensure that rollback is done before a new commit is started
    if (rollbackPending) {
      // Only rollback inflight commit/delta-commits. Do not touch compaction commits
      rollbackPendingCommits();
    }
    startCommit(instantTime, actionType, metaClient);
  }

  private void startCommit(String instantTime, String actionType, HoodieTableMetaClient metaClient) {
    LOG.info("Generate a new instant time: " + instantTime + " action: " + actionType);
    // if there are pending compactions, their instantTime must not be greater than that of this instant time
    metaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant().ifPresent(latestPending ->
        ValidationUtils.checkArgument(
            HoodieTimeline.compareTimestamps(latestPending.getTimestamp(), HoodieTimeline.LESSER_THAN, instantTime),
        "Latest pending compaction instant time must be earlier than this instant time. Latest Compaction :"
            + latestPending + ",  Ingesting at " + instantTime));
    metaClient.getActiveTimeline().createNewInstant(new HoodieInstant(HoodieInstant.State.REQUESTED, actionType,
        instantTime));
  }

  /**
   * Schedules a new compaction instant.
   *
   * @param extraMetadata Extra Metadata to be stored
   */
  public Option<String> scheduleCompaction(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    return scheduleCompactionAtInstant(instantTime, extraMetadata) ? Option.of(instantTime) : Option.empty();
  }

  /**
   * Schedules a new compaction instant with passed-in instant time.
   *
   * @param instantTime Compaction Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  public boolean scheduleCompactionAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    LOG.info("Scheduling compaction at instant time :" + instantTime);
    Option<HoodieCompactionPlan> plan = createTable(config, hadoopConf)
        .scheduleCompaction(context, instantTime, extraMetadata);
    return plan.isPresent();
  }

  /**
   * Performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @return Collection of WriteStatus to inspect errors and counts
   */
  public O compact(String compactionInstantTime) {
    return compact(compactionInstantTime, config.shouldAutoCommit());
  }

  /**
   * Commit a compaction operation. Allow passing additional meta-data to be stored in commit instant file.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @param writeStatuses Collection of WriteStatus to inspect errors and counts
   * @param extraMetadata Extra Metadata to be stored
   */
  public abstract void commitCompaction(String compactionInstantTime, O writeStatuses,
                                        Option<Map<String, String>> extraMetadata) throws IOException;

  /**
   * Commit Compaction and track metrics.
   */
  protected abstract void completeCompaction(HoodieCommitMetadata metadata, O writeStatuses,
                                             HoodieTable<T, I, K, O> table, String compactionCommitTime);
  
  /**
   * Rollback failed compactions. Inflight rollbacks for compactions revert the .inflight file to the .requested file
   *
   * @param inflightInstant Inflight Compaction Instant
   * @param table Hoodie Table
   */
  public void rollbackInflightCompaction(HoodieInstant inflightInstant, HoodieTable<T, I, K, O> table) {
    table.rollback(context, HoodieActiveTimeline.createNewInstantTime(), inflightInstant, false);
    table.getActiveTimeline().revertCompactionInflightToRequested(inflightInstant);
  }

  /**
   * Cleanup all pending commits.
   */
  private void rollbackPendingCommits() {
    HoodieTable<T, I, K, O> table = createTable(config, hadoopConf);
    HoodieTimeline inflightTimeline = table.getMetaClient().getCommitsTimeline().filterPendingExcludingCompaction();
    List<String> commits = inflightTimeline.getReverseOrderedInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    for (String commit : commits) {
      if (HoodieTimeline.compareTimestamps(commit, HoodieTimeline.LESSER_THAN_OR_EQUALS,
          HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS)) {
        rollBackInflightBootstrap();
        break;
      } else {
        rollback(commit);
      }
    }
  }

  /**
   * Ensures compaction instant is in expected state and performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @return Collection of Write Status
   */
  protected abstract O compact(String compactionInstantTime, boolean shouldComplete);

  /**
   * Performs a compaction operation on a table, serially before or after an insert/upsert action.
   */
  protected Option<String> inlineCompact(Option<Map<String, String>> extraMetadata) {
    Option<String> compactionInstantTimeOpt = scheduleCompaction(extraMetadata);
    compactionInstantTimeOpt.ifPresent(compactionInstantTime -> {
      // inline compaction should auto commit as the user is never given control
      compact(compactionInstantTime, true);
    });
    return compactionInstantTimeOpt;
  }

  /**
   * Schedules a new clustering instant.
   *
   * @param extraMetadata Extra Metadata to be stored
   */
  public Option<String> scheduleClustering(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    return scheduleClusteringAtInstant(instantTime, extraMetadata) ? Option.of(instantTime) : Option.empty();
  }

  /**
   * Schedules a new clustering instant with passed-in instant time.
   *
   * @param instantTime clustering Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  public boolean scheduleClusteringAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    LOG.info("Scheduling clustering at instant time :" + instantTime);
    Option<HoodieClusteringPlan> plan = createTable(config, hadoopConf)
        .scheduleClustering(context, instantTime, extraMetadata);
    return plan.isPresent();
  }

  /**
   * Ensures clustering instant is in expected state and performs clustering for the plan stored in metadata.
   *
   * @param clusteringInstant Clustering Instant Time
   * @return Collection of Write Status
   */
  public abstract HoodieWriteMetadata<O> cluster(String clusteringInstant, boolean shouldComplete);

  /**
   * Executes a clustering plan on a table, serially before or after an insert/upsert action.
   */
  protected Option<String> inlineCluster(Option<Map<String, String>> extraMetadata) {
    Option<String> clusteringInstantOpt = scheduleClustering(extraMetadata);
    clusteringInstantOpt.ifPresent(clusteringInstant -> {
      // inline cluster should auto commit as the user is never given control
      cluster(clusteringInstant, true);
    });
    return clusteringInstantOpt;
  }

  protected void rollbackInflightClustering(HoodieInstant inflightInstant, HoodieTable<T, I, K, O> table) {
    table.rollback(context, HoodieActiveTimeline.createNewInstantTime(), inflightInstant, false);
    table.getActiveTimeline().revertReplaceCommitInflightToRequested(inflightInstant);
  }

  /**
   * Finalize Write operation.
   *
   * @param table HoodieTable
   * @param instantTime Instant Time
   * @param stats Hoodie Write Stat
   */
  protected void finalizeWrite(HoodieTable<T, I, K, O> table, String instantTime, List<HoodieWriteStat> stats) {
    try {
      final Timer.Context finalizeCtx = metrics.getFinalizeCtx();
      table.finalizeWrite(context, instantTime, stats);
      if (finalizeCtx != null) {
        Option<Long> durationInMs = Option.of(metrics.getDurationInMs(finalizeCtx.stop()));
        durationInMs.ifPresent(duration -> {
          LOG.info("Finalize write elapsed time (milliseconds): " + duration);
          metrics.updateFinalizeWriteMetrics(duration, stats.size());
        });
      }
    } catch (HoodieIOException ioe) {
      throw new HoodieCommitException("Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
  }

  public HoodieMetrics getMetrics() {
    return metrics;
  }

  public HoodieIndex<T, I, K, O> getIndex() {
    return index;
  }

  /**
   * Get HoodieTable and init {@link Timer.Context}.
   *
   * @param operationType write operation type
   * @param instantTime current inflight instant time
   * @return HoodieTable
   */
  protected abstract HoodieTable<T, I, K, O> getTableAndInitCtx(WriteOperationType operationType, String instantTime);

  /**
   * Sets write schema from last instant since deletes may not have schema set in the config.
   */
  protected void setWriteSchemaForDeletes(HoodieTableMetaClient metaClient) {
    try {
      HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
      Option<HoodieInstant> lastInstant =
          activeTimeline.filterCompletedInstants().filter(s -> s.getAction().equals(metaClient.getCommitActionType())
          || s.getAction().equals(HoodieActiveTimeline.REPLACE_COMMIT_ACTION))
              .lastInstant();
      if (lastInstant.isPresent()) {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            activeTimeline.getInstantDetails(lastInstant.get()).get(), HoodieCommitMetadata.class);
        if (commitMetadata.getExtraMetadata().containsKey(HoodieCommitMetadata.SCHEMA_KEY)) {
          config.setSchema(commitMetadata.getExtraMetadata().get(HoodieCommitMetadata.SCHEMA_KEY));
        } else {
          throw new HoodieIOException("Latest commit does not have any schema in commit metadata");
        }
      } else {
        throw new HoodieIOException("Deletes issued without any prior commits");
      }
    } catch (IOException e) {
      throw new HoodieIOException("IOException thrown while reading last commit metadata", e);
    }
  }

  @Override
  public void close() {
    // release AsyncCleanerService
    AsyncCleanerService.forceShutdown(asyncCleanerService);
    asyncCleanerService = null;

    // Stop timeline-server if running
    super.close();
    // Calling this here releases any resources used by your index, so make sure to finish any related operations
    // before this point
    this.index.close();
  }
}
