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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.embedded.AbstractEmbeddedTimelineService;
import org.apache.hudi.client.embedded.SparkEmbeddedTimelineService;
import org.apache.hudi.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRestoreException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.exception.HoodieSavepointException;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.BaseHoodieTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTimelineArchiveLog;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.SparkCompactHelpers;
import org.apache.hudi.table.action.savepoint.SavepointHelpers;

import com.codahale.metrics.Timer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Spark implementation of {@link BaseHoodieWriteClient}.
 *
 * @param <T> Type of payload in {@link org.apache.hudi.common.model.HoodieRecord}
 */
public class HoodieSparkWriteClient<T extends HoodieRecordPayload> extends BaseHoodieWriteClient<T,
    JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(HoodieSparkWriteClient.class);
  private static final String LOOKUP_STR = "lookup";
  private final boolean rollbackPending;
  private final transient HoodieMetrics metrics;
  private transient Timer.Context compactionTimer;
  private transient AsyncCleanerService asyncCleanerService;

  /**
   * Create a write client, without cleaning up failed/inflight commits.
   *
   * @param context      HoodieSparkEngineContext
   * @param clientConfig instance of HoodieWriteConfig
   */
  public HoodieSparkWriteClient(HoodieSparkEngineContext context, HoodieWriteConfig clientConfig) {
    this(context, clientConfig, false);
  }

  /**
   * Create a write client, with new hudi index.
   *
   * @param context         HoodieSparkEngineContext
   * @param clientConfig    instance of HoodieWriteConfig
   * @param rollbackPending whether need to cleanup pending commits
   */
  public HoodieSparkWriteClient(HoodieSparkEngineContext context, HoodieWriteConfig clientConfig, boolean rollbackPending) {
    this(context, clientConfig, rollbackPending, HoodieSparkTable.create(clientConfig, context.getHadoopConf().get()));
  }

  public HoodieSparkWriteClient(HoodieSparkEngineContext context, HoodieWriteConfig clientConfig, boolean rollbackPending, BaseHoodieTable table) {
    this(context, clientConfig, rollbackPending, table, Option.empty());
  }

  /**
   * Create a write client, allows to specify all parameters.
   *
   * @param context         HoodieSparkEngineContext
   * @param clientConfig    instance of HoodieWriteConfig
   * @param rollbackPending whether need to cleanup pending commits
   * @param timelineService Timeline Service that runs as part of write client.
   */
  public HoodieSparkWriteClient(HoodieSparkEngineContext context, HoodieWriteConfig clientConfig, boolean rollbackPending,
                                BaseHoodieTable table, Option<AbstractEmbeddedTimelineService> timelineService) {
    super(context, table, clientConfig, timelineService);
    this.metrics = new HoodieMetrics(config, config.getTableName());
    this.rollbackPending = rollbackPending;
  }

  /**
   * Register hudi classes for Kryo serialization.
   *
   * @param conf instance of SparkConf
   * @return SparkConf
   */
  public static SparkConf registerClasses(SparkConf conf) {
    conf.registerKryoClasses(new Class[]{HoodieWriteConfig.class, HoodieRecord.class, HoodieKey.class});
    return conf;
  }

  /**
   * Filter out HoodieRecords that already exists in the output folder. This is useful in deduplication.
   *
   * @param hoodieRecords Input RDD of Hoodie records.
   * @return A subset of hoodieRecords RDD, with existing records filtered out.
   */
  public JavaRDD<HoodieRecord<T>> filterExists(JavaRDD<HoodieRecord<T>> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, hadoopConf);
    Timer.Context indexTimer = metrics.getIndexCtx();
    JavaRDD<HoodieRecord<T>> recordsWithLocation = getIndex().tagLocation(hoodieRecords, context, table);
    metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
    return recordsWithLocation.filter(v1 -> !v1.isCurrentLocationKnown());
  }

  /**
   * Upsert a batch of new records into Hoodie table at the supplied instantTime.
   *
   * @param records     JavaRDD of hoodieRecords to upsert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> upsert(JavaRDD<HoodieRecord<T>> records, final String instantTime) {
    HoodieSparkTable<T> table = getTableAndInitCtx(WriteOperationType.UPSERT);
    table.validateUpsertSchema();
    setOperationType(WriteOperationType.UPSERT);
    HoodieWriteMetadata<JavaRDD<WriteStatus>> result = table.upsert(context, instantTime, records);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    return postWrite(result, instantTime, table);
  }

  /**
   * Upserts the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * @param preppedRecords Prepared HoodieRecords to upsert
   * @param instantTime    Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> upsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, final String instantTime) {
    HoodieSparkTable<T> table = getTableAndInitCtx(WriteOperationType.UPSERT_PREPPED);
    table.validateUpsertSchema();
    setOperationType(WriteOperationType.UPSERT_PREPPED);
    HoodieWriteMetadata result = table.upsertPrepped(context, instantTime, preppedRecords);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
    return postWrite(result, instantTime, table);
  }

  /**
   * Inserts the given HoodieRecords, into the table. This API is intended to be used for normal writes.
   * <p>
   * This implementation skips the index check and is able to leverage benefits such as small file handling/blocking
   * alignment, as with upsert(), by profiling the workload
   *
   * @param records     HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> insert(JavaRDD<HoodieRecord<T>> records, final String instantTime) {
    HoodieSparkTable<T> table = getTableAndInitCtx(WriteOperationType.INSERT);
    table.validateInsertSchema();
    setOperationType(WriteOperationType.INSERT);
    HoodieWriteMetadata result = table.insert(context, instantTime, records);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
    return postWrite(result, instantTime, table);
  }

  /**
   * Inserts the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation skips the index check, skips de-duping and is able to leverage benefits such as small file
   * handling/blocking alignment, as with insert(), by profiling the workload. The prepared HoodieRecords should be
   * de-duped if needed.
   *
   * @param preppedRecords HoodieRecords to insert
   * @param instantTime    Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> insertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, final String instantTime) {
    HoodieSparkTable<T> table = getTableAndInitCtx(WriteOperationType.INSERT_PREPPED);
    table.validateInsertSchema();
    setOperationType(WriteOperationType.INSERT_PREPPED);
    HoodieWriteMetadata result = table.insertPrepped(context, instantTime, preppedRecords);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
    return postWrite(result, instantTime, table);
  }

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk loads into a Hoodie
   * table for the very first time (e.g: converting an existing table to Hoodie).
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and attempts to control
   * the numbers of files with less memory compared to the {@link HoodieSparkWriteClient#insert(JavaRDD, String)}
   *
   * @param records     HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records, final String instantTime) {
    return bulkInsert(records, instantTime, Option.empty());
  }

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk loads into a Hoodie
   * table for the very first time (e.g: converting an existing table to Hoodie).
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and attempts to control
   * the numbers of files with less memory compared to the {@link HoodieSparkWriteClient#insert(JavaRDD, String)}. Optionally
   * it allows users to specify their own partitioner. If specified then it will be used for repartitioning records. See
   * {@link UserDefinedBulkInsertPartitioner}.
   *
   * @param records               HoodieRecords to insert
   * @param instantTime           Instant time of the commit
   * @param bulkInsertPartitioner If specified then it will be used to partition input records before they are inserted
   *                              into hoodie.
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records, final String instantTime,
                                         Option<UserDefinedBulkInsertPartitioner<JavaRDD<HoodieRecord<T>>>> bulkInsertPartitioner) {
    HoodieSparkTable<T> table = getTableAndInitCtx(WriteOperationType.BULK_INSERT);
    table.validateInsertSchema();
    setOperationType(WriteOperationType.BULK_INSERT);
    HoodieWriteMetadata result = table.bulkInsert(context, instantTime, records, bulkInsertPartitioner);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
    return postWrite(result, instantTime, table);
  }

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk loads into a Hoodie
   * table for the very first time (e.g: converting an existing table to Hoodie). The input records should contain no
   * duplicates if needed.
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and attempts to control
   * the numbers of files with less memory compared to the {@link HoodieSparkWriteClient#insert(JavaRDD, String)}. Optionally
   * it allows users to specify their own partitioner. If specified then it will be used for repartitioning records. See
   * {@link UserDefinedBulkInsertPartitioner}.
   *
   * @param preppedRecords        HoodieRecords to insert
   * @param instantTime           Instant time of the commit
   * @param bulkInsertPartitioner If specified then it will be used to partition input records before they are inserted
   *                              into hoodie.
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> bulkInsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, final String instantTime,
                                                       Option<UserDefinedBulkInsertPartitioner<JavaRDD<HoodieRecord<T>>>> bulkInsertPartitioner) {
    HoodieSparkTable<T> table = getTableAndInitCtx(WriteOperationType.BULK_INSERT_PREPPED);
    table.validateInsertSchema();
    setOperationType(WriteOperationType.BULK_INSERT_PREPPED);
    HoodieWriteMetadata result = table.bulkInsertPrepped(context, instantTime, preppedRecords, bulkInsertPartitioner);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
    return postWrite(result, instantTime, table);
  }

  /**
   * Deletes a list of {@link HoodieKey}s from the Hoodie table, at the supplied instantTime {@link HoodieKey}s will be
   * de-duped and non existent keys will be removed before deleting.
   *
   * @param keys        {@link List} of {@link HoodieKey}s to be deleted
   * @param instantTime Commit time handle
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> delete(JavaRDD<HoodieKey> keys, final String instantTime) {
    HoodieSparkTable<T> table = getTableAndInitCtx(WriteOperationType.DELETE);
    setOperationType(WriteOperationType.DELETE);
    HoodieWriteMetadata result = table.delete(context, instantTime, keys);
    return postWrite(result, instantTime, table);
  }

  /**
   * Common method containing steps to be performed after write (upsert/insert/..) operations including auto-commit.
   *
   * @param result      Commit Action Result
   * @param instantTime Instant Time
   * @param hoodieTable Hoodie Table
   * @return Write Status
   */
  private JavaRDD<WriteStatus> postWrite(HoodieWriteMetadata<JavaRDD<WriteStatus>> result, String instantTime, HoodieSparkTable<T> hoodieTable) {
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(getOperationType().name(), result.getIndexUpdateDuration().get().toMillis());
    }
    if (result.isCommitted()) {
      // Perform post commit operations.
      if (result.getFinalizeDuration().isPresent()) {
        metrics.updateFinalizeWriteMetrics(result.getFinalizeDuration().get().toMillis(),
            result.getWriteStats().get().size());
      }

      postCommit(result.getCommitMetadata().get(), instantTime, Option.empty());

      emitCommitMetrics(instantTime, result.getCommitMetadata().get(),
          hoodieTable.getMetaClient().getCommitActionType());
    }
    return result.getWriteStatuses();
  }

  @Override
  protected boolean commit(String instantTime, JavaRDD<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata, String actionType) {

    LOG.info("Committing " + instantTime);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, hadoopConf);

    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    List<HoodieWriteStat> stats = writeStatuses.map(WriteStatus::getStat).collect();
    stats.forEach(stat -> metadata.addWriteStat(stat.getPartitionPath(), stat));

    // Finalize write
    finalizeWrite(table, instantTime, stats);

    // add in extra metadata
    if (extraMetadata.isPresent()) {
      extraMetadata.get().forEach(metadata::addMetadata);
    }
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, config.getSchema());
    metadata.setOperationType(operationType);

    try {
      activeTimeline.saveAsComplete(new HoodieInstant(true, actionType, instantTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
      postCommit(metadata, instantTime, extraMetadata);
      emitCommitMetrics(instantTime, metadata, actionType);
      LOG.info("Committed " + instantTime);
    } catch (IOException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + instantTime,
          e);
    }
    return true;
  }

  @Override
  protected void postCommit(HoodieCommitMetadata metadata, String instantTime,
                            Option<Map<String, String>> extraMetadata) {
    try {
      // Do an inline compaction if enabled
      if (config.isInlineCompaction()) {
        metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "true");
        inlineCompact(extraMetadata);
      } else {
        metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "false");
      }
      // We cannot have unbounded commit files. Archive commits if we have to archive
      HoodieSparkTable<T> table = HoodieSparkTable.create(config, hadoopConf);
      HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(config, createMetaClient(true), table);
      archiveLog.archiveIfRequired(hadoopConf);
      autoCleanOnCommit(instantTime);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Handle auto clean during commit.
   * @param instantTime
   */
  private void autoCleanOnCommit(String instantTime) {
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

  @Override
  protected void finalizeWrite(BaseHoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> table, String instantTime, List<HoodieWriteStat> stats) {
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

  /**
   * Create a savepoint based on the latest commit action on the timeline.
   *
   * @param user    - User creating the savepoint
   * @param comment - Comment for the savepoint
   */
  public void savepoint(String user, String comment) {
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, hadoopConf);
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
   * @param user        - User creating the savepoint
   * @param comment     - Comment for the savepoint
   */
  public void savepoint(String instantTime, String user, String comment) {
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, hadoopConf);
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
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, hadoopConf);
    SavepointHelpers.deleteSavepoint(table, savepointTime);
  }

  /**
   * Restore the data to the savepoint.
   * <p>
   * WARNING: This rolls back recent commits and deleted data files and also pending compactions after savepoint time.
   * Queries accessing the files will mostly fail. This is expected to be a manual operation and no concurrent write or
   * compaction is expected to be running
   *
   * @param savepointTime - savepoint time to rollback to
   * @return true if the savepoint was restored to successfully
   */
  public void restoreToSavepoint(String savepointTime) {
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, hadoopConf);
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
    final Timer.Context context = this.metrics.getRollbackCtx();
    try {
      HoodieSparkTable<T> table = HoodieSparkTable.create(config, hadoopConf);
      Option<HoodieInstant> commitInstantOpt = Option.fromJavaOptional(table.getActiveTimeline().getCommitsTimeline().getInstants()
          .filter(instant -> HoodieActiveTimeline.EQUALS.test(instant.getTimestamp(), commitInstantTime))
          .findFirst());
      if (commitInstantOpt.isPresent()) {
        HoodieRollbackMetadata rollbackMetadata = table.rollback(super.context, rollbackInstantTime, commitInstantOpt.get(), true);
        if (context != null) {
          long durationInMs = metrics.getDurationInMs(context.stop());
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
    Timer.Context context = metrics.getRollbackCtx();
    try {
      HoodieSparkTable<T> table = HoodieSparkTable.create(config, hadoopConf);
      HoodieRestoreMetadata restoreMetadata = table.restore(super.context, restoreInstantTime, instantTime);
      if (context != null) {
        final long durationInMs = metrics.getDurationInMs(context.stop());
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
   * Releases any resources used by the client.
   */
  @Override
  public void close() {
    AsyncCleanerService.forceShutdown(asyncCleanerService);
    asyncCleanerService = null;
    super.close();
  }

  @Override
  public void startEmbeddedServerView() {
    if (config.isEmbeddedTimelineServerEnabled()) {
      if (!timelineServer.isPresent()) {
        // Run Embedded Timeline Server
        LOG.info("Starting Timeline service !!");
        timelineServer = Option.of(new SparkEmbeddedTimelineService(context,
            config.getClientSpecifiedViewStorageConfig()));
        try {
          timelineServer.get().startServer();
          // Allow executor to find this newly instantiated timeline service
          config.setViewStorageConfig(timelineServer.get().getRemoteFileSystemViewConfig());
        } catch (IOException e) {
          LOG.warn("Unable to start timeline service. Proceeding as if embedded server is disabled", e);
          stopEmbeddedServerView(false);
        }
      } else {
        LOG.info("Timeline Server already running. Not restarting the service");
      }
    } else {
      LOG.info("Embedded Timeline Server is disabled. Not starting timeline service");
    }
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   */
  @Override
  public HoodieCleanMetadata clean(String cleanInstantTime) throws HoodieIOException {
    LOG.info("Cleaner started");
    final Timer.Context context = metrics.getCleanCtx();
    HoodieCleanMetadata metadata = HoodieSparkTable.create(config, hadoopConf).clean(super.context, cleanInstantTime);
    if (context != null && metadata != null) {
      long durationMs = metrics.getDurationInMs(context.stop());
      metrics.updateCleanMetrics(durationMs, metadata.getTotalFilesDeleted());
      LOG.info("Cleaned " + metadata.getTotalFilesDeleted() + " files"
          + " Earliest Retained Instant :" + metadata.getEarliestCommitToRetain()
          + " cleanerElaspsedMs" + durationMs);
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
    startCommit(instantTime);
    return instantTime;
  }

  /**
   * Provides a new commit time for a write operation (insert/update/delete).
   *
   * @param instantTime Instant time to be generated
   */
  public void startCommitWithTime(String instantTime) {
    // NOTE : Need to ensure that rollback is done before a new commit is started
    if (rollbackPending) {
      // Only rollback inflight commit/delta-commits. Do not touch compaction commits
      rollbackPendingCommits();
    }
    startCommit(instantTime);
  }

  private void startCommit(String instantTime) {
    LOG.info("Generate a new instant time " + instantTime);
    HoodieTableMetaClient metaClient = createMetaClient(true);
    // if there are pending compactions, their instantTime must not be greater than that of this instant time
    metaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant().ifPresent(latestPending ->
        ValidationUtils.checkArgument(
            HoodieTimeline.compareTimestamps(latestPending.getTimestamp(), HoodieTimeline.LESSER_THAN, instantTime),
            "Latest pending compaction instant time must be earlier than this instant time. Latest Compaction :"
                + latestPending + ",  Ingesting at " + instantTime));
    metaClient.getActiveTimeline().createNewInstant(new HoodieInstant(HoodieInstant.State.REQUESTED, metaClient.getCommitActionType(),
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
   * @param instantTime   Compaction Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  public boolean scheduleCompactionAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    LOG.info("Scheduling compaction at instant time :" + instantTime);
    Option<HoodieCompactionPlan> plan = HoodieSparkTable.create(config, hadoopConf)
        .scheduleCompaction(context, instantTime, extraMetadata);
    return plan.isPresent();
  }

  /**
   * Performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @return RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> compact(String compactionInstantTime) {
    return compact(compactionInstantTime, config.shouldAutoCommit());
  }

  /**
   * Commit a compaction operation. Allow passing additional meta-data to be stored in commit instant file.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @param writeStatuses         RDD of WriteStatus to inspect errors and counts
   * @param extraMetadata         Extra Metadata to be stored
   */
  public void commitCompaction(String compactionInstantTime, JavaRDD<WriteStatus> writeStatuses,
                               Option<Map<String, String>> extraMetadata) throws IOException {
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, hadoopConf);
    HoodieCommitMetadata metadata = SparkCompactHelpers.newInstance().createCompactionMetadata(
        table, compactionInstantTime, writeStatuses, config.getSchema());
    extraMetadata.ifPresent(m -> m.forEach(metadata::addMetadata));
    completeCompaction(metadata, writeStatuses, table, compactionInstantTime);
  }

  /**
   * Commit Compaction and track metrics.
   */
  protected void completeCompaction(HoodieCommitMetadata metadata, JavaRDD<WriteStatus> writeStatuses, HoodieSparkTable<T> table,
                                    String compactionCommitTime) {

    List<HoodieWriteStat> writeStats = writeStatuses.map(WriteStatus::getStat).collect();
    finalizeWrite(table, compactionCommitTime, writeStats);
    LOG.info("Committing Compaction " + compactionCommitTime + ". Finished with result " + metadata);
    SparkCompactHelpers.newInstance().completeInflightCompaction(table, compactionCommitTime, metadata);

    if (compactionTimer != null) {
      long durationInMs = metrics.getDurationInMs(compactionTimer.stop());
      try {
        metrics.updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(compactionCommitTime).getTime(),
            durationInMs, metadata, HoodieActiveTimeline.COMPACTION_ACTION);
      } catch (ParseException e) {
        throw new HoodieCommitException("Commit time is not of valid format. Failed to commit compaction "
            + config.getBasePath() + " at time " + compactionCommitTime, e);
      }
    }
    LOG.info("Compacted successfully on commit " + compactionCommitTime);
  }

  /**
   * Rollback failed compactions. Inflight rollbacks for compactions revert the .inflight file to the .requested file
   *
   * @param inflightInstant Inflight Compaction Instant
   * @param table           Hoodie Table
   */
  public void rollbackInflightCompaction(HoodieInstant inflightInstant, HoodieSparkTable<T> table) {
    table.rollback(context, HoodieActiveTimeline.createNewInstantTime(), inflightInstant, false);
    table.getActiveTimeline().revertCompactionInflightToRequested(inflightInstant);
  }

  /**
   * Cleanup all pending commits.
   */
  private void rollbackPendingCommits() {
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, hadoopConf);
    HoodieTimeline inflightTimeline = table.getMetaClient().getCommitsTimeline().filterPendingExcludingCompaction();
    List<String> commits = inflightTimeline.getReverseOrderedInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    for (String commit : commits) {
      rollback(commit);
    }
  }

  /**
   * Ensures compaction instant is in expected state and performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @return RDD of Write Status
   */
  private JavaRDD<WriteStatus> compact(String compactionInstantTime, boolean shouldComplete) {
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, hadoopConf);
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      rollbackInflightCompaction(inflightInstant, table);
      table.getMetaClient().reloadActiveTimeline();
    }
    compactionTimer = metrics.getCompactionCtx();
    HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = table.compact(context, compactionInstantTime);
    JavaRDD<WriteStatus> statuses = compactionMetadata.getWriteStatuses();
    if (shouldComplete && compactionMetadata.getCommitMetadata().isPresent()) {
      completeCompaction(compactionMetadata.getCommitMetadata().get(), statuses, table, compactionInstantTime);
    }
    return statuses;
  }

  /**
   * Performs a compaction operation on a table, serially before or after an insert/upsert action.
   */
  private Option<String> inlineCompact(Option<Map<String, String>> extraMetadata) {
    Option<String> compactionInstantTimeOpt = scheduleCompaction(extraMetadata);
    compactionInstantTimeOpt.ifPresent(compactionInstantTime -> {
      // inline compaction should auto commit as the user is never given control
      compact(compactionInstantTime, true);
    });
    return compactionInstantTimeOpt;
  }

  /**
   * Get HoodieTable and init {@link Timer.Context}.
   *
   * @param operationType write operation type
   * @return HoodieTable
   */
  private HoodieSparkTable<T> getTableAndInitCtx(WriteOperationType operationType) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    if (operationType == WriteOperationType.DELETE) {
      setWriteSchemaForDeletes(metaClient);
    }
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieSparkTable<T> table = HoodieSparkTable.create(metaClient, config, hadoopConf);
    if (table.getMetaClient().getCommitActionType().equals(HoodieTimeline.COMMIT_ACTION)) {
      writeContext = metrics.getCommitCtx();
    } else {
      writeContext = metrics.getDeltaCommitCtx();
    }
    return table;
  }

}
