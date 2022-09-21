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

import org.apache.hudi.async.AsyncArchiveService;
import org.apache.hudi.async.AsyncCleanerService;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieIndexCommitMetadata;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.callback.HoodieWriteCommitCallback;
import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage;
import org.apache.hudi.callback.util.HoodieCommitCallbackFactory;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.heartbeat.HeartbeatUtils;
import org.apache.hudi.client.utils.TransactionUtils;
import org.apache.hudi.common.HoodiePendingRollbackInfo;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieRestoreException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.exception.HoodieSavepointException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.action.InternalSchemaChangeApplier;
import org.apache.hudi.internal.schema.action.TableChange;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.internal.schema.utils.AvroSchemaEvolutionUtils;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.savepoint.SavepointHelpers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.table.upgrade.SupportsUpgradeDowngrade;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;

import com.codahale.metrics.Timer;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.common.model.HoodieCommitMetadata.SCHEMA_KEY;

/**
 * Abstract Write Client providing functionality for performing commit, index updates and rollback
 * Reused for regular write operations like upsert/insert/bulk-insert.. as well as bootstrap
 *
 * @param <T> Sub type of HoodieRecordPayload
 * @param <I> Type of inputs
 * @param <K> Type of keys
 * @param <O> Type of outputs
 */
public abstract class BaseHoodieWriteClient<T extends HoodieRecordPayload, I, K, O> extends CommonHoodieClient
    implements RunsTableService {

  protected static final String LOOKUP_STR = "lookup";
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(BaseHoodieWriteClient.class);

  private final transient HoodieIndex<?, ?> index;
  private final SupportsUpgradeDowngrade upgradeDowngradeHelper;
  private transient HoodieWriteCommitCallback commitCallback;

  protected BaseTableServiceClient<O> tableServiceClient;
  protected Option<Pair<HoodieInstant, Map<String, String>>> lastCompletedTxnAndMetadata = Option.empty();
  protected Set<String> pendingInflightAndRequestedInstants;

  /**
   * Create a write client, with new hudi index.
   * @param context HoodieEngineContext
   * @param writeConfig instance of HoodieWriteConfig
   * @param upgradeDowngradeHelper engine-specific instance of {@link SupportsUpgradeDowngrade}
   */
  @Deprecated
  public BaseHoodieWriteClient(HoodieEngineContext context,
                               HoodieWriteConfig writeConfig,
                               SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    this(context, writeConfig, Option.empty(), upgradeDowngradeHelper);
  }

  /**
   * Create a write client, allows to specify all parameters.
   *
   * @param context         HoodieEngineContext
   * @param writeConfig     instance of HoodieWriteConfig
   * @param timelineService Timeline Service that runs as part of write client.
   */
  @Deprecated
  public BaseHoodieWriteClient(HoodieEngineContext context,
                               HoodieWriteConfig writeConfig,
                               Option<EmbeddedTimelineService> timelineService,
                               SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    super(context, writeConfig, timelineService);
    this.index = createIndex(writeConfig);
    this.upgradeDowngradeHelper = upgradeDowngradeHelper;
  }

  protected abstract HoodieIndex<?, ?> createIndex(HoodieWriteConfig writeConfig);

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
    // Skip the empty commit if not allowed
    if (!config.allowEmptyCommit() && stats.isEmpty()) {
      return true;
    }
    LOG.info("Committing " + instantTime + " action " + commitActionType);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable table = createTable(config, hadoopConf);
    HoodieCommitMetadata metadata = CommitUtils.buildMetadata(stats, partitionToReplaceFileIds,
        extraMetadata, operationType, config.getWriteSchema(), commitActionType);
    HoodieInstant inflightInstant = new HoodieInstant(State.INFLIGHT, table.getMetaClient().getCommitActionType(), instantTime);
    HeartbeatUtils.abortIfHeartbeatExpired(instantTime, table, heartbeatClient, config);
    this.txnManager.beginTransaction(Option.of(inflightInstant),
        lastCompletedTxnAndMetadata.isPresent() ? Option.of(lastCompletedTxnAndMetadata.get().getLeft()) : Option.empty());
    try {
      preCommit(inflightInstant, metadata);
      commit(table, commitActionType, instantTime, metadata, stats);
      // already within lock, and so no lock requried for archival
      postCommit(table, metadata, instantTime, extraMetadata, false);
      LOG.info("Committed " + instantTime);
      releaseResources();
    } catch (IOException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + instantTime, e);
    } finally {
      this.txnManager.endTransaction(Option.of(inflightInstant));
    }
    // do this outside of lock since compaction, clustering can be time taking and we don't need a lock for the entire execution period
    runTableServicesInline(table, metadata, extraMetadata);
    emitCommitMetrics(instantTime, metadata, commitActionType);
    // callback if needed.
    if (config.writeCommitCallbackOn()) {
      if (null == commitCallback) {
        commitCallback = HoodieCommitCallbackFactory.create(config);
      }
      commitCallback.call(new HoodieWriteCommitCallbackMessage(instantTime, config.getTableName(), config.getBasePath(), stats));
    }
    return true;
  }

  protected void commit(HoodieTable table, String commitActionType, String instantTime, HoodieCommitMetadata metadata,
                      List<HoodieWriteStat> stats) throws IOException {
    LOG.info("Committing " + instantTime + " action " + commitActionType);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    // Finalize write
    finalizeWrite(table, instantTime, stats);
    // do save internal schema to support Implicitly add columns in write process
    if (!metadata.getExtraMetadata().containsKey(SerDeHelper.LATEST_SCHEMA)
        && metadata.getExtraMetadata().containsKey(SCHEMA_KEY) && table.getConfig().getSchemaEvolutionEnable()) {
      saveInternalSchema(table, instantTime, metadata);
    }
    // update Metadata table
    writeTableMetadata(table, instantTime, commitActionType, metadata);
    activeTimeline.saveAsComplete(new HoodieInstant(true, commitActionType, instantTime),
        Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
  }

  // Save internal schema
  private void saveInternalSchema(HoodieTable table, String instantTime, HoodieCommitMetadata metadata) {
    TableSchemaResolver schemaUtil = new TableSchemaResolver(table.getMetaClient());
    String historySchemaStr = schemaUtil.getTableHistorySchemaStrFromCommitMetadata().orElse("");
    FileBasedInternalSchemaStorageManager schemasManager = new FileBasedInternalSchemaStorageManager(table.getMetaClient());
    if (!historySchemaStr.isEmpty() || Boolean.parseBoolean(config.getString(HoodieCommonConfig.RECONCILE_SCHEMA.key()))) {
      InternalSchema internalSchema;
      Schema avroSchema = HoodieAvroUtils.createHoodieWriteSchema(new Schema.Parser().parse(config.getSchema()));
      if (historySchemaStr.isEmpty()) {
        internalSchema = AvroInternalSchemaConverter.convert(avroSchema);
        internalSchema.setSchemaId(Long.parseLong(instantTime));
      } else {
        internalSchema = InternalSchemaUtils.searchSchema(Long.parseLong(instantTime),
            SerDeHelper.parseSchemas(historySchemaStr));
      }
      InternalSchema evolvedSchema = AvroSchemaEvolutionUtils.reconcileSchema(avroSchema, internalSchema);
      if (evolvedSchema.equals(internalSchema)) {
        metadata.addMetadata(SerDeHelper.LATEST_SCHEMA, SerDeHelper.toJson(evolvedSchema));
        //TODO save history schema by metaTable
        schemasManager.persistHistorySchemaStr(instantTime, historySchemaStr.isEmpty() ? SerDeHelper.inheritSchemas(evolvedSchema, "") : historySchemaStr);
      } else {
        evolvedSchema.setSchemaId(Long.parseLong(instantTime));
        String newSchemaStr = SerDeHelper.toJson(evolvedSchema);
        metadata.addMetadata(SerDeHelper.LATEST_SCHEMA, newSchemaStr);
        schemasManager.persistHistorySchemaStr(instantTime, SerDeHelper.inheritSchemas(evolvedSchema, historySchemaStr));
      }
      // update SCHEMA_KEY
      metadata.addMetadata(SCHEMA_KEY, AvroInternalSchemaConverter.convert(evolvedSchema, avroSchema.getName()).toString());
    }
  }

  protected abstract HoodieTable<T, I, K, O> createTable(HoodieWriteConfig config, Configuration hadoopConf);

  void emitCommitMetrics(String instantTime, HoodieCommitMetadata metadata, String actionType) {
    if (writeTimer != null) {
      long durationInMs = metrics.getDurationInMs(writeTimer.stop());
      // instantTime could be a non-standard value, so use `parseDateFromInstantTimeSafely`
      // e.g. INIT_INSTANT_TS, METADATA_BOOTSTRAP_INSTANT_TS and FULL_BOOTSTRAP_INSTANT_TS in HoodieTimeline
      HoodieActiveTimeline.parseDateFromInstantTimeSafely(instantTime).ifPresent(parsedInstant ->
          metrics.updateCommitMetrics(parsedInstant.getTime(), durationInMs, metadata, actionType)
      );
      writeTimer = null;
    }
  }

  /**
   * Any pre-commit actions like conflict resolution goes here.
   * @param inflightInstant instant of inflight operation.
   * @param metadata commit metadata for which pre commit is being invoked.
   */
  protected void preCommit(HoodieInstant inflightInstant, HoodieCommitMetadata metadata) {
    // To be overridden by specific engines to perform conflict resolution if any.
  }

  /**
   * Write the HoodieCommitMetadata to metadata table if available.
   * @param table {@link HoodieTable} of interest.
   * @param instantTime instant time of the commit.
   * @param actionType action type of the commit.
   * @param metadata instance of {@link HoodieCommitMetadata}.
   */
  protected void writeTableMetadata(HoodieTable table, String instantTime, String actionType, HoodieCommitMetadata metadata) {
    context.setJobStatus(this.getClass().getSimpleName(), "Committing to metadata table: " + config.getTableName());
    table.getMetadataWriter(instantTime).ifPresent(w -> ((HoodieTableMetadataWriter) w).update(metadata, instantTime,
        table.isTableServiceAction(actionType)));
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
    // TODO : MULTIWRITER -> check if failed bootstrap files can be cleaned later
    if (config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl()) {
      throw new HoodieException("Cannot bootstrap the table in multi-writer mode");
    }
    HoodieTable<T, I, K, O> table = initTable(WriteOperationType.UPSERT, Option.ofNullable(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS));
    rollbackFailedBootstrap();
    table.bootstrap(context, extraMetadata);
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
   * the numbers of files with less memory compared to the {@link BaseHoodieWriteClient#insert(I, String)}
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
   * the numbers of files with less memory compared to the {@link BaseHoodieWriteClient#insert(I, String)}. Optionally
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
                               Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner);

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk loads into a Hoodie
   * table for the very first time (e.g: converting an existing table to Hoodie). The input records should contain no
   * duplicates if needed.
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and attempts to control
   * the numbers of files with less memory compared to the {@link BaseHoodieWriteClient#insert(I, String)}. Optionally
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
                                             Option<BulkInsertPartitioner> bulkInsertPartitioner);

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
   * Common method containing steps to be performed before write (upsert/insert/...
   * @param instantTime
   * @param writeOperationType
   * @param metaClient
   */
  public void preWrite(String instantTime, WriteOperationType writeOperationType,
      HoodieTableMetaClient metaClient) {
    setOperationType(writeOperationType);
    this.lastCompletedTxnAndMetadata = TransactionUtils.getLastCompletedTxnInstantAndMetadata(metaClient);
    this.pendingInflightAndRequestedInstants = TransactionUtils.getInflightAndRequestedInstants(metaClient);
    this.pendingInflightAndRequestedInstants.remove(instantTime);
    if (null == this.asyncCleanerService) {
      this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this);
    } else {
      this.asyncCleanerService.start(null);
    }
    if (null == this.asyncArchiveService) {
      this.asyncArchiveService = AsyncArchiveService.startAsyncArchiveIfEnabled(this);
    } else {
      this.asyncArchiveService.start(null);
    }
  }

  /**
   * Common method containing steps to be performed after write (upsert/insert/..) operations including auto-commit.
   * @param result  Commit Action Result
   * @param instantTime Instant Time
   * @param hoodieTable Hoodie Table
   * @return Write Status
   */
  protected abstract O postWrite(HoodieWriteMetadata<O> result, String instantTime, HoodieTable hoodieTable);

  /**
   * Post Commit Hook. Derived classes use this method to perform post-commit processing
   *
   * @param table         table to commit on
   * @param metadata      Commit Metadata corresponding to committed instant
   * @param instantTime   Instant Time
   * @param extraMetadata Additional Metadata passed by user
   * @param acquireLockForArchival true if lock has to be acquired for archival. false otherwise.
   */
  protected void postCommit(HoodieTable table, HoodieCommitMetadata metadata, String instantTime, Option<Map<String, String>> extraMetadata,
                            boolean acquireLockForArchival) {
    try {
      // Delete the marker directory for the instant.
      WriteMarkersFactory.get(config.getMarkersType(), table, instantTime)
          .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
      autoCleanOnCommit();
      autoArchiveOnCommit(table, acquireLockForArchival);
    } finally {
      this.heartbeatClient.stop(instantTime);
    }
  }

  protected void runTableServicesInline(HoodieTable table, HoodieCommitMetadata metadata, Option<Map<String, String>> extraMetadata) {
    if (!tableServicesEnabled(config)) {
      return;
    }

    if (!config.areAnyTableServicesExecutedInline() && !config.areAnyTableServicesScheduledInline()) {
      return;
    }

    tableServiceClient.runTableServicesInline(table, metadata, extraMetadata);
  }

  protected void autoCleanOnCommit() {
    if (!config.isAutoClean()) {
      return;
    }

    if (config.isAsyncClean()) {
      LOG.info("Async cleaner has been spawned. Waiting for it to finish");
      AsyncCleanerService.waitForCompletion(asyncCleanerService);
      LOG.info("Async cleaner has finished");
    } else {
      LOG.info("Start to clean synchronously.");
      // Do not reuse instantTime for clean as metadata table requires all changes to have unique instant timestamps.
      clean(true);
    }
  }

  protected void autoArchiveOnCommit(HoodieTable table, boolean acquireLockForArchival) {
    if (!config.isAutoArchive()) {
      return;
    }

    if (config.isAsyncArchive()) {
      LOG.info("Async archiver has been spawned. Waiting for it to finish");
      AsyncArchiveService.waitForCompletion(asyncArchiveService);
      LOG.info("Async archiver has finished");
    } else {
      LOG.info("Start to archive synchronously.");
      archive(table, acquireLockForArchival);
    }
  }

  /**
   * Run any pending compactions.
   */
  public void runAnyPendingCompactions() {
    tableServiceClient.runAnyPendingCompactions(createTable(config, hadoopConf));
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
    boolean initialMetadataTableIfNecessary = config.isMetadataTableEnabled();
    if (initialMetadataTableIfNecessary) {
      try {
        // Delete metadata table directly when users trigger savepoint rollback if mdt existed and beforeTimelineStarts
        String metadataTableBasePathStr = HoodieTableMetadata.getMetadataTableBasePath(config.getBasePath());
        HoodieTableMetaClient mdtClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePathStr).build();
        // Same as HoodieTableMetadataUtil#processRollbackMetadata
        HoodieInstant syncedInstant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, savepointTime);
        // The instant required to sync rollback to MDT has been archived and the mdt syncing will be failed
        // So that we need to delete the whole MDT here.
        if (mdtClient.getCommitsTimeline().isBeforeTimelineStarts(syncedInstant.getTimestamp())) {
          mdtClient.getFs().delete(new Path(metadataTableBasePathStr), true);
          // rollbackToSavepoint action will try to bootstrap MDT at first but sync to MDT will fail at the current scenario.
          // so that we need to disable metadata initialized here.
          initialMetadataTableIfNecessary = false;
        }
      } catch (Exception e) {
        // Metadata directory does not exist
      }
    }

    HoodieTable<T, I, K, O> table = initTable(WriteOperationType.UNKNOWN, Option.empty(), initialMetadataTableIfNecessary);
    SavepointHelpers.validateSavepointPresence(table, savepointTime);
    ValidationUtils.checkArgument(!config.shouldArchiveBeyondSavepoint(), "Restore is not supported when " + HoodieArchivalConfig.ARCHIVE_BEYOND_SAVEPOINT.key()
        + " is enabled");
    restoreToInstant(savepointTime, initialMetadataTableIfNecessary);
    SavepointHelpers.validateSavepointRestore(table, savepointTime);
  }

  @Deprecated
  public boolean rollback(final String commitInstantTime) throws HoodieRollbackException {
    HoodieTable<T, I, K, O> table = initTable(WriteOperationType.UNKNOWN, Option.empty());
    Option<HoodiePendingRollbackInfo> pendingRollbackInfo = getPendingRollbackInfo(table.getMetaClient(), commitInstantTime);
    return rollback(commitInstantTime, pendingRollbackInfo, false);
  }

  /**
   * NOTE : This action requires all writers (ingest and compact) to a table to be stopped before proceeding. Revert
   * the (inflight/committed) record changes for all commits after the provided instant time.
   *
   * @param instantTime Instant time to which restoration is requested
   */
  public HoodieRestoreMetadata restoreToInstant(final String instantTime, boolean initialMetadataTableIfNecessary) throws HoodieRestoreException {
    LOG.info("Begin restore to instant " + instantTime);
    final String restoreInstantTime = HoodieActiveTimeline.createNewInstantTime();
    Timer.Context timerContext = metrics.getRollbackCtx();
    try {
      HoodieTable<T, I, K, O> table = initTable(WriteOperationType.UNKNOWN, Option.of(restoreInstantTime), initialMetadataTableIfNecessary);
      Option<HoodieRestorePlan> restorePlanOption = table.scheduleRestore(context, restoreInstantTime, instantTime);
      if (restorePlanOption.isPresent()) {
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
      } else {
        throw new HoodieRestoreException("Failed to restore " + config.getBasePath() + " to commit " + instantTime);
      }
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
    return clean(cleanInstantTime, true, false);
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   * @param cleanInstantTime instant time for clean.
   * @param skipLocking if this is triggered by another parent transaction, locking can be skipped.
   * @return instance of {@link HoodieCleanMetadata}.
   */
  public HoodieCleanMetadata clean(String cleanInstantTime, boolean skipLocking) throws HoodieIOException {
    return clean(cleanInstantTime, true, skipLocking);
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned). This API provides the flexibility to schedule clean instant asynchronously via
   * {@link BaseHoodieWriteClient#scheduleTableService(String, Option, TableServiceType)} and disable inline scheduling
   * of clean.
   * @param cleanInstantTime instant time for clean.
   * @param scheduleInline true if needs to be scheduled inline. false otherwise.
   * @param skipLocking if this is triggered by another parent transaction, locking can be skipped.
   */
  public HoodieCleanMetadata clean(String cleanInstantTime, boolean scheduleInline, boolean skipLocking) throws HoodieIOException {
    return tableServiceClient.clean(cleanInstantTime, scheduleInline, skipLocking);
  }

  public HoodieCleanMetadata clean() {
    return clean(false);
  }

  /**
   * Triggers clean for the table. This refers to Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   *    * configurations and CleaningPolicy used.
   * @param skipLocking if this is triggered by another parent transaction, locking can be skipped.
   * @return instance of {@link HoodieCleanMetadata}.
   */
  public HoodieCleanMetadata clean(boolean skipLocking) {
    return clean(HoodieActiveTimeline.createNewInstantTime(), skipLocking);
  }

  /**
   * Trigger archival for the table. This ensures that the number of commits do not explode
   * and keep increasing unbounded over time.
   * @param table table to commit on.
   * @param acquireLockForArchival true if lock has to be acquired for archival. false otherwise.
   */
  protected void archive(HoodieTable table, boolean acquireLockForArchival) {
    if (!tableServicesEnabled(config)) {
      return;
    }
    try {
      // We cannot have unbounded commit files. Archive commits if we have to archive
      HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(config, table);
      archiver.archiveIfRequired(context, acquireLockForArchival);
    } catch (IOException ioe) {
      throw new HoodieIOException("Failed to archive", ioe);
    }
  }

  /**
   * Trigger archival for the table. This ensures that the number of commits do not explode
   * and keep increasing unbounded over time.
   */
  public void archive() {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable table = createTable(config, hadoopConf);
    archive(table, true);
  }

  /**
   * Provides a new commit time for a write operation (insert/update/delete).
   */
  public String startCommit() {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    return startCommit(metaClient.getCommitActionType(), metaClient);
  }

  /**
   * Provides a new commit time for a write operation (insert/update/delete/insert_overwrite/insert_overwrite_table) with specified action.
   */
  public String startCommit(String actionType, HoodieTableMetaClient metaClient) {
    CleanerUtils.rollbackFailedWrites(config.getFailedWritesCleanPolicy(),
        HoodieTimeline.COMMIT_ACTION, () -> rollbackFailedWrites());
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    startCommit(instantTime, actionType, metaClient);
    return instantTime;
  }

  /**
   * Provides a new commit time for a write operation (insert/update/delete/insert_overwrite/insert_overwrite_table) without specified action.
   * @param instantTime Instant time to be generated
   */
  public void startCommitWithTime(String instantTime) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    startCommitWithTime(instantTime, metaClient.getCommitActionType(), metaClient);
  }

  /**
   * Completes a new commit time for a write operation (insert/update/delete/insert_overwrite/insert_overwrite_table) with specified action.
   */
  public void startCommitWithTime(String instantTime, String actionType) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    startCommitWithTime(instantTime, actionType, metaClient);
  }

  /**
   * Completes a new commit time for a write operation (insert/update/delete) with specified action.
   */
  private void startCommitWithTime(String instantTime, String actionType, HoodieTableMetaClient metaClient) {
    CleanerUtils.rollbackFailedWrites(config.getFailedWritesCleanPolicy(),
        HoodieTimeline.COMMIT_ACTION, () -> rollbackFailedWrites());
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
    if (config.getFailedWritesCleanPolicy().isLazy()) {
      this.heartbeatClient.start(instantTime);
    }

    if (actionType.equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
      metaClient.getActiveTimeline().createRequestedReplaceCommit(instantTime, actionType);
    } else {
      metaClient.getActiveTimeline().createNewInstant(new HoodieInstant(HoodieInstant.State.REQUESTED, actionType,
              instantTime));
    }
  }

  /**
   * Schedules a new compaction instant.
   * @param extraMetadata Extra Metadata to be stored
   */
  public Option<String> scheduleCompaction(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    return scheduleCompactionAtInstant(instantTime, extraMetadata) ? Option.of(instantTime) : Option.empty();
  }

  /**
   * Schedules a new compaction instant with passed-in instant time.
   * @param instantTime Compaction Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  public boolean scheduleCompactionAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return scheduleTableService(instantTime, extraMetadata, TableServiceType.COMPACT).isPresent();
  }

  /**
   * Schedules INDEX action.
   *
   * @param partitionTypes - list of {@link MetadataPartitionType} which needs to be indexed
   * @return instant time for the requested INDEX action
   */
  public Option<String> scheduleIndexing(List<MetadataPartitionType> partitionTypes) {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    Option<HoodieIndexPlan> indexPlan = createTable(config, hadoopConf)
        .scheduleIndexing(context, instantTime, partitionTypes);
    return indexPlan.isPresent() ? Option.of(instantTime) : Option.empty();
  }

  /**
   * Runs INDEX action to build out the metadata partitions as planned for the given instant time.
   *
   * @param indexInstantTime - instant time for the requested INDEX action
   * @return {@link Option<HoodieIndexCommitMetadata>} after successful indexing.
   */
  public Option<HoodieIndexCommitMetadata> index(String indexInstantTime) {
    return createTable(config, hadoopConf).index(context, indexInstantTime);
  }

  /**
   * Drops the index and removes the metadata partitions.
   *
   * @param partitionTypes - list of {@link MetadataPartitionType} which needs to be indexed
   */
  public void dropIndex(List<MetadataPartitionType> partitionTypes) {
    HoodieTable table = createTable(config, hadoopConf);
    String dropInstant = HoodieActiveTimeline.createNewInstantTime();
    HoodieInstant ownerInstant = new HoodieInstant(true, HoodieTimeline.INDEXING_ACTION, dropInstant);
    this.txnManager.beginTransaction(Option.of(ownerInstant), Option.empty());
    try {
      context.setJobStatus(this.getClass().getSimpleName(), "Dropping partitions from metadata table: " + config.getTableName());
      table.getMetadataWriter(dropInstant).ifPresent(w -> {
        try {
          ((HoodieTableMetadataWriter) w).dropMetadataPartitions(partitionTypes);
        } catch (IOException e) {
          throw new HoodieIndexException("Failed to drop metadata index. ", e);
        }
      });
    } finally {
      this.txnManager.endTransaction(Option.of(ownerInstant));
    }
  }

  /**
   * Performs Clustering for the workload stored in instant-time.
   *
   * @param clusteringInstantTime Clustering Instant Time
   * @return Collection of WriteStatus to inspect errors and counts
   */
  public HoodieWriteMetadata<O> cluster(String clusteringInstantTime) {
    if (delegateToTableManagerService(config, ActionType.replacecommit)) {
      throw new HoodieException(ActionType.replacecommit.name() + " delegate to table management service!");
    }
    return cluster(clusteringInstantTime, true);
  }

  /**
   * Performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @return Collection of WriteStatus to inspect errors and counts
   */
  public HoodieWriteMetadata<O> compact(String compactionInstantTime) {
    if (delegateToTableManagerService(config, ActionType.compaction)) {
      throw new HoodieException(ActionType.compaction.name() + " delegate to table management service!");
    }
    return compact(compactionInstantTime, config.shouldAutoCommit());
  }

  /**
   * Commit a compaction operation. Allow passing additional meta-data to be stored in commit instant file.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @param metadata All the metadata that gets stored along with a commit
   * @param extraMetadata Extra Metadata to be stored
   */
  public abstract void commitCompaction(String compactionInstantTime, HoodieCommitMetadata metadata,
                                        Option<Map<String, String>> extraMetadata);

  /**
   * Commit Compaction and track metrics.
   */
  protected abstract void completeCompaction(HoodieCommitMetadata metadata, HoodieTable table, String compactionCommitTime);

  /**
   * Ensures compaction instant is in expected state and performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @return Collection of Write Status
   */
  protected abstract HoodieWriteMetadata<O> compact(String compactionInstantTime, boolean shouldComplete);

  /**
   * Performs a compaction operation on a table, serially before or after an insert/upsert action.
   * Scheduling and execution is done inline.
   */
  protected Option<String> inlineCompaction(Option<Map<String, String>> extraMetadata) {
    Option<String> compactionInstantTimeOpt = inlineScheduleCompaction(extraMetadata);
    compactionInstantTimeOpt.ifPresent(compactInstantTime -> {
      // inline compaction should auto commit as the user is never given control
      compact(compactInstantTime, true);
    });
    return compactionInstantTimeOpt;
  }

  /***
   * Schedules compaction inline.
   * @param extraMetadata extrametada to be used.
   * @return compaction instant if scheduled.
   */
  protected Option<String> inlineScheduleCompaction(Option<Map<String, String>> extraMetadata) {
    return scheduleCompaction(extraMetadata);
  }

  /**
   * Schedules a new clustering instant.
   * @param extraMetadata Extra Metadata to be stored
   */
  public Option<String> scheduleClustering(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    return scheduleClusteringAtInstant(instantTime, extraMetadata) ? Option.of(instantTime) : Option.empty();
  }

  /**
   * Schedules a new clustering instant with passed-in instant time.
   * @param instantTime clustering Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  public boolean scheduleClusteringAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return scheduleTableService(instantTime, extraMetadata, TableServiceType.CLUSTER).isPresent();
  }

  /**
   * Schedules a new cleaning instant.
   * @param extraMetadata Extra Metadata to be stored
   */
  protected Option<String> scheduleCleaning(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    return scheduleCleaningAtInstant(instantTime, extraMetadata) ? Option.of(instantTime) : Option.empty();
  }

  /**
   * Schedules a new cleaning instant with passed-in instant time.
   * @param instantTime cleaning Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  protected boolean scheduleCleaningAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return scheduleTableService(instantTime, extraMetadata, TableServiceType.CLEAN).isPresent();
  }

  /**
   * Ensures clustering instant is in expected state and performs clustering for the plan stored in metadata.
   * @param clusteringInstant Clustering Instant Time
   * @return Collection of Write Status
   */
  public abstract HoodieWriteMetadata<O> cluster(String clusteringInstant, boolean shouldComplete);

  /**
   * Schedule table services such as clustering, compaction & cleaning.
   *
   * @param extraMetadata Metadata to pass onto the scheduled service instant
   * @param tableServiceType Type of table service to schedule
   * @return
   */
  public Option<String> scheduleTableService(Option<Map<String, String>> extraMetadata, TableServiceType tableServiceType) {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    return scheduleTableService(instantTime, extraMetadata, tableServiceType);
  }

  /**
   * Schedule table services such as clustering, compaction & cleaning.
   *
   * @param extraMetadata Metadata to pass onto the scheduled service instant
   * @param tableServiceType Type of table service to schedule
   * @return
   */
  public Option<String> scheduleTableService(String instantTime, Option<Map<String, String>> extraMetadata,
                                             TableServiceType tableServiceType) {
    // A lock is required to guard against race conditions between an on-going writer and scheduling a table service.
    final Option<HoodieInstant> inflightInstant = Option.of(new HoodieInstant(HoodieInstant.State.REQUESTED,
        tableServiceType.getAction(), instantTime));
    try {
      this.txnManager.beginTransaction(inflightInstant, Option.empty());
      LOG.info("Scheduling table service " + tableServiceType);
      return tableServiceClient.scheduleTableServiceInternal(instantTime, extraMetadata, tableServiceType);
    } finally {
      this.txnManager.endTransaction(inflightInstant);
    }
  }

  /**
   * Executes a clustering plan on a table, serially before or after an insert/upsert action.
   * Schedules and executes clustering inline.
   */
  protected Option<String> inlineClustering(Option<Map<String, String>> extraMetadata) {
    Option<String> clusteringInstantOpt = inlineScheduleClustering(extraMetadata);
    clusteringInstantOpt.ifPresent(clusteringInstant -> {
      // inline cluster should auto commit as the user is never given control
      cluster(clusteringInstant, true);
    });
    return clusteringInstantOpt;
  }

  /**
   * Schedules clustering inline.
   * @param extraMetadata extrametadata to use.
   * @return clustering instant if scheduled.
   */
  protected Option<String> inlineScheduleClustering(Option<Map<String, String>> extraMetadata) {
    return scheduleClustering(extraMetadata);
  }

  /**
   * Finalize Write operation.
   *
   * @param table HoodieTable
   * @param instantTime Instant Time
   * @param stats Hoodie Write Stat
   */
  protected void finalizeWrite(HoodieTable table, String instantTime, List<HoodieWriteStat> stats) {
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

  public HoodieIndex<?, ?> getIndex() {
    return index;
  }

  /**
   * Instantiates engine-specific instance of {@link HoodieTable} as well as performs necessary
   * bootstrapping operations (for ex, validating whether Metadata Table has to be bootstrapped)
   *
   * NOTE: THIS OPERATION IS EXECUTED UNDER LOCK, THEREFORE SHOULD AVOID ANY OPERATIONS
   *       NOT REQUIRING EXTERNAL SYNCHRONIZATION
   *
   * @param metaClient instance of {@link HoodieTableMetaClient}
   * @param instantTime current inflight instant time
   * @return instantiated {@link HoodieTable}
   */
  protected abstract HoodieTable doInitTable(HoodieTableMetaClient metaClient, Option<String> instantTime, boolean initialMetadataTableIfNecessary);

  /**
   * Instantiates and initializes instance of {@link HoodieTable}, performing crucial bootstrapping
   * operations such as:
   *
   * NOTE: This method is engine-agnostic and SHOULD NOT be overloaded, please check on
   * {@link #doInitTable(HoodieTableMetaClient, Option, boolean)} instead
   *
   * <ul>
   *   <li>Checking whether upgrade/downgrade is required</li>
   *   <li>Bootstrapping Metadata Table (if required)</li>
   *   <li>Initializing metrics contexts</li>
   * </ul>
   */
  protected final HoodieTable initTable(WriteOperationType operationType, Option<String> instantTime, boolean initialMetadataTableIfNecessary) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    // Setup write schemas for deletes
    if (operationType == WriteOperationType.DELETE) {
      setWriteSchemaForDeletes(metaClient);
    }

    HoodieTable table;
    Option<HoodieInstant> ownerInstant = Option.empty();
    if (instantTime.isPresent()) {
      ownerInstant = Option.of(new HoodieInstant(true, CommitUtils.getCommitActionType(operationType, metaClient.getTableType()), instantTime.get()));
    }
    this.txnManager.beginTransaction(ownerInstant, Option.empty());
    try {
      tryUpgrade(metaClient, instantTime);
      table = doInitTable(metaClient, instantTime, initialMetadataTableIfNecessary);
    } finally {
      this.txnManager.endTransaction(ownerInstant);
    }

    // Validate table properties
    metaClient.validateTableProperties(config.getProps());

    switch (operationType) {
      case INSERT:
      case INSERT_PREPPED:
      case UPSERT:
      case UPSERT_PREPPED:
      case BULK_INSERT:
      case BULK_INSERT_PREPPED:
      case INSERT_OVERWRITE:
      case INSERT_OVERWRITE_TABLE:
        setWriteTimer(table);
        break;
      case CLUSTER:
        clusteringTimer = metrics.getClusteringCtx();
        break;
      case COMPACT:
        compactionTimer = metrics.getCompactionCtx();
        break;
      default:
    }

    return table;
  }

  protected final HoodieTable initTable(WriteOperationType operationType, Option<String> instantTime) {
    return initTable(operationType, instantTime, config.isMetadataTableEnabled());
  }

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
        if (commitMetadata.getExtraMetadata().containsKey(SCHEMA_KEY)) {
          config.setSchema(commitMetadata.getExtraMetadata().get(SCHEMA_KEY));
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

  /**
   * Called after each write, to release any resources used.
   */
  protected void releaseResources() {
    // do nothing here
  }

  @Override
  public void close() {
    AsyncArchiveService.forceShutdown(asyncArchiveService);
    asyncArchiveService = null;
    AsyncCleanerService.forceShutdown(asyncCleanerService);
    asyncCleanerService = null;
    // Stop timeline-server if running
    super.close();
    // Calling this here releases any resources used by your index, so make sure to finish any related operations
    // before this point
    this.index.close();
    this.heartbeatClient.stop();
    this.txnManager.close();
  }

  private void setWriteTimer(HoodieTable table) {
    String commitType = table.getMetaClient().getCommitActionType();
    if (commitType.equals(HoodieTimeline.COMMIT_ACTION)) {
      writeTimer = metrics.getCommitCtx();
    } else if (commitType.equals(HoodieTimeline.DELTA_COMMIT_ACTION)) {
      writeTimer = metrics.getDeltaCommitCtx();
    }
  }

  protected void tryUpgrade(HoodieTableMetaClient metaClient, Option<String> instantTime) {
    UpgradeDowngrade upgradeDowngrade =
        new UpgradeDowngrade(metaClient, config, context, upgradeDowngradeHelper);

    if (upgradeDowngrade.needsUpgradeOrDowngrade(HoodieTableVersion.current())) {
      metaClient = HoodieTableMetaClient.reload(metaClient);
      // Ensure no inflight commits by setting EAGER policy and explicitly cleaning all failed commits
      List<String> instantsToRollback = getInstantsToRollback(metaClient, HoodieFailedWritesCleaningPolicy.EAGER, instantTime);

      if (!instantsToRollback.isEmpty()) {
        Map<String, Option<HoodiePendingRollbackInfo>> pendingRollbacks = getPendingRollbackInfos(metaClient);
        instantsToRollback.forEach(entry -> pendingRollbacks.putIfAbsent(entry, Option.empty()));

        rollbackFailedWrites(pendingRollbacks, true);
      }

      new UpgradeDowngrade(metaClient, config, context, upgradeDowngradeHelper)
          .run(HoodieTableVersion.current(), instantTime.orElse(null));

      metaClient.reloadActiveTimeline();
    }
  }

  /**
   * add columns to table.
   *
   * @param colName col name to be added. if we want to add col to a nested filed, the fullName should be specify
   * @param schema col type to be added.
   * @param doc col doc to be added.
   * @param position col position to be added
   * @param positionType col position change type. now support three change types: first/after/before
   */
  public void addColumn(String colName, Schema schema, String doc, String position, TableChange.ColumnPositionChange.ColumnPositionType positionType) {
    Pair<InternalSchema, HoodieTableMetaClient> pair = getInternalSchemaAndMetaClient();
    InternalSchema newSchema = new InternalSchemaChangeApplier(pair.getLeft())
        .applyAddChange(colName, AvroInternalSchemaConverter.convertToField(schema), doc, position, positionType);
    commitTableChange(newSchema, pair.getRight());
  }

  public void addColumn(String colName, Schema schema) {
    addColumn(colName, schema, null, "", TableChange.ColumnPositionChange.ColumnPositionType.NO_OPERATION);
  }

  /**
   * delete columns to table.
   *
   * @param colNames col name to be deleted. if we want to delete col from a nested filed, the fullName should be specify
   */
  public void deleteColumns(String... colNames) {
    Pair<InternalSchema, HoodieTableMetaClient> pair = getInternalSchemaAndMetaClient();
    InternalSchema newSchema = new InternalSchemaChangeApplier(pair.getLeft()).applyDeleteChange(colNames);
    commitTableChange(newSchema, pair.getRight());
  }

  /**
   * rename col name for hudi table.
   *
   * @param colName col name to be renamed. if we want to rename col from a nested filed, the fullName should be specify
   * @param newName new name for current col. no need to specify fullName.
   */
  public void renameColumn(String colName, String newName) {
    Pair<InternalSchema, HoodieTableMetaClient> pair = getInternalSchemaAndMetaClient();
    InternalSchema newSchema = new InternalSchemaChangeApplier(pair.getLeft()).applyRenameChange(colName, newName);
    commitTableChange(newSchema, pair.getRight());
  }

  /**
   * update col nullable attribute for hudi table.
   *
   * @param colName col name to be changed. if we want to change col from a nested filed, the fullName should be specify
   * @param nullable .
   */
  public void updateColumnNullability(String colName, boolean nullable) {
    Pair<InternalSchema, HoodieTableMetaClient> pair = getInternalSchemaAndMetaClient();
    InternalSchema newSchema = new InternalSchemaChangeApplier(pair.getLeft()).applyColumnNullabilityChange(colName, nullable);
    commitTableChange(newSchema, pair.getRight());
  }

  /**
   * update col Type for hudi table.
   * only support update primitive type to primitive type.
   * cannot update nest type to nest type or primitive type eg: RecordType -> MapType, MapType -> LongType.
   *
   * @param colName col name to be changed. if we want to change col from a nested filed, the fullName should be specify
   * @param newType .
   */
  public void updateColumnType(String colName, Type newType) {
    Pair<InternalSchema, HoodieTableMetaClient> pair = getInternalSchemaAndMetaClient();
    InternalSchema newSchema = new InternalSchemaChangeApplier(pair.getLeft()).applyColumnTypeChange(colName, newType);
    commitTableChange(newSchema, pair.getRight());
  }

  /**
   * update col comment for hudi table.
   *
   * @param colName col name to be changed. if we want to change col from a nested filed, the fullName should be specify
   * @param doc .
   */
  public void updateColumnComment(String colName, String doc) {
    Pair<InternalSchema, HoodieTableMetaClient> pair = getInternalSchemaAndMetaClient();
    InternalSchema newSchema = new InternalSchemaChangeApplier(pair.getLeft()).applyColumnCommentChange(colName, doc);
    commitTableChange(newSchema, pair.getRight());
  }

  /**
   * reorder the position of col.
   *
   * @param colName column which need to be reordered. if we want to change col from a nested filed, the fullName should be specify.
   * @param referColName reference position.
   * @param orderType col position change type. now support three change types: first/after/before
   */
  public void reOrderColPosition(String colName, String referColName, TableChange.ColumnPositionChange.ColumnPositionType orderType) {
    if (colName == null || orderType == null || referColName == null) {
      return;
    }
    //get internalSchema
    Pair<InternalSchema, HoodieTableMetaClient> pair = getInternalSchemaAndMetaClient();
    InternalSchema newSchema = new InternalSchemaChangeApplier(pair.getLeft())
        .applyReOrderColPositionChange(colName, referColName, orderType);
    commitTableChange(newSchema, pair.getRight());
  }

  private Pair<InternalSchema, HoodieTableMetaClient> getInternalSchemaAndMetaClient() {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
    Option<InternalSchema> internalSchemaOption = schemaUtil.getTableInternalSchemaFromCommitMetadata();
    if (!internalSchemaOption.isPresent()) {
      throw new HoodieException(String.format("cannot find schema for current table: %s", config.getBasePath()));
    }
    return Pair.of(internalSchemaOption.get(), metaClient);
  }

  private void commitTableChange(InternalSchema newSchema, HoodieTableMetaClient metaClient) {
    TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
    String historySchemaStr = schemaUtil.getTableHistorySchemaStrFromCommitMetadata().orElse("");
    Schema schema = AvroInternalSchemaConverter.convert(newSchema, config.getTableName());
    String commitActionType = CommitUtils.getCommitActionType(WriteOperationType.ALTER_SCHEMA, metaClient.getTableType());
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    startCommitWithTime(instantTime, commitActionType, metaClient);
    config.setSchema(schema.toString());
    HoodieActiveTimeline timeLine = metaClient.getActiveTimeline();
    HoodieInstant requested = new HoodieInstant(State.REQUESTED, commitActionType, instantTime);
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.setOperationType(WriteOperationType.ALTER_SCHEMA);
    try {
      timeLine.transitionRequestedToInflight(requested, Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException io) {
      throw new HoodieCommitException("Failed to commit " + instantTime + " unable to save inflight metadata ", io);
    }
    Map<String, String> extraMeta = new HashMap<>();
    extraMeta.put(SerDeHelper.LATEST_SCHEMA, SerDeHelper.toJson(newSchema.setSchemaId(Long.getLong(instantTime))));
    // try to save history schemas
    FileBasedInternalSchemaStorageManager schemasManager = new FileBasedInternalSchemaStorageManager(metaClient);
    schemasManager.persistHistorySchemaStr(instantTime, SerDeHelper.inheritSchemas(newSchema, historySchemaStr));
    commitStats(instantTime, Collections.emptyList(), Option.of(extraMeta), commitActionType);
  }
}
