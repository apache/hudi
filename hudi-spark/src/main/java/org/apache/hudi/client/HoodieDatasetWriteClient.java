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
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.utils.ClientUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRollingStat;
import org.apache.hudi.common.model.HoodieRollingStatMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.HoodieTimelineArchiveLog;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.hudi.table.action.HoodieDatasetWriteMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Client for performing data set operations.
 */
public class HoodieDatasetWriteClient<T extends HoodieRecordPayload> implements Serializable,
    AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(HoodieDatasetWriteClient.class);

  private static final long serialVersionUID = 1L;

  private final transient HoodieMetrics metrics;
  private final transient HoodieIndex<T> index;

  private transient Timer.Context writeContext = null;
  private transient WriteOperationType operationType;

  protected final transient FileSystem fs;
  protected final transient JavaSparkContext jsc;
  protected final HoodieWriteConfig config;
  protected final String basePath;
  private final boolean rollbackPending;

  /**
   * Create a write client, with new hudi index.
   *
   * @param jsc Java Spark Context
   * @param clientConfig instance of HoodieWriteConfig
   * @param rollbackPending whether need to cleanup pending commits
   */
  public HoodieDatasetWriteClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig,
      boolean rollbackPending) {
    this(jsc, clientConfig, rollbackPending, HoodieIndex.createIndex(clientConfig, jsc));
  }

  public HoodieDatasetWriteClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig,
      boolean rollbackPending, HoodieIndex index) {
    this.fs = FSUtils.getFs(clientConfig.getBasePath(), jsc.hadoopConfiguration());
    this.jsc = jsc;
    this.basePath = clientConfig.getBasePath();
    this.config = clientConfig;
    this.metrics = new HoodieMetrics(clientConfig, clientConfig.getTableName());
    this.index = index;
    this.rollbackPending = rollbackPending;
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
    metaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant()
        .ifPresent(latestPending ->
            ValidationUtils.checkArgument(
                HoodieTimeline
                    .compareTimestamps(latestPending.getTimestamp(), HoodieTimeline.LESSER_THAN,
                        instantTime),
                "Latest pending compaction instant time must be earlier than this instant time. Latest Compaction :"
                    + latestPending + ",  Ingesting at " + instantTime));
    HoodieTable<T> table = HoodieTable.create(metaClient, config, jsc);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    String commitActionType = table.getMetaClient().getCommitActionType();
    activeTimeline
        .createNewInstant(new HoodieInstant(State.REQUESTED, commitActionType, instantTime));
  }

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk
   * loads into a Hoodie table for the very first time (e.g: converting an existing table to
   * Hoodie).
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and
   * attempts to control the numbers of files with less memory compared to the {@link
   * HoodieWriteClient#insert(JavaRDD, String)}
   *
   * @param records HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public Dataset<EncodableWriteStatus> bulkInsertDataset(Dataset<Row> records,
      final String instantTime) {
    return bulkInsertDataset(records, instantTime, Option.empty());
  }

  public Dataset<EncodableWriteStatus> bulkInsertDataset(Dataset<Row> records,
      final String instantTime, Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    HoodieTable<T> table = getTableAndInitCtx(WriteOperationType.BULK_INSERT);
    table.validateInsertSchema();
    setOperationType(WriteOperationType.BULK_INSERT);
    HoodieDatasetWriteMetadata result = table
        .bulkInsertDataset(jsc, instantTime, records, bulkInsertPartitioner);
    return postWriteDataset(result, instantTime, table);
  }

  /**
   * Common method containing steps to be performed after write (upsert/insert/..) operations
   * including auto-commit.
   *
   * @param result Commit Action Result
   * @param instantTime Instant Time
   * @param hoodieTable Hoodie Table
   * @return Write Status
   */
  private Dataset<EncodableWriteStatus> postWriteDataset(HoodieDatasetWriteMetadata result,
      String instantTime, HoodieTable<T> hoodieTable) {
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(getOperationType().name(),
          result.getIndexUpdateDuration().get().toMillis());
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
    return result.getEncodableWriteStatuses();
  }

  /**
   * Commit changes performed at the given instantTime marker.
   */
  public boolean commitDataset(String instantTime, Dataset<EncodableWriteStatus> encWriteStatuses) {
    return commitDataset(instantTime, encWriteStatuses, Option.empty());
  }

  public boolean commitDataset(String instantTime, Dataset<EncodableWriteStatus> encWriteStatuses,
      Option<Map<String, String>> extraMetadata) {
    HoodieTableMetaClient metaClient = createMetaClient(false);
    return commitDataset(instantTime, encWriteStatuses, extraMetadata,
        metaClient.getCommitActionType());
  }

  private boolean commitDataset(String instantTime, Dataset<EncodableWriteStatus> encWriteStatuses,
      Option<Map<String, String>> extraMetadata, String actionType) {
    LOG.info("Committing " + instantTime);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.create(config, jsc);

    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    List<HoodieWriteStat> stats = encWriteStatuses.toJavaRDD().map(EncodableWriteStatus::getStat)
        .collect();

    updateMetadataAndRollingStats(actionType, metadata, stats);

    // Finalize write
    finalizeWrite(table, instantTime, stats);

    // add in extra metadata
    if (extraMetadata.isPresent()) {
      extraMetadata.get().forEach(metadata::addMetadata);
    }
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, config.getSchema());
    metadata.setOperationType(getOperationType());

    try {
      activeTimeline.saveAsComplete(new HoodieInstant(true, actionType, instantTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
      postCommit(metadata, instantTime, extraMetadata);
      emitCommitMetrics(instantTime, metadata, actionType);
      LOG.info("Committed " + instantTime);
    } catch (IOException e) {
      throw new HoodieCommitException(
          "Failed to complete commit " + config.getBasePath() + " at time " + instantTime,
          e);
    }
    return true;
  }

  void emitCommitMetrics(String instantTime, HoodieCommitMetadata metadata, String actionType) {
    try {

      if (writeContext != null) {
        long durationInMs = metrics.getDurationInMs(writeContext.stop());
        metrics
            .updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(instantTime).getTime(),
                durationInMs,
                metadata, actionType);
        writeContext = null;
      }
    } catch (ParseException e) {
      throw new HoodieCommitException(
          "Failed to complete commit " + config.getBasePath() + " at time " + instantTime
              + "Instant time is not of valid format", e);
    }
  }

  protected HoodieTableMetaClient createMetaClient(boolean loadActiveTimelineOnLoad) {
    return ClientUtils.createMetaClient(jsc, config, loadActiveTimelineOnLoad);
  }

  protected void updateMetadataAndRollingStats(String actionType, HoodieCommitMetadata metadata,
      List<HoodieWriteStat> writeStats) {
    // TODO : make sure we cannot rollback / archive last commit file
    try {
      // Create a Hoodie table which encapsulated the commits and files visible
      HoodieTable table = HoodieTable.create(config, jsc);
      // 0. All of the rolling stat management is only done by the DELTA commit for MOR and COMMIT for COW other wise
      // there may be race conditions
      HoodieRollingStatMetadata rollingStatMetadata = new HoodieRollingStatMetadata(actionType);
      // 1. Look up the previous compaction/commit and get the HoodieCommitMetadata from there.
      // 2. Now, first read the existing rolling stats and merge with the result of current metadata.

      // Need to do this on every commit (delta or commit) to support COW and MOR.

      for (HoodieWriteStat stat : writeStats) {
        String partitionPath = stat.getPartitionPath();
        // TODO: why is stat.getPartitionPath() null at times here.
        metadata.addWriteStat(partitionPath, stat);
        HoodieRollingStat hoodieRollingStat = new HoodieRollingStat(stat.getFileId(),
            stat.getNumWrites() - (stat.getNumUpdateWrites() - stat.getNumDeletes()),
            stat.getNumUpdateWrites(),
            stat.getNumDeletes(), stat.getTotalWriteBytes());
        rollingStatMetadata.addRollingStat(partitionPath, hoodieRollingStat);
      }
      // The last rolling stat should be present in the completed timeline
      Option<HoodieInstant> lastInstant =
          table.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant();
      if (lastInstant.isPresent()) {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            table.getActiveTimeline().getInstantDetails(lastInstant.get()).get(),
            HoodieCommitMetadata.class);
        Option<String> lastRollingStat = Option
            .ofNullable(commitMetadata.getExtraMetadata()
                .get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY));
        if (lastRollingStat.isPresent()) {
          rollingStatMetadata = rollingStatMetadata
              .merge(HoodieCommitMetadata
                  .fromBytes(lastRollingStat.get().getBytes(), HoodieRollingStatMetadata.class));
        }
      }
      metadata.addMetadata(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY,
          rollingStatMetadata.toJsonString());
    } catch (IOException io) {
      throw new HoodieCommitException("Unable to save rolling stats");
    }
  }

  public void setOperationType(WriteOperationType operationType) {
    this.operationType = operationType;
  }

  public WriteOperationType getOperationType() {
    return this.operationType;
  }

  protected HoodieTable getTableAndInitCtx(WriteOperationType operationType) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable table = HoodieTable.create(metaClient, config, jsc);
    if (table.getMetaClient().getCommitActionType().equals(HoodieTimeline.COMMIT_ACTION)) {
      writeContext = metrics.getCommitCtx();
    } else {
      writeContext = metrics.getDeltaCommitCtx();
    }
    return table;
  }

  private void postCommit(HoodieCommitMetadata metadata, String instantTime,
      Option<Map<String, String>> extraMetadata) {
    try {
      metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "false");
      // We cannot have unbounded commit files. Archive commits if we have to archive
      HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(config,
          createMetaClient(true));
      archiveLog.archiveIfRequired(jsc);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Finalize Write operation.
   *
   * @param table HoodieTable
   * @param instantTime Instant Time
   * @param stats Hoodie Write Stat
   */
  protected void finalizeWrite(HoodieTable<T> table, String instantTime,
      List<HoodieWriteStat> stats) {
    try {
      final Timer.Context finalizeCtx = metrics.getFinalizeCtx();
      table.finalizeWrite(jsc, instantTime, stats);
      if (finalizeCtx != null) {
        Option<Long> durationInMs = Option.of(metrics.getDurationInMs(finalizeCtx.stop()));
        durationInMs.ifPresent(duration -> {
          LOG.info("Finalize write elapsed time (milliseconds): " + duration);
          metrics.updateFinalizeWriteMetrics(duration, stats.size());
        });
      }
    } catch (HoodieIOException ioe) {
      throw new HoodieCommitException(
          "Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
  }

  @Override
  public void close() throws Exception {
    // noop for now
  }

  /**
   * Cleanup all pending commits.
   */
  private void rollbackPendingCommits() {
    HoodieTable<T> table = HoodieTable.create(config, jsc);
    HoodieTimeline inflightTimeline = table.getMetaClient().getCommitsTimeline()
        .filterPendingExcludingCompaction();
    List<String> commits = inflightTimeline.getReverseOrderedInstants()
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    for (String commit : commits) {
      rollback(commit);
    }
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
      HoodieTable<T> table = HoodieTable.create(config, jsc);
      Option<HoodieInstant> commitInstantOpt = Option
          .fromJavaOptional(table.getActiveTimeline().getCommitsTimeline().getInstants()
              .filter(instant -> HoodieActiveTimeline.EQUALS
                  .test(instant.getTimestamp(), commitInstantTime))
              .findFirst());
      if (commitInstantOpt.isPresent()) {
        HoodieRollbackMetadata rollbackMetadata = table
            .rollback(jsc, rollbackInstantTime, commitInstantOpt.get(), true);
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
      throw new HoodieRollbackException(
          "Failed to rollback " + config.getBasePath() + " commits " + commitInstantTime, e);
    }
  }
}
