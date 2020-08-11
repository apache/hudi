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
import org.apache.hudi.callback.HoodieWriteCommitCallback;
import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage;
import org.apache.hudi.callback.util.HoodieCommitCallbackFactory;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

/**
 * Abstract Write Client providing functionality for performing commit, index updates and rollback
 *  Reused for regular write operations like upsert/insert/bulk-insert.. as well as bootstrap
 * @param <T> Sub type of HoodieRecordPayload
 */
public abstract class AbstractHoodieWriteClient<T extends HoodieRecordPayload> extends AbstractHoodieClient {

  private static final Logger LOG = LogManager.getLogger(AbstractHoodieWriteClient.class);

  private final transient HoodieMetrics metrics;
  private final transient HoodieIndex<T> index;

  private transient Timer.Context writeContext = null;
  private transient WriteOperationType operationType;
  private transient HoodieWriteCommitCallback commitCallback;

  public void setOperationType(WriteOperationType operationType) {
    this.operationType = operationType;
  }

  public WriteOperationType getOperationType() {
    return this.operationType;
  }

  protected AbstractHoodieWriteClient(JavaSparkContext jsc, HoodieIndex index, HoodieWriteConfig clientConfig,
      Option<EmbeddedTimelineService> timelineServer) {
    super(jsc, clientConfig, timelineServer);
    this.metrics = new HoodieMetrics(config, config.getTableName());
    this.index = index;
  }

  /**
   * Commit changes performed at the given instantTime marker.
   */
  public boolean commit(String instantTime, JavaRDD<WriteStatus> writeStatuses) {
    return commit(instantTime, writeStatuses, Option.empty());
  }

  /**
   * Commit changes performed at the given instantTime marker.
   */
  public boolean commit(String instantTime, JavaRDD<WriteStatus> writeStatuses,
      Option<Map<String, String>> extraMetadata) {
    List<HoodieWriteStat> stats = writeStatuses.map(WriteStatus::getStat).collect();
    return commitStats(instantTime, stats, extraMetadata);
  }

  public boolean commitStats(String instantTime, List<HoodieWriteStat> stats, Option<Map<String, String>> extraMetadata) {
    LOG.info("Committing " + instantTime);
    HoodieTableMetaClient metaClient = createMetaClient(false);
    String actionType = metaClient.getCommitActionType();
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.create(config, hadoopConf);

    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
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
      postCommit(table, metadata, instantTime, extraMetadata);
      emitCommitMetrics(instantTime, metadata, actionType);
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

  void emitCommitMetrics(String instantTime, HoodieCommitMetadata metadata, String actionType) {
    try {

      if (writeContext != null) {
        long durationInMs = metrics.getDurationInMs(writeContext.stop());
        metrics.updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(instantTime).getTime(), durationInMs,
            metadata, actionType);
        writeContext = null;
      }
    } catch (ParseException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + instantTime
          + "Instant time is not of valid format", e);
    }
  }

  /**
   * Post Commit Hook. Derived classes use this method to perform post-commit processing
   *
   * @param table         table to commit on
   * @param metadata      Commit Metadata corresponding to committed instant
   * @param instantTime   Instant Time
   * @param extraMetadata Additional Metadata passed by user
   */
  protected abstract void postCommit(HoodieTable<?> table, HoodieCommitMetadata metadata, String instantTime, Option<Map<String, String>> extraMetadata);

  /**
   * Finalize Write operation.
   * @param table  HoodieTable
   * @param instantTime Instant Time
   * @param stats Hoodie Write Stat
   */
  protected void finalizeWrite(HoodieTable<T> table, String instantTime, List<HoodieWriteStat> stats) {
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
      throw new HoodieCommitException("Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
  }

  public HoodieMetrics getMetrics() {
    return metrics;
  }

  public HoodieIndex<T> getIndex() {
    return index;
  }

  /**
   * Get HoodieTable and init {@link Timer.Context}.
   *
   * @param operationType write operation type
   * @param instantTime current inflight instant time
   * @return HoodieTable
   */
  protected HoodieTable getTableAndInitCtx(WriteOperationType operationType, String instantTime) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    UpgradeDowngrade.run(metaClient, HoodieTableVersion.current(), config, jsc, instantTime);
    return getTableAndInitCtx(metaClient, operationType);
  }

  private HoodieTable getTableAndInitCtx(HoodieTableMetaClient metaClient, WriteOperationType operationType) {
    if (operationType == WriteOperationType.DELETE) {
      setWriteSchemaForDeletes(metaClient);
    }
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable table = HoodieTable.create(metaClient, config, hadoopConf);
    if (table.getMetaClient().getCommitActionType().equals(HoodieTimeline.COMMIT_ACTION)) {
      writeContext = metrics.getCommitCtx();
    } else {
      writeContext = metrics.getDeltaCommitCtx();
    }
    return table;
  }

  /**
   * Sets write schema from last instant since deletes may not have schema set in the config.
   */
  private void setWriteSchemaForDeletes(HoodieTableMetaClient metaClient) {
    try {
      HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
      Option<HoodieInstant> lastInstant =
          activeTimeline.filterCompletedInstants().filter(s -> s.getAction().equals(metaClient.getCommitActionType()))
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
    // Stop timeline-server if running
    super.close();
    // Calling this here releases any resources used by your index, so make sure to finish any related operations
    // before this point
    this.index.close();
  }
}
