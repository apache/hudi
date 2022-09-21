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
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.heartbeat.HeartbeatUtils;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.HoodiePendingRollbackInfo;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.rollback.RollbackUtils;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class CommonHoodieClient extends BaseHoodieClient {

  private static final Logger LOG = LogManager.getLogger(CommonHoodieClient.class);

  protected final transient HoodieMetrics metrics;
  protected transient Timer.Context writeTimer = null;
  protected transient Timer.Context compactionTimer;
  protected transient Timer.Context clusteringTimer;

  protected transient WriteOperationType operationType;

  protected transient AsyncCleanerService asyncCleanerService;
  protected transient AsyncArchiveService asyncArchiveService;
  protected final TransactionManager txnManager;

  protected CommonHoodieClient(HoodieEngineContext context, HoodieWriteConfig clientConfig, Option<EmbeddedTimelineService> timelineServer) {
    super(context, clientConfig, timelineServer);
    this.metrics = new HoodieMetrics(config);
    this.txnManager = new TransactionManager(config, fs);
  }

  public void setOperationType(WriteOperationType operationType) {
    this.operationType = operationType;
  }

  public WriteOperationType getOperationType() {
    return this.operationType;
  }

  /**
   * Get inflight time line exclude compaction and clustering.
   * @param metaClient
   * @return
   */
  private HoodieTimeline getInflightTimelineExcludeCompactionAndClustering(HoodieTableMetaClient metaClient) {
    HoodieTimeline inflightTimelineWithReplaceCommit = metaClient.getCommitsTimeline().filterPendingExcludingCompaction();
    HoodieTimeline inflightTimelineExcludeClusteringCommit = inflightTimelineWithReplaceCommit.filter(instant -> {
      if (instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
        Option<Pair<HoodieInstant, HoodieClusteringPlan>> instantPlan = ClusteringUtils.getClusteringPlan(metaClient, instant);
        return !instantPlan.isPresent();
      } else {
        return true;
      }
    });
    return inflightTimelineExcludeClusteringCommit;
  }

  protected Option<HoodiePendingRollbackInfo> getPendingRollbackInfo(HoodieTableMetaClient metaClient, String commitToRollback) {
    return getPendingRollbackInfo(metaClient, commitToRollback, true);
  }

  public Option<HoodiePendingRollbackInfo> getPendingRollbackInfo(HoodieTableMetaClient metaClient, String commitToRollback, boolean ignoreCompactionAndClusteringInstants) {
    return getPendingRollbackInfos(metaClient, ignoreCompactionAndClusteringInstants).getOrDefault(commitToRollback, Option.empty());
  }

  protected Map<String, Option<HoodiePendingRollbackInfo>> getPendingRollbackInfos(HoodieTableMetaClient metaClient) {
    return getPendingRollbackInfos(metaClient, true);
  }

  /**
   * Fetch map of pending commits to be rolled-back to {@link HoodiePendingRollbackInfo}.
   * @param metaClient instance of {@link HoodieTableMetaClient} to use.
   * @return map of pending commits to be rolled-back instants to Rollback Instant and Rollback plan Pair.
   */
  protected Map<String, Option<HoodiePendingRollbackInfo>> getPendingRollbackInfos(HoodieTableMetaClient metaClient, boolean ignoreCompactionAndClusteringInstants) {
    List<HoodieInstant> instants = metaClient.getActiveTimeline().filterPendingRollbackTimeline().getInstants().collect(Collectors.toList());
    Map<String, Option<HoodiePendingRollbackInfo>> infoMap = new HashMap<>();
    for (HoodieInstant rollbackInstant : instants) {
      HoodieRollbackPlan rollbackPlan;
      try {
        rollbackPlan = RollbackUtils.getRollbackPlan(metaClient, rollbackInstant);
      } catch (Exception e) {
        if (rollbackInstant.isRequested()) {
          LOG.warn("Fetching rollback plan failed for " + rollbackInstant + ", deleting the plan since it's in REQUESTED state", e);
          try {
            metaClient.getActiveTimeline().deletePending(rollbackInstant);
          } catch (HoodieIOException he) {
            LOG.warn("Cannot delete " + rollbackInstant, he);
            continue;
          }
        } else {
          // Here we assume that if the rollback is inflight, the rollback plan is intact
          // in instant.rollback.requested.  The exception here can be due to other reasons.
          LOG.warn("Fetching rollback plan failed for " + rollbackInstant + ", skip the plan", e);
        }
        continue;
      }

      try {
        String action = rollbackPlan.getInstantToRollback().getAction();
        if (ignoreCompactionAndClusteringInstants) {
          if (!HoodieTimeline.COMPACTION_ACTION.equals(action)) {
            boolean isClustering = HoodieTimeline.REPLACE_COMMIT_ACTION.equals(action)
                && ClusteringUtils.getClusteringPlan(metaClient, new HoodieInstant(true, rollbackPlan.getInstantToRollback().getAction(),
                rollbackPlan.getInstantToRollback().getCommitTime())).isPresent();
            if (!isClustering) {
              String instantToRollback = rollbackPlan.getInstantToRollback().getCommitTime();
              infoMap.putIfAbsent(instantToRollback, Option.of(new HoodiePendingRollbackInfo(rollbackInstant, rollbackPlan)));
            }
          }
        } else {
          infoMap.putIfAbsent(rollbackPlan.getInstantToRollback().getCommitTime(), Option.of(new HoodiePendingRollbackInfo(rollbackInstant, rollbackPlan)));
        }
      } catch (Exception e) {
        LOG.warn("Processing rollback plan failed for " + rollbackInstant + ", skip the plan", e);
      }
    }
    return infoMap;
  }

  /**
   * Rollback all failed writes.
   */
  protected Boolean rollbackFailedWrites() {
    return rollbackFailedWrites(false);
  }

  /**
   * Rollback all failed writes.
   * @param skipLocking if this is triggered by another parent transaction, locking can be skipped.
   */
  protected Boolean rollbackFailedWrites(boolean skipLocking) {
    HoodieTable table = createTable(config, hadoopConf);
    List<String> instantsToRollback = getInstantsToRollback(table.getMetaClient(), config.getFailedWritesCleanPolicy(), Option.empty());
    Map<String, Option<HoodiePendingRollbackInfo>> pendingRollbacks = getPendingRollbackInfos(table.getMetaClient());
    instantsToRollback.forEach(entry -> pendingRollbacks.putIfAbsent(entry, Option.empty()));
    rollbackFailedWrites(pendingRollbacks, skipLocking);
    return true;
  }

  protected void rollbackFailedWrites(Map<String, Option<HoodiePendingRollbackInfo>> instantsToRollback, boolean skipLocking) {
    // sort in reverse order of commit times
    LinkedHashMap<String, Option<HoodiePendingRollbackInfo>> reverseSortedRollbackInstants = instantsToRollback.entrySet()
        .stream().sorted((i1, i2) -> i2.getKey().compareTo(i1.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    for (Map.Entry<String, Option<HoodiePendingRollbackInfo>> entry : reverseSortedRollbackInstants.entrySet()) {
      if (HoodieTimeline.compareTimestamps(entry.getKey(), HoodieTimeline.LESSER_THAN_OR_EQUALS,
          HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS)) {
        // do we need to handle failed rollback of a bootstrap
        rollbackFailedBootstrap();
        HeartbeatUtils.deleteHeartbeatFile(fs, basePath, entry.getKey(), config);
        break;
      } else {
        rollback(entry.getKey(), entry.getValue(), skipLocking);
        HeartbeatUtils.deleteHeartbeatFile(fs, basePath, entry.getKey(), config);
      }
    }
  }

  protected List<String> getInstantsToRollback(HoodieTableMetaClient metaClient, HoodieFailedWritesCleaningPolicy cleaningPolicy, Option<String> curInstantTime) {
    Stream<HoodieInstant> inflightInstantsStream = getInflightTimelineExcludeCompactionAndClustering(metaClient)
        .getReverseOrderedInstants();
    if (cleaningPolicy.isEager()) {
      return inflightInstantsStream.map(HoodieInstant::getTimestamp).filter(entry -> {
        if (curInstantTime.isPresent()) {
          return !entry.equals(curInstantTime.get());
        } else {
          return true;
        }
      }).collect(Collectors.toList());
    } else if (cleaningPolicy.isLazy()) {
      return inflightInstantsStream.filter(instant -> {
        try {
          return heartbeatClient.isHeartbeatExpired(instant.getTimestamp());
        } catch (IOException io) {
          throw new HoodieException("Failed to check heartbeat for instant " + instant, io);
        }
      }).map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    } else if (cleaningPolicy.isNever()) {
      return Collections.EMPTY_LIST;
    } else {
      throw new IllegalArgumentException("Invalid Failed Writes Cleaning Policy " + config.getFailedWritesCleanPolicy());
    }
  }

  /**
   * @Deprecated
   * Rollback the inflight record changes with the given commit time. This
   * will be removed in future in favor of {@link BaseHoodieWriteClient#restoreToInstant(String, boolean)
   *
   * @param commitInstantTime Instant time of the commit
   * @param pendingRollbackInfo pending rollback instant and plan if rollback failed from previous attempt.
   * @param skipLocking if this is triggered by another parent transaction, locking can be skipped.
   * @throws HoodieRollbackException if rollback cannot be performed successfully
   */
  @Deprecated
  public boolean rollback(final String commitInstantTime, Option<HoodiePendingRollbackInfo> pendingRollbackInfo, boolean skipLocking) throws HoodieRollbackException {
    LOG.info("Begin rollback of instant " + commitInstantTime);
    final String rollbackInstantTime = pendingRollbackInfo.map(entry -> entry.getRollbackInstant().getTimestamp()).orElse(HoodieActiveTimeline.createNewInstantTime());
    final Timer.Context timerContext = this.metrics.getRollbackCtx();
    try {
      HoodieTable table = createTable(config, hadoopConf);
      Option<HoodieInstant> commitInstantOpt = Option.fromJavaOptional(table.getActiveTimeline().getCommitsTimeline().getInstants()
          .filter(instant -> HoodieActiveTimeline.EQUALS.test(instant.getTimestamp(), commitInstantTime))
          .findFirst());
      if (commitInstantOpt.isPresent() || pendingRollbackInfo.isPresent()) {
        LOG.info(String.format("Scheduling Rollback at instant time : %s "
                + "(exists in active timeline: %s), with rollback plan: %s",
            rollbackInstantTime, commitInstantOpt.isPresent(), pendingRollbackInfo.isPresent()));
        Option<HoodieRollbackPlan> rollbackPlanOption = pendingRollbackInfo.map(entry -> Option.of(entry.getRollbackPlan()))
            .orElseGet(() -> table.scheduleRollback(context, rollbackInstantTime, commitInstantOpt.get(), false, config.shouldRollbackUsingMarkers()));
        if (rollbackPlanOption.isPresent()) {
          // There can be a case where the inflight rollback failed after the instant files
          // are deleted for commitInstantTime, so that commitInstantOpt is empty as it is
          // not present in the timeline.  In such a case, the hoodie instant instance
          // is reconstructed to allow the rollback to be reattempted, and the deleteInstants
          // is set to false since they are already deleted.
          // Execute rollback
          HoodieRollbackMetadata rollbackMetadata = commitInstantOpt.isPresent()
              ? table.rollback(context, rollbackInstantTime, commitInstantOpt.get(), true, skipLocking)
              : table.rollback(context, rollbackInstantTime, new HoodieInstant(
                  true, rollbackPlanOption.get().getInstantToRollback().getAction(), commitInstantTime),
              false, skipLocking);
          if (timerContext != null) {
            long durationInMs = metrics.getDurationInMs(timerContext.stop());
            metrics.updateRollbackMetrics(durationInMs, rollbackMetadata.getTotalFilesDeleted());
          }
          return true;
        } else {
          throw new HoodieRollbackException("Failed to rollback " + config.getBasePath() + " commits " + commitInstantTime);
        }
      } else {
        LOG.warn("Cannot find instant " + commitInstantTime + " in the timeline, for rollback");
        return false;
      }
    } catch (Exception e) {
      throw new HoodieRollbackException("Failed to rollback " + config.getBasePath() + " commits " + commitInstantTime, e);
    }
  }

  /**
   * Main API to rollback failed bootstrap.
   */
  protected void rollbackFailedBootstrap() {
    LOG.info("Rolling back pending bootstrap if present");
    HoodieTable table = createTable(config, hadoopConf);
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

  protected abstract HoodieTable createTable(HoodieWriteConfig config, Configuration hadoopConf);
}
