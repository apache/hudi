/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.index;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieIndexCommitMetadata;
import org.apache.hudi.avro.model.HoodieIndexPartitionInfo;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.INDEX_ACTION;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_CONCURRENCY_MODE;

/**
 * Reads the index plan and executes the plan.
 * It also reconciles updates on data timeline while indexing was in progress.
 */
public class RunIndexActionExecutor<T extends HoodieRecordPayload, I, K, O> extends BaseActionExecutor<T, I, K, O, Option<HoodieIndexCommitMetadata>> {

  private static final Logger LOG = LogManager.getLogger(RunIndexActionExecutor.class);
  private static final Integer INDEX_COMMIT_METADATA_VERSION_1 = 1;
  private static final Integer LATEST_INDEX_COMMIT_METADATA_VERSION = INDEX_COMMIT_METADATA_VERSION_1;
  private static final int MAX_CONCURRENT_INDEXING = 1;

  // we use this to update the latest instant in data timeline that has been indexed in metadata table
  // this needs to be volatile as it can be updated in the IndexingCheckTask spawned by this executor
  // assumption is that only one indexer can execute at a time
  private volatile String currentIndexedInstant;

  private final TransactionManager txnManager;

  public RunIndexActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table, String instantTime) {
    super(context, config, table, instantTime);
    this.txnManager = new TransactionManager(config, table.getMetaClient().getFs());
  }

  @Override
  public Option<HoodieIndexCommitMetadata> execute() {
    HoodieTimer indexTimer = new HoodieTimer();
    indexTimer.startTimer();

    // ensure lock provider configured
    if (!config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl() || StringUtils.isNullOrEmpty(config.getLockProviderClass())) {
      throw new HoodieIndexException(String.format("Need to set %s as %s and configure lock provider class",
          WRITE_CONCURRENCY_MODE.key(), OPTIMISTIC_CONCURRENCY_CONTROL.name()));
    }

    HoodieInstant indexInstant = table.getActiveTimeline()
        .filterPendingIndexTimeline()
        .filter(instant -> instant.getTimestamp().equals(instantTime) && REQUESTED.equals(instant.getState()))
        .lastInstant()
        .orElseThrow(() -> new HoodieIndexException(String.format("No requested index instant found: %s", instantTime)));
    try {
      // read HoodieIndexPlan
      HoodieIndexPlan indexPlan = TimelineMetadataUtils.deserializeIndexPlan(table.getActiveTimeline().readIndexPlanAsBytes(indexInstant).get());
      List<HoodieIndexPartitionInfo> indexPartitionInfos = indexPlan.getIndexPartitionInfos();
      if (indexPartitionInfos == null || indexPartitionInfos.isEmpty()) {
        throw new HoodieIndexException(String.format("No partitions to index for instant: %s", instantTime));
      }
      // transition requested indexInstant to inflight
      table.getActiveTimeline().transitionIndexRequestedToInflight(indexInstant, Option.empty());
      // start indexing for each partition
      HoodieTableMetadataWriter metadataWriter = table.getMetadataWriter(instantTime)
          .orElseThrow(() -> new HoodieIndexException(String.format("Could not get metadata writer to run index action for instant: %s", instantTime)));
      // this will only build index upto base instant as generated by the plan, we will be doing catchup later
      LOG.info("Starting Index Building");
      metadataWriter.buildIndex(context, indexPartitionInfos);

      // get all instants since the plan completed (both from active timeline and archived timeline)
      // assumption is that all metadata partitions had same instant upto which they were scheduled to be indexed
      table.getMetaClient().reloadActiveTimeline();
      String indexUptoInstant = indexPartitionInfos.get(0).getIndexUptoInstant();
      List<HoodieInstant> instantsToIndex = getRemainingArchivedAndActiveInstantsSince(indexUptoInstant, table.getMetaClient());
      LOG.info("Total remaining instants to index: " + instantsToIndex.size());

      // reconcile with metadata table timeline
      String metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(table.getMetaClient().getBasePath());
      HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataBasePath).build();
      Set<String> metadataCompletedTimestamps = getCompletedArchivedAndActiveInstantsAfter(indexUptoInstant, metadataMetaClient).stream()
          .map(HoodieInstant::getTimestamp).collect(Collectors.toSet());

      // index catchup for all remaining instants with a timeout
      currentIndexedInstant = indexUptoInstant;
      ExecutorService executorService = Executors.newFixedThreadPool(MAX_CONCURRENT_INDEXING);
      Future<?> indexingCatchupTaskFuture = executorService.submit(
          new IndexingCatchupTask(metadataWriter, instantsToIndex, metadataCompletedTimestamps, table.getMetaClient()));
      try {
        LOG.info("Starting index catchup task");
        indexingCatchupTaskFuture.get(config.getIndexingCheckTimeoutSeconds(), TimeUnit.SECONDS);
      } catch (Exception e) {
        indexingCatchupTaskFuture.cancel(true);
        throw new HoodieIndexException(String.format("Index catchup failed. Current indexed instant = %s. Aborting!", currentIndexedInstant), e);
      } finally {
        executorService.shutdownNow();
      }
      // save index commit metadata and update table config
      List<HoodieIndexPartitionInfo> finalIndexPartitionInfos = indexPartitionInfos.stream()
          .map(info -> new HoodieIndexPartitionInfo(
              info.getVersion(),
              info.getMetadataPartitionPath(),
              currentIndexedInstant))
          .collect(Collectors.toList());
      HoodieIndexCommitMetadata indexCommitMetadata = HoodieIndexCommitMetadata.newBuilder()
          .setVersion(LATEST_INDEX_COMMIT_METADATA_VERSION).setIndexPartitionInfos(finalIndexPartitionInfos).build();
      try {
        // update the table config and timeline in a lock as there could be another indexer running
        txnManager.beginTransaction();
        updateTableConfig(table.getMetaClient(), finalIndexPartitionInfos);
        table.getActiveTimeline().saveAsComplete(
            new HoodieInstant(true, INDEX_ACTION, indexInstant.getTimestamp()),
            TimelineMetadataUtils.serializeIndexCommitMetadata(indexCommitMetadata));
      } finally {
        txnManager.endTransaction();
      }
      return Option.of(indexCommitMetadata);
    } catch (IOException e) {
      throw new HoodieIndexException(String.format("Unable to index instant: %s", indexInstant));
    }
  }

  private static List<HoodieInstant> getRemainingArchivedAndActiveInstantsSince(String instant, HoodieTableMetaClient metaClient) {
    List<HoodieInstant> remainingInstantsToIndex = metaClient.getArchivedTimeline()
        .getWriteTimeline()
        .findInstantsAfter(instant)
        .getInstants().collect(Collectors.toList());
    remainingInstantsToIndex.addAll(metaClient.getActiveTimeline().getWriteTimeline().findInstantsAfter(instant).getInstants().collect(Collectors.toList()));
    return remainingInstantsToIndex;
  }

  private static List<HoodieInstant> getCompletedArchivedAndActiveInstantsAfter(String instant, HoodieTableMetaClient metaClient) {
    List<HoodieInstant> completedInstants = metaClient.getArchivedTimeline()
        .filterCompletedInstants()
        .findInstantsAfter(instant)
        .getInstants().collect(Collectors.toList());
    completedInstants.addAll(metaClient.reloadActiveTimeline().filterCompletedInstants().findInstantsAfter(instant).getInstants().collect(Collectors.toList()));
    return completedInstants;
  }

  private void updateTableConfig(HoodieTableMetaClient metaClient, List<HoodieIndexPartitionInfo> indexPartitionInfos) {
    // remove from inflight and update completed indexes
    Set<String> inflightIndexes = Stream.of(metaClient.getTableConfig().getInflightMetadataIndexes().split(","))
        .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
    Set<String> completedIndexes = Stream.of(metaClient.getTableConfig().getCompletedMetadataIndexes().split(","))
        .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
    Set<String> indexesRequested = indexPartitionInfos.stream().map(HoodieIndexPartitionInfo::getMetadataPartitionPath).collect(Collectors.toSet());
    inflightIndexes.removeAll(indexesRequested);
    completedIndexes.addAll(indexesRequested);
    // update table config
    metaClient.getTableConfig().setValue(HoodieTableConfig.TABLE_METADATA_INDEX_INFLIGHT.key(), String.join(",", inflightIndexes));
    metaClient.getTableConfig().setValue(HoodieTableConfig.TABLE_METADATA_INDEX_COMPLETED.key(), String.join(",", completedIndexes));
    HoodieTableConfig.update(metaClient.getFs(), new Path(metaClient.getMetaPath()), metaClient.getTableConfig().getProps());
  }

  /**
   * Indexing check runs for instants that completed after the base instant (in the index plan).
   * It will check if these later instants have logged updates to metadata table or not.
   * If not, then it will do the update. If a later instant is inflight, it will wait until it is completed or the task times out.
   */
  class IndexingCatchupTask implements Runnable {

    private final HoodieTableMetadataWriter metadataWriter;
    private final List<HoodieInstant> instantsToIndex;
    private final Set<String> metadataCompletedInstants;
    private final HoodieTableMetaClient metaClient;

    IndexingCatchupTask(HoodieTableMetadataWriter metadataWriter,
                        List<HoodieInstant> instantsToIndex,
                        Set<String> metadataCompletedInstants,
                        HoodieTableMetaClient metaClient) {
      this.metadataWriter = metadataWriter;
      this.instantsToIndex = instantsToIndex;
      this.metadataCompletedInstants = metadataCompletedInstants;
      this.metaClient = metaClient;
    }

    @Override
    public void run() {
      for (HoodieInstant instant : instantsToIndex) {
        // metadata index already updated for this instant
        if (metadataCompletedInstants.contains(instant.getTimestamp())) {
          currentIndexedInstant = instant.getTimestamp();
          continue;
        }
        while (!instant.isCompleted()) {
          try {
            LOG.info("instant not completed, reloading timeline " + instant);
            // reload timeline and fetch instant details again wait until timeout
            String instantTime = instant.getTimestamp();
            Option<HoodieInstant> currentInstant = metaClient.reloadActiveTimeline()
                .filterCompletedInstants().filter(i -> i.getTimestamp().equals(instantTime)).firstInstant();
            instant = currentInstant.orElse(instant);
            // so that timeline is not reloaded very frequently
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            throw new HoodieIndexException(String.format("Thread interrupted while running indexing check for instant: %s", instant), e);
          }
        }
        // update metadata for this completed instant
        if (COMPLETED.equals(instant.getState())) {
          try {
            // we need take a lock here as inflight writer could also try to update the timeline
            txnManager.beginTransaction(Option.of(instant), Option.empty());
            LOG.info("Updating metadata table for instant: " + instant);
            switch (instant.getAction()) {
              case HoodieTimeline.COMMIT_ACTION:
              case HoodieTimeline.DELTA_COMMIT_ACTION:
              case HoodieTimeline.REPLACE_COMMIT_ACTION:
                HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
                    table.getActiveTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);
                // do not trigger any table service as partition is not fully built out yet
                metadataWriter.update(commitMetadata, instant.getTimestamp(), false);
                break;
              case HoodieTimeline.CLEAN_ACTION:
                HoodieCleanMetadata cleanMetadata = CleanerUtils.getCleanerMetadata(table.getMetaClient(), instant);
                metadataWriter.update(cleanMetadata, instant.getTimestamp());
                break;
              case HoodieTimeline.RESTORE_ACTION:
                HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.deserializeHoodieRestoreMetadata(
                    table.getActiveTimeline().getInstantDetails(instant).get());
                metadataWriter.update(restoreMetadata, instant.getTimestamp());
                break;
              case HoodieTimeline.ROLLBACK_ACTION:
                HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.deserializeHoodieRollbackMetadata(
                    table.getActiveTimeline().getInstantDetails(instant).get());
                metadataWriter.update(rollbackMetadata, instant.getTimestamp());
                break;
              default:
                throw new IllegalStateException("Unexpected value: " + instant.getAction());
            }
          } catch (IOException e) {
            throw new HoodieIndexException(String.format("Could not update metadata partition for instant: %s", instant), e);
          } finally {
            txnManager.endTransaction(Option.of(instant));
          }
        }
      }
    }
  }
}
