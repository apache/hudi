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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL;
import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS;
import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS_INFLIGHT;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.INDEXING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.RESTORE_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.ROLLBACK_ACTION;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_CONCURRENCY_MODE;
import static org.apache.hudi.metadata.HoodieTableMetadata.getMetadataTableBasePath;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.deleteMetadataPartition;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getInflightAndCompletedMetadataPartitions;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getInflightMetadataPartitions;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.metadataPartitionExists;

/**
 * Reads the index plan and executes the plan.
 * It also reconciles updates on data timeline while indexing was in progress.
 */
public class RunIndexActionExecutor<T, I, K, O> extends BaseActionExecutor<T, I, K, O, Option<HoodieIndexCommitMetadata>> {

  private static final Logger LOG = LogManager.getLogger(RunIndexActionExecutor.class);
  private static final Integer INDEX_COMMIT_METADATA_VERSION_1 = 1;
  private static final Integer LATEST_INDEX_COMMIT_METADATA_VERSION = INDEX_COMMIT_METADATA_VERSION_1;
  private static final int MAX_CONCURRENT_INDEXING = 1;
  private static final int TIMELINE_RELOAD_INTERVAL_MILLIS = 5000;

  // we use this to update the latest instant in data timeline that has been indexed in metadata table
  // this needs to be volatile as it can be updated in the IndexingCheckTask spawned by this executor
  // assumption is that only one indexer can execute at a time
  private volatile String currentCaughtupInstant;

  private final TransactionManager txnManager;

  public RunIndexActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table, String instantTime) {
    super(context, config, table, instantTime);
    this.txnManager = new TransactionManager(config, table.getMetaClient().getFs());
  }

  @Override
  public Option<HoodieIndexCommitMetadata> execute() {
    HoodieTimer indexTimer = new HoodieTimer();
    indexTimer.startTimer();

    HoodieInstant indexInstant = validateAndGetIndexInstant();
    // read HoodieIndexPlan
    HoodieIndexPlan indexPlan;
    try {
      indexPlan = TimelineMetadataUtils.deserializeIndexPlan(table.getActiveTimeline().readIndexPlanAsBytes(indexInstant).get());
    } catch (IOException e) {
      throw new HoodieIndexException("Failed to read the index plan for instant: " + indexInstant);
    }
    List<HoodieIndexPartitionInfo> indexPartitionInfos = indexPlan.getIndexPartitionInfos();
    try {
      if (indexPartitionInfos == null || indexPartitionInfos.isEmpty()) {
        throw new HoodieIndexException(String.format("No partitions to index for instant: %s", instantTime));
      }
      boolean firstTimeInitializingMetadataTable = false;
      HoodieIndexPartitionInfo fileIndexPartitionInfo = null;
      if (indexPartitionInfos.size() == 1 && indexPartitionInfos.get(0).getMetadataPartitionPath().equals(MetadataPartitionType.FILES.getPartitionPath())) {
        firstTimeInitializingMetadataTable = true;
        fileIndexPartitionInfo = indexPartitionInfos.get(0);
      }
      // ensure the metadata partitions for the requested indexes are not already available (or inflight)
      Set<String> indexesInflightOrCompleted = getInflightAndCompletedMetadataPartitions(table.getMetaClient().getTableConfig());
      Set<String> requestedPartitions = indexPartitionInfos.stream()
          .map(HoodieIndexPartitionInfo::getMetadataPartitionPath).collect(Collectors.toSet());
      requestedPartitions.retainAll(indexesInflightOrCompleted);
      if (!firstTimeInitializingMetadataTable && !requestedPartitions.isEmpty()) {
        throw new HoodieIndexException(String.format("Following partitions already exist or inflight: %s", requestedPartitions));
      }

      // transition requested indexInstant to inflight
      table.getActiveTimeline().transitionIndexRequestedToInflight(indexInstant, Option.empty());
      List<HoodieIndexPartitionInfo> finalIndexPartitionInfos = null;
      if (!firstTimeInitializingMetadataTable) {
        // start indexing for each partition
        HoodieTableMetadataWriter metadataWriter = table.getMetadataWriter(instantTime)
            .orElseThrow(() -> new HoodieIndexException(String.format("Could not get metadata writer to run index action for instant: %s", instantTime)));
        // this will only build index upto base instant as generated by the plan, we will be doing catchup later
        String indexUptoInstant = indexPartitionInfos.get(0).getIndexUptoInstant();
        LOG.info("Starting Index Building with base instant: " + indexUptoInstant);
        metadataWriter.buildMetadataPartitions(context, indexPartitionInfos);

        // get remaining instants to catchup
        List<HoodieInstant> instantsToCatchup = getInstantsToCatchup(indexUptoInstant);
        LOG.info("Total remaining instants to index: " + instantsToCatchup.size());

        // reconcile with metadata table timeline
        String metadataBasePath = getMetadataTableBasePath(table.getMetaClient().getBasePath());
        HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataBasePath).build();
        Set<String> metadataCompletedTimestamps = getCompletedArchivedAndActiveInstantsAfter(indexUptoInstant, metadataMetaClient).stream()
            .map(HoodieInstant::getTimestamp).collect(Collectors.toSet());

        // index catchup for all remaining instants with a timeout
        currentCaughtupInstant = indexUptoInstant;
        catchupWithInflightWriters(metadataWriter, instantsToCatchup, metadataMetaClient, metadataCompletedTimestamps);
        // save index commit metadata and update table config
        finalIndexPartitionInfos = indexPartitionInfos.stream()
            .map(info -> new HoodieIndexPartitionInfo(
                info.getVersion(),
                info.getMetadataPartitionPath(),
                currentCaughtupInstant))
            .collect(Collectors.toList());
      } else {
        String indexUptoInstant = fileIndexPartitionInfo.getIndexUptoInstant();
        // save index commit metadata and update table config
        finalIndexPartitionInfos = Collections.singletonList(fileIndexPartitionInfo).stream()
            .map(info -> new HoodieIndexPartitionInfo(
                info.getVersion(),
                info.getMetadataPartitionPath(),
                indexUptoInstant))
            .collect(Collectors.toList());
      }

      HoodieIndexCommitMetadata indexCommitMetadata = HoodieIndexCommitMetadata.newBuilder()
          .setVersion(LATEST_INDEX_COMMIT_METADATA_VERSION).setIndexPartitionInfos(finalIndexPartitionInfos).build();
      updateTableConfigAndTimeline(indexInstant, finalIndexPartitionInfos, indexCommitMetadata);
      return Option.of(indexCommitMetadata);
    } catch (IOException e) {
      // abort gracefully
      abort(indexInstant, indexPartitionInfos.stream().map(HoodieIndexPartitionInfo::getMetadataPartitionPath).collect(Collectors.toSet()));
      throw new HoodieIndexException(String.format("Unable to index instant: %s", indexInstant));
    }
  }

  private void abort(HoodieInstant indexInstant, Set<String> requestedPartitions) {
    Set<String> inflightPartitions = getInflightMetadataPartitions(table.getMetaClient().getTableConfig());
    Set<String> completedPartitions = table.getMetaClient().getTableConfig().getMetadataPartitions();
    // update table config
    requestedPartitions.forEach(partition -> {
      inflightPartitions.remove(partition);
      completedPartitions.remove(partition);
    });
    table.getMetaClient().getTableConfig().setValue(TABLE_METADATA_PARTITIONS_INFLIGHT.key(), String.join(",", inflightPartitions));
    table.getMetaClient().getTableConfig().setValue(TABLE_METADATA_PARTITIONS.key(), String.join(",", completedPartitions));
    HoodieTableConfig.update(table.getMetaClient().getFs(), new Path(table.getMetaClient().getMetaPath()), table.getMetaClient().getTableConfig().getProps());

    // delete metadata partition
    requestedPartitions.forEach(partition -> {
      MetadataPartitionType partitionType = MetadataPartitionType.valueOf(partition.toUpperCase(Locale.ROOT));
      if (metadataPartitionExists(table.getMetaClient().getBasePath(), context, partitionType)) {
        deleteMetadataPartition(table.getMetaClient().getBasePath(), context, partitionType);
      }
    });

    // delete inflight instant
    table.getMetaClient().reloadActiveTimeline().deleteInstantFileIfExists(HoodieTimeline.getIndexInflightInstant(indexInstant.getTimestamp()));
  }

  private List<HoodieInstant> getInstantsToCatchup(String indexUptoInstant) {
    // since only write timeline was considered while scheduling index, which gives us the indexUpto instant
    // here we consider other valid actions to pick catchupStart instant
    Set<String> validActions = CollectionUtils.createSet(CLEAN_ACTION, RESTORE_ACTION, ROLLBACK_ACTION);
    Option<HoodieInstant> catchupStartInstant = table.getMetaClient().reloadActiveTimeline()
        .getTimelineOfActions(validActions)
        .filterInflightsAndRequested()
        .findInstantsBefore(indexUptoInstant)
        .firstInstant();
    // get all instants since the plan completed (both from active timeline and archived timeline)
    List<HoodieInstant> instantsToIndex;
    if (catchupStartInstant.isPresent()) {
      instantsToIndex = getRemainingArchivedAndActiveInstantsSince(catchupStartInstant.get().getTimestamp(), table.getMetaClient());
    } else {
      instantsToIndex = getRemainingArchivedAndActiveInstantsSince(indexUptoInstant, table.getMetaClient());
    }
    return instantsToIndex;
  }

  private HoodieInstant validateAndGetIndexInstant() {
    // ensure lock provider configured
    if (!config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl() || StringUtils.isNullOrEmpty(config.getLockProviderClass())) {
      throw new HoodieIndexException(String.format("Need to set %s as %s and configure lock provider class",
          WRITE_CONCURRENCY_MODE.key(), OPTIMISTIC_CONCURRENCY_CONTROL.name()));
    }

    return table.getActiveTimeline()
        .filterPendingIndexTimeline()
        .filter(instant -> instant.getTimestamp().equals(instantTime) && REQUESTED.equals(instant.getState()))
        .lastInstant()
        .orElseThrow(() -> new HoodieIndexException(String.format("No requested index instant found: %s", instantTime)));
  }

  private void updateTableConfigAndTimeline(HoodieInstant indexInstant,
                                            List<HoodieIndexPartitionInfo> finalIndexPartitionInfos,
                                            HoodieIndexCommitMetadata indexCommitMetadata) throws IOException {
    try {
      // update the table config and timeline in a lock as there could be another indexer running
      txnManager.beginTransaction(Option.of(indexInstant), Option.empty());
      updateMetadataPartitionsTableConfig(table.getMetaClient(),
          finalIndexPartitionInfos.stream().map(HoodieIndexPartitionInfo::getMetadataPartitionPath).collect(Collectors.toSet()));
      table.getActiveTimeline().saveAsComplete(
          new HoodieInstant(true, INDEXING_ACTION, indexInstant.getTimestamp()),
          TimelineMetadataUtils.serializeIndexCommitMetadata(indexCommitMetadata));
    } finally {
      txnManager.endTransaction(Option.of(indexInstant));
    }
  }

  private void catchupWithInflightWriters(HoodieTableMetadataWriter metadataWriter, List<HoodieInstant> instantsToIndex,
                                          HoodieTableMetaClient metadataMetaClient, Set<String> metadataCompletedTimestamps) {
    ExecutorService executorService = Executors.newFixedThreadPool(MAX_CONCURRENT_INDEXING);
    Future<?> indexingCatchupTaskFuture = executorService.submit(
        new IndexingCatchupTask(metadataWriter, instantsToIndex, metadataCompletedTimestamps, table.getMetaClient(), metadataMetaClient));
    try {
      LOG.info("Starting index catchup task");
      indexingCatchupTaskFuture.get(config.getIndexingCheckTimeoutSeconds(), TimeUnit.SECONDS);
    } catch (Exception e) {
      indexingCatchupTaskFuture.cancel(true);
      throw new HoodieIndexException(String.format("Index catchup failed. Current indexed instant = %s. Aborting!", currentCaughtupInstant), e);
    } finally {
      executorService.shutdownNow();
    }
  }

  private static List<HoodieInstant> getRemainingArchivedAndActiveInstantsSince(String instant, HoodieTableMetaClient metaClient) {
    List<HoodieInstant> remainingInstantsToIndex = metaClient.getArchivedTimeline().getInstants()
        .filter(i -> HoodieTimeline.compareTimestamps(i.getTimestamp(), GREATER_THAN_OR_EQUALS, instant))
        .filter(i -> !INDEXING_ACTION.equals(i.getAction()))
        .collect(Collectors.toList());
    remainingInstantsToIndex.addAll(metaClient.getActiveTimeline().findInstantsAfter(instant).getInstants()
        .filter(i -> HoodieTimeline.compareTimestamps(i.getTimestamp(), GREATER_THAN_OR_EQUALS, instant))
        .filter(i -> !INDEXING_ACTION.equals(i.getAction()))
        .collect(Collectors.toList()));
    return remainingInstantsToIndex;
  }

  private static List<HoodieInstant> getCompletedArchivedAndActiveInstantsAfter(String instant, HoodieTableMetaClient metaClient) {
    List<HoodieInstant> completedInstants = metaClient.getArchivedTimeline().filterCompletedInstants().findInstantsAfter(instant)
        .getInstants().filter(i -> !INDEXING_ACTION.equals(i.getAction())).collect(Collectors.toList());
    completedInstants.addAll(metaClient.reloadActiveTimeline().filterCompletedInstants().findInstantsAfter(instant)
        .getInstants().filter(i -> !INDEXING_ACTION.equals(i.getAction())).collect(Collectors.toList()));
    return completedInstants;
  }

  private void updateMetadataPartitionsTableConfig(HoodieTableMetaClient metaClient, Set<String> metadataPartitions) {
    // remove from inflight and update completed indexes
    Set<String> inflightPartitions = getInflightMetadataPartitions(metaClient.getTableConfig());
    Set<String> completedPartitions = metaClient.getTableConfig().getMetadataPartitions();
    inflightPartitions.removeAll(metadataPartitions);
    completedPartitions.addAll(metadataPartitions);
    // update table config
    metaClient.getTableConfig().setValue(TABLE_METADATA_PARTITIONS_INFLIGHT.key(), String.join(",", inflightPartitions));
    metaClient.getTableConfig().setValue(TABLE_METADATA_PARTITIONS.key(), String.join(",", completedPartitions));
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
    private final HoodieTableMetaClient metadataMetaClient;

    IndexingCatchupTask(HoodieTableMetadataWriter metadataWriter,
                        List<HoodieInstant> instantsToIndex,
                        Set<String> metadataCompletedInstants,
                        HoodieTableMetaClient metaClient,
                        HoodieTableMetaClient metadataMetaClient) {
      this.metadataWriter = metadataWriter;
      this.instantsToIndex = instantsToIndex;
      this.metadataCompletedInstants = metadataCompletedInstants;
      this.metaClient = metaClient;
      this.metadataMetaClient = metadataMetaClient;
    }

    @Override
    public void run() {
      for (HoodieInstant instant : instantsToIndex) {
        // metadata index already updated for this instant
        if (!metadataCompletedInstants.isEmpty() && metadataCompletedInstants.contains(instant.getTimestamp())) {
          currentCaughtupInstant = instant.getTimestamp();
          continue;
        }
        while (!instant.isCompleted()) {
          try {
            LOG.warn("instant not completed, reloading timeline " + instant);
            // reload timeline and fetch instant details again wait until timeout
            String instantTime = instant.getTimestamp();
            Option<HoodieInstant> currentInstant = metaClient.reloadActiveTimeline()
                .filterCompletedInstants().filter(i -> i.getTimestamp().equals(instantTime)).firstInstant();
            instant = currentInstant.orElse(instant);
            // so that timeline is not reloaded very frequently
            Thread.sleep(TIMELINE_RELOAD_INTERVAL_MILLIS);
          } catch (InterruptedException e) {
            throw new HoodieIndexException(String.format("Thread interrupted while running indexing check for instant: %s", instant), e);
          }
        }
        // if instant completed, ensure that there was metadata commit, else update metadata for this completed instant
        if (COMPLETED.equals(instant.getState())) {
          String instantTime = instant.getTimestamp();
          Option<HoodieInstant> metadataInstant = metadataMetaClient.reloadActiveTimeline()
              .filterCompletedInstants().filter(i -> i.getTimestamp().equals(instantTime)).firstInstant();
          if (metadataInstant.isPresent()) {
            currentCaughtupInstant = instantTime;
            continue;
          }
          try {
            // we need take a lock here as inflight writer could also try to update the timeline
            txnManager.beginTransaction(Option.of(instant), Option.empty());
            LOG.info("Updating metadata table for instant: " + instant);
            switch (instant.getAction()) {
              // TODO: see if this can be moved to metadata writer itself
              case HoodieTimeline.COMMIT_ACTION:
              case HoodieTimeline.DELTA_COMMIT_ACTION:
              case HoodieTimeline.REPLACE_COMMIT_ACTION:
                HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
                    table.getActiveTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);
                // do not trigger any table service as partition is not fully built out yet
                metadataWriter.update(commitMetadata, instant.getTimestamp(), false);
                break;
              case CLEAN_ACTION:
                HoodieCleanMetadata cleanMetadata = CleanerUtils.getCleanerMetadata(table.getMetaClient(), instant);
                metadataWriter.update(cleanMetadata, instant.getTimestamp());
                break;
              case RESTORE_ACTION:
                HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.deserializeHoodieRestoreMetadata(
                    table.getActiveTimeline().getInstantDetails(instant).get());
                metadataWriter.update(restoreMetadata, instant.getTimestamp());
                break;
              case ROLLBACK_ACTION:
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
