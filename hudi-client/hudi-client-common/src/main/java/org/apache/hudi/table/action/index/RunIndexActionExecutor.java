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

import org.apache.hudi.avro.model.HoodieIndexCommitMetadata;
import org.apache.hudi.avro.model.HoodieIndexPartitionInfo;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metadata.HoodieMetadataMetrics;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL;
import static org.apache.hudi.common.model.WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL;
import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS;
import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS_INFLIGHT;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.INDEXING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.RESTORE_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.ROLLBACK_ACTION;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
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

  static final int TIMELINE_RELOAD_INTERVAL_MILLIS = 5000;
  private static final Logger LOG = LoggerFactory.getLogger(RunIndexActionExecutor.class);
  private static final Integer INDEX_COMMIT_METADATA_VERSION_1 = 1;
  private static final Integer LATEST_INDEX_COMMIT_METADATA_VERSION = INDEX_COMMIT_METADATA_VERSION_1;
  private static final int MAX_CONCURRENT_INDEXING = 1;

  private final Option<HoodieMetadataMetrics> metrics;

  // we use this to update the latest instant in data timeline that has been indexed in metadata table
  // this needs to be volatile as it can be updated in the IndexingCheckTask spawned by this executor
  // assumption is that only one indexer can execute at a time
  private volatile String currentCaughtupInstant;

  private final TransactionManager txnManager;

  public RunIndexActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table, String instantTime) {
    super(context, config, table, instantTime);
    this.txnManager = table.getTxnManager();
    if (config.getMetadataConfig().isMetricsEnabled()) {
      this.metrics = Option.of(new HoodieMetadataMetrics(config.getMetricsConfig(), table.getStorage()));
    } else {
      this.metrics = Option.empty();
    }
  }

  @Override
  public Option<HoodieIndexCommitMetadata> execute() {
    HoodieInstant indexInstant = validateAndGetIndexInstant();
    // read HoodieIndexPlan
    HoodieIndexPlan indexPlan;
    try {
      indexPlan = table.getActiveTimeline().readIndexPlan(indexInstant);
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
      table.getActiveTimeline().transitionIndexRequestedToInflight(indexInstant);
      List<HoodieIndexPartitionInfo> finalIndexPartitionInfos;
      if (!firstTimeInitializingMetadataTable) {
        // start indexing for each partition
        try (HoodieTableMetadataWriter metadataWriter = table.getIndexingMetadataWriter(instantTime)
            .orElseThrow(() -> new HoodieIndexException(String.format(
                "Could not get metadata writer to run index action for instant: %s", instantTime)))) {
          // this will only build index upto base instant as generated by the plan, we will be doing catchup later
          String indexUptoInstant = indexPartitionInfos.get(0).getIndexUptoInstant();
          LOG.info("Starting Index Building with base instant: " + indexUptoInstant);
          HoodieTimer timer = HoodieTimer.start();
          metadataWriter.buildMetadataPartitions(context, indexPartitionInfos, indexInstant.requestedTime());
          metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.INITIALIZE_STR, timer.endTimer()));

          // get remaining instants to catchup
          List<HoodieInstant> instantsToCatchup = getInstantsToCatchup(indexUptoInstant);
          LOG.info("Total remaining instants to index: " + instantsToCatchup.size());

          // reconcile with metadata table timeline
          String metadataBasePath = getMetadataTableBasePath(table.getMetaClient().getBasePath().toString());
          HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
              .setConf(storageConf.newInstance())
              .setBasePath(metadataBasePath).build();
          Set<String> metadataCompletedTimestamps = getCompletedArchivedAndActiveInstantsAfter(indexUptoInstant, metadataMetaClient).stream()
              .map(HoodieInstant::requestedTime).collect(Collectors.toSet());

          // index catchup for all remaining instants with a timeout
          currentCaughtupInstant = indexUptoInstant;
          catchupWithInflightWriters(metadataWriter, instantsToCatchup, metadataMetaClient, metadataCompletedTimestamps, indexPartitionInfos);
          // save index commit metadata and update table config
          finalIndexPartitionInfos = indexPartitionInfos.stream()
              .map(info -> new HoodieIndexPartitionInfo(
                  info.getVersion(),
                  info.getMetadataPartitionPath(),
                  currentCaughtupInstant,
                  Collections.emptyMap()))
              .collect(Collectors.toList());
        } catch (Exception e) {
          throw new HoodieMetadataException("Failed to index partition " + Arrays.toString(indexPartitionInfos.stream()
              .map(HoodieIndexPartitionInfo::getMetadataPartitionPath).collect(Collectors.toList()).toArray()), e);
        }
      } else {
        String indexUptoInstant = fileIndexPartitionInfo.getIndexUptoInstant();
        // save index commit metadata and update table config
        // instantiation of metadata writer will automatically instantiate the partitions.
        table.getIndexingMetadataWriter(instantTime)
            .orElseThrow(() -> new HoodieIndexException(String.format(
                "Could not get metadata writer to run index action for instant: %s", instantTime)));
        finalIndexPartitionInfos = Stream.of(fileIndexPartitionInfo)
            .map(info -> new HoodieIndexPartitionInfo(
                info.getVersion(),
                info.getMetadataPartitionPath(),
                indexUptoInstant,
                Collections.emptyMap()))
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
    HoodieTableConfig.update(table.getStorage(),
        table.getMetaClient().getMetaPath(), table.getMetaClient().getTableConfig().getProps());

    // delete metadata partition
    requestedPartitions.forEach(partition -> {
      if (metadataPartitionExists(table.getMetaClient().getBasePath(), context, partition)) {
        deleteMetadataPartition(table.getMetaClient().getBasePath(), context, partition);
      }
    });

    // delete inflight instant
    table.getMetaClient().reloadActiveTimeline().deleteInstantFileIfExists(instantGenerator.getIndexInflightInstant(indexInstant.requestedTime()));
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
      instantsToIndex = getRemainingArchivedAndActiveInstantsSince(catchupStartInstant.get().requestedTime(), table.getMetaClient());
    } else {
      instantsToIndex = getRemainingArchivedAndActiveInstantsSince(indexUptoInstant, table.getMetaClient());
    }
    return instantsToIndex;
  }

  private HoodieInstant validateAndGetIndexInstant() {
    // ensure lock provider configured
    if (!config.getWriteConcurrencyMode().supportsMultiWriter() || StringUtils.isNullOrEmpty(config.getLockProviderClass())) {
      throw new HoodieIndexException(String.format("Need to set %s as %s or %s and configure lock provider class",
          WRITE_CONCURRENCY_MODE.key(), OPTIMISTIC_CONCURRENCY_CONTROL.name(), NON_BLOCKING_CONCURRENCY_CONTROL.name()));
    }

    return table.getActiveTimeline()
        .filterPendingIndexTimeline()
        .filter(instant -> instant.requestedTime().equals(instantTime) && REQUESTED.equals(instant.getState()))
        .lastInstant()
        .orElseThrow(() -> new HoodieIndexException(String.format("No requested index instant found: %s", instantTime)));
  }

  private void updateTableConfigAndTimeline(HoodieInstant indexInstant,
                                            List<HoodieIndexPartitionInfo> finalIndexPartitionInfos,
                                            HoodieIndexCommitMetadata indexCommitMetadata) throws IOException {
    try {
      // update the table config and timeline in a lock as there could be another indexer running
      txnManager.beginStateChange(Option.of(indexInstant), Option.empty());
      updateMetadataPartitionsTableConfig(table.getMetaClient(),
          finalIndexPartitionInfos.stream().map(HoodieIndexPartitionInfo::getMetadataPartitionPath).collect(Collectors.toSet()));
      table.getActiveTimeline().saveAsComplete(instantGenerator.createNewInstant(HoodieInstant.State.INFLIGHT, INDEXING_ACTION, indexInstant.requestedTime()),
          Option.of(indexCommitMetadata), txnManager.generateInstantTime());
    } finally {
      txnManager.endStateChange(Option.of(indexInstant));
    }
  }

  private void catchupWithInflightWriters(HoodieTableMetadataWriter metadataWriter, List<HoodieInstant> instantsToIndex,
                                          HoodieTableMetaClient metadataMetaClient, Set<String> metadataCompletedTimestamps,
                                          List<HoodieIndexPartitionInfo> indexPartitionInfos) {
    HoodieHeartbeatClient heartbeatClient = new HoodieHeartbeatClient(table.getStorage(), table.getMetaClient().getBasePath().toString(),
        table.getConfig().getHoodieClientHeartbeatIntervalInMs(), table.getConfig().getHoodieClientHeartbeatTolerableMisses());
    ExecutorService executorService = Executors.newFixedThreadPool(MAX_CONCURRENT_INDEXING);
    Future<?> indexingCatchupTaskFuture = executorService.submit(
        IndexingCatchupTaskFactory.createCatchupTask(indexPartitionInfos, metadataWriter, instantsToIndex, metadataCompletedTimestamps,
            table, metadataMetaClient, currentCaughtupInstant, txnManager, context, heartbeatClient));
    try {
      LOG.info("Starting index catchup task");
      HoodieTimer timer = HoodieTimer.start();
      indexingCatchupTaskFuture.get(config.getIndexingCheckTimeoutSeconds(), TimeUnit.SECONDS);
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.ASYNC_INDEXER_CATCHUP_TIME, timer.endTimer()));
    } catch (Exception e) {
      indexingCatchupTaskFuture.cancel(true);
      throw new HoodieIndexException(String.format("Index catchup failed. Current indexed instant = %s. Aborting!", currentCaughtupInstant), e);
    } finally {
      executorService.shutdownNow();
      heartbeatClient.close();
    }
  }

  private static List<HoodieInstant> getRemainingArchivedAndActiveInstantsSince(String instant, HoodieTableMetaClient metaClient) {
    List<HoodieInstant> remainingInstantsToIndex = metaClient.getArchivedTimeline().getInstantsAsStream()
        .filter(i -> compareTimestamps(i.requestedTime(), GREATER_THAN_OR_EQUALS, instant))
        .filter(i -> !INDEXING_ACTION.equals(i.getAction()))
        .collect(Collectors.toList());
    remainingInstantsToIndex.addAll(metaClient.getActiveTimeline().findInstantsAfter(instant).getInstantsAsStream()
        .filter(i -> compareTimestamps(i.requestedTime(), GREATER_THAN_OR_EQUALS, instant))
        .filter(i -> !INDEXING_ACTION.equals(i.getAction()))
        .collect(Collectors.toList()));
    return remainingInstantsToIndex;
  }

  private static List<HoodieInstant> getCompletedArchivedAndActiveInstantsAfter(String instant, HoodieTableMetaClient metaClient) {
    List<HoodieInstant> completedInstants = metaClient.getArchivedTimeline().filterCompletedInstants().findInstantsAfter(instant)
        .getInstantsAsStream().filter(i -> !INDEXING_ACTION.equals(i.getAction())).collect(Collectors.toList());
    completedInstants.addAll(metaClient.reloadActiveTimeline().filterCompletedInstants().findInstantsAfter(instant)
        .getInstantsAsStream().filter(i -> !INDEXING_ACTION.equals(i.getAction())).collect(Collectors.toList()));
    return completedInstants;
  }

  private void updateMetadataPartitionsTableConfig(HoodieTableMetaClient metaClient, Set<String> metadataPartitions) {
    metadataPartitions.forEach(metadataPartition -> {
      metaClient.getTableConfig().setMetadataPartitionState(metaClient, metadataPartition, true);
    });
  }
}
