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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RunIndexActionExecutor<T extends HoodieRecordPayload, I, K, O> extends BaseActionExecutor<T, I, K, O, Option<HoodieIndexCommitMetadata>> {

  private static final Logger LOG = LogManager.getLogger(RunIndexActionExecutor.class);
  private static final Integer INDEX_COMMIT_METADATA_VERSION_1 = 1;
  private static final Integer LATEST_INDEX_COMMIT_METADATA_VERSION = INDEX_COMMIT_METADATA_VERSION_1;
  private static final int MAX_CONCURRENT_INDEXING = 1;

  public RunIndexActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table, String instantTime) {
    super(context, config, table, instantTime);
  }

  @Override
  public Option<HoodieIndexCommitMetadata> execute() {
    HoodieTimer indexTimer = new HoodieTimer();
    indexTimer.startTimer();

    HoodieInstant indexInstant = table.getActiveTimeline()
        .filterPendingIndexTimeline()
        .filter(instant -> instant.getTimestamp().equals(instantTime))
        .lastInstant()
        .orElseThrow(() -> new HoodieIndexException(String.format("No pending index instant found: %s", instantTime)));
    ValidationUtils.checkArgument(HoodieInstant.State.INFLIGHT.equals(indexInstant.getState()),
        String.format("Index instant %s already inflight", instantTime));
    try {
      // read HoodieIndexPlan assuming indexInstant is requested
      // TODO: handle inflight instant, if it is inflight then throw error.
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
      metadataWriter.index(context, indexPartitionInfos);
      // get all completed instants since the plan completed
      // assumption is that all metadata partitions had same instant upto which they were scheduled to be indexed
      String indexUptoInstant = indexPartitionInfos.get(0).getIndexUptoInstant();
      Stream<HoodieInstant> remainingInstantsToIndex = table.getActiveTimeline().getWriteTimeline().getReverseOrderedInstants()
          .filter(instant -> instant.isCompleted() && HoodieActiveTimeline.GREATER_THAN.test(instant.getTimestamp(), indexUptoInstant));
      // reconcile with metadata table timeline
      String metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(table.getMetaClient().getBasePath());
      HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataBasePath).build();
      Set<HoodieInstant> metadataCompletedTimeline = metadataMetaClient.getActiveTimeline()
          .getCommitsTimeline().filterCompletedInstants().getInstants().collect(Collectors.toSet());
      List<HoodieInstant> finalRemainingInstantsToIndex = remainingInstantsToIndex.map(
          instant -> new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instant.getTimestamp())
      ).filter(instant -> !metadataCompletedTimeline.contains(instant)).collect(Collectors.toList());

      // index all remaining instants with a timeout
      ExecutorService executorService = Executors.newFixedThreadPool(MAX_CONCURRENT_INDEXING);
      Future<?> postRequestIndexingTaskFuture = executorService.submit(new PostRequestIndexingTask(metadataWriter, finalRemainingInstantsToIndex));
      try {
        // TODO: configure timeout
        postRequestIndexingTaskFuture.get(60, TimeUnit.SECONDS);
      } catch (TimeoutException | InterruptedException | ExecutionException e) {
        postRequestIndexingTaskFuture.cancel(true);
      } finally {
        executorService.shutdownNow();
      }
      Option<HoodieInstant> lastMetadataInstant = metadataMetaClient.reloadActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant();
      if (lastMetadataInstant.isPresent() && indexUptoInstant.equals(lastMetadataInstant.get().getTimestamp())) {
        return Option.of(HoodieIndexCommitMetadata.newBuilder()
            .setVersion(LATEST_INDEX_COMMIT_METADATA_VERSION).setIndexPartitionInfos(indexPartitionInfos).build());
      }
      List<HoodieIndexPartitionInfo> finalIndexPartitionInfos = indexPartitionInfos.stream()
          .map(info -> new HoodieIndexPartitionInfo(
              info.getVersion(),
              info.getMetadataPartitionPath(),
              lastMetadataInstant.get().getTimestamp())).collect(Collectors.toList());
      return Option.of(HoodieIndexCommitMetadata.newBuilder()
          .setVersion(LATEST_INDEX_COMMIT_METADATA_VERSION).setIndexPartitionInfos(finalIndexPartitionInfos).build());
    } catch (IOException e) {
      throw new HoodieIndexException(String.format("Unable to index instant: %s", indexInstant));
    }
  }

  class PostRequestIndexingTask implements Runnable {

    private final HoodieTableMetadataWriter metadataWriter;
    private final List<HoodieInstant> instantsToIndex;

    PostRequestIndexingTask(HoodieTableMetadataWriter metadataWriter, List<HoodieInstant> instantsToIndex) {
      this.metadataWriter = metadataWriter;
      this.instantsToIndex = instantsToIndex;
    }

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        for (HoodieInstant instant : instantsToIndex) {
          try {
            switch (instant.getAction()) {
              case HoodieTimeline.COMMIT_ACTION:
              case HoodieTimeline.DELTA_COMMIT_ACTION:
              case HoodieTimeline.REPLACE_COMMIT_ACTION:
                HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(table.getActiveTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);
                metadataWriter.update(commitMetadata, instant.getTimestamp(), false);
                break;
              case HoodieTimeline.CLEAN_ACTION:
                HoodieCleanMetadata cleanMetadata = CleanerUtils.getCleanerMetadata(table.getMetaClient(), instant);
                metadataWriter.update(cleanMetadata, instant.getTimestamp());
                break;
              case HoodieTimeline.RESTORE_ACTION:
                HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.deserializeHoodieRestoreMetadata(table.getActiveTimeline().getInstantDetails(instant).get());
                metadataWriter.update(restoreMetadata, instant.getTimestamp());
                break;
              case HoodieTimeline.ROLLBACK_ACTION:
                HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.deserializeHoodieRollbackMetadata(table.getActiveTimeline().getInstantDetails(instant).get());
                metadataWriter.update(rollbackMetadata, instant.getTimestamp());
                break;
              default:
                throw new IllegalStateException("Unexpected value: " + instant.getAction());
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }
  }
}
