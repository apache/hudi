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
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.RESTORE_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.ROLLBACK_ACTION;
import static org.apache.hudi.table.action.index.RunIndexActionExecutor.TIMELINE_RELOAD_INTERVAL_MILLIS;

/**
 * Indexing check runs for instants that completed after the base instant (in the index plan).
 * It will check if these later instants have logged updates to metadata table or not.
 * If not, then it will do the update. If a later instant is inflight, it will wait until it is completed or the task times out.
 */
public abstract class AbstractIndexingCatchupTask implements IndexingCatchupTask {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractIndexingCatchupTask.class);

  protected final HoodieTableMetadataWriter metadataWriter;
  protected final List<HoodieInstant> instantsToIndex;
  protected final Set<String> metadataCompletedInstants;
  protected final HoodieTableMetaClient metaClient;
  protected final HoodieTableMetaClient metadataMetaClient;
  protected final TransactionManager transactionManager;
  protected final HoodieEngineContext engineContext;
  protected String currentCaughtupInstant;

  public AbstractIndexingCatchupTask(HoodieTableMetadataWriter metadataWriter,
                                     List<HoodieInstant> instantsToIndex,
                                     Set<String> metadataCompletedInstants,
                                     HoodieTableMetaClient metaClient,
                                     HoodieTableMetaClient metadataMetaClient,
                                     TransactionManager transactionManager,
                                     String currentCaughtupInstant,
                                     HoodieEngineContext engineContext) {
    this.metadataWriter = metadataWriter;
    this.instantsToIndex = instantsToIndex;
    this.metadataCompletedInstants = metadataCompletedInstants;
    this.metaClient = metaClient;
    this.metadataMetaClient = metadataMetaClient;
    this.transactionManager = transactionManager;
    this.currentCaughtupInstant = currentCaughtupInstant;
    this.engineContext = engineContext;
  }

  @Override
  public void run() {
    for (HoodieInstant instant : instantsToIndex) {
      // metadata index already updated for this instant
      instant = awaitInstantCaughtUp(instant);
      if (instant == null) {
        continue;
      }
      // if instant completed, ensure that there was metadata commit, else update metadata for this completed instant
      if (COMPLETED.equals(instant.getState())) {
        String instantTime = instant.getRequestTime();
        Option<HoodieInstant> metadataInstant = metadataMetaClient.reloadActiveTimeline()
            .filterCompletedInstants().filter(i -> i.getRequestTime().equals(instantTime)).firstInstant();
        if (metadataInstant.isPresent()) {
          currentCaughtupInstant = instantTime;
          continue;
        }
        try {
          // we need take a lock here as inflight writer could also try to update the timeline
          transactionManager.beginTransaction(Option.of(instant), Option.empty());
          LOG.info("Updating metadata table for instant: " + instant);
          switch (instant.getAction()) {
            case HoodieTimeline.COMMIT_ACTION:
            case HoodieTimeline.DELTA_COMMIT_ACTION:
            case HoodieTimeline.REPLACE_COMMIT_ACTION:
              updateIndexForWriteAction(instant);
              break;
            case CLEAN_ACTION:
              HoodieCleanMetadata cleanMetadata = CleanerUtils.getCleanerMetadata(metaClient, instant);
              metadataWriter.update(cleanMetadata, instant.getRequestTime());
              break;
            case RESTORE_ACTION:
              HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.deserializeHoodieRestoreMetadata(
                  metaClient.getActiveTimeline().getInstantDetails(instant).get());
              metadataWriter.update(restoreMetadata, instant.getRequestTime());
              break;
            case ROLLBACK_ACTION:
              HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.deserializeHoodieRollbackMetadata(
                  metaClient.getActiveTimeline().getInstantDetails(instant).get());
              metadataWriter.update(rollbackMetadata, instant.getRequestTime());
              break;
            default:
              throw new IllegalStateException("Unexpected value: " + instant.getAction());
          }
        } catch (IOException e) {
          throw new HoodieIndexException(String.format("Could not update metadata partition for instant: %s", instant), e);
        } finally {
          transactionManager.endTransaction(Option.of(instant));
        }
      }
    }
  }

  /**
   * Updates metadata table for the instant. This is only called for actions that do actual writes,
   * i.e. for commit/deltacommit/compaction/replacecommit and not for clean/restore/rollback actions.
   *
   * @param instant HoodieInstant for which to update metadata table
   */
  public abstract void updateIndexForWriteAction(HoodieInstant instant) throws IOException;

  /**
   * For the given instant, this method checks if it is already caught up or not.
   * If not, it waits until the instant is completed.
   *
   * @param instant HoodieInstant to check
   * @return null if instant is already caught up, else the instant after it is completed.
   */
  HoodieInstant awaitInstantCaughtUp(HoodieInstant instant) {
    if (!metadataCompletedInstants.isEmpty() && metadataCompletedInstants.contains(instant.getRequestTime())) {
      currentCaughtupInstant = instant.getRequestTime();
      return null;
    }
    if (!instant.isCompleted()) {
      try {
        LOG.warn("instant not completed, reloading timeline " + instant);
        reloadTimelineWithWait(instant);
      } catch (InterruptedException e) {
        throw new HoodieIndexException(String.format("Thread interrupted while running indexing check for instant: %s", instant), e);
      }
    }
    return instant;
  }

  private void reloadTimelineWithWait(HoodieInstant instant) throws InterruptedException {
    String instantTime = instant.getRequestTime();
    Option<HoodieInstant> currentInstant;

    do {
      currentInstant = metaClient.reloadActiveTimeline()
          .filterCompletedInstants().filter(i -> i.getRequestTime().equals(instantTime)).firstInstant();
      if (!currentInstant.isPresent() || !currentInstant.get().isCompleted()) {
        Thread.sleep(TIMELINE_RELOAD_INTERVAL_MILLIS);
      }
    } while (!currentInstant.isPresent() || !currentInstant.get().isCompleted());
  }
}
