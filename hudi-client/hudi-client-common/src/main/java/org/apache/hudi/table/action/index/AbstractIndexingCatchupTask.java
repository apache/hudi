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
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;

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
@Slf4j
public abstract class AbstractIndexingCatchupTask implements IndexingCatchupTask {

  protected final HoodieTableMetadataWriter metadataWriter;
  protected final List<HoodieInstant> instantsToIndex;
  protected final Set<String> metadataCompletedInstants;
  protected final HoodieTableMetaClient metaClient;
  protected final HoodieTableMetaClient metadataMetaClient;
  protected final TransactionManager transactionManager;
  protected final HoodieEngineContext engineContext;
  protected final HoodieTable table;
  protected final HoodieHeartbeatClient heartbeatClient;
  protected String currentCaughtupInstant;

  public AbstractIndexingCatchupTask(HoodieTableMetadataWriter metadataWriter,
                                     List<HoodieInstant> instantsToIndex,
                                     Set<String> metadataCompletedInstants,
                                     HoodieTableMetaClient metaClient,
                                     HoodieTableMetaClient metadataMetaClient,
                                     TransactionManager transactionManager,
                                     String currentCaughtupInstant,
                                     HoodieEngineContext engineContext,
                                     HoodieTable table,
                                     HoodieHeartbeatClient heartbeatClient) {
    this.metadataWriter = metadataWriter;
    this.instantsToIndex = instantsToIndex;
    this.metadataCompletedInstants = metadataCompletedInstants;
    this.metaClient = metaClient;
    this.metadataMetaClient = metadataMetaClient;
    this.transactionManager = transactionManager;
    this.currentCaughtupInstant = currentCaughtupInstant;
    this.engineContext = engineContext;
    this.table = table;
    this.heartbeatClient = heartbeatClient;
  }

  @Override
  public void run() {
    for (HoodieInstant instant : instantsToIndex) {
      // Already caught up to this instant, or no heartbeat, or heartbeat expired for this instant
      if (awaitInstantCaughtUp(instant)) {
        continue;
      }
      // if instant completed, ensure that there was metadata commit, else update metadata for this completed instant
      if (COMPLETED.equals(instant.getState())) {
        String instantTime = instant.requestedTime();
        Option<HoodieInstant> metadataInstant = metadataMetaClient.reloadActiveTimeline()
            .filterCompletedInstants().filter(i -> i.requestedTime().equals(instantTime)).firstInstant();
        if (metadataInstant.isPresent()) {
          currentCaughtupInstant = instantTime;
          continue;
        }
        try {
          // we need take a lock here as inflight writer could also try to update the timeline
          transactionManager.beginStateChange(Option.of(instant), Option.empty());
          log.info("Updating metadata table for instant: " + instant);
          switch (instant.getAction()) {
            case HoodieTimeline.COMMIT_ACTION:
            case HoodieTimeline.DELTA_COMMIT_ACTION:
            case HoodieTimeline.REPLACE_COMMIT_ACTION:
              updateIndexForWriteAction(instant);
              break;
            case CLEAN_ACTION:
              HoodieCleanMetadata cleanMetadata = CleanerUtils.getCleanerMetadata(metaClient, instant);
              metadataWriter.update(cleanMetadata, instant.requestedTime());
              break;
            case RESTORE_ACTION:
              HoodieRestoreMetadata restoreMetadata = metaClient.getActiveTimeline().readRestoreMetadata(instant);
              metadataWriter.update(restoreMetadata, instant.requestedTime());
              break;
            case ROLLBACK_ACTION:
              HoodieRollbackMetadata rollbackMetadata = metaClient.getActiveTimeline().readRollbackMetadata(instant);
              metadataWriter.update(rollbackMetadata, instant.requestedTime());
              break;
            default:
              throw new IllegalStateException("Unexpected value: " + instant.getAction());
          }
        } catch (IOException e) {
          throw new HoodieIndexException(String.format("Could not update metadata partition for instant: %s", instant), e);
        } finally {
          transactionManager.endStateChange(Option.of(instant));
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
   * <p>
   * 1. single writer.
   * a. pending ingestion commit: If no heartbeat, then we are good to ignore.
   * b. pending table service commit: There won't be any heartbeat. If no heartbeat, then we are good to ignore (strictly assuming single writer and inline table service).
   * <p>
   * 2. streamer + async table service.
   * a. pending ingestion commit: If no heartbeat, then we are good to ignore.
   * b. pending table service commit: There won't be any heartbeat. If no heartbeat, then we are good to ignore because we assume that user stops the main writer to create the index.
   * <p>
   * 3. Multi-writer scenarios:
   * a. Spark datasource ingestion (OR streamer all inline) going on. User is trying to build index via spark-sql concurrently (w/o stopping the main writer)
   * b. deltastreamer + async table services ongoing. User concurrently builds the index via spark-sql.
   * c. multi-writer spark-ds writers. User is trying to build index via spark-sql concurrently (w/o stopping the all other writer)
   * For new indexes added in 1.0.0, these flows are experimental. TODO: HUDI-8607.
   *
   * @param instant HoodieInstant to check
   * @return True if instant is already caught up, or no heartbeat, or expired heartbeat. If heartbeat exists and not expired, then return false.
   */
  boolean awaitInstantCaughtUp(HoodieInstant instant) {
    if (!metadataCompletedInstants.isEmpty() && metadataCompletedInstants.contains(instant.requestedTime())) {
      currentCaughtupInstant = instant.requestedTime();
      return true;
    }
    if (!instant.isCompleted()) {
      // check heartbeat
      try {
        // if no heartbeat, then ignore this instant
        if (!HoodieHeartbeatClient.heartbeatExists(metaClient.getStorage(), metaClient.getBasePath().toString(), instant.requestedTime())) {
          log.info("Ignoring instant {} as no heartbeat found", instant);
          return true;
        }
        // if heartbeat exists, but expired, then ignore this instant
        if (table.getConfig().getFailedWritesCleanPolicy().isLazy() && heartbeatClient.isHeartbeatExpired(instant.requestedTime())) {
          log.info("Ignoring instant {} as heartbeat expired", instant);
          return true;
        }
      } catch (IOException e) {
        throw new HoodieIOException("Unable to check if heartbeat expired for instant " + instant, e);
      }
      try {
        log.info("instant not completed, reloading timeline {}", instant);
        reloadTimelineWithWait(instant);
      } catch (InterruptedException e) {
        throw new HoodieIndexException(String.format("Thread interrupted while running indexing check for instant: %s", instant), e);
      }
    }
    return false;
  }

  private void reloadTimelineWithWait(HoodieInstant instant) throws InterruptedException {
    String instantTime = instant.requestedTime();
    Option<HoodieInstant> currentInstant;

    do {
      currentInstant = metaClient.reloadActiveTimeline()
          .filterCompletedInstants().filter(i -> i.requestedTime().equals(instantTime)).firstInstant();
      if (!currentInstant.isPresent() || !currentInstant.get().isCompleted()) {
        Thread.sleep(TIMELINE_RELOAD_INTERVAL_MILLIS);
      }
    } while (!currentInstant.isPresent() || !currentInstant.get().isCompleted());
  }
}
