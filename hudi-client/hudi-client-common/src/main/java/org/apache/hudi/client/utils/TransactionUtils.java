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

package org.apache.hudi.client.utils;

import org.apache.hudi.client.transaction.ConcurrentOperation;
import org.apache.hudi.client.transaction.ConflictResolutionStrategy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransactionUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionUtils.class);

  /**
   * Resolve any write conflicts when committing data.
   *
   * @param table
   * @param currentTxnOwnerInstant
   * @param thisCommitMetadata
   * @param config
   * @param lastCompletedTxnOwnerInstant
   * @param pendingInstants
   *
   * @return
   * @throws HoodieWriteConflictException
   */
  public static Option<HoodieCommitMetadata> resolveWriteConflictIfAny(
      final HoodieTable table,
      final Option<HoodieInstant> currentTxnOwnerInstant,
      final Option<HoodieCommitMetadata> thisCommitMetadata,
      final HoodieWriteConfig config,
      Option<HoodieInstant> lastCompletedTxnOwnerInstant,
      boolean reloadActiveTimeline,
      Set<String> pendingInstants) throws HoodieWriteConflictException {
    if (config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl()) {
      // deal with pendingInstants
      Stream<HoodieInstant> completedInstantsDuringCurrentWriteOperation = getCompletedInstantsDuringCurrentWriteOperation(table.getMetaClient(), pendingInstants);

      ConflictResolutionStrategy resolutionStrategy = config.getWriteConflictResolutionStrategy();
      if (reloadActiveTimeline) {
        table.getMetaClient().reloadActiveTimeline();
      }
      Stream<HoodieInstant> instantStream = Stream.concat(resolutionStrategy.getCandidateInstants(
          table.getMetaClient(), currentTxnOwnerInstant.get(), lastCompletedTxnOwnerInstant),
              completedInstantsDuringCurrentWriteOperation);

      final ConcurrentOperation thisOperation = new ConcurrentOperation(currentTxnOwnerInstant.get(), thisCommitMetadata.orElseGet(HoodieCommitMetadata::new));
      instantStream.forEach(instant -> {
        try {
          ConcurrentOperation otherOperation = new ConcurrentOperation(instant, table.getMetaClient());
          if (resolutionStrategy.hasConflict(thisOperation, otherOperation)) {
            LOG.info("Conflict encountered between current instant = " + thisOperation + " and instant = "
                + otherOperation + ", attempting to resolve it...");
            resolutionStrategy.resolveConflict(table, thisOperation, otherOperation);
          }
        } catch (IOException io) {
          throw new HoodieWriteConflictException("Unable to resolve conflict, if present", io);
        }
      });
      LOG.info("Successfully resolved conflicts, if any");

      return thisOperation.getCommitMetadataOption();
    }
    return thisCommitMetadata;
  }

  /**
   * Get the last completed transaction hoodie instant and {@link HoodieCommitMetadata#getExtraMetadata()}.
   *
   * @param metaClient
   * @return
   */
  public static Option<Pair<HoodieInstant, Map<String, String>>> getLastCompletedTxnInstantAndMetadata(
      HoodieTableMetaClient metaClient) {
    Option<HoodieInstant> hoodieInstantOption = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().lastInstant();
    return getHoodieInstantAndMetaDataPair(metaClient, hoodieInstantOption);
  }

  private static Option<Pair<HoodieInstant, Map<String, String>>> getHoodieInstantAndMetaDataPair(HoodieTableMetaClient metaClient, Option<HoodieInstant> hoodieInstantOption) {
    try {
      if (hoodieInstantOption.isPresent()) {
        HoodieCommitMetadata commitMetadata = TimelineUtils.getCommitMetadata(hoodieInstantOption.get(), metaClient.getActiveTimeline());
        return Option.of(Pair.of(hoodieInstantOption.get(), commitMetadata.getExtraMetadata()));
      } else {
        return Option.empty();
      }
    } catch (IOException io) {
      throw new HoodieIOException("Unable to read metadata for instant " + hoodieInstantOption.get(), io);
    }
  }

  /**
   * Get InflightAndRequest instants.
   *
   * @param metaClient
   * @return
   */
  public static Set<String> getInflightAndRequestedInstants(HoodieTableMetaClient metaClient) {
    // collect InflightAndRequest instants for deltaCommit/commit/compaction/clustering
    Set<String> timelineActions = CollectionUtils
        .createImmutableSet(HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.COMMIT_ACTION);
    return metaClient
        .getActiveTimeline()
        .getTimelineOfActions(timelineActions)
        .filterInflightsAndRequested()
        .getInstantsAsStream()
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toSet());
  }

  public static Stream<HoodieInstant> getCompletedInstantsDuringCurrentWriteOperation(HoodieTableMetaClient metaClient, Set<String> pendingInstants) {
    // deal with pendingInstants
    // some pending instants maybe finished during current write operation,
    // we should check the conflict of those pending operation
    return metaClient
        .reloadActiveTimeline()
        .getCommitsTimeline()
        .filterCompletedInstants()
        .getInstantsAsStream()
        .filter(f -> pendingInstants.contains(f.getTimestamp()));
  }
}
