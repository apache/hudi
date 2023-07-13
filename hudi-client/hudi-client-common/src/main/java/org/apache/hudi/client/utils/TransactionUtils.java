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
   * Resolve any write conflicts when committing data, need reload active timeline before calling
   *
   * @param table
   * @param currentTxnOwnerInstant
   * @param thisCommitMetadata
   * @param config
   * @param pendingInstants
   */
  public static void resolveWriteConflictIfAny(
      final HoodieTable table,
      final Option<HoodieInstant> currentTxnOwnerInstant,
      final Option<HoodieCommitMetadata> thisCommitMetadata,
      final HoodieWriteConfig config,
      Set<String> pendingInstants
      ) throws HoodieWriteConflictException {
    if (config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl()) {

      ConflictResolutionStrategy resolutionStrategy = config.getWriteConflictResolutionStrategy();

      Stream<HoodieInstant> instantStream = resolutionStrategy.getCandidateInstants(
          table.getMetaClient(), currentTxnOwnerInstant.get(), Option.of(pendingInstants));

      final ConcurrentOperation thisOperation = new ConcurrentOperation(currentTxnOwnerInstant.get(),
          thisCommitMetadata.orElse(new HoodieCommitMetadata()));

      instantStream.forEach(instant -> {
        try {
          ConcurrentOperation otherOperation = new ConcurrentOperation(instant, table.getMetaClient());
          if (resolutionStrategy.hasConflict(thisOperation, otherOperation)) {
            LOG.info("Conflict encountered between current instant = " + thisOperation + " and instant = "
                + otherOperation + ", attempting to resolve it...");
            resolutionStrategy.resolveConflict(thisOperation, otherOperation);
          }
        } catch (IOException io) {
          throw new HoodieWriteConflictException("Unable to resolve conflict, if present", io);
        }
      });
      LOG.info("Successfully resolved conflicts, if any");
    }
  }

  /**
   * Get InflightAndRequest instants and without current
   *
   * @param metaClient meta client
   * @param currentTimestamp current instant's timestamp
   * @return set of picked instants' timestamp
   */
  public static Set<String> getInflightAndRequestedInstantsWithoutCurrent(HoodieTableMetaClient metaClient, String currentTimestamp) {
    // collect InflightAndRequest instants for deltaCommit/commit/compaction/clustering
    Set<String> timelineActions = CollectionUtils
        .createImmutableSet(HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.COMMIT_ACTION);
    return metaClient
        .getActiveTimeline()
        .getTimelineOfActions(timelineActions)
        .filterInflightsAndRequested()
        .filter(i -> !i.getTimestamp().equals(currentTimestamp))
        .getInstantsAsStream()
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toSet());
  }

  public static Stream<HoodieInstant> getCompletedInstantsDuringCurrentWriteOperation(HoodieTableMetaClient metaClient, Set<String> pendingInstants) {
    // deal with pendingInstants
    // some pending instants maybe finished during current write operation,
    // we should check the conflict of those pending operation
    return metaClient
        .getCommitsTimeline()
        .filterCompletedInstants()
        .getInstantsAsStream()
        .filter(f -> pendingInstants.contains(f.getTimestamp()));
  }
}
