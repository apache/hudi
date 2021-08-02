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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hudi.client.transaction.ConflictResolutionStrategy;
import org.apache.hudi.client.transaction.ConcurrentOperation;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.stream.Stream;

public class TransactionUtils {

  private static final Logger LOG = LogManager.getLogger(TransactionUtils.class);

  /**
   * Resolve any write conflicts when committing data.
   * @param table
   * @param currentTxnOwnerInstant
   * @param thisCommitMetadata
   * @param config
   * @param lastCompletedTxnOwnerInstant
   * @return
   * @throws HoodieWriteConflictException
   */
  public static Option<HoodieCommitMetadata> resolveWriteConflictIfAny(final HoodieTable table, final Option<HoodieInstant> currentTxnOwnerInstant,
      final Option<HoodieCommitMetadata> thisCommitMetadata, final HoodieWriteConfig config, Option<HoodieInstant> lastCompletedTxnOwnerInstant) throws HoodieWriteConflictException {
    if (config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl()) {
      ConflictResolutionStrategy resolutionStrategy = config.getWriteConflictResolutionStrategy();
      Stream<HoodieInstant> instantStream = resolutionStrategy.getCandidateInstants(table.getActiveTimeline(), currentTxnOwnerInstant.get(), lastCompletedTxnOwnerInstant);
      final ConcurrentOperation thisOperation = new ConcurrentOperation(currentTxnOwnerInstant.get(), thisCommitMetadata.orElse(new HoodieCommitMetadata()));
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
      // carry over necessary metadata from latest commit metadata
      overrideWithLatestCommitMetadata(table.getMetaClient(), thisOperation.getCommitMetadataOption(), currentTxnOwnerInstant, Arrays.asList(config.getWriteMetaKeyPrefixes().split(",")));
      return thisOperation.getCommitMetadataOption();
    }
    return thisCommitMetadata;
  }

  /**
   * Get the last completed transaction hoodie instant and {@link HoodieCommitMetadata#getExtraMetadata()}.
   * @param metaClient
   * @return
   */
  public static Option<Pair<HoodieInstant, Map<String, String>>> getLastCompletedTxnInstantAndMetadata(
      HoodieTableMetaClient metaClient) {
    Option<HoodieInstant> hoodieInstantOption = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().lastInstant();
    try {
      if (hoodieInstantOption.isPresent()) {
        switch (hoodieInstantOption.get().getAction()) {
          case HoodieTimeline.REPLACE_COMMIT_ACTION:
            HoodieReplaceCommitMetadata replaceCommitMetadata = HoodieReplaceCommitMetadata
                .fromBytes(metaClient.getActiveTimeline().getInstantDetails(hoodieInstantOption.get()).get(), HoodieReplaceCommitMetadata.class);
            return Option.of(Pair.of(hoodieInstantOption.get(), replaceCommitMetadata.getExtraMetadata()));
          case HoodieTimeline.DELTA_COMMIT_ACTION:
          case HoodieTimeline.COMMIT_ACTION:
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
                .fromBytes(metaClient.getActiveTimeline().getInstantDetails(hoodieInstantOption.get()).get(), HoodieCommitMetadata.class);
            return Option.of(Pair.of(hoodieInstantOption.get(), commitMetadata.getExtraMetadata()));
          default:
            throw new IllegalArgumentException("Unknown instant action" + hoodieInstantOption.get().getAction());
        }
      } else {
        return Option.empty();
      }
    } catch (IOException io) {
      throw new HoodieIOException("Unable to read metadata for instant " + hoodieInstantOption.get(), io);
    }
  }

  // override the current metadata with the metadata from the latest instant for the specified key prefixes
  private static void overrideWithLatestCommitMetadata(HoodieTableMetaClient metaClient, Option<HoodieCommitMetadata> thisMetadata,
      Option<HoodieInstant> thisInstant, List<String> keyPrefixes) {
    if (keyPrefixes.size() == 1 && keyPrefixes.get(0).length() < 1) {
      return;
    }
    Option<Pair<HoodieInstant, Map<String, String>>> lastInstant = getLastCompletedTxnInstantAndMetadata(metaClient);
    if (lastInstant.isPresent() && thisMetadata.isPresent()) {
      Stream<String> keys = thisMetadata.get().getExtraMetadata().keySet().stream();
      keyPrefixes.stream().forEach(keyPrefix -> keys
          .filter(key -> key.startsWith(keyPrefix))
          .forEach(key -> thisMetadata.get().getExtraMetadata().put(key, lastInstant.get().getRight().get(key))));
    }
  }
}