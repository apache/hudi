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

import org.apache.hudi.client.transaction.ConflictResolutionStrategy;
import org.apache.hudi.client.transaction.ConcurrentOperation;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
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
      final ConcurrentOperation thisOperation = new ConcurrentOperation(currentTxnOwnerInstant.get(), thisCommitMetadata.get());
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
}