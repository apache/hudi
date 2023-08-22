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

import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static org.apache.hudi.table.action.index.RunIndexActionExecutor.TIMELINE_RELOAD_INTERVAL_MILLIS;

/**
 * Indexing check runs for instants that completed after the base instant (in the index plan).
 * It will check if these later instants have logged updates to metadata table or not.
 * If not, then it will do the update. If a later instant is inflight, it will wait until it is completed or the task times out.
 */
public abstract class BaseIndexingCatchupTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(BaseIndexingCatchupTask.class);

  protected final HoodieTableMetadataWriter metadataWriter;
  protected final List<HoodieInstant> instantsToIndex;
  protected final Set<String> metadataCompletedInstants;
  protected final HoodieTableMetaClient metaClient;
  protected final HoodieTableMetaClient metadataMetaClient;
  protected final TransactionManager transactionManager;
  protected final HoodieEngineContext engineContext;
  protected String currentCaughtupInstant;

  public BaseIndexingCatchupTask(HoodieTableMetadataWriter metadataWriter,
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
  public abstract void run();

  HoodieInstant getHoodieInstant(HoodieInstant instant) {
    if (!metadataCompletedInstants.isEmpty() && metadataCompletedInstants.contains(instant.getTimestamp())) {
      currentCaughtupInstant = instant.getTimestamp();
      return null;
    }
    while (!instant.isCompleted()) {
      try {
        LOG.warn("instant not completed, reloading timeline " + instant);
        // reload timeline and fetch instant details again wait until timeout
        String instantTime = instant.getTimestamp();
        Option<HoodieInstant> currentInstant = metaClient.reloadActiveTimeline()
            .filterCompletedInstants().filter(i -> i.getTimestamp().equals(instantTime)).firstInstant();
        instant = currentInstant.orElse(instant);
        Thread.sleep(TIMELINE_RELOAD_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        throw new HoodieIndexException(String.format("Thread interrupted while running indexing check for instant: %s", instant), e);
      }
    }
    return instant;
  }
}
