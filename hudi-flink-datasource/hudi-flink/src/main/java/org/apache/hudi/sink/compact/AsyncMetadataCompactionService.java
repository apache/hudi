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

package org.apache.hudi.sink.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.sink.compact.strategy.CompactionPlanStrategies;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 * An asynchronous compaction service specifically for metadata tables.
 *
 * <p>The metadata compaction service manages both regular compaction and log compaction
 * operations on the metadata table.
 */
public class AsyncMetadataCompactionService extends AsyncCompactionService {

  public AsyncMetadataCompactionService(FlinkCompactionConfig cfg, Configuration conf) {
    super(cfg, conf);
  }

  /**
   * Schedules a new compaction plan for the metadata table.
   * This method overrides the parent implementation to specifically
   * handle scheduling of compaction plans for the metadata table.
   *
   * @return true if a metadata compaction plan was successfully scheduled, false otherwise
   */
  @Override
  protected boolean scheduleCompaction() {
    return CompactionUtil.scheduleMetadataCompaction(writeClient, true).isPresent();
  }

  /**
   * Retrieves candidate instants for metadata compaction from the timeline.
   * This method overrides the parent implementation to handle both regular
   * compaction and log compaction instants for the metadata table.
   *
   * @return List of HoodieInstant objects representing pending compaction instants to process
   */
  @Override
  protected List<HoodieInstant> getCandidateInstants() {
    HoodieTimeline pendingTimeline = cfg.logCompactionEnabled
        ? table.getActiveTimeline()
        .filter(s -> !s.isCompleted()
            && (s.getAction().equals(HoodieTimeline.COMPACTION_ACTION) || s.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION)))
        : table.getActiveTimeline().filterPendingCompactionTimeline();
    return CompactionPlanStrategies.getStrategy(cfg).select(pendingTimeline);
  }

  /**
   * Rolls back an inflight compaction instant for the metadata table.
   * This method overrides the parent implementation to handle both regular
   * compaction and log compaction rollback operations.
   *
   * @param instant The HoodieInstant representing the compaction instant to roll back
   */
  @Override
  protected void rollbackCompactionInstant(HoodieInstant instant) {
    if (instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION)) {
      HoodieInstant inflightInstant = table.getInstantGenerator().getCompactionInflightInstant(instant.requestedTime());
      table.rollbackInflightCompaction(inflightInstant, writeClient.getTransactionManager());
    } else {
      HoodieInstant inflightInstant = table.getInstantGenerator().getLogCompactionInflightInstant(instant.requestedTime());
      table.rollbackInflightLogCompaction(inflightInstant, writeClient.getTransactionManager());
    }
  }

  /**
   * Retrieves the compaction plan for a given instant from the metadata table.
   * This method overrides the parent implementation to handle both regular
   * compaction and log compaction plans for the metadata table.
   *
   * @param instant The HoodieInstant for which to retrieve the compaction plan
   * @return The HoodieCompactionPlan containing the details of the compaction operation
   */
  @Override
  protected HoodieCompactionPlan getCompactionPlan(HoodieInstant instant) {
    return instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION)
        ? CompactionUtils.getCompactionPlan(table.getMetaClient(), instant.requestedTime())
        : CompactionUtils.getLogCompactionPlan(table.getMetaClient(), instant.requestedTime());
  }

  /**
   * Transitions compaction instants from requested to inflight state for the metadata table.
   * This method overrides the parent implementation to handle both regular compaction
   * and log compaction transitions for the metadata table.
   *
   * @param requested List of HoodieInstant objects to transition to inflight state
   */
  @Override
  protected void transitionCompactionRequestedToInflight(List<HoodieInstant> requested) {
    InstantGenerator instantGenerator = table.getInstantGenerator();
    requested.forEach(instant -> {
      if (instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION)) {
        HoodieInstant inflightInstant = instantGenerator.getCompactionRequestedInstant(instant.requestedTime());
        // Mark instant as compaction inflight
        table.getActiveTimeline().transitionCompactionRequestedToInflight(inflightInstant);
      } else {
        HoodieInstant inflightInstant = instantGenerator.getLogCompactionRequestedInstant(instant.requestedTime());
        // Mark instant as compaction inflight
        table.getActiveTimeline().transitionLogCompactionRequestedToInflight(inflightInstant);
      }
    });
  }

  /**
   * Creates a write client for metadata table.
   *
   * @param conf The Flink configuration to use for creating the write client
   *
   * @return A HoodieFlinkWriteClient instance configured for compaction operations
   */
  @Override
  protected HoodieFlinkWriteClient createWriteClient(Configuration conf) {
    HoodieFlinkWriteClient dataTableWriteClient = super.createWriteClient(conf);
    return StreamerUtil.createMetadataWriteClient(dataTableWriteClient);
  }
}
