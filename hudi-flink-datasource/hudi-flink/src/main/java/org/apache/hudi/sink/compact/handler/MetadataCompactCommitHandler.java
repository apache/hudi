/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.compact.handler;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.FlinkCompactionMetrics;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.util.CompactionUtil;

import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.List;

/**
 * Specialized handler for committing compaction operations on metadata tables.
 *
 * <p>This handler extends {@link CompactCommitHandler} to support metadata table-specific
 * compaction commit operations, including:
 * <ul>
 *   <li>Retrieving compaction plans for both compaction types</li>
 *   <li>Committing regular compaction for metadata tables</li>
 *   <li>Committing log compaction for metadata tables</li>
 *   <li>Rolling back both regular and log compactions</li>
 * </ul>
 *
 * <p>The handler distinguishes between regular compaction and log compaction based on
 * the {@link CompactionCommitEvent#isLogCompaction()} flag, and invokes the appropriate
 * completion or rollback methods with the write client.
 *
 * @see CompactCommitHandler
 * @see CompactionCommitEvent
 */
public class MetadataCompactCommitHandler extends CompactCommitHandler {

  public MetadataCompactCommitHandler(Configuration conf, HoodieFlinkWriteClient writeClient) {
    super(conf, writeClient);
  }

  /**
   * Completes a compaction operation for metadata tables.
   *
   * <p>This method overrides the parent implementation to support both regular compaction
   * and log compaction for metadata tables. It creates appropriate metadata based on the
   * operation type and invokes the corresponding completion method on the write client.
   *
   * @param event             The compaction commit event indicating the type of compaction
   * @param statuses          List of write statuses from all compaction operations
   * @param compactionMetrics Metrics collector for tracking compaction progress
   */
  @Override
  protected void completeCompaction(CompactionCommitEvent event, List<WriteStatus> statuses, FlinkCompactionMetrics compactionMetrics) throws IOException {
    String instant = event.getInstant();
    WriteOperationType operationType = event.isLogCompaction() ? WriteOperationType.LOG_COMPACT : WriteOperationType.COMPACT;
    HoodieCommitMetadata metadata = CompactHelpers.getInstance().createCompactionMetadata(
        table, instant, HoodieListData.eager(statuses), writeClient.getConfig().getSchema(), operationType);

    // commit the compaction
    if (event.isLogCompaction()) {
      writeClient.completeLogCompaction(metadata, table, instant);
    } else {
      writeClient.completeCompaction(metadata, table, instant);
    }

    compactionMetrics.updateCommitMetrics(instant, metadata);
    compactionMetrics.markCompactionCompleted();
  }

  /**
   * Rolls back a failed compaction operation for metadata tables.
   *
   * <p>This method overrides the parent implementation to support rolling back both
   * regular compaction and log compaction based on the event type. It invokes the
   * appropriate rollback method for the compaction type.
   *
   * @param event The compaction commit event indicating the type of compaction to roll back
   */
  @Override
  protected void rollbackCompaction(CompactionCommitEvent event) {
    if (event.isLogCompaction()) {
      CompactionUtil.rollbackLogCompaction(table, event.getInstant(), writeClient.getTransactionManager());
    } else {
      CompactionUtil.rollbackCompaction(table, event.getInstant(), writeClient.getTransactionManager());
    }
  }

  /**
   * Retrieves the compaction plan for a metadata table compaction instant.
   *
   * <p>This method overrides the parent implementation to support retrieving both
   * regular compaction plans and log compaction plans based on the event type.
   * It uses a cache to avoid repeatedly reading plans from storage.
   *
   * @param event The compaction commit event indicating the type of compaction
   * @return The compaction plan for the instant
   * @throws HoodieException If the compaction plan cannot be retrieved
   */
  @Override
  protected HoodieCompactionPlan getCompactionPlan(CompactionCommitEvent event) {
    String instant = event.getInstant();
    return compactionPlanCache.computeIfAbsent(instant, k -> {
      try {
        return event.isLogCompaction()
            ? CompactionUtils.getLogCompactionPlan(this.writeClient.getHoodieTable().getMetaClient(), instant)
            : CompactionUtils.getCompactionPlan(this.writeClient.getHoodieTable().getMetaClient(), instant);
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    });
  }
}
