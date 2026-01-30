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
 * Handler for committing compaction metadata to metadata table timeline.
 *
 * <p>This handler extends {@link CompactCommitHandler} to support metadata table specific
 * compaction commit actions, including:
 * <ul>
 *   <li>Retrieves compaction plans for both compaction and log compaction;</li>
 *   <li>Commits compaction metadata;</li>
 *   <li>Commits log compaction metadata;</li>
 *   <li>Rolls back failed compactions;</li>
 * </ul>
 *
 * <p>The handler distinguishes between compaction(full compaction) and log compaction(minor compaction) based on
 * the {@link CompactionCommitEvent#isLogCompaction()} flag, and triggers compaction completion or rollback
 * appropriately with the write client.
 *
 * @see CompactCommitHandler
 * @see CompactionCommitEvent
 */
public class MetadataCompactCommitHandler extends CompactCommitHandler {

  public MetadataCompactCommitHandler(Configuration conf, HoodieFlinkWriteClient writeClient) {
    super(conf, writeClient);
  }

  /**
   * Completes a compaction for metadata tables.
   *
   * <p>This method is overridden to support both compaction(full compaction)
   * and log compaction(minor compaction) for metadata tables. It creates appropriate metadata based on the
   * operation type and completes the compaction.
   *
   * @param instant           The compaction instant time
   * @param isLogCompaction   Whether the compaction is log compaction
   * @param statuses          List of write statuses from all compaction operations
   * @param compactionMetrics Metrics collector for tracking compaction progress
   */
  @Override

  protected void completeCompaction(String instant,
                                    boolean isLogCompaction,
                                    List<WriteStatus> statuses,
                                    FlinkCompactionMetrics compactionMetrics) throws IOException {
    WriteOperationType operationType = isLogCompaction ? WriteOperationType.LOG_COMPACT : WriteOperationType.COMPACT;
    HoodieCommitMetadata metadata = CompactHelpers.getInstance().createCompactionMetadata(
        table, instant, HoodieListData.eager(statuses), writeClient.getConfig().getSchema(), operationType);

    // commit the compaction
    if (isLogCompaction) {
      writeClient.completeLogCompaction(metadata, table, instant);
    } else {
      writeClient.completeCompaction(metadata, table, instant);
    }

    compactionMetrics.updateCommitMetrics(instant, metadata);
    compactionMetrics.markCompactionCompleted();
  }

  /**
   * Rolls back a failed compaction.
   *
   * <p>This method is overridden to support rolling back of both compaction and log compaction.
   *
   * @param instant         The compaction instant time
   * @param isLogCompaction Whether the compaction is log compaction
   */
  @Override
  protected void rollbackCompaction(String instant, boolean isLogCompaction) {
    if (isLogCompaction) {
      CompactionUtil.rollbackLogCompaction(table, instant, writeClient.getTransactionManager());
    } else {
      CompactionUtil.rollbackCompaction(table, instant, writeClient.getTransactionManager());
    }
  }

  /**
   * Retrieves the compaction plan for a metadata table compaction instant.
   *
   * <p>This method is overridden to support retrieving both
   * compaction plans and log compaction plans based on the compaction type.
   * It uses a cache to avoid repeatedly reading plans from storage.
   *
   * @param instant         The compaction instant time
   * @param isLogCompaction Whether the compaction is log compaction
   *
   * @return The compaction plan for the instant
   * @throws HoodieException If the compaction plan cannot be retrieved
   */
  @Override
  protected HoodieCompactionPlan getCompactionPlan(String instant, boolean isLogCompaction) {
    return compactionPlanCache.computeIfAbsent(instant, k -> {
      try {
        return isLogCompaction ? CompactionUtils.getLogCompactionPlan(this.writeClient.getHoodieTable().getMetaClient(), instant)
            : CompactionUtils.getCompactionPlan(this.writeClient.getHoodieTable().getMetaClient(), instant);
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    });
  }
}
