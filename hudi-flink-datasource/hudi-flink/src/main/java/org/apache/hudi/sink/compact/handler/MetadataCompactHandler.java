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

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.AvroReaderContextFactory;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metrics.FlinkCompactionMetrics;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.table.action.compact.HoodieFlinkMergeOnReadTableCompactor;

import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Handler for compaction operation execution on Hudi metadata tables.
 *
 * <p>This handler extends {@link CompactHandler} to support metadata table specific
 * compaction operations, including compaction and log compaction.
 *
 * <p>The handler uses {@link AvroReaderContextFactory} to create reader contexts
 * for metadata table payloads, which differs from data table records(engine native).
 *
 * @see CompactHandler
 * @see CompactionPlanEvent
 * @see AvroReaderContextFactory
 */
public class MetadataCompactHandler extends CompactHandler {

  public MetadataCompactHandler(HoodieFlinkWriteClient writeClient, int taskId) {
    super(writeClient, taskId);
  }

  /**
   * Creates a reader context for reading metadata table records.
   *
   * <p>This method is overridden to create an Avro-based reader context
   * adapted for metadata table records, which has a custom payload class.
   *
   * @param needReloadMetaClient Whether the meta client needs to be reloaded
   * @return A reader context configured for metadata table records
   */
  @Override
  protected HoodieReaderContext<?> createReaderContext(boolean needReloadMetaClient) {
    String payloadClass = ConfigUtils.getPayloadClass(writeClient.getConfig().getProps());
    AvroReaderContextFactory readerContextFactory = new AvroReaderContextFactory(table.getMetaClient(), payloadClass, writeClient.getConfig().getProps());
    return readerContextFactory.getContext();
  }

  /**
   * Executes a compaction operation.
   *
   * <p>This method is overridden to support both normal compaction(full compaction)
   * and log compaction(minor compaction) for metadata tables. For normal compaction.
   *
   * @param event                The compaction plan event containing the operation details
   * @param collector            Collector for emitting compaction commit events
   * @param needReloadMetaClient Whether the meta client needs to be reloaded
   * @param compactionMetrics    Metrics collector for tracking compaction progress
   */
  @Override
  protected void doCompaction(CompactionPlanEvent event,
                              Collector<CompactionCommitEvent> collector,
                              boolean needReloadMetaClient,
                              FlinkCompactionMetrics compactionMetrics) throws Exception {
    if (!event.isLogCompaction()) {
      super.doCompaction(event, collector, needReloadMetaClient, compactionMetrics);
    } else {
      compactionMetrics.startCompaction();
      // Create a write client specifically for the metadata table
      HoodieFlinkMergeOnReadTableCompactor<?> compactor = new HoodieFlinkMergeOnReadTableCompactor<>();
      HoodieTableMetaClient metaClient = table.getMetaClient();
      if (needReloadMetaClient) {
        // reload the timeline
        metaClient.reload();
      }
      Option<InstantRange> instantRange = CompactHelpers.getInstance().getInstantRange(metaClient);
      List<WriteStatus> writeStatuses = compactor.logCompact(
          writeClient.getConfig(),
          event.getOperation(),
          event.getCompactionInstantTime(),
          instantRange,
          table,
          table.getTaskContextSupplier());
      compactionMetrics.endCompaction();
      collector.collect(createCommitEvent(event, writeStatuses));
    }
  }
}
