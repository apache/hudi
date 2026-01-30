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
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metrics.FlinkCompactionMetrics;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.action.compact.HoodieFlinkMergeOnReadTableCompactor;
import org.apache.hudi.table.format.FlinkRowDataReaderContext;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.CompactionUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Handler for executing compaction operations in compaction sub-pipeline.
 *
 * <p>The responsibilities:
 * <ul>
 *   <li>Executes compaction tasks for individual file groups;</li>
 *   <li>Handles schema evolution during compaction;</li>
 *   <li>Collects compaction metadata and generates commit events {@code CompactionCommitEvent}.</li>
 * </ul>
 *
 * <p>The handler supports both synchronous and asynchronous compaction execution.
 * It uses {@link HoodieFlinkMergeOnReadTableCompactor} to perform the actual compaction
 * operations.
 *
 * @see CompactionPlanEvent
 * @see CompactionCommitEvent
 * @see HoodieFlinkMergeOnReadTableCompactor
 */
@Slf4j
public class CompactHandler implements Closeable {
  protected final HoodieFlinkTable table;
  protected final HoodieFlinkWriteClient writeClient;
  protected final int taskID;
  /**
   * InternalSchema manager used for handling schema evolution.
   */
  private transient InternalSchemaManager internalSchemaManager;

  public CompactHandler(HoodieFlinkWriteClient writeClient, int taskId) {
    this.table = writeClient.getHoodieTable();
    this.writeClient = writeClient;
    this.taskID = taskId;
  }

  /**
   * Executes a compaction operation for a single file group.
   *
   * <p>This method supports both asynchronous and synchronous execution. When an executor
   * is provided, the compaction runs asynchronously with error handling. Otherwise, it
   * runs synchronously.
   *
   * @param executor             The executor for asynchronous execution, or null for synchronous execution
   * @param event                The compaction plan event containing operation details
   * @param collector            The collector for emitting compaction commit events
   * @param needReloadMetaClient Whether to reload the meta client before compaction
   * @param compactionMetrics    Metrics collector for compaction operations
   * @throws Exception           If compaction fails
   */
  public void compact(@Nullable NonThrownExecutor executor,
                      CompactionPlanEvent event,
                      Collector<CompactionCommitEvent> collector,
                      boolean needReloadMetaClient,
                      FlinkCompactionMetrics compactionMetrics) throws Exception {
    String instantTime = event.getCompactionInstantTime();
    if (executor != null) {
      executor.execute(
          () -> doCompaction(event, collector, needReloadMetaClient, compactionMetrics),
          (errMsg, t) -> collector.collect(createFailedCommitEvent(event)),
          "Execute compaction for instant %s from task %d", instantTime, taskID);
    } else {
      // executes the compaction task synchronously for batch mode.
      log.info("Execute compaction for instant {} from task {}", instantTime, taskID);
      doCompaction(event, collector, needReloadMetaClient, compactionMetrics);
    }
  }

  /**
   * Performs the actual compaction operation.
   *
   * @param event                The compaction plan event containing operation details
   * @param collector            The collector for emitting compaction commit events
   * @param needReloadMetaClient Whether to reload the meta client before compaction
   * @param compactionMetrics    Metrics collector for compaction operations
   * @throws Exception           If compaction fails
   */
  protected void doCompaction(CompactionPlanEvent event,
                              Collector<CompactionCommitEvent> collector,
                              boolean needReloadMetaClient,
                              FlinkCompactionMetrics compactionMetrics) throws Exception {
    compactionMetrics.startCompaction();
    HoodieFlinkMergeOnReadTableCompactor<?> compactor = new HoodieFlinkMergeOnReadTableCompactor<>();
    HoodieTableMetaClient metaClient = table.getMetaClient();
    if (needReloadMetaClient) {
      // reload the timeline
      metaClient.reload();
    }
    // schema evolution
    CompactionUtil.setAvroSchema(writeClient.getConfig(), metaClient);
    List<WriteStatus> writeStatuses = compactor.compact(
        writeClient.getConfig(),
        event.getOperation(),
        event.getCompactionInstantTime(),
        table.getTaskContextSupplier(),
        createReaderContext(needReloadMetaClient),
        table);
    compactionMetrics.endCompaction();
    collector.collect(createCommitEvent(event, writeStatuses));
  }

  /**
   * Creates a compaction commit event for successful compaction.
   *
   * @param event         The compaction plan event
   * @param writeStatuses The write statuses from the compaction operation
   * @return a new compaction commit event with the write statuses
   */
  protected CompactionCommitEvent createCommitEvent(CompactionPlanEvent event, List<WriteStatus> writeStatuses) {
    return new CompactionCommitEvent(event.getCompactionInstantTime(), event.getOperation().getFileId(), writeStatuses, taskID, event.isMetadataTable(), event.isLogCompaction());
  }

  /**
   * Creates a compaction commit event for failed compaction.
   *
   * @param event the compaction plan event
   * @return a new compaction commit event marked as failed
   */
  private CompactionCommitEvent createFailedCommitEvent(CompactionPlanEvent event) {
    return new CompactionCommitEvent(event.getCompactionInstantTime(), event.getOperation().getFileId(), taskID, event.isMetadataTable(), event.isLogCompaction());
  }

  /**
   * Creates a reader context for reading data during compaction.
   *
   * <p>The internal schema manager is instantiated lazily and reused across operations
   * unless the meta client needs to be reloaded.
   *
   * @param needReloadMetaClient whether to reload the meta client and schema manager
   * @return a new Flink row data reader context
   */
  protected HoodieReaderContext<?> createReaderContext(boolean needReloadMetaClient) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    // CAUTION: InternalSchemaManager will scan timeline, reusing the meta client so that the timeline is updated.
    // Instantiate internalSchemaManager lazily here since it may not be needed for FG reader, e.g., schema evolution
    // for log files in FG reader do not use internalSchemaManager.
    Supplier<InternalSchemaManager> internalSchemaManagerSupplier = () -> {
      if (internalSchemaManager == null || needReloadMetaClient) {
        internalSchemaManager = InternalSchemaManager.get(metaClient.getStorageConf(), metaClient);
      }
      return internalSchemaManager;
    };
    // initialize storage conf lazily.
    StorageConfiguration<?> readerConf = writeClient.getEngineContext().getStorageConf();
    return new FlinkRowDataReaderContext(readerConf, internalSchemaManagerSupplier, Collections.emptyList(), metaClient.getTableConfig(), Option.empty());
  }

  /**
   * Closes the handler and releases resources.
   *
   * <p>This method closes the write client and any associated resources.
   */
  @Override
  public void close() {
    this.writeClient.close();
  }
}
