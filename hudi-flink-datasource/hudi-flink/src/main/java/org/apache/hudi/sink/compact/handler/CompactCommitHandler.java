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
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.FlinkCompactionMetrics;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.util.CompactionUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Handler for committing compaction operations in compaction sub-pipeline.
 *
 * <p>The responsibilities:
 * <ul>
 *   <li>Buffers compaction commit events from multiple parallel tasks;</li>
 *   <li>Determines whether all compaction operations for an instant are collected as complete;</li>
 *   <li>Commits the compaction to the timeline;</li>
 *   <li>Rolls back failed compactions;</li>
 *   <li>Triggers cleaning operations after successful compaction.</li>
 * </ul>
 *
 * <p>The handler uses a commit buffer to collect events from all parallel compaction tasks.
 * Once all operations for a compaction instant are complete, it validates the results and
 * either commits the compaction or rolls it back based on the success/failure status.
 *
 * <p>The commit condition is met when the commit buffer has the same number of events as
 * the compaction plan operations, and all events share the same compaction instant time.
 *
 * @see CompactionCommitEvent
 * @see HoodieCompactionPlan
 */
@Slf4j
public class CompactCommitHandler implements Closeable {
  protected final HoodieFlinkTable table;
  protected final HoodieFlinkWriteClient writeClient;
  protected final Configuration conf;
  /**
   * Buffer to collect the event from each compact task {@code CompactFunction}.
   *
   * <p>Stores the mapping of instant_time -> file_id -> event. Use a map to collect the
   * events because the rolling back of intermediate compaction tasks generates corrupt
   * events.
   */
  private transient Map<String, Map<String, CompactionCommitEvent>> commitBuffer;

  /**
   * Cache to store compaction plan for each instant.
   * Stores the mapping of instant_time -> compactionPlan.
   */
  protected transient Map<String, HoodieCompactionPlan> compactionPlanCache;

  public CompactCommitHandler(Configuration conf, HoodieFlinkWriteClient writeClient) {
    this.conf = conf;
    this.table = writeClient.getHoodieTable();
    this.writeClient = writeClient;
    this.commitBuffer = new HashMap<>();
    this.compactionPlanCache = new HashMap<>();
  }

  /**
   * Commits the compaction if all operations for the instant are complete.
   *
   * <p>Condition to commit: the commit buffer has equal size with the compaction plan operations
   * and all the compact commit event {@link CompactionCommitEvent} has the same compaction instant time.
   *
   * @param event             The compaction commit event
   * @param compactionMetrics Metrics collector for tracking compaction progress
   */
  public void commitIfNecessary(CompactionCommitEvent event, FlinkCompactionMetrics compactionMetrics) {
    String instant = event.getInstant();
    commitBuffer.computeIfAbsent(instant, k -> new HashMap<>())
        .put(event.getFileId(), event);

    boolean isLogCompaction = event.isLogCompaction();
    HoodieCompactionPlan compactionPlan = getCompactionPlan(instant, isLogCompaction);
    Collection<CompactionCommitEvent> events = commitBuffer.get(instant).values();

    boolean isReady = compactionPlan.getOperations().size() == events.size();
    if (!isReady) {
      return;
    }

    if (events.stream().anyMatch(CompactionCommitEvent::isFailed)) {
      try {
        // handle the failure case
        rollbackCompaction(instant, isLogCompaction);
      } finally {
        // remove commitBuffer to avoid obsolete metadata commit
        reset(instant);
        compactionMetrics.markCompactionRolledBack();
      }
      return;
    }

    try {
      doCommit(instant, isLogCompaction, events, compactionMetrics);
    } catch (Throwable throwable) {
      // make it fail-safe
      log.error("Error while committing compaction instant: {}", instant, throwable);
      compactionMetrics.markCompactionRolledBack();
    } finally {
      // reset the status
      reset(instant);
    }
  }

  /**
   * Performs the actual commit operation for a compaction instant.
   *
   * <p>This method aggregates write statuses from all compaction events, checks for errors,
   * and either completes the compaction or rolls it back based on the error count and
   * configuration. If successful and cleaning is enabled, it triggers a cleaning operation.
   *
   * @param instant           The compaction instant time
   * @param isLogCompaction   Whether the compaction is log compaction
   * @param events            All compaction commit events for this instant
   * @param compactionMetrics Metrics collector for tracking compaction progress
   * @throws IOException      If an I/O error occurs during commit
   */
  @SuppressWarnings("unchecked")
  private void doCommit(
      String instant,
      boolean isLogCompaction,
      Collection<CompactionCommitEvent> events,
      FlinkCompactionMetrics compactionMetrics) throws IOException {
    List<WriteStatus> statuses = events.stream()
        .map(CompactionCommitEvent::getWriteStatuses)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

    long numErrorRecords = statuses.stream().map(WriteStatus::getTotalErrorRecords).reduce(Long::sum).orElse(0L);

    if (numErrorRecords > 0 && !this.conf.get(FlinkOptions.IGNORE_FAILED)) {
      // handle failure case
      log.error("Got {} error records during compaction of instant {},\n"
          + "option '{}' is configured as false,"
          + "rolls back the compaction", numErrorRecords, instant, FlinkOptions.IGNORE_FAILED.key());
      rollbackCompaction(instant, isLogCompaction);
      compactionMetrics.markCompactionRolledBack();
      return;
    }

    // complete the compaction
    completeCompaction(instant, isLogCompaction, statuses, compactionMetrics);

    // Whether to clean up the old log file when compaction
    if (!conf.get(FlinkOptions.CLEAN_ASYNC_ENABLED)) {
      writeClient.clean();
    }
  }

  /**
   * Rolls back a failed compaction operation.
   *
   * @param instant         The compaction instant time
   * @param isLogCompaction Whether the compaction is log compaction
   */
  protected void rollbackCompaction(String instant, boolean isLogCompaction) {
    CompactionUtil.rollbackCompaction(table, instant, writeClient.getTransactionManager());
  }

  /**
   * Completes a successful compaction operation by creating metadata and committing to the timeline.
   *
   * <p>This method creates compaction metadata from the write statuses, commits the compaction
   * to the timeline, and updates compaction metrics.
   *
   * @param instant           The compaction instant time
   * @param isLogCompaction   Whether the compaction is log compaction
   * @param statuses          List of write statuses from all compaction operations
   * @param compactionMetrics Metrics collector for tracking compaction progress
   * @throws IOException      If an I/O error occurs during completion
   */
  protected void completeCompaction(String instant,
                                    boolean isLogCompaction,
                                    List<WriteStatus> statuses,
                                    FlinkCompactionMetrics compactionMetrics) throws IOException {
    HoodieCommitMetadata metadata = CompactHelpers.getInstance().createCompactionMetadata(
        table, instant, HoodieListData.eager(statuses), writeClient.getConfig().getSchema());
    writeClient.completeCompaction(metadata, table, instant);
    compactionMetrics.updateCommitMetrics(instant, metadata);
    compactionMetrics.markCompactionCompleted();
  }

  /**
   * Retrieves the compaction plan for a given instant.
   *
   * <p>This method uses a cache to avoid repeatedly reading the compaction plan from storage.
   * If the plan is not in the cache, it reads it from the timeline and caches it.
   *
   * @param instant         The compaction instant time
   * @param isLogCompaction Whether the compaction is log compaction
   *
   * @return The compaction plan for the instant
   */
  protected HoodieCompactionPlan getCompactionPlan(String instant, boolean isLogCompaction) {
    return compactionPlanCache.computeIfAbsent(instant, k -> {
      try {
        return CompactionUtils.getCompactionPlan(
            this.writeClient.getHoodieTable().getMetaClient(), instant);
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    });
  }

  /**
   * Resets the internal state for a completed or failed compaction instant.
   *
   * @param instant The compaction instant time to reset
   */
  private void reset(String instant) {
    this.commitBuffer.remove(instant);
    this.compactionPlanCache.remove(instant);
  }

  @Override
  public void close() {
    writeClient.close();
  }
}
