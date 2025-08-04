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

package org.apache.hudi.sink.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.FlinkCompactionMetrics;
import org.apache.hudi.sink.CleanFunction;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Function to check and commit the compaction action.
 *
 * <p> Each time after receiving a compaction commit event {@link CompactionCommitEvent},
 * it loads and checks the compaction plan {@link HoodieCompactionPlan},
 * if all the compaction operations {@link org.apache.hudi.common.model.CompactionOperation}
 * of the plan are finished, tries to commit the compaction action.
 *
 * <p>It also inherits the {@link CleanFunction} cleaning ability. This is needed because
 * the SQL API does not allow multiple sinks in one table sink provider.
 */
public class CompactionCommitSink extends CleanFunction<CompactionCommitEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(CompactionCommitSink.class);

  /**
   * Config options.
   */
  private final Configuration conf;

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
  private transient Map<String, HoodieCompactionPlan> compactionPlanCache;

  /**
   * The hoodie table.
   */
  private transient HoodieFlinkTable<?> table;

  /**
   * Compaction metrics.
   */
  private transient FlinkCompactionMetrics compactionMetrics;

  public CompactionCommitSink(Configuration conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    if (writeClient == null) {
      this.writeClient = FlinkWriteClients.createWriteClient(conf, getRuntimeContext());
    }
    this.commitBuffer = new HashMap<>();
    this.compactionPlanCache = new HashMap<>();
    this.table = this.writeClient.getHoodieTable();
    registerMetrics();
  }

  @Override
  public void invoke(CompactionCommitEvent event, Context context) throws Exception {
    final String instant = event.getInstant();
    if (event.isFailed()
        || (event.getWriteStatuses() != null
        && event.getWriteStatuses().stream().anyMatch(writeStatus -> writeStatus.getTotalErrorRecords() > 0))) {
      LOG.warn("Receive abnormal CompactionCommitEvent of instant {}, task ID is {},"
              + " is failed: {}, error record count: {}",
          instant, event.getTaskID(), event.isFailed(), getNumErrorRecords(event));
    }
    commitBuffer.computeIfAbsent(instant, k -> new HashMap<>())
        .put(event.getFileId(), event);
    commitIfNecessary(instant, commitBuffer.get(instant).values());
  }

  private long getNumErrorRecords(CompactionCommitEvent event) {
    if (event.getWriteStatuses() == null) {
      return -1L;
    }
    return event.getWriteStatuses().stream()
        .map(WriteStatus::getTotalErrorRecords).reduce(Long::sum).orElse(0L);
  }

  /**
   * Condition to commit: the commit buffer has equal size with the compaction plan operations
   * and all the compact commit event {@link CompactionCommitEvent} has the same compaction instant time.
   *
   * @param instant Compaction commit instant time
   * @param events  Commit events ever received for the instant
   */
  private void commitIfNecessary(String instant, Collection<CompactionCommitEvent> events) throws IOException {
    HoodieCompactionPlan compactionPlan = compactionPlanCache.computeIfAbsent(instant, k -> {
      try {
        return CompactionUtils.getCompactionPlan(
            this.writeClient.getHoodieTable().getMetaClient(), instant);
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    });

    boolean isReady = compactionPlan.getOperations().size() == events.size();
    if (!isReady) {
      return;
    }

    if (events.stream().anyMatch(CompactionCommitEvent::isFailed)) {
      try {
        // handle failure case
        CompactionUtil.rollbackCompaction(table, instant, writeClient.getTransactionManager());
      } finally {
        // remove commitBuffer to avoid obsolete metadata commit
        reset(instant);
        this.compactionMetrics.markCompactionRolledBack();
      }
      return;
    }

    try {
      doCommit(instant, events);
    } catch (Throwable throwable) {
      // make it fail-safe
      LOG.error("Error while committing compaction instant: " + instant, throwable);
      this.compactionMetrics.markCompactionRolledBack();
    } finally {
      // reset the status
      reset(instant);
    }
  }

  @SuppressWarnings("unchecked")
  private void doCommit(String instant, Collection<CompactionCommitEvent> events) throws IOException {
    List<WriteStatus> statuses = events.stream()
        .map(CompactionCommitEvent::getWriteStatuses)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

    long numErrorRecords = statuses.stream().map(WriteStatus::getTotalErrorRecords).reduce(Long::sum).orElse(0L);

    if (numErrorRecords > 0 && !this.conf.get(FlinkOptions.IGNORE_FAILED)) {
      // handle failure case
      LOG.error("Got {} error records during compaction of instant {},\n"
          + "option '{}' is configured as false,"
          + "rolls back the compaction", numErrorRecords, instant, FlinkOptions.IGNORE_FAILED.key());
      CompactionUtil.rollbackCompaction(table, instant, writeClient.getTransactionManager());
      this.compactionMetrics.markCompactionRolledBack();
      return;
    }

    HoodieCommitMetadata metadata = CompactHelpers.getInstance().createCompactionMetadata(
        table, instant, HoodieListData.eager(statuses), writeClient.getConfig().getSchema());

    // commit the compaction
    this.writeClient.completeCompaction(metadata, table, instant);

    this.compactionMetrics.updateCommitMetrics(instant, metadata);
    this.compactionMetrics.markCompactionCompleted();

    // Whether to clean up the old log file when compaction
    if (!conf.get(FlinkOptions.CLEAN_ASYNC_ENABLED) && !isCleaning) {
      this.writeClient.clean();
    }
  }

  private void reset(String instant) {
    this.commitBuffer.remove(instant);
    this.compactionPlanCache.remove(instant);
  }

  private void registerMetrics() {
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    compactionMetrics = new FlinkCompactionMetrics(metrics);
    compactionMetrics.registerMetrics();
  }
}
