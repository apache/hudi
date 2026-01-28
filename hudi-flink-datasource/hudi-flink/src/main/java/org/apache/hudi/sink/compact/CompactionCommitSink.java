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
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.FlinkHoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metrics.FlinkCompactionMetrics;
import org.apache.hudi.sink.CleanFunction;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkWriteClients;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;

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
@Slf4j
public class CompactionCommitSink extends CleanFunction<CompactionCommitEvent> {

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
   * Buffer to collect the compaction event for metadata table from each compact task {@code CompactFunction}.
   */
  private transient Map<String, Map<String, CompactionCommitEvent>> metadataCommitBuffer;

  /**
   * Cache to store compaction plan for each instant.
   * Stores the mapping of instant_time -> compactionPlan.
   */
  private transient Map<String, HoodieCompactionPlan> compactionPlanCache;

  /**
   * Cache to store compaction plan for each instant for metadata table
   * Stores the mapping of instant_time -> compactionPlan.
   */
  private transient Map<String, HoodieCompactionPlan> metadataCompactionPlanCache;

  /**
   * Write client for the metadata table.
   */
  private transient HoodieFlinkWriteClient metadataWriteClient;

  /**
   * The hoodie table.
   */
  private transient HoodieFlinkTable<?> table;

  /**
   * The hoodie table.
   */
  private transient HoodieFlinkTable<?> metadataTable;

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
    this.metadataCommitBuffer = new HashMap<>();
    this.compactionPlanCache = new HashMap<>();
    this.metadataCompactionPlanCache = new HashMap<>();
    this.table = this.writeClient.getHoodieTable();
    registerMetrics();
  }

  @Override
  public void invoke(CompactionCommitEvent event, Context context) throws Exception {
    final String instant = event.getInstant();
    if (event.isFailed()
        || (event.getWriteStatuses() != null
        && event.getWriteStatuses().stream().anyMatch(writeStatus -> writeStatus.getTotalErrorRecords() > 0))) {
      log.warn("Received abnormal CompactionCommitEvent of instant {}, task ID is {},"
              + " is failed: {}, error record count: {}",
          instant, event.getTaskID(), event.isFailed(), getNumErrorRecords(event));
    }
    if (event.isMetadataTable()) {
      initializeMetadataClientIfNecessary();
      metadataCommitBuffer.computeIfAbsent(instant, k -> new HashMap<>())
          .put(event.getFileId(), event);
      commitIfNecessary(metadataTable, metadataWriteClient, instant, metadataCommitBuffer.get(instant).values(), getCompactionPlan(event), event.isLogCompaction());
    } else {
      commitBuffer.computeIfAbsent(instant, k -> new HashMap<>())
          .put(event.getFileId(), event);
      commitIfNecessary(table, writeClient, instant, commitBuffer.get(instant).values(), getCompactionPlan(event), event.isLogCompaction());
    }
  }

  private void initializeMetadataClientIfNecessary() {
    if (this.metadataWriteClient != null) {
      return;
    }
    // Get the metadata writer from the table and use its write client
    Option<HoodieTableMetadataWriter> metadataWriterOpt =
        this.writeClient.getHoodieTable().getMetadataWriter(null, true, true);
    ValidationUtils.checkArgument(metadataWriterOpt.isPresent(), "Failed to close the metadata writer");
    FlinkHoodieBackedTableMetadataWriter metadataWriter = (FlinkHoodieBackedTableMetadataWriter) metadataWriterOpt.get();
    this.metadataWriteClient = (HoodieFlinkWriteClient) metadataWriter.getWriteClient();
    this.metadataTable = metadataWriteClient.getHoodieTable();
  }

  private HoodieCompactionPlan getCompactionPlan(CompactionCommitEvent event) {
    String instant = event.getInstant();
    if (event.isMetadataTable()) {
      return metadataCompactionPlanCache.computeIfAbsent(instant, k -> {
        try {
          return event.isLogCompaction()
              ? CompactionUtils.getLogCompactionPlan(this.metadataWriteClient.getHoodieTable().getMetaClient(), instant)
              : CompactionUtils.getCompactionPlan(this.metadataWriteClient.getHoodieTable().getMetaClient(), instant);
        } catch (Exception e) {
          throw new HoodieException("Failed to get the compaction plan.", e);
        }
      });
    } else {
      return compactionPlanCache.computeIfAbsent(instant, k -> {
        try {
          return CompactionUtils.getCompactionPlan(
              this.writeClient.getHoodieTable().getMetaClient(), instant);
        } catch (Exception e) {
          throw new HoodieException(e);
        }
      });
    }
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
  private void commitIfNecessary(
      HoodieFlinkTable<?> table,
      HoodieFlinkWriteClient writeClient,
      String instant,
      Collection<CompactionCommitEvent> events,
      HoodieCompactionPlan compactionPlan,
      boolean isLogCompaction) {

    boolean isReady = compactionPlan.getOperations().size() == events.size();
    if (!isReady) {
      return;
    }

    if (events.stream().anyMatch(CompactionCommitEvent::isFailed)) {
      try {
        // handle the failure case
        if (isLogCompaction) {
          CompactionUtil.rollbackLogCompaction(table, instant, writeClient.getTransactionManager());
        } else {
          CompactionUtil.rollbackCompaction(table, instant, writeClient.getTransactionManager());
        }
      } finally {
        // remove commitBuffer to avoid obsolete metadata commit
        reset(instant, table.isMetadataTable());
        this.compactionMetrics.markCompactionRolledBack();
      }
      return;
    }

    try {
      doCommit(table, writeClient, instant, events, isLogCompaction);
    } catch (Throwable throwable) {
      // make it fail-safe
      log.error("Error while committing compaction instant: {}", instant, throwable);
      this.compactionMetrics.markCompactionRolledBack();
    } finally {
      // reset the status
      reset(instant, table.isMetadataTable());
    }
  }

  @SuppressWarnings("unchecked")
  private void doCommit(
      HoodieFlinkTable<?> table,
      HoodieFlinkWriteClient writeClient,
      String instant,
      Collection<CompactionCommitEvent> events,
      boolean isLogCompaction) throws IOException {
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
      if (isLogCompaction) {
        CompactionUtil.rollbackLogCompaction(table, instant, writeClient.getTransactionManager());
      } else {
        CompactionUtil.rollbackCompaction(table, instant, writeClient.getTransactionManager());
      }
      this.compactionMetrics.markCompactionRolledBack();
      return;
    }

    WriteOperationType operationType = isLogCompaction ? WriteOperationType.LOG_COMPACT : WriteOperationType.COMPACT;
    HoodieCommitMetadata metadata = CompactHelpers.getInstance().createCompactionMetadata(
        table, instant, HoodieListData.eager(statuses), writeClient.getConfig().getSchema(), operationType);

    // commit the compaction
    if (isLogCompaction) {
      writeClient.completeLogCompaction(metadata, table, instant);
    } else {
      writeClient.completeCompaction(metadata, table, instant);
    }

    this.compactionMetrics.updateCommitMetrics(instant, metadata);
    this.compactionMetrics.markCompactionCompleted();

    // Whether to clean up the old log file when compaction
    if (!conf.get(FlinkOptions.CLEAN_ASYNC_ENABLED) && !isCleaning) {
      writeClient.clean();
    }
  }

  private void reset(String instant, boolean isMetadata) {
    if (isMetadata) {
      this.metadataCommitBuffer.remove(instant);
      this.metadataCompactionPlanCache.remove(instant);
    } else {
      this.commitBuffer.remove(instant);
      this.compactionPlanCache.remove(instant);
    }
  }

  private void registerMetrics() {
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    compactionMetrics = new FlinkCompactionMetrics(metrics);
    compactionMetrics.registerMetrics();
  }
}
