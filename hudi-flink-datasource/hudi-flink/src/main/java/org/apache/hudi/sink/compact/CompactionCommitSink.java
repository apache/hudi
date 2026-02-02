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
import org.apache.hudi.metrics.FlinkCompactionMetrics;
import org.apache.hudi.sink.CleanFunction;
import org.apache.hudi.sink.compact.handler.CompactCommitHandler;
import org.apache.hudi.sink.compact.handler.MetadataCompactCommitHandler;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.Lazy;
import org.apache.hudi.util.StreamerUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;

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

  private transient Lazy<CompactCommitHandler> compactCommitHandler;

  private transient Lazy<CompactCommitHandler> mdtCompactCommitHandler;

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
    HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf, getRuntimeContext());
    this.compactCommitHandler = Lazy.lazily(() -> new CompactCommitHandler(conf, writeClient));
    this.mdtCompactCommitHandler = Lazy.lazily(() -> new MetadataCompactCommitHandler(conf, StreamerUtil.createMetadataWriteClient(writeClient)));
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
      mdtCompactCommitHandler.get().commitIfNecessary(event, compactionMetrics);
    } else {
      compactCommitHandler.get().commitIfNecessary(event, compactionMetrics);
    }
  }

  @Override
  public void close() throws Exception {
    if (compactCommitHandler.isInitialized()) {
      compactCommitHandler.get().close();
    }
    if (mdtCompactCommitHandler.isInitialized()) {
      mdtCompactCommitHandler.get().close();

    }
    super.close();
  }

  private long getNumErrorRecords(CompactionCommitEvent event) {
    if (event.getWriteStatuses() == null) {
      return -1L;
    }
    return event.getWriteStatuses().stream()
        .map(WriteStatus::getTotalErrorRecords).reduce(Long::sum).orElse(0L);
  }

  private void registerMetrics() {
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    compactionMetrics = new FlinkCompactionMetrics(metrics);
    compactionMetrics.registerMetrics();
  }
}
