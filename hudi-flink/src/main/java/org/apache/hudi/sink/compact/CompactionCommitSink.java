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
import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Function to check and commit the compaction action.
 *
 * <p> Each time after receiving a compaction commit event {@link CompactionCommitEvent},
 * it loads and checks the compaction plan {@link HoodieCompactionPlan},
 * if all the compaction operations {@link org.apache.hudi.common.model.CompactionOperation}
 * of the plan are finished, tries to commit the compaction action.
 */
public class CompactionCommitSink extends RichSinkFunction<CompactionCommitEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(CompactionCommitSink.class);

  /**
   * Config options.
   */
  private final Configuration conf;

  /**
   * Write Client.
   */
  private transient HoodieFlinkWriteClient writeClient;

  /**
   * Buffer to collect the event from each compact task {@code CompactFunction}.
   */
  private transient List<CompactionCommitEvent> commitBuffer;

  /**
   * Current on-going compaction instant time.
   */
  private String compactionInstantTime;

  public CompactionCommitSink(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    initWriteClient();
    this.commitBuffer = new ArrayList<>();
  }

  @Override
  public void invoke(CompactionCommitEvent event, Context context) throws Exception {
    if (compactionInstantTime == null) {
      compactionInstantTime = event.getInstant();
    } else if (!event.getInstant().equals(compactionInstantTime)) {
      // last compaction still not finish, rolls it back
      HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(this.compactionInstantTime);
      writeClient.rollbackInflightCompaction(inflightInstant);
      this.compactionInstantTime = event.getInstant();
    }
    this.commitBuffer.add(event);
    commitIfNecessary();
  }

  /**
   * Condition to commit: the commit buffer has equal size with the compaction plan operations
   * and all the compact commit event {@link CompactionCommitEvent} has the same compaction instant time.
   */
  private void commitIfNecessary() throws IOException {
    HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
        this.writeClient.getHoodieTable().getMetaClient(), compactionInstantTime);
    boolean isReady = compactionPlan.getOperations().size() == commitBuffer.size()
        && commitBuffer.stream().allMatch(event -> event != null && Objects.equals(event.getInstant(), compactionInstantTime));
    if (!isReady) {
      return;
    }
    List<WriteStatus> statuses = this.commitBuffer.stream()
        .map(CompactionCommitEvent::getWriteStatuses)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

    if (this.writeClient.getConfig().shouldAutoCommit()) {
      // Prepare the commit metadata.
      List<HoodieWriteStat> updateStatusMap = statuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
      HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
      for (HoodieWriteStat stat : updateStatusMap) {
        metadata.addWriteStat(stat.getPartitionPath(), stat);
      }
      metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, writeClient.getConfig().getSchema());
      this.writeClient.completeCompaction(
          metadata, statuses, this.writeClient.getHoodieTable(), compactionInstantTime);
    }
    // commit the compaction
    this.writeClient.commitCompaction(compactionInstantTime, statuses, Option.empty());
    // reset the status
    reset();
  }

  private void reset() {
    this.commitBuffer.clear();
    this.compactionInstantTime = null;
  }

  private void initWriteClient() {
    HoodieFlinkEngineContext context =
        new HoodieFlinkEngineContext(
            new SerializableConfiguration(StreamerUtil.getHadoopConf()),
            new FlinkTaskContextSupplier(getRuntimeContext()));

    writeClient = new HoodieFlinkWriteClient<>(context, StreamerUtil.getHoodieClientConfig(this.conf));
  }
}
