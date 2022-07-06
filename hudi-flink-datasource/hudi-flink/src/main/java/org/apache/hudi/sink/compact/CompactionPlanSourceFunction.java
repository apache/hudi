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

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.StreamerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;

/**
 * Flink hudi compaction source function.
 *
 * <P>This function read the compaction plan as {@link CompactionOperation}s then assign the compaction task
 * event {@link CompactionPlanEvent} to downstream operators.
 *
 * <p>The compaction instant time is specified explicitly with strategies:
 *
 * <ul>
 *   <li>If the timeline has no inflight instants,
 *   use {@link org.apache.hudi.common.table.timeline.HoodieActiveTimeline#createNewInstantTime()}
 *   as the instant time;</li>
 *   <li>If the timeline has inflight instants,
 *   use the median instant time between [last complete instant time, earliest inflight instant time]
 *   as the instant time.</li>
 * </ul>
 */
public class CompactionPlanSourceFunction
    extends AbstractRichFunction implements SourceFunction<CompactionPlanEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(CompactionPlanSourceFunction.class);

  private static final long serialVersionUID = 1L;

  /**
   * The interval between consecutive path scans.
   */
  private final long interval;

  private volatile boolean isRunning = true;

  private final Configuration conf;

  protected HoodieFlinkWriteClient writeClient;

  /**
   * The hoodie table.
   */
  private transient HoodieFlinkTable<?> table;

  /**
   * The path to monitor.
   */
  private final transient Path path;

  private final Boolean isStreamingMode;

  public CompactionPlanSourceFunction(
      Configuration conf,
      String path,
      Boolean isStreamingMode) {
    this.conf = conf;
    this.path = new Path(path);
    this.isStreamingMode = isStreamingMode;
    this.interval = conf.getInteger(FlinkOptions.COMPACTION_STREAMING_CHECK_INTERVAL);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    if (writeClient == null) {
      this.writeClient = StreamerUtil.createWriteClient(conf, getRuntimeContext());
    }
    this.table = this.writeClient.getHoodieTable();
  }

  @Override
  public void run(SourceContext<CompactionPlanEvent> context) throws Exception {
    if (isStreamingMode) {
      while (isRunning) {
        monitorCompactionPlan(context);
        TimeUnit.SECONDS.sleep(interval);
      }
    } else {
      monitorCompactionPlan(context);
    }
  }

  public void monitorCompactionPlan(SourceContext<CompactionPlanEvent> context) throws IOException {
    table.getMetaClient().reloadActiveTimeline();

    if (!this.isStreamingMode) {
      // checks the compaction plan and do compaction.
      if (OptionsResolver.needsScheduleCompaction(conf)) {
        Option<String> compactionInstantTimeOption = CompactionUtil.getCompactionInstantTime(table.getMetaClient());
        if (compactionInstantTimeOption.isPresent()) {
          boolean scheduled = writeClient.scheduleCompactionAtInstant(compactionInstantTimeOption.get(), Option.empty());
          if (!scheduled) {
            // do nothing.
            LOG.info("No compaction plan for this job ");
            return;
          }
          table.getMetaClient().reloadActiveTimeline();
        }
      }
    }

    // fetch the instant based on the configured execution sequence
    String compactionSeq = conf.getString(FlinkOptions.COMPACTION_SEQUENCE);
    HoodieTimeline timeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    Option<HoodieInstant> requested = CompactionUtil.isLIFO(compactionSeq) ? timeline.lastInstant() : timeline.firstInstant();
    if (!requested.isPresent()) {
      // do nothing.
      LOG.info("No compaction plan scheduled, turns on the compaction plan schedule with --schedule option");
      return;
    }

    String compactionInstantTime = requested.get().getTimestamp();

    // generate compaction plan
    // should support configurable commit metadata
    HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
        table.getMetaClient(), compactionInstantTime);

    if (compactionPlan == null || (compactionPlan.getOperations() == null)
        || (compactionPlan.getOperations().isEmpty())) {
      // No compaction plan, do nothing and return.
      LOG.info("No compaction plan for instant " + compactionInstantTime);
      return;
    }

    HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    if (!pendingCompactionTimeline.containsInstant(instant)) {
      // this means that the compaction plan was written to auxiliary path(.tmp)
      // but not the meta path(.hoodie), this usually happens when the job crush
      // exceptionally.

      // clean the compaction plan in auxiliary path and cancels the compaction.

      LOG.warn("The compaction plan was fetched through the auxiliary path(.tmp) but not the meta path(.hoodie).\n"
          + "Clean the compaction plan in auxiliary path and cancels the compaction");
      CompactionUtil.cleanInstant(table.getMetaClient(), instant);
      return;
    }

    LOG.info("Start to compaction for instant " + compactionInstantTime);

    // Mark instant as compaction inflight
    table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);
    table.getMetaClient().reloadActiveTimeline();

    List<CompactionOperation> operations = compactionPlan.getOperations().stream()
        .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
    LOG.info("CompactionPlanFunction compacting " + operations + " files");
    for (CompactionOperation operation : operations) {
      context.collect(new CompactionPlanEvent(compactionInstantTime, operation));
    }
  }

  @Override
  public void close() throws Exception {
    super.close();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Closed CompactionPlan Source for path: " + path + ".");
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }
}
