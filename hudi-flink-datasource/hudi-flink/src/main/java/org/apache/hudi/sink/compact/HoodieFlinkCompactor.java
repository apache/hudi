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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hudi.async.HoodieAsyncTableService;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.compact.strategy.CompactionPlanSelectStrategy;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.StreamerUtil;

import com.beust.jcommander.JCommander;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Flink hudi compaction program that can be executed manually.
 */
public class HoodieFlinkCompactor {

  protected static final Logger LOG = LoggerFactory.getLogger(HoodieFlinkCompactor.class);

  /**
   * Flink Execution Environment.
   */
  private final AsyncCompactionService compactionScheduleService;

  public HoodieFlinkCompactor(AsyncCompactionService service) {
    this.compactionScheduleService = service;
  }

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    FlinkCompactionConfig cfg = getFlinkCompactionConfig(args);
    Configuration conf = FlinkCompactionConfig.toFlinkConfig(cfg);

    AsyncCompactionService service = new AsyncCompactionService(cfg, conf, env);

    new HoodieFlinkCompactor(service).start(cfg.serviceMode);
  }

  /**
   * Main method to start compaction service.
   */
  public void start(boolean serviceMode) throws Exception {
    if (serviceMode) {
      compactionScheduleService.start(null);
      try {
        compactionScheduleService.waitForShutdown();
      } catch (Exception e) {
        throw new HoodieException(e.getMessage(), e);
      } finally {
        LOG.info("Shut down hoodie flink compactor");
      }
    } else {
      LOG.info("Hoodie Flink Compactor running only single round");
      try {
        compactionScheduleService.compact();
      } catch (Exception e) {
        LOG.error("Got error running delta sync once. Shutting down", e);
        throw e;
      } finally {
        LOG.info("Shut down hoodie flink compactor");
      }
    }
  }

  public static FlinkCompactionConfig getFlinkCompactionConfig(String[] args) {
    FlinkCompactionConfig cfg = new FlinkCompactionConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    return cfg;
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * Schedules compaction in service.
   */
  public static class AsyncCompactionService extends HoodieAsyncTableService {
    private static final long serialVersionUID = 1L;

    /**
     * Flink Compaction Config.
     */
    private final FlinkCompactionConfig cfg;

    /**
     * Flink Config.
     */
    private final Configuration conf;

    /**
     * Meta Client.
     */
    private final HoodieTableMetaClient metaClient;

    /**
     * Write Client.
     */
    private final HoodieFlinkWriteClient<?> writeClient;

    /**
     * The hoodie table.
     */
    private final HoodieFlinkTable<?> table;

    /**
     * Flink Execution Environment.
     */
    private final StreamExecutionEnvironment env;

    /**
     * Executor Service.
     */
    private final ExecutorService executor;

    public AsyncCompactionService(FlinkCompactionConfig cfg, Configuration conf, StreamExecutionEnvironment env) throws Exception {
      this.cfg = cfg;
      this.conf = conf;
      this.env = env;
      this.executor = Executors.newFixedThreadPool(1);

      // create metaClient
      this.metaClient = StreamerUtil.createMetaClient(conf);

      // get the table name
      conf.setString(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

      // set table schema
      CompactionUtil.setAvroSchema(conf, metaClient);

      // infer changelog mode
      CompactionUtil.inferChangelogMode(conf, metaClient);

      this.writeClient = StreamerUtil.createWriteClient(conf);
      this.writeConfig = writeClient.getConfig();
      this.table = writeClient.getHoodieTable();
    }

    @Override
    protected Pair<CompletableFuture, ExecutorService> startService() {
      return Pair.of(CompletableFuture.supplyAsync(() -> {
        boolean error = false;

        try {
          while (!isShutdownRequested()) {
            try {
              compact();
              Thread.sleep(cfg.minCompactionIntervalSeconds * 1000);
            } catch (Exception e) {
              LOG.error("Shutting down compaction service due to exception", e);
              error = true;
              throw new HoodieException(e.getMessage(), e);
            }
          }
        } finally {
          shutdownAsyncService(error);
        }
        return true;
      }, executor), executor);
    }

    private void compact() throws Exception {
      table.getMetaClient().reloadActiveTimeline();

      // checks the compaction plan and do compaction.
      if (cfg.schedule) {
        Option<String> compactionInstantTimeOption = CompactionUtil.getCompactionInstantTime(metaClient);
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

      // fetch the instant based on the configured execution sequence
      HoodieTimeline timeline = table.getActiveTimeline();
      List<HoodieInstant> requested = ((CompactionPlanSelectStrategy) ReflectionUtils.loadClass(cfg.compactionPlanSelectStrategy))
          .select(timeline.filterPendingCompactionTimeline(), cfg);
      if (requested.isEmpty()) {
        // do nothing.
        LOG.info("No compaction plan scheduled, turns on the compaction plan schedule with --schedule option");
        return;
      }

      List<String> compactionInstantTimes = requested.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
      compactionInstantTimes.forEach(timestamp -> {
        HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(timestamp);
        if (timeline.containsInstant(inflightInstant)) {
          LOG.info("Rollback inflight compaction instant: [" + timestamp + "]");
          table.rollbackInflightCompaction(inflightInstant);
          table.getMetaClient().reloadActiveTimeline();
        }
      });

      // generate timestamp and compaction plan pair
      // should support configurable commit metadata
      List<Pair<String, HoodieCompactionPlan>> compactionPlans = compactionInstantTimes.stream()
          .map(timestamp -> {
            try {
              return Pair.of(timestamp, CompactionUtils.getCompactionPlan(table.getMetaClient(), timestamp));
            } catch (IOException e) {
              throw new HoodieException(e);
            }
          })
          // reject empty compaction plan
          .filter(pair -> !(pair.getRight() == null
              || pair.getRight().getOperations() == null
              || pair.getRight().getOperations().isEmpty()))
          .collect(Collectors.toList());

      if (compactionPlans.isEmpty()) {
        // No compaction plan, do nothing and return.
        LOG.info("No compaction plan for instant " + String.join(",", compactionInstantTimes));
        return;
      }

      List<HoodieInstant> instants = compactionInstantTimes.stream().map(HoodieTimeline::getCompactionRequestedInstant).collect(Collectors.toList());
      HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
      for (HoodieInstant instant : instants) {
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
      }

      // get compactionParallelism.
      int compactionParallelism = conf.getInteger(FlinkOptions.COMPACTION_TASKS) == -1
          ? Math.toIntExact(compactionPlans.stream().mapToLong(pair -> pair.getRight().getOperations().size()).sum())
          : conf.getInteger(FlinkOptions.COMPACTION_TASKS);

      LOG.info("Start to compaction for instant " + compactionInstantTimes);

      // Mark instant as compaction inflight
      for (HoodieInstant instant : instants) {
        table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);
      }
      table.getMetaClient().reloadActiveTimeline();

      // use side-output to make operations that is in the same plan to be placed in the same stream
      // keyby() cannot sure that different operations are in the different stream
      Pair<String, HoodieCompactionPlan> firstPlan = compactionPlans.get(0);
      DataStream<CompactionPlanEvent> source = env.addSource(new CompactionPlanSourceFunction(firstPlan.getRight(), firstPlan.getLeft()))
          .name("compaction_source " + firstPlan.getLeft())
          .uid("uid_compaction_source " + firstPlan.getLeft());
      if (compactionPlans.size() > 1) {
        for (Pair<String, HoodieCompactionPlan> pair : compactionPlans.subList(1, compactionPlans.size())) {
          source = source.union(env.addSource(new CompactionPlanSourceFunction(pair.getRight(), pair.getLeft()))
              .name("compaction_source " + pair.getLeft())
              .uid("uid_compaction_source " + pair.getLeft()));
        }
      }

      SingleOutputStreamOperator<Void> operator = source.rebalance()
          .transform("compact_task",
              TypeInformation.of(CompactionCommitEvent.class),
              new ProcessOperator<>(new CompactFunction(conf)))
          .setParallelism(compactionParallelism)
          .process(new ProcessFunction<CompactionCommitEvent, Void>() {
            @Override
            public void processElement(CompactionCommitEvent event, ProcessFunction<CompactionCommitEvent, Void>.Context context, Collector<Void> out) {
              context.output(new OutputTag<>(event.getInstant(), TypeInformation.of(CompactionCommitEvent.class)), event);
            }
          })
          .name("group_by_compaction_plan")
          .uid("uid_group_by_compaction_plan")
          .setParallelism(1);

      compactionPlans.forEach(pair ->
          operator.getSideOutput(new OutputTag<>(pair.getLeft(), TypeInformation.of(CompactionCommitEvent.class)))
              .addSink(new CompactionCommitSink(conf))
              .name("clean_commits")
              .uid("uid_clean_commits")
              .setParallelism(1));

      env.execute("flink_hudi_compaction_" + String.join(",", compactionInstantTimes));
    }

    /**
     * Shutdown async services like compaction/clustering as DeltaSync is shutdown.
     */
    public void shutdownAsyncService(boolean error) {
      LOG.info("Gracefully shutting down compactor. Error ?" + error);
      executor.shutdown();
      writeClient.close();
    }

    @VisibleForTesting
    public void shutDown() {
      shutdownAsyncService(false);
    }
  }
}
