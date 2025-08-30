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

import org.apache.hudi.async.HoodieAsyncTableService;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.compact.strategy.CompactionPlanStrategies;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import com.beust.jcommander.JCommander;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.client.deployment.application.ApplicationExecutionException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final String NO_EXECUTE_KEYWORD = "no execute";

  /**
   * Flink Execution Environment.
   */
  private final AsyncCompactionService compactionScheduleService;

  public HoodieFlinkCompactor(AsyncCompactionService service) {
    this.compactionScheduleService = service;
  }

  public static void main(String[] args) throws Exception {
    FlinkCompactionConfig cfg = getFlinkCompactionConfig(args);
    Configuration conf = FlinkCompactionConfig.toFlinkConfig(cfg);

    AsyncCompactionService service = new AsyncCompactionService(cfg, conf);

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
      } catch (ApplicationExecutionException aee) {
        if (aee.getMessage().contains(NO_EXECUTE_KEYWORD)) {
          LOG.info("Compaction is not performed");
        } else {
          throw aee;
        }
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
     * Executor Service.
     */
    private final ExecutorService executor;

    public AsyncCompactionService(FlinkCompactionConfig cfg, Configuration conf) throws Exception {
      this.cfg = cfg;
      this.conf = conf;
      this.executor = Executors.newFixedThreadPool(1);

      // create metaClient
      this.metaClient = StreamerUtil.createMetaClient(conf);

      // get the table name
      conf.set(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

      // set table schema
      CompactionUtil.setAvroSchema(conf, metaClient);

      CompactionUtil.setOrderingFields(conf, metaClient);

      // infer changelog mode
      CompactionUtil.inferChangelogMode(conf, metaClient);

      // infer metadata config
      CompactionUtil.inferMetadataConf(conf, metaClient);

      this.writeClient = FlinkWriteClients.createWriteClientV2(conf);
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
            } catch (ApplicationExecutionException aee) {
              if (aee.getMessage().contains(NO_EXECUTE_KEYWORD)) {
                LOG.info("Compaction is not performed.");
              } else {
                throw new HoodieException(aee.getMessage(), aee);
              }
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
        boolean scheduled = writeClient.scheduleCompaction(Option.empty()).isPresent();
        if (!scheduled) {
          // do nothing.
          LOG.info("No compaction plan for this job ");
          return;
        }
        table.getMetaClient().reloadActiveTimeline();
      }

      // fetch the instant based on the configured execution sequence
      HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
      List<HoodieInstant> requested = CompactionPlanStrategies.getStrategy(cfg).select(pendingCompactionTimeline);
      if (requested.isEmpty()) {
        // do nothing.
        LOG.info("No compaction plan scheduled, turns on the compaction plan schedule with --schedule option");
        return;
      }

      List<String> compactionInstantTimes = requested.stream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
      compactionInstantTimes.forEach(timestamp -> {
        HoodieInstant inflightInstant = table.getInstantGenerator().getCompactionInflightInstant(timestamp);
        if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
          LOG.info("Rollback inflight compaction instant: [" + timestamp + "]");
          table.rollbackInflightCompaction(inflightInstant, writeClient.getTransactionManager());
          table.getMetaClient().reloadActiveTimeline();
        }
      });

      // generate timestamp and compaction plan pair
      // should support configurable commit metadata
      List<Pair<String, HoodieCompactionPlan>> compactionPlans = compactionInstantTimes.stream()
          .map(timestamp -> {
            try {
              return Pair.of(timestamp, CompactionUtils.getCompactionPlan(table.getMetaClient(), timestamp));
            } catch (Exception e) {
              throw new HoodieException("Get compaction plan at instant " + timestamp + " error", e);
            }
          })
          // reject empty compaction plan
          .filter(pair -> validCompactionPlan(pair.getRight()))
          .collect(Collectors.toList());

      if (compactionPlans.isEmpty()) {
        // No compaction plan, do nothing and return.
        LOG.info("No compaction plan for instant " + String.join(",", compactionInstantTimes));
        return;
      }

      InstantGenerator instantGenerator = table.getInstantGenerator();
      List<HoodieInstant> instants = compactionInstantTimes.stream().map(instantGenerator::getCompactionRequestedInstant).collect(Collectors.toList());

      int totalOperations = Math.toIntExact(compactionPlans.stream().mapToLong(pair -> pair.getRight().getOperations().size()).sum());

      // get compactionParallelism.
      int compactionParallelism = conf.get(FlinkOptions.COMPACTION_TASKS) == -1
          ? totalOperations
          : Math.min(conf.get(FlinkOptions.COMPACTION_TASKS), totalOperations);

      LOG.info("Start to compaction for instant " + compactionInstantTimes);

      // Mark instant as compaction inflight
      for (HoodieInstant instant : instants) {
        table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);
      }
      table.getMetaClient().reloadActiveTimeline();

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.addSource(new CompactionPlanSourceFunction(compactionPlans, conf))
          .name("compaction_source")
          .uid("uid_compaction_source")
          .rebalance()
          .transform("compact_task",
              TypeInformation.of(CompactionCommitEvent.class),
              new CompactOperator(conf))
          .setParallelism(compactionParallelism)
          .addSink(new CompactionCommitSink(conf))
          .name("compaction_commit")
          .uid("uid_compaction_commit")
          .setParallelism(1)
          .getTransformation()
          .setMaxParallelism(1);

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

  private static boolean validCompactionPlan(HoodieCompactionPlan plan) {
    return plan != null && plan.getOperations() != null && plan.getOperations().size() > 0;
  }
}
