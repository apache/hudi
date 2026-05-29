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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.TableServiceUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.compact.strategy.CompactionPlanStrategies;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.client.deployment.application.ApplicationExecutionException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * An asynchronous compaction service for Hudi tables.
 * <p>This service handles the scheduling and execution of compaction operations
 * on data tables in a continuous streaming fashion.
 *
 * <p>The service operates by periodically checking for pending compaction plans,
 * transitioning them from requested to inflight state, and executing the actual
 * compaction process using Flink's streaming capabilities.
 */
public class AsyncCompactionService extends HoodieAsyncTableService {

  private static final long serialVersionUID = 1L;

  /**
   * Flink Compaction Config.
   */
  protected final FlinkCompactionConfig cfg;

  /**
   * Flink Config.
   */
  private final Configuration conf;

  /**
   * Write Client.
   */
  protected final HoodieFlinkWriteClient<?> writeClient;

  /**
   * The hoodie table.
   */
  protected final HoodieFlinkTable<?> table;

  /**
   * Executor Service.
   */
  private final ExecutorService executor;

  public static final String NO_EXECUTE_KEYWORD = "no execute";

  public AsyncCompactionService(FlinkCompactionConfig cfg, Configuration conf) {
    this.cfg = cfg;
    this.conf = conf;
    this.executor = Executors.newFixedThreadPool(1);
    this.writeClient = createWriteClient(conf);
    this.writeConfig = writeClient.getConfig();
    this.table = writeClient.getHoodieTable();
  }

  /**
   * Starts the asynchronous compaction service.
   * This method creates a CompletableFuture that runs the compaction loop
   * in a separate thread, periodically checking for and executing compaction plans.
   *
   * @return A Pair containing the CompletableFuture representing the service and the executor service
   */
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
            if (aee.getMessage() != null && aee.getMessage().contains(NO_EXECUTE_KEYWORD)) {
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

  /**
   * Executes the compaction process.
   * This method orchestrates the entire compaction workflow including
   * scheduling, planning, and executing compaction operations.
   */
  public void compact() throws Exception {
    // Process data table compaction
    table.getMetaClient().reloadActiveTimeline();
    // checks the compaction plan and do compaction.
    if (cfg.schedule) {
      boolean scheduled = scheduleCompaction();
      if (!scheduled) {
        // do nothing.
        LOG.info("No compaction plan for this job ");
        return;
      }
      table.getMetaClient().reloadActiveTimeline();
    }

    // fetch the instant based on the configured execution sequence
    List<HoodieInstant> requested = getCandidateInstants();

    // If retry-last-failed is enabled, check for stale inflight compaction instants
    if (requested.isEmpty() && cfg.retryLastFailedJob && cfg.maxProcessingTimeMs > 0) {
      Option<HoodieInstant> staleInflightInstant = TableServiceUtils.findStaleInflightInstant(
          table.getMetaClient(), HoodieTimeline.COMPACTION_ACTION, cfg.maxProcessingTimeMs);
      if (staleInflightInstant.isPresent()) {
        LOG.info("Found stale inflight compaction instant [{}] exceeding max processing time {}ms. Will rollback and retry.",
            staleInflightInstant.get(), cfg.maxProcessingTimeMs);
        requested = java.util.Collections.singletonList(staleInflightInstant.get());
      }
    }

    if (requested.isEmpty()) {
      // do nothing.
      LOG.info("No compaction plan scheduled, turns on the compaction plan schedule with --schedule option");
      return;
    }

    requested.forEach(instant -> {
      if (instant.isInflight()) {
        rollbackCompactionInstant(instant);
      }
    });
    if (requested.stream().anyMatch(HoodieInstant::isInflight)) {
      table.getMetaClient().reloadActiveTimeline();
    }

    // generate timestamp and compaction plan pair
    // should support configurable commit metadata
    List<Pair<String, HoodieCompactionPlan>> compactionPlans = requested.stream()
        .map(instant -> {
          try {
            return Pair.of(instant.requestedTime(), getCompactionPlan(instant));
          } catch (Exception e) {
            throw new HoodieException("Get compaction plan at instant " + instant.requestedTime() + " error", e);
          }
        })
        // reject empty compaction plan
        .filter(pair -> validCompactionPlan(pair.getRight()))
        .collect(Collectors.toList());

    List<String> compactionInstantTimes = requested.stream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
    if (compactionPlans.isEmpty()) {
      // No compaction plan, do nothing and return.
      LOG.info("No compaction plan for instant {}", String.join(",", compactionInstantTimes));
      return;
    }

    // transition compaction instant to inflight
    transitionCompactionRequestedToInflight(requested);

    int totalOperations = Math.toIntExact(compactionPlans.stream().mapToLong(pair -> pair.getRight().getOperations().size()).sum());
    // get compactionParallelism.
    int compactionParallelism = conf.get(FlinkOptions.COMPACTION_TASKS) == -1
        ? totalOperations
        : Math.min(conf.get(FlinkOptions.COMPACTION_TASKS), totalOperations);

    LOG.info("Start to compaction for data table instant {}", compactionInstantTimes);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.addSource(new CompactionPlanSourceFunction(compactionPlans, conf, cfg.metadataTable))
        .name(getOperatorName("compaction_source"))
        .uid("uid_compaction_source")
        .rebalance()
        .transform(getOperatorName("compact_task"),
            TypeInformation.of(CompactionCommitEvent.class),
            new CompactOperator(conf))
        .setParallelism(compactionParallelism)
        .addSink(new CompactionCommitSink(conf))
        .name(getOperatorName("compaction_commit"))
        .uid("uid_compaction_commit")
        .setParallelism(1)
        .getTransformation()
        .setMaxParallelism(1);

    env.execute("flink_hudi_compaction_" + String.join(",", compactionInstantTimes));
  }

  /**
   * Schedules a new compaction plan for the table.
   * This method uses the write client to generate a compaction plan based on
   * the current state of the table and the configured compaction strategy.
   *
   * @return true if a compaction plan was successfully scheduled, false otherwise
   */
  protected boolean scheduleCompaction() {
    return writeClient.scheduleCompaction(Option.empty()).isPresent();
  }

  /**
   * Retrieves candidate instants for compaction from the timeline.
   *
   * @return List of HoodieInstant objects representing pending compaction instants to process
   */
  protected List<HoodieInstant> getCandidateInstants() {
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    return CompactionPlanStrategies.getStrategy(cfg).select(pendingCompactionTimeline);
  }

  /**
   * Rolls back an inflight compaction instant.
   * This method handles the rollback of a compaction operation that is currently in progress.
   *
   * @param instant The HoodieInstant representing the compaction instant to roll back
   */
  protected void rollbackCompactionInstant(HoodieInstant instant) {
    HoodieInstant inflightInstant = table.getInstantGenerator().getCompactionInflightInstant(instant.requestedTime());
    LOG.info("Rollback inflight compaction instant: [{}]", instant.requestedTime());
    table.rollbackInflightCompaction(inflightInstant, writeClient.getTransactionManager());
  }

  /**
   * Transitions compaction instants from requested to inflight state.
   *
   * @param requested List of HoodieInstant objects to transition to inflight state
   */
  protected void transitionCompactionRequestedToInflight(List<HoodieInstant> requested) {
    InstantGenerator instantGenerator = table.getInstantGenerator();
    List<HoodieInstant> instants = requested.stream()
        .map(instant -> instantGenerator.getCompactionRequestedInstant(instant.requestedTime()))
        .collect(Collectors.toList());
    // Mark instant as compaction inflight
    for (HoodieInstant instant : instants) {
      table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);
    }
    table.getMetaClient().reloadActiveTimeline();
  }

  /**
   * Retrieves the compaction plan for a given instant.
   *
   * @param instant The HoodieInstant for which to retrieve the compaction plan
   * @return The HoodieCompactionPlan containing the details of the compaction operation
   */
  protected HoodieCompactionPlan getCompactionPlan(HoodieInstant instant) {
    return CompactionUtils.getCompactionPlan(table.getMetaClient(), instant.requestedTime());
  }

  private String getOperatorName(String name) {
    return cfg.metadataTable ? "metadata_" + name : name;
  }

  /**
   * Shuts down the asynchronous compaction service gracefully.
   *
   * @param error Indicates whether the shutdown is due to an error condition
   */
  public void shutdownAsyncService(boolean error) {
    LOG.info("Gracefully shutting down compactor. Error: {}",  error);
    executor.shutdown();
    writeClient.close();
  }

  @VisibleForTesting
  public void shutDown() {
    shutdownAsyncService(false);
  }

  private static boolean validCompactionPlan(HoodieCompactionPlan plan) {
    return plan != null && plan.getOperations() != null && plan.getOperations().size() > 0;
  }

  /**
   * Creates a write client for compaction operations.
   * This method initializes a HoodieFlinkWriteClient with the necessary
   * configuration for performing compaction on the data table.
   *
   * @param conf The Flink configuration to use for creating the write client
   *
   * @return A HoodieFlinkWriteClient instance configured for compaction operations
   */
  protected HoodieFlinkWriteClient createWriteClient(Configuration conf) {
    // create metaClient
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    // setup necessary configuration
    try {
      // get the table name
      conf.set(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());
      // get the primary key if absent in conf, but presented in table configs
      if (!conf.containsKey(FlinkOptions.RECORD_KEY_FIELD.key())
          && StringUtils.nonEmpty(metaClient.getTableConfig().getRecordKeyFieldProp())) {
        conf.set(FlinkOptions.RECORD_KEY_FIELD, metaClient.getTableConfig().getRecordKeyFieldProp());
      }
      // set table schema
      CompactionUtil.setAvroSchema(conf, metaClient);
      // set ordering fields
      CompactionUtil.setOrderingFields(conf, metaClient);
      // infer changelog mode
      CompactionUtil.inferChangelogMode(conf, metaClient);
      // infer metadata config
      CompactionUtil.inferMetadataConf(conf, metaClient);
      return FlinkWriteClients.createWriteClientV2(conf);
    } catch (Exception e) {
      throw new HoodieException("Failed to create write client.", e);
    }
  }
}


