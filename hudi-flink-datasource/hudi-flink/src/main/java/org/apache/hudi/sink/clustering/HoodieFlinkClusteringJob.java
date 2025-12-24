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

package org.apache.hudi.sink.clustering;

import org.apache.hudi.async.HoodieAsyncTableService;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.compact.HoodieFlinkCompactor;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.ClusteringUtil;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import com.beust.jcommander.JCommander;
import org.apache.avro.Schema;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.client.deployment.application.ApplicationExecutionException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Flink hudi clustering program that can be executed manually.
 */
public class HoodieFlinkClusteringJob {

  protected static final Logger LOG = LoggerFactory.getLogger(HoodieFlinkClusteringJob.class);

  private static final String NO_EXECUTE_KEYWORD = "no execute";

  /**
   * Flink Execution Environment.
   */
  private final AsyncClusteringService clusteringScheduleService;

  public HoodieFlinkClusteringJob(AsyncClusteringService service) {
    this.clusteringScheduleService = service;
  }

  public static void main(String[] args) throws Exception {
    FlinkClusteringConfig cfg = getFlinkClusteringConfig(args);
    Configuration conf = FlinkClusteringConfig.toFlinkConfig(cfg);

    AsyncClusteringService service = new AsyncClusteringService(cfg, conf);

    new HoodieFlinkClusteringJob(service).start(cfg.serviceMode);
  }

  /**
   * Main method to start clustering service.
   */
  public void start(boolean serviceMode) throws Exception {
    if (serviceMode) {
      clusteringScheduleService.start(null);
      try {
        clusteringScheduleService.waitForShutdown();
      } catch (Exception e) {
        throw new HoodieException(e.getMessage(), e);
      } finally {
        LOG.info("Shut down hoodie flink clustering");
      }
    } else {
      LOG.info("Hoodie Flink Clustering running only single round");
      try {
        clusteringScheduleService.cluster();
      } catch (ApplicationExecutionException aee) {
        if (aee.getMessage().contains(NO_EXECUTE_KEYWORD)) {
          LOG.info("Clustering is not performed");
        } else {
          LOG.error("Got error trying to perform clustering. Shutting down", aee);
          throw aee;
        }
      } catch (Exception e) {
        LOG.error("Got error running delta sync once. Shutting down", e);
        throw e;
      } finally {
        LOG.info("Shut down hoodie flink clustering");
      }
    }
  }

  public static FlinkClusteringConfig getFlinkClusteringConfig(String[] args) {
    FlinkClusteringConfig cfg = new FlinkClusteringConfig();
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
   * Schedules clustering in service.
   */
  public static class AsyncClusteringService extends HoodieAsyncTableService {

    private static final long serialVersionUID = 1L;

    /**
     * Flink Clustering Config.
     */
    private final FlinkClusteringConfig cfg;

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

    public AsyncClusteringService(FlinkClusteringConfig cfg, Configuration conf) throws Exception {
      this.cfg = cfg;
      this.conf = conf;
      this.executor = Executors.newFixedThreadPool(1);

      // create metaClient
      this.metaClient = StreamerUtil.createMetaClient(conf);

      // set table name
      conf.setString(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

      // set table type
      conf.setString(FlinkOptions.TABLE_TYPE, metaClient.getTableConfig().getTableType().name());

      // set record key field
      conf.setString(FlinkOptions.RECORD_KEY_FIELD, metaClient.getTableConfig().getRecordKeyFieldProp());

      // set partition field
      conf.setString(FlinkOptions.PARTITION_PATH_FIELD, metaClient.getTableConfig().getPartitionFieldProp());

      // set table schema
      CompactionUtil.setAvroSchema(conf, metaClient);

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
              cluster();
              Thread.sleep(cfg.minClusteringIntervalSeconds * 1000);
            } catch (ApplicationExecutionException aee) {
              if (aee.getMessage().contains(NO_EXECUTE_KEYWORD)) {
                LOG.info("Clustering is not performed.");
              } else {
                throw new HoodieException(aee.getMessage(), aee);
              }
            } catch (Exception e) {
              LOG.error("Shutting down clustering service due to exception", e);
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
     * Follows the same execution methodology of HoodieFlinkCompactor, where only one clustering job is allowed to be
     * executed at any point in time.
     * <p>
     * If there is an inflight clustering job, it will be rolled back and re-attempted.
     * <p>
     * A clustering plan will be generated if `schedule` is true.
     *
     * @throws Exception
     * @see HoodieFlinkCompactor
     */
    private void cluster() throws Exception {
      table.getMetaClient().reloadActiveTimeline();

      if (cfg.schedule) {
        // create a clustering plan on the timeline
        ClusteringUtil.validateClusteringScheduling(conf);

        String clusteringInstantTime = cfg.clusteringInstantTime != null ? cfg.clusteringInstantTime
            : HoodieActiveTimeline.createNewInstantTime();

        LOG.info("Creating a clustering plan for instant [" + clusteringInstantTime + "]");
        boolean scheduled = writeClient.scheduleClusteringAtInstant(clusteringInstantTime, Option.empty());
        if (!scheduled) {
          // do nothing.
          LOG.info("No clustering plan for this job");
          return;
        }
        table.getMetaClient().reloadActiveTimeline();
      }

      // fetch the instant based on the configured execution sequence
      List<HoodieInstant> instants = ClusteringUtils.getPendingClusteringInstantTimes(table.getMetaClient());
      if (instants.isEmpty()) {
        // do nothing.
        LOG.info("No clustering plan scheduled, turns on the clustering plan schedule with --schedule option");
        return;
      }

      final HoodieInstant clusteringInstant;
      if (cfg.clusteringInstantTime != null) {
        clusteringInstant = instants.stream()
            .filter(i -> i.getTimestamp().equals(cfg.clusteringInstantTime))
            .findFirst()
            .orElseThrow(() -> new HoodieException("Clustering instant [" + cfg.clusteringInstantTime + "] not found"));
      } else {
        // check for inflight clustering plans and roll them back if required
        clusteringInstant =
            CompactionUtil.isLIFO(cfg.clusteringSeq) ? instants.get(instants.size() - 1) : instants.get(0);
      }

      HoodieInstant inflightInstant = HoodieTimeline.getReplaceCommitInflightInstant(
          clusteringInstant.getTimestamp());
      if (table.getMetaClient().getActiveTimeline().containsInstant(inflightInstant)) {
        LOG.info("Rollback inflight clustering instant: [" + clusteringInstant + "]");
        table.rollbackInflightClustering(inflightInstant,
            commitToRollback -> writeClient.getTableServiceClient().getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false));
        table.getMetaClient().reloadActiveTimeline();
      }

      // generate clustering plan
      // should support configurable commit metadata
      Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlanOption = ClusteringUtils.getClusteringPlan(
          table.getMetaClient(), clusteringInstant);

      if (!clusteringPlanOption.isPresent()) {
        // do nothing.
        LOG.info("No clustering plan scheduled, turns on the clustering plan schedule with --schedule option");
        return;
      }

      HoodieClusteringPlan clusteringPlan = clusteringPlanOption.get().getRight();

      if (clusteringPlan == null || (clusteringPlan.getInputGroups() == null)
          || (clusteringPlan.getInputGroups().isEmpty())) {
        // no clustering plan, do nothing and return.
        LOG.info("No clustering plan for instant " + clusteringInstant.getTimestamp());
        return;
      }

      HoodieInstant instant = HoodieTimeline.getReplaceCommitRequestedInstant(clusteringInstant.getTimestamp());
      HoodieTimeline pendingClusteringTimeline = table.getActiveTimeline().filterPendingReplaceTimeline();
      if (!pendingClusteringTimeline.containsInstant(instant)) {
        // this means that the clustering plan was written to auxiliary path(.tmp)
        // but not the meta path(.hoodie), this usually happens when the job crush
        // exceptionally.

        // clean the clustering plan in auxiliary path and cancels the clustering.
        LOG.warn("The clustering plan was fetched through the auxiliary path(.tmp) but not the meta path(.hoodie).\n"
            + "Clean the clustering plan in auxiliary path and cancels the clustering");
        CompactionUtil.cleanInstant(table.getMetaClient(), instant);
        return;
      }

      // get clusteringParallelism.
      int clusteringParallelism = conf.getInteger(FlinkOptions.CLUSTERING_TASKS) == -1
          ? clusteringPlan.getInputGroups().size() : conf.getInteger(FlinkOptions.CLUSTERING_TASKS);

      // Mark instant as clustering inflight
      table.getActiveTimeline().transitionReplaceRequestedToInflight(instant, Option.empty());

      final Schema tableAvroSchema = StreamerUtil.getTableAvroSchema(table.getMetaClient(), false);
      final DataType rowDataType = AvroSchemaConverter.convertToDataType(tableAvroSchema);
      final RowType rowType = (RowType) rowDataType.getLogicalType();

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      // setup configuration
      long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
      conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);

      DataStream<ClusteringCommitEvent> dataStream = env.addSource(new ClusteringPlanSourceFunction(clusteringInstant.getTimestamp(), clusteringPlan))
          .name("clustering_source")
          .uid("uid_clustering_source")
          .rebalance()
          .transform("clustering_task",
              TypeInformation.of(ClusteringCommitEvent.class),
              new ClusteringOperator(conf, rowType))
          .setParallelism(clusteringParallelism);

      ExecNodeUtil.setManagedMemoryWeight(dataStream.getTransformation(),
          conf.getInteger(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);

      dataStream
          .addSink(new ClusteringCommitSink(conf))
          .name("clustering_commit")
          .uid("uid_clustering_commit")
          .setParallelism(1)
          .getTransformation()
          .setMaxParallelism(1);

      env.execute("flink_hudi_clustering_" + clusteringInstant.getTimestamp());
    }

    /**
     * Shutdown async services like compaction/clustering as DeltaSync is shutdown.
     */
    public void shutdownAsyncService(boolean error) {
      LOG.info("Gracefully shutting down clustering job. Error ?" + error);
      executor.shutdown();
      writeClient.close();
    }

    @VisibleForTesting
    public void shutDown() {
      shutdownAsyncService(false);
    }
  }
}
