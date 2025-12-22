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

package org.apache.hudi.utilities;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieSparkTable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.hudi.utilities.UtilHelpers.EXECUTE;
import static org.apache.hudi.utilities.UtilHelpers.PURGE_PENDING_INSTANT;
import static org.apache.hudi.utilities.UtilHelpers.SCHEDULE;
import static org.apache.hudi.utilities.UtilHelpers.SCHEDULE_AND_EXECUTE;

public class HoodieClusteringJob {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieClusteringJob.class);
  private final Config cfg;
  private final TypedProperties props;
  private final JavaSparkContext jsc;
  private HoodieTableMetaClient metaClient;

  public HoodieClusteringJob(JavaSparkContext jsc, Config cfg) {
    this.cfg = cfg;
    this.jsc = jsc;
    this.props = StringUtils.isNullOrEmpty(cfg.propsFilePath)
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
    // Disable async cleaning, will trigger synchronous cleaning manually.
    this.props.put(HoodieCleanConfig.ASYNC_CLEAN.key(), false);
    this.metaClient = UtilHelpers.createMetaClient(jsc, cfg.basePath, true);
    if (this.metaClient.getTableConfig().isMetadataTableAvailable()) {
      // add default lock config options if MDT is enabled.
      UtilHelpers.addLockOptions(cfg.basePath, this.props);
    }
  }

  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, Config cfg) {
    return UtilHelpers.readConfig(jsc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.configs)
        .getProps(true);
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = true)
    public String basePath = null;
    @Parameter(names = {"--table-name", "-tn"}, description = "Table name", required = true)
    public String tableName = null;
    @Parameter(names = {"--instant-time", "-it"}, description = "Clustering Instant time, only used when set --mode execute. "
        + "If the instant time is not provided with --mode execute, "
        + "the earliest scheduled clustering instant time is used by default. "
        + "When set \"--mode scheduleAndExecute\" this instant-time will be ignored.")
    public String clusteringInstantTime = null;
    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for hoodie insert")
    public int parallelism = 1;
    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master")
    public String sparkMaster = null;
    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = null;
    @Parameter(names = {"--retry", "-rt"}, description = "number of retries")
    public int retry = 0;

    @Parameter(names = {"--schedule", "-sc"}, description = "Schedule clustering @desperate soon please use \"--mode schedule\" instead")
    public Boolean runSchedule = false;

    @Parameter(names = {"--retry-last-failed-clustering-job", "-rc"}, description = "Take effect when using --mode/-m scheduleAndExecute. Set true means "
        + "check, rollback and execute last failed clustering plan instead of planing a new clustering job directly.")
    public Boolean retryLastFailedClusteringJob = false;

    @Parameter(names = {"--mode", "-m"}, description = "Set job mode: Set \"schedule\" means make a cluster plan; "
        + "Set \"execute\" means execute a cluster plan at given instant which means --instant-time is needed here; "
        + "Set \"scheduleAndExecute\" means make a cluster plan first and execute that plan immediately")
    public String runningMode = null;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Parameter(names = {"--job-max-processing-time-ms", "-jt"}, description = "Take effect when using --mode/-m scheduleAndExecute and --retry-last-failed-clustering-job/-rc true. "
        + "If maxProcessingTimeMs passed but clustering job is still unfinished, hoodie would consider this job as failed and relaunch.")
    public long maxProcessingTimeMs = 0;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for clustering")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Override
    public String toString() {
      return "HoodieClusteringJobConfig{\n"
          + "   --base-path " + basePath + ", \n"
          + "   --table-name " + tableName + ", \n"
          + "   --instant-time " + clusteringInstantTime + ", \n"
          + "   --parallelism " + parallelism + ", \n"
          + "   --spark-master " + sparkMaster + ", \n"
          + "   --spark-memory " + sparkMemory + ", \n"
          + "   --retry " + retry + ", \n"
          + "   --schedule " + runSchedule + ", \n"
          + "   --retry-last-failed-clustering-job " + retryLastFailedClusteringJob + ", \n"
          + "   --mode " + runningMode + ", \n"
          + "   --job-max-processing-time-ms " + maxProcessingTimeMs + ", \n"
          + "   --props " + propsFilePath + ", \n"
          + "   --hoodie-conf " + configs + ", \n"
          + "\n}";
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      throw new HoodieException("Clustering failed for basePath: " + cfg.basePath);
    }

    final JavaSparkContext jsc = UtilHelpers.buildSparkContext("clustering-" + cfg.tableName, cfg.sparkMaster, cfg.sparkMemory);
    int result = new HoodieClusteringJob(jsc, cfg).cluster(cfg.retry);
    String resultMsg = String.format("Clustering with basePath: %s, tableName: %s, runningMode: %s",
        cfg.basePath, cfg.tableName, cfg.runningMode);
    if (result != 0) {
      throw new HoodieException(resultMsg + " failed");
    }
    LOG.info(resultMsg + " success");
    jsc.stop();
  }

  // make sure that cfg.runningMode couldn't be null
  private static void validateRunningMode(Config cfg) {
    // --mode has a higher priority than --schedule
    // If we remove --schedule option in the future we need to change runningMode default value to EXECUTE
    if (StringUtils.isNullOrEmpty(cfg.runningMode)) {
      cfg.runningMode = cfg.runSchedule ? SCHEDULE : EXECUTE;
    }
  }

  public int cluster(int retry) {
    // need to do validate in case that users call cluster() directly without setting cfg.runningMode
    validateRunningMode(cfg);
    return UtilHelpers.retry(retry, () -> {
      switch (cfg.runningMode.toLowerCase()) {
        case SCHEDULE: {
          LOG.info("Running Mode: [" + SCHEDULE + "]; Do schedule");
          Option<String> instantTime = doSchedule(jsc);
          int result = instantTime.isPresent() ? 0 : -1;
          if (result == 0) {
            LOG.info("The schedule instant time is " + instantTime.get());
          }
          return result;
        }
        case SCHEDULE_AND_EXECUTE: {
          LOG.info("Running Mode: [" + SCHEDULE_AND_EXECUTE + "]");
          return doScheduleAndCluster(jsc);
        }
        case EXECUTE: {
          LOG.info("Running Mode: [" + EXECUTE + "]; Do cluster");
          return doCluster(jsc);
        }
        case PURGE_PENDING_INSTANT: {
          LOG.info("Running Mode: [" + PURGE_PENDING_INSTANT + "];");
          return doPurgePendingInstant(jsc);
        }
        default: {
          LOG.error("Unsupported running mode [" + cfg.runningMode + "], quit the job directly");
          return -1;
        }
      }
    }, "Cluster failed");
  }

  private int doCluster(JavaSparkContext jsc) throws Exception {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    String schemaStr = UtilHelpers.getSchemaFromLatestInstant(metaClient);
    try (SparkRDDWriteClient<HoodieRecordPayload> client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {
      if (StringUtils.isNullOrEmpty(cfg.clusteringInstantTime)) {
        // Instant time is not specified
        // Find the earliest scheduled clustering instant for execution
        Option<HoodieInstant> firstClusteringInstant =
            metaClient.getActiveTimeline().getFirstPendingClusterInstant();
        if (firstClusteringInstant.isPresent()) {
          cfg.clusteringInstantTime = firstClusteringInstant.get().getTimestamp();
          LOG.info("Found the earliest scheduled clustering instant which will be executed: "
              + cfg.clusteringInstantTime);
        } else {
          LOG.info("There is no scheduled clustering in the table.");
          return 0;
        }
      }
      Option<HoodieCommitMetadata> commitMetadata = client.cluster(cfg.clusteringInstantTime).getCommitMetadata();
      clean(client);
      return UtilHelpers.handleErrors(commitMetadata.get(), cfg.clusteringInstantTime);
    }
  }

  @TestOnly
  public Option<String> doSchedule() throws Exception {
    return this.doSchedule(jsc);
  }

  private Option<String> doSchedule(JavaSparkContext jsc) throws Exception {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    String schemaStr = UtilHelpers.getSchemaFromLatestInstant(metaClient);
    try (SparkRDDWriteClient<HoodieRecordPayload> client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {
      return doSchedule(client);
    }
  }

  private Option<String> doSchedule(SparkRDDWriteClient<HoodieRecordPayload> client) {
    if (cfg.clusteringInstantTime != null) {
      client.scheduleClusteringAtInstant(cfg.clusteringInstantTime, Option.empty());
      return Option.of(cfg.clusteringInstantTime);
    }
    return client.scheduleClustering(Option.empty());
  }

  private int doScheduleAndCluster(JavaSparkContext jsc) throws Exception {
    LOG.info("Step 1: Do schedule");
    metaClient = HoodieTableMetaClient.reload(metaClient);
    String schemaStr = UtilHelpers.getSchemaFromLatestInstant(metaClient);
    try (SparkRDDWriteClient<HoodieRecordPayload> client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {
      Option<String> instantTime = Option.empty();

      if (cfg.retryLastFailedClusteringJob) {
        HoodieSparkTable<HoodieRecordPayload> table = HoodieSparkTable.create(client.getConfig(), client.getEngineContext());
        Option<HoodieInstant> lastClusterOpt = table.getActiveTimeline().getLastPendingClusterInstant();

        if (lastClusterOpt.isPresent()) {
          HoodieInstant inflightClusteringInstant = lastClusterOpt.get();
          Date clusteringStartTime = HoodieActiveTimeline.parseDateFromInstantTime(inflightClusteringInstant.getTimestamp());
          if (clusteringStartTime.getTime() + cfg.maxProcessingTimeMs < System.currentTimeMillis()) {
            // if there has failed clustering, then we will use the failed clustering instant-time to trigger next clustering action which will rollback and clustering.
            LOG.info("Found failed clustering instant at : " + inflightClusteringInstant + "; Will rollback the failed clustering and re-trigger again.");
            instantTime = Option.of(inflightClusteringInstant.getTimestamp());
          } else {
            LOG.info(inflightClusteringInstant + " might still be in progress, will trigger a new clustering job.");
          }
        }
      }

      instantTime = instantTime.isPresent() ? instantTime : doSchedule(client);
      if (!instantTime.isPresent()) {
        LOG.info("Couldn't generate cluster plan");
        return -1;
      }

      LOG.info("The schedule instant time is " + instantTime.get());
      LOG.info("Step 2: Do cluster");
      Option<HoodieCommitMetadata> metadata = client.cluster(instantTime.get()).getCommitMetadata();
      clean(client);
      return UtilHelpers.handleErrors(metadata.get(), instantTime.get());
    }
  }

  private int doPurgePendingInstant(JavaSparkContext jsc) throws Exception {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    String schemaStr = UtilHelpers.getSchemaFromLatestInstant(metaClient);
    try (SparkRDDWriteClient<HoodieRecordPayload> client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {
      client.purgePendingClustering(cfg.clusteringInstantTime);
    }
    return 0;
  }

  private void clean(SparkRDDWriteClient<?> client) {
    if (client.getConfig().isAutoClean()) {
      client.clean();
    }
  }
}
