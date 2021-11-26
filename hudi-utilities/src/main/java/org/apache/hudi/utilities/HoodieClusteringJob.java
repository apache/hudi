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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieSparkTable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.jetbrains.annotations.TestOnly;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class HoodieClusteringJob {

  public static final String EXECUTE = "execute";
  public static final String SCHEDULE = "schedule";
  public static final String SCHEDULE_AND_EXECUTE = "scheduleandexecute";
  private static final Logger LOG = LogManager.getLogger(HoodieClusteringJob.class);
  private final Config cfg;
  private transient FileSystem fs;
  private TypedProperties props;
  private final JavaSparkContext jsc;
  private final HoodieTableMetaClient metaClient;

  public HoodieClusteringJob(JavaSparkContext jsc, Config cfg) {
    this.cfg = cfg;
    this.jsc = jsc;
    this.props = StringUtils.isNullOrEmpty(cfg.propsFilePath)
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
    this.metaClient = UtilHelpers.createMetaClient(jsc, cfg.basePath, true);
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
        + "When set \"--mode scheduleAndExecute\" this instant-time will be ignored.", required = false)
    public String clusteringInstantTime = null;
    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for hoodie insert", required = false)
    public int parallelism = 1;
    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;
    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = true)
    public String sparkMemory = null;
    @Parameter(names = {"--retry", "-rt"}, description = "number of retries", required = false)
    public int retry = 0;

    @Parameter(names = {"--schedule", "-sc"}, description = "Schedule clustering @desperate soon please use \"--mode schedule\" instead")
    public Boolean runSchedule = false;

    @Parameter(names = {"--retry-last-failed-clustering-job", "-rc"}, description = "Take effect when using --mode/-m scheduleAndExecute. Set true means "
            + "check, rollback and execute last failed clustering plan instead of planing a new clustering job directly.", required = false)
    public Boolean retryLastFailedClusteringJob = false;

    @Parameter(names = {"--mode", "-m"}, description = "Set job mode: Set \"schedule\" means make a cluster plan; "
            + "Set \"execute\" means execute a cluster plan at given instant which means --instant-time is needed here; "
            + "Set \"scheduleAndExecute\" means make a cluster plan first and execute that plan immediately", required = false)
    public String runningMode = null;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Parameter(names = {"--job-max-processing-time-ms", "-jt"}, description = "Take effect when using --mode/-m scheduleAndExecute and --retry-last-failed-clustering-job/-rc true. "
        + "If maxProcessingTimeMs passed but clustering job is still unfinished, hoodie would consider this job as failed and relaunch.", required = false)
    public long maxProcessingTimeMs = 0;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for clustering")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
            splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    final JavaSparkContext jsc = UtilHelpers.buildSparkContext("clustering-" + cfg.tableName, cfg.sparkMaster, cfg.sparkMemory);
    HoodieClusteringJob clusteringJob = new HoodieClusteringJob(jsc, cfg);
    int result = clusteringJob.cluster(cfg.retry);
    String resultMsg = String.format("Clustering with basePath: %s, tableName: %s, runningMode: %s",
        cfg.basePath, cfg.tableName, cfg.runningMode);
    if (result == -1) {
      LOG.error(resultMsg + " failed");
    } else {
      LOG.info(resultMsg + " success");
    }
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
    this.fs = FSUtils.getFs(cfg.basePath, jsc.hadoopConfiguration());
    // need to do validate in case that users call cluster() directly without setting cfg.runningMode
    validateRunningMode(cfg);
    int ret = UtilHelpers.retry(retry, () -> {
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
        default: {
          LOG.info("Unsupported running mode [" + cfg.runningMode + "], quit the job directly");
          return -1;
        }
      }
    }, "Cluster failed");
    return ret;
  }

  private String getSchemaFromLatestInstant() throws Exception {
    TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
    if (metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants() == 0) {
      throw new HoodieException("Cannot run clustering without any completed commits");
    }
    Schema schema = schemaUtil.getTableAvroSchema(false);
    return schema.toString();
  }

  private int doCluster(JavaSparkContext jsc) throws Exception {
    String schemaStr = getSchemaFromLatestInstant();
    try (SparkRDDWriteClient<HoodieRecordPayload> client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {
      if (StringUtils.isNullOrEmpty(cfg.clusteringInstantTime)) {
        // Instant time is not specified
        // Find the earliest scheduled clustering instant for execution
        Option<HoodieInstant> firstClusteringInstant =
            metaClient.getActiveTimeline().firstInstant(
                HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.REQUESTED);
        if (firstClusteringInstant.isPresent()) {
          cfg.clusteringInstantTime = firstClusteringInstant.get().getTimestamp();
          LOG.info("Found the earliest scheduled clustering instant which will be executed: "
              + cfg.clusteringInstantTime);
        } else {
          throw new HoodieClusteringException("There is no scheduled clustering in the table.");
        }
      }
      Option<HoodieCommitMetadata> commitMetadata = client.cluster(cfg.clusteringInstantTime, true).getCommitMetadata();

      return handleErrors(commitMetadata.get(), cfg.clusteringInstantTime);
    }
  }

  @TestOnly
  public Option<String> doSchedule() throws Exception {
    return this.doSchedule(jsc);
  }

  private Option<String> doSchedule(JavaSparkContext jsc) throws Exception {
    String schemaStr = getSchemaFromLatestInstant();
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
    String schemaStr = getSchemaFromLatestInstant();
    try (SparkRDDWriteClient<HoodieRecordPayload> client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {
      Option<String> instantTime = Option.empty();

      if (cfg.retryLastFailedClusteringJob) {
        HoodieSparkTable<HoodieRecordPayload> table = HoodieSparkTable.create(client.getConfig(), client.getEngineContext());
        HoodieTimeline inflightHoodieTimeline = table.getActiveTimeline().filterPendingReplaceTimeline().filterInflights();
        if (!inflightHoodieTimeline.empty()) {
          HoodieInstant inflightClusteringInstant = inflightHoodieTimeline.lastInstant().get();
          Date clusteringStartTime = HoodieActiveTimeline.parseDateFromInstantTime(inflightClusteringInstant.getTimestamp());
          if (clusteringStartTime.getTime() + cfg.maxProcessingTimeMs < System.currentTimeMillis()) {
            // if there has failed clustering, then we will use the failed clustering instant-time to trigger next clustering action which will rollback and clustering.
            LOG.info("Found failed clustering instant at : " + inflightClusteringInstant + "; Will rollback the failed clustering and re-trigger again.");
            instantTime = Option.of(inflightHoodieTimeline.lastInstant().get().getTimestamp());
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
      Option<HoodieCommitMetadata> metadata = client.cluster(instantTime.get(), true).getCommitMetadata();
      return handleErrors(metadata.get(), instantTime.get());
    }
  }

  private int handleErrors(HoodieCommitMetadata metadata, String instantTime) {
    List<HoodieWriteStat> writeStats = metadata.getPartitionToWriteStats().entrySet().stream().flatMap(e ->
        e.getValue().stream()).collect(Collectors.toList());
    long errorsCount = writeStats.stream().mapToLong(HoodieWriteStat::getTotalWriteErrors).sum();
    if (errorsCount == 0) {
      LOG.info(String.format("Table imported into hoodie with %s instant time.", instantTime));
      return 0;
    }

    LOG.error(String.format("Import failed with %d errors.", errorsCount));
    return -1;
  }
}
