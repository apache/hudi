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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class HoodieCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieCompactor.class);
  public static final String EXECUTE = "execute";
  public static final String SCHEDULE = "schedule";
  public static final String SCHEDULE_AND_EXECUTE = "scheduleandexecute";
  private final Config cfg;
  private transient FileSystem fs;
  private final TypedProperties props;
  private final JavaSparkContext jsc;
  private HoodieTableMetaClient metaClient;

  public HoodieCompactor(JavaSparkContext jsc, Config cfg) {
    this(jsc, cfg, UtilHelpers.buildProperties(jsc.hadoopConfiguration(), cfg.propsFilePath, cfg.configs),
        UtilHelpers.createMetaClient(jsc, cfg.basePath, true));
  }

  public HoodieCompactor(JavaSparkContext jsc, Config cfg, TypedProperties props, HoodieTableMetaClient metaClient) {
    this.cfg = cfg;
    this.jsc = jsc;
    this.props = props;
    this.metaClient = metaClient;
    // Disable async cleaning, will trigger synchronous cleaning manually.
    this.props.put(HoodieCleanConfig.ASYNC_CLEAN.key(), false);
    if (this.metaClient.getTableConfig().isMetadataTableAvailable()) {
      // add default lock config options if MDT is enabled.
      UtilHelpers.addLockOptions(cfg.basePath, this.metaClient.getBasePath().toUri().getScheme(),  this.props);
    }
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = true)
    public String basePath = null;
    @Parameter(names = {"--table-name", "-tn"}, description = "Table name", required = true)
    public String tableName = null;
    @Parameter(names = {"--instant-time", "-it"}, description = "Compaction Instant time", required = false)
    public String compactionInstantTime = null;
    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for hoodie insert", required = false)
    public int parallelism = 200;
    @Parameter(names = {"--schema-file", "-sf"}, description = "path for Avro schema file", required = false)
    public String schemaFile = null;
    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;
    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = null;
    @Parameter(names = {"--retry", "-rt"}, description = "number of retries", required = false)
    public int retry = 0;
    @Parameter(names = {"--skip-clean", "-sc"}, description = "do not trigger clean after compaction", required = false)
    public Boolean skipClean = true;
    @Parameter(names = {"--schedule", "-sch"}, description = "Schedule compaction", required = false)
    public Boolean runSchedule = false;
    @Parameter(names = {"--mode", "-m"}, description = "Set job mode: Set \"schedule\" means make a compact plan; "
        + "Set \"execute\" means execute a compact plan at given instant which means --instant-time is needed here; "
        + "Set \"scheduleAndExecute\" means make a compact plan first and execute that plan immediately", required = false)
    public String runningMode = null;
    @Parameter(names = {"--strategy", "-st"}, description = "Strategy Class", required = false)
    public String strategyClassName = LogFileSizeBasedCompactionStrategy.class.getName();
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for compacting")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Override
    public String toString() {
      return "HoodieCompactorConfig {\n"
          + "   --base-path " + basePath + ", \n"
          + "   --table-name " + tableName + ", \n"
          + "   --instant-time " + compactionInstantTime + ", \n"
          + "   --parallelism " + parallelism + ", \n"
          + "   --schema-file " + schemaFile + ", \n"
          + "   --spark-master " + sparkMaster + ", \n"
          + "   --spark-memory " + sparkMemory + ", \n"
          + "   --skipClean " + skipClean + ", \n"
          + "   --retry " + retry + ", \n"
          + "   --schedule " + runSchedule + ", \n"
          + "   --mode " + runningMode + ", \n"
          + "   --strategy " + strategyClassName + ", \n"
          + "   --props " + propsFilePath + ", \n"
          + "   --hoodie-conf " + configs
          + "\n}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Config config = (Config) o;
      return basePath.equals(config.basePath)
          && Objects.equals(tableName, config.tableName)
          && Objects.equals(compactionInstantTime, config.compactionInstantTime)
          && Objects.equals(parallelism, config.parallelism)
          && Objects.equals(schemaFile, config.schemaFile)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(retry, config.retry)
          && Objects.equals(skipClean, config.skipClean)
          && Objects.equals(runSchedule, config.runSchedule)
          && Objects.equals(runningMode, config.runningMode)
          && Objects.equals(strategyClassName, config.strategyClassName)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, tableName, compactionInstantTime, schemaFile,
          sparkMaster, parallelism, sparkMemory, retry, skipClean, runSchedule, runningMode, strategyClassName, propsFilePath, configs, help);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      throw new HoodieException("Fail to run compaction for " + cfg.tableName + ", return code: " + 1);
    }
    final JavaSparkContext jsc = UtilHelpers.buildSparkContext("compactor-" + cfg.tableName, cfg.sparkMaster, cfg.sparkMemory);
    int ret = new HoodieCompactor(jsc, cfg).compact(cfg.retry);
    if (ret != 0) {
      throw new HoodieException("Fail to run compaction for " + cfg.tableName + ", return code: " + ret);
    }
    LOG.info("Success to run compaction for " + cfg.tableName);
    jsc.stop();
  }

  public int compact(int retry) {
    this.fs = HadoopFSUtils.getFs(cfg.basePath, jsc.hadoopConfiguration());
    // need to do validate in case that users call compact() directly without setting cfg.runningMode
    validateRunningMode(cfg);
    LOG.info(cfg.toString());

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
          return doScheduleAndCompact(jsc);
        }
        case EXECUTE: {
          LOG.info("Running Mode: [" + EXECUTE + "]; Do compaction");
          return doCompact(jsc);
        }
        default: {
          LOG.info("Unsupported running mode [" + cfg.runningMode + "], quit the job directly");
          return -1;
        }
      }
    }, "Compact failed");
    return ret;
  }

  private Integer doScheduleAndCompact(JavaSparkContext jsc) throws Exception {
    LOG.info("Step 1: Do schedule");
    Option<String> instantTime = doSchedule(jsc);
    if (!instantTime.isPresent()) {
      LOG.warn("Couldn't do schedule");
      return -1;
    } else {
      cfg.compactionInstantTime = instantTime.get();
    }

    LOG.info("The schedule instant time is " + instantTime.get());
    LOG.info("Step 2: Do compaction");

    return doCompact(jsc);
  }

  // make sure that cfg.runningMode couldn't be null
  private static void validateRunningMode(Config cfg) {
    // --mode has a higher priority than --schedule
    // If we remove --schedule option in the future we need to change runningMode default value to EXECUTE
    if (StringUtils.isNullOrEmpty(cfg.runningMode)) {
      cfg.runningMode = cfg.runSchedule ? SCHEDULE : EXECUTE;
    }
  }

  private int doCompact(JavaSparkContext jsc) throws Exception {
    // Get schema.
    String schemaStr;
    if (StringUtils.isNullOrEmpty(cfg.schemaFile)) {
      schemaStr = getSchemaFromLatestInstant();
    } else {
      schemaStr = UtilHelpers.parseSchema(fs, cfg.schemaFile);
    }
    LOG.info("Schema --> : " + schemaStr);

    try (SparkRDDWriteClient<HoodieRecordPayload> client =
             UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {
      // If no compaction instant is provided by --instant-time, find the earliest scheduled compaction
      // instant from the active timeline
      if (StringUtils.isNullOrEmpty(cfg.compactionInstantTime)) {
        metaClient = HoodieTableMetaClient.reload(metaClient);
        Option<HoodieInstant> firstCompactionInstant = metaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant();
        if (firstCompactionInstant.isPresent()) {
          cfg.compactionInstantTime = firstCompactionInstant.get().requestedTime();
          LOG.info("Found the earliest scheduled compaction instant which will be executed: "
              + cfg.compactionInstantTime);
        } else {
          LOG.info("There is no scheduled compaction in the table.");
          return 0;
        }
      }
      HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = client.compact(cfg.compactionInstantTime);
      client.commitCompaction(cfg.compactionInstantTime, compactionMetadata, Option.empty());
      clean(client);
      return UtilHelpers.handleErrors(compactionMetadata.getCommitMetadata().get(), cfg.compactionInstantTime);
    }
  }

  private Option<String> doSchedule(JavaSparkContext jsc) {
    try (SparkRDDWriteClient client =
             UtilHelpers.createHoodieClient(jsc, cfg.basePath, "", cfg.parallelism, Option.of(cfg.strategyClassName), props)) {

      return client.scheduleCompaction(Option.empty());
    }
  }

  private String getSchemaFromLatestInstant() throws Exception {
    TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
    Schema schema = schemaUtil.getTableAvroSchema(false);
    return schema.toString();
  }

  private void clean(SparkRDDWriteClient<?> client) {
    if (!cfg.skipClean && client.getConfig().isAutoClean()) {
      client.clean();
    }
  }
}
