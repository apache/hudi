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

import org.apache.hudi.avro.model.HoodieIndexCommitMetadata;
import org.apache.hudi.avro.model.HoodieIndexPartitionInfo;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieIndexException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieInstant.State.INFLIGHT;
import static org.apache.hudi.utilities.UtilHelpers.EXECUTE;
import static org.apache.hudi.utilities.UtilHelpers.SCHEDULE;
import static org.apache.hudi.utilities.UtilHelpers.SCHEDULE_AND_EXECUTE;

public class HoodieIndexer {

  private static final Logger LOG = LogManager.getLogger(HoodieIndexer.class);

  private final HoodieIndexer.Config cfg;
  private TypedProperties props;
  private final JavaSparkContext jsc;
  private final HoodieTableMetaClient metaClient;

  public HoodieIndexer(JavaSparkContext jsc, HoodieIndexer.Config cfg) {
    this.cfg = cfg;
    this.jsc = jsc;
    this.props = StringUtils.isNullOrEmpty(cfg.propsFilePath)
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
    this.metaClient = UtilHelpers.createMetaClient(jsc, cfg.basePath, true);
  }

  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, HoodieIndexer.Config cfg) {
    return UtilHelpers.readConfig(jsc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.configs)
        .getProps(true);
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = true)
    public String basePath = null;
    @Parameter(names = {"--table-name", "-tn"}, description = "Table name", required = true)
    public String tableName = null;
    @Parameter(names = {"--instant-time", "-it"}, description = "Indexing Instant time")
    public String indexInstantTime = null;
    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for hoodie insert", required = true)
    public int parallelism = 1;
    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master")
    public String sparkMaster = null;
    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = true)
    public String sparkMemory = null;
    @Parameter(names = {"--retry", "-rt"}, description = "number of retries")
    public int retry = 0;
    @Parameter(names = {"--schedule", "-sc"}, description = "Schedule indexing")
    public Boolean runSchedule = false;
    @Parameter(names = {"--strategy", "-st"}, description = "Comma-separated index types to be built, e.g. BLOOM,FILES,COLSTATS")
    public String indexTypes = null;
    @Parameter(names = {"--mode", "-m"}, description = "Set job mode: Set \"schedule\" to generate an indexing plan; "
        + "Set \"execute\" to execute the indexing plan at the given instant, which means --instant-time is required here; "
        + "Set \"scheduleAndExecute\" to generate an indexing plan first and execute that plan immediately")
    public String runningMode = null;
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for compacting")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();
  }

  public static void main(String[] args) {
    final HoodieIndexer.Config cfg = new HoodieIndexer.Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    final JavaSparkContext jsc = UtilHelpers.buildSparkContext("indexing-" + cfg.tableName, cfg.sparkMaster, cfg.sparkMemory);
    HoodieIndexer indexer = new HoodieIndexer(jsc, cfg);
    int result = indexer.start(cfg.retry);
    String resultMsg = String.format("Indexing with basePath: %s, tableName: %s, runningMode: %s",
        cfg.basePath, cfg.tableName, cfg.runningMode);
    if (result == -1) {
      LOG.error(resultMsg + " failed");
    } else {
      LOG.info(resultMsg + " success");
    }
    jsc.stop();
  }

  private int start(int retry) {
    return UtilHelpers.retry(retry, () -> {
      switch (cfg.runningMode.toLowerCase()) {
        case SCHEDULE: {
          LOG.info("Running Mode: [" + SCHEDULE + "]; Do schedule");
          Option<String> instantTime = scheduleIndexing(jsc);
          int result = instantTime.isPresent() ? 0 : -1;
          if (result == 0) {
            LOG.info("The schedule instant time is " + instantTime.get());
          }
          return result;
        }
        case SCHEDULE_AND_EXECUTE: {
          LOG.info("Running Mode: [" + SCHEDULE_AND_EXECUTE + "]");
          return scheduleAndRunIndexing(jsc);
        }
        case EXECUTE: {
          LOG.info("Running Mode: [" + EXECUTE + "];");
          return runIndexing(jsc);
        }
        default: {
          LOG.info("Unsupported running mode [" + cfg.runningMode + "], quit the job directly");
          return -1;
        }
      }
    }, "Indexer failed");
  }

  private Option<String> scheduleIndexing(JavaSparkContext jsc) throws Exception {
    String schemaStr = UtilHelpers.getSchemaFromLatestInstant(metaClient);
    try (SparkRDDWriteClient<HoodieRecordPayload> client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {
      return doSchedule(client);
    }
  }

  private Option<String> doSchedule(SparkRDDWriteClient<HoodieRecordPayload> client) {
    List<String> partitionsToIndex = Arrays.asList(cfg.indexTypes.split(","));
    Option<String> indexingInstant = client.scheduleIndexing(partitionsToIndex);
    if (!indexingInstant.isPresent()) {
      LOG.error("Scheduling of index action did not return any instant.");
    }
    return indexingInstant;
  }

  private int runIndexing(JavaSparkContext jsc) throws Exception {
    String schemaStr = UtilHelpers.getSchemaFromLatestInstant(metaClient);
    try (SparkRDDWriteClient<HoodieRecordPayload> client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {
      if (StringUtils.isNullOrEmpty(cfg.indexInstantTime)) {
        // Instant time is not specified
        // Find the earliest scheduled indexing instant for execution
        Option<HoodieInstant> earliestPendingIndexInstant = metaClient.getActiveTimeline()
            .filterPendingIndexTimeline()
            .filter(i -> !(i.isCompleted() || INFLIGHT.equals(i.getState())))
            .firstInstant();
        if (earliestPendingIndexInstant.isPresent()) {
          cfg.indexInstantTime = earliestPendingIndexInstant.get().getTimestamp();
          LOG.info("Found the earliest scheduled indexing instant which will be executed: "
              + cfg.indexInstantTime);
        } else {
          throw new HoodieIndexException("There is no scheduled indexing in the table.");
        }
      }
      return handleError(client.index(cfg.indexInstantTime));
    }
  }

  private int scheduleAndRunIndexing(JavaSparkContext jsc) throws Exception {
    String schemaStr = UtilHelpers.getSchemaFromLatestInstant(metaClient);
    try (SparkRDDWriteClient<HoodieRecordPayload> client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {
      Option<String> indexingInstantTime = doSchedule(client);
      if (indexingInstantTime.isPresent()) {
        return handleError(client.index(indexingInstantTime.get()));
      } else {
        return -1;
      }
    }
  }

  private int handleError(Option<HoodieIndexCommitMetadata> commitMetadata) {
    if (!commitMetadata.isPresent()) {
      LOG.error("Indexing failed as no commit metadata present.");
      return -1;
    }
    List<HoodieIndexPartitionInfo> indexPartitionInfos = commitMetadata.get().getIndexPartitionInfos();
    LOG.info(String.format("Indexing complete for partitions: %s",
        indexPartitionInfos.stream().map(HoodieIndexPartitionInfo::getMetadataPartitionPath).collect(Collectors.toList())));
    return 0;
  }
}
