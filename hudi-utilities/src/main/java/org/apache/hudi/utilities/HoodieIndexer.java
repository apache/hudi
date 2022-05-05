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
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.metadata.MetadataPartitionType;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.jetbrains.annotations.TestOnly;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getCompletedMetadataPartitions;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getInflightAndCompletedMetadataPartitions;
import static org.apache.hudi.utilities.UtilHelpers.EXECUTE;
import static org.apache.hudi.utilities.UtilHelpers.SCHEDULE;
import static org.apache.hudi.utilities.UtilHelpers.SCHEDULE_AND_EXECUTE;

/**
 * A tool to run metadata indexing asynchronously.
 * <p>
 * Example command (assuming indexer.properties contains related index configs, see {@link org.apache.hudi.common.config.HoodieMetadataConfig} for configs):
 * <p>
 * spark-submit \
 * --class org.apache.hudi.utilities.HoodieIndexer \
 * /path/to/hudi/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.11.0-SNAPSHOT.jar \
 * --props /path/to/indexer.properties \
 * --mode scheduleAndExecute \
 * --base-path /tmp/hudi_trips_cow \
 * --table-name hudi_trips_cow \
 * --index-types COLUMN_STATS \
 * --parallelism 1 \
 * --spark-memory 1g
 * <p>
 * A sample indexer.properties file:
 * <p>
 * hoodie.metadata.index.async=true
 * hoodie.metadata.index.column.stats.enable=true
 * hoodie.metadata.index.check.timeout.seconds=60
 * hoodie.write.concurrency.mode=optimistic_concurrency_control
 * hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
 */
public class HoodieIndexer {

  private static final Logger LOG = LogManager.getLogger(HoodieIndexer.class);
  static final String DROP_INDEX = "dropindex";

  private final HoodieIndexer.Config cfg;
  private TypedProperties props;
  private final JavaSparkContext jsc;
  private final HoodieTableMetaClient metaClient;

  public HoodieIndexer(JavaSparkContext jsc, HoodieIndexer.Config cfg) {
    this.cfg = cfg;
    this.jsc = jsc;
    this.props = isNullOrEmpty(cfg.propsFilePath)
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
    @Parameter(names = {"--index-types", "-ixt"}, description = "Comma-separated index types to be built, e.g. BLOOM_FILTERS,COLUMN_STATS", required = true)
    public String indexTypes = null;
    @Parameter(names = {"--mode", "-m"}, description = "Set job mode: Set \"schedule\" to generate an indexing plan; "
        + "Set \"execute\" to execute the indexing plan at the given instant, which means --instant-time is required here; "
        + "Set \"scheduleandExecute\" to generate an indexing plan first and execute that plan immediately;"
        + "Set \"dropindex\" to drop the index types specified in --index-types;")
    public String runningMode = null;
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for hoodie client for indexing")
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

  public int start(int retry) {
    // indexing should be done only if metadata is enabled
    if (!props.getBoolean(HoodieMetadataConfig.ENABLE.key())) {
      LOG.error(String.format("Metadata is not enabled. Please set %s to true.", HoodieMetadataConfig.ENABLE.key()));
      return -1;
    }

    // all inflight or completed metadata partitions have already been initialized
    // so enable corresponding indexes in the props so that they're not deleted
    Set<String> initializedMetadataPartitions = getInflightAndCompletedMetadataPartitions(metaClient.getTableConfig());
    LOG.info("Setting props for: " + initializedMetadataPartitions);
    initializedMetadataPartitions.forEach(p -> {
      if (PARTITION_NAME_COLUMN_STATS.equals(p)) {
        props.setProperty(ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
      }
      if (PARTITION_NAME_BLOOM_FILTERS.equals(p)) {
        props.setProperty(ENABLE_METADATA_INDEX_BLOOM_FILTER.key(), "true");
      }
    });

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
        case DROP_INDEX: {
          LOG.info("Running Mode: [" + DROP_INDEX + "];");
          return dropIndex(jsc);
        }
        default: {
          LOG.info("Unsupported running mode [" + cfg.runningMode + "], quit the job directly");
          return -1;
        }
      }
    }, "Indexer failed");
  }

  @TestOnly
  public Option<String> doSchedule() throws Exception {
    return this.scheduleIndexing(jsc);
  }

  private Option<String> scheduleIndexing(JavaSparkContext jsc) throws Exception {
    String schemaStr = UtilHelpers.getSchemaFromLatestInstant(metaClient);
    try (SparkRDDWriteClient<HoodieRecordPayload> client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {
      return doSchedule(client);
    }
  }

  private Option<String> doSchedule(SparkRDDWriteClient<HoodieRecordPayload> client) {
    List<MetadataPartitionType> partitionTypes = getRequestedPartitionTypes(cfg.indexTypes);
    checkArgument(partitionTypes.size() == 1, "Currently, only one index type can be scheduled at a time.");
    if (indexExists(partitionTypes)) {
      return Option.empty();
    }
    Option<String> indexingInstant = client.scheduleIndexing(partitionTypes);
    if (!indexingInstant.isPresent()) {
      LOG.error("Scheduling of index action did not return any instant.");
    }
    return indexingInstant;
  }

  private boolean indexExists(List<MetadataPartitionType> partitionTypes) {
    Set<String> indexedMetadataPartitions = getCompletedMetadataPartitions(metaClient.getTableConfig());
    Set<String> requestedIndexPartitionPaths = partitionTypes.stream().map(MetadataPartitionType::getPartitionPath).collect(Collectors.toSet());
    requestedIndexPartitionPaths.retainAll(indexedMetadataPartitions);
    if (!requestedIndexPartitionPaths.isEmpty()) {
      LOG.error("Following indexes already built: " + requestedIndexPartitionPaths);
      return true;
    }
    return false;
  }

  private int runIndexing(JavaSparkContext jsc) throws Exception {
    String schemaStr = UtilHelpers.getSchemaFromLatestInstant(metaClient);
    try (SparkRDDWriteClient<HoodieRecordPayload> client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {
      if (isNullOrEmpty(cfg.indexInstantTime)) {
        // Instant time is not specified
        // Find the earliest scheduled indexing instant for execution
        Option<HoodieInstant> earliestPendingIndexInstant = metaClient.getActiveTimeline()
            .filterPendingIndexTimeline()
            .firstInstant();
        if (earliestPendingIndexInstant.isPresent()) {
          cfg.indexInstantTime = earliestPendingIndexInstant.get().getTimestamp();
          LOG.info("Found the earliest scheduled indexing instant which will be executed: "
              + cfg.indexInstantTime);
        } else {
          throw new HoodieIndexException("There is no scheduled indexing in the table.");
        }
      }
      return handleResponse(client.index(cfg.indexInstantTime)) ? 0 : 1;
    }
  }

  private int scheduleAndRunIndexing(JavaSparkContext jsc) throws Exception {
    String schemaStr = UtilHelpers.getSchemaFromLatestInstant(metaClient);
    try (SparkRDDWriteClient<HoodieRecordPayload> client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {
      Option<String> indexingInstantTime = doSchedule(client);
      if (indexingInstantTime.isPresent()) {
        return handleResponse(client.index(indexingInstantTime.get())) ? 0 : 1;
      } else {
        return -1;
      }
    }
  }

  private int dropIndex(JavaSparkContext jsc) throws Exception {
    List<MetadataPartitionType> partitionTypes = getRequestedPartitionTypes(cfg.indexTypes);
    String schemaStr = UtilHelpers.getSchemaFromLatestInstant(metaClient);
    try (SparkRDDWriteClient<HoodieRecordPayload> client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {
      client.dropIndex(partitionTypes);
      return 0;
    } catch (Exception e) {
      LOG.error("Failed to drop index. ", e);
      return -1;
    }
  }

  private boolean handleResponse(Option<HoodieIndexCommitMetadata> commitMetadata) {
    if (!commitMetadata.isPresent()) {
      LOG.error("Indexing failed as no commit metadata present.");
      return false;
    }
    List<HoodieIndexPartitionInfo> indexPartitionInfos = commitMetadata.get().getIndexPartitionInfos();
    LOG.info(String.format("Indexing complete for partitions: %s",
        indexPartitionInfos.stream().map(HoodieIndexPartitionInfo::getMetadataPartitionPath).collect(Collectors.toList())));
    return isIndexBuiltForAllRequestedTypes(indexPartitionInfos);
  }

  boolean isIndexBuiltForAllRequestedTypes(List<HoodieIndexPartitionInfo> indexPartitionInfos) {
    Set<String> indexedPartitions = indexPartitionInfos.stream()
        .map(HoodieIndexPartitionInfo::getMetadataPartitionPath).collect(Collectors.toSet());
    Set<String> requestedPartitions = getRequestedPartitionTypes(cfg.indexTypes).stream()
        .map(MetadataPartitionType::getPartitionPath).collect(Collectors.toSet());
    requestedPartitions.removeAll(indexedPartitions);
    return requestedPartitions.isEmpty();
  }

  List<MetadataPartitionType> getRequestedPartitionTypes(String indexTypes) {
    List<String> requestedIndexTypes = Arrays.asList(indexTypes.split(","));
    return requestedIndexTypes.stream()
        .map(p -> MetadataPartitionType.valueOf(p.toUpperCase(Locale.ROOT)))
        // FILES partition is initialized synchronously while getting metadata writer
        .filter(p -> !MetadataPartitionType.FILES.equals(p))
        .collect(Collectors.toList());
  }
}
