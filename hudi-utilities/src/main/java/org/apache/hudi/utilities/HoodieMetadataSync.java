/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.HoodieSparkTable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY;

public class HoodieMetadataSync implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMetadataSync.class);

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
          .setSerializationInclusion(JsonInclude.Include.NON_NULL);

  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;

  public HoodieMetadataSync(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.cfg = cfg;
    this.props = UtilHelpers.buildProperties(cfg.configs);
  }

  /**
   * Reads config from the file system.
   *
   * @param jsc {@link JavaSparkContext} instance.
   * @param cfg {@link TableSizeStats.Config} instance.
   * @return the {@link TypedProperties} instance.
   */
  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, TableSizeStats.Config cfg) {
    return UtilHelpers.readConfig(jsc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.configs)
        .getProps(true);
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--source-base-path", "-sbp"}, description = "Source Base path for the table", required = true)
    public String sourceBasePath = null;

    @Parameter(names = {"--target-base-path", "-tbp"}, description = "Target Base path for the table", required = true)
    public String targetBasePath = null;

    @Parameter(names = {"--target-table-name", "-ttn"}, description = "Target table name", required = true)
    public String targetTableName = null;

    @Parameter(names = {"--commit-to-sync", "-cts"}, description = "Commit of interest to sync", required = false)
    public String commitToSync = null;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "TableSizeStats {\n"
          + "   --source-base-path " + sourceBasePath + ", \n"
          + "   --target-base-path " + targetBasePath + ", \n"
          + "   --commit-to-sync " + commitToSync + ", \n"
          + "   --hoodie-conf " + configs
          + "\n}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Config)) {
        return false;
      }
      Config config = (Config) o;
      return sourceBasePath.equals(config.sourceBasePath) && targetBasePath.equals(config.targetBasePath) && Objects.equals(commitToSync, config.commitToSync) &&
          Objects.equals(sparkMaster, config.sparkMaster) && Objects.equals(sparkMemory, config.sparkMemory);
    }

    @Override
    public int hashCode() {
      return Objects.hash(sourceBasePath, targetBasePath, commitToSync, sparkMaster, sparkMemory);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    SparkConf sparkConf = UtilHelpers.buildSparkConf("HoodieMetadataSync", cfg.sparkMaster);
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      HoodieMetadataSync hoodieMetadataSync = new HoodieMetadataSync(jsc, cfg);
      hoodieMetadataSync.run();
    } catch (TableNotFoundException e) {
      LOG.warn(String.format("The Hudi data table is not found: [%s].", cfg.sourceBasePath), e);
    } catch (Throwable throwable) {
      LOG.error("Failed to get table size stats for " + cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() throws Exception {
    Path sourceTablePath = new Path(cfg.targetBasePath);
    FileSystem fs = sourceTablePath.getFileSystem(jsc.hadoopConfiguration());
    HoodieTableMetaClient sourceTableMetaClient = HoodieTableMetaClient.builder().setBasePath(cfg.sourceBasePath).setConf(jsc.hadoopConfiguration())
        .build();

    Path targetTablePath = new Path(cfg.targetBasePath);
    boolean targetTableExists = fs.exists(targetTablePath);
    if (!targetTableExists) {
      LOG.info("Initializing target table for first time", cfg.targetBasePath);
      TypedProperties props = sourceTableMetaClient.getTableConfig().getProps();
      props.remove("hoodie.table.metadata.partitions");
      props.setProperty(HoodieTableConfig.ALLOW_BASE_PATH_OVERRIDES_WITH_METADATA.key() ,"true");
      props.setProperty(HoodieTableConfig.NUM_PARTITION_PATH_LEVELS.key(), "1");
      HoodieTableMetaClient.withPropertyBuilder()
          .fromProperties(props)
          .setTableName(cfg.targetTableName)
          .initTable(jsc.hadoopConfiguration(), cfg.targetBasePath);
    }
    HoodieTableMetaClient targetTableMetaClient = HoodieTableMetaClient.builder().setBasePath(cfg.targetBasePath)
        .setConf(jsc.hadoopConfiguration()).build();
    Schema schema = new TableSchemaResolver(sourceTableMetaClient).getTableAvroSchema(false);
    List<HoodieInstant> instantsToSync = getInstantsToSync(cfg.sourceBasePath, targetTableMetaClient, sourceTableMetaClient);
    runMetadataSync(sourceTableMetaClient, targetTableMetaClient, instantsToSync, schema);
    LOG.info("Completed syncing " + cfg.commitToSync +" to target table");
  }

  private void runMetadataSync(HoodieTableMetaClient sourceTableMetaClient, HoodieTableMetaClient targetTableMetaClient, List<HoodieInstant> instants, Schema schema) throws Exception {
    HoodieWriteConfig writeConfig = getWriteConfig(schema, targetTableMetaClient, cfg.sourceBasePath);
    HoodieSparkEngineContext hoodieSparkEngineContext = new HoodieSparkEngineContext(jsc);
    for (HoodieInstant instant : instants) {
      try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(hoodieSparkEngineContext, writeConfig)) {
        String commitTime = writeClient.startCommit(instant.getAction(), targetTableMetaClient); // single writer. will rollback any pending commits from previous round.
        targetTableMetaClient
            .reloadActiveTimeline()
            .transitionRequestedToInflight(
                instant.getAction(),
                commitTime);

        Option<byte[]> commitMetadataInBytes = Option.empty();
        List<String> pendingInstants = getPendingInstants(sourceTableMetaClient.getActiveTimeline(), instant);
        SyncMetadata syncMetadata = getTableSyncExtraMetadata(targetTableMetaClient.reloadActiveTimeline().getWriteTimeline().filterCompletedInstants().lastInstant(), targetTableMetaClient,
            sourceTableMetaClient.getBasePathV2().toString(), instant.getTimestamp(), pendingInstants);


        HoodieSparkTable sparkTable = HoodieSparkTable.create(writeConfig, hoodieSparkEngineContext, targetTableMetaClient);
        if (instant.getAction().equals(HoodieTimeline.COMMIT_ACTION)) {
          HoodieCommitMetadata sourceCommitMetadata = getHoodieCommitMetadata(instant.getTimestamp(), sourceTableMetaClient);
          HoodieCommitMetadata tgtCommitMetadata = buildHoodieCommitMetadata(sourceCommitMetadata, commitTime);

          try (HoodieTableMetadataWriter hoodieTableMetadataWriter =
                   (HoodieTableMetadataWriter) sparkTable.getMetadataWriter(commitTime).get()) {
            hoodieTableMetadataWriter.update(tgtCommitMetadata, hoodieSparkEngineContext.emptyHoodieData(), commitTime);
          }

          // add metadata sync checkpoint info
          tgtCommitMetadata.addMetadata(SyncMetadata.TABLE_SYNC_METADATA, syncMetadata.toJson());
          commitMetadataInBytes = Option.of(tgtCommitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8));
        } else if (instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {

          HoodieReplaceCommitMetadata srcReplaceCommitMetadata = HoodieReplaceCommitMetadata.fromBytes(
              sourceTableMetaClient.getCommitsTimeline().getInstantDetails(instant).get(), HoodieReplaceCommitMetadata.class);
          HoodieReplaceCommitMetadata tgtReplaceCommitMetadata = buildReplaceCommitMetadata(srcReplaceCommitMetadata, commitTime);

          try (HoodieTableMetadataWriter hoodieTableMetadataWriter =
                   (HoodieTableMetadataWriter) sparkTable.getMetadataWriter(commitTime).get()) {
            hoodieTableMetadataWriter.update(tgtReplaceCommitMetadata, hoodieSparkEngineContext.emptyHoodieData(), commitTime);
          }
          // add metadata sync checkpoint info
          tgtReplaceCommitMetadata.addMetadata(SyncMetadata.TABLE_SYNC_METADATA, syncMetadata.toJson());
          commitMetadataInBytes = Option.of(tgtReplaceCommitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8));
        } else if (instant.getAction().equals(HoodieTimeline.CLEAN_ACTION)) {
          HoodieCleanMetadata srcCleanMetadata = TimelineMetadataUtils.deserializeHoodieCleanMetadata(
              sourceTableMetaClient.getCommitsTimeline().getInstantDetails(instant).get());

          HoodieCleanMetadata tgtCleanMetadata = buildHoodieCleanMetadata(srcCleanMetadata, commitTime);
          try (HoodieTableMetadataWriter hoodieTableMetadataWriter =
                   (HoodieTableMetadataWriter) sparkTable.getMetadataWriter(commitTime).get()) {
            hoodieTableMetadataWriter.update(tgtCleanMetadata, commitTime);
          }

          commitMetadataInBytes = TimelineMetadataUtils.serializeCleanMetadata(tgtCleanMetadata);
        }

        targetTableMetaClient
            .reloadActiveTimeline()
            .saveAsComplete(new HoodieInstant(true, instant.getAction(), commitTime), commitMetadataInBytes);
      }
    }
  }

  private SyncMetadata getTableSyncExtraMetadata(Option<HoodieInstant> targetTableLastInstant, HoodieTableMetaClient metaClient, String sourceIdentifier,
                                                       String sourceInstantSynced, List<String> pendingInstantsToSync) {
    return targetTableLastInstant.map(instant -> {
      SyncMetadata syncMetadata = null;
      try {
        syncMetadata = getTableSyncMetadataFromCommitMetadata(instant, metaClient);
      } catch (IOException e) {
        throw new HoodieException("Failed to get sync metadata");
      }

      TableCheckpointInfo checkpointInfo = TableCheckpointInfo.of(sourceInstantSynced, pendingInstantsToSync, sourceIdentifier);
      List<TableCheckpointInfo> updatedCheckpointInfos = syncMetadata.getTableCheckpointInfos().stream()
          .filter(metadata -> !metadata.getSourceIdentifier().equals(sourceIdentifier)).collect(Collectors.toList());
      updatedCheckpointInfos.add(checkpointInfo);
      return SyncMetadata.of(Instant.now(), updatedCheckpointInfos);
    }).orElseGet(() -> {
      List<TableCheckpointInfo> checkpointInfos = Collections.singletonList(TableCheckpointInfo.of(sourceInstantSynced, pendingInstantsToSync, sourceIdentifier));
      return SyncMetadata.of(Instant.now(), checkpointInfos);
    });
  }

  private List<HoodieInstant> getInstantsToSync(String sourceIdentifier, HoodieTableMetaClient targetTableMetaClient, HoodieTableMetaClient sourceTableMetaClient) throws IOException {
    Option<HoodieInstant> lastInstant = targetTableMetaClient.reloadActiveTimeline().getWriteTimeline().filterCompletedInstants().lastInstant();
    if (lastInstant.isPresent()) {
      SyncMetadata syncMetadata = getTableSyncMetadataFromCommitMetadata(lastInstant.get(), targetTableMetaClient);
      Optional<TableCheckpointInfo> sourceTableSyncMetadata = syncMetadata.getTableCheckpointInfos().stream()
          .filter(checkpointInfo -> checkpointInfo.getSourceIdentifier().equals(sourceIdentifier)).findFirst();

      if (!sourceTableSyncMetadata.isPresent()) {
        return sourceTableMetaClient.reloadActiveTimeline().getInstants();
      }

      String lastInstantSynced = sourceTableSyncMetadata.get().getLastInstantSynced();
      HoodieActiveTimeline sourceActiveTimeline = sourceTableMetaClient.getActiveTimeline();
      List<HoodieInstant> newInstantsToSync = sourceActiveTimeline.filterCompletedInstants().findInstantsAfter(lastInstantSynced).getInstants();
      List<HoodieInstant> pendingInstantsFromLastSync = sourceTableSyncMetadata.get().getInstantsToConsiderForNextSync().stream().map(instant ->
          sourceActiveTimeline.findInstantsAfterOrEquals(instant, 1).getInstants().stream().findFirst().orElse(null)
      ).collect(Collectors.toList());
      pendingInstantsFromLastSync.addAll(newInstantsToSync);
      return pendingInstantsFromLastSync;
    } else {
      return sourceTableMetaClient.reloadActiveTimeline().getInstants();
    }
  }

  private List<String> getPendingInstants(
      HoodieActiveTimeline activeTimeline,
      HoodieInstant latestCommit) {
    List<HoodieInstant> pendingHoodieInstants =
        activeTimeline
            .filterInflightsAndRequested()
            .findInstantsBefore(latestCommit.getTimestamp())
            .getInstants();
    return pendingHoodieInstants.stream()
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
  }

  private SyncMetadata getTableSyncMetadataFromCommitMetadata(HoodieInstant instant, HoodieTableMetaClient metaClient) throws IOException {
    HoodieCommitMetadata commitMetadata = getHoodieCommitMetadata(instant.getTimestamp(), metaClient);
    Option<String> tableSyncMetadataJson = Option.ofNullable(commitMetadata.getMetadata(SyncMetadata.TABLE_SYNC_METADATA));
    if (!tableSyncMetadataJson.isPresent()) {
      // if table sync metadata is not present, sync all commits from source table
      throw new HoodieException("Table sync metadata is missing in the target table commit metadata");
    }
    Option<SyncMetadata> tableSyncMetadataListOpt = SyncMetadata.fromJson(tableSyncMetadataJson.get());
    if (!tableSyncMetadataListOpt.isPresent()) {
      throw new HoodieException("Table Sync metadata is empty in the target table commit metadata");
    }

    return tableSyncMetadataListOpt.get();
  }

  private Option<byte[]> buildHoodieInstantMetadata(HoodieInstant instant, String targetTableInstantTime, HoodieTableMetaClient sourceTableMetaClient, HoodieTableMetaClient targetTableMetaClient) throws IOException {
    List<String> pendingInstants = getPendingInstants(sourceTableMetaClient.getActiveTimeline(), instant);
    SyncMetadata syncMetadata = getTableSyncExtraMetadata(targetTableMetaClient.reloadActiveTimeline().filterCompletedInstants().lastInstant(), targetTableMetaClient,
        sourceTableMetaClient.getBasePathV2().toString(), instant.getTimestamp(), pendingInstants);

    if (instant.getAction().equals(HoodieTimeline.COMMIT_ACTION)) {
      HoodieCommitMetadata srcCommitMetadata = HoodieCommitMetadata.fromBytes(
          sourceTableMetaClient.getCommitsTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);
      HoodieCommitMetadata tgtCommitMetadata = buildHoodieCommitMetadata(srcCommitMetadata, targetTableInstantTime);
      tgtCommitMetadata.addMetadata(SyncMetadata.TABLE_SYNC_METADATA, syncMetadata.toJson());
      return Option.of(tgtCommitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8));
    } else if (instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
      HoodieReplaceCommitMetadata srcReplaceCommitMetadata = HoodieReplaceCommitMetadata.fromBytes(
          sourceTableMetaClient.getCommitsTimeline().getInstantDetails(instant).get(), HoodieReplaceCommitMetadata.class);
      HoodieReplaceCommitMetadata tgtReplaceCommitMetadata = buildReplaceCommitMetadata(srcReplaceCommitMetadata, targetTableInstantTime);
      tgtReplaceCommitMetadata.addMetadata(SyncMetadata.TABLE_SYNC_METADATA, syncMetadata.toJson());
      return Option.of(tgtReplaceCommitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8));
    }

    return Option.empty();
  }

  private HoodieCommitMetadata buildHoodieCommitMetadata(HoodieCommitMetadata sourceCommitMetadata, String commitTime) {
    HoodieCommitMetadata tgtTableCommitMetadata = new HoodieCommitMetadata();

    for (HoodieWriteStat writeStat : sourceCommitMetadata.getWriteStats()) {
      String partition = writeStat.getPartitionPath();
      fixWriteStatusForExternalPaths(writeStat, commitTime);
      tgtTableCommitMetadata.addWriteStat(partition, writeStat);
    }

    WriteOperationType operationType = sourceCommitMetadata.getOperationType();
    tgtTableCommitMetadata.setOperationType(operationType);
    tgtTableCommitMetadata.setCompacted(false);
    if (!sourceCommitMetadata.getExtraMetadata().isEmpty()) {
      sourceCommitMetadata.getExtraMetadata().forEach(tgtTableCommitMetadata::addMetadata);
    }
    return tgtTableCommitMetadata;
  }

  private HoodieReplaceCommitMetadata buildReplaceCommitMetadata(HoodieReplaceCommitMetadata srcReplaceMetadata, String commitTime) {
    HoodieReplaceCommitMetadata tgtReplaceCommitMetadata = new HoodieReplaceCommitMetadata();

    for (HoodieWriteStat writeStat : srcReplaceMetadata.getWriteStats()) {
      String partition = writeStat.getPartitionPath();
      fixWriteStatusForExternalPaths(writeStat, commitTime);
      tgtReplaceCommitMetadata.addWriteStat(partition, writeStat);
    }

    WriteOperationType operationType = srcReplaceMetadata.getOperationType();
    tgtReplaceCommitMetadata.setOperationType(operationType);
    tgtReplaceCommitMetadata.setCompacted(false);
    tgtReplaceCommitMetadata.setPartitionToReplaceFileIds(srcReplaceMetadata.getPartitionToReplaceFileIds());
    if (!srcReplaceMetadata.getExtraMetadata().isEmpty()) {
      srcReplaceMetadata.getExtraMetadata().forEach(tgtReplaceCommitMetadata::addMetadata);
    }
    return tgtReplaceCommitMetadata;
  }

  private HoodieCleanMetadata buildHoodieCleanMetadata(HoodieCleanMetadata srcCleanMetadata, String commitTime) {
    HoodieCleanMetadata tgtCleanMetadata = new HoodieCleanMetadata();

    Map<String, HoodieCleanPartitionMetadata> tgtCleanMetadataMap = new HashMap<>();
    for(Map.Entry<String, HoodieCleanPartitionMetadata> entry:  srcCleanMetadata.getPartitionMetadata().entrySet()) {
      HoodieCleanPartitionMetadata tgtCleanPartitionMetadata = new HoodieCleanPartitionMetadata();
      String partition = entry.getKey();
      HoodieCleanPartitionMetadata srcCleanPartitionMetadata = entry.getValue();
      tgtCleanPartitionMetadata.setSuccessDeleteFiles(srcCleanPartitionMetadata.getSuccessDeleteFiles());
      tgtCleanPartitionMetadata.setDeletePathPatterns(srcCleanPartitionMetadata.getDeletePathPatterns());
      tgtCleanPartitionMetadata.setFailedDeleteFiles(srcCleanPartitionMetadata.getFailedDeleteFiles());
      tgtCleanPartitionMetadata.setIsPartitionDeleted(srcCleanPartitionMetadata.getIsPartitionDeleted());
      tgtCleanPartitionMetadata.setPartitionPath(srcCleanPartitionMetadata.getPartitionPath());
      tgtCleanPartitionMetadata.setPolicy(srcCleanPartitionMetadata.getPolicy());

      tgtCleanMetadataMap.put(partition, tgtCleanPartitionMetadata);
    }

    tgtCleanMetadata.setPartitionMetadata(tgtCleanMetadataMap);
    tgtCleanMetadata.setStartCleanTime(srcCleanMetadata.getStartCleanTime());
    tgtCleanMetadata.setVersion(srcCleanMetadata.getVersion());
    tgtCleanMetadata.setEarliestCommitToRetain(srcCleanMetadata.getEarliestCommitToRetain());
    tgtCleanMetadata.setLastCompletedCommitTimestamp(srcCleanMetadata.getLastCompletedCommitTimestamp());
    tgtCleanMetadata.setTotalFilesDeleted(srcCleanMetadata.getTotalFilesDeleted());
    return tgtCleanMetadata;
  }

  private HoodieCommitMetadata getHoodieCommitMetadata(String instantTime, HoodieTableMetaClient metaClient) throws IOException {
    Option<HoodieInstant> instantOpt = metaClient.getActiveTimeline().filterCompletedInstants().filter(instant -> instant.getTimestamp().equalsIgnoreCase(instantTime)).firstInstant();
    if (instantOpt.isPresent()) {
      HoodieInstant instant = instantOpt.get();
      return HoodieCommitMetadata.fromBytes(
          metaClient.getCommitsTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);
    } else {
      throw new HoodieException("Could not find " + instantTime + " in source table timeline ");
    }
  }

  private void fixWriteStatusForExternalPaths(HoodieWriteStat writeStat, String commitTime) {
    String originalPath = writeStat.getPath();
    writeStat.setPath(ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(originalPath, commitTime));
  }

  private HoodieWriteConfig getWriteConfig(
      Schema schema,
      HoodieTableMetaClient metaClient,
      String basePathOverride) {
    return getWriteConfig(schema, Integer.parseInt(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.defaultValue()),
        HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.defaultValue(), 120, metaClient,
        basePathOverride);
  }

  private HoodieWriteConfig getWriteConfig(
      Schema schema,
      int numCommitsToKeep,
      int maxNumDeltaCommitsBeforeCompaction,
      int timelineRetentionInHours,
      HoodieTableMetaClient metaClient,
      String basePathOverride) {
    Properties properties = new Properties();
    properties.setProperty(HoodieMetadataConfig.AUTO_INITIALIZE.key(), "false");
    return HoodieWriteConfig.newBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(INMEMORY).build())
        .withPath(metaClient.getBasePathV2().toString())
        .withPopulateMetaFields(metaClient.getTableConfig().populateMetaFields())
        .withEmbeddedTimelineServerEnabled(false)
        .withSchema(schema == null ? "" : schema.toString())
        .withArchivalConfig(
            HoodieArchivalConfig.newBuilder()
                .archiveCommitsWith(
                    Math.max(0, numCommitsToKeep - 1), Math.max(1, numCommitsToKeep))
                .withAutoArchive(false)
                .build())
        .withCleanConfig(
            HoodieCleanConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS)
                .cleanerNumHoursRetained(timelineRetentionInHours)
                .withAutoClean(false)
                .build())
        .withMetadataConfig(
            HoodieMetadataConfig.newBuilder()
                .enable(true)
                .withProperties(properties)
                .withMetadataIndexColumnStats(false)
                .withMaxNumDeltaCommitsBeforeCompaction(maxNumDeltaCommitsBeforeCompaction)
                .withEnableBasePathForPartitions(true)
                .withBasePathOverride(basePathOverride)
                .build())
        .build();
  }
}
