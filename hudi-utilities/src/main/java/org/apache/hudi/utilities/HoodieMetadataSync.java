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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.FileSlice;
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
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
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
import org.apache.hadoop.fs.StorageType;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.filechooser.FileSystemView;

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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY;
import static org.apache.hudi.utilities.MetadataSyncUtils.getHoodieCommitMetadata;
import static org.apache.hudi.utilities.MetadataSyncUtils.getPendingInstants;
import static org.apache.hudi.utilities.MetadataSyncUtils.getTableSyncExtraMetadata;
import static org.apache.hudi.utilities.MetadataSyncUtils.getTableSyncMetadataFromCommitMetadata;

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

    @Parameter(names = {"--boostrap"}, description = "boostraps metadata table",
        splitter = IdentitySplitter.class)
    public Boolean boostrap = false;

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
    runMetadataSync(sourceTableMetaClient, targetTableMetaClient, schema);
    LOG.info("Completed syncing " + cfg.commitToSync +" to target table");
  }

  private void runMetadataSync(HoodieTableMetaClient sourceTableMetaClient, HoodieTableMetaClient targetTableMetaClient, Schema schema) throws Exception {
    HoodieWriteConfig writeConfig = getWriteConfig(schema, targetTableMetaClient, cfg.sourceBasePath);

    if (cfg.boostrap) {
      HoodieBootstrapMetadataSync bootstrapMetadataSync = new HoodieBootstrapMetadataSync(writeConfig, jsc, cfg.sourceBasePath, cfg.targetBasePath, schema);
      bootstrapMetadataSync.run();
      return;
    }

    List<HoodieInstant> instantsToSync = getInstantsToSync(cfg.sourceBasePath, targetTableMetaClient, sourceTableMetaClient);
    HoodieSparkEngineContext hoodieSparkEngineContext = new HoodieSparkEngineContext(jsc);
    for (HoodieInstant instant : instantsToSync) {
      try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(hoodieSparkEngineContext, writeConfig)) {
        String commitTime = writeClient.startCommit(instant.getAction(), targetTableMetaClient); // single writer. will rollback any pending commits from previous round.
        targetTableMetaClient
            .reloadActiveTimeline()
            .transitionRequestedToInflight(
                instant.getAction(),
                commitTime);

        Option<byte[]> commitMetadataInBytes = Option.empty();
        List<String> pendingInstants = getPendingInstants(sourceTableMetaClient.getActiveTimeline(), instant);
        SyncMetadata syncMetadata = getTableSyncExtraMetadata(targetTableMetaClient.reloadActiveTimeline().getWriteTimeline().filterCompletedInstants().lastInstant(),
            targetTableMetaClient, sourceTableMetaClient.getBasePathV2().toString(), instant.getTimestamp(), pendingInstants);


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
          HoodieCleanMetadata tgtCleanMetadata = reconstructHoodieCleanCommitMetadata(srcCleanMetadata,
              writeConfig, hoodieSparkEngineContext, targetTableMetaClient);
          //HoodieCleanMetadata tgtCleanMetadata = buildHoodieCleanMetadata(srcCleanMetadata, commitTime);
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

  private HoodieCleanMetadata reconstructHoodieCleanCommitMetadata(HoodieCleanMetadata originalCleanMetadata, HoodieWriteConfig config, HoodieSparkEngineContext engineContext,
                                                                   HoodieTableMetaClient metaClient) {
    HoodieCleanMetadata cleanMetadata = new HoodieCleanMetadata();
    HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(engineContext, config.getMetadataConfig(), metaClient.getBasePathV2().toString(), true);
    //HoodieTableFileSystemView fsv = getFSV(config, FileSystemViewStorageType.MEMORY, engineContext, metaClient);
    try {
      originalCleanMetadata.getPartitionMetadata().entrySet().forEach(entry -> {
        String partitionPath = entry.getKey();
        System.out.println("Processing partition path " + partitionPath);
        HoodieCleanPartitionMetadata cleanPartitionMetadata = entry.getValue();
        List<String> deletePathPatterns = cleanPartitionMetadata.getDeletePathPatterns();
        boolean isPartitionDeleted = cleanPartitionMetadata.getIsPartitionDeleted();

        Map<String, List<Pair<String, String>>> fileIdsToDeletedFiles = new HashMap<>();
        deletePathPatterns.forEach(deletePathPattern -> {
          String fileId = FSUtils.getFileId(deletePathPattern);
          String baseInstantTime = FSUtils.getCommitTime(deletePathPattern);
          fileIdsToDeletedFiles.computeIfAbsent(fileId, s -> new ArrayList<>());
          fileIdsToDeletedFiles.get(fileId).add(Pair.of(deletePathPattern, baseInstantTime));
        });
        Set<String> fileIds = fileIdsToDeletedFiles.keySet();
      /*fsv.getAllFileGroups(partitionPath).filter(fileGroup -> fileIds.contains(fileGroup.getFileGroupId().getFileId())).forEach(fileGroup -> {
        // found matched file group.
        // find file slice matching base instant time.
        String fileId = fileGroup.getFileGroupId().getFileId();
        List<Pair<String, String>> fileNameAndBaseInstantTimePair = fileIdsToDeletedFiles.get(fileId);
        Map<String, String> baseInstantTimeToSrcFileNameMap = fileNameAndBaseInstantTimePair.stream()
            .map(pair -> Pair.of(pair.getValue(), pair.getKey()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

       Map<String, FileSlice> srcBaseInstantTimeToFileSlice = fileGroup.getAllFileSlices().map(fileSlice -> {
          String baseFileName = fileSlice.getBaseFile().get().getFileName();
          String srcBaseInstantTime = FSUtils.getCommitTime(baseFileName);
          return Pair.of(srcBaseInstantTime, fileSlice);
        }).filter(pair -> baseInstantTimeToSrcFileNameMap.containsKey(pair.getKey())).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

       srcBaseInstantTimeToFileSlice.entrySet().forEach(entry1 -> {
         String baseInstantTime = entry1.getKey();
         FileSlice fileSlice = entry1.getValue();
         String originalFile = baseInstantTimeToSrcFileNameMap.get(baseInstantTime);
         System.out.println("  "+ fileId + " :: original file " + originalFile + ", file path from target table " + fileSlice.getBaseFile().get().getPath());
       });
      });*/

        List<String> allFilesInPartition = ((HoodieBackedTableMetadata) tableMetadata).getRecordByKey(partitionPath, MetadataPartitionType.FILES.getPartitionPath()).get().getData().getFilenames();

        Map<String, Map<String, String>> fileIdToBaseInstantTimeToFileName = new HashMap<>();
        allFilesInPartition.forEach(fileName -> {
          String fileId = FSUtils.getFileId(fileName);
          String baseInstantTime = FSUtils.getCommitTime(fileName);
          fileIdToBaseInstantTimeToFileName.computeIfAbsent(fileId, s -> new HashMap<>());
          fileIdToBaseInstantTimeToFileName.get(fileId).put(baseInstantTime, fileName);
        });

        List<String> deletePathPatternsFromTarget = new ArrayList<>();
        fileIdsToDeletedFiles.forEach((k, v) -> {
          String fileId = k;
          List<Pair<String, String>> fileNameAndBaseInstantTime = v;
          fileNameAndBaseInstantTime.forEach(pair -> {
            if (fileIdToBaseInstantTimeToFileName.get(fileId) != null && fileIdToBaseInstantTimeToFileName.get(fileId).containsKey(pair.getValue())) {
              //System.out.println("Source file id " + k + ", base instant time " + pair.getRight() + ", file name " + pair.getLeft() + " => target table file Name " + fileIdToBaseInstantTimeToFileName.get(fileId).get(pair.getRight()));
              deletePathPatternsFromTarget.add(fileIdToBaseInstantTimeToFileName.get(fileId).get(pair.getRight()));
            } else {
              //System.out.println("Source file id " + k + ", base instant time " + pair.getRight() + ", file name " + pair.getLeft() + " => No matching file found in target table XXXXXXX ");
            }
          });
        });
        if (cleanPartitionMetadata.getDeletePathPatterns().size() == deletePathPatternsFromTarget.size()) {
          cleanPartitionMetadata.setDeletePathPatterns(deletePathPatternsFromTarget);
        } else {
          throw new HoodieException("Failed to find matching file from target table");
        }
      });

      return originalCleanMetadata;
    } finally {
      if (tableMetadata != null) {
        try {
          tableMetadata.close();
        } catch (Exception e) {
          throw new HoodieException("Failed to close Table Metadata", e);
        }
      }
    }
  }

  private HoodieTableFileSystemView getFSV(HoodieWriteConfig config, FileSystemViewStorageType storageType, HoodieSparkEngineContext context, HoodieTableMetaClient metaClient) {
    FileSystemViewStorageConfig viewStorageConfig = FileSystemViewStorageConfig.newBuilder().fromProperties(config.getProps())
        .withStorageType(storageType).build();
    return (HoodieTableFileSystemView) FileSystemViewManager
        .createViewManager(context, config.getMetadataConfig(), viewStorageConfig, config.getCommonConfig(),
            (SerializableFunctionUnchecked<HoodieTableMetaClient, HoodieTableMetadata>) v1 ->
                HoodieTableMetadata.create(context, config.getMetadataConfig(), metaClient.getBasePathV2().toString(), true))
        .getFileSystemView(config.getBasePath());
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
