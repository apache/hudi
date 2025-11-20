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
import org.apache.hudi.client.HoodieTimelineArchiver;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.SparkHoodieBackedMetadataSyncMetadataWriter;
import org.apache.hudi.table.HoodieSparkTable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY;
import static org.apache.hudi.utilities.MetadataSyncUtils.getHoodieCommitMetadata;
import static org.apache.hudi.utilities.MetadataSyncUtils.getInstantsToSyncAndLastSyncCheckpoint;
import static org.apache.hudi.utilities.MetadataSyncUtils.getPendingInstants;
import static org.apache.hudi.utilities.MetadataSyncUtils.getTableSyncExtraMetadata;

/**
 * Handles metadata table synchronization for a Hudi table.
 * <p>
 * This class initializes Spark context, loads configuration properties,
 * and provides utilities required to perform metadata sync between a
 * source Hudi table and a target Hudi table.
 * </p>
 */
public class HoodieMetadataSync implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMetadataSync.class);
  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;

  public HoodieMetadataSync(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.cfg = cfg;
    this.props = StringUtils.isNullOrEmpty(cfg.propsFilePath)
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
  }

  /**
   * Reads config from the file system.
   *
   * @param jsc {@link JavaSparkContext} instance.
   * @param cfg {@link TableSizeStats.Config} instance.
   * @return the {@link TypedProperties} instance.
   */
  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, Config cfg) {
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

    @Parameter(names = {"--restore"}, description = "restore target table",
        splitter = IdentitySplitter.class)
    public Boolean doRestore = false;

    @Parameter(names = {"--commit-to-restore", "-ctr"}, description = "Commit to restore to", required = false)
    public String commitToRestore = null;

    @Parameter(names = {"--boostrap"}, description = "performs table maintenance",
        splitter = IdentitySplitter.class)
    public Boolean performTableMaintenance = false;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for clustering")
    public String propsFilePath = null;

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
    this.props = StringUtils.isNullOrEmpty(cfg.propsFilePath)
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
    boolean targetTableExists = fs.exists(targetTablePath);
    if (!targetTableExists) {
      LOG.info("Initializing target table for first time", cfg.targetBasePath);
      TypedProperties props = sourceTableMetaClient.getTableConfig().getProps();
      props.remove("hoodie.table.metadata.partitions");
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
    HoodieWriteConfig writeConfig = getWriteConfig(schema, targetTableMetaClient, cfg.sourceBasePath, cfg.boostrap);
    HoodieSparkEngineContext hoodieSparkEngineContext = new HoodieSparkEngineContext(jsc);
    TransactionManager txnManager = new TransactionManager(writeConfig, FSUtils.getFs(writeConfig.getBasePath(), hoodieSparkEngineContext.getHadoopConf().get()));

    HoodieSparkTable sparkTable = HoodieSparkTable.create(writeConfig, hoodieSparkEngineContext, targetTableMetaClient);
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(hoodieSparkEngineContext, writeConfig)) {
      txnManager.beginTransaction(Option.empty(), Option.empty());
      if (cfg.boostrap) {
        runBootstrapSync(sparkTable, sourceTableMetaClient, targetTableMetaClient, writeClient, schema);
      } else if (cfg.doRestore) {
        writeClient.savepoint(cfg.commitToRestore, "user1", "comment1");

        // restore.
        writeClient.restoreToSavepoint(cfg.commitToRestore);
      }
      {
        Pair<TreeMap<HoodieInstant, Boolean>, Option<String>> instantsToSyncAndLastSyncCheckpointPair =
            getInstantsToSyncAndLastSyncCheckpoint(cfg.sourceBasePath, targetTableMetaClient, sourceTableMetaClient);
        TreeMap<HoodieInstant, Boolean> instantsStatusMap = instantsToSyncAndLastSyncCheckpointPair.getLeft();
        Option<String> lastSyncCheckpoint = instantsToSyncAndLastSyncCheckpointPair.getRight();
        for (Map.Entry<HoodieInstant, Boolean> entry : instantsToSyncAndLastSyncCheckpointPair.getKey().entrySet()) {
          HoodieInstant instant = entry.getKey();
          boolean isCompleted = entry.getValue();
          if (!isCompleted) {
            // if instant is pending state, skip it
            continue;
          }

          Option<byte[]> commitMetadataInBytes = Option.empty();
          SyncMetadata syncMetadata = getTableSyncExtraMetadata(targetTableMetaClient.reloadActiveTimeline().getWriteTimeline().filterCompletedInstants().lastInstant(),
              targetTableMetaClient, cfg.sourceBasePath, instant.getTimestamp(), instantsStatusMap, lastSyncCheckpoint, instant);

          if (!getPendingInstants(targetTableMetaClient.reloadActiveTimeline(), Option.empty()).isEmpty()) {
            // rollback failing writes
            writeClient.rollbackFailedWrites();
          }

          String commitTime = writeClient.startCommit(instant.getAction(), targetTableMetaClient); // single writer. will rollback any pending commits from previous round.
          targetTableMetaClient
              .reloadActiveTimeline()
              .transitionRequestedToInflight(
                  instant.getAction(),
                  commitTime);
          HoodieTableMetadataWriter hoodieTableMetadataWriter =
              (HoodieTableMetadataWriter) sparkTable.getMetadataWriter(commitTime).get();
          // perform table services if required on metadata table
          hoodieTableMetadataWriter.performTableServices(Option.of(commitTime));
          switch (instant.getAction()) {
            case HoodieTimeline.COMMIT_ACTION:
              HoodieCommitMetadata sourceCommitMetadata = getHoodieCommitMetadata(instant.getTimestamp(), sourceTableMetaClient);
              HoodieCommitMetadata tgtCommitMetadata = buildHoodieCommitMetadata(sourceCommitMetadata, commitTime);
              hoodieTableMetadataWriter.update(tgtCommitMetadata, hoodieSparkEngineContext.emptyHoodieData(), commitTime);

              // add metadata sync checkpoint info
              tgtCommitMetadata.addMetadata(SyncMetadata.TABLE_SYNC_METADATA, syncMetadata.toJson());
              commitMetadataInBytes = Option.of(tgtCommitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8));
              break;
            case HoodieTimeline.REPLACE_COMMIT_ACTION:

              HoodieReplaceCommitMetadata srcReplaceCommitMetadata = HoodieReplaceCommitMetadata.fromBytes(
                  sourceTableMetaClient.getCommitsTimeline().getInstantDetails(instant).get(), HoodieReplaceCommitMetadata.class);
              HoodieReplaceCommitMetadata tgtReplaceCommitMetadata = buildReplaceCommitMetadata(srcReplaceCommitMetadata, commitTime);
              hoodieTableMetadataWriter.update(tgtReplaceCommitMetadata, hoodieSparkEngineContext.emptyHoodieData(), commitTime);

              // add metadata sync checkpoint info
              tgtReplaceCommitMetadata.addMetadata(SyncMetadata.TABLE_SYNC_METADATA, syncMetadata.toJson());
              commitMetadataInBytes = Option.of(tgtReplaceCommitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8));
              break;
            case HoodieTimeline.CLEAN_ACTION:
              HoodieCleanMetadata srcCleanMetadata = TimelineMetadataUtils.deserializeHoodieCleanMetadata(
                  sourceTableMetaClient.getCommitsTimeline().getInstantDetails(instant).get());
              HoodieCleanMetadata tgtCleanMetadata = reconstructHoodieCleanCommitMetadata(srcCleanMetadata,
                  writeConfig, hoodieSparkEngineContext, targetTableMetaClient);
              //HoodieCleanMetadata tgtCleanMetadata = buildHoodieCleanMetadata(srcCleanMetadata, commitTime);
              hoodieTableMetadataWriter.update(tgtCleanMetadata, commitTime);

              commitMetadataInBytes = TimelineMetadataUtils.serializeCleanMetadata(tgtCleanMetadata);
              break;
          }

          targetTableMetaClient
              .reloadActiveTimeline()
              .saveAsComplete(new HoodieInstant(true, instant.getAction(), commitTime), commitMetadataInBytes);

          if (cfg.performTableMaintenance) {
            runArchiver(sparkTable, writeClient.getConfig(), hoodieSparkEngineContext);
          }
        }
      }
    } finally {
      txnManager.endTransaction(Option.empty());
    }
  }

  private void runBootstrapSync(HoodieSparkTable sparkTable, HoodieTableMetaClient sourceTableMetaClient,
                                HoodieTableMetaClient targetTableMetaClient, SparkRDDWriteClient writeClient, Schema schema) throws Exception {
    Option<HoodieInstant> sourceLastInstant = sourceTableMetaClient.getActiveTimeline().getWriteTimeline().filterCompletedInstants().lastInstant();
    if (!sourceLastInstant.isPresent()) {
      return;
    }
    String commitTime = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, targetTableMetaClient); // single writer. will rollback any pending commits from previous round.
    targetTableMetaClient
        .reloadActiveTimeline()
        .transitionRequestedToInflight(
            HoodieTimeline.REPLACE_COMMIT_ACTION,
            commitTime);

      SparkHoodieBackedMetadataSyncMetadataWriter metadataWriter =
          (SparkHoodieBackedMetadataSyncMetadataWriter) sparkTable.getMetadataWriter(commitTime).get();
      metadataWriter.bootstrap(sourceLastInstant.map(HoodieInstant::getTimestamp));
    Option<HoodieInstant> targetTableLastInstant = targetTableMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
    List<String> pendingInstants = getPendingInstants(sourceTableMetaClient.getActiveTimeline(), sourceLastInstant).stream().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    SyncMetadata syncMetadata = getTableSyncExtraMetadata(targetTableLastInstant, targetTableMetaClient,
        cfg.sourceBasePath, sourceLastInstant.get().getTimestamp(), pendingInstants);
    HoodieReplaceCommitMetadata replaceCommitMetadata = buildComprehensiveReplaceCommitMetadata(sourceTableMetaClient, schema);
    replaceCommitMetadata.addMetadata(SyncMetadata.TABLE_SYNC_METADATA, syncMetadata.toJson());
    targetTableMetaClient
        .reloadActiveTimeline()
        .saveAsComplete(new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, commitTime),
            Option.of(replaceCommitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
  }

  private HoodieReplaceCommitMetadata buildComprehensiveReplaceCommitMetadata(HoodieTableMetaClient sourceTableMetaClient, Schema schema) {
    List<HoodieInstant> replaceCommits =  sourceTableMetaClient.getActiveTimeline().filterCompletedInstants().getInstants().stream()
        .filter(instant -> instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)).collect(Collectors.toList());

    HoodieReplaceCommitMetadata replaceCommitMetadata = new HoodieReplaceCommitMetadata();
    Map<String, List<String>> totalPartitionToReplacedFiles = new HashMap<>();
    replaceCommits.forEach(replaceCommit -> {
      try {
        HoodieReplaceCommitMetadata commitMetadata = HoodieReplaceCommitMetadata.fromBytes(
            sourceTableMetaClient.getCommitsTimeline().getInstantDetails(replaceCommit).get(), HoodieReplaceCommitMetadata.class);
        Map<String, List<String>> partitionsToReplacedFileGroups = commitMetadata.getPartitionToReplaceFileIds();
        for (Map.Entry<String, List<String>> entry : partitionsToReplacedFileGroups.entrySet()) {
          String partition = entry.getKey();
          List<String> replacedFiles = entry.getValue();
          totalPartitionToReplacedFiles.computeIfAbsent(partition, k -> new ArrayList<>());
          totalPartitionToReplacedFiles.get(partition).addAll(replacedFiles);
        }
      } catch (IOException e) {
        throw new HoodieException("Failed to deserialize instant " + replaceCommit.getTimestamp(), e);
      }
    });

    replaceCommitMetadata.setPartitionToReplaceFileIds(totalPartitionToReplacedFiles);

    replaceCommitMetadata.addMetadata("schema", schema.toString());
    replaceCommitMetadata.setOperationType(WriteOperationType.BOOTSTRAP);
    replaceCommitMetadata.setCompacted(false);
    return replaceCommitMetadata;
  }

  private void runArchiver(
      HoodieSparkTable<?> table, HoodieWriteConfig config, HoodieEngineContext engineContext) {
    // trigger archiver manually
    try {
      HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(config, table);
      archiver.archiveIfRequired(engineContext, true);
    } catch (IOException ex) {
      throw new HoodieException("Unable to archive Hudi timeline", ex);
    }
  }

  private HoodieCleanMetadata reconstructHoodieCleanCommitMetadata(HoodieCleanMetadata originalCleanMetadata, HoodieWriteConfig config, HoodieSparkEngineContext engineContext,
                                                                   HoodieTableMetaClient metaClient) {
    HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(engineContext, config.getMetadataConfig(), metaClient.getBasePathV2().toString(), true);
    try {
      originalCleanMetadata.getPartitionMetadata().entrySet().forEach(entry -> {
        String partitionPath = entry.getKey();
        System.out.println("Processing partition path " + partitionPath);
        HoodieCleanPartitionMetadata cleanPartitionMetadata = entry.getValue();
        List<String> deletePathPatterns = cleanPartitionMetadata.getDeletePathPatterns();

        Map<String, List<Pair<String, String>>> fileIdsToDeletedFiles = new HashMap<>();
        deletePathPatterns.forEach(deletePathPattern -> {
          String fileId = FSUtils.getFileId(deletePathPattern);
          String baseInstantTime = FSUtils.getCommitTime(deletePathPattern);
          fileIdsToDeletedFiles.computeIfAbsent(fileId, s -> new ArrayList<>());
          fileIdsToDeletedFiles.get(fileId).add(Pair.of(deletePathPattern, baseInstantTime));
        });

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

  private HoodieCommitMetadata buildHoodieCommitMetadata(HoodieCommitMetadata sourceCommitMetadata, String commitTime) {
    HoodieCommitMetadata tgtTableCommitMetadata = new HoodieCommitMetadata();

    for (HoodieWriteStat writeStat : sourceCommitMetadata.getWriteStats()) {
      String partition = writeStat.getPartitionPath();
      fixWriteStatusForExternalPaths(writeStat, commitTime, partition);
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
      fixWriteStatusForExternalPaths(writeStat, commitTime, partition);
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

  private void fixWriteStatusForExternalPaths(HoodieWriteStat writeStat, String commitTime, String partitionPath) {
    String originalPath = writeStat.getPath();
    writeStat.setPath(ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(originalPath, commitTime, partitionPath));
  }

  private HoodieWriteConfig getWriteConfig(
      Schema schema,
      HoodieTableMetaClient metaClient,
      String basePathOverride,
      boolean enableBoostrapSync) {
    Properties properties = new Properties();
    properties.setProperty(HoodieMetadataConfig.AUTO_INITIALIZE.key(), "false");
    properties.putAll(this.props);
    return HoodieWriteConfig.newBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(INMEMORY).build())
        .withPath(metaClient.getBasePathV2().toString())
        .withPopulateMetaFields(metaClient.getTableConfig().populateMetaFields())
        .withEmbeddedTimelineServerEnabled(false)
        .withSchema(schema == null ? "" : schema.toString())
        .withArchivalConfig(
            HoodieArchivalConfig.newBuilder()
                .fromProperties(props)
                .build())
        .withMetadataConfig(
            HoodieMetadataConfig.newBuilder()
                .enable(true)
                .withProperties(properties)
                .withMetadataIndexColumnStats(false)
                .withEnableBasePathOverride(true)
                .withEnableBootstrapMetadataSync(enableBoostrapSync)
                .withBasePathOverride(basePathOverride)
                .build())
        .build();
  }
}
