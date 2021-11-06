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

package org.apache.hudi.metadata;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.AbstractHoodieWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsGraphiteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsJmxConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER;
import static org.apache.hudi.metadata.HoodieTableMetadata.METADATA_TABLE_NAME_SUFFIX;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;

/**
 * Writer implementation backed by an internal hudi table. Partition and file listing are saved within an internal MOR table
 * called Metadata Table. This table is created by listing files and partitions (first time)
 * and kept in sync using the instants on the main dataset.
 */
public abstract class HoodieBackedTableMetadataWriter implements HoodieTableMetadataWriter {

  private static final Logger LOG = LogManager.getLogger(HoodieBackedTableMetadataWriter.class);

  protected HoodieWriteConfig metadataWriteConfig;
  protected HoodieWriteConfig dataWriteConfig;
  protected String tableName;

  protected HoodieBackedTableMetadata metadata;
  protected HoodieTableMetaClient metadataMetaClient;
  protected HoodieTableMetaClient dataMetaClient;
  protected Option<HoodieMetadataMetrics> metrics;
  protected boolean enabled;
  protected SerializableConfiguration hadoopConf;
  protected final transient HoodieEngineContext engineContext;

  /**
   * Hudi backed table metadata writer.
   *
   * @param hadoopConf     - Hadoop configuration to use for the metadata writer
   * @param writeConfig    - Writer config
   * @param engineContext  - Engine context
   * @param actionMetadata - Optional action metadata to help decide bootstrap operations
   * @param <T>            - Action metadata types extending Avro generated SpecificRecordBase
   */
  protected <T extends SpecificRecordBase> HoodieBackedTableMetadataWriter(Configuration hadoopConf,
                                                                           HoodieWriteConfig writeConfig,
                                                                           HoodieEngineContext engineContext,
                                                                           Option<T> actionMetadata) {
    this.dataWriteConfig = writeConfig;
    this.engineContext = engineContext;
    this.hadoopConf = new SerializableConfiguration(hadoopConf);

    if (writeConfig.isMetadataTableEnabled()) {
      this.tableName = writeConfig.getTableName() + METADATA_TABLE_NAME_SUFFIX;
      this.metadataWriteConfig = createMetadataWriteConfig(writeConfig);
      enabled = true;

      // Inline compaction and auto clean is required as we dont expose this table outside
      ValidationUtils.checkArgument(!this.metadataWriteConfig.isAutoClean(),
          "Cleaning is controlled internally for Metadata table.");
      ValidationUtils.checkArgument(!this.metadataWriteConfig.inlineCompactionEnabled(),
          "Compaction is controlled internally for metadata table.");
      // Metadata Table cannot have metadata listing turned on. (infinite loop, much?)
      ValidationUtils.checkArgument(this.metadataWriteConfig.shouldAutoCommit(),
          "Auto commit is required for Metadata Table");
      ValidationUtils.checkArgument(!this.metadataWriteConfig.isMetadataTableEnabled(),
          "File listing cannot be used for Metadata Table");

      initRegistry();
      this.dataMetaClient =
          HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(dataWriteConfig.getBasePath()).build();
      initialize(engineContext, actionMetadata);
      initTableMetadata();
    } else {
      enabled = false;
      this.metrics = Option.empty();
    }
  }

  protected abstract void initRegistry();

  /**
   * Create a {@code HoodieWriteConfig} to use for the Metadata Table.
   *
   * @param writeConfig {@code HoodieWriteConfig} of the main dataset writer
   */
  private HoodieWriteConfig createMetadataWriteConfig(HoodieWriteConfig writeConfig) {
    int parallelism = writeConfig.getMetadataInsertParallelism();

    int minCommitsToKeep = Math.max(writeConfig.getMetadataMinCommitsToKeep(), writeConfig.getMinCommitsToKeep());
    int maxCommitsToKeep = Math.max(writeConfig.getMetadataMaxCommitsToKeep(), writeConfig.getMaxCommitsToKeep());

    // Create the write config for the metadata table by borrowing options from the main write config.
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder()
            .withConsistencyCheckEnabled(writeConfig.getConsistencyGuardConfig().isConsistencyCheckEnabled())
            .withInitialConsistencyCheckIntervalMs(writeConfig.getConsistencyGuardConfig().getInitialConsistencyCheckIntervalMs())
            .withMaxConsistencyCheckIntervalMs(writeConfig.getConsistencyGuardConfig().getMaxConsistencyCheckIntervalMs())
            .withMaxConsistencyChecks(writeConfig.getConsistencyGuardConfig().getMaxConsistencyChecks())
            .build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.SINGLE_WRITER)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).withFileListingParallelism(writeConfig.getFileListingParallelism()).build())
        .withAutoCommit(true)
        .withAvroSchemaValidate(true)
        .withEmbeddedTimelineServerEnabled(false)
        .withPath(HoodieTableMetadata.getMetadataTableBasePath(writeConfig.getBasePath()))
        .withSchema(HoodieMetadataRecord.getClassSchema().toString())
        .forTable(tableName)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withAsyncClean(writeConfig.isMetadataAsyncClean())
            // we will trigger cleaning manually, to control the instant times
            .withAutoClean(false)
            .withCleanerParallelism(parallelism)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .retainCommits(writeConfig.getMetadataCleanerCommitsRetained())
            .archiveCommitsWith(minCommitsToKeep, maxCommitsToKeep)
            // we will trigger compaction manually, to control the instant times
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(writeConfig.getMetadataCompactDeltaCommitMax()).build())
        .withParallelism(parallelism, parallelism)
        .withDeleteParallelism(parallelism)
        .withRollbackParallelism(parallelism)
        .withFinalizeWriteParallelism(parallelism)
        .withAllowMultiWriteOnSameInstant(true);

    if (writeConfig.isMetricsOn()) {
      builder.withMetricsConfig(HoodieMetricsConfig.newBuilder()
          .withReporterType(writeConfig.getMetricsReporterType().toString())
          .withExecutorMetrics(writeConfig.isExecutorMetricsEnabled())
          .on(true).build());
      switch (writeConfig.getMetricsReporterType()) {
        case GRAPHITE:
          builder.withMetricsGraphiteConfig(HoodieMetricsGraphiteConfig.newBuilder()
              .onGraphitePort(writeConfig.getGraphiteServerPort())
              .toGraphiteHost(writeConfig.getGraphiteServerHost())
              .usePrefix(writeConfig.getGraphiteMetricPrefix()).build());
          break;
        case JMX:
          builder.withMetricsJmxConfig(HoodieMetricsJmxConfig.newBuilder()
              .onJmxPort(writeConfig.getJmxPort())
              .toJmxHost(writeConfig.getJmxHost())
              .build());
          break;
        case DATADOG:
        case PROMETHEUS:
        case PROMETHEUS_PUSHGATEWAY:
        case CONSOLE:
        case INMEMORY:
          break;
        default:
          throw new HoodieMetadataException("Unsupported Metrics Reporter type " + writeConfig.getMetricsReporterType());
      }
    }
    return builder.build();
  }

  public HoodieWriteConfig getWriteConfig() {
    return metadataWriteConfig;
  }

  public HoodieBackedTableMetadata metadata() {
    return metadata;
  }

  /**
   * Initialize the metadata table if it does not exist.
   * <p>
   * If the metadata table does not exist, then file and partition listing is used to bootstrap the table.
   */
  protected abstract <T extends SpecificRecordBase> void initialize(HoodieEngineContext engineContext,
                                                                    Option<T> actionMetadata);

  public void initTableMetadata() {
    try {
      if (this.metadata != null) {
        this.metadata.close();
      }
      this.metadata = new HoodieBackedTableMetadata(engineContext, dataWriteConfig.getMetadataConfig(),
          dataWriteConfig.getBasePath(), dataWriteConfig.getSpillableMapBasePath());
      this.metadataMetaClient = metadata.getMetadataMetaClient();
    } catch (Exception e) {
      throw new HoodieException("Error initializing metadata table for reads", e);
    }
  }

  /**
   * Bootstrap the metadata table if needed.
   *
   * @param engineContext  - Engine context
   * @param dataMetaClient - Meta client for the data table
   * @param actionMetadata - Optional action metadata
   * @param <T>            - Action metadata types extending Avro generated SpecificRecordBase
   * @throws IOException
   */
  protected <T extends SpecificRecordBase> void bootstrapIfNeeded(HoodieEngineContext engineContext,
                                                                  HoodieTableMetaClient dataMetaClient,
                                                                  Option<T> actionMetadata) throws IOException {
    HoodieTimer timer = new HoodieTimer().startTimer();

    boolean exists = dataMetaClient.getFs().exists(new Path(metadataWriteConfig.getBasePath(),
        HoodieTableMetaClient.METAFOLDER_NAME));
    boolean rebootstrap = false;

    // If the un-synced instants have been archived, then
    // the metadata table will need to be bootstrapped again.
    if (exists) {
      final HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf.get())
          .setBasePath(metadataWriteConfig.getBasePath()).build();
      final Option<HoodieInstant> latestMetadataInstant =
          metadataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();

      rebootstrap = isBootstrapNeeded(latestMetadataInstant, actionMetadata);
    }

    if (rebootstrap) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.REBOOTSTRAP_STR, 1));
      LOG.info("Deleting Metadata Table directory so that it can be re-bootstrapped");
      dataMetaClient.getFs().delete(new Path(metadataWriteConfig.getBasePath()), true);
      exists = false;
    }

    if (!exists) {
      // Initialize for the first time by listing partitions and files directly from the file system
      if (bootstrapFromFilesystem(engineContext, dataMetaClient)) {
        metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.INITIALIZE_STR, timer.endTimer()));
      }
    }
  }

  /**
   * Whether bootstrap operation needed for this metadata table.
   * <p>
   * Rollback of the first commit would look like un-synced instants in the metadata table.
   * Action metadata is needed to verify the instant time and avoid erroneous bootstrapping.
   * <p>
   * TODO: Revisit this logic and validate that filtering for all
   *       commits timeline is the right thing to do
   *
   * @return True if the bootstrap is not needed, False otherwise
   */
  private <T extends SpecificRecordBase> boolean isBootstrapNeeded(Option<HoodieInstant> latestMetadataInstant,
                                                                   Option<T> actionMetadata) {
    if (!latestMetadataInstant.isPresent()) {
      LOG.warn("Metadata Table will need to be re-bootstrapped as no instants were found");
      return true;
    }

    final String latestMetadataInstantTimestamp = latestMetadataInstant.get().getTimestamp();
    if (latestMetadataInstantTimestamp.equals(SOLO_COMMIT_TIMESTAMP)) {
      return false;
    }

    boolean isRollbackAction = false;
    List<String> rollbackedTimestamps = Collections.emptyList();
    if (actionMetadata.isPresent() && actionMetadata.get() instanceof HoodieRollbackMetadata) {
      isRollbackAction = true;
      List<HoodieInstantInfo> rollbackedInstants =
          ((HoodieRollbackMetadata) actionMetadata.get()).getInstantsRollback();
      rollbackedTimestamps = rollbackedInstants.stream().map(instant -> {
        return instant.getCommitTime().toString();
      }).collect(Collectors.toList());
    }

    if (dataMetaClient.getActiveTimeline().getAllCommitsTimeline().isBeforeTimelineStarts(
        latestMetadataInstant.get().getTimestamp())
        && (!isRollbackAction || !rollbackedTimestamps.contains(latestMetadataInstantTimestamp))) {
      LOG.warn("Metadata Table will need to be re-bootstrapped as un-synced instants have been archived."
          + " latestMetadataInstant=" + latestMetadataInstant.get().getTimestamp()
          + ", latestDataInstant=" + dataMetaClient.getActiveTimeline().firstInstant().get().getTimestamp());
      return true;
    }

    return false;
  }

  /**
   * Initialize the Metadata Table by listing files and partitions from the file system.
   *
   * @param dataMetaClient {@code HoodieTableMetaClient} for the dataset.
   */
  private boolean bootstrapFromFilesystem(HoodieEngineContext engineContext, HoodieTableMetaClient dataMetaClient) throws IOException {
    ValidationUtils.checkState(enabled, "Metadata table cannot be initialized as it is not enabled");

    // We can only bootstrap if there are no pending operations on the dataset
    Option<HoodieInstant> pendingDataInstant = Option.fromJavaOptional(dataMetaClient.getActiveTimeline()
        .getReverseOrderedInstants().filter(i -> !i.isCompleted()).findFirst());
    if (pendingDataInstant.isPresent()) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.BOOTSTRAP_ERR_STR, 1));
      LOG.warn("Cannot bootstrap metadata table as operation is in progress in dataset: " + pendingDataInstant.get());
      return false;
    }

    // If there is no commit on the dataset yet, use the SOLO_COMMIT_TIMESTAMP as the instant time for initial commit
    // Otherwise, we use the latest commit timestamp.
    String createInstantTime = dataMetaClient.getActiveTimeline().getReverseOrderedInstants().findFirst()
        .map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);
    LOG.info("Creating a new metadata table in " + metadataWriteConfig.getBasePath() + " at instant " + createInstantTime);

    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setTableName(tableName)
        .setArchiveLogFolder(ARCHIVELOG_FOLDER.defaultValue())
      .setPayloadClassName(HoodieMetadataPayload.class.getName())
      .setBaseFileFormat(HoodieFileFormat.HFILE.toString())
      .initTable(hadoopConf.get(), metadataWriteConfig.getBasePath());

    initTableMetadata();
    initializeFileGroups(dataMetaClient, MetadataPartitionType.FILES, createInstantTime, 1);

    // List all partitions in the basePath of the containing dataset
    LOG.info("Initializing metadata table by using file listings in " + dataWriteConfig.getBasePath());
    List<DirectoryInfo> dirInfoList = listAllPartitions(dataMetaClient);

    // During bootstrap, the list of files to be committed can be huge. So creating a HoodieCommitMetadata out of these
    // large number of files and calling the existing update(HoodieCommitMetadata) function does not scale well.
    // Hence, we have a special commit just for the bootstrap scenario.
    commit(dirInfoList, createInstantTime, false);
    return true;
  }

  /**
   * Function to find hoodie partitions and list files in them in parallel.
   *
   * @param dataMetaClient
   * @return Map of partition names to a list of FileStatus for all the files in the partition
   */
  private List<DirectoryInfo> listAllPartitions(HoodieTableMetaClient datasetMetaClient) {
    List<Path> pathsToList = new LinkedList<>();
    pathsToList.add(new Path(dataWriteConfig.getBasePath()));

    List<DirectoryInfo> partitionsToBootstrap = new LinkedList<>();
    final int fileListingParallelism = metadataWriteConfig.getFileListingParallelism();
    SerializableConfiguration conf = new SerializableConfiguration(dataMetaClient.getHadoopConf());
    final String dirFilterRegex = dataWriteConfig.getMetadataConfig().getDirectoryFilterRegex();
    final String datasetBasePath = dataMetaClient.getBasePath();

    while (!pathsToList.isEmpty()) {
      // In each round we will list a section of directories
      int numDirsToList = Math.min(fileListingParallelism, pathsToList.size());
      // List all directories in parallel
      List<DirectoryInfo> processedDirectories = engineContext.map(pathsToList.subList(0,  numDirsToList), path -> {
        FileSystem fs = path.getFileSystem(conf.get());
        String relativeDirPath = FSUtils.getRelativePartitionPath(new Path(datasetBasePath), path);
        return new DirectoryInfo(relativeDirPath, fs.listStatus(path));
      }, numDirsToList);

      pathsToList = new LinkedList<>(pathsToList.subList(numDirsToList, pathsToList.size()));

      // If the listing reveals a directory, add it to queue. If the listing reveals a hoodie partition, add it to
      // the results.
      for (DirectoryInfo dirInfo : processedDirectories) {
        if (!dirFilterRegex.isEmpty()) {
          final String relativePath = dirInfo.getRelativePath();
          if (!relativePath.isEmpty()) {
            Path partitionPath = new Path(datasetBasePath, relativePath);
            if (partitionPath.getName().matches(dirFilterRegex)) {
              LOG.info("Ignoring directory " + partitionPath + " which matches the filter regex " + dirFilterRegex);
              continue;
            }
          }
        }

        if (dirInfo.isPartition()) {
          // Add to result
          partitionsToBootstrap.add(dirInfo);
        } else {
          // Add sub-dirs to the queue
          pathsToList.addAll(dirInfo.getSubdirs());
        }
      }
    }

    return partitionsToBootstrap;
  }

  /**
   * Initialize file groups for a partition. For file listing, we just have one file group.
   *
   * All FileGroups for a given metadata partition has a fixed prefix as per the {@link MetadataPartitionType#getFileIdPrefix()}.
   * Each file group is suffixed with 4 digits with increments of 1 starting with 0000.
   *
   * Lets say we configure 10 file groups for record level index partittion, and prefix as "record-index-bucket-"
   * File groups will be named as :
   *    record-index-bucket-0000, .... -> ..., record-index-bucket-0009
   */
  private void initializeFileGroups(HoodieTableMetaClient dataMetaClient, MetadataPartitionType metadataPartition, String instantTime,
                                    int fileGroupCount) throws IOException {

    final HashMap<HeaderMetadataType, String> blockHeader = new HashMap<>();
    blockHeader.put(HeaderMetadataType.INSTANT_TIME, instantTime);
    // Archival of data table has a dependency on compaction(base files) in metadata table.
    // It is assumed that as of time Tx of base instant (/compaction time) in metadata table,
    // all commits in data table is in sync with metadata table. So, we always start with log file for any fileGroup.
    final HoodieDeleteBlock block = new HoodieDeleteBlock(new HoodieKey[0], blockHeader);

    LOG.info(String.format("Creating %d file groups for partition %s with base fileId %s at instant time %s",
        fileGroupCount, metadataPartition.partitionPath(), metadataPartition.getFileIdPrefix(), instantTime));
    for (int i = 0; i < fileGroupCount; ++i) {
      final String fileGroupFileId = String.format("%s%04d", metadataPartition.getFileIdPrefix(), i);
      try {
        HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder()
            .onParentPath(FSUtils.getPartitionPath(metadataWriteConfig.getBasePath(), metadataPartition.partitionPath()))
            .withFileId(fileGroupFileId).overBaseCommit(instantTime)
            .withLogVersion(HoodieLogFile.LOGFILE_BASE_VERSION)
            .withFileSize(0L)
            .withSizeThreshold(metadataWriteConfig.getLogFileMaxSize())
            .withFs(dataMetaClient.getFs())
            .withRolloverLogWriteToken(HoodieLogFormat.DEFAULT_WRITE_TOKEN)
            .withLogWriteToken(HoodieLogFormat.DEFAULT_WRITE_TOKEN)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
        writer.appendBlock(block);
        writer.close();
      } catch (InterruptedException e) {
        throw new HoodieException("Failed to created fileGroup " + fileGroupFileId + " for partition " + metadataPartition.partitionPath(), e);
      }
    }
  }

  /**
   * Interface to assist in converting commit metadata to List of HoodieRecords to be written to metadata table.
   * Updates of different commit metadata uses the same method to convert to HoodieRecords and hence.
   */
  private interface ConvertMetadataFunction {
    List<HoodieRecord> convertMetadata();
  }

  /**
   * Processes commit metadata from data table and commits to metadata table.
   * @param instantTime instant time of interest.
   * @param convertMetadataFunction converter function to convert the respective metadata to List of HoodieRecords to be written to metadata table.
   * @param <T> type of commit metadata.
   * @param canTriggerTableService true if table services can be triggered. false otherwise.
   */
  private <T> void processAndCommit(String instantTime, ConvertMetadataFunction convertMetadataFunction, boolean canTriggerTableService) {
    if (enabled && metadata != null) {
      List<HoodieRecord> records = convertMetadataFunction.convertMetadata();
      commit(records, MetadataPartitionType.FILES.partitionPath(), instantTime, canTriggerTableService);
    }
  }

  /**
   * Update from {@code HoodieCommitMetadata}.
   * @param commitMetadata {@code HoodieCommitMetadata}
   * @param instantTime Timestamp at which the commit was performed
   * @param isTableServiceAction {@code true} if commit metadata is pertaining to a table service. {@code false} otherwise.
   */
  @Override
  public void update(HoodieCommitMetadata commitMetadata, String instantTime, boolean isTableServiceAction) {
    processAndCommit(instantTime, () -> HoodieTableMetadataUtil.convertMetadataToRecords(commitMetadata, instantTime), !isTableServiceAction);
  }

  /**
   * Update from {@code HoodieCleanMetadata}.
   *
   * @param cleanMetadata {@code HoodieCleanMetadata}
   * @param instantTime Timestamp at which the clean was completed
   */
  @Override
  public void update(HoodieCleanMetadata cleanMetadata, String instantTime) {
    processAndCommit(instantTime, () -> HoodieTableMetadataUtil.convertMetadataToRecords(cleanMetadata, instantTime),
        false);
  }

  /**
   * Update from {@code HoodieRestoreMetadata}.
   *
   * @param restoreMetadata {@code HoodieRestoreMetadata}
   * @param instantTime Timestamp at which the restore was performed
   */
  @Override
  public void update(HoodieRestoreMetadata restoreMetadata, String instantTime) {
    processAndCommit(instantTime, () -> HoodieTableMetadataUtil.convertMetadataToRecords(metadataMetaClient.getActiveTimeline(),
        restoreMetadata, instantTime, metadata.getSyncedInstantTime()), false);
  }

  /**
   * Update from {@code HoodieRollbackMetadata}.
   *
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param instantTime Timestamp at which the rollback was performed
   */
  @Override
  public void update(HoodieRollbackMetadata rollbackMetadata, String instantTime) {
    if (enabled && metadata != null) {
      // Is this rollback of an instant that has been synced to the metadata table?
      String rollbackInstant = rollbackMetadata.getCommitsRollback().get(0);
      boolean wasSynced = metadataMetaClient.getActiveTimeline().containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, rollbackInstant));
      if (!wasSynced) {
        // A compaction may have taken place on metadata table which would have included this instant being rolled back.
        // Revisit this logic to relax the compaction fencing : https://issues.apache.org/jira/browse/HUDI-2458
        Option<String> latestCompaction = metadata.getLatestCompactionTime();
        if (latestCompaction.isPresent()) {
          wasSynced = HoodieTimeline.compareTimestamps(rollbackInstant, HoodieTimeline.LESSER_THAN_OR_EQUALS, latestCompaction.get());
        }
      }

      List<HoodieRecord> records = HoodieTableMetadataUtil.convertMetadataToRecords(metadataMetaClient.getActiveTimeline(), rollbackMetadata, instantTime,
          metadata.getSyncedInstantTime(), wasSynced);
      commit(records, MetadataPartitionType.FILES.partitionPath(), instantTime, false);
    }
  }

  @Override
  public void close() throws Exception {
    if (metadata != null) {
      metadata.close();
    }
  }

  /**
   * Commit the {@code HoodieRecord}s to Metadata Table as a new delta-commit.
   *  @param records The list of records to be written.
   * @param partitionName The partition to which the records are to be written.
   * @param instantTime The timestamp to use for the deltacommit.
   * @param canTriggerTableService true if table services can be scheduled and executed. false otherwise.
   */
  protected abstract void commit(List<HoodieRecord> records, String partitionName, String instantTime, boolean canTriggerTableService);

  /**
   *  Perform a compaction on the Metadata Table.
   *
   * Cases to be handled:
   *   1. We cannot perform compaction if there are previous inflight operations on the dataset. This is because
   *      a compacted metadata base file at time Tx should represent all the actions on the dataset till time Tx.
   *
   *   2. In multi-writer scenario, a parallel operation with a greater instantTime may have completed creating a
   *      deltacommit.
   */
  protected void compactIfNecessary(AbstractHoodieWriteClient writeClient, String instantTime) {
    String latestDeltacommitTime = metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().lastInstant()
        .get().getTimestamp();
    List<HoodieInstant> pendingInstants = dataMetaClient.reloadActiveTimeline().filterInflightsAndRequested()
        .findInstantsBefore(latestDeltacommitTime).getInstants().collect(Collectors.toList());

    if (!pendingInstants.isEmpty()) {
      LOG.info(String.format("Cannot compact metadata table as there are %d inflight instants before latest deltacommit %s: %s",
          pendingInstants.size(), latestDeltacommitTime, Arrays.toString(pendingInstants.toArray())));
      return;
    }

    // Trigger compaction with suffixes based on the same instant time. This ensures that any future
    // delta commits synced over will not have an instant time lesser than the last completed instant on the
    // metadata table.
    final String compactionInstantTime = latestDeltacommitTime + "001";
    if (writeClient.scheduleCompactionAtInstant(compactionInstantTime, Option.empty())) {
      writeClient.compact(compactionInstantTime);
    }
  }

  protected void doClean(AbstractHoodieWriteClient writeClient, String instantTime) {
    // Trigger cleaning with suffixes based on the same instant time. This ensures that any future
    // delta commits synced over will not have an instant time lesser than the last completed instant on the
    // metadata table.
    writeClient.clean(instantTime + "002");
  }

  /**
   * Commit the partition to file listing information to Metadata Table as a new delta-commit.
   *
   */
  protected abstract void commit(List<DirectoryInfo> dirInfoList, String createInstantTime, boolean canTriggerTableService);

  /**
   * A class which represents a directory and the files and directories inside it.
   *
   * A {@code PartitionFileInfo} object saves the name of the partition and various properties requires of each file
   * required for bootstrapping the metadata table. Saving limited properties reduces the total memory footprint when
   * a very large number of files are present in the dataset being bootstrapped.
   */
  public static class DirectoryInfo implements Serializable {
    // Relative path of the directory (relative to the base directory)
    private String relativePath;
    // Map of filenames within this partition to their respective sizes
    HashMap<String, Long> filenameToSizeMap;
    // List of directories within this partition
    private List<Path> subdirs = new ArrayList<>();
    // Is this a HUDI partition
    private boolean isPartition = false;

    public DirectoryInfo(String relativePath, FileStatus[] fileStatus) {
      this.relativePath = relativePath;

      // Pre-allocate with the maximum length possible
      filenameToSizeMap = new HashMap<>(fileStatus.length);

      for (FileStatus status : fileStatus) {
        if (status.isDirectory()) {
          // Ignore .hoodie directory as there cannot be any partitions inside it
          if (!status.getPath().getName().equals(HoodieTableMetaClient.METAFOLDER_NAME)) {
            this.subdirs.add(status.getPath());
          }
        } else if (status.getPath().getName().equals(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE)) {
          // Presence of partition meta file implies this is a HUDI partition
          this.isPartition = true;
        } else if (FSUtils.isDataFile(status.getPath())) {
          // Regular HUDI data file (base file or log file)
          filenameToSizeMap.put(status.getPath().getName(), status.getLen());
        }
      }
    }

    public String getRelativePath() {
      return relativePath;
    }

    public int getTotalFiles() {
      return filenameToSizeMap.size();
    }

    public boolean isPartition() {
      return isPartition;
    }

    public List<Path> getSubdirs() {
      return subdirs;
    }

    // Returns a map of filenames mapped to their lengths
    public Map<String, Long> getFileMap() {
      return filenameToSizeMap;
    }
  }
}
