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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.transaction.TransactionManager;
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
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieMetricsConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_ARCHIVELOG_FOLDER_PROP;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.metadata.HoodieTableMetadata.METADATA_TABLE_NAME_SUFFIX;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;

/**
 * Writer implementation backed by an internal hudi table. Partition and file listing are saved within an internal MOR table
 * called Metadata Table. This table is created by listing files and partitions (first time)
 * and kept in sync using the instants on the main dataset.
 */
public abstract class HoodieBackedTableMetadataWriter<R> implements HoodieTableMetadataWriter {

  private static final Logger LOG = LogManager.getLogger(HoodieBackedTableMetadataWriter.class);

  protected HoodieWriteConfig metadataWriteConfig;
  protected HoodieWriteConfig datasetWriteConfig;
  protected String tableName;

  protected HoodieBackedTableMetadata metadata;
  protected HoodieTableMetaClient metaClient;
  protected HoodieTableMetaClient datasetMetaClient;
  protected Option<HoodieMetadataMetrics> metrics;
  protected boolean enabled;
  protected SerializableConfiguration hadoopConf;
  protected final transient HoodieEngineContext engineContext;
  protected TransactionManager txnManager;

  protected HoodieBackedTableMetadataWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig,
      HoodieEngineContext engineContext) {
    this.datasetWriteConfig = writeConfig;
    this.engineContext = engineContext;
    this.hadoopConf = new SerializableConfiguration(hadoopConf);

    if (writeConfig.useFileListingMetadata()) {
      this.tableName = writeConfig.getTableName() + METADATA_TABLE_NAME_SUFFIX;
      this.metadataWriteConfig = createMetadataWriteConfig(writeConfig);
      enabled = true;

      // Inline compaction and auto clean is required as we dont expose this table outside
      ValidationUtils.checkArgument(!this.metadataWriteConfig.isAutoClean(), "Cleaning is controlled internally for Metadata table.");
      ValidationUtils.checkArgument(!this.metadataWriteConfig.inlineCompactionEnabled(), "Compaction is controlled internally for metadata table.");
      // Metadata Table cannot have metadata listing turned on. (infinite loop, much?)
      ValidationUtils.checkArgument(this.metadataWriteConfig.shouldAutoCommit(), "Auto commit is required for Metadata Table");
      ValidationUtils.checkArgument(!this.metadataWriteConfig.useFileListingMetadata(), "File listing cannot be used for Metadata Table");

      initRegistry();
      this.datasetMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(datasetWriteConfig.getBasePath()).build();
      initTransactionManager();
      initTableMetadata();
      initialize(engineContext);
    } else {
      enabled = false;
      this.metrics = Option.empty();
    }
  }

  /**
   * Initialize the {@code TransactionManager} to use for metadata table.
   *
   * In HUDI multi writer mode, each operation will sync to metadata table before completion. Metadata table has common
   * base and log files to update for each operation. So we can only support serialized operations.
   */
  private void initTransactionManager() {
    // The lock location should be different from the dataset
    Properties properties = new Properties();
    properties.putAll(datasetWriteConfig.getProps());
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, properties.getProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, datasetWriteConfig.getBasePath() + "/.hoodie/.locks") + "/metadata");
    HoodieWriteConfig txConfig = HoodieWriteConfig.newBuilder().withProperties(properties).build();
    this.txnManager = new TransactionManager(txConfig, datasetMetaClient.getFs());
  }

  /**
   * Create a {@code HoodieWriteConfig} to use for the Metadata Table.
   *
   * @param writeConfig {@code HoodieWriteConfig} of the main dataset writer
   */
  private HoodieWriteConfig createMetadataWriteConfig(HoodieWriteConfig writeConfig) {
    int parallelism = writeConfig.getMetadataInsertParallelism();

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
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
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
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER)
            .retainCommits(writeConfig.getMetadataCleanerCommitsRetained())
            .archiveCommitsWith(writeConfig.getMetadataMinCommitsToKeep(), writeConfig.getMetadataMaxCommitsToKeep())
            // we will trigger compaction manually, to control the instant times
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(writeConfig.getMetadataCompactDeltaCommitMax()).build())
        .withParallelism(parallelism, parallelism)
        .withDeleteParallelism(parallelism)
        .withRollbackParallelism(parallelism)
        .withFinalizeWriteParallelism(parallelism);

    if (writeConfig.isMetricsOn()) {
      HoodieMetricsConfig.Builder metricsConfig = HoodieMetricsConfig.newBuilder()
          .withReporterType(writeConfig.getMetricsReporterType().toString())
          .withExecutorMetrics(writeConfig.isExecutorMetricsEnabled())
          .on(true);
      switch (writeConfig.getMetricsReporterType()) {
        case GRAPHITE:
          metricsConfig.onGraphitePort(writeConfig.getGraphiteServerPort())
              .toGraphiteHost(writeConfig.getGraphiteServerHost())
              .usePrefix(writeConfig.getGraphiteMetricPrefix());
          break;
        case JMX:
          metricsConfig.onJmxPort(writeConfig.getJmxPort())
              .toJmxHost(writeConfig.getJmxHost());
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

      builder.withMetricsConfig(metricsConfig.build());
    }

    return builder.build();
  }

  public HoodieWriteConfig getWriteConfig() {
    return metadataWriteConfig;
  }

  public HoodieBackedTableMetadata metadata() {
    return metadata;
  }

  protected void initTableMetadata() {
    try {
      if (this.metadata != null) {
        this.metadata.close();
      }
      this.metadata = new HoodieBackedTableMetadata(engineContext, datasetWriteConfig.getMetadataConfig(),
          datasetWriteConfig.getBasePath(), datasetWriteConfig.getSpillableMapBasePath());
      this.metaClient = metadata.getMetaClient();
    } catch (Exception e) {
      throw new HoodieException("Error initializing metadata table for reads", e);
    }
  }

  protected void bootstrapIfNeeded(HoodieEngineContext engineContext, HoodieTableMetaClient datasetMetaClient) throws IOException {
    boolean exists = datasetMetaClient.getFs().exists(new Path(metadataWriteConfig.getBasePath(), HoodieTableMetaClient.METAFOLDER_NAME));
    boolean rebootstrap = false;
    if (exists) {
      // If the un-synched instants have been archived then the metadata table will need to be bootstrapped again
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf.get())
          .setBasePath(metadataWriteConfig.getBasePath()).build();
      Option<HoodieInstant> latestMetadataInstant = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
      if (!latestMetadataInstant.isPresent()) {
        LOG.warn("Metadata Table will need to be re-bootstrapped as no instants were found");
        rebootstrap = true;
      } else if (!latestMetadataInstant.get().getTimestamp().equals(SOLO_COMMIT_TIMESTAMP)
          && datasetMetaClient.getActiveTimeline().isBeforeTimelineStarts(latestMetadataInstant.get().getTimestamp())) {
        LOG.warn("Metadata Table will need to be re-bootstrapped as un-synced instants have been archived."
            + " latestMetadataInstant=" + latestMetadataInstant.get().getTimestamp()
            + ", latestDatasetInstant=" + datasetMetaClient.getActiveTimeline().firstInstant().get().getTimestamp());
        rebootstrap = true;
      }
    }

    if (rebootstrap) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.REBOOTSTRAP_STR, 1));
      LOG.info("Deleting Metadata Table directory so that it can be re-bootstrapped");
      datasetMetaClient.getFs().delete(new Path(metadataWriteConfig.getBasePath()), true);
      exists = false;
    }

    // If there is no commit on the dataset yet, use the SOLO_COMMIT_TIMESTAMP as the instant time for initial commit
    // Otherwise, we use the latest commit timestamp.
    String createInstantTime = datasetMetaClient.getActiveTimeline().getReverseOrderedInstants().findFirst()
        .map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);

    Option<List<String>> partitionsBootstrapped = Option.empty();

    // Bootstrap file listing if required
    if (!exists) {
      HoodieTimer timer = new HoodieTimer().startTimer();
      partitionsBootstrapped = bootstrapFileListings(engineContext, datasetMetaClient, createInstantTime);
      if (partitionsBootstrapped.isPresent()) {
        metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.INITIALIZE_FILE_LISTING_STR, timer.endTimer()));
      } else if (datasetWriteConfig.isRecordLevelIndexEnabled()) {
        // Since record level index is enabled, we need the file listing bootstrap to succeed.
        throw new HoodieMetadataException("Failed to bootstrap file listing for Metadata Table");
      } else {
        // Bootstrap will be attempted next time.
        return;
      }
    }

    // Bootstrap record level index if required
    if (datasetWriteConfig.isRecordLevelIndexEnabled()) {
      HoodieTimer timer = new HoodieTimer().startTimer();
      final long foundShardCount = HoodieTableMetadataUtil.loadPartitionShards(metaClient, MetadataPartitionType.RECORD_LEVEL_INDEX.partitionPath()).size();
      // The number of existing shards should be 0 if bootstrap failed as the commit would not have succeeded. We
      // do not support changing the number of shards here.
      ValidationUtils.checkState(foundShardCount == 0 || foundShardCount == datasetWriteConfig.getRecordLevelIndexShardCount(),
          String.format("Found %d shards for record index which is different than configured count %d", foundShardCount,
            datasetWriteConfig.getRecordLevelIndexShardCount()));

      // If we did not bootstrap file listing, we need to get the list of partitions
      if (!partitionsBootstrapped.isPresent()) {
        partitionsBootstrapped = Option.of(FSUtils.getAllPartitionPaths(engineContext, datasetMetaClient.getBasePath(), false, datasetWriteConfig.shouldAssumeDatePartitioning()));
      }

      if (bootstrapRecordLevelIndex(engineContext, datasetMetaClient, createInstantTime, partitionsBootstrapped.get())) {
        metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.INITIALIZE_RECORD_INDEX_STR, timer.endTimer()));
      } else {
        // Failure to bootstrap is a fatal error as we wont be able to tag records for commit and it may lead to
        // duplicates.
        throw new HoodieMetadataException("Failed to bootstrap file listing for Metadata Table");
      }
    }

    performInsert(createInstantTime);
  }

  /**
   * Initialize the Metadata Table by listing files and partitions from the file system.
   *
   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset
   * @param createInstantTime InstantTime to use for the commit
   */
  private Option<List<String>> bootstrapFileListings(HoodieEngineContext engineContext, HoodieTableMetaClient datasetMetaClient,
      String createInstantTime) throws IOException {
    ValidationUtils.checkState(enabled, "Metadata table cannot be initialized as it is not enabled");

    // We can only bootstrap if there are no pending operations on the dataset
    Option<HoodieInstant> pendingInstantOption = Option.fromJavaOptional(datasetMetaClient.getActiveTimeline()
        .getReverseOrderedInstants().filter(i -> !i.isCompleted()).findFirst());
    // TODO: disabled for testing
    //if (pendingInstantOption.isPresent()) {
    //  metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.BOOTSTRAP_ERR_STR, 1));
    //  LOG.warn("Cannot bootstrap metadata table as operation is in progress: " + pendingInstantOption.get());
    //  return Option.empty();
    //}

    LOG.info("Creating a new metadata table in " + metadataWriteConfig.getBasePath() + " at instant " + createInstantTime);

    HoodieTableMetaClient.withPropertyBuilder()
      .setTableType(HoodieTableType.MERGE_ON_READ)
      .setTableName(tableName)
      .setArchiveLogFolder(HOODIE_ARCHIVELOG_FOLDER_PROP.defaultValue())
      .setPayloadClassName(HoodieMetadataPayload.class.getName())
      .setBaseFileFormat(HoodieFileFormat.HFILE.toString())
      .initTable(hadoopConf.get(), metadataWriteConfig.getBasePath());

    initTableMetadata();
    initializeShards(datasetMetaClient, MetadataPartitionType.FILES.partitionPath(), createInstantTime, 1);

    // List all partitions in the basePath of the containing dataset
    LOG.info("Initializing metadata table by using file listings in " + datasetWriteConfig.getBasePath());
    Map<String, List<FileStatus>> partitionToFileStatus = getPartitionsToFilesMapping(datasetMetaClient);

    // Create a HoodieCommitMetadata with writeStats for all discovered files
    int[] stats = {0};
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();

    partitionToFileStatus.forEach((partition, statuses) -> {
      // Filter the statuses to only include files which were created before or on createInstantTime
      statuses.stream().filter(status -> {
        String filename = status.getPath().getName();
        return !HoodieTimeline.compareTimestamps(FSUtils.getCommitTime(filename), HoodieTimeline.GREATER_THAN,
            createInstantTime);
      }).forEach(status -> {
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setPath((partition.isEmpty() ? "" : partition + Path.SEPARATOR) + status.getPath().getName());
        writeStat.setPartitionPath(partition);
        writeStat.setTotalWriteBytes(status.getLen());
        commitMetadata.addWriteStat(partition, writeStat);
        stats[0] += 1;
      });

      // If the partition has no files then create a writeStat with no file path
      if (commitMetadata.getWriteStats(partition) == null) {
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setPartitionPath(partition);
        commitMetadata.addWriteStat(partition, writeStat);
      }
    });

    LOG.info("Committing " + partitionToFileStatus.size() + " partitions and " + stats[0] + " files to metadata");
    List<HoodieRecord> records = HoodieTableMetadataUtil.convertMetadataToRecords(commitMetadata, createInstantTime);
    queueForUpdate(records, MetadataPartitionType.FILES, createInstantTime);

    return Option.of(partitionToFileStatus.keySet().stream().sorted().collect(Collectors.toList()));
  }

  /**
   * Bootstrap the record level index.
   *
   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset
   * @param createInstantTime InstantTime to use for the commit
   * @param partitions List of partitions from which the data files are to be read
   */
  private boolean bootstrapRecordLevelIndex(HoodieEngineContext engineContext, HoodieTableMetaClient datasetMetaClient,
      String createInstantTime, List<String> partitions) throws IOException {
    ValidationUtils.checkState(enabled, "Record level index cannot be initialized as Metadata Table is not enabled");
    ValidationUtils.checkState(datasetWriteConfig.isRecordLevelIndexEnabled(), "Record level index cannot be initialized as it is not enabled");

    HoodieTimer timer = new HoodieTimer().startTimer();

    // Initialize the shards
    final int numShards = datasetWriteConfig.getRecordLevelIndexShardCount();
    initializeShards(datasetMetaClient, MetadataPartitionType.RECORD_LEVEL_INDEX.partitionPath(), createInstantTime, numShards);

    BaseFileOnlyView fsView = getTableFileSystemView();

    // TODO: scale is an issue here. Remove this limit later
    partitions = partitions.subList(partitions.size() - Math.min(partitions.size(), 2), partitions.size());

    // Collect the list of base files present
    List<Pair<String, String>> partitionBaseFilePairs = engineContext.flatMap(partitions, partition -> {
      return fsView.getLatestBaseFiles(partition).map(basefile -> Pair.of(partition, basefile.getFileName()));
    }, partitions.size());

    LOG.info("Initializing record level index from " + partitionBaseFilePairs.size() + " base files in "
        + partitions.size() + " partitions");

    // Collect record keys from the files in parallel
    if (!partitionBaseFilePairs.isEmpty()) {
      Pair<R, Long> recordKeysWithCount = readRecordKeysFromBaseFiles(engineContext, partitionBaseFilePairs);
      queueForUpdate(recordKeysWithCount.getLeft(), MetadataPartitionType.RECORD_LEVEL_INDEX, createInstantTime);
      metrics.ifPresent(m -> m.incrementMetric(HoodieMetadataMetrics.RECORDINDEX_BOOTSTRAP_COUNT_STR, recordKeysWithCount.getRight()));
    }

    metrics.ifPresent(m -> m.incrementMetric(HoodieMetadataMetrics.RECORDINDEX_SHARD_COUNT_STR, numShards));
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.READ_RECORDKEYS_BOOTSTRAP_STR, timer.endTimer()));
    return true;
  }

  private void performInsert(String instantTime) {
    commit(instantTime, true);
  }

  private void performUpdate(String instantTime) {
    commit(instantTime, false);
  }

  /**
   * Function to find hoodie partitions and list files in them in parallel.
   *
   * @param datasetMetaClient
   * @return Map of partition names to a list of FileStatus for all the files in the partition
   */
  private Map<String, List<FileStatus>> getPartitionsToFilesMapping(HoodieTableMetaClient datasetMetaClient) {
    List<Path> pathsToList = new LinkedList<>();
    pathsToList.add(new Path(datasetWriteConfig.getBasePath()));

    Map<String, List<FileStatus>> partitionToFileStatus = new HashMap<>();
    final int fileListingParallelism = metadataWriteConfig.getFileListingParallelism();
    SerializableConfiguration conf = new SerializableConfiguration(datasetMetaClient.getHadoopConf());
    final String dirFilterRegex = datasetWriteConfig.getMetadataConfig().getDirectoryFilterRegex();

    while (!pathsToList.isEmpty()) {
      int listingParallelism = Math.min(fileListingParallelism, pathsToList.size());
      // List all directories in parallel
      List<Pair<Path, FileStatus[]>> dirToFileListing = engineContext.map(pathsToList, path -> {
        FileSystem fs = path.getFileSystem(conf.get());
        return Pair.of(path, fs.listStatus(path));
      }, listingParallelism);
      pathsToList.clear();

      // If the listing reveals a directory, add it to queue. If the listing reveals a hoodie partition, add it to
      // the results.
      dirToFileListing.forEach(p -> {
        if (!dirFilterRegex.isEmpty() && p.getLeft().getName().matches(dirFilterRegex)) {
          LOG.info("Ignoring directory " + p.getLeft() + " which matches the filter regex " + dirFilterRegex);
          return;
        }

        List<FileStatus> filesInDir = Arrays.stream(p.getRight()).parallel()
            .filter(fs -> !fs.getPath().getName().equals(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE))
            .collect(Collectors.toList());

        if (p.getRight().length > filesInDir.size()) {
          String partitionName = FSUtils.getRelativePartitionPath(new Path(datasetMetaClient.getBasePath()), p.getLeft());
          // deal with Non-partition table, we should exclude .hoodie
          partitionToFileStatus.put(partitionName, filesInDir.stream()
              .filter(f -> !f.getPath().getName().equals(HoodieTableMetaClient.METAFOLDER_NAME)).collect(Collectors.toList()));
        } else {
          // Add sub-dirs to the queue
          pathsToList.addAll(Arrays.stream(p.getRight())
              .filter(fs -> fs.isDirectory() && !fs.getPath().getName().equals(HoodieTableMetaClient.METAFOLDER_NAME))
              .map(fs -> fs.getPath())
              .collect(Collectors.toList()));
        }
      });
    }

    return partitionToFileStatus;
  }

  /**
   * Initialize shards for a partition.
   *
   * Each shard is a single log file with the following format:
   *    <fileIdPrefix>ABCD
   * where ABCD are digits. This allows up to 9999 shards.
   *
   * Example:
   *    fc9f18eb-6049-4f47-bc51-23884bef0001
   *    fc9f18eb-6049-4f47-bc51-23884bef0002
   */
  private void initializeShards(HoodieTableMetaClient datasetMetaClient, String partition, String instantTime,
      int shardCount) throws IOException {
    ValidationUtils.checkArgument(shardCount <= 9999, "Maximum 9999 shards are supported.");

    final String newFileId = FSUtils.createNewFileIdPfx();
    final String newFileIdPrefix = newFileId.substring(0, 32);
    final HashMap<HeaderMetadataType, String> blockHeader = new HashMap<>();
    blockHeader.put(HeaderMetadataType.INSTANT_TIME, instantTime);
    final HoodieDeleteBlock block = new HoodieDeleteBlock(new HoodieKey[0], blockHeader);

    LOG.info(String.format("Creating %d shards for partition %s with base fileId %s at instant time %s",
        shardCount, partition, newFileId, instantTime));
    for (int i = 0; i < shardCount; ++i) {
      // Generate a indexed fileId for each shard and write a log block into it to create the file.
      final String shardFileId = String.format("%s%04d", newFileIdPrefix, i + 1);
      ValidationUtils.checkArgument(newFileId.length() == shardFileId.length(), "FileId should be of length " + newFileId.length());
      try {
        HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder()
            .onParentPath(FSUtils.getPartitionPath(metadataWriteConfig.getBasePath(), partition))
            .withFileId(shardFileId).overBaseCommit(instantTime)
            .withLogVersion(HoodieLogFile.LOGFILE_BASE_VERSION)
            .withFileSize(0L)
            .withSizeThreshold(metadataWriteConfig.getLogFileMaxSize())
            .withFs(datasetMetaClient.getFs())
            .withRolloverLogWriteToken(FSUtils.makeWriteToken(0, 0, 0))
            .withLogWriteToken(FSUtils.makeWriteToken(0, 0, 0))
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
        writer.appendBlock(block);
        writer.close();
      } catch (InterruptedException e) {
        throw new IOException("Failed to created record level index shard " + shardFileId, e);
      }
    }
  }

  protected String getShardFileName(String fileId, int shardIndex) {
    ValidationUtils.checkArgument(shardIndex <= 9999, "Maximum 9999 shards are supported.");
    return String.format("%s%04d", fileId.substring(0, 32), shardIndex + 1);
  }

  /**
   * Update from {@code HoodieCommitMetadata}.
   *
   * @param commitMetadata {@code HoodieCommitMetadata}
   * @param instantTime Timestamp at which the commit was performed
   */
  @Override
  public void update(HoodieCommitMetadata commitMetadata, String instantTime) {
    if (enabled) {
      this.txnManager.beginTransaction(Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime)),
          Option.empty());
      try {
        List<HoodieRecord> records = HoodieTableMetadataUtil.convertMetadataToRecords(commitMetadata, instantTime);
        queueForUpdate(records, MetadataPartitionType.FILES, instantTime);
        performUpdate(instantTime);
      } finally {
        this.txnManager.endTransaction();
      }
    }
  }

  /**
   * Update from {@code HoodieCleanMetadata}.
   *
   * @param cleanMetadata {@code HoodieCleanMetadata}
   * @param instantTime Timestamp at which the clean was completed
   */
  @Override
  public void update(HoodieCleanMetadata cleanMetadata, String instantTime) {
    if (enabled) {
      this.txnManager.beginTransaction(Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime)),
          Option.empty());
      try {
        List<HoodieRecord> records = HoodieTableMetadataUtil.convertMetadataToRecords(cleanMetadata, instantTime);
        queueForUpdate(records, MetadataPartitionType.FILES, instantTime);
        performUpdate(instantTime);
      } finally {
        this.txnManager.endTransaction();
      }
    }
  }

  /**
   * Update from {@code HoodieRestoreMetadata}.
   *
   * @param restoreMetadata {@code HoodieRestoreMetadata}
   * @param instantTime Timestamp at which the restore was performed
   */
  @Override
  public void update(HoodieRestoreMetadata restoreMetadata, String instantTime) {
    if (enabled) {
      this.txnManager.beginTransaction(Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime)),
          Option.empty());
      try {
        List<HoodieRecord> records = HoodieTableMetadataUtil.convertMetadataToRecords(restoreMetadata, instantTime, metadata.getSyncedInstantTime());
        queueForUpdate(records, MetadataPartitionType.FILES, instantTime);
        performUpdate(instantTime);
      } finally {
        this.txnManager.endTransaction();
      }
    }
  }

  /**
   * Update from {@code HoodieRollbackMetadata}.
   *
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param instantTime Timestamp at which the rollback was performed
   */
  @Override
  public void update(HoodieRollbackMetadata rollbackMetadata, String instantTime) {
    if (enabled) {
      this.txnManager.beginTransaction(Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime)),
          Option.empty());
      try {
        // Is this rollback of an instant that has been synced to the metadata table?
        String rollbackInstant = rollbackMetadata.getCommitsRollback().get(0);
        boolean wasSynced = metaClient.getActiveTimeline().containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, rollbackInstant));
        if (!wasSynced) {
          // A compaction may have taken place on metadata table which would have included this instant being rolled back.
          Option<String> latestCompaction = metadata.getLatestCompactionTime();
          if (latestCompaction.isPresent()) {
            wasSynced = HoodieTimeline.compareTimestamps(rollbackInstant, HoodieTimeline.LESSER_THAN_OR_EQUALS, latestCompaction.get());
          }
        }

        List<HoodieRecord> records = HoodieTableMetadataUtil.convertMetadataToRecords(rollbackMetadata, instantTime,
            metadata.getSyncedInstantTime(), wasSynced);
        queueForUpdate(records, MetadataPartitionType.FILES, instantTime);
        performUpdate(instantTime);
      } finally {
        this.txnManager.endTransaction();
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (metadata != null) {
      metadata.close();
    }
  }

  protected abstract void initRegistry();

  /**
   * Initialize the metadata table if it does not exist.
   *
   * If the metadata table did not exist, then file and partition listing is used to bootstrap the table.
   */
  protected abstract void initialize(HoodieEngineContext engineContext);

  /**
   * Returns a {@code HoodieTableFileSystemView} on the dataset.
   */
  protected abstract BaseFileOnlyView getTableFileSystemView();

  /**
   * Queue the given records for an update to the given partition.
   *
   * Each Metadata Table partition has fixed number of shards. The records should be sharded by their record-key so that
   * they are written to the correct shard.
   *
   * @param records The records to be queued
   * @param partitionName The partition to which the records are to be written.
   * @param instantTime The timestamp to use for the deltacommit.
   */
  protected abstract void queueForUpdate(R records, MetadataPartitionType partitionType, String instantTime);

  protected abstract void queueForUpdate(List<HoodieRecord> records, MetadataPartitionType partitionType, String instantTime);

  /**
   * Read the record keys from the given base files in parallel and return records.
   *
   * @param engineContext
   * @param partitionBaseFilePairs
   */
  protected abstract Pair<R, Long> readRecordKeysFromBaseFiles(HoodieEngineContext engineContext,
      List<Pair<String, String>> partitionBaseFilePairs);

  /**
   * Commit the previously queued records to Metadata Table as a new delta-commit.
   *
   * @param instantTime The instant time for the delta-commit
   * @param isInsert If true, perform an insert. If false, perform an update
   */
  protected abstract void commit(String instantTime, boolean isInsert);
}
