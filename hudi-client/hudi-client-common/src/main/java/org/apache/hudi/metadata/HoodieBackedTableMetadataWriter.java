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
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
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
import java.util.Arrays;
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
  private static final Integer MAX_BUCKET_COUNT = 9999;
  private static final  String BUCKET_PREFIX = "bucket-";

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

  protected HoodieBackedTableMetadataWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig,
      HoodieEngineContext engineContext) {
    this.datasetWriteConfig = writeConfig;
    this.engineContext = engineContext;
    this.hadoopConf = new SerializableConfiguration(hadoopConf);

    if (writeConfig.isMetadataTableEnabled()) {
      this.tableName = writeConfig.getTableName() + METADATA_TABLE_NAME_SUFFIX;
      this.metadataWriteConfig = createMetadataWriteConfig(writeConfig);
      enabled = true;

      // Inline compaction and auto clean is required as we dont expose this table outside
      ValidationUtils.checkArgument(!this.metadataWriteConfig.isAutoClean(), "Cleaning is controlled internally for Metadata table.");
      ValidationUtils.checkArgument(!this.metadataWriteConfig.inlineCompactionEnabled(), "Compaction is controlled internally for metadata table.");
      // Metadata Table cannot have metadata listing turned on. (infinite loop, much?)
      ValidationUtils.checkArgument(this.metadataWriteConfig.shouldAutoCommit(), "Auto commit is required for Metadata Table");
      ValidationUtils.checkArgument(!this.metadataWriteConfig.isMetadataTableEnabled(), "File listing cannot be used for Metadata Table");

      initRegistry();
      this.datasetMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(datasetWriteConfig.getBasePath()).build();
      initialize(engineContext);
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
   *
   * If the metadata table did not exist, then file and partition listing is used to bootstrap the table.
   */
  protected abstract void initialize(HoodieEngineContext engineContext);

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
    HoodieTimer timer = new HoodieTimer().startTimer();
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
          && datasetMetaClient.getActiveTimeline().getAllCommitsTimeline().isBeforeTimelineStarts(latestMetadataInstant.get().getTimestamp())) {
        // TODO: Revisit this logic and validate that filtering for all commits timeline is the right thing to do
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

    if (!exists) {
      // Initialize for the first time by listing partitions and files directly from the file system
      if (bootstrapFromFilesystem(engineContext, datasetMetaClient)) {
        metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.INITIALIZE_STR, timer.endTimer()));
      }
    }
  }

  /**
   * Initialize the Metadata Table by listing files and partitions from the file system.
   *
   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset
   */
  private boolean bootstrapFromFilesystem(HoodieEngineContext engineContext, HoodieTableMetaClient datasetMetaClient) throws IOException {
    ValidationUtils.checkState(enabled, "Metadata table cannot be initialized as it is not enabled");

    // We can only bootstrap if there are no pending operations on the dataset
    Option<HoodieInstant> pendingInstantOption = Option.fromJavaOptional(datasetMetaClient.getActiveTimeline()
        .getReverseOrderedInstants().filter(i -> !i.isCompleted()).findFirst());
    if (pendingInstantOption.isPresent()) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.BOOTSTRAP_ERR_STR, 1));
      LOG.warn("Cannot bootstrap metadata table as operation is in progress: " + pendingInstantOption.get());
      return false;
    }

    // If there is no commit on the dataset yet, use the SOLO_COMMIT_TIMESTAMP as the instant time for initial commit
    // Otherwise, we use the latest commit timestamp.
    String createInstantTime = datasetMetaClient.getActiveTimeline().getReverseOrderedInstants().findFirst()
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
    initializeBuckets(datasetMetaClient, MetadataPartitionType.FILES.partitionPath(), createInstantTime, 1);

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
    update(commitMetadata, createInstantTime);
    return true;
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
   * Initialize buckets for a partition. For file listing, we just have one bucket. But for record level index, we might have N number of buckets
   * per partition. Technically speaking buckets here map to FileGroups in Hudi.
   *
   * Each bucket maps to FileGroups in Hudi and is represented with the following format:
   *    bucket-ABCD
   * where ABCD are digits. This allows up to 9999 buckets.
   *
   * Example:
   *    bucket-0001
   *    bucket-0002
   */
  private void initializeBuckets(HoodieTableMetaClient datasetMetaClient, String partition, String instantTime,
                                 int bucketCount) throws IOException {
    ValidationUtils.checkArgument(bucketCount <= MAX_BUCKET_COUNT, "Maximum  " + MAX_BUCKET_COUNT  + " buckets are supported.");

    final HashMap<HeaderMetadataType, String> blockHeader = new HashMap<>();
    blockHeader.put(HeaderMetadataType.INSTANT_TIME, instantTime);
    // Archival of data table has a dependency on compaction(base files) in metadata table.
    // It is assumed that as of time Tx of base instant (/compaction time) in metadata table,
    // all commits in data table is in sync with metadata table. So, we always create start with log file for any bucket.
    // but we have to work on relaxing that in future : https://issues.apache.org/jira/browse/HUDI-2458
    final HoodieDeleteBlock block = new HoodieDeleteBlock(new HoodieKey[0], blockHeader);

    LOG.info(String.format("Creating %d buckets for partition %s with base fileId %s at instant time %s",
        bucketCount, partition, BUCKET_PREFIX, instantTime));
    for (int i = 0; i < bucketCount; ++i) {
      final String bucketFileId = String.format("%s%04d", BUCKET_PREFIX, i + 1);
      try {
        // since all shards are initialized in driver, we don't need to create a random write token.
        String writeToken = FSUtils.makeWriteToken(0, 0, 0);
        // TODO: what if driver crashed mid-way during initialization of buckets. during re-initializing, we might run into issues.
        // https://issues.apache.org/jira/browse/HUDI-2478
        HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder()
            .onParentPath(FSUtils.getPartitionPath(metadataWriteConfig.getBasePath(), partition))
            .withFileId(bucketFileId).overBaseCommit(instantTime)
            .withLogVersion(HoodieLogFile.LOGFILE_BASE_VERSION)
            .withFileSize(0L)
            .withSizeThreshold(metadataWriteConfig.getLogFileMaxSize())
            .withFs(datasetMetaClient.getFs())
            .withRolloverLogWriteToken(writeToken)
            .withLogWriteToken(writeToken)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
        writer.appendBlock(block);
        writer.close();
      } catch (InterruptedException e) {
        throw new HoodieException("Failed to created bucket " + bucketFileId + " for partition " + partition, e);
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
   */
  private <T> void processAndCommit(String instantTime, ConvertMetadataFunction convertMetadataFunction) {
    if (enabled) {
      List<HoodieRecord> records = convertMetadataFunction.convertMetadata();
      commit(records, MetadataPartitionType.FILES.partitionPath(), instantTime);
    }
  }

  /**
   * Update from {@code HoodieCommitMetadata}.
   *
   * @param commitMetadata {@code HoodieCommitMetadata}
   * @param instantTime Timestamp at which the commit was performed
   */
  @Override
  public void update(HoodieCommitMetadata commitMetadata, String instantTime) {
    processAndCommit(instantTime, () -> HoodieTableMetadataUtil.convertMetadataToRecords(commitMetadata, instantTime));
  }

  /**
   * Update from {@code HoodieCleanMetadata}.
   *
   * @param cleanMetadata {@code HoodieCleanMetadata}
   * @param instantTime Timestamp at which the clean was completed
   */
  @Override
  public void update(HoodieCleanMetadata cleanMetadata, String instantTime) {
    processAndCommit(instantTime, () -> HoodieTableMetadataUtil.convertMetadataToRecords(cleanMetadata, instantTime));
  }

  /**
   * Update from {@code HoodieRestoreMetadata}.
   *
   * @param restoreMetadata {@code HoodieRestoreMetadata}
   * @param instantTime Timestamp at which the restore was performed
   */
  @Override
  public void update(HoodieRestoreMetadata restoreMetadata, String instantTime) {
    processAndCommit(instantTime, () -> HoodieTableMetadataUtil.convertMetadataToRecords(metaClient.getActiveTimeline(),
        restoreMetadata, instantTime, metadata.getSyncedInstantTime()));
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
      // Is this rollback of an instant that has been synced to the metadata table?
      String rollbackInstant = rollbackMetadata.getCommitsRollback().get(0);
      boolean wasSynced = metaClient.getActiveTimeline().containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, rollbackInstant));
      if (!wasSynced) {
        // A compaction may have taken place on metadata table which would have included this instant being rolled back.
        // Revisit this logic to relax the compaction fencing : https://issues.apache.org/jira/browse/HUDI-2458
        Option<String> latestCompaction = metadata.getLatestCompactionTime();
        if (latestCompaction.isPresent()) {
          wasSynced = HoodieTimeline.compareTimestamps(rollbackInstant, HoodieTimeline.LESSER_THAN_OR_EQUALS, latestCompaction.get());
        }
      }

      List<HoodieRecord> records = HoodieTableMetadataUtil.convertMetadataToRecords(metaClient.getActiveTimeline(), rollbackMetadata, instantTime,
          metadata.getSyncedInstantTime(), wasSynced);
      commit(records, MetadataPartitionType.FILES.partitionPath(), instantTime);
    }
  }

  @Override
  public void close() throws Exception {
    if (metadata != null) {
      metadata.close();
    }
  }

  public HoodieBackedTableMetadata getMetadataReader() {
    return metadata;
  }

  /**
   * Commit the {@code HoodieRecord}s to Metadata Table as a new delta-commit.
   *
   * @param records The list of records to be written.
   * @param partitionName The partition to which the records are to be written.
   * @param instantTime The timestamp to use for the deltacommit.
   */
  protected abstract void commit(List<HoodieRecord> records, String partitionName, String instantTime);
}
