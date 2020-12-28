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
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieMetricsConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadata.METADATA_TABLE_NAME_SUFFIX;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;

/**
 * Writer implementation backed by an internal hudi table. Partition and file listing are saved within an internal MOR table
 * called Metadata Table. This table is created by listing files and partitions (first time)
 * and kept in sync using the instants on the main dataset.
 */
public class HoodieBackedTableMetadataWriter implements HoodieTableMetadataWriter {

  private static final Logger LOG = LogManager.getLogger(HoodieBackedTableMetadataWriter.class);

  private HoodieWriteConfig metadataWriteConfig;
  private HoodieWriteConfig datasetWriteConfig;
  private String tableName;

  private HoodieBackedTableMetadata metadata;
  private HoodieTableMetaClient metaClient;
  private Option<HoodieMetadataMetrics> metrics;
  private boolean enabled;
  private SerializableConfiguration hadoopConf;
  private final transient JavaSparkContext jsc;

  HoodieBackedTableMetadataWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig, JavaSparkContext jsc) {
    this.datasetWriteConfig = writeConfig;
    this.jsc = jsc;
    this.hadoopConf = new SerializableConfiguration(hadoopConf);

    if (writeConfig.useFileListingMetadata()) {
      this.tableName = writeConfig.getTableName() + METADATA_TABLE_NAME_SUFFIX;
      this.metadataWriteConfig = createMetadataWriteConfig(writeConfig);
      enabled = true;

      // Inline compaction and auto clean is required as we dont expose this table outside
      ValidationUtils.checkArgument(!this.metadataWriteConfig.isAutoClean(), "Cleaning is controlled internally for Metadata table.");
      ValidationUtils.checkArgument(!this.metadataWriteConfig.isInlineCompaction(), "Compaction is controlled internally for metadata table.");
      // Metadata Table cannot have metadata listing turned on. (infinite loop, much?)
      ValidationUtils.checkArgument(this.metadataWriteConfig.shouldAutoCommit(), "Auto commit is required for Metadata Table");
      ValidationUtils.checkArgument(!this.metadataWriteConfig.useFileListingMetadata(), "File listing cannot be used for Metadata Table");

      if (metadataWriteConfig.isMetricsOn()) {
        Registry registry;
        if (metadataWriteConfig.isExecutorMetricsEnabled()) {
          registry = Registry.getRegistry("HoodieMetadata", DistributedRegistry.class.getName());
        } else {
          registry = Registry.getRegistry("HoodieMetadata");
        }
        this.metrics = Option.of(new HoodieMetadataMetrics(registry));
      } else {
        this.metrics = Option.empty();
      }

      HoodieTableMetaClient datasetMetaClient = new HoodieTableMetaClient(hadoopConf, datasetWriteConfig.getBasePath());
      initialize(jsc, datasetMetaClient);
      if (enabled) {
        // (re) init the metadata for reading.
        initTableMetadata();

        // This is always called even in case the table was created for the first time. This is because
        // initFromFilesystem() does file listing and hence may take a long time during which some new updates
        // may have occurred on the table. Hence, calling this always ensures that the metadata is brought in sync
        // with the active timeline.
        HoodieTimer timer = new HoodieTimer().startTimer();
        syncFromInstants(datasetMetaClient);
        metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.SYNC_STR, timer.endTimer()));
      }
    } else {
      enabled = false;
      this.metrics = Option.empty();
    }
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
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withAutoCommit(true)
        .withAvroSchemaValidate(true)
        .withEmbeddedTimelineServerEnabled(false)
        .withAssumeDatePartitioning(false)
        .withPath(HoodieTableMetadata.getMetadataTableBasePath(writeConfig.getBasePath()))
        .withSchema(HoodieMetadataRecord.getClassSchema().toString())
        .forTable(tableName)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withAsyncClean(writeConfig.isMetadataAsyncClean())
            // we will trigger cleaning manually, to control the instant times
            .withAutoClean(false)
            .withCleanerParallelism(parallelism)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(writeConfig.getMetadataCleanerCommitsRetained())
            .archiveCommitsWith(writeConfig.getMetadataMinCommitsToKeep(), writeConfig.getMetadataMaxCommitsToKeep())
            // we will trigger compaction manually, to control the instant times
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(writeConfig.getMetadataCompactDeltaCommitMax()).build())
        .withParallelism(parallelism, parallelism)
        .withDeleteParallelism(parallelism)
        .withRollbackParallelism(parallelism)
        .withFileListingParallelism(writeConfig.getFileListingParallelism())
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
          // TODO:
          break;
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

  /**
   * Initialize the metadata table if it does not exist. Update the metadata to bring it in sync with the file system.
   *
   * This can happen in two ways:
   * 1. If the metadata table did not exist, then file and partition listing is used
   * 2. If the metadata table exists, the instants from active timeline are read in order and changes applied
   *
   * The above logic has been chosen because it is faster to perform #1 at scale rather than read all the Instants
   * which are large in size (AVRO or JSON encoded and not compressed) and incur considerable IO for de-serialization
   * and decoding.
   */
  private void initialize(JavaSparkContext jsc, HoodieTableMetaClient datasetMetaClient) {
    try {
      metrics.map(HoodieMetadataMetrics::registry).ifPresent(registry -> {
        if (registry instanceof DistributedRegistry) {
          ((DistributedRegistry) registry).register(jsc);
        }
      });

      if (enabled) {
        bootstrapIfNeeded(jsc, datasetMetaClient);
      }
    } catch (IOException e) {
      LOG.error("Failed to initialize metadata table. Disabling the writer.", e);
      enabled = false;
    }
  }

  private void initTableMetadata() {
    this.metadata = new HoodieBackedTableMetadata(hadoopConf.get(), datasetWriteConfig.getBasePath(), datasetWriteConfig.getSpillableMapBasePath(),
        datasetWriteConfig.useFileListingMetadata(), datasetWriteConfig.getFileListingMetadataVerify(), false,
        datasetWriteConfig.shouldAssumeDatePartitioning());
    this.metaClient = metadata.getMetaClient();
  }

  private void bootstrapIfNeeded(JavaSparkContext jsc, HoodieTableMetaClient datasetMetaClient) throws IOException {
    HoodieTimer timer = new HoodieTimer().startTimer();
    boolean exists = datasetMetaClient.getFs().exists(new Path(metadataWriteConfig.getBasePath(), HoodieTableMetaClient.METAFOLDER_NAME));
    if (!exists) {
      // Initialize for the first time by listing partitions and files directly from the file system
      bootstrapFromFilesystem(jsc, datasetMetaClient);
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.INITIALIZE_STR, timer.endTimer()));
    }
  }

  /**
   * Initialize the Metadata Table by listing files and partitions from the file system.
   *
   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset
   */
  private void bootstrapFromFilesystem(JavaSparkContext jsc, HoodieTableMetaClient datasetMetaClient) throws IOException {
    ValidationUtils.checkState(enabled, "Metadata table cannot be initialized as it is not enabled");

    // If there is no commit on the dataset yet, use the SOLO_COMMIT_TIMESTAMP as the instant time for initial commit
    // Otherwise, we use the timestamp of the instant which does not have any non-completed instants before it.
    Option<HoodieInstant> latestInstant = Option.empty();
    boolean foundNonComplete = false;
    for (HoodieInstant instant : datasetMetaClient.getActiveTimeline().getInstants().collect(Collectors.toList())) {
      if (!instant.isCompleted()) {
        foundNonComplete = true;
      } else if (!foundNonComplete) {
        latestInstant = Option.of(instant);
      }
    }

    String createInstantTime = latestInstant.map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);
    LOG.info("Creating a new metadata table in " + metadataWriteConfig.getBasePath() + " at instant " + createInstantTime);

    HoodieTableMetaClient.initTableType(hadoopConf.get(), metadataWriteConfig.getBasePath(),
        HoodieTableType.MERGE_ON_READ, tableName, "archived", HoodieMetadataPayload.class.getName(),
        HoodieFileFormat.HFILE.toString());
    initTableMetadata();

    // List all partitions in the basePath of the containing dataset
    LOG.info("Initializing metadata table by using file listings in " + datasetWriteConfig.getBasePath());
    Map<String, List<FileStatus>> partitionToFileStatus = getPartitionsToFilesMapping(jsc, datasetMetaClient);

    // Create a HoodieCommitMetadata with writeStats for all discovered files
    int[] stats = {0};
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();

    partitionToFileStatus.forEach((partition, statuses) -> {
      // Filter the statuses to only include files which were created before or on createInstantTime
      statuses.stream().filter(status -> {
        String filename = status.getPath().getName();
        if (HoodieTimeline.compareTimestamps(FSUtils.getCommitTime(filename), HoodieTimeline.GREATER_THAN,
            createInstantTime)) {
          return false;
        }
        return true;
      }).forEach(status -> {
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setPath(partition + Path.SEPARATOR + status.getPath().getName());
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
  }

  /**
   * Function to find hoodie partitions and list files in them in parallel.
   *
   * @param jsc
   * @param datasetMetaClient
   * @return Map of partition names to a list of FileStatus for all the files in the partition
   */
  private Map<String, List<FileStatus>> getPartitionsToFilesMapping(JavaSparkContext jsc, HoodieTableMetaClient datasetMetaClient) {

    List<Path> pathsToList = new LinkedList<>();
    pathsToList.add(new Path(datasetWriteConfig.getBasePath()));

    Map<String, List<FileStatus>> partitionToFileStatus = new HashMap<>();
    final int fileListingParallelism = metadataWriteConfig.getFileListingParallelism();
    SerializableConfiguration conf = new SerializableConfiguration(datasetMetaClient.getHadoopConf());

    while (!pathsToList.isEmpty()) {
      int listingParallelism = Math.min(fileListingParallelism, pathsToList.size());
      // List all directories in parallel
      List<Pair<Path, FileStatus[]>> dirToFileListing = jsc.parallelize(pathsToList, listingParallelism)
            .map(path -> {
              FileSystem fs = path.getFileSystem(conf.get());
              return Pair.of(path, fs.listStatus(path));
            }).collect();
      pathsToList.clear();

      // If the listing reveals a directory, add it to queue. If the listing reveals a hoodie partition, add it to
      // the results.
      dirToFileListing.forEach(p -> {
        List<FileStatus> filesInDir = Arrays.stream(p.getRight()).parallel()
            .filter(fs -> !fs.getPath().getName().equals(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE))
            .collect(Collectors.toList());

        if (p.getRight().length > filesInDir.size()) {
          // Is a partition. Add all data files to result.
          String partitionName = FSUtils.getRelativePartitionPath(new Path(datasetMetaClient.getBasePath()), p.getLeft());
          partitionToFileStatus.put(partitionName, filesInDir);
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
   * Sync the Metadata Table from the instants created on the dataset.
   *
   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset
   */
  private void syncFromInstants(HoodieTableMetaClient datasetMetaClient) {
    ValidationUtils.checkState(enabled, "Metadata table cannot be synced as it is not enabled");

    try {
      List<HoodieInstant> instantsToSync = metadata.findInstantsToSync(datasetMetaClient);
      if (instantsToSync.isEmpty()) {
        return;
      }

      LOG.info("Syncing " + instantsToSync.size() + " instants to metadata table: " + instantsToSync);

      // Read each instant in order and sync it to metadata table
      for (HoodieInstant instant : instantsToSync) {
        LOG.info("Syncing instant " + instant + " to metadata table");

        Option<List<HoodieRecord>> records = HoodieTableMetadataUtil.convertInstantToMetaRecords(datasetMetaClient, instant, metadata.getSyncedInstantTime());
        if (records.isPresent()) {
          commit(jsc, prepRecords(jsc, records.get(), MetadataPartitionType.FILES.partitionPath()), instant.getTimestamp());
        }
      }
      // re-init the table metadata, for any future writes.
      initTableMetadata();
    } catch (IOException ioe) {
      throw new HoodieIOException("Unable to sync instants from data to metadata table.", ioe);
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
    if (!enabled) {
      return;
    }

    List<HoodieRecord> records = HoodieTableMetadataUtil.convertMetadataToRecords(commitMetadata, instantTime);
    commit(jsc, prepRecords(jsc, records, MetadataPartitionType.FILES.partitionPath()), instantTime);
  }

  /**
   * Update from {@code HoodieCleanerPlan}.
   *
   * @param cleanerPlan {@code HoodieCleanerPlan}
   * @param instantTime Timestamp at which the clean plan was generated
   */
  @Override
  public void update(HoodieCleanerPlan cleanerPlan, String instantTime) {
    if (!enabled) {
      return;
    }

    List<HoodieRecord> records = HoodieTableMetadataUtil.convertMetadataToRecords(cleanerPlan, instantTime);
    commit(jsc, prepRecords(jsc, records, MetadataPartitionType.FILES.partitionPath()), instantTime);
  }

  /**
   * Update from {@code HoodieCleanMetadata}.
   *
   * @param cleanMetadata {@code HoodieCleanMetadata}
   * @param instantTime Timestamp at which the clean was completed
   */
  @Override
  public void update(HoodieCleanMetadata cleanMetadata, String instantTime) {
    if (!enabled) {
      return;
    }

    List<HoodieRecord> records = HoodieTableMetadataUtil.convertMetadataToRecords(cleanMetadata, instantTime);
    commit(jsc, prepRecords(jsc, records, MetadataPartitionType.FILES.partitionPath()), instantTime);
  }

  /**
   * Update from {@code HoodieRestoreMetadata}.
   *
   * @param restoreMetadata {@code HoodieRestoreMetadata}
   * @param instantTime Timestamp at which the restore was performed
   */
  @Override
  public void update(HoodieRestoreMetadata restoreMetadata, String instantTime) {
    if (!enabled) {
      return;
    }

    List<HoodieRecord> records = HoodieTableMetadataUtil.convertMetadataToRecords(restoreMetadata, instantTime, metadata.getSyncedInstantTime());
    commit(jsc, prepRecords(jsc, records, MetadataPartitionType.FILES.partitionPath()), instantTime);
  }

  /**
   * Update from {@code HoodieRollbackMetadata}.
   *
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param instantTime Timestamp at which the rollback was performed
   */
  @Override
  public void update(HoodieRollbackMetadata rollbackMetadata, String instantTime) {
    if (!enabled) {
      return;
    }

    List<HoodieRecord> records = HoodieTableMetadataUtil.convertMetadataToRecords(rollbackMetadata, instantTime, metadata.getSyncedInstantTime());
    commit(jsc, prepRecords(jsc, records, MetadataPartitionType.FILES.partitionPath()), instantTime);
  }

  /**
   * Commit the {@code HoodieRecord}s to Metadata Table as a new delta-commit.
   *
   * @param recordRDD The records to commit
   * @param instantTime The timestamp of instant to create
   */
  private synchronized void commit(JavaSparkContext jsc, JavaRDD<HoodieRecord> recordRDD, String instantTime) {
    ValidationUtils.checkState(enabled, "Metadata table cannot be committed to as it is not enabled");
    metadata.closeReaders();

    try (HoodieWriteClient writeClient = new HoodieWriteClient(jsc, metadataWriteConfig, true)) {
      writeClient.startCommitWithTime(instantTime);
      List<WriteStatus> statuses = writeClient.upsertPreppedRecords(recordRDD, instantTime).collect();
      statuses.forEach(writeStatus -> {
        if (writeStatus.hasErrors()) {
          throw new HoodieMetadataException("Failed to commit metadata table records at instant " + instantTime);
        }
      });
      // trigger cleaning, compaction, with suffixes based on the same instant time. This ensures that any future
      // delta commits synced over will not have an instant time lesser than the last completed instant on the
      // metadata table.
      writeClient.clean(instantTime + "001");
      if (writeClient.scheduleCompactionAtInstant(instantTime + "002", Option.empty())) {
        writeClient.compact(instantTime + "002");
      }
    }

    // Update total size of the metadata and count of base/log files
    metrics.ifPresent(m -> {
      try {
        Map<String, String> stats = m.getStats(false, metaClient, metadata);
        m.updateMetrics(Long.parseLong(stats.get(HoodieMetadataMetrics.STAT_TOTAL_BASE_FILE_SIZE)),
            Long.parseLong(stats.get(HoodieMetadataMetrics.STAT_TOTAL_LOG_FILE_SIZE)),
            Integer.parseInt(stats.get(HoodieMetadataMetrics.STAT_COUNT_BASE_FILES)),
            Integer.parseInt(stats.get(HoodieMetadataMetrics.STAT_COUNT_LOG_FILES)));
      } catch (HoodieIOException e) {
        LOG.error("Could not publish metadata size metrics", e);
      }
    });
  }

  /**
   * Tag each record with the location.
   *
   * Since we only read the latest base file in a partition, we tag the records with the instant time of the latest
   * base file.
   */
  private JavaRDD<HoodieRecord> prepRecords(JavaSparkContext jsc, List<HoodieRecord> records, String partitionName) {
    HoodieTable table = HoodieTable.create(metadataWriteConfig, hadoopConf.get());
    SliceView fsView = table.getSliceView();
    List<HoodieBaseFile> baseFiles = fsView.getLatestFileSlices(partitionName)
        .map(FileSlice::getBaseFile)
        .filter(Option::isPresent)
        .map(Option::get)
        .collect(Collectors.toList());

    // All the metadata fits within a single base file
    if (partitionName.equals(MetadataPartitionType.FILES.partitionPath())) {
      if (baseFiles.size() > 1) {
        throw new HoodieMetadataException("Multiple base files found in metadata partition");
      }
    }

    String fileId;
    String instantTime;
    if (!baseFiles.isEmpty()) {
      fileId = baseFiles.get(0).getFileId();
      instantTime = baseFiles.get(0).getCommitTime();
    } else {
      // If there is a log file then we can assume that it has the data
      List<HoodieLogFile> logFiles = fsView.getLatestFileSlices(MetadataPartitionType.FILES.partitionPath())
          .map(FileSlice::getLatestLogFile)
          .filter(Option::isPresent)
          .map(Option::get)
          .collect(Collectors.toList());
      if (logFiles.isEmpty()) {
        // No base and log files. All are new inserts
        return jsc.parallelize(records, 1);
      }

      fileId = logFiles.get(0).getFileId();
      instantTime = logFiles.get(0).getBaseCommitTime();
    }

    return jsc.parallelize(records, 1).map(r -> r.setCurrentLocation(new HoodieRecordLocation(instantTime, fileId)));
  }
}
