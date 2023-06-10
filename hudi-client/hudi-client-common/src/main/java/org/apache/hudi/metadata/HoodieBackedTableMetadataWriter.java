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
import org.apache.hudi.avro.model.HoodieIndexPartitionInfo;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.CachingPath;
import org.apache.hudi.hadoop.SerializablePath;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_POPULATE_META_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.getIndexInflightInstant;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeIndexPlan;
import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;
import static org.apache.hudi.metadata.HoodieTableMetadata.METADATA_TABLE_NAME_SUFFIX;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.METADATA_INDEXER_TIME_SUFFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getInflightAndCompletedMetadataPartitions;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getInflightMetadataPartitions;

/**
 * Writer implementation backed by an internal hudi table. Partition and file listing are saved within an internal MOR table
 * called Metadata Table. This table is created by listing files and partitions (first time)
 * and kept in sync using the instants on the main dataset.
 */
public abstract class HoodieBackedTableMetadataWriter implements HoodieTableMetadataWriter {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieBackedTableMetadataWriter.class);

  public static final String METADATA_COMPACTION_TIME_SUFFIX = "001";

  // Virtual keys support for metadata table. This Field is
  // from the metadata payload schema.
  private static final String RECORD_KEY_FIELD_NAME = HoodieMetadataPayload.KEY_FIELD_NAME;

  // Average size of a record saved within the record index.
  // Record index has a fixed size schema. This has been calculated based on experiments with default settings
  // for block size (4MB), compression (GZ) and disabling the hudi metadata fields.
  private static final int RECORD_INDEX_AVERAGE_RECORD_SIZE = 48;

  protected HoodieWriteConfig metadataWriteConfig;
  protected HoodieWriteConfig dataWriteConfig;

  protected HoodieBackedTableMetadata metadata;
  protected HoodieTableMetaClient metadataMetaClient;
  protected HoodieTableMetaClient dataMetaClient;
  protected Option<HoodieMetadataMetrics> metrics;
  protected SerializableConfiguration hadoopConf;
  protected final transient HoodieEngineContext engineContext;
  protected final List<MetadataPartitionType> enabledPartitionTypes;
  // Is the MDT bootstrapped and ready to be read from
  private boolean initialized = false;

  /**
   * Hudi backed table metadata writer.
   *
   * @param hadoopConf                 Hadoop configuration to use for the metadata writer
   * @param writeConfig                Writer config
   * @param failedWritesCleaningPolicy Cleaning policy on failed writes
   * @param engineContext              Engine context
   * @param actionMetadata             Optional action metadata to help decide initialize operations
   * @param <T>                        Action metadata types extending Avro generated SpecificRecordBase
   * @param inflightInstantTimestamp   Timestamp of any instant in progress
   */
  protected <T extends SpecificRecordBase> HoodieBackedTableMetadataWriter(Configuration hadoopConf,
                                                                           HoodieWriteConfig writeConfig,
                                                                           HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                                           HoodieEngineContext engineContext,
                                                                           Option<T> actionMetadata,
                                                                           Option<String> inflightInstantTimestamp) {
    this.dataWriteConfig = writeConfig;
    this.engineContext = engineContext;
    this.hadoopConf = new SerializableConfiguration(hadoopConf);
    this.metrics = Option.empty();
    this.enabledPartitionTypes = new ArrayList<>();

    this.dataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(dataWriteConfig.getBasePath()).build();

    if (dataMetaClient.getTableConfig().isMetadataTableEnabled() || writeConfig.isMetadataTableEnabled()) {
      this.metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig, failedWritesCleaningPolicy);

      try {
        enablePartitions();
        initRegistry();

        initialized = initializeIfNeeded(dataMetaClient, actionMetadata, inflightInstantTimestamp);

      } catch (IOException e) {
        LOG.error("Failed to initialize metadata table", e);
      }
    }
    ValidationUtils.checkArgument(!initialized || this.metadata != null, "MDT Reader should have been opened post initialization");
  }

  private void initMetadataReader() {
    if (this.metadata != null) {
      this.metadata.close();
    }

    try {
      this.metadata = new HoodieBackedTableMetadata(engineContext, dataWriteConfig.getMetadataConfig(), dataWriteConfig.getBasePath());
      this.metadataMetaClient = metadata.getMetadataMetaClient();
    } catch (Exception e) {
      throw new HoodieException("Could not open MDT for reads", e);
    }
  }

  /**
   * Enable metadata table partitions based on config.
   */
  private void enablePartitions() {
    final HoodieMetadataConfig metadataConfig = dataWriteConfig.getMetadataConfig();
    if (dataWriteConfig.isMetadataTableEnabled() || dataMetaClient.getTableConfig().isMetadataPartitionEnabled(MetadataPartitionType.FILES)) {
      this.enabledPartitionTypes.add(MetadataPartitionType.FILES);
    }
    if (metadataConfig.isBloomFilterIndexEnabled() || dataMetaClient.getTableConfig().isMetadataPartitionEnabled(MetadataPartitionType.BLOOM_FILTERS)) {
      this.enabledPartitionTypes.add(MetadataPartitionType.BLOOM_FILTERS);
    }
    if (metadataConfig.isColumnStatsIndexEnabled() || dataMetaClient.getTableConfig().isMetadataPartitionEnabled(MetadataPartitionType.COLUMN_STATS)) {
      this.enabledPartitionTypes.add(MetadataPartitionType.COLUMN_STATS);
    }
    if (dataWriteConfig.createMetadataRecordIndex() || dataMetaClient.getTableConfig().isMetadataPartitionEnabled(MetadataPartitionType.RECORD_INDEX)) {
      this.enabledPartitionTypes.add(MetadataPartitionType.RECORD_INDEX);
    }
  }

  protected abstract void initRegistry();

  public HoodieWriteConfig getWriteConfig() {
    return metadataWriteConfig;
  }

  public HoodieBackedTableMetadata getTableMetadata() {
    return metadata;
  }

  public List<MetadataPartitionType> getEnabledPartitionTypes() {
    return this.enabledPartitionTypes;
  }

  /**
   * Initialize the metadata table if needed.
   *
   * @param dataMetaClient           - meta client for the data table
   * @param actionMetadata           - optional action metadata
   * @param inflightInstantTimestamp - timestamp of an instant in progress on the dataset
   * @param <T>                      - action metadata types extending Avro generated SpecificRecordBase
   * @throws IOException on errors
   */
  protected <T extends SpecificRecordBase> boolean initializeIfNeeded(HoodieTableMetaClient dataMetaClient,
                                                                      Option<T> actionMetadata,
                                                                      Option<String> inflightInstantTimestamp) throws IOException {
    HoodieTimer timer = HoodieTimer.start();
    List<MetadataPartitionType> partitionsToInit = new ArrayList<>(MetadataPartitionType.values().length);

    try {
      boolean exists = metadataTableExists(dataMetaClient, actionMetadata);
      if (!exists) {
        // FILES partition is always required
        partitionsToInit.add(MetadataPartitionType.FILES);
      }

      // check if any of the enabled partition types needs to be initialized
      // NOTE: It needs to be guarded by async index config because if that is enabled then initialization happens through the index scheduler.
      if (!dataWriteConfig.isMetadataAsyncIndex()) {
        Set<String> inflightAndCompletedPartitions = getInflightAndCompletedMetadataPartitions(dataMetaClient.getTableConfig());
        LOG.info("Async metadata indexing disabled and following partitions already initialized: " + inflightAndCompletedPartitions);
        this.enabledPartitionTypes.stream()
            .filter(p -> !inflightAndCompletedPartitions.contains(p.getPartitionPath()) && !MetadataPartitionType.FILES.equals(p))
            .forEach(partitionsToInit::add);
      }

      if (partitionsToInit.isEmpty()) {
        // No partitions left to initialize, since all the metadata enabled partitions are either initialized before
        // or current in the process of initialization.
        initMetadataReader();
        return true;
      }

      // If there is no commit on the dataset yet, use the SOLO_COMMIT_TIMESTAMP as the instant time for initial commit
      // Otherwise, we use the timestamp of the latest completed action.
      String initializationTime = dataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant().map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);

      // Initialize partitions for the first time using data from the files on the file system
      if (!initializeFromFilesystem(initializationTime, partitionsToInit, inflightInstantTimestamp)) {
        LOG.error("Failed to initialize MDT from filesystem");
        return false;
      }

      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.INITIALIZE_STR, timer.endTimer()));
      return true;
    } catch (IOException e) {
      LOG.error("Failed to initialize metadata table. Disabling the writer.", e);
      return false;
    }
  }

  private <T extends SpecificRecordBase> boolean metadataTableExists(HoodieTableMetaClient dataMetaClient,
                                                                     Option<T> actionMetadata) throws IOException {
    boolean exists = dataMetaClient.getTableConfig().isMetadataTableEnabled();
    boolean reInitialize = false;

    // If the un-synced instants have been archived, then
    // the metadata table will need to be initialized again.
    if (exists) {
      metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf.get()).setBasePath(metadataWriteConfig.getBasePath()).build();
      if (DEFAULT_METADATA_POPULATE_META_FIELDS != metadataMetaClient.getTableConfig().populateMetaFields()) {
        LOG.info("Re-initiating metadata table properties since populate meta fields have changed");
        metadataMetaClient = initializeMetaClient();
      }
      final Option<HoodieInstant> latestMetadataInstant =
          metadataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();

      reInitialize = isBootstrapNeeded(latestMetadataInstant, actionMetadata);
    }

    if (reInitialize) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.REBOOTSTRAP_STR, 1));
      LOG.info("Deleting Metadata Table directory so that it can be re-initialized");
      HoodieTableMetadataUtil.deleteMetadataTable(dataMetaClient, engineContext, false);
      exists = false;
    }

    return  exists;
  }

  /**
   * Whether initialize operation needed for this metadata table.
   * <p>
   * Rollback of the first commit would look like un-synced instants in the metadata table.
   * Action metadata is needed to verify the instant time and avoid erroneous initializing.
   * <p>
   * TODO: Revisit this logic and validate that filtering for all
   *       commits timeline is the right thing to do
   *
   * @return True if the initialization is not needed, False otherwise
   */
  private <T extends SpecificRecordBase> boolean isBootstrapNeeded(Option<HoodieInstant> latestMetadataInstant,
                                                                   Option<T> actionMetadata) {
    if (!latestMetadataInstant.isPresent()) {
      LOG.warn("Metadata Table will need to be re-initialized as no instants were found");
      return true;
    }

    final String latestMetadataInstantTimestamp = latestMetadataInstant.get().getTimestamp();
    if (latestMetadataInstantTimestamp.startsWith(SOLO_COMMIT_TIMESTAMP)) { // the initialization timestamp is SOLO_COMMIT_TIMESTAMP + offset
      return false;
    }
    return false;
  }

  /**
   * Is the latest commit instant reverted by the in-flight instant action?
   *
   * @param actionMetadata                 - In-flight instant action metadata
   * @param latestMetadataInstantTimestamp - Metadata table latest instant timestamp
   * @param <T>                            - ActionMetadata type
   * @return True if the latest instant action is reverted by the action
   */
  private <T extends SpecificRecordBase> boolean isCommitRevertedByInFlightAction(Option<T> actionMetadata,
                                                                                  final String latestMetadataInstantTimestamp) {
    if (!actionMetadata.isPresent()) {
      return false;
    }

    final String INSTANT_ACTION = (actionMetadata.get() instanceof HoodieRollbackMetadata
        ? HoodieTimeline.ROLLBACK_ACTION
        : (actionMetadata.get() instanceof HoodieRestoreMetadata ? HoodieTimeline.RESTORE_ACTION : EMPTY_STRING));

    List<String> affectedInstantTimestamps;
    switch (INSTANT_ACTION) {
      case HoodieTimeline.ROLLBACK_ACTION:
        List<HoodieInstantInfo> rollbackInstants =
            ((HoodieRollbackMetadata) actionMetadata.get()).getInstantsRollback();
        affectedInstantTimestamps = rollbackInstants.stream().map(HoodieInstantInfo::getCommitTime).collect(Collectors.toList());

        if (affectedInstantTimestamps.contains(latestMetadataInstantTimestamp)) {
          return true;
        }
        break;
      case HoodieTimeline.RESTORE_ACTION:
        List<HoodieInstantInfo> restoredInstants =
            ((HoodieRestoreMetadata) actionMetadata.get()).getRestoreInstantInfo();
        affectedInstantTimestamps = restoredInstants.stream().map(HoodieInstantInfo::getCommitTime).collect(Collectors.toList());

        if (affectedInstantTimestamps.contains(latestMetadataInstantTimestamp)) {
          return true;
        }
        break;
      default:
        return false;
    }

    return false;
  }

  /**
   * Initialize the Metadata Table by listing files and partitions from the file system.
   *
   * @param initializationTime       - Timestamp to use for the commit
   * @param partitionsToInit         - List of MDT partitions to initialize
   * @param inflightInstantTimestamp - Current action instant responsible for this initialization
   */
  private boolean initializeFromFilesystem(String initializationTime, List<MetadataPartitionType> partitionsToInit,
                                           Option<String> inflightInstantTimestamp) throws IOException {
    if (anyPendingDataInstant(dataMetaClient, inflightInstantTimestamp)) {
      return false;
    }

    // FILES partition is always initialized first
    ValidationUtils.checkArgument(!partitionsToInit.contains(MetadataPartitionType.FILES)
        || partitionsToInit.get(0).equals(MetadataPartitionType.FILES), "FILES partition should be initialized first: " + partitionsToInit);

    metadataMetaClient = initializeMetaClient();

    // Get a complete list of files and partitions from the file system or from already initialized FILES partition of MDT
    boolean filesPartitionAvailable = dataMetaClient.getTableConfig().isMetadataPartitionEnabled(MetadataPartitionType.FILES);
    List<DirectoryInfo> partitionInfoList = filesPartitionAvailable ? listAllPartitionsFromMDT(initializationTime) : listAllPartitionsFromFilesystem(initializationTime);
    Map<String, Map<String, Long>> partitionToFilesMap = partitionInfoList.stream()
        .map(p -> {
          String partitionName = HoodieTableMetadataUtil.getPartitionIdentifier(p.getRelativePath());
          return Pair.of(partitionName, p.getFileNameToSizeMap());
        })
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    for (MetadataPartitionType partitionType : partitionsToInit) {
      // Find the commit timestamp to use for this partition. Each initialization should use its own unique commit time.
      String commitTimeForPartition = generateUniqueCommitInstantTime(initializationTime);

      LOG.info("Initializing MDT partition " + partitionType + " at instant " + commitTimeForPartition);

      Pair<Integer, HoodieData<HoodieRecord>> fileGroupCountAndRecordsPair;
      try {
        // Check and then open the metadata table reader to create a file system view
        if (this.metadata != null) {
          initMetadataReader();
        }
        switch (partitionType) {
          case FILES:
            fileGroupCountAndRecordsPair = initializeFilesPartition(partitionInfoList);
            break;
          case BLOOM_FILTERS:
            fileGroupCountAndRecordsPair = initializeBloomFiltersPartition(initializationTime, partitionToFilesMap);
            break;
          case COLUMN_STATS:
            fileGroupCountAndRecordsPair = initializeColumnStatsPartition(partitionToFilesMap);
            break;
          case RECORD_INDEX:
            fileGroupCountAndRecordsPair = initializeRecordIndexPartition();
            break;
          default:
            throw new HoodieMetadataException("Unsupported MDT partition type: " + partitionType);
        }
      } catch (Exception e) {
        String metricKey = partitionType.getPartitionPath() + "_" + HoodieMetadataMetrics.BOOTSTRAP_ERR_STR;
        metrics.ifPresent(m -> m.setMetric(metricKey, 1));
        LOG.error("Bootstrap on " + partitionType.getPartitionPath() + " partition failed for "
            + metadataMetaClient.getBasePath(), e);
        throw new HoodieMetadataException(partitionType.getPartitionPath()
            + " bootstrap failed for " + metadataMetaClient.getBasePath(), e);
      }

      // Generate the file groups
      final int fileGroupCount = fileGroupCountAndRecordsPair.getKey();
      ValidationUtils.checkArgument(fileGroupCount > 0, "FileGroup count for MDT partition " + partitionType + " should be > 0");
      initializeFileGroups(dataMetaClient, partitionType, commitTimeForPartition, fileGroupCount);

      // Perform the commit using bulkCommit
      HoodieData<HoodieRecord> records = fileGroupCountAndRecordsPair.getValue();
      bulkCommit(commitTimeForPartition, partitionType, records, fileGroupCount);
      metadataMetaClient.reloadActiveTimeline();
      dataMetaClient.getTableConfig().setMetadataPartitionState(dataMetaClient, partitionType, true);
      initMetadataReader();
    }

    return true;
  }

  /**
   * Returns a unique timestamp to use for initializing a MDT partition.
   * <p>
   * Since commits are immutable, we should use unique timestamps to initialize each partition. For this, we will add a suffix to the given initializationTime
   * until we find a unique timestamp.
   *
   * @param initializationTime Timestamp from dataset to use for initialization
   * @return a unique timestamp for MDT
   */
  private String generateUniqueCommitInstantTime(String initializationTime) {
    // Add suffix to initializationTime to find an unused instant time for the next index initialization.
    // This function would be called multiple times in a single application if multiple indexes are being
    // initialized one after the other.
    for (int offset = 0; ; ++offset) {
      final String commitInstantTime = HoodieTableMetadataUtil.createIndexInitTimestamp(initializationTime, offset);
      if (!metadataMetaClient.getCommitsTimeline().containsInstant(commitInstantTime)) {
        return commitInstantTime;
      }
    }
  }

  private Pair<Integer, HoodieData<HoodieRecord>> initializeColumnStatsPartition(Map<String, Map<String, Long>> partitionToFilesMap) {
    HoodieData<HoodieRecord> records = HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
        engineContext, Collections.emptyMap(), partitionToFilesMap, getRecordsGenerationParams());

    final int fileGroupCount = dataWriteConfig.getMetadataConfig().getColumnStatsIndexFileGroupCount();
    return Pair.of(fileGroupCount, records);
  }

  private Pair<Integer, HoodieData<HoodieRecord>> initializeBloomFiltersPartition(String createInstantTime, Map<String, Map<String, Long>> partitionToFilesMap) {
    HoodieData<HoodieRecord> records = HoodieTableMetadataUtil.convertFilesToBloomFilterRecords(
        engineContext, Collections.emptyMap(), partitionToFilesMap, getRecordsGenerationParams(), createInstantTime);

    final int fileGroupCount = dataWriteConfig.getMetadataConfig().getBloomFilterIndexFileGroupCount();
    return Pair.of(fileGroupCount, records);
  }

  private Pair<Integer, HoodieData<HoodieRecord>> initializeRecordIndexPartition() throws IOException {
    final HoodieMetadataFileSystemView fsView = new HoodieMetadataFileSystemView(dataMetaClient,
        dataMetaClient.getActiveTimeline(), metadata);

    // Collect the list of latest base files present in each partition
    List<String> partitions = metadata.getAllPartitionPaths();
    final List<Pair<String, String>> partitionBaseFilePairs = new ArrayList<>();
    for (String partition : partitions) {
      partitionBaseFilePairs.addAll(fsView.getLatestBaseFiles(partition)
          .map(basefile -> Pair.of(partition, basefile.getFileName())).collect(Collectors.toList()));
    }

    LOG.info("Initializing record index from " + partitionBaseFilePairs.size() + " base files in "
        + partitions.size() + " partitions");

    // Collect record keys from the files in parallel
    HoodieData<HoodieRecord> records = readRecordKeysFromBaseFiles(engineContext, partitionBaseFilePairs);
    records.persist("MEMORY_AND_DISK_SER");
    final long recordCount = records.count();

    // Initialize the file groups
    final int fileGroupCount = HoodieTableMetadataUtil.estimateFileGroupCount(MetadataPartitionType.RECORD_INDEX, recordCount,
        RECORD_INDEX_AVERAGE_RECORD_SIZE, dataWriteConfig.getRecordIndexMinFileGroupCount(),
        dataWriteConfig.getRecordIndexMaxFileGroupCount(), dataWriteConfig.getRecordIndexGrowthFactor(),
        dataWriteConfig.getRecordIndexMaxFileGroupSizeBytes());

    LOG.info(String.format("Initializing record index with %d mappings and %d file groups.", recordCount, fileGroupCount));
    return Pair.of(fileGroupCount, records);
  }

  /**
   * Read the record keys from base files in partitions and return records.
   */
  private HoodieData<HoodieRecord> readRecordKeysFromBaseFiles(HoodieEngineContext engineContext,
      List<Pair<String, String>> partitionBaseFilePairs) {
    if (partitionBaseFilePairs.isEmpty()) {
      return engineContext.emptyHoodieData();
    }

    engineContext.setJobStatus(this.getClass().getSimpleName(), "Record Index: reading record keys from base files");
    return engineContext.parallelize(partitionBaseFilePairs, partitionBaseFilePairs.size()).flatMap(p -> {
      final String partition = p.getKey();
      final String filename = p.getValue();
      Path dataFilePath = new Path(dataWriteConfig.getBasePath(), partition + Path.SEPARATOR + filename);

      final String fileId = FSUtils.getFileId(filename);
      final String instantTime = FSUtils.getCommitTime(filename);
      try (HoodieFileReader reader = HoodieFileReaderFactory.getReaderFactory(HoodieRecord.HoodieRecordType.AVRO).getFileReader(hadoopConf.get(), dataFilePath)) {
        Iterator<String> recordKeyIterator = reader.getRecordKeyIterator();

        return new Iterator<HoodieRecord>() {
          @Override
          public boolean hasNext() {
            return recordKeyIterator.hasNext();
          }

          @Override
          public HoodieRecord next() {
            return HoodieMetadataPayload.createRecordIndexUpdate(recordKeyIterator.next(), partition, fileId,
                instantTime);
          }
        };
      }
    });
  }

  private Pair<Integer, HoodieData<HoodieRecord>> initializeFilesPartition(List<DirectoryInfo> partitionInfoList) {
    // FILES partition uses a single file group
    final int fileGroupCount = 1;

    List<String> partitions = partitionInfoList.stream().map(p -> HoodieTableMetadataUtil.getPartitionIdentifier(p.getRelativePath()))
        .collect(Collectors.toList());
    final int totalDataFilesCount = partitionInfoList.stream().mapToInt(DirectoryInfo::getTotalFiles).sum();
    LOG.info("Committing total {} partitions and {} files to metadata", partitions.size(), totalDataFilesCount);

    // Record which saves the list of all partitions
    HoodieRecord record = HoodieMetadataPayload.createPartitionListRecord(partitions);
    HoodieData<HoodieRecord> allPartitionsRecord = engineContext.parallelize(Collections.singletonList(record), 1);
    if (partitionInfoList.isEmpty()) {
      return Pair.of(fileGroupCount, allPartitionsRecord);
    }

    // Records which save the file listing of each partition
    engineContext.setJobStatus(this.getClass().getSimpleName(), "Creating records for MDT FILES partition");
    HoodieData<HoodieRecord> fileListRecords = engineContext.parallelize(partitionInfoList, partitionInfoList.size()).map(partitionInfo -> {
      Map<String, Long> fileNameToSizeMap = partitionInfo.getFileNameToSizeMap();
      return HoodieMetadataPayload.createPartitionFilesRecord(
          HoodieTableMetadataUtil.getPartitionIdentifier(partitionInfo.getRelativePath()), Option.of(fileNameToSizeMap), Option.empty());
    });
    ValidationUtils.checkState(fileListRecords.count() == partitions.size());

    return Pair.of(fileGroupCount, allPartitionsRecord.union(fileListRecords));
  }

  private boolean anyPendingDataInstant(HoodieTableMetaClient dataMetaClient, Option<String> inflightInstantTimestamp) {
    // We can only initialize if there are no pending operations on the dataset
    List<HoodieInstant> pendingDataInstant = dataMetaClient.getActiveTimeline()
        .getInstantsAsStream().filter(i -> !i.isCompleted())
        .filter(i -> !inflightInstantTimestamp.isPresent() || !i.getTimestamp().equals(inflightInstantTimestamp.get()))
        // regular writers should not be blocked due to pending indexing action
        .filter(i -> !HoodieTimeline.INDEXING_ACTION.equals(i.getAction()))
        .collect(Collectors.toList());

    if (!pendingDataInstant.isEmpty()) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.BOOTSTRAP_ERR_STR, 1));
      LOG.warn("Cannot initialize metadata table as operation(s) are in progress on the dataset: "
          + Arrays.toString(pendingDataInstant.toArray()));
      return true;
    }
    return false;
  }

  private HoodieTableMetaClient initializeMetaClient() throws IOException {
    return HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setTableName(dataWriteConfig.getTableName() + METADATA_TABLE_NAME_SUFFIX)
        .setArchiveLogFolder(ARCHIVELOG_FOLDER.defaultValue())
        .setPayloadClassName(HoodieMetadataPayload.class.getName())
        .setBaseFileFormat(HoodieFileFormat.HFILE.toString())
        .setRecordKeyFields(RECORD_KEY_FIELD_NAME)
        .setPopulateMetaFields(DEFAULT_METADATA_POPULATE_META_FIELDS)
        .setKeyGeneratorClassProp(HoodieTableMetadataKeyGenerator.class.getCanonicalName())
        .initTable(hadoopConf.get(), metadataWriteConfig.getBasePath());
  }

  /**
   * Function to find hoodie partitions and list files in them in parallel.
   *
   * @param initializationTime Files which have a timestamp after this are neglected
   * @return List consisting of {@code DirectoryInfo} for each partition found.
   */
  private List<DirectoryInfo> listAllPartitionsFromFilesystem(String initializationTime) {
    List<SerializablePath> pathsToList = new LinkedList<>();
    pathsToList.add(new SerializablePath(new CachingPath(dataWriteConfig.getBasePath())));

    List<DirectoryInfo> partitionsToBootstrap = new LinkedList<>();
    final int fileListingParallelism = metadataWriteConfig.getFileListingParallelism();
    SerializableConfiguration conf = new SerializableConfiguration(dataMetaClient.getHadoopConf());
    final String dirFilterRegex = dataWriteConfig.getMetadataConfig().getDirectoryFilterRegex();
    final String datasetBasePath = dataMetaClient.getBasePath();
    SerializablePath serializableBasePath = new SerializablePath(new CachingPath(datasetBasePath));

    while (!pathsToList.isEmpty()) {
      // In each round we will list a section of directories
      int numDirsToList = Math.min(fileListingParallelism, pathsToList.size());
      // List all directories in parallel
      List<DirectoryInfo> processedDirectories = engineContext.map(pathsToList.subList(0, numDirsToList), path -> {
        FileSystem fs = path.get().getFileSystem(conf.get());
        String relativeDirPath = FSUtils.getRelativePartitionPath(serializableBasePath.get(), path.get());
        return new DirectoryInfo(relativeDirPath, fs.listStatus(path.get()), initializationTime);
      }, numDirsToList);

      pathsToList = new LinkedList<>(pathsToList.subList(numDirsToList, pathsToList.size()));

      // If the listing reveals a directory, add it to queue. If the listing reveals a hoodie partition, add it to
      // the results.
      for (DirectoryInfo dirInfo : processedDirectories) {
        if (!dirFilterRegex.isEmpty()) {
          final String relativePath = dirInfo.getRelativePath();
          if (!relativePath.isEmpty() && relativePath.matches(dirFilterRegex)) {
            LOG.info("Ignoring directory " + relativePath + " which matches the filter regex " + dirFilterRegex);
            continue;
          }
        }

        if (dirInfo.isHoodiePartition()) {
          // Add to result
          partitionsToBootstrap.add(dirInfo);
        } else {
          // Add sub-dirs to the queue
          pathsToList.addAll(dirInfo.getSubDirectories().stream()
              .map(path -> new SerializablePath(new CachingPath(path.toUri())))
              .collect(Collectors.toList()));
        }
      }
    }

    return partitionsToBootstrap;
  }

  /**
   * Function to find hoodie partitions and list files in them in parallel from MDT.
   *
   * @param initializationTime Files which have a timestamp after this are neglected
   * @return List consisting of {@code DirectoryInfo} for each partition found.
   */
  private List<DirectoryInfo> listAllPartitionsFromMDT(String initializationTime) throws IOException {
    List<DirectoryInfo> dirinfoList = new LinkedList<>();
    if (metadata == null) {
      this.metadata = new HoodieBackedTableMetadata(engineContext, dataWriteConfig.getMetadataConfig(),
          dataWriteConfig.getBasePath());
    }
    List<String> allPartitionPaths = metadata.getAllPartitionPaths().stream()
        .map(partitionPath -> dataWriteConfig.getBasePath() + "/" + partitionPath).collect(Collectors.toList());
    Map<String, FileStatus[]> partitionFileMap = metadata.getAllFilesInPartitions(allPartitionPaths);
    for (Map.Entry<String, FileStatus[]> entry : partitionFileMap.entrySet()) {
      dirinfoList.add(new DirectoryInfo(entry.getKey(), entry.getValue(), initializationTime));
    }
    return dirinfoList;
  }

  /**
   * Initialize file groups for a partition. For file listing, we just have one file group.
   * <p>
   * All FileGroups for a given metadata partition has a fixed prefix as per the {@link MetadataPartitionType#getFileIdPrefix()}.
   * Each file group is suffixed with 4 digits with increments of 1 starting with 0000.
   * <p>
   * Let's say we configure 10 file groups for record level index partition, and prefix as "record-index-bucket-"
   * File groups will be named as :
   * record-index-bucket-0000, .... -> ..., record-index-bucket-0009
   */
  private void initializeFileGroups(HoodieTableMetaClient dataMetaClient, MetadataPartitionType metadataPartition, String instantTime,
                                    int fileGroupCount) throws IOException {
    // Remove all existing file groups or leftover files in the partition
    final Path partitionPath = new Path(metadataWriteConfig.getBasePath(), metadataPartition.getPartitionPath());
    FileSystem fs = metadataMetaClient.getFs();
    try {
      final FileStatus[] existingFiles = fs.listStatus(partitionPath);
      if (existingFiles.length > 0) {
        LOG.warn("Deleting all existing files found in MDT partition " + metadataPartition.getPartitionPath());
        fs.delete(partitionPath, true);
        ValidationUtils.checkState(!fs.exists(partitionPath), "Failed to delete MDT partition " + metadataPartition);
      }
    } catch (FileNotFoundException e) {
      // If the partition did not exist yet, it will be created below
      LOG.warn("Exception seen while removing existing file groups in partition {} ", partitionPath.getName(), e);
    }

    // Archival of data table has a dependency on compaction(base files) in metadata table.
    // It is assumed that as of time Tx of base instant (/compaction time) in metadata table,
    // all commits in data table is in sync with metadata table. So, we always start with log file for any fileGroup.

    // Even though the initial commit is a bulkInsert which creates the first baseFiles directly, we still
    // create a log file first. This ensures that if any fileGroups of the MDT index do not receive any records
    // during initial commit, then the fileGroup would still be recognized (as a FileSlice with no baseFiles but a
    // valid logFile). Since these log files being created have no content, it is safe to add them here before
    // the bulkInsert.
    LOG.info(String.format("Creating %d file groups for partition %s with base fileId %s at instant time %s",
        fileGroupCount, metadataPartition.getPartitionPath(), metadataPartition.getFileIdPrefix(), instantTime));
    final List<String> fileGroupFileIds = IntStream.range(0, fileGroupCount)
        .mapToObj(i -> HoodieTableMetadataUtil.getFileIDForFileGroup(metadataPartition, i))
        .collect(Collectors.toList());
    ValidationUtils.checkArgument(fileGroupFileIds.size() == fileGroupCount);
    engineContext.foreach(fileGroupFileIds, fileGroupFileId -> {
      try {
        final HashMap<HeaderMetadataType, String> blockHeader = new HashMap<>();
        blockHeader.put(HeaderMetadataType.INSTANT_TIME, instantTime);
        final HoodieDeleteBlock block = new HoodieDeleteBlock(new DeleteRecord[0], blockHeader);

        HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder()
            .onParentPath(FSUtils.getPartitionPath(metadataWriteConfig.getBasePath(), metadataPartition.getPartitionPath()))
            .withFileId(fileGroupFileId)
            .overBaseCommit(instantTime)
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
        throw new HoodieException("Failed to created fileGroup " + fileGroupFileId + " for partition " + metadataPartition.getPartitionPath(), e);
      }
    }, fileGroupFileIds.size());
  }

  public void dropMetadataPartitions(List<MetadataPartitionType> metadataPartitions) throws IOException {
    for (MetadataPartitionType partitionType : metadataPartitions) {
      String partitionPath = partitionType.getPartitionPath();
      // first update table config
      dataMetaClient.getTableConfig().setMetadataPartitionState(dataMetaClient, partitionType, false);
      LOG.warn("Deleting Metadata Table partition: " + partitionPath);
      dataMetaClient.getFs().delete(new Path(metadataWriteConfig.getBasePath(), partitionPath), true);
      // delete corresponding pending indexing instant file in the timeline
      LOG.warn("Deleting pending indexing instant from the timeline for partition: " + partitionPath);
      deletePendingIndexingInstant(dataMetaClient, partitionPath);
    }
    closeInternal();
  }

  /**
   * Deletes any pending indexing instant, if it exists.
   * It reads the plan from indexing.requested file and deletes both requested and inflight instants,
   * if the partition path in the plan matches with the given partition path.
   */
  private static void deletePendingIndexingInstant(HoodieTableMetaClient metaClient, String partitionPath) {
    metaClient.reloadActiveTimeline().filterPendingIndexTimeline().getInstantsAsStream().filter(instant -> REQUESTED.equals(instant.getState()))
        .forEach(instant -> {
          try {
            HoodieIndexPlan indexPlan = deserializeIndexPlan(metaClient.getActiveTimeline().readIndexPlanAsBytes(instant).get());
            if (indexPlan.getIndexPartitionInfos().stream()
                .anyMatch(indexPartitionInfo -> indexPartitionInfo.getMetadataPartitionPath().equals(partitionPath))) {
              metaClient.getActiveTimeline().deleteInstantFileIfExists(instant);
              metaClient.getActiveTimeline().deleteInstantFileIfExists(getIndexInflightInstant(instant.getTimestamp()));
            }
          } catch (IOException e) {
            LOG.error("Failed to delete the instant file corresponding to " + instant);
          }
        });
  }

  protected static void checkNumDeltaCommits(HoodieTableMetaClient metaClient, int maxNumDeltaCommitsWhenPending) {
    final HoodieActiveTimeline activeTimeline = metaClient.reloadActiveTimeline();
    Option<HoodieInstant> lastCompaction = activeTimeline.filterCompletedInstants()
        .filter(s -> s.getAction().equals(COMPACTION_ACTION)).lastInstant();
    int numDeltaCommits = lastCompaction.isPresent()
        ? activeTimeline.getDeltaCommitTimeline().findInstantsAfter(lastCompaction.get().getTimestamp()).countInstants()
        : activeTimeline.getDeltaCommitTimeline().countInstants();
    if (numDeltaCommits > maxNumDeltaCommitsWhenPending) {
      throw new HoodieMetadataException(String.format("Metadata table's deltacommits exceeded %d: "
              + "this is likely caused by a pending instant in the data table. Resolve the pending instant "
              + "or adjust `%s`, then restart the pipeline.",
          maxNumDeltaCommitsWhenPending, HoodieMetadataConfig.METADATA_MAX_NUM_DELTACOMMITS_WHEN_PENDING.key()));
    }
  }

  private MetadataRecordsGenerationParams getRecordsGenerationParams() {
    return new MetadataRecordsGenerationParams(
        dataMetaClient,
        enabledPartitionTypes,
        dataWriteConfig.getBloomFilterType(),
        dataWriteConfig.getMetadataBloomFilterIndexParallelism(),
        dataWriteConfig.isMetadataColumnStatsIndexEnabled(),
        dataWriteConfig.getColumnStatsIndexParallelism(),
        dataWriteConfig.getColumnsEnabledForColumnStatsIndex(),
        dataWriteConfig.getColumnsEnabledForBloomFilterIndex());
  }

  /**
   * Interface to assist in converting commit metadata to List of HoodieRecords to be written to metadata table.
   * Updates of different commit metadata uses the same method to convert to HoodieRecords and hence.
   */
  private interface ConvertMetadataFunction {
    Map<MetadataPartitionType, HoodieData<HoodieRecord>> convertMetadata();
  }

  /**
   * Processes commit metadata from data table and commits to metadata table.
   *
   * @param instantTime             instant time of interest.
   * @param convertMetadataFunction converter function to convert the respective metadata to List of HoodieRecords to be written to metadata table.
   */
  private void processAndCommit(String instantTime, ConvertMetadataFunction convertMetadataFunction) {
    Set<String> partitionsToUpdate = getMetadataPartitionsToUpdate();
    Set<String> inflightIndexes = getInflightMetadataPartitions(dataMetaClient.getTableConfig());

    if (initialized && metadata != null) {
      // convert metadata and filter only the entries whose partition path are in partitionsToUpdate
      Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap = convertMetadataFunction.convertMetadata().entrySet().stream()
          .filter(entry -> partitionsToUpdate.contains(entry.getKey().getPartitionPath())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      commit(instantTime, partitionRecordsMap);
    }
  }

  private Set<String> getMetadataPartitionsToUpdate() {
    // fetch partitions to update from table config
    Set<String> partitionsToUpdate = dataMetaClient.getTableConfig().getMetadataPartitions();
    // add inflight indexes as well because the file groups have already been initialized, so writers can log updates
    // NOTE: Async HoodieIndexer can move some partition to inflight. While that partition is still being built,
    //       the regular ingestion writers should not be blocked. They can go ahead and log updates to the metadata partition.
    //       Instead of depending on enabledPartitionTypes, the table config becomes the source of truth for which partitions to update.
    partitionsToUpdate.addAll(getInflightMetadataPartitions(dataMetaClient.getTableConfig()));
    if (!partitionsToUpdate.isEmpty()) {
      return partitionsToUpdate;
    }
    // fallback to all enabled partitions if table config returned no partitions
    LOG.warn("There are no partitions to update according to table config. Falling back to enabled partition types in the write config.");
    return getEnabledPartitionTypes().stream().map(MetadataPartitionType::getPartitionPath).collect(Collectors.toSet());
  }

  public void buildMetadataPartitions(HoodieEngineContext engineContext, List<HoodieIndexPartitionInfo> indexPartitionInfos) throws IOException {
    if (indexPartitionInfos.isEmpty()) {
      LOG.warn("No partition to index in the plan");
      return;
    }
    String indexUptoInstantTime = indexPartitionInfos.get(0).getIndexUptoInstant();
    List<MetadataPartitionType> partitionTypes = new ArrayList<>();
    indexPartitionInfos.forEach(indexPartitionInfo -> {
      String relativePartitionPath = indexPartitionInfo.getMetadataPartitionPath();
      LOG.info(String.format("Creating a new metadata index for partition '%s' under path %s upto instant %s",
          relativePartitionPath, metadataWriteConfig.getBasePath(), indexUptoInstantTime));

      // return early and populate enabledPartitionTypes correctly (check in initialCommit)
      MetadataPartitionType partitionType = MetadataPartitionType.valueOf(relativePartitionPath.toUpperCase(Locale.ROOT));
      if (!enabledPartitionTypes.contains(partitionType)) {
        throw new HoodieIndexException(String.format("Indexing for metadata partition: %s is not enabled", partitionType));
      }
      partitionTypes.add(partitionType);
    });

    // before initialization set these  partitions as inflight in table config
    dataMetaClient.getTableConfig().setMetadataPartitionsInflight(dataMetaClient, partitionTypes);

    // initialize partitions
    initializeFromFilesystem(indexUptoInstantTime + METADATA_INDEXER_TIME_SUFFIX, partitionTypes, Option.empty());
  }

  /**
   * Update from {@code HoodieCommitMetadata}.
   *
   * @param commitMetadata {@code HoodieCommitMetadata}
   * @param instantTime    Timestamp at which the commit was performed
   */
  @Override
  public void update(HoodieCommitMetadata commitMetadata, HoodieData<WriteStatus> writeStatus, String instantTime) {
    processAndCommit(instantTime, () -> {
      Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionToRecordMap =
          HoodieTableMetadataUtil.convertMetadataToRecords(engineContext, commitMetadata, instantTime, getRecordsGenerationParams());

      // Updates for record index are created by parsing the WriteStatus which is a hudi-client object. Hence, we cannot yet move this code
      // to the HoodieTableMetadataUtil class in hudi-common.
      if (writeStatus != null && !writeStatus.isEmpty()) {
        partitionToRecordMap.put(MetadataPartitionType.RECORD_INDEX, getRecordIndexUpdates(writeStatus));
      }
      return partitionToRecordMap;
    });
    closeInternal();
  }

  /**
   * Update from {@code HoodieCleanMetadata}.
   *
   * @param cleanMetadata {@code HoodieCleanMetadata}
   * @param instantTime Timestamp at which the clean was completed
   */
  @Override
  public void update(HoodieCleanMetadata cleanMetadata, String instantTime) {
    processAndCommit(instantTime, () -> HoodieTableMetadataUtil.convertMetadataToRecords(engineContext,
        cleanMetadata, getRecordsGenerationParams(), instantTime));
    closeInternal();
  }

  /**
   * Update from {@code HoodieRestoreMetadata}.
   *
   * @param restoreMetadata {@code HoodieRestoreMetadata}
   * @param instantTime Timestamp at which the restore was performed
   */
  @Override
  public void update(HoodieRestoreMetadata restoreMetadata, String instantTime) {
    processAndCommit(instantTime, () -> HoodieTableMetadataUtil.convertMetadataToRecords(engineContext,
        metadataMetaClient.getActiveTimeline(), restoreMetadata, getRecordsGenerationParams(), instantTime,
        metadata.getSyncedInstantTime()));
    closeInternal();
  }

  /**
   * Update from {@code HoodieRollbackMetadata}.
   *
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param instantTime Timestamp at which the rollback was performed
   */
  @Override
  public void update(HoodieRollbackMetadata rollbackMetadata, String instantTime) {
    if (initialized && metadata != null) {
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

      Map<MetadataPartitionType, HoodieData<HoodieRecord>> records =
          HoodieTableMetadataUtil.convertMetadataToRecords(engineContext, metadataMetaClient.getActiveTimeline(),
              rollbackMetadata, getRecordsGenerationParams(), instantTime,
              metadata.getSyncedInstantTime(), wasSynced);
      commit(instantTime, records);
      closeInternal();
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
   *
   * @param instantTime         - Action instant time for this commit
   * @param partitionRecordsMap - Map of partition type to its records to commit
   */
  protected abstract void commit(String instantTime, Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap);

  /**
   * Commit the {@code HoodieRecord}s to Metadata Table as a new delta-commit using bulk commit (if supported).
   * <p>
   * This is used to optimize the initial commit to the MDT partition which may contains a large number of
   * records and hence is more suited to bulkInsert for write performance.
   *
   * @param instantTime    - Action instant time for this commit
   * @param partitionType  - The MDT partition to which records are to be committed
   * @param records        - records to be bulk inserted
   * @param fileGroupCount - The maximum number of file groups to which the records will be written.
   */
  protected void bulkCommit(
      String instantTime, MetadataPartitionType partitionType, HoodieData<HoodieRecord> records,
      int fileGroupCount) {
    Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap =
        Collections.singletonMap(partitionType, records);
    commit(instantTime, partitionRecordsMap);
  }

  /**
   * Tag each record with the location in the given partition.
   * The record is tagged with respective file slice's location based on its record key.
   */
  protected HoodieData<HoodieRecord> prepRecords(Map<MetadataPartitionType,
      HoodieData<HoodieRecord>> partitionRecordsMap) {
    // The result set
    HoodieData<HoodieRecord> allPartitionRecords = engineContext.emptyHoodieData();

    HoodieTableFileSystemView fsView = HoodieTableMetadataUtil.getFileSystemView(metadataMetaClient);
    for (Map.Entry<MetadataPartitionType, HoodieData<HoodieRecord>> entry : partitionRecordsMap.entrySet()) {
      final String partitionName = entry.getKey().getPartitionPath();
      HoodieData<HoodieRecord> records = entry.getValue();

      List<FileSlice> fileSlices =
          HoodieTableMetadataUtil.getPartitionLatestFileSlices(metadataMetaClient, Option.ofNullable(fsView), partitionName);
      if (fileSlices.isEmpty()) {
        // scheduling of INDEX only initializes the file group and not add commit
        // so if there are no committed file slices, look for inflight slices
        fileSlices = HoodieTableMetadataUtil.getPartitionLatestFileSlicesIncludingInflight(metadataMetaClient, Option.ofNullable(fsView), partitionName);
      }
      final int fileGroupCount = fileSlices.size();
      ValidationUtils.checkArgument(fileGroupCount > 0, "FileGroup count for MDT partition " + partitionName + " should be >0");

      List<FileSlice> finalFileSlices = fileSlices;
      HoodieData<HoodieRecord> rddSinglePartitionRecords = records.map(r -> {
        FileSlice slice = finalFileSlices.get(HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(r.getRecordKey(),
            fileGroupCount));
        r.unseal();
        r.setCurrentLocation(new HoodieRecordLocation(slice.getBaseInstantTime(), slice.getFileId()));
        r.seal();
        return r;
      });

      allPartitionRecords = allPartitionRecords.union(rddSinglePartitionRecords);
    }
    return allPartitionRecords;
  }

  /**
   * Optimize the metadata table by running compaction, clean and archive as required.
   * <p>
   * Don't perform optimization if there are inflight operations on the dataset. This is for two reasons:
   * - The compaction will contain the correct data as all failed operations have been rolled back.
   * - Clean/compaction etc. will have the highest timestamp on the MDT and we won't be adding new operations
   * with smaller timestamps to metadata table (makes for easier debugging)
   * <p>
   * This adds the limitations that long-running async operations (clustering, etc.) may cause delay in such MDT
   * optimizations. We will relax this after MDT code has been hardened.
   */
  @Override
  public void performTableServices(Option<String> inFlightInstantTimestamp) {
    HoodieTimer metadataTableServicesTimer = HoodieTimer.start();
    boolean allTableServicesExecutedSuccessfullyOrSkipped = true;
    try {
      BaseHoodieWriteClient writeClient = getWriteClient();
      // Run any pending table services operations.
      runPendingTableServicesOperations(writeClient);

      // Check and run clean operations.
      String latestDeltacommitTime = metadataMetaClient.reloadActiveTimeline().getDeltaCommitTimeline()
          .filterCompletedInstants()
          .lastInstant().get()
          .getTimestamp();
      LOG.info("Latest deltacommit time found is " + latestDeltacommitTime + ", running clean operations.");
      cleanIfNecessary(writeClient, latestDeltacommitTime);

      // Do timeline validation before scheduling compaction/logcompaction operations.
      if (validateTimelineBeforeSchedulingCompaction(inFlightInstantTimestamp, latestDeltacommitTime)) {
        compactIfNecessary(writeClient, latestDeltacommitTime);
      }
      writeClient.archive();
      LOG.info("All the table services operations on MDT completed successfully");
    } catch (Exception e) {
      LOG.error("Exception in running table services on metadata table", e);
      allTableServicesExecutedSuccessfullyOrSkipped = false;
      throw e;
    } finally {
      long timeSpent = metadataTableServicesTimer.endTimer();
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.TABLE_SERVICE_EXECUTION_DURATION, timeSpent));
      if (allTableServicesExecutedSuccessfullyOrSkipped) {
        metrics.ifPresent(m -> m.incrementMetric(HoodieMetadataMetrics.TABLE_SERVICE_EXECUTION_STATUS, 1));
      } else {
        metrics.ifPresent(m -> m.incrementMetric(HoodieMetadataMetrics.TABLE_SERVICE_EXECUTION_STATUS, -1));
      }
    }
  }

  private void runPendingTableServicesOperations(BaseHoodieWriteClient writeClient) {
    // finish off any pending log compaction or compactions operations if any from previous attempt.
    writeClient.runAnyPendingCompactions();
    writeClient.runAnyPendingLogCompactions();
  }

  /**
   * Perform a compaction on the Metadata Table.
   * <p>
   * Cases to be handled:
   * 1. We cannot perform compaction if there are previous inflight operations on the dataset. This is because
   * a compacted metadata base file at time Tx should represent all the actions on the dataset till time Tx.
   * <p>
   * 2. In multi-writer scenario, a parallel operation with a greater instantTime may have completed creating a
   * deltacommit.
   */
  protected void compactIfNecessary(BaseHoodieWriteClient writeClient, String latestDeltacommitTime) {
    // Trigger compaction with suffixes based on the same instant time. This ensures that any future
    // delta commits synced over will not have an instant time lesser than the last completed instant on the
    // metadata table.
    final String compactionInstantTime = HoodieTableMetadataUtil.createCompactionTimestamp(latestDeltacommitTime);

    // we need to avoid checking compaction w/ same instant again.
    // let's say we trigger compaction after C5 in MDT and so compaction completes with C4001. but C5 crashed before completing in MDT.
    // and again w/ C6, we will re-attempt compaction at which point latest delta commit is C4 in MDT.
    // and so we try compaction w/ instant C4001. So, we can avoid compaction if we already have compaction w/ same instant time.
    if (metadataMetaClient.getActiveTimeline().filterCompletedInstants().containsInstant(compactionInstantTime)) {
      LOG.info(String.format("Compaction with same %s time is already present in the timeline.", compactionInstantTime));
    } else if (writeClient.scheduleCompactionAtInstant(compactionInstantTime, Option.empty())) {
      LOG.info("Compaction is scheduled for timestamp " + compactionInstantTime);
      writeClient.compact(compactionInstantTime);
    } else if (metadataWriteConfig.isLogCompactionEnabled()) {
      // Schedule and execute log compaction with suffixes based on the same instant time. This ensures that any future
      // delta commits synced over will not have an instant time lesser than the last completed instant on the
      // metadata table.
      final String logCompactionInstantTime = HoodieTableMetadataUtil.createLogCompactionTimestamp(latestDeltacommitTime);
      if (metadataMetaClient.getActiveTimeline().filterCompletedInstants().containsInstant(logCompactionInstantTime)) {
        LOG.info(String.format("Log compaction with same %s time is already present in the timeline.", logCompactionInstantTime));
      } else if (writeClient.scheduleLogCompactionAtInstant(logCompactionInstantTime, Option.empty())) {
        LOG.info("Log compaction is scheduled for timestamp " + logCompactionInstantTime);
        writeClient.logCompact(logCompactionInstantTime);
      }
    }
  }

  protected void cleanIfNecessary(BaseHoodieWriteClient writeClient, String instantTime) {
    Option<HoodieInstant> lastCompletedCompactionInstant = metadataMetaClient.reloadActiveTimeline()
        .getCommitTimeline().filterCompletedInstants().lastInstant();
    if (lastCompletedCompactionInstant.isPresent()
        && metadataMetaClient.getActiveTimeline().filterCompletedInstants()
            .findInstantsAfter(lastCompletedCompactionInstant.get().getTimestamp()).countInstants() < 3) {
      // do not clean the log files immediately after compaction to give some buffer time for metadata table reader,
      // because there is case that the reader has prepared for the log file readers already before the compaction completes
      // while before/during the reading of the log files, the cleaning triggers and delete the reading files,
      // then a FileNotFoundException(for LogFormatReader) or NPE(for HFileReader) would throw.

      // 3 is a value that I think is enough for metadata table reader.
      return;
    }
    // Trigger cleaning with suffixes based on the same instant time. This ensures that any future
    // delta commits synced over will not have an instant time lesser than the last completed instant on the
    // metadata table.
    writeClient.clean(HoodieTableMetadataUtil.createCleanTimestamp(instantTime));
    writeClient.lazyRollbackFailedIndexing();
  }

  /**
   * Validates the timeline for both main and metadata tables to ensure compaction on MDT can be scheduled.
   */
  protected boolean validateTimelineBeforeSchedulingCompaction(Option<String> inFlightInstantTimestamp, String latestDeltaCommitTimeInMetadataTable) {
    // we need to find if there are any inflights in data table timeline before or equal to the latest delta commit in metadata table.
    // Whenever you want to change this logic, please ensure all below scenarios are considered.
    // a. There could be a chance that latest delta commit in MDT is committed in MDT, but failed in DT. And so findInstantsBeforeOrEquals() should be employed
    // b. There could be DT inflights after latest delta commit in MDT and we are ok with it. bcoz, the contract is, the latest compaction instant time in MDT represents
    // any instants before that is already synced with metadata table.
    // c. Do consider out of order commits. For eg, c4 from DT could complete before c3. and we can't trigger compaction in MDT with c4 as base instant time, until every
    // instant before c4 is synced with metadata table.
    List<HoodieInstant> pendingInstants = dataMetaClient.reloadActiveTimeline().filterInflightsAndRequested()
        .findInstantsBeforeOrEquals(latestDeltaCommitTimeInMetadataTable).getInstants();

    if (!pendingInstants.isEmpty()) {
      checkNumDeltaCommits(metadataMetaClient, dataWriteConfig.getMetadataConfig().getMaxNumDeltacommitsWhenPending());
      LOG.info(String.format(
          "Cannot compact metadata table as there are %d inflight instants in data table before latest deltacommit in metadata table: %s. Inflight instants in data table: %s",
          pendingInstants.size(), latestDeltaCommitTimeInMetadataTable, Arrays.toString(pendingInstants.toArray())));
      return false;
    }

    // Check if there are any pending compaction or log compaction instants in the timeline.
    // If pending compact/logcompaction operations are found abort scheduling new compaction/logcompaction operations.
    Option<HoodieInstant> pendingLogCompactionInstant =
        metadataMetaClient.getActiveTimeline().filterPendingLogCompactionTimeline().firstInstant();
    Option<HoodieInstant> pendingCompactionInstant =
        metadataMetaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant();
    if (pendingLogCompactionInstant.isPresent() || pendingCompactionInstant.isPresent()) {
      LOG.warn(String.format("Not scheduling compaction or logcompaction, since a pending compaction instant %s or logcompaction %s instant is present",
          pendingCompactionInstant, pendingLogCompactionInstant));
      return false;
    }
    return true;
  }

  /**
   * Return records that represent update to the record index due to write operation on the dataset.
   *
   * @param writeStatuses (@code WriteStatus} from the write operation
   */
  private HoodieData<HoodieRecord> getRecordIndexUpdates(HoodieData<WriteStatus> writeStatuses) {
    return writeStatuses.flatMap(writeStatus -> {
      List<HoodieRecord> recordList = new LinkedList<>();
      for (HoodieRecord writtenRecord : writeStatus.getWrittenRecords()) {
        if (!writeStatus.isErrored(writtenRecord.getKey())) {
          HoodieRecord hoodieRecord;
          HoodieKey key = writtenRecord.getKey();
          Option<HoodieRecordLocation> newLocation = writtenRecord.getNewLocation();
          if (newLocation.isPresent()) {
            if (writtenRecord.getCurrentLocation() != null) {
              // This is an update, no need to update index if the location has not changed
              // newLocation should have the same fileID as currentLocation. The instantTimes differ as newLocation's
              // instantTime refers to the current commit which was completed.
              if (!writtenRecord.getCurrentLocation().getFileId().equals(newLocation.get().getFileId())) {
                final String msg = String.format("Detected update in location of record with key %s from %s "
                        + " to %s. The fileID should not change.",
                    writtenRecord.getKey(), writtenRecord.getCurrentLocation(), newLocation.get());
                LOG.error(msg);
                throw new HoodieMetadataException(msg);
              } else {
                // TODO: This may be required for clustering usecases where record location changes
                continue;
              }
            }

            hoodieRecord = HoodieMetadataPayload.createRecordIndexUpdate(key.getRecordKey(), key.getPartitionPath(),
                newLocation.get().getFileId(), newLocation.get().getInstantTime());
          } else {
            // Delete existing index for a deleted record
            hoodieRecord = HoodieMetadataPayload.createRecordIndexDelete(key.getRecordKey());
          }

          recordList.add(hoodieRecord);
        }
      }

      return recordList.iterator();
    });
  }

  protected void closeInternal() {
    try {
      close();
    } catch (Exception e) {
      throw new HoodieException("Failed to close HoodieMetadata writer ", e);
    }
  }

  public boolean isInitialized() {
    return initialized;
  }

  /**
   * A class which represents a directory and the files and directories inside it.
   * <p>
   * A {@code PartitionFileInfo} object saves the name of the partition and various properties requires of each file
   * required for initializing the metadata table. Saving limited properties reduces the total memory footprint when
   * a very large number of files are present in the dataset being initialized.
   */
  static class DirectoryInfo implements Serializable {
    // Relative path of the directory (relative to the base directory)
    private final String relativePath;
    // Map of filenames within this partition to their respective sizes
    private final HashMap<String, Long> filenameToSizeMap;
    // List of directories within this partition
    private final List<Path> subDirectories = new ArrayList<>();
    // Is this a hoodie partition
    private boolean isHoodiePartition = false;

    public DirectoryInfo(String relativePath, FileStatus[] fileStatus, String maxInstantTime) {
      this.relativePath = relativePath;

      // Pre-allocate with the maximum length possible
      filenameToSizeMap = new HashMap<>(fileStatus.length);

      for (FileStatus status : fileStatus) {
        if (status.isDirectory()) {
          // Ignore .hoodie directory as there cannot be any partitions inside it
          if (!status.getPath().getName().equals(HoodieTableMetaClient.METAFOLDER_NAME)) {
            this.subDirectories.add(status.getPath());
          }
        } else if (status.getPath().getName().startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX)) {
          // Presence of partition meta file implies this is a HUDI partition
          this.isHoodiePartition = true;
        } else if (FSUtils.isDataFile(status.getPath())) {
          // Regular HUDI data file (base file or log file)
          String dataFileCommitTime = FSUtils.getCommitTime(status.getPath().getName());
          // Limit the file listings to files which were created before the maxInstant time.
          if (HoodieTimeline.compareTimestamps(dataFileCommitTime, HoodieTimeline.LESSER_THAN_OR_EQUALS, maxInstantTime)) {
            filenameToSizeMap.put(status.getPath().getName(), status.getLen());
          }
        }
      }
    }

    String getRelativePath() {
      return relativePath;
    }

    int getTotalFiles() {
      return filenameToSizeMap.size();
    }

    boolean isHoodiePartition() {
      return isHoodiePartition;
    }

    List<Path> getSubDirectories() {
      return subDirectories;
    }

    // Returns a map of filenames mapped to their lengths
    Map<String, Long> getFileNameToSizeMap() {
      return filenameToSizeMap;
    }
  }
}
