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
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.CachingPath;
import org.apache.hudi.hadoop.SerializablePath;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.BulkInsertPartitioner;

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
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_POPULATE_META_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.getIndexInflightInstant;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeIndexPlan;
import static org.apache.hudi.metadata.HoodieTableMetadata.METADATA_TABLE_NAME_SUFFIX;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.createRollbackTimestamp;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getInflightMetadataPartitions;

/**
 * Writer implementation backed by an internal hudi table. Partition and file listing are saved within an internal MOR table
 * called Metadata Table. This table is created by listing files and partitions (first time)
 * and kept in sync using the instants on the main dataset.
 *
 * @param <I> Type of input for the write client
 */
public abstract class HoodieBackedTableMetadataWriter<I> implements HoodieTableMetadataWriter {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieBackedTableMetadataWriter.class);

  public static final String METADATA_COMPACTION_TIME_SUFFIX = "001";

  // Virtual keys support for metadata table. This Field is
  // from the metadata payload schema.
  private static final String RECORD_KEY_FIELD_NAME = HoodieMetadataPayload.KEY_FIELD_NAME;

  // Average size of a record saved within the record index.
  // Record index has a fixed size schema. This has been calculated based on experiments with default settings
  // for block size (1MB), compression (GZ) and disabling the hudi metadata fields.
  private static final int RECORD_INDEX_AVERAGE_RECORD_SIZE = 48;
  private transient BaseHoodieWriteClient<?, I, ?, ?> writeClient;

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
   * @param inflightInstantTimestamp   Timestamp of any instant in progress
   */
  protected HoodieBackedTableMetadataWriter(Configuration hadoopConf,
                                            HoodieWriteConfig writeConfig,
                                            HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                            HoodieEngineContext engineContext,
                                            Option<String> inflightInstantTimestamp) {
    this.dataWriteConfig = writeConfig;
    this.engineContext = engineContext;
    this.hadoopConf = new SerializableConfiguration(hadoopConf);
    this.metrics = Option.empty();
    this.enabledPartitionTypes = new ArrayList<>(4);

    this.dataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(dataWriteConfig.getBasePath()).build();

    if (writeConfig.isMetadataTableEnabled()) {
      this.metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig, failedWritesCleaningPolicy);

      try {
        enablePartitions();
        initRegistry();

        initialized = initializeIfNeeded(dataMetaClient, inflightInstantTimestamp);

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
      this.metadata = new HoodieBackedTableMetadata(engineContext, dataWriteConfig.getMetadataConfig(), dataWriteConfig.getBasePath(), true);
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
    if (dataWriteConfig.isMetadataTableEnabled() || dataMetaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.FILES)) {
      this.enabledPartitionTypes.add(MetadataPartitionType.FILES);
    }
    if (metadataConfig.isBloomFilterIndexEnabled() || dataMetaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.BLOOM_FILTERS)) {
      this.enabledPartitionTypes.add(MetadataPartitionType.BLOOM_FILTERS);
    }
    if (metadataConfig.isColumnStatsIndexEnabled() || dataMetaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.COLUMN_STATS)) {
      this.enabledPartitionTypes.add(MetadataPartitionType.COLUMN_STATS);
    }
    if (dataWriteConfig.isRecordIndexEnabled() || dataMetaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)) {
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
   * @param inflightInstantTimestamp - timestamp of an instant in progress on the dataset
   * @throws IOException on errors
   */
  protected boolean initializeIfNeeded(HoodieTableMetaClient dataMetaClient,
                                       Option<String> inflightInstantTimestamp) throws IOException {
    HoodieTimer timer = HoodieTimer.start();
    List<MetadataPartitionType> partitionsToInit = new ArrayList<>(MetadataPartitionType.values().length);

    try {
      boolean exists = metadataTableExists(dataMetaClient);
      if (!exists) {
        // FILES partition is always required
        partitionsToInit.add(MetadataPartitionType.FILES);
      }

      // check if any of the enabled partition types needs to be initialized
      // NOTE: It needs to be guarded by async index config because if that is enabled then initialization happens through the index scheduler.
      if (!dataWriteConfig.isMetadataAsyncIndex()) {
        Set<String> completedPartitions = dataMetaClient.getTableConfig().getMetadataPartitions();
        LOG.info("Async metadata indexing disabled and following partitions already initialized: " + completedPartitions);
        this.enabledPartitionTypes.stream()
            .filter(p -> !completedPartitions.contains(p.getPartitionPath()) && !MetadataPartitionType.FILES.equals(p))
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

  private boolean metadataTableExists(HoodieTableMetaClient dataMetaClient) throws IOException {
    boolean exists = dataMetaClient.getTableConfig().isMetadataTableAvailable();
    boolean reInitialize = false;

    // If the un-synced instants have been archived, then
    // the metadata table will need to be initialized again.
    if (exists) {
      try {
        metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf.get()).setBasePath(metadataWriteConfig.getBasePath()).build();
        if (DEFAULT_METADATA_POPULATE_META_FIELDS != metadataMetaClient.getTableConfig().populateMetaFields()) {
          LOG.info("Re-initiating metadata table properties since populate meta fields have changed");
          metadataMetaClient = initializeMetaClient();
        }
      } catch (TableNotFoundException e) {
        return false;
      }
      final Option<HoodieInstant> latestMetadataInstant =
          metadataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();

      reInitialize = isBootstrapNeeded(latestMetadataInstant);
    }

    if (reInitialize) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.REBOOTSTRAP_STR, 1));
      LOG.info("Deleting Metadata Table directory so that it can be re-initialized");
      HoodieTableMetadataUtil.deleteMetadataTable(dataMetaClient, engineContext, false);
      exists = false;
    }

    return exists;
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
  private boolean isBootstrapNeeded(Option<HoodieInstant> latestMetadataInstant) {
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

    // FILES partition is always required and is initialized first
    boolean filesPartitionAvailable = dataMetaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.FILES);
    if (!filesPartitionAvailable) {
      partitionsToInit.remove(MetadataPartitionType.FILES);
      partitionsToInit.add(0, MetadataPartitionType.FILES);
      // Initialize the metadata table for the first time
      metadataMetaClient = initializeMetaClient();
    } else {
      // Check and then open the metadata table reader so FILES partition can be read during initialization of other partitions
      initMetadataReader();
      // Load the metadata table metaclient if required
      if (metadataMetaClient == null) {
        metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf.get()).setBasePath(metadataWriteConfig.getBasePath()).build();
      }
    }

    // Already initialized partitions can be ignored
    partitionsToInit.removeIf(metadataPartition -> dataMetaClient.getTableConfig().isMetadataPartitionAvailable((metadataPartition)));


    // Get a complete list of files and partitions from the file system or from already initialized FILES partition of MDT
    List<DirectoryInfo> partitionInfoList;
    if (filesPartitionAvailable) {
      partitionInfoList = listAllPartitionsFromMDT(initializationTime);
    } else {
      // if auto initialization is enabled, then we need to list all partitions from the file system
      if (dataWriteConfig.getMetadataConfig().shouldAutoInitialize()) {
        partitionInfoList = listAllPartitionsFromFilesystem(initializationTime, dataWriteConfig.getBasePath());
      } else {
        // if auto initialization is disabled, we can return an empty list
        partitionInfoList = Collections.emptyList();
      }
    }
    Map<String, Map<String, Long>> partitionToFilesMap = partitionInfoList.stream()
        .map(p -> {
          String partitionName = HoodieTableMetadataUtil.getPartitionIdentifier(p.getRelativePath());
          return Pair.of(partitionName, p.getFileNameToSizeMap());
        })
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    for (MetadataPartitionType partitionType : partitionsToInit) {
      // Find the commit timestamp to use for this partition. Each initialization should use its own unique commit time.
      String commitTimeForPartition = generateUniqueCommitInstantTime(initializationTime);

      LOG.info("Initializing MDT partition " + partitionType.name() + " at instant " + commitTimeForPartition);

      Pair<Integer, HoodieData<HoodieRecord>> fileGroupCountAndRecordsPair;
      try {
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

      LOG.info(String.format("Initializing %s index with %d mappings and %d file groups.", partitionType.name(), fileGroupCountAndRecordsPair.getKey(),
          fileGroupCountAndRecordsPair.getValue().count()));
      HoodieTimer partitionInitTimer = HoodieTimer.start();

      // Generate the file groups
      final int fileGroupCount = fileGroupCountAndRecordsPair.getKey();
      ValidationUtils.checkArgument(fileGroupCount > 0, "FileGroup count for MDT partition " + partitionType.name() + " should be > 0");
      initializeFileGroups(dataMetaClient, partitionType, commitTimeForPartition, fileGroupCount);

      // Perform the commit using bulkCommit
      HoodieData<HoodieRecord> records = fileGroupCountAndRecordsPair.getValue();
      bulkCommit(commitTimeForPartition, partitionType, records, fileGroupCount);
      metadataMetaClient.reloadActiveTimeline();
      dataMetaClient.getTableConfig().setMetadataPartitionState(dataMetaClient, partitionType, true);
      // initialize the metadata reader again so the MDT partition can be read after initialization
      initMetadataReader();
      long totalInitTime = partitionInitTimer.endTimer();
      LOG.info(String.format("Initializing %s index in metadata table took " + totalInitTime + " in ms", partitionType.name()));
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
    // if its initialized via Async indexer, we don't need to alter the init time
    if (HoodieTableMetadataUtil.isIndexingCommit(initializationTime)) {
      return initializationTime;
    }
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
    fsView.loadAllPartitions();
    final List<Pair<String, HoodieBaseFile>> partitionBaseFilePairs = new ArrayList<>();
    for (String partition : partitions) {
      partitionBaseFilePairs.addAll(fsView.getLatestBaseFiles(partition)
          .map(basefile -> Pair.of(partition, basefile)).collect(Collectors.toList()));
    }

    LOG.info("Initializing record index from " + partitionBaseFilePairs.size() + " base files in "
        + partitions.size() + " partitions");

    // Collect record keys from the files in parallel
    HoodieData<HoodieRecord> records = readRecordKeysFromBaseFiles(engineContext, partitionBaseFilePairs, false);
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
                                                               List<Pair<String, HoodieBaseFile>> partitionBaseFilePairs,
                                                               boolean forDelete) {
    if (partitionBaseFilePairs.isEmpty()) {
      return engineContext.emptyHoodieData();
    }

    engineContext.setJobStatus(this.getClass().getSimpleName(), "Record Index: reading record keys from " + partitionBaseFilePairs.size() + " base files");
    final int parallelism = Math.min(partitionBaseFilePairs.size(), dataWriteConfig.getMetadataConfig().getRecordIndexMaxParallelism());
    return engineContext.parallelize(partitionBaseFilePairs, parallelism).flatMap(partitionAndBaseFile -> {
      final String partition = partitionAndBaseFile.getKey();
      final HoodieBaseFile baseFile = partitionAndBaseFile.getValue();
      final String filename = baseFile.getFileName();
      Path dataFilePath = new Path(dataWriteConfig.getBasePath(), partition + Path.SEPARATOR + filename);

      final String fileId = baseFile.getFileId();
      final String instantTime = baseFile.getCommitTime();
      HoodieFileReader reader = HoodieFileReaderFactory.getReaderFactory(HoodieRecord.HoodieRecordType.AVRO).getFileReader(hadoopConf.get(), dataFilePath);
      ClosableIterator<String> recordKeyIterator = reader.getRecordKeyIterator();

      return new ClosableIterator<HoodieRecord>() {
        @Override
        public void close() {
          recordKeyIterator.close();
        }

        @Override
        public boolean hasNext() {
          return recordKeyIterator.hasNext();
        }

        @Override
        public HoodieRecord next() {
          return forDelete
              ? HoodieMetadataPayload.createRecordIndexDelete(recordKeyIterator.next())
              : HoodieMetadataPayload.createRecordIndexUpdate(recordKeyIterator.next(), partition, fileId, instantTime, 0);
        }
      };
    });
  }

  protected Pair<Integer, HoodieData<HoodieRecord>> initializeFilesPartition(List<DirectoryInfo> partitionInfoList) {
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
    engineContext.setJobStatus(this.getClass().getSimpleName(), "Creating records for metadata FILES partition");
    boolean enableBasePathForPartitions = dataWriteConfig.shouldEnableBasePathForPartitions();

    HoodieData<HoodieRecord> fileListRecords = engineContext.parallelize(partitionInfoList, partitionInfoList.size()).map(partitionInfo -> {
      Map<String, Long> fileNameToSizeMap = partitionInfo.getFileNameToSizeMap();
      return HoodieMetadataPayload.createPartitionFilesRecord(
          HoodieTableMetadataUtil.getPartitionIdentifier(partitionInfo.getRelativePath()), fileNameToSizeMap, Collections.emptyList(),
          enableBasePathForPartitions, dataWriteConfig.getMetadataConfig().getBasePathOverride());
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

  protected HoodieTableMetaClient initializeMetaClient() throws IOException {
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
  protected List<DirectoryInfo> listAllPartitionsFromFilesystem(String initializationTime, String basePath) {
    List<SerializablePath> pathsToList = new LinkedList<>();
    pathsToList.add(new SerializablePath(new CachingPath(basePath)));

    List<DirectoryInfo> partitionsToBootstrap = new LinkedList<>();
    final int fileListingParallelism = metadataWriteConfig.getFileListingParallelism();
    SerializableConfiguration conf = new SerializableConfiguration(dataMetaClient.getHadoopConf());
    final String dirFilterRegex = dataWriteConfig.getMetadataConfig().getDirectoryFilterRegex();
    final String datasetBasePath = basePath;
    SerializablePath serializableBasePath = new SerializablePath(new CachingPath(datasetBasePath));

    while (!pathsToList.isEmpty()) {
      // In each round we will list a section of directories
      int numDirsToList = Math.min(fileListingParallelism, pathsToList.size());
      // List all directories in parallel
      engineContext.setJobStatus(this.getClass().getSimpleName(), "Listing " + numDirsToList + " partitions from filesystem");
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
  protected void initializeFileGroups(HoodieTableMetaClient dataMetaClient, MetadataPartitionType metadataPartition, String instantTime,
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
    } catch (FileNotFoundException ignored) {
      // If the partition did not exist yet, it will be created below
    }

    // Archival of data table has a dependency on compaction(base files) in metadata table.
    // It is assumed that as of time Tx of base instant (/compaction time) in metadata table,
    // all commits in data table is in sync with metadata table. So, we always start with log file for any fileGroup.

    // Even though the initial commit is a bulkInsert which creates the first baseFiles directly, we still
    // create a log file first. This ensures that if any fileGroups of the MDT index do not receive any records
    // during initial commit, then the fileGroup would still be recognized (as a FileSlice with no baseFiles but a
    // valid logFile). Since these log files being created have no content, it is safe to add them here before
    // the bulkInsert.
    final String msg = String.format("Creating %d file groups for partition %s with base fileId %s at instant time %s",
        fileGroupCount, metadataPartition.getPartitionPath(), metadataPartition.getFileIdPrefix(), instantTime);
    LOG.info(msg);
    final List<String> fileGroupFileIds = IntStream.range(0, fileGroupCount)
        .mapToObj(i -> HoodieTableMetadataUtil.getFileIDForFileGroup(metadataPartition, i))
        .collect(Collectors.toList());
    ValidationUtils.checkArgument(fileGroupFileIds.size() == fileGroupCount);
    engineContext.setJobStatus(this.getClass().getSimpleName(), msg);
    engineContext.foreach(fileGroupFileIds, fileGroupFileId -> {
      try {
        final Map<HeaderMetadataType, String> blockHeader = Collections.singletonMap(HeaderMetadataType.INSTANT_TIME, instantTime);
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
        dataWriteConfig.getColumnsEnabledForBloomFilterIndex(),
        dataWriteConfig.shouldEnableBasePathForPartitions(),
        dataWriteConfig.getMetadataConfig().getBasePathOverride());
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
    LOG.debug("There are no partitions to update according to table config. Falling back to enabled partition types in the write config.");
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
    initializeFromFilesystem(HoodieTableMetadataUtil.createAsyncIndexerTimestamp(indexUptoInstantTime), partitionTypes, Option.empty());
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
      HoodieData<HoodieRecord> updatesFromWriteStatuses = getRecordIndexUpdates(writeStatus);
      HoodieData<HoodieRecord> additionalUpdates = getRecordIndexAdditionalUpdates(updatesFromWriteStatuses, commitMetadata);
      partitionToRecordMap.put(MetadataPartitionType.RECORD_INDEX, updatesFromWriteStatuses.union(additionalUpdates));

      return partitionToRecordMap;
    });
    closeInternal();
  }

  /**
   * Update from {@code HoodieCleanMetadata}.
   *
   * @param cleanMetadata {@code HoodieCleanMetadata}
   * @param instantTime   Timestamp at which the clean was completed
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
   * @param instantTime     Timestamp at which the restore was performed
   */
  @Override
  public void update(HoodieRestoreMetadata restoreMetadata, String instantTime) {
    dataMetaClient.reloadActiveTimeline();

    // Fetch the commit to restore to (savepointed commit time)
    HoodieInstant restoreInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.RESTORE_ACTION, instantTime);
    HoodieInstant requested = HoodieTimeline.getRestoreRequestedInstant(restoreInstant);
    HoodieRestorePlan restorePlan = null;
    try {
      restorePlan = TimelineMetadataUtils.deserializeAvroMetadata(
          dataMetaClient.getActiveTimeline().readRestoreInfoAsBytes(requested).get(), HoodieRestorePlan.class);
    } catch (IOException e) {
      throw new HoodieIOException("Deserialization of restore plan failed whose restore instant time is " + instantTime + " in data table", e);
    }
    final String restoreToInstantTime = restorePlan.getSavepointToRestoreTimestamp();
    LOG.info("Triggering restore to " + restoreToInstantTime + " in metadata table");

    // fetch the earliest commit to retain and ensure the base file prior to the time to restore is present
    List<HoodieFileGroup> filesGroups = metadata.getMetadataFileSystemView().getAllFileGroups(MetadataPartitionType.FILES.getPartitionPath()).collect(Collectors.toList());

    boolean cannotRestore = filesGroups.stream().map(fileGroup -> fileGroup.getAllFileSlices().map(fileSlice -> fileSlice.getBaseInstantTime()).anyMatch(
        instantTime1 -> HoodieTimeline.compareTimestamps(instantTime1, LESSER_THAN_OR_EQUALS, restoreToInstantTime))).anyMatch(canRestore -> !canRestore);
    if (cannotRestore) {
      throw new HoodieMetadataException(String.format("Can't restore to %s since there is no base file in MDT lesser than the commit to restore to. "
          + "Please delete metadata table and retry", restoreToInstantTime));
    }

    // Restore requires the existing pipelines to be shutdown. So we can safely scan the dataset to find the current
    // list of files in the filesystem.
    List<DirectoryInfo> dirInfoList = listAllPartitionsFromFilesystem(instantTime, dataWriteConfig.getBasePath());
    Map<String, DirectoryInfo> dirInfoMap = dirInfoList.stream().collect(Collectors.toMap(DirectoryInfo::getRelativePath, Function.identity()));
    dirInfoList.clear();

    BaseHoodieWriteClient<?, I, ?, ?> writeClient = getWriteClient();
    writeClient.restoreToInstant(restoreToInstantTime, false);

    // At this point we have also reverted the cleans which have occurred after the restoreToInstantTime. Hence, a sync
    // is required to bring back those cleans.
    try {
      initMetadataReader();
      Map<String, Map<String, Long>> partitionFilesToAdd = new HashMap<>();
      Map<String, List<String>> partitionFilesToDelete = new HashMap<>();
      List<String> partitionsToDelete = new ArrayList<>();
      fetchOutofSyncFilesRecordsFromMetadataTable(dirInfoMap, partitionFilesToAdd, partitionFilesToDelete, partitionsToDelete);

      // Even if we don't have any deleted files to sync, we still create an empty commit so that we can track the restore has completed.
      // We cannot create a deltaCommit at instantTime now because a future (rollback) block has already been written to the logFiles.
      // We need to choose a timestamp which would be a validInstantTime for MDT. This is either a commit timestamp completed on the dataset
      // or a timestamp with suffix which we use for MDT clean, compaction etc.
      String syncCommitTime = HoodieTableMetadataUtil.createRestoreTimestamp(HoodieActiveTimeline.createNewInstantTime());
      processAndCommit(syncCommitTime, () -> HoodieTableMetadataUtil.convertMissingPartitionRecords(engineContext,
          partitionsToDelete, partitionFilesToAdd, partitionFilesToDelete, syncCommitTime,
          dataWriteConfig.shouldEnableBasePathForPartitions(), dataWriteConfig.getMetadataConfig().getBasePathOverride()));
      closeInternal();
    } catch (IOException e) {
      throw new HoodieMetadataException("IOException during MDT restore sync", e);
    }
  }

  /**
   * Update from {@code HoodieRollbackMetadata}.
   *
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param instantTime      Timestamp at which the rollback was performed
   */
  @Override
  public void update(HoodieRollbackMetadata rollbackMetadata, String instantTime) {
    if (initialized && metadata != null) {
      // The commit which is being rolled back on the dataset
      final String commitToRollbackInstantTime = rollbackMetadata.getCommitsRollback().get(0);
      // Find the deltacommits since the last compaction
      Option<Pair<HoodieTimeline, HoodieInstant>> deltaCommitsInfo =
          CompactionUtils.getDeltaCommitsSinceLatestCompaction(metadataMetaClient.getActiveTimeline());

      // This could be a compaction or deltacommit instant (See CompactionUtils.getDeltaCommitsSinceLatestCompaction)
      HoodieInstant compactionInstant = deltaCommitsInfo.get().getValue();
      HoodieTimeline deltacommitsSinceCompaction = deltaCommitsInfo.get().getKey();

      // The deltacommit that will be rolled back
      HoodieInstant deltaCommitInstant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, commitToRollbackInstantTime);

      validateRollback(commitToRollbackInstantTime, compactionInstant, deltacommitsSinceCompaction);

      // lets apply a delta commit with DT's rb instant(with special suffix) containing following records:
      // a. any log files as part of RB commit metadata that was added
      // b. log files added by the commit in DT being rolled back. By rolled back, we mean, a rollback block will be added and does not mean it will be deleted.
      // both above list should only be added to FILES partition.

      String rollbackInstantTime = createRollbackTimestamp(instantTime);
      processAndCommit(instantTime, () -> HoodieTableMetadataUtil.convertMetadataToRecords(engineContext, dataMetaClient, rollbackMetadata, instantTime,
          dataWriteConfig.getMetadataConfig().shouldEnableBasePathForPartitions(), dataWriteConfig.getMetadataConfig().getBasePathOverride()));

      if (deltacommitsSinceCompaction.containsInstant(deltaCommitInstant)) {
        LOG.info("Rolling back MDT deltacommit " + commitToRollbackInstantTime);
        if (!getWriteClient().rollback(commitToRollbackInstantTime, rollbackInstantTime)) {
          throw new HoodieMetadataException("Failed to rollback deltacommit at " + commitToRollbackInstantTime);
        }
      } else {
        LOG.info(String.format("Ignoring rollback of instant %s at %s. The commit to rollback is not found in MDT",
            commitToRollbackInstantTime, instantTime));
      }
      closeInternal();
    }
  }

  protected void validateRollback(
      String commitToRollbackInstantTime,
      HoodieInstant compactionInstant,
      HoodieTimeline deltacommitsSinceCompaction) {
    // The commit being rolled back should not be earlier than the latest compaction on the MDT. Compaction on MDT only occurs when all actions
    // are completed on the dataset. Hence, this case implies a rollback of completed commit which should actually be handled using restore.
    if (compactionInstant.getAction().equals(HoodieTimeline.COMMIT_ACTION)) {
      final String compactionInstantTime = compactionInstant.getTimestamp();
      if (HoodieTimeline.LESSER_THAN_OR_EQUALS.test(commitToRollbackInstantTime, compactionInstantTime)) {
        throw new HoodieMetadataException(String.format("Commit being rolled back %s is earlier than the latest compaction %s. "
                + "There are %d deltacommits after this compaction: %s", commitToRollbackInstantTime, compactionInstantTime,
            deltacommitsSinceCompaction.countInstants(), deltacommitsSinceCompaction.getInstants()));
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (metadata != null) {
      metadata.close();
    }
    if (writeClient != null) {
      writeClient.close();
      writeClient = null;
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
   * Converts the input records to the input format expected by the write client.
   * @param records records to be converted
   * @return converted records
   */
  protected abstract I convertHoodieDataToEngineSpecificData(HoodieData<HoodieRecord> records);

  protected void commitInternal(String instantTime, Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap, boolean isInitializing,
                                Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    ValidationUtils.checkState(metadataMetaClient != null, "Metadata table is not fully initialized yet.");
    HoodieData<HoodieRecord> preppedRecords = prepRecords(partitionRecordsMap);
    I preppedRecordInputs = convertHoodieDataToEngineSpecificData(preppedRecords);

    BaseHoodieWriteClient<?, I, ?, ?> writeClient = getWriteClient();
    // rollback partially failed writes if any.
    if (dataWriteConfig.getFailedWritesCleanPolicy().isEager() && writeClient.rollbackFailedWrites()) {
      metadataMetaClient = HoodieTableMetaClient.reload(metadataMetaClient);
    }

    if (!metadataMetaClient.getActiveTimeline().getCommitsTimeline().containsInstant(instantTime)) {
      // if this is a new commit being applied to metadata for the first time
      LOG.info("New commit at " + instantTime + " being applied to MDT.");
    } else {
      // this code path refers to a re-attempted commit that:
      //   1. got committed to metadata table, but failed in datatable.
      //   2. failed while committing to metadata table
      // for e.g., let's say compaction c1 on 1st attempt succeeded in metadata table and failed before committing to datatable.
      // when retried again, data table will first rollback pending compaction. these will be applied to metadata table, but all changes
      // are upserts to metadata table and so only a new delta commit will be created.
      // once rollback is complete in datatable, compaction will be retried again, which will eventually hit this code block where the respective commit is
      // already part of completed commit. So, we have to manually rollback the completed instant and proceed.
      Option<HoodieInstant> alreadyCompletedInstant = metadataMetaClient.getActiveTimeline().filterCompletedInstants().filter(entry -> entry.getTimestamp().equals(instantTime))
          .lastInstant();
      LOG.info(String.format("%s completed commit at %s being applied to MDT.",
          alreadyCompletedInstant.isPresent() ? "Already" : "Partially", instantTime));

      // Rollback the previous commit
      if (!writeClient.rollback(instantTime)) {
        throw new HoodieMetadataException("Failed to rollback deltacommit at " + instantTime + " from MDT");
      }
      metadataMetaClient.reloadActiveTimeline();
    }

    writeClient.startCommitWithTime(instantTime);
    preWrite(instantTime);
    if (isInitializing) {
      engineContext.setJobStatus(this.getClass().getSimpleName(), String.format("Bulk inserting at %s into metadata table %s", instantTime, metadataWriteConfig.getTableName()));
      writeClient.bulkInsertPreppedRecords(preppedRecordInputs, instantTime, bulkInsertPartitioner);
    } else {
      engineContext.setJobStatus(this.getClass().getSimpleName(), String.format("Upserting at %s into metadata table %s", instantTime, metadataWriteConfig.getTableName()));
      writeClient.upsertPreppedRecords(preppedRecordInputs, instantTime);
    }

    metadataMetaClient.reloadActiveTimeline();

    // Update total size of the metadata and count of base/log files
    metrics.ifPresent(m -> m.updateSizeMetrics(metadataMetaClient, metadata, dataMetaClient.getTableConfig().getMetadataPartitions()));
  }

  /**
   * Allows the implementation to perform any pre-commit operations like transitioning a commit to inflight if required.
   * @param instantTime time of commit
   */
  protected void preWrite(String instantTime) {
    // Default is No-Op
  }

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
  public abstract void bulkCommit(
      String instantTime, MetadataPartitionType partitionType, HoodieData<HoodieRecord> records,
      int fileGroupCount);

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
    BaseHoodieWriteClient<?, I, ?, ?> writeClient = getWriteClient();
    try {
      // Run any pending table services operations.
      runPendingTableServicesOperations(writeClient);

      Option<HoodieInstant> lastInstant = metadataMetaClient.reloadActiveTimeline().getDeltaCommitTimeline()
          .filterCompletedInstants()
          .lastInstant();
      if (!lastInstant.isPresent()) {
        return;
      }
      // Check and run clean operations.
      String latestDeltacommitTime = lastInstant.get()
          .getTimestamp();
      LOG.info("Latest deltacommit time found is " + latestDeltacommitTime + ", running clean operations.");
      cleanIfNecessary(writeClient, latestDeltacommitTime);

      // Do timeline validation before scheduling compaction/logCompaction operations.
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
    // If pending compact/logCompaction operations are found abort scheduling new compaction/logCompaction operations.
    Option<HoodieInstant> pendingLogCompactionInstant =
        metadataMetaClient.getActiveTimeline().filterPendingLogCompactionTimeline().firstInstant();
    Option<HoodieInstant> pendingCompactionInstant =
        metadataMetaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant();
    if (pendingLogCompactionInstant.isPresent() || pendingCompactionInstant.isPresent()) {
      LOG.warn(String.format("Not scheduling compaction or logCompaction, since a pending compaction instant %s or logCompaction %s instant is present",
          pendingCompactionInstant, pendingLogCompactionInstant));
      return false;
    }
    return true;
  }

  private void fetchOutofSyncFilesRecordsFromMetadataTable(Map<String, DirectoryInfo> dirInfoMap, Map<String, Map<String, Long>> partitionFilesToAdd,
                                                           Map<String, List<String>> partitionFilesToDelete, List<String> partitionsToDelete) throws IOException {

    for (String partition : metadata.fetchAllPartitionPaths()) {
      Path partitionPath = null;
      if (StringUtils.isNullOrEmpty(partition) && !dataMetaClient.getTableConfig().isTablePartitioned()) {
        partitionPath = new Path(dataWriteConfig.getBasePath());
      } else {
        partitionPath = new Path(dataWriteConfig.getBasePath(), partition);
      }
      final String partitionId = HoodieTableMetadataUtil.getPartitionIdentifier(partition);
      FileStatus[] metadataFiles = metadata.getAllFilesInPartition(partitionPath);
      if (!dirInfoMap.containsKey(partition)) {
        // Entire partition has been deleted
        partitionsToDelete.add(partitionId);
        if (metadataFiles != null && metadataFiles.length > 0) {
          partitionFilesToDelete.put(partitionId, Arrays.stream(metadataFiles).map(f -> f.getPath().getName()).collect(Collectors.toList()));
        }
      } else {
        // Some files need to be cleaned and some to be added in the partition
        Map<String, Long> fsFiles = dirInfoMap.get(partition).getFileNameToSizeMap();
        List<String> mdtFiles = Arrays.stream(metadataFiles).map(mdtFile -> mdtFile.getPath().getName()).collect(Collectors.toList());
        List<String> filesDeleted = Arrays.stream(metadataFiles).map(f -> f.getPath().getName())
            .filter(n -> !fsFiles.containsKey(n)).collect(Collectors.toList());
        Map<String, Long> filesToAdd = new HashMap<>();
        // new files could be added to DT due to restore that just happened which may not be tracked in RestoreMetadata.
        dirInfoMap.get(partition).getFileNameToSizeMap().forEach((k,v) -> {
          if (!mdtFiles.contains(k)) {
            filesToAdd.put(k,v);
          }
        });
        if (!filesToAdd.isEmpty()) {
          partitionFilesToAdd.put(partitionId, filesToAdd);
        }
        if (!filesDeleted.isEmpty()) {
          partitionFilesToDelete.put(partitionId, filesDeleted);
        }
      }
    }
  }

  /**
   * Return records that represent update to the record index due to write operation on the dataset.
   *
   * @param writeStatuses {@code WriteStatus} from the write operation
   */
  private HoodieData<HoodieRecord> getRecordIndexUpdates(HoodieData<WriteStatus> writeStatuses) {
    HoodiePairData<String, HoodieRecordDelegate> recordKeyDelegatePairs = null;
    // if update partition path is true, chances that we might get two records (1 delete in older partition and 1 insert to new partition)
    // and hence we might have to do reduce By key before ingesting to RLI partition.
    if (dataWriteConfig.getRecordIndexUpdatePartitionPath()) {
      recordKeyDelegatePairs = writeStatuses.map(writeStatus -> writeStatus.getWrittenRecordDelegates().stream()
              .map(recordDelegate -> Pair.of(recordDelegate.getRecordKey(), recordDelegate)))
          .flatMapToPair(Stream::iterator)
          .reduceByKey((recordDelegate1, recordDelegate2) -> {
            if (recordDelegate1.getRecordKey().equals(recordDelegate2.getRecordKey())) {
              if (!recordDelegate1.getNewLocation().isPresent() && !recordDelegate2.getNewLocation().isPresent()) {
                throw new HoodieIOException("Both version of records do not have location set. Record V1 " + recordDelegate1.toString()
                    + ", Record V2 " + recordDelegate2.toString());
              }
              if (recordDelegate1.getNewLocation().isPresent()) {
                return recordDelegate1;
              } else {
                // if record delegate 1 does not have location set, record delegate 2 should have location set.
                return recordDelegate2;
              }
            } else {
              return recordDelegate1;
            }
          }, Math.max(1, writeStatuses.getNumPartitions()));
    } else {
      // if update partition path = false, we should get only one entry per record key.
      recordKeyDelegatePairs = writeStatuses.flatMapToPair(
          (SerializableFunction<WriteStatus, Iterator<? extends Pair<String, HoodieRecordDelegate>>>) writeStatus
              -> writeStatus.getWrittenRecordDelegates().stream().map(rec -> Pair.of(rec.getRecordKey(), rec)).iterator());
    }
    return recordKeyDelegatePairs
        .map(writeStatusRecordDelegate -> {
          HoodieRecordDelegate recordDelegate = writeStatusRecordDelegate.getValue();
          HoodieRecord hoodieRecord = null;
          Option<HoodieRecordLocation> newLocation = recordDelegate.getNewLocation();
          if (newLocation.isPresent()) {
            if (recordDelegate.getCurrentLocation().isPresent()) {
              // This is an update, no need to update index if the location has not changed
              // newLocation should have the same fileID as currentLocation. The instantTimes differ as newLocation's
              // instantTime refers to the current commit which was completed.
              if (!recordDelegate.getCurrentLocation().get().getFileId().equals(newLocation.get().getFileId())) {
                final String msg = String.format("Detected update in location of record with key %s from %s "
                        + " to %s. The fileID should not change.",
                    recordDelegate, recordDelegate.getCurrentLocation().get(), newLocation.get());
                LOG.error(msg);
                throw new HoodieMetadataException(msg);
              }
              // for updates, we can skip updating RLI partition in MDT
            } else {
              hoodieRecord = HoodieMetadataPayload.createRecordIndexUpdate(
                  recordDelegate.getRecordKey(), recordDelegate.getPartitionPath(),
                  newLocation.get().getFileId(), newLocation.get().getInstantTime(), dataWriteConfig.getWritesFileIdEncoding());
            }
          } else {
            // Delete existing index for a deleted record
            hoodieRecord = HoodieMetadataPayload.createRecordIndexDelete(recordDelegate.getRecordKey());
          }
          return hoodieRecord;
        })
        .filter(Objects::nonNull);
  }

  private HoodieData<HoodieRecord> getRecordIndexReplacedRecords(HoodieReplaceCommitMetadata replaceCommitMetadata) {
    final HoodieMetadataFileSystemView fsView = new HoodieMetadataFileSystemView(dataMetaClient,
        dataMetaClient.getActiveTimeline(), metadata);
    List<Pair<String, HoodieBaseFile>> partitionBaseFilePairs = replaceCommitMetadata
        .getPartitionToReplaceFileIds()
        .keySet().stream().flatMap(partition
            -> fsView.getLatestBaseFiles(partition).map(f -> Pair.of(partition, f)))
        .collect(Collectors.toList());

    return readRecordKeysFromBaseFiles(engineContext, partitionBaseFilePairs, true);
  }

  private HoodieData<HoodieRecord> getRecordIndexAdditionalUpdates(HoodieData<HoodieRecord> updatesFromWriteStatuses, HoodieCommitMetadata commitMetadata) {
    WriteOperationType operationType = commitMetadata.getOperationType();
    if (operationType == WriteOperationType.INSERT_OVERWRITE) {
      // load existing records from replaced filegroups and left anti join overwriting records
      // return partition-level unmatched records (with newLocation being null) to be deleted from RLI
      return getRecordIndexReplacedRecords((HoodieReplaceCommitMetadata) commitMetadata)
          .mapToPair(r -> Pair.of(r.getKey(), r))
          .leftOuterJoin(updatesFromWriteStatuses.mapToPair(r -> Pair.of(r.getKey(), r)))
          .values()
          .filter(p -> !p.getRight().isPresent())
          .map(Pair::getLeft);
    } else if (operationType == WriteOperationType.INSERT_OVERWRITE_TABLE) {
      // load existing records from replaced filegroups and left anti join overwriting records
      // return globally unmatched records (with newLocation being null) to be deleted from RLI
      return getRecordIndexReplacedRecords((HoodieReplaceCommitMetadata) commitMetadata)
          .mapToPair(r -> Pair.of(r.getRecordKey(), r))
          .leftOuterJoin(updatesFromWriteStatuses.mapToPair(r -> Pair.of(r.getRecordKey(), r)))
          .values()
          .filter(p -> !p.getRight().isPresent())
          .map(Pair::getLeft);
    } else if (operationType == WriteOperationType.DELETE_PARTITION) {
      // all records from the target partition(s) to be deleted from RLI
      return getRecordIndexReplacedRecords((HoodieReplaceCommitMetadata) commitMetadata);
    } else {
      return engineContext.emptyHoodieData();
    }
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

  protected BaseHoodieWriteClient<?, I, ?, ?> getWriteClient() {
    if (writeClient == null) {
      writeClient = initializeWriteClient();
    }
    return writeClient;
  }

  protected abstract BaseHoodieWriteClient<?, I, ?, ?> initializeWriteClient();

  /**
   * A class which represents a directory and the files and directories inside it.
   * <p>
   * A {@code PartitionFileInfo} object saves the name of the partition and various properties requires of each file
   * required for initializing the metadata table. Saving limited properties reduces the total memory footprint when
   * a very large number of files are present in the dataset being initialized.
   */
  public static class DirectoryInfo implements Serializable {
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

    public String getRelativePath() {
      return relativePath;
    }

    public int getTotalFiles() {
      return filenameToSizeMap.size();
    }

    public boolean isHoodiePartition() {
      return isHoodiePartition;
    }

    public List<Path> getSubDirectories() {
      return subDirectories;
    }

    // Returns a map of filenames mapped to their lengths
    public Map<String, Long> getFileNameToSizeMap() {
      return filenameToSizeMap;
    }
  }
}
