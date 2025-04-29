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
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadataUtil.DirectoryInfo;
import org.apache.hudi.metadata.index.ExpressionIndexRecordGenerator;
import org.apache.hudi.metadata.index.Indexer;
import org.apache.hudi.metadata.index.IndexerFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.Lazy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_POPULATE_META_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.TIMELINE_HISTORY_PATH;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.metadata.HoodieMetadataWriteUtils.createMetadataWriteConfig;
import static org.apache.hudi.metadata.HoodieTableMetadata.METADATA_TABLE_NAME_SUFFIX;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getInflightMetadataPartitions;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.apache.hudi.metadata.MetadataPartitionType.fromPartitionPath;
import static org.apache.hudi.metadata.index.partitionstats.PartitionStatsIndexer.getPartitionLatestFileSlicesIncludingInflight;

/**
 * Writer implementation backed by an internal hudi table. Partition and file listing are saved within an internal MOR table
 * called Metadata Table. This table is created by listing files and partitions (first time)
 * and kept in sync using the instants on the main dataset.
 *
 * @param <I> Type of input for the write client
 */
public abstract class HoodieBackedTableMetadataWriter<I> implements HoodieTableMetadataWriter {

  static final Logger LOG = LoggerFactory.getLogger(HoodieBackedTableMetadataWriter.class);

  // Virtual keys support for metadata table. This Field is
  // from the metadata payload schema.
  private static final String RECORD_KEY_FIELD_NAME = HoodieMetadataPayload.KEY_FIELD_NAME;

  protected transient BaseHoodieWriteClient<?, I, ?, ?> writeClient;

  protected HoodieWriteConfig metadataWriteConfig;
  protected HoodieWriteConfig dataWriteConfig;

  protected HoodieBackedTableMetadata metadata;
  protected HoodieTableMetaClient metadataMetaClient;
  protected HoodieTableMetaClient dataMetaClient;
  protected Option<HoodieMetadataMetrics> metrics;
  protected StorageConfiguration<?> storageConf;
  protected final transient HoodieEngineContext engineContext;
  protected final transient Map<MetadataPartitionType, Indexer> enabledIndexBuilderMap;
  protected final transient ExpressionIndexRecordGenerator expressionIndexRecordGenerator;
  // Is the MDT bootstrapped and ready to be read from
  boolean initialized = false;
  private HoodieTableFileSystemView metadataView;

  /**
   * Hudi backed table metadata writer.
   *
   * @param storageConf                Storage configuration to use for the metadata writer
   * @param writeConfig                Writer config
   * @param failedWritesCleaningPolicy Cleaning policy on failed writes
   * @param engineContext              Engine context
   * @param inflightInstantTimestamp   Timestamp of any instant in progress
   */
  protected HoodieBackedTableMetadataWriter(StorageConfiguration<?> storageConf,
                                            HoodieWriteConfig writeConfig,
                                            HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                            HoodieEngineContext engineContext,
                                            ExpressionIndexRecordGenerator expressionIndexRecordGenerator,
                                            Option<String> inflightInstantTimestamp) {
    this.dataWriteConfig = writeConfig;
    this.engineContext = engineContext;
    this.storageConf = storageConf;
    this.metrics = Option.empty();
    this.dataMetaClient = HoodieTableMetaClient.builder().setConf(storageConf.newInstance())
        .setBasePath(dataWriteConfig.getBasePath())
        .setTimeGeneratorConfig(dataWriteConfig.getTimeGeneratorConfig()).build();
    this.enabledIndexBuilderMap =
        IndexerFactory.getEnabledIndexBuilderMap(
            getEngineType(), engineContext, dataWriteConfig, dataMetaClient,
            getTable(dataWriteConfig, dataMetaClient), expressionIndexRecordGenerator);
    this.expressionIndexRecordGenerator = expressionIndexRecordGenerator;
    if (writeConfig.isMetadataTableEnabled()) {
      this.metadataWriteConfig = createMetadataWriteConfig(writeConfig, failedWritesCleaningPolicy);
      try {
        initRegistry();
        initialized = initializeIfNeeded(dataMetaClient, inflightInstantTimestamp);
      } catch (IOException e) {
        LOG.error("Failed to initialize metadata table", e);
      }
    }
    ValidationUtils.checkArgument(!initialized || this.metadata != null, "MDT Reader should have been opened post initialization");
  }

  abstract HoodieTable getTable(HoodieWriteConfig writeConfig, HoodieTableMetaClient metaClient);

  private void mayBeReinitMetadataReader() {
    if (metadata == null || metadataMetaClient == null || metadata.getMetadataFileSystemView() == null) {
      initMetadataReader();
    }
  }

  private void initMetadataReader() {
    if (this.metadata != null) {
      this.metadata.close();
    }

    try {
      this.metadata = new HoodieBackedTableMetadata(engineContext, dataMetaClient.getStorage(), dataWriteConfig.getMetadataConfig(), dataWriteConfig.getBasePath(), true);
      this.metadataMetaClient = metadata.getMetadataMetaClient();
    } catch (Exception e) {
      throw new HoodieException("Could not open MDT for reads", e);
    }
  }

  private HoodieTableFileSystemView getMetadataView() {
    if (metadataView == null || !metadataView.equals(metadata.getMetadataFileSystemView())) {
      ValidationUtils.checkState(metadata != null, "Metadata table not initialized");
      ValidationUtils.checkState(dataMetaClient != null, "Data table meta client not initialized");
      metadataView = new HoodieTableFileSystemView(metadata, dataMetaClient, dataMetaClient.getActiveTimeline());
    }
    return metadataView;
  }

  protected abstract void initRegistry();

  public HoodieWriteConfig getWriteConfig() {
    return metadataWriteConfig;
  }

  public HoodieBackedTableMetadata getTableMetadata() {
    return metadata;
  }

  public Set<MetadataPartitionType> getEnabledPartitionTypes() {
    return this.enabledIndexBuilderMap.keySet();
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
    List<MetadataPartitionType> metadataPartitionsToInit = new ArrayList<>(MetadataPartitionType.getValidValues().length);

    try {
      boolean exists = metadataTableExists(dataMetaClient);
      if (!exists) {
        // FILES partition is always required
        metadataPartitionsToInit.add(FILES);
      }

      // check if any of the enabled partition types needs to be initialized
      // NOTE: It needs to be guarded by async index config because if that is enabled then initialization happens through the index scheduler.
      if (!dataWriteConfig.isMetadataAsyncIndex()) {
        Set<String> completedPartitions = dataMetaClient.getTableConfig().getMetadataPartitions();
        LOG.info("Async metadata indexing disabled and following partitions already initialized: {}", completedPartitions);
        this.enabledIndexBuilderMap.keySet().stream()
            .filter(p -> !completedPartitions.contains(p.getPartitionPath()) && !FILES.equals(p))
            .forEach(metadataPartitionsToInit::add);
      }

      if (metadataPartitionsToInit.isEmpty()) {
        // No partitions left to initialize, since all the metadata enabled partitions are either initialized before
        // or current in the process of initialization.
        initMetadataReader();
        return true;
      }

      // If there is no commit on the dataset yet, use the SOLO_COMMIT_TIMESTAMP as the instant time for initial commit
      // Otherwise, we use the timestamp of the latest completed action.
      String initializationTime = dataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant().map(HoodieInstant::requestedTime).orElse(SOLO_COMMIT_TIMESTAMP);
      if (!initializeFromFilesystem(initializationTime, metadataPartitionsToInit.stream()
          .collect(Collectors.toMap(Function.identity(),
              type -> IndexerFactory.getIndexBuilder(
                  type, getEngineType(), engineContext, dataWriteConfig, dataMetaClient,
                  // TODO(yihua): Revisit this to make sure we don't recreate table instance
                  getTable(dataWriteConfig, dataMetaClient), expressionIndexRecordGenerator))), inflightInstantTimestamp)) {
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
    // TODO(yihua): take care of this in the constructor with indexer map when reinitialization is needed.
    if (exists) {
      try {
        metadataMetaClient = HoodieTableMetaClient.builder()
            .setConf(storageConf.newInstance())
            .setBasePath(metadataWriteConfig.getBasePath())
            .setTimeGeneratorConfig(dataWriteConfig.getTimeGeneratorConfig()).build();
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
      metrics.ifPresent(m -> m.incrementMetric(HoodieMetadataMetrics.REBOOTSTRAP_STR, 1));
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
    return false;
  }

  boolean shouldInitializeFromFilesystem(Set<String> pendingDataInstants, Option<String> inflightInstantTimestamp) {
    return true;
  }

  /**
   * Initialize the Metadata Table by listing files and partitions from the file system.
   *
   * @param initializationTime       - Timestamp to use for the commit
   * @param builderMapForPartitionsToInit         - List of MDT partitions to initialize
   * @param inflightInstantTimestamp - Current action instant responsible for this initialization
   */
  private boolean initializeFromFilesystem(String initializationTime,
                                           Map<MetadataPartitionType, Indexer> builderMapForPartitionsToInit,
                                           Option<String> inflightInstantTimestamp)
      throws IOException {
    Set<String> pendingDataInstants = getPendingDataInstants(dataMetaClient);
    if (!shouldInitializeFromFilesystem(pendingDataInstants, inflightInstantTimestamp)) {
      return false;
    }

    // FILES partition is always required and is initialized first
    boolean filesPartitionAvailable = dataMetaClient.getTableConfig().isMetadataPartitionAvailable(FILES);
    if (!filesPartitionAvailable) {
      // Initialize the metadata table for the first time
      metadataMetaClient = initializeMetaClient();
    } else {
      // Check and then open the metadata table reader so FILES partition can be read during initialization of other partitions
      initMetadataReader();
      // Load the metadata table metaclient if required
      if (metadataMetaClient == null) {
        metadataMetaClient = HoodieTableMetaClient.builder()
            .setConf(storageConf.newInstance()).setBasePath(metadataWriteConfig.getBasePath())
            .setTimeGeneratorConfig(dataWriteConfig.getTimeGeneratorConfig()).build();
      }
    }

    builderMapForPartitionsToInit.keySet().removeIf(
        metadataPartition -> dataMetaClient.getTableConfig().isMetadataPartitionAvailable(metadataPartition));

    // Get a complete list of files and partitions from the file system or from already initialized FILES partition of MDT
    List<DirectoryInfo> partitionInfoList;
    if (filesPartitionAvailable) {
      partitionInfoList = listAllPartitionsFromMDT(initializationTime, pendingDataInstants);
    } else {
      // if auto initialization is enabled, then we need to list all partitions from the file system
      if (dataWriteConfig.getMetadataConfig().shouldAutoInitialize()) {
        partitionInfoList = listAllPartitionsFromFilesystem(initializationTime, pendingDataInstants);
      } else {
        // if auto initialization is disabled, we can return an empty list
        partitionInfoList = Collections.emptyList();
      }
    }
    Map<String, Map<String, Long>> partitionToFilesMap = partitionInfoList.stream()
        .map(p -> {
          String partitionName = HoodieTableMetadataUtil.getPartitionIdentifierForFilesPartition(p.getRelativePath());
          return Pair.of(partitionName, p.getFileNameToSizeMap());
        })
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    if (!filesPartitionAvailable) {
      initializeMetadataPartition(
          FILES, builderMapForPartitionsToInit.get(FILES), initializationTime, partitionInfoList, partitionToFilesMap);
    }
    // TODO(yihua): FILES partition should not be included here (dead code?) as it should be initialized already
    for (Map.Entry<MetadataPartitionType, Indexer> entry :
        builderMapForPartitionsToInit.entrySet().stream()
            .filter(e -> e.getKey() != FILES).collect(Collectors.toList())) {
      initializeMetadataPartition(
          entry.getKey(), entry.getValue(), initializationTime, partitionInfoList, partitionToFilesMap);
    }
    return true;
  }

  private void initializeMetadataPartition(MetadataPartitionType partitionType,
                                           Indexer indexer,
                                           String initializationTime,
                                           List<DirectoryInfo> partitionInfoList,
                                           Map<String, Map<String, Long>> partitionToFilesMap)
      throws IOException {
    // Find the commit timestamp to use for this partition. Each initialization should use its own unique commit time.
    String instantTimeForPartition = generateUniqueInstantTime(initializationTime);
    String partitionTypeName = partitionType.name();
    LOG.info("Initializing MDT partition {} at instant {}", partitionTypeName, instantTimeForPartition);
    List<Indexer.InitialIndexPartitionData> initialIndexPartitionDataList;
    try {
      initialIndexPartitionDataList = indexer.build(
          partitionInfoList, partitionToFilesMap, initializationTime,
          Lazy.lazily(this::getMetadataView), metadata, instantTimeForPartition);
    } catch (Exception e) {
      String metricKey = partitionType.getPartitionPath() + "_" + HoodieMetadataMetrics.BOOTSTRAP_ERR_STR;
      metrics.ifPresent(m -> m.setMetric(metricKey, 1));
      String errMsg = String.format("Bootstrap on %s partition failed for %s",
          partitionType.getPartitionPath(), metadataMetaClient.getBasePath());
      LOG.error(errMsg, e);
      throw new HoodieMetadataException(errMsg, e);
    }

    // Generate the file groups
    if (initialIndexPartitionDataList.isEmpty()) {
      LOG.info("Skip building {} index in metadata table", partitionTypeName);
      return;
    }

    // TODO(HUDI-): support initializing multiple partitions per index type
    ValidationUtils.checkArgument(initialIndexPartitionDataList.size() == 1,
        "Only support the initialization of one partition per index type");

    Indexer.InitialIndexPartitionData initialIndexPartitionData =
        initialIndexPartitionDataList.get(0);
    final int numFileGroup = initialIndexPartitionData.numFileGroup();
    LOG.info("Initializing {} index with {} file groups", partitionTypeName, numFileGroup);
    HoodieTimer partitionInitTimer = HoodieTimer.start();

    String partitionName = initialIndexPartitionData.partitionName();
    initializeFileGroups(dataMetaClient, partitionType, instantTimeForPartition, numFileGroup, partitionName);

    // Perform the commit using bulkCommit
    bulkCommit(instantTimeForPartition, partitionName, initialIndexPartitionData.records(), numFileGroup);
    indexer.updateTableConfig();
    dataMetaClient.getTableConfig().setMetadataPartitionState(dataMetaClient, partitionName, true);
    // initialize the metadata reader again so the MDT partition can be read after initialization
    initMetadataReader();
    long totalInitTime = partitionInitTimer.endTimer();
    LOG.info("Initializing {} index in metadata table took {} in ms", partitionTypeName, totalInitTime);
  }

  /**
   * Updates the list of columns to index with col stats index.
   * @param columnsToIndex list of columns to index.
   */
  protected abstract void updateColumnsToIndexWithColStats(List<String> columnsToIndex);

  /**
   * Returns a unique timestamp to use for initializing a MDT partition.
   * <p>
   * Since commits are immutable, we should use unique timestamps to initialize each partition. For this, we will add a suffix to the given initializationTime
   * until we find a unique timestamp.
   *
   * @param initializationTime Timestamp from dataset to use for initialization
   * @return a unique timestamp for MDT
   */
  String generateUniqueInstantTime(String initializationTime) {
    // If it's initialized via Async indexer, we don't need to alter the init time.
    // otherwise yields the timestamp on the fly.
    // This function would be called multiple times in a single application if multiple indexes are being
    // initialized one after the other.
    HoodieTimeline dataIndexTimeline = dataMetaClient.getActiveTimeline().filter(instant -> instant.getAction().equals(HoodieTimeline.INDEXING_ACTION));
    if (HoodieTableMetadataUtil.isIndexingCommit(dataIndexTimeline, initializationTime)) {
      return initializationTime;
    }
    for (int offset = 0; ; ++offset) {
      final String commitInstantTime = HoodieInstantTimeGenerator.instantTimePlusMillis(SOLO_COMMIT_TIMESTAMP, offset);
      if (!metadataMetaClient.getCommitsTimeline().containsInstant(commitInstantTime)) {
        return commitInstantTime;
      }
    }
  }

  protected abstract EngineType getEngineType();

  private Set<String> getPendingDataInstants(HoodieTableMetaClient dataMetaClient) {
    // Initialize excluding the pending operations on the dataset
    return dataMetaClient.getActiveTimeline()
        .getInstantsAsStream().filter(i -> !i.isCompleted())
        // regular writers should not be blocked due to pending indexing action
        .filter(i -> !HoodieTimeline.INDEXING_ACTION.equals(i.getAction()))
        .map(HoodieInstant::requestedTime)
        .collect(Collectors.toSet());
  }

  String getTimelineHistoryPath() {
    return TIMELINE_HISTORY_PATH.defaultValue();
  }

  private HoodieTableMetaClient initializeMetaClient() throws IOException {
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setTableName(dataWriteConfig.getTableName() + METADATA_TABLE_NAME_SUFFIX)
        // MT version should match DT, such that same readers can read both.
        .setTableVersion(dataWriteConfig.getWriteVersion())
        .setArchiveLogFolder(getTimelineHistoryPath())
        .setPayloadClassName(HoodieMetadataPayload.class.getName())
        .setBaseFileFormat(HoodieFileFormat.HFILE.toString())
        .setRecordKeyFields(RECORD_KEY_FIELD_NAME)
        .setPopulateMetaFields(DEFAULT_METADATA_POPULATE_META_FIELDS)
        .setKeyGeneratorClassProp(HoodieTableMetadataKeyGenerator.class.getCanonicalName())
        .initTable(storageConf.newInstance(), metadataWriteConfig.getBasePath());

    // reconcile the meta client with time generator config.
    return HoodieTableMetaClient.builder()
        .setBasePath(metadataWriteConfig.getBasePath()).setConf(storageConf.newInstance())
        .setTimeGeneratorConfig(dataWriteConfig.getTimeGeneratorConfig())
        .build();
  }

  /**
   * Function to find hoodie partitions and list files in them in parallel.
   *
   * @param initializationTime Files which have a timestamp after this are neglected
   * @param pendingDataInstants Pending instants on data set
   * @return List consisting of {@code DirectoryInfo} for each partition found.
   */
  private List<DirectoryInfo> listAllPartitionsFromFilesystem(String initializationTime, Set<String> pendingDataInstants) {
    if (dataMetaClient.getActiveTimeline().countInstants() == 0) {
      return Collections.emptyList();
    }
    Queue<StoragePath> pathsToList = new ArrayDeque<>();
    pathsToList.add(new StoragePath(dataWriteConfig.getBasePath()));

    List<DirectoryInfo> partitionsToBootstrap = new LinkedList<>();
    final int fileListingParallelism = metadataWriteConfig.getFileListingParallelism();
    StorageConfiguration<?> storageConf = dataMetaClient.getStorageConf();
    final String dirFilterRegex = dataWriteConfig.getMetadataConfig().getDirectoryFilterRegex();
    StoragePath storageBasePath = dataMetaClient.getBasePath();

    while (!pathsToList.isEmpty()) {
      // In each round we will list a section of directories
      int numDirsToList = Math.min(fileListingParallelism, pathsToList.size());
      List<StoragePath> pathsToProcess = new ArrayList<>(numDirsToList);
      for (int i = 0; i < numDirsToList; i++) {
        pathsToProcess.add(pathsToList.poll());
      }
      // List all directories in parallel
      engineContext.setJobStatus(this.getClass().getSimpleName(), "Listing " + numDirsToList + " partitions from filesystem");
      List<DirectoryInfo> processedDirectories = engineContext.map(pathsToProcess, path -> {
        HoodieStorage storage = new HoodieHadoopStorage(path, storageConf);
        String relativeDirPath = FSUtils.getRelativePartitionPath(storageBasePath, path);
        return new DirectoryInfo(relativeDirPath, storage.listDirectEntries(path), initializationTime, pendingDataInstants);
      }, numDirsToList);

      // If the listing reveals a directory, add it to queue. If the listing reveals a hoodie partition, add it to
      // the results.
      for (DirectoryInfo dirInfo : processedDirectories) {
        if (!dirFilterRegex.isEmpty()) {
          final String relativePath = dirInfo.getRelativePath();
          if (!relativePath.isEmpty() && relativePath.matches(dirFilterRegex)) {
            LOG.info("Ignoring directory {} which matches the filter regex {}", relativePath, dirFilterRegex);
            continue;
          }
        }

        if (dirInfo.isHoodiePartition()) {
          // Add to result
          partitionsToBootstrap.add(dirInfo);
        } else {
          // Add sub-dirs to the queue
          pathsToList.addAll(dirInfo.getSubDirectories());
        }
      }
    }

    return partitionsToBootstrap;
  }

  /**
   * Function to find hoodie partitions and list files in them in parallel from MDT.
   *
   * @param initializationTime Files which have a timestamp after this are neglected
   * @param pendingDataInstants Files coming from pending instants are neglected
   * @return List consisting of {@code DirectoryInfo} for each partition found.
   */
  private List<DirectoryInfo> listAllPartitionsFromMDT(String initializationTime, Set<String> pendingDataInstants) throws IOException {
    List<String> allAbsolutePartitionPaths = metadata.getAllPartitionPaths().stream()
        .map(partitionPath -> dataWriteConfig.getBasePath() + StoragePath.SEPARATOR_CHAR + partitionPath).collect(Collectors.toList());
    Map<String, List<StoragePathInfo>> partitionFileMap = metadata.getAllFilesInPartitions(allAbsolutePartitionPaths);
    List<DirectoryInfo> dirinfoList = new ArrayList<>(partitionFileMap.size());
    for (Map.Entry<String, List<StoragePathInfo>> entry : partitionFileMap.entrySet()) {
      String relativeDirPath = FSUtils.getRelativePartitionPath(new StoragePath(dataWriteConfig.getBasePath()), new StoragePath(entry.getKey()));
      dirinfoList.add(new DirectoryInfo(relativeDirPath, entry.getValue(), initializationTime, pendingDataInstants, false));
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
                                    int fileGroupCount, String partitionName) throws IOException {
    // Remove all existing file groups or leftover files in the partition
    final StoragePath partitionPath = new StoragePath(metadataWriteConfig.getBasePath(), partitionName);
    HoodieStorage storage = metadataMetaClient.getStorage();
    try {
      final List<StoragePathInfo> existingFiles = storage.listDirectEntries(partitionPath);
      if (existingFiles.size() > 0) {
        LOG.warn("Deleting all existing files found in MDT partition {}", partitionName);
        storage.deleteDirectory(partitionPath);
        ValidationUtils.checkState(!storage.exists(partitionPath),
            "Failed to delete MDT partition " + partitionName);
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
        fileGroupCount, partitionName, metadataPartition.getFileIdPrefix(), instantTime);
    LOG.info(msg);
    final List<String> fileGroupFileIds = IntStream.range(0, fileGroupCount)
        .mapToObj(i -> HoodieTableMetadataUtil.getFileIDForFileGroup(metadataPartition, i, partitionName))
        .collect(Collectors.toList());
    ValidationUtils.checkArgument(fileGroupFileIds.size() == fileGroupCount);
    engineContext.setJobStatus(this.getClass().getSimpleName(), msg);
    engineContext.foreach(fileGroupFileIds, fileGroupFileId -> {
      try {
        final Map<HeaderMetadataType, String> blockHeader = Collections.singletonMap(HeaderMetadataType.INSTANT_TIME, instantTime);

        final HoodieDeleteBlock block = new HoodieDeleteBlock(Collections.emptyList(), blockHeader);

        try (HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder()
            .onParentPath(FSUtils.constructAbsolutePath(metadataWriteConfig.getBasePath(), partitionName))
            .withFileId(fileGroupFileId)
            .withInstantTime(instantTime)
            .withLogVersion(HoodieLogFile.LOGFILE_BASE_VERSION)
            .withFileSize(0L)
            .withSizeThreshold(metadataWriteConfig.getLogFileMaxSize())
            .withStorage(dataMetaClient.getStorage())
            .withLogWriteToken(HoodieLogFormat.DEFAULT_WRITE_TOKEN)
            .withTableVersion(metadataWriteConfig.getWriteVersion())
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build()) {
          writer.appendBlock(block);
        }
      } catch (InterruptedException e) {
        throw new HoodieException(String.format("Failed to created fileGroup %s for partition %s", fileGroupFileId, partitionName), e);
      }
    }, fileGroupFileIds.size());
  }

  public void dropMetadataPartitions(List<String> metadataPartitions) throws IOException {
    for (String partitionPath : metadataPartitions) {
      LOG.warn("Deleting Metadata Table partition: {}", partitionPath);
      dataMetaClient.getStorage()
          .deleteDirectory(new StoragePath(metadataWriteConfig.getBasePath(), partitionPath));
      // delete corresponding pending indexing instant file in the timeline
      LOG.warn("Deleting pending indexing instant from the timeline for partition: {}", partitionPath);
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
    InstantGenerator instantGenerator = metaClient.getInstantGenerator();
    metaClient.reloadActiveTimeline().filterPendingIndexTimeline().getInstantsAsStream().filter(instant -> REQUESTED.equals(instant.getState()))
        .forEach(instant -> {
          try {
            HoodieIndexPlan indexPlan = metaClient.getActiveTimeline().readIndexPlan(instant);
            if (indexPlan.getIndexPartitionInfos().stream()
                .anyMatch(indexPartitionInfo -> indexPartitionInfo.getMetadataPartitionPath().equals(partitionPath))) {
              metaClient.getActiveTimeline().deleteInstantFileIfExists(instant);
              metaClient.getActiveTimeline().deleteInstantFileIfExists(instantGenerator.getIndexInflightInstant(instant.requestedTime()));
            }
          } catch (IOException e) {
            LOG.error("Failed to delete the instant file corresponding to {}", instant);
          }
        });
  }

  /**
   * Interface to assist in converting commit metadata to List of HoodieRecords to be written to metadata table.
   * Updates of different commit metadata uses the same method to convert to HoodieRecords and hence.
   */
  interface ConvertMetadataFunction {
    List<Indexer.IndexPartitionData> convertMetadata();
  }

  /**
   * Processes commit metadata from data table and commits to metadata table.
   *
   * @param instantTime             instant time of interest.
   * @param convertMetadataFunction converter function to convert the respective metadata to List of HoodieRecords to be written to metadata table.
   */
  void processAndCommit(String instantTime, ConvertMetadataFunction convertMetadataFunction) {
    Set<String> partitionsToUpdate = getMetadataPartitionsToUpdate();
    if (initialized && metadata != null) {
      // convert metadata and filter only the entries whose partition path are in partitionsToUpdate
      Map<String, HoodieData<HoodieRecord>> partitionRecordsMap = convertMetadataFunction.convertMetadata().stream()
          .filter(entry -> partitionsToUpdate.contains(entry.partitionName()))
          .collect(Collectors.toMap(
              Indexer.IndexPartitionData::partitionName, Indexer.IndexPartitionData::records));
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

  public void buildMetadataPartitions(HoodieEngineContext engineContext, List<HoodieIndexPartitionInfo> indexPartitionInfos, String instantTime) throws IOException {
    if (indexPartitionInfos.isEmpty()) {
      LOG.warn("No partition to index in the plan");
      return;
    }
    String indexUptoInstantTime = indexPartitionInfos.get(0).getIndexUptoInstant();
    List<String> partitionPaths = new ArrayList<>();
    List<MetadataPartitionType> partitionTypes = new ArrayList<>();
    indexPartitionInfos.forEach(indexPartitionInfo -> {
      String relativePartitionPath = indexPartitionInfo.getMetadataPartitionPath();
      LOG.info("Creating a new metadata index for partition '{}' under path {} upto instant {}",
          relativePartitionPath, metadataWriteConfig.getBasePath(), indexUptoInstantTime);

      // return early and populate enabledPartitionTypes correctly (check in initialCommit)
      MetadataPartitionType partitionType = fromPartitionPath(relativePartitionPath);
      if (!enabledIndexBuilderMap.containsKey(partitionType)) {
        throw new HoodieIndexException(String.format("Indexing for metadata partition: %s is not enabled", partitionType));
      }
      partitionTypes.add(partitionType);
      partitionPaths.add(relativePartitionPath);
    });

    // before initialization set these  partitions as inflight in table config
    dataMetaClient.getTableConfig().setMetadataPartitionsInflight(dataMetaClient, partitionPaths);

    // initialize partitions
    initializeFromFilesystem(instantTime, partitionTypes
        .stream().collect(Collectors.toMap(
            Function.identity(),
            enabledIndexBuilderMap::get)), Option.empty());
  }

  /**
   * Update from {@code HoodieCommitMetadata}.
   *
   * @param commitMetadata {@code HoodieCommitMetadata}
   * @param instantTime    Timestamp at which the commit was performed
   */
  @Override
  public void update(HoodieCommitMetadata commitMetadata, String instantTime) {
    mayBeReinitMetadataReader();
    processAndCommit(instantTime, () ->
        // TODO(yihua): directly use indexer map from the constructor instead of
        //  recomputing the records per partition
        // TODO(yihua): Take care of custom partition path to partition type handling
        getMetadataPartitionsToUpdate().stream()
            .map(partition -> MetadataPartitionType.fromPartitionPath(partition))
            .flatMap(partitionType ->
                enabledIndexBuilderMap.get(partitionType)
                    .update(instantTime, getTableMetadata(),
                        Lazy.lazily(this::getMetadataView), commitMetadata).stream())
            .collect(Collectors.toList()));
    //.collect(Collectors.toMap(
    //    Function.identity(),
    //    partition -> enabledIndexBuilderMap.get(MetadataPartitionType.valueOf(partition))
    //        .update(instantTime, getTableMetadata(), Lazy.lazily(this::getMetadataView), commitMetadata))));
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
    mayBeReinitMetadataReader();
    processAndCommit(instantTime, () ->
        // TODO(yihua): revisit the usage of indexer map from the constructor.
        //  Also, a few index is not handled for clean action
        enabledIndexBuilderMap.values().stream()
            .flatMap(indexer -> indexer.clean(instantTime, cleanMetadata).stream())
            .collect(Collectors.toList()));
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
    mayBeReinitMetadataReader();
    dataMetaClient.reloadActiveTimeline();

    // Fetch the commit to restore to (savepointed commit time)
    InstantGenerator datainstantGenerator = dataMetaClient.getInstantGenerator();
    HoodieInstant restoreInstant = datainstantGenerator.createNewInstant(REQUESTED, HoodieTimeline.RESTORE_ACTION, instantTime);
    HoodieInstant requested = datainstantGenerator.getRestoreRequestedInstant(restoreInstant);
    HoodieRestorePlan restorePlan = null;
    try {
      restorePlan = dataMetaClient.getActiveTimeline().readRestorePlan(requested);
    } catch (IOException e) {
      throw new HoodieIOException(String.format("Deserialization of restore plan failed whose restore instant time is %s in data table", instantTime), e);
    }
    final String restoreToInstantTime = restorePlan.getSavepointToRestoreTimestamp();
    LOG.info("Triggering restore to {} in metadata table", restoreToInstantTime);

    // fetch the earliest commit to retain and ensure the base file prior to the time to restore is present
    List<HoodieFileGroup> filesGroups = metadata.getMetadataFileSystemView().getAllFileGroups(FILES.getPartitionPath()).collect(Collectors.toList());

    boolean cannotRestore = filesGroups.stream().map(fileGroup -> fileGroup.getAllFileSlices().map(FileSlice::getBaseInstantTime).anyMatch(
        instantTime1 -> compareTimestamps(instantTime1, LESSER_THAN_OR_EQUALS, restoreToInstantTime))).anyMatch(canRestore -> !canRestore);
    if (cannotRestore) {
      throw new HoodieMetadataException(String.format("Can't restore to %s since there is no base file in MDT lesser than the commit to restore to. "
          + "Please delete metadata table and retry", restoreToInstantTime));
    }

    // Restore requires the existing pipelines to be shutdown. So we can safely scan the dataset to find the current
    // list of files in the filesystem.
    List<DirectoryInfo> dirInfoList = listAllPartitionsFromFilesystem(instantTime, Collections.emptySet());
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
      // or a new timestamp which we use for MDT clean, compaction etc.
      String syncCommitTime = createRestoreInstantTime();
      processAndCommit(syncCommitTime, () ->
          Collections.singletonList(Indexer.IndexPartitionData.of(
              FILES.getPartitionPath(),
              HoodieTableMetadataUtil.convertMissingPartitionRecords(
                  engineContext, partitionsToDelete, partitionFilesToAdd,
                  partitionFilesToDelete, syncCommitTime))));
      closeInternal();
    } catch (IOException e) {
      throw new HoodieMetadataException("IOException during MDT restore sync", e);
    }
  }

  String createRestoreInstantTime() {
    return writeClient.createNewInstantTime(false);
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
      mayBeReinitMetadataReader();
      // The commit which is being rolled back on the dataset
      final String commitToRollbackInstantTime = rollbackMetadata.getCommitsRollback().get(0);
      // The deltacommit that will be rolled back
      HoodieInstant deltaCommitInstant = metadataMetaClient.createNewInstant(HoodieInstant.State.COMPLETED,
          HoodieTimeline.DELTA_COMMIT_ACTION, commitToRollbackInstantTime);
      if (metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().containsInstant(deltaCommitInstant)) {
        validateRollback(commitToRollbackInstantTime);
        LOG.info("Rolling back MDT deltacommit {}", commitToRollbackInstantTime);
        if (!getWriteClient().rollback(commitToRollbackInstantTime, instantTime)) {
          throw new HoodieMetadataException(String.format("Failed to rollback deltacommit at %s", commitToRollbackInstantTime));
        }
      } else {
        // if the instant is pending on MDT or not even exists the timeline, just ignore.
        LOG.info("Ignoring rollback of instant {} at {}. The commit to rollback is not found in MDT",
            commitToRollbackInstantTime, instantTime);
      }
      closeInternal();
    }
  }

  private void validateRollback(String commitToRollbackInstantTime) {
    // Find the deltacommits since the last compaction
    Option<Pair<HoodieTimeline, HoodieInstant>> deltaCommitsInfo =
        CompactionUtils.getDeltaCommitsSinceLatestCompaction(metadataMetaClient.getActiveTimeline());

    // This could be a compaction or deltacommit instant (See CompactionUtils.getDeltaCommitsSinceLatestCompaction)
    HoodieInstant compactionInstant = deltaCommitsInfo.get().getValue();
    HoodieTimeline deltacommitsSinceCompaction = deltaCommitsInfo.get().getKey();

    // The commit being rolled back should not be earlier than the latest compaction on the MDT because the latest file slice does not change after all.
    // Compaction on MDT only occurs when all actions are completed on the dataset.
    // Hence, this case implies a rollback of completed commit which should actually be handled using restore.
    if (compactionInstant.getAction().equals(COMMIT_ACTION)) {
      final String compactionInstantTime = compactionInstant.requestedTime();
      if (commitToRollbackInstantTime.length() == compactionInstantTime.length() && LESSER_THAN_OR_EQUALS.test(commitToRollbackInstantTime, compactionInstantTime)) {
        throw new HoodieMetadataException(
            String.format("Commit being rolled back %s is earlier than the latest compaction %s. There are %d deltacommits after this compaction: %s",
                commitToRollbackInstantTime, compactionInstantTime, deltacommitsSinceCompaction.countInstants(), deltacommitsSinceCompaction.getInstants())
        );
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
    if (metadataView != null) {
      metadataView.close();
      metadataView = null;
    }
  }

  /**
   * Commit the {@code HoodieRecord}s to Metadata Table as a new delta-commit.
   *
   * @param instantTime         - Action instant time for this commit
   * @param partitionRecordsMap - Map of partition type to its records to commit
   */
  protected abstract void commit(String instantTime, Map<String, HoodieData<HoodieRecord>> partitionRecordsMap);

  /**
   * Converts the input records to the input format expected by the write client.
   *
   * @param records records to be converted
   * @return converted records
   */
  protected abstract I convertHoodieDataToEngineSpecificData(HoodieData<HoodieRecord> records);

  protected void commitInternal(String instantTime, Map<String, HoodieData<HoodieRecord>> partitionRecordsMap, boolean isInitializing,
                                Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    ValidationUtils.checkState(metadataMetaClient != null, "Metadata table is not fully initialized yet.");
    HoodieData<HoodieRecord> preppedRecords = prepRecords(partitionRecordsMap);
    I preppedRecordInputs = convertHoodieDataToEngineSpecificData(preppedRecords);

    BaseHoodieWriteClient<?, I, ?, ?> writeClient = getWriteClient();
    // rollback partially failed writes if any.
    metadataMetaClient = rollbackFailedWrites(dataWriteConfig, writeClient, metadataMetaClient);

    if (!metadataMetaClient.getActiveTimeline().getCommitsTimeline().containsInstant(instantTime)) {
      // if this is a new commit being applied to metadata for the first time
      LOG.info("New commit at {} being applied to MDT.", instantTime);
    } else {
      // this code path refers to a re-attempted commit that:
      //   1. got committed to metadata table, but failed in datatable.
      //   2. failed while committing to metadata table
      // for e.g., let's say compaction c1 on 1st attempt succeeded in metadata table and failed before committing to datatable.
      // when retried again, data table will first rollback pending compaction. these will be applied to metadata table, but all changes
      // are upserts to metadata table and so only a new delta commit will be created.
      // once rollback is complete in datatable, compaction will be retried again, which will eventually hit this code block where the respective commit is
      // already part of completed commit. So, we have to manually rollback the completed instant and proceed.
      Option<HoodieInstant> alreadyCompletedInstant = metadataMetaClient.getActiveTimeline().filterCompletedInstants().filter(entry -> entry.requestedTime().equals(instantTime))
          .lastInstant();
      LOG.info("{} completed commit at {} being applied to MDT.",
          alreadyCompletedInstant.isPresent() ? "Already" : "Partially", instantTime);

      // Rollback the previous commit
      if (!writeClient.rollback(instantTime)) {
        throw new HoodieMetadataException(String.format("Failed to rollback deltacommit at %s from MDT", instantTime));
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
   * Rolls back any failed writes if cleanup policy is EAGER. If any writes were cleaned up, the meta client is reloaded.
   * @param dataWriteConfig write config for the data table
   * @param writeClient write client for the metadata table
   * @param metadataMetaClient meta client for the metadata table
   */
  static <I> HoodieTableMetaClient rollbackFailedWrites(HoodieWriteConfig dataWriteConfig, BaseHoodieWriteClient<?, I, ?, ?> writeClient, HoodieTableMetaClient metadataMetaClient) {
    if (dataWriteConfig.getFailedWritesCleanPolicy().isEager() && writeClient.rollbackFailedWrites(metadataMetaClient)) {
      metadataMetaClient = HoodieTableMetaClient.reload(metadataMetaClient);
    }
    return metadataMetaClient;
  }

  /**
   * Allows the implementation to perform any pre-commit operations like transitioning a commit to inflight if required.
   *
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
   * @param instantTime    Action instant time for this commit
   * @param partitionName  MDT partition to which records are to be committed. The relative path of the partition is same as the partition name.
   *                       For functional and secondary indexes, partition name is the index name, which is recorded in index definition.
   * @param records        records to be bulk inserted
   * @param fileGroupCount The maximum number of file groups to which the records will be written.
   */
  protected abstract void bulkCommit(
      String instantTime, String partitionName, HoodieData<HoodieRecord> records,
      int fileGroupCount);

  /**
   * Tag each record with the location in the given partition.
   * The record is tagged with respective file slice's location based on its record key.
   */
  protected HoodieData<HoodieRecord> prepRecords(Map<String, HoodieData<HoodieRecord>> partitionRecordsMap) {
    // The result set
    HoodieData<HoodieRecord> allPartitionRecords = engineContext.emptyHoodieData();
    try (HoodieTableFileSystemView fsView = HoodieTableMetadataUtil.getFileSystemViewForMetadataTable(metadataMetaClient)) {
      for (Map.Entry<String, HoodieData<HoodieRecord>> entry : partitionRecordsMap.entrySet()) {
        final String partitionName = entry.getKey();
        HoodieData<HoodieRecord> records = entry.getValue();
        List<FileSlice> fileSlices =
            HoodieTableMetadataUtil.getPartitionLatestFileSlices(metadataMetaClient, Option.ofNullable(fsView), partitionName);
        if (fileSlices.isEmpty()) {
          // scheduling of INDEX only initializes the file group and not add commit
          // so if there are no committed file slices, look for inflight slices
          fileSlices = getPartitionLatestFileSlicesIncludingInflight(metadataMetaClient, Option.ofNullable(fsView), partitionName);
        }
        final int fileGroupCount = fileSlices.size();
        ValidationUtils.checkArgument(fileGroupCount > 0, String.format("FileGroup count for MDT partition %s should be > 0", partitionName));

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
  public void performTableServices(Option<String> inFlightInstantTimestamp, boolean requiresTimelineRefresh) {
    HoodieTimer metadataTableServicesTimer = HoodieTimer.start();
    boolean allTableServicesExecutedSuccessfullyOrSkipped = true;
    BaseHoodieWriteClient<?, I, ?, ?> writeClient = getWriteClient();
    try {
      // Run any pending table services operations and return the active timeline
      HoodieActiveTimeline activeTimeline = runPendingTableServicesOperationsAndRefreshTimeline(metadataMetaClient, writeClient, requiresTimelineRefresh);

      Option<HoodieInstant> lastInstant = activeTimeline.getDeltaCommitTimeline()
          .filterCompletedInstants()
          .lastInstant();
      if (!lastInstant.isPresent()) {
        return;
      }
      // Check and run clean operations.
      cleanIfNecessary(writeClient, lastInstant.get().requestedTime());
      // Do timeline validation before scheduling compaction/logCompaction operations.
      if (validateCompactionScheduling(inFlightInstantTimestamp, lastInstant.get().requestedTime())) {
        String latestDeltacommitTime = lastInstant.get().requestedTime();
        LOG.info("Latest deltacommit time found is {}, running compaction operations.", latestDeltacommitTime);
        compactIfNecessary(writeClient, Option.of(latestDeltacommitTime));
      }
      writeClient.archive();
      LOG.info("All the table services operations on MDT completed successfully");
    } catch (Exception e) {
      LOG.error("Exception in running table services on metadata table", e);
      allTableServicesExecutedSuccessfullyOrSkipped = false;
      throw e;
    } finally {
      String metadataTableName = writeClient.getConfig().getTableName();
      boolean tableNameExists = StringUtils.nonEmpty(metadataTableName);
      String executionDurationMetricName = tableNameExists
              ? String.format("%s.%s", metadataTableName, HoodieMetadataMetrics.TABLE_SERVICE_EXECUTION_DURATION)
              : HoodieMetadataMetrics.TABLE_SERVICE_EXECUTION_DURATION;
      String executionStatusMetricName = tableNameExists
              ? String.format("%s.%s", metadataTableName, HoodieMetadataMetrics.TABLE_SERVICE_EXECUTION_STATUS)
              : HoodieMetadataMetrics.TABLE_SERVICE_EXECUTION_STATUS;
      long timeSpent = metadataTableServicesTimer.endTimer();
      metrics.ifPresent(m -> m.setMetric(executionDurationMetricName, timeSpent));
      if (allTableServicesExecutedSuccessfullyOrSkipped) {
        metrics.ifPresent(m -> m.setMetric(executionStatusMetricName, 1));
      } else {
        metrics.ifPresent(m -> m.setMetric(executionStatusMetricName, -1));
      }
    }
  }

  static HoodieActiveTimeline runPendingTableServicesOperationsAndRefreshTimeline(HoodieTableMetaClient metadataMetaClient,
                                                                                  BaseHoodieWriteClient<?, ?, ?, ?> writeClient,
                                                                                  boolean initialTimelineRequiresRefresh) {
    HoodieActiveTimeline activeTimeline = initialTimelineRequiresRefresh ? metadataMetaClient.reloadActiveTimeline() : metadataMetaClient.getActiveTimeline();
    // finish off any pending log compaction or compactions operations if any from previous attempt.
    boolean ranServices = false;
    if (activeTimeline.filterPendingCompactionTimeline().countInstants() > 0) {
      writeClient.runAnyPendingCompactions();
      ranServices = true;
    }
    if (activeTimeline.filterPendingLogCompactionTimeline().countInstants() > 0) {
      writeClient.runAnyPendingLogCompactions();
      ranServices = true;
    }
    return ranServices ? metadataMetaClient.reloadActiveTimeline() : activeTimeline;
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
  void compactIfNecessary(BaseHoodieWriteClient<?,I,?,?> writeClient, Option<String> latestDeltaCommitTimeOpt) {
    // IMPORTANT: Trigger compaction with max instant time that is smaller than(or equals) the earliest pending instant from DT.
    // The compaction planner will manage to filter out the log files that finished with greater completion time.
    // see BaseHoodieCompactionPlanGenerator.generateCompactionPlan for more details.
    HoodieTimeline metadataCompletedTimeline = metadataMetaClient.getActiveTimeline().filterCompletedInstants();
    final String compactionInstantTime = dataMetaClient.reloadActiveTimeline()
        // The filtering strategy is kept in line with the rollback premise, if an instant is pending on DT but completed on MDT,
        // generates a compaction time smaller than it so that the instant could then been rolled back.
        .filterInflightsAndRequested().filter(instant -> metadataCompletedTimeline.containsInstant(instant.requestedTime())).firstInstant()
        // minus the pending instant time by 1 millisecond to avoid conflicts on the MDT.
        .map(instant -> HoodieInstantTimeGenerator.instantTimeMinusMillis(instant.requestedTime(), 1L))
        .orElse(writeClient.createNewInstantTime(false));

    // we need to avoid checking compaction w/ same instant again.
    // let's say we trigger compaction after C5 in MDT and so compaction completes with C4001. but C5 crashed before completing in MDT.
    // and again w/ C6, we will re-attempt compaction at which point latest delta commit is C4 in MDT.
    // and so we try compaction w/ instant C4001. So, we can avoid compaction if we already have compaction w/ same instant time.
    if (metadataMetaClient.getActiveTimeline().filterCompletedInstants().containsInstant(compactionInstantTime)) {
      LOG.info("Compaction with same {} time is already present in the timeline.", compactionInstantTime);
    } else if (writeClient.scheduleCompactionAtInstant(compactionInstantTime, Option.empty())) {
      LOG.info("Compaction is scheduled for timestamp {}", compactionInstantTime);
      writeClient.compact(compactionInstantTime);
    } else if (metadataWriteConfig.isLogCompactionEnabled()) {
      // Schedule and execute log compaction with new instant time.
      final String logCompactionInstantTime = metadataMetaClient.createNewInstantTime(false);
      if (metadataMetaClient.getActiveTimeline().filterCompletedInstants().containsInstant(logCompactionInstantTime)) {
        LOG.info("Log compaction with same {} time is already present in the timeline.", logCompactionInstantTime);
      } else if (writeClient.scheduleLogCompactionAtInstant(logCompactionInstantTime, Option.empty())) {
        LOG.info("Log compaction is scheduled for timestamp {}", logCompactionInstantTime);
        writeClient.logCompact(logCompactionInstantTime);
      }
    }
  }

  protected void cleanIfNecessary(BaseHoodieWriteClient writeClient, String instantTime) {
    Option<HoodieInstant> lastCompletedCompactionInstant = metadataMetaClient.getActiveTimeline()
        .getCommitAndReplaceTimeline().filterCompletedInstants().lastInstant();
    if (lastCompletedCompactionInstant.isPresent()
        && metadataMetaClient.getActiveTimeline().filterCompletedInstants()
        .findInstantsAfter(lastCompletedCompactionInstant.get().requestedTime()).countInstants() < 3) {
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
    writeClient.clean(createCleanInstantTime(instantTime));
    writeClient.lazyRollbackFailedIndexing();
  }

  String createCleanInstantTime(String instantTime) {
    return metadataMetaClient.createNewInstantTime(false);
  }

  /**
   * Validates the timeline for both main and metadata tables to ensure compaction on MDT can be scheduled.
   */
  boolean validateCompactionScheduling(Option<String> inFlightInstantTimestamp, String latestDeltaCommitTimeInMetadataTable) {
    // Under the log compaction scope, the sequence of the log-compaction and compaction needs to be ensured because metadata items such as RLI
    // only has proc-time ordering semantics. For "ensured", it means the completion sequence of the log-compaction/compaction is the same as the start sequence.
    if (metadataWriteConfig.isLogCompactionEnabled()) {
      Option<HoodieInstant> pendingLogCompactionInstant =
          metadataMetaClient.getActiveTimeline().filterPendingLogCompactionTimeline().firstInstant();
      Option<HoodieInstant> pendingCompactionInstant =
          metadataMetaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant();
      if (pendingLogCompactionInstant.isPresent() || pendingCompactionInstant.isPresent()) {
        LOG.warn("Not scheduling compaction or logCompaction, since a pending compaction instant {} or logCompaction {} instant is present",
            pendingCompactionInstant, pendingLogCompactionInstant);
        return false;
      }
    }
    return true;
  }

  private void fetchOutofSyncFilesRecordsFromMetadataTable(Map<String, DirectoryInfo> dirInfoMap, Map<String, Map<String, Long>> partitionFilesToAdd,
                                                           Map<String, List<String>> partitionFilesToDelete, List<String> partitionsToDelete) throws IOException {

    for (String partition : metadata.fetchAllPartitionPaths()) {
      StoragePath partitionPath = null;
      if (StringUtils.isNullOrEmpty(partition) && !dataMetaClient.getTableConfig().isTablePartitioned()) {
        partitionPath = new StoragePath(dataWriteConfig.getBasePath());
      } else {
        partitionPath = new StoragePath(dataWriteConfig.getBasePath(), partition);
      }
      final String partitionId = HoodieTableMetadataUtil.getPartitionIdentifierForFilesPartition(partition);
      List<StoragePathInfo> metadataFiles = metadata.getAllFilesInPartition(partitionPath);
      if (!dirInfoMap.containsKey(partition)) {
        // Entire partition has been deleted
        partitionsToDelete.add(partitionId);
        if (metadataFiles != null && metadataFiles.size() > 0) {
          partitionFilesToDelete.put(partitionId, metadataFiles.stream().map(f -> f.getPath().getName()).collect(Collectors.toList()));
        }
      } else {
        // Some files need to be cleaned and some to be added in the partition
        Map<String, Long> fsFiles = dirInfoMap.get(partition).getFileNameToSizeMap();
        List<String> mdtFiles = metadataFiles.stream().map(mdtFile -> mdtFile.getPath().getName()).collect(Collectors.toList());
        List<String> filesDeleted = metadataFiles.stream().map(f -> f.getPath().getName())
            .filter(n -> !fsFiles.containsKey(n)).collect(Collectors.toList());
        Map<String, Long> filesToAdd = new HashMap<>();
        // new files could be added to DT due to restore that just happened which may not be tracked in RestoreMetadata.
        dirInfoMap.get(partition).getFileNameToSizeMap().forEach((k, v) -> {
          if (!mdtFiles.contains(k)) {
            filesToAdd.put(k, v);
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
}
