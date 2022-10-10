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
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.config.metrics.HoodieMetricsGraphiteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsJmxConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieMetadataException;

import org.apache.avro.specific.SpecificRecordBase;
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
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.getIndexInflightInstant;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeIndexPlan;
import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;
import static org.apache.hudi.metadata.HoodieTableMetadata.METADATA_TABLE_NAME_SUFFIX;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getInflightAndCompletedMetadataPartitions;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getInflightMetadataPartitions;

/**
 * Writer implementation backed by an internal hudi table. Partition and file listing are saved within an internal MOR table
 * called Metadata Table. This table is created by listing files and partitions (first time)
 * and kept in sync using the instants on the main dataset.
 */
public abstract class HoodieBackedTableMetadataWriter implements HoodieTableMetadataWriter {

  private static final Logger LOG = LogManager.getLogger(HoodieBackedTableMetadataWriter.class);

  // Virtual keys support for metadata table. This Field is
  // from the metadata payload schema.
  private static final String RECORD_KEY_FIELD_NAME = HoodieMetadataPayload.KEY_FIELD_NAME;

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
  protected final List<MetadataPartitionType> enabledPartitionTypes;

  /**
   * Hudi backed table metadata writer.
   *
   * @param hadoopConf               - Hadoop configuration to use for the metadata writer
   * @param writeConfig              - Writer config
   * @param engineContext            - Engine context
   * @param actionMetadata           - Optional action metadata to help decide initialize operations
   * @param <T>                      - Action metadata types extending Avro generated SpecificRecordBase
   * @param inflightInstantTimestamp - Timestamp of any instant in progress
   */
  protected <T extends SpecificRecordBase> HoodieBackedTableMetadataWriter(Configuration hadoopConf,
                                                                           HoodieWriteConfig writeConfig,
                                                                           HoodieEngineContext engineContext,
                                                                           Option<T> actionMetadata,
                                                                           Option<String> inflightInstantTimestamp) {
    this.dataWriteConfig = writeConfig;
    this.engineContext = engineContext;
    this.hadoopConf = new SerializableConfiguration(hadoopConf);
    this.metrics = Option.empty();
    this.enabledPartitionTypes = new ArrayList<>();

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

      this.dataMetaClient =
          HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(dataWriteConfig.getBasePath()).build();
      enablePartitions();
      initRegistry();
      initialize(engineContext, actionMetadata, inflightInstantTimestamp);
      initTableMetadata();
    } else {
      enabled = false;
    }
  }

  public HoodieBackedTableMetadataWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig,
                                         HoodieEngineContext engineContext) {
    this(hadoopConf, writeConfig, engineContext, Option.empty(), Option.empty());
  }

  /**
   * Enable metadata table partitions based on config.
   */
  private void enablePartitions() {
    final HoodieMetadataConfig metadataConfig = dataWriteConfig.getMetadataConfig();
    boolean isBootstrapCompleted;
    Option<HoodieTableMetaClient> metaClient = Option.empty();
    try {
      isBootstrapCompleted = dataMetaClient.getFs().exists(new Path(metadataWriteConfig.getBasePath(), HoodieTableMetaClient.METAFOLDER_NAME));
      if (isBootstrapCompleted) {
        metaClient = Option.of(HoodieTableMetaClient.builder().setConf(hadoopConf.get())
            .setBasePath(metadataWriteConfig.getBasePath()).build());
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to enable metadata partitions!", e);
    }

    Option<HoodieTableFileSystemView> fsView = Option.ofNullable(
        metaClient.isPresent() ? HoodieTableMetadataUtil.getFileSystemView(metaClient.get()) : null);
    enablePartition(MetadataPartitionType.FILES, metadataConfig, metaClient, fsView, isBootstrapCompleted);
    if (metadataConfig.isBloomFilterIndexEnabled()) {
      enablePartition(MetadataPartitionType.BLOOM_FILTERS, metadataConfig, metaClient, fsView, isBootstrapCompleted);
    }
    if (metadataConfig.isColumnStatsIndexEnabled()) {
      enablePartition(MetadataPartitionType.COLUMN_STATS, metadataConfig, metaClient, fsView, isBootstrapCompleted);
    }
  }

  /**
   * Enable metadata table partition.
   *
   * @param partitionType        - Metadata table partition type
   * @param metadataConfig       - Table config
   * @param metaClient           - Meta client for the metadata table
   * @param fsView               - Metadata table filesystem view to use
   * @param isBootstrapCompleted - Is metadata table initializing completed
   */
  private void enablePartition(final MetadataPartitionType partitionType, final HoodieMetadataConfig metadataConfig,
                               final Option<HoodieTableMetaClient> metaClient, Option<HoodieTableFileSystemView> fsView, boolean isBootstrapCompleted) {
    final int fileGroupCount = HoodieTableMetadataUtil.getPartitionFileGroupCount(partitionType, metaClient, fsView,
        metadataConfig, isBootstrapCompleted);
    partitionType.setFileGroupCount(fileGroupCount);
    this.enabledPartitionTypes.add(partitionType);
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
        .withMarkersType(MarkerType.DIRECT.name())
        .withRollbackUsingMarkers(false)
        .withPath(HoodieTableMetadata.getMetadataTableBasePath(writeConfig.getBasePath()))
        .withSchema(HoodieMetadataRecord.getClassSchema().toString())
        .forTable(tableName)
        // we will trigger cleaning manually, to control the instant times
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withAsyncClean(writeConfig.isMetadataAsyncClean())
            .withAutoClean(false)
            .withCleanerParallelism(parallelism)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .retainCommits(writeConfig.getMetadataCleanerCommitsRetained())
            .build())
        // we will trigger archive manually, to ensure only regular writer invokes it
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(minCommitsToKeep, maxCommitsToKeep)
            .withAutoArchive(false)
            .build())
        // we will trigger compaction manually, to control the instant times
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(writeConfig.getMetadataCompactDeltaCommitMax())
            // by default, the HFile does not keep the metadata fields, set up as false
            // to always use the metadata of the new record.
            .withPreserveCommitMetadata(false)
            .withLogRecordReaderScanV2(String.valueOf(writeConfig.useScanV2ForLogRecordReader()))
            .build())
        .withParallelism(parallelism, parallelism)
        .withDeleteParallelism(parallelism)
        .withRollbackParallelism(parallelism)
        .withFinalizeWriteParallelism(parallelism)
        .withAllowMultiWriteOnSameInstant(true)
        .withKeyGenerator(HoodieTableMetadataKeyGenerator.class.getCanonicalName())
        .withPopulateMetaFields(dataWriteConfig.getMetadataConfig().populateMetaFields());

    // RecordKey properties are needed for the metadata table records
    final Properties properties = new Properties();
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), RECORD_KEY_FIELD_NAME);
    properties.put("hoodie.datasource.write.recordkey.field", RECORD_KEY_FIELD_NAME);
    builder.withProperties(properties);

    if (writeConfig.isMetricsOn()) {
      // Table Name is needed for metric reporters prefix
      Properties commonProperties = new Properties();
      commonProperties.put(HoodieWriteConfig.TBL_NAME.key(), tableName);

      builder.withMetricsConfig(HoodieMetricsConfig.newBuilder()
          .fromProperties(commonProperties)
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
        case CLOUDWATCH:
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

  public HoodieBackedTableMetadata getTableMetadata() {
    return metadata;
  }

  public List<MetadataPartitionType> getEnabledPartitionTypes() {
    return this.enabledPartitionTypes;
  }

  /**
   * Initialize the metadata table if it does not exist.
   * <p>
   * If the metadata table does not exist, then file and partition listing is used to initialize the table.
   *
   * @param engineContext
   * @param actionMetadata           Action metadata types extending Avro generated SpecificRecordBase
   * @param inflightInstantTimestamp Timestamp of an instant in progress on the dataset. This instant is ignored
   *                                 while deciding to initialize the metadata table.
   */
  protected abstract <T extends SpecificRecordBase> void initialize(HoodieEngineContext engineContext,
                                                                    Option<T> actionMetadata,
                                                                    Option<String> inflightInstantTimestamp);

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
   * Initialize the metadata table if needed.
   *
   * @param dataMetaClient           - meta client for the data table
   * @param actionMetadata           - optional action metadata
   * @param inflightInstantTimestamp - timestamp of an instant in progress on the dataset
   * @param <T>                      - action metadata types extending Avro generated SpecificRecordBase
   * @throws IOException
   */
  protected <T extends SpecificRecordBase> void initializeIfNeeded(HoodieTableMetaClient dataMetaClient,
                                                                   Option<T> actionMetadata,
                                                                   Option<String> inflightInstantTimestamp) throws IOException {
    HoodieTimer timer = new HoodieTimer().startTimer();

    boolean exists = metadataTableExists(dataMetaClient, actionMetadata);

    if (!exists) {
      // Initialize for the first time by listing partitions and files directly from the file system
      if (initializeFromFilesystem(dataMetaClient, inflightInstantTimestamp)) {
        metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.INITIALIZE_STR, timer.endTimer()));
      }
      return;
    }

    // if metadata table exists, then check if any of the enabled partition types needs to be initialized
    // NOTE: It needs to be guarded by async index config because if that is enabled then initialization happens through the index scheduler.
    if (!dataWriteConfig.isMetadataAsyncIndex()) {
      Set<String> inflightAndCompletedPartitions = getInflightAndCompletedMetadataPartitions(dataMetaClient.getTableConfig());
      LOG.info("Async metadata indexing enabled and following partitions already initialized: " + inflightAndCompletedPartitions);
      List<MetadataPartitionType> partitionsToInit = this.enabledPartitionTypes.stream()
          .filter(p -> !inflightAndCompletedPartitions.contains(p.getPartitionPath()) && !MetadataPartitionType.FILES.equals(p))
          .collect(Collectors.toList());
      // if there are no partitions to initialize or there is a pending operation, then don't initialize in this round
      if (partitionsToInit.isEmpty() || anyPendingDataInstant(dataMetaClient, inflightInstantTimestamp)) {
        return;
      }

      String createInstantTime = getInitialCommitInstantTime(dataMetaClient);
      initTableMetadata(); // re-init certain flags in BaseTableMetadata
      initializeEnabledFileGroups(dataMetaClient, createInstantTime, partitionsToInit);
      initialCommit(createInstantTime, partitionsToInit);
      updateInitializedPartitionsInTableConfig(partitionsToInit);
    }
  }

  private <T extends SpecificRecordBase> boolean metadataTableExists(HoodieTableMetaClient dataMetaClient,
                                                                     Option<T> actionMetadata) throws IOException {
    boolean exists = dataMetaClient.getFs().exists(new Path(metadataWriteConfig.getBasePath(),
        HoodieTableMetaClient.METAFOLDER_NAME));
    boolean reInitialize = false;

    // If the un-synced instants have been archived, then
    // the metadata table will need to be initialized again.
    if (exists) {
      HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf.get())
          .setBasePath(metadataWriteConfig.getBasePath()).build();

      if (dataWriteConfig.getMetadataConfig().populateMetaFields() != metadataMetaClient.getTableConfig().populateMetaFields()) {
        LOG.info("Re-initiating metadata table properties since populate meta fields have changed");
        metadataMetaClient = initializeMetaClient(dataWriteConfig.getMetadataConfig().populateMetaFields());
      }

      final Option<HoodieInstant> latestMetadataInstant =
          metadataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();

      reInitialize = isBootstrapNeeded(latestMetadataInstant, actionMetadata);
    }

    if (reInitialize) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.REBOOTSTRAP_STR, 1));
      LOG.info("Deleting Metadata Table directory so that it can be re-initialized");
      dataMetaClient.getFs().delete(new Path(metadataWriteConfig.getBasePath()), true);
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
   * @return True if the initialize is not needed, False otherwise
   */
  private <T extends SpecificRecordBase> boolean isBootstrapNeeded(Option<HoodieInstant> latestMetadataInstant,
                                                                   Option<T> actionMetadata) {
    if (!latestMetadataInstant.isPresent()) {
      LOG.warn("Metadata Table will need to be re-initialized as no instants were found");
      return true;
    }

    final String latestMetadataInstantTimestamp = latestMetadataInstant.get().getTimestamp();
    if (latestMetadataInstantTimestamp.equals(SOLO_COMMIT_TIMESTAMP)) {
      return false;
    }

    // Detect the commit gaps if any from the data and the metadata active timeline
    if (dataMetaClient.getActiveTimeline().getAllCommitsTimeline().isBeforeTimelineStarts(
        latestMetadataInstant.get().getTimestamp())
        && !isCommitRevertedByInFlightAction(actionMetadata, latestMetadataInstantTimestamp)) {
      LOG.error("Metadata Table will need to be re-initialized as un-synced instants have been archived."
          + " latestMetadataInstant=" + latestMetadataInstant.get().getTimestamp()
          + ", latestDataInstant=" + dataMetaClient.getActiveTimeline().firstInstant().get().getTimestamp());
      return true;
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
        List<HoodieInstantInfo> rollbackedInstants =
            ((HoodieRollbackMetadata) actionMetadata.get()).getInstantsRollback();
        affectedInstantTimestamps = rollbackedInstants.stream().map(HoodieInstantInfo::getCommitTime).collect(Collectors.toList());

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
   * @param dataMetaClient           - {@code HoodieTableMetaClient} for the dataset.
   * @param inflightInstantTimestamp - Current action instant responsible for this initialization
   */
  private boolean initializeFromFilesystem(HoodieTableMetaClient dataMetaClient,
                                           Option<String> inflightInstantTimestamp) throws IOException {
    if (anyPendingDataInstant(dataMetaClient, inflightInstantTimestamp)) {
      return false;
    }

    String createInstantTime = getInitialCommitInstantTime(dataMetaClient);

    initializeMetaClient(dataWriteConfig.getMetadataConfig().populateMetaFields());
    initTableMetadata();
    // if async metadata indexing is enabled,
    // then only initialize files partition as other partitions will be built using HoodieIndexer
    List<MetadataPartitionType> enabledPartitionTypes =  new ArrayList<>();
    if (dataWriteConfig.isMetadataAsyncIndex()) {
      enabledPartitionTypes.add(MetadataPartitionType.FILES);
    } else {
      // all enabled ones should be initialized
      enabledPartitionTypes = this.enabledPartitionTypes;
    }
    initializeEnabledFileGroups(dataMetaClient, createInstantTime, enabledPartitionTypes);
    initialCommit(createInstantTime, enabledPartitionTypes);
    updateInitializedPartitionsInTableConfig(enabledPartitionTypes);
    return true;
  }

  private String getInitialCommitInstantTime(HoodieTableMetaClient dataMetaClient) {
    // If there is no commit on the dataset yet, use the SOLO_COMMIT_TIMESTAMP as the instant time for initial commit
    // Otherwise, we use the timestamp of the latest completed action.
    String createInstantTime = dataMetaClient.getActiveTimeline().filterCompletedInstants()
        .getReverseOrderedInstants().findFirst().map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);
    LOG.info("Creating a new metadata table in " + metadataWriteConfig.getBasePath() + " at instant " + createInstantTime);
    return createInstantTime;
  }

  private boolean anyPendingDataInstant(HoodieTableMetaClient dataMetaClient, Option<String> inflightInstantTimestamp) {
    ValidationUtils.checkState(enabled, "Metadata table cannot be initialized as it is not enabled");

    // We can only initialize if there are no pending operations on the dataset
    List<HoodieInstant> pendingDataInstant = dataMetaClient.getActiveTimeline()
        .getInstants().filter(i -> !i.isCompleted())
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

  private void updateInitializedPartitionsInTableConfig(List<MetadataPartitionType> partitionTypes) {
    Set<String> completedPartitions = dataMetaClient.getTableConfig().getMetadataPartitions();
    completedPartitions.addAll(partitionTypes.stream().map(MetadataPartitionType::getPartitionPath).collect(Collectors.toSet()));
    dataMetaClient.getTableConfig().setValue(HoodieTableConfig.TABLE_METADATA_PARTITIONS.key(), String.join(",", completedPartitions));
    HoodieTableConfig.update(dataMetaClient.getFs(), new Path(dataMetaClient.getMetaPath()), dataMetaClient.getTableConfig().getProps());
  }

  private HoodieTableMetaClient initializeMetaClient(boolean populateMetaFields) throws IOException {
    return HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setTableName(tableName)
        .setArchiveLogFolder(ARCHIVELOG_FOLDER.defaultValue())
        .setPayloadClassName(HoodieMetadataPayload.class.getName())
        .setBaseFileFormat(HoodieFileFormat.HFILE.toString())
        .setRecordKeyFields(RECORD_KEY_FIELD_NAME)
        .setPopulateMetaFields(populateMetaFields)
        .setKeyGeneratorClassProp(HoodieTableMetadataKeyGenerator.class.getCanonicalName())
        .initTable(hadoopConf.get(), metadataWriteConfig.getBasePath());
  }

  /**
   * Function to find hoodie partitions and list files in them in parallel.
   *
   * @param datasetMetaClient data set meta client instance.
   * @return Map of partition names to a list of FileStatus for all the files in the partition
   */
  private List<DirectoryInfo> listAllPartitions(HoodieTableMetaClient datasetMetaClient) {
    List<Path> pathsToList = new LinkedList<>();
    pathsToList.add(new Path(dataWriteConfig.getBasePath()));

    List<DirectoryInfo> partitionsToBootstrap = new LinkedList<>();
    final int fileListingParallelism = metadataWriteConfig.getFileListingParallelism();
    SerializableConfiguration conf = new SerializableConfiguration(datasetMetaClient.getHadoopConf());
    final String dirFilterRegex = dataWriteConfig.getMetadataConfig().getDirectoryFilterRegex();
    final String datasetBasePath = datasetMetaClient.getBasePath();

    while (!pathsToList.isEmpty()) {
      // In each round we will list a section of directories
      int numDirsToList = Math.min(fileListingParallelism, pathsToList.size());
      // List all directories in parallel
      List<DirectoryInfo> processedDirectories = engineContext.map(pathsToList.subList(0, numDirsToList), path -> {
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
   * Initialize file groups for all the enabled partition types.
   *
   * @param dataMetaClient    - Meta client for the data table
   * @param createInstantTime - Metadata table create instant time
   * @throws IOException
   */
  private void initializeEnabledFileGroups(HoodieTableMetaClient dataMetaClient, String createInstantTime, List<MetadataPartitionType> partitionTypes) throws IOException {
    for (MetadataPartitionType enabledPartitionType : partitionTypes) {
      initializeFileGroups(dataMetaClient, enabledPartitionType, createInstantTime,
          enabledPartitionType.getFileGroupCount());
    }
  }

  public void initializeMetadataPartitions(HoodieTableMetaClient dataMetaClient, List<MetadataPartitionType> metadataPartitions, String instantTime) throws IOException {
    for (MetadataPartitionType partitionType : metadataPartitions) {
      initializeFileGroups(dataMetaClient, partitionType, instantTime, partitionType.getFileGroupCount());
    }
  }

  /**
   * Initialize file groups for a partition. For file listing, we just have one file group.
   *
   * All FileGroups for a given metadata partition has a fixed prefix as per the {@link MetadataPartitionType#getFileIdPrefix()}.
   * Each file group is suffixed with 4 digits with increments of 1 starting with 0000.
   *
   * Lets say we configure 10 file groups for record level index partition, and prefix as "record-index-bucket-"
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
    final HoodieDeleteBlock block = new HoodieDeleteBlock(new DeleteRecord[0], blockHeader);

    LOG.info(String.format("Creating %d file groups for partition %s with base fileId %s at instant time %s",
        fileGroupCount, metadataPartition.getPartitionPath(), metadataPartition.getFileIdPrefix(), instantTime));
    for (int i = 0; i < fileGroupCount; ++i) {
      final String fileGroupFileId = String.format("%s%04d", metadataPartition.getFileIdPrefix(), i);
      try {
        HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder()
            .onParentPath(FSUtils.getPartitionPath(metadataWriteConfig.getBasePath(), metadataPartition.getPartitionPath()))
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
        throw new HoodieException("Failed to created fileGroup " + fileGroupFileId + " for partition " + metadataPartition.getPartitionPath(), e);
      }
    }
  }

  public void dropMetadataPartitions(List<MetadataPartitionType> metadataPartitions) throws IOException {
    Set<String> completedIndexes = dataMetaClient.getTableConfig().getMetadataPartitions();
    Set<String> inflightIndexes = getInflightMetadataPartitions(dataMetaClient.getTableConfig());

    for (MetadataPartitionType partitionType : metadataPartitions) {
      String partitionPath = partitionType.getPartitionPath();
      // first update table config
      if (inflightIndexes.contains(partitionPath)) {
        inflightIndexes.remove(partitionPath);
        dataMetaClient.getTableConfig().setValue(HoodieTableConfig.TABLE_METADATA_PARTITIONS_INFLIGHT.key(), String.join(",", inflightIndexes));
      } else if (completedIndexes.contains(partitionPath)) {
        completedIndexes.remove(partitionPath);
        dataMetaClient.getTableConfig().setValue(HoodieTableConfig.TABLE_METADATA_PARTITIONS.key(), String.join(",", completedIndexes));
      }
      HoodieTableConfig.update(dataMetaClient.getFs(), new Path(dataMetaClient.getMetaPath()), dataMetaClient.getTableConfig().getProps());
      LOG.warn("Deleting Metadata Table partitions: " + partitionPath);
      dataMetaClient.getFs().delete(new Path(metadataWriteConfig.getBasePath(), partitionPath), true);
      // delete corresponding pending indexing instant file in the timeline
      LOG.warn("Deleting pending indexing instant from the timeline for partition: " + partitionPath);
      deletePendingIndexingInstant(dataMetaClient, partitionPath);
    }
  }

  /**
   * Deletes any pending indexing instant, if it exists.
   * It reads the plan from indexing.requested file and deletes both requested and inflight instants,
   * if the partition path in the plan matches with the given partition path.
   */
  private static void deletePendingIndexingInstant(HoodieTableMetaClient metaClient, String partitionPath) {
    metaClient.reloadActiveTimeline().filterPendingIndexTimeline().getInstants().filter(instant -> REQUESTED.equals(instant.getState()))
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
   * @param instantTime instant time of interest.
   * @param convertMetadataFunction converter function to convert the respective metadata to List of HoodieRecords to be written to metadata table.
   * @param <T> type of commit metadata.
   * @param canTriggerTableService true if table services can be triggered. false otherwise.
   */
  private <T> void processAndCommit(String instantTime, ConvertMetadataFunction convertMetadataFunction, boolean canTriggerTableService) {
    if (!dataWriteConfig.isMetadataTableEnabled()) {
      return;
    }
    Set<String> partitionsToUpdate = getMetadataPartitionsToUpdate();
    Set<String> inflightIndexes = getInflightMetadataPartitions(dataMetaClient.getTableConfig());
    // if indexing is inflight then do not trigger table service
    boolean doNotTriggerTableService = partitionsToUpdate.stream().anyMatch(inflightIndexes::contains);

    if (enabled && metadata != null) {
      // convert metadata and filter only the entries whose partition path are in partitionsToUpdate
      Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap = convertMetadataFunction.convertMetadata().entrySet().stream()
          .filter(entry -> partitionsToUpdate.contains(entry.getKey().getPartitionPath())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      commit(instantTime, partitionRecordsMap, !doNotTriggerTableService && canTriggerTableService);
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

  @Override
  public void buildMetadataPartitions(HoodieEngineContext engineContext, List<HoodieIndexPartitionInfo> indexPartitionInfos) {
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
      try {
        // file group should have already been initialized while scheduling index for this partition
        if (!dataMetaClient.getFs().exists(new Path(metadataWriteConfig.getBasePath(), relativePartitionPath))) {
          throw new HoodieIndexException(String.format("File group not initialized for metadata partition: %s, indexUptoInstant: %s. Looks like index scheduling failed!",
              relativePartitionPath, indexUptoInstantTime));
        }
      } catch (IOException e) {
        throw new HoodieIndexException(String.format("Unable to check whether file group is initialized for metadata partition: %s, indexUptoInstant: %s",
            relativePartitionPath, indexUptoInstantTime));
      }

      // return early and populate enabledPartitionTypes correctly (check in initialCommit)
      MetadataPartitionType partitionType = MetadataPartitionType.valueOf(relativePartitionPath.toUpperCase(Locale.ROOT));
      if (!enabledPartitionTypes.contains(partitionType)) {
        throw new HoodieIndexException(String.format("Indexing for metadata partition: %s is not enabled", partitionType));
      }
      partitionTypes.add(partitionType);
    });
    // before initial commit update inflight indexes in table config
    Set<String> inflightIndexes = getInflightMetadataPartitions(dataMetaClient.getTableConfig());
    inflightIndexes.addAll(indexPartitionInfos.stream().map(HoodieIndexPartitionInfo::getMetadataPartitionPath).collect(Collectors.toSet()));
    dataMetaClient.getTableConfig().setValue(HoodieTableConfig.TABLE_METADATA_PARTITIONS_INFLIGHT.key(), String.join(",", inflightIndexes));
    HoodieTableConfig.update(dataMetaClient.getFs(), new Path(dataMetaClient.getMetaPath()), dataMetaClient.getTableConfig().getProps());
    initialCommit(indexUptoInstantTime, partitionTypes);
  }

  /**
   * Update from {@code HoodieCommitMetadata}.
   *
   * @param commitMetadata {@code HoodieCommitMetadata}
   * @param instantTime Timestamp at which the commit was performed
   * @param isTableServiceAction {@code true} if commit metadata is pertaining to a table service. {@code false} otherwise.
   */
  @Override
  public void update(HoodieCommitMetadata commitMetadata, String instantTime, boolean isTableServiceAction) {
    processAndCommit(instantTime, () -> HoodieTableMetadataUtil.convertMetadataToRecords(
        engineContext, commitMetadata, instantTime, getRecordsGenerationParams()), !isTableServiceAction);
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
        cleanMetadata, getRecordsGenerationParams(), instantTime), false);
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
        metadata.getSyncedInstantTime()), false);
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

      Map<MetadataPartitionType, HoodieData<HoodieRecord>> records =
          HoodieTableMetadataUtil.convertMetadataToRecords(engineContext, metadataMetaClient.getActiveTimeline(),
              rollbackMetadata, getRecordsGenerationParams(), instantTime,
              metadata.getSyncedInstantTime(), wasSynced);
      commit(instantTime, records, false);
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
   * @param instantTime            - Action instant time for this commit
   * @param partitionRecordsMap    - Map of partition name to its records to commit
   * @param canTriggerTableService true if table services can be scheduled and executed. false otherwise.
   */
  protected abstract void commit(
      String instantTime, Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap,
      boolean canTriggerTableService);

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
      final int fileGroupCount = entry.getKey().getFileGroupCount();
      HoodieData<HoodieRecord> records = entry.getValue();

      List<FileSlice> fileSlices =
          HoodieTableMetadataUtil.getPartitionLatestFileSlices(metadataMetaClient, Option.ofNullable(fsView), partitionName);
      if (fileSlices.isEmpty()) {
        // scheduling of INDEX only initializes the file group and not add commit
        // so if there are no committed file slices, look for inflight slices
        fileSlices = HoodieTableMetadataUtil.getPartitionLatestFileSlicesIncludingInflight(metadataMetaClient, Option.ofNullable(fsView), partitionName);
      }
      ValidationUtils.checkArgument(fileSlices.size() == fileGroupCount,
          String.format("Invalid number of file groups for partition:%s, found=%d, required=%d",
              partitionName, fileSlices.size(), fileGroupCount));

      List<FileSlice> finalFileSlices = fileSlices;
      HoodieData<HoodieRecord> rddSinglePartitionRecords = records.map(r -> {
        FileSlice slice = finalFileSlices.get(HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(r.getRecordKey(),
            fileGroupCount));
        r.setCurrentLocation(new HoodieRecordLocation(slice.getBaseInstantTime(), slice.getFileId()));
        return r;
      });

      allPartitionRecords = allPartitionRecords.union(rddSinglePartitionRecords);
    }
    return allPartitionRecords;
  }

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
  protected void compactIfNecessary(BaseHoodieWriteClient writeClient, String instantTime) {
    // finish off any pending compactions if any from previous attempt.
    writeClient.runAnyPendingCompactions();

    String latestDeltaCommitTime = metadataMetaClient.reloadActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().lastInstant()
        .get().getTimestamp();
    List<HoodieInstant> pendingInstants = dataMetaClient.reloadActiveTimeline().filterInflightsAndRequested()
        .findInstantsBefore(instantTime).getInstants().collect(Collectors.toList());

    if (!pendingInstants.isEmpty()) {
      LOG.info(String.format("Cannot compact metadata table as there are %d inflight instants before latest deltacommit %s: %s",
          pendingInstants.size(), latestDeltaCommitTime, Arrays.toString(pendingInstants.toArray())));
      return;
    }

    // Trigger compaction with suffixes based on the same instant time. This ensures that any future
    // delta commits synced over will not have an instant time lesser than the last completed instant on the
    // metadata table.
    final String compactionInstantTime = latestDeltaCommitTime + "001";
    if (writeClient.scheduleCompactionAtInstant(compactionInstantTime, Option.empty())) {
      writeClient.compact(compactionInstantTime);
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
    writeClient.clean(instantTime + "002");
  }

  /**
   * This is invoked to initialize metadata table for a dataset.
   * Initial commit has special handling mechanism due to its scale compared to other regular commits.
   * During cold startup, the list of files to be committed can be huge.
   * So creating a HoodieCommitMetadata out of these large number of files,
   * and calling the existing update(HoodieCommitMetadata) function does not scale well.
   * Hence, we have a special commit just for the initialization scenario.
   */
  private void initialCommit(String createInstantTime, List<MetadataPartitionType> partitionTypes) {
    // List all partitions in the basePath of the containing dataset
    LOG.info("Initializing metadata table by using file listings in " + dataWriteConfig.getBasePath());
    engineContext.setJobStatus(this.getClass().getSimpleName(), "Initializing metadata table by listing files and partitions: " + dataWriteConfig.getTableName());

    Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();

    List<DirectoryInfo> partitionInfoList = listAllPartitions(dataMetaClient);
    Map<String, Map<String, Long>> partitionToFilesMap = partitionInfoList.stream()
        .map(p -> {
          String partitionName = HoodieTableMetadataUtil.getPartitionIdentifier(p.getRelativePath());
          return Pair.of(partitionName, p.getFileNameToSizeMap());
        })
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    int totalDataFilesCount = partitionToFilesMap.values().stream().mapToInt(Map::size).sum();
    List<String> partitions = new ArrayList<>(partitionToFilesMap.keySet());

    if (partitionTypes.contains(MetadataPartitionType.FILES)) {
      // Record which saves the list of all partitions
      HoodieRecord allPartitionRecord = HoodieMetadataPayload.createPartitionListRecord(partitions);
      HoodieData<HoodieRecord> filesPartitionRecords = getFilesPartitionRecords(createInstantTime, partitionInfoList, allPartitionRecord);
      ValidationUtils.checkState(filesPartitionRecords.count() == (partitions.size() + 1));
      partitionToRecordsMap.put(MetadataPartitionType.FILES, filesPartitionRecords);
    }

    if (partitionTypes.contains(MetadataPartitionType.BLOOM_FILTERS) && totalDataFilesCount > 0) {
      final HoodieData<HoodieRecord> recordsRDD = HoodieTableMetadataUtil.convertFilesToBloomFilterRecords(
          engineContext, Collections.emptyMap(), partitionToFilesMap, getRecordsGenerationParams(), createInstantTime);
      partitionToRecordsMap.put(MetadataPartitionType.BLOOM_FILTERS, recordsRDD);
    }

    if (partitionTypes.contains(MetadataPartitionType.COLUMN_STATS) && totalDataFilesCount > 0) {
      final HoodieData<HoodieRecord> recordsRDD = HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
          engineContext, Collections.emptyMap(), partitionToFilesMap, getRecordsGenerationParams());
      partitionToRecordsMap.put(MetadataPartitionType.COLUMN_STATS, recordsRDD);
    }

    LOG.info("Committing " + partitions.size() + " partitions and " + totalDataFilesCount + " files to metadata");

    commit(createInstantTime, partitionToRecordsMap, false);
  }

  private HoodieData<HoodieRecord> getFilesPartitionRecords(String createInstantTime, List<DirectoryInfo> partitionInfoList, HoodieRecord allPartitionRecord) {
    HoodieData<HoodieRecord> filesPartitionRecords = engineContext.parallelize(Arrays.asList(allPartitionRecord), 1);
    if (partitionInfoList.isEmpty()) {
      return filesPartitionRecords;
    }

    HoodieData<HoodieRecord> fileListRecords = engineContext.parallelize(partitionInfoList, partitionInfoList.size()).map(partitionInfo -> {
      Map<String, Long> fileNameToSizeMap = partitionInfo.getFileNameToSizeMap();
      // filter for files that are part of the completed commits
      Map<String, Long> validFileNameToSizeMap = fileNameToSizeMap.entrySet().stream().filter(fileSizePair -> {
        String commitTime = FSUtils.getCommitTime(fileSizePair.getKey());
        return HoodieTimeline.compareTimestamps(commitTime, HoodieTimeline.LESSER_THAN_OR_EQUALS, createInstantTime);
      }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      // Record which saves files within a partition
      return HoodieMetadataPayload.createPartitionFilesRecord(
          HoodieTableMetadataUtil.getPartitionIdentifier(partitionInfo.getRelativePath()), Option.of(validFileNameToSizeMap), Option.empty());
    });

    return filesPartitionRecords.union(fileListRecords);
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

    public DirectoryInfo(String relativePath, FileStatus[] fileStatus) {
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
          filenameToSizeMap.put(status.getPath().getName(), status.getLen());
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
