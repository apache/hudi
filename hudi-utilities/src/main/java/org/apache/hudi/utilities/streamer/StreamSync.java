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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieConversionUtils;
import org.apache.hudi.HoodieSchemaUtils;
import org.apache.hudi.HoodieSparkSqlWriter;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.callback.common.WriteStatusValidator;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineServerHelper;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.commit.BaseDatasetBulkInsertCommitActionExecutor;
import org.apache.hudi.commit.HoodieStreamerDatasetBulkInsertCommitActionExecutor;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieMetaSyncException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.sync.common.util.SyncUtilHelpers;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.util.JavaScalaConverters;
import org.apache.hudi.util.SparkKeyGenUtils;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallback;
import org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallbackConfig;
import org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallback;
import org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig;
import org.apache.hudi.utilities.exception.HoodieSchemaFetchException;
import org.apache.hudi.utilities.exception.HoodieSourceTimeoutException;
import org.apache.hudi.utilities.exception.HoodieStreamerException;
import org.apache.hudi.utilities.exception.HoodieStreamerWriteException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.DelegatingSchemaProvider;
import org.apache.hudi.utilities.schema.LazyCastingIterator;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.schema.SchemaSet;
import org.apache.hudi.utilities.schema.SimpleSchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.Source;
import org.apache.hudi.utilities.streamer.HoodieStreamer.Config;
import org.apache.hudi.utilities.transform.Transformer;

import com.codahale.metrics.Timer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.DataSourceUtils.createUserDefinedBulkInsertPartitioner;
import static org.apache.hudi.avro.AvroSchemaUtils.getAvroRecordQualifiedName;
import static org.apache.hudi.common.table.HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE;
import static org.apache.hudi.common.table.HoodieTableConfig.TIMELINE_HISTORY_PATH;
import static org.apache.hudi.common.table.HoodieTableConfig.URL_ENCODE_PARTITIONING;
import static org.apache.hudi.common.table.checkpoint.CheckpointUtils.buildCheckpointFromGeneralSource;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.config.HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE;
import static org.apache.hudi.config.HoodieClusteringConfig.INLINE_CLUSTERING;
import static org.apache.hudi.config.HoodieCompactionConfig.INLINE_COMPACT;
import static org.apache.hudi.config.HoodieErrorTableConfig.ENABLE_ERROR_TABLE_WRITE_UNIFICATION;
import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_ENABLED;
import static org.apache.hudi.config.HoodieWriteConfig.COMBINE_BEFORE_INSERT;
import static org.apache.hudi.config.HoodieWriteConfig.COMBINE_BEFORE_UPSERT;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_TABLE_VERSION;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_BUCKET_SYNC;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_BUCKET_SYNC_SPEC;
import static org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory.getKeyGeneratorClassName;
import static org.apache.hudi.sync.common.util.SyncUtilHelpers.getHoodieMetaSyncException;
import static org.apache.hudi.utilities.UtilHelpers.createRecordMerger;
import static org.apache.hudi.utilities.config.HoodieStreamerConfig.CHECKPOINT_FORCE_SKIP;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME;
import static org.apache.hudi.utilities.streamer.StreamerCheckpointUtils.getLatestInstantWithValidCheckpointInfo;

/**
 * Sync's one batch of data to hoodie table.
 */
public class StreamSync implements Serializable, Closeable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(StreamSync.class);
  private static final String NULL_PLACEHOLDER = "[null]";
  public static final String CHECKPOINT_IGNORE_KEY = "deltastreamer.checkpoint.ignore_key";

  /**
   * Delta Sync Config.
   */
  private final HoodieStreamer.Config cfg;

  /**
   * Source to pull deltas from.
   */
  private transient SourceFormatAdapter formatAdapter;

  /**
   * User Provided Schema Provider.
   */
  private transient SchemaProvider userProvidedSchemaProvider;

  /**
   * Schema provider that supplies the command for reading the input and writing out the target table.
   */
  private transient SchemaProvider schemaProvider;

  /**
   * Allows transforming source to target table before writing.
   */
  private transient Option<Transformer> transformer;

  private final String keyGenClassName;

  /**
   * Filesystem used.
   */
  private transient HoodieStorage storage;

  /**
   * Spark context Wrapper.
   */
  private transient HoodieSparkEngineContext hoodieSparkContext;

  /**
   * Spark Session.
   */
  private final transient SparkSession sparkSession;

  /**
   * Hive Config.
   */
  private transient Configuration conf;

  /**
   * Bag of properties with source, hoodie client, key generator etc.
   *
   * NOTE: These properties are already consolidated w/ CLI provided config-overrides
   */
  private final TypedProperties props;

  /**
   * Callback when write client is instantiated.
   */
  private transient Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient;

  /**
   * Timeline with completed commits, including both .commit and .deltacommit.
   */
  private transient Option<HoodieTimeline> commitsTimelineOpt;

  // all commits timeline, including all (commits, delta commits, compaction, clean, savepoint, rollback, replace commits, index)
  private transient Option<HoodieTimeline> allCommitsTimelineOpt;

  /**
   * Tracks whether new schema is being seen and creates client accordingly.
   */
  private final SchemaSet processedSchema;

  /**
   * DeltaSync will explicitly manage embedded timeline server so that they can be reused across Write Client
   * instantiations.
   */
  private transient Option<EmbeddedTimelineService> embeddedTimelineService = Option.empty();

  /**
   * Write Client.
   */
  private transient SparkRDDWriteClient writeClient;

  private Option<BaseErrorTableWriter> errorTableWriter = Option.empty();
  private HoodieErrorTableConfig.ErrorWriteFailureStrategy errorWriteFailureStrategy;

  private transient HoodieIngestionMetrics metrics;
  private transient HoodieMetrics hoodieMetrics;

  private final boolean autoGenerateRecordKeys;
  private final boolean isErrorTableWriteUnificationEnabled;

  @VisibleForTesting
  StreamSync(HoodieStreamer.Config cfg, SparkSession sparkSession,
             TypedProperties props, HoodieSparkEngineContext hoodieSparkContext, HoodieStorage storage, Configuration conf,
             Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient, SchemaProvider userProvidedSchemaProvider,
             Option<BaseErrorTableWriter> errorTableWriter, SourceFormatAdapter formatAdapter, Option<Transformer> transformer,
             boolean autoGenerateRecordKeys) {
    this.cfg = cfg;
    this.hoodieSparkContext = hoodieSparkContext;
    this.sparkSession = sparkSession;
    this.storage = storage;
    this.onInitializingHoodieWriteClient = onInitializingHoodieWriteClient;
    this.props = props;
    this.userProvidedSchemaProvider = userProvidedSchemaProvider;
    this.processedSchema = new SchemaSet();
    this.autoGenerateRecordKeys = autoGenerateRecordKeys;
    this.keyGenClassName = getKeyGeneratorClassName(props);
    this.conf = conf;

    this.errorTableWriter = errorTableWriter;
    this.isErrorTableWriteUnificationEnabled = getBooleanWithAltKeys(props, ENABLE_ERROR_TABLE_WRITE_UNIFICATION);
    this.formatAdapter = formatAdapter;
    this.transformer = transformer;
  }

  @Deprecated
  public StreamSync(HoodieStreamer.Config cfg, SparkSession sparkSession,
                    SchemaProvider schemaProvider,
                    TypedProperties props, JavaSparkContext jssc, FileSystem fs, Configuration conf,
                    Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient) throws IOException {
    this(cfg, sparkSession, props, new HoodieSparkEngineContext(jssc),
        fs, conf, onInitializingHoodieWriteClient,
        new DefaultStreamContext(schemaProvider, Option.empty()));
  }

  public StreamSync(HoodieStreamer.Config cfg, SparkSession sparkSession,
                    TypedProperties props, HoodieSparkEngineContext hoodieSparkContext,
                    FileSystem fs, Configuration conf,
                    Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient, StreamContext streamContext) throws IOException {
    this.cfg = cfg;
    this.hoodieSparkContext = hoodieSparkContext;
    this.sparkSession = sparkSession;
    this.storage = new HoodieHadoopStorage(fs);
    this.onInitializingHoodieWriteClient = onInitializingHoodieWriteClient;
    this.props = props;
    this.userProvidedSchemaProvider = streamContext.getSchemaProvider();
    this.processedSchema = new SchemaSet();
    this.autoGenerateRecordKeys = KeyGenUtils.isAutoGeneratedRecordKeysEnabled(props);
    this.keyGenClassName = getKeyGeneratorClassName(props);
    this.conf = conf;

    HoodieWriteConfig hoodieWriteConfig = getHoodieClientConfig();
    this.metrics = (HoodieIngestionMetrics) ReflectionUtils.loadClass(cfg.ingestionMetricsClass,
        new Class<?>[] {HoodieMetricsConfig.class, HoodieStorage.class},
        hoodieWriteConfig.getMetricsConfig(), storage);
    this.hoodieMetrics = new HoodieMetrics(hoodieWriteConfig, storage);
    if (props.getBoolean(ERROR_TABLE_ENABLED.key(), ERROR_TABLE_ENABLED.defaultValue())) {
      this.errorTableWriter = ErrorTableUtils.getErrorTableWriter(
          cfg, sparkSession, props, hoodieSparkContext, fs, Option.of(metrics));
      this.errorWriteFailureStrategy = ErrorTableUtils.getErrorWriteFailureStrategy(props);
    }
    this.isErrorTableWriteUnificationEnabled = getBooleanWithAltKeys(props, ENABLE_ERROR_TABLE_WRITE_UNIFICATION);
    initializeMetaClient();
    Source source = UtilHelpers.createSource(cfg.sourceClassName, props, hoodieSparkContext.jsc(), sparkSession, metrics, streamContext);
    this.formatAdapter = new SourceFormatAdapter(source, this.errorTableWriter, Option.of(props));

    Supplier<Option<Schema>> schemaSupplier = schemaProvider == null ? Option::empty : () -> Option.ofNullable(schemaProvider.getSourceSchema());
    this.transformer = UtilHelpers.createTransformer(Option.ofNullable(cfg.transformerClassNames), schemaSupplier, this.errorTableWriter.isPresent());
  }

  /**
   * Creates a meta client for the table, and refresh timeline.
   *
   * @throws IOException in case of any IOException
   */
  public HoodieTableMetaClient initializeMetaClientAndRefreshTimeline() throws IOException {
    return initializeMetaClient(true);
  }

  /**
   * Creates a meta client for the table without refreshing timeline.
   *
   * @throws IOException in case of any IOException
   */
  private HoodieTableMetaClient initializeMetaClient() throws IOException {
    return initializeMetaClient(false);
  }

  /**
   * Creates a meta client for the table, and refresh timeline if specified.
   * If the table does not yet exist, it will be initialized.
   *
   * @param refreshTimeline when set to true, loads the active timeline and updates the {@link #commitsTimelineOpt} and {@link #allCommitsTimelineOpt} values
   * @return a meta client for the table
   * @throws IOException in case of any IOException
   */
  private HoodieTableMetaClient initializeMetaClient(boolean refreshTimeline) throws IOException {
    if (storage.exists(new StoragePath(cfg.targetBasePath))) {
      try {
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
            .setConf(HadoopFSUtils.getStorageConfWithCopy(conf))
            .setBasePath(cfg.targetBasePath)
            .setTimeGeneratorConfig(HoodieTimeGeneratorConfig.newBuilder().fromProperties(props).withPath(cfg.targetBasePath).build())
            .build();
        if (refreshTimeline) {
          switch (metaClient.getTableType()) {
            case COPY_ON_WRITE:
            case MERGE_ON_READ:
              // we can use getCommitsTimeline for both COW and MOR here, because for COW there is no deltacommit
              this.commitsTimelineOpt = Option.of(metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
              this.allCommitsTimelineOpt = Option.of(metaClient.getActiveTimeline().getAllCommitsTimeline());
              break;
            default:
              throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
          }
        }
        return metaClient;
      } catch (HoodieIOException e) {
        LOG.warn("Full exception msg " + e.getMessage());
        if (e.getMessage().contains("Could not load Hoodie properties") && e.getMessage().contains(HoodieTableConfig.HOODIE_PROPERTIES_FILE)) {
          String basePathWithForwardSlash = cfg.targetBasePath.endsWith("/") ? cfg.targetBasePath :
              String.format("%s/", cfg.targetBasePath);
          String pathToHoodieProps = String.format("%s%s/%s", basePathWithForwardSlash,
              HoodieTableMetaClient.METAFOLDER_NAME, HoodieTableConfig.HOODIE_PROPERTIES_FILE);
          String pathToHoodiePropsBackup = String.format("%s%s/%s", basePathWithForwardSlash,
              HoodieTableMetaClient.METAFOLDER_NAME,
              HoodieTableConfig.HOODIE_PROPERTIES_FILE_BACKUP);
          boolean hoodiePropertiesExists =
              storage.exists(new StoragePath(basePathWithForwardSlash))
                  && storage.exists(new StoragePath(pathToHoodieProps))
                  && storage.exists(new StoragePath(pathToHoodiePropsBackup));

          if (!hoodiePropertiesExists) {
            LOG.warn("Base path exists, but table is not fully initialized. Re-initializing again");
            HoodieTableMetaClient metaClientToValidate = initializeEmptyTable();
            // reload the timeline from metaClient and validate that its empty table. If there are any instants found, then we should fail the pipeline, bcoz hoodie.properties got deleted by mistake.
            if (metaClientToValidate.reloadActiveTimeline().countInstants() > 0) {
              // Deleting the recreated hoodie.properties and throwing exception.
              storage.deleteDirectory(new StoragePath(String.format("%s%s/%s", basePathWithForwardSlash,
                  HoodieTableMetaClient.METAFOLDER_NAME,
                  HoodieTableConfig.HOODIE_PROPERTIES_FILE)));
              throw new HoodieIOException(
                  "hoodie.properties is missing. Likely due to some external entity. Please populate the hoodie.properties and restart the pipeline. ",
                  e.getIOException());
            }
            return metaClientToValidate;
          }
        }
        throw e;
      }
    } else {
      return initializeEmptyTable();
    }
  }

  private HoodieTableMetaClient initializeEmptyTable() throws IOException {
    return initializeEmptyTable(HoodieTableMetaClient.newTableBuilder(),
        SparkKeyGenUtils.getPartitionColumnsForKeyGenerator(props, HoodieTableVersion.fromVersionCode(ConfigUtils.getIntWithAltKeys(props, WRITE_TABLE_VERSION))),
        HadoopFSUtils.getStorageConfWithCopy(hoodieSparkContext.hadoopConfiguration()));
  }

  HoodieTableMetaClient initializeEmptyTable(HoodieTableMetaClient.TableBuilder tableBuilder, String partitionColumns,
                            StorageConfiguration<?> storageConf) throws IOException {
    this.commitsTimelineOpt = Option.empty();
    this.allCommitsTimelineOpt = Option.empty();
    return tableBuilder.setTableType(cfg.tableType)
        .setTableName(cfg.targetTableName)
        .setArchiveLogFolder(TIMELINE_HISTORY_PATH.defaultValue())
        .setPayloadClassName(cfg.payloadClassName)
        .setRecordMergeStrategyId(cfg.recordMergeStrategyId)
        .setRecordMergeMode(cfg.recordMergeMode)
        .setBaseFileFormat(cfg.baseFileFormat)
        .setPartitionFields(partitionColumns)
        .setTableVersion(ConfigUtils.getIntWithAltKeys(props, WRITE_TABLE_VERSION))
        .setRecordKeyFields(props.getProperty(DataSourceWriteOptions.RECORDKEY_FIELD().key()))
        .setPopulateMetaFields(props.getBoolean(HoodieTableConfig.POPULATE_META_FIELDS.key(),
            HoodieTableConfig.POPULATE_META_FIELDS.defaultValue()))
        .setKeyGeneratorClassProp(keyGenClassName)
        .setOrderingFields(cfg.sourceOrderingFields)
        .setPartitionMetafileUseBaseFormat(props.getBoolean(HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.key(),
            HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.defaultValue()))
        .setCDCEnabled(props.getBoolean(HoodieTableConfig.CDC_ENABLED.key(),
            HoodieTableConfig.CDC_ENABLED.defaultValue()))
        .setCDCSupplementalLoggingMode(props.getString(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key(),
            HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.defaultValue()))
        .setShouldDropPartitionColumns(HoodieStreamerUtils.isDropPartitionColumns(props))
        .setHiveStylePartitioningEnable(props.getBoolean(HIVE_STYLE_PARTITIONING_ENABLE.key(),
            Boolean.parseBoolean(HIVE_STYLE_PARTITIONING_ENABLE.defaultValue())))
        .setUrlEncodePartitioning(props.getBoolean(URL_ENCODE_PARTITIONING.key(),
            Boolean.parseBoolean(URL_ENCODE_PARTITIONING.defaultValue())))
        .setTableFormat(props.getProperty(HoodieTableConfig.TABLE_FORMAT.key(), HoodieTableConfig.TABLE_FORMAT.defaultValue()))
        .initTable(storageConf, cfg.targetBasePath);
  }

  /**
   * Run one round of delta sync and return new compaction instant if one got scheduled.
   */
  public Pair<Option<String>, JavaRDD<WriteStatus>> syncOnce() throws IOException {
    Pair<Option<String>, JavaRDD<WriteStatus>> result = null;
    Timer.Context overallTimerContext = metrics.getOverallTimerContext();

    try {
      // Refresh Timeline
      HoodieTableMetaClient metaClient = initializeMetaClientAndRefreshTimeline();

      Pair<InputBatch, Boolean> inputBatchAndUseRowWriter = readFromSource(metaClient);

      if (inputBatchAndUseRowWriter != null) {
        InputBatch inputBatch = inputBatchAndUseRowWriter.getLeft();
        boolean useRowWriter = inputBatchAndUseRowWriter.getRight();
        initializeWriteClientAndRetryTableServices(inputBatch, metaClient);
        result = writeToSinkAndDoMetaSync(metaClient, inputBatch, useRowWriter, metrics, overallTimerContext);
      }
      // refresh schemas if need be before next batch
      if (schemaProvider != null) {
        schemaProvider.refresh();
      }
      metrics.updateStreamerSyncMetrics(System.currentTimeMillis());
      return result;
    } finally {
      this.formatAdapter.getSource().releaseResources();
    }
  }

  private void initializeWriteClientAndRetryTableServices(InputBatch inputBatch, HoodieTableMetaClient metaClient) throws IOException {
    // this is the first input batch. If schemaProvider not set, use it and register Avro Schema and start
    // compactor
    if (writeClient == null) {
      this.schemaProvider = inputBatch.getSchemaProvider();
      // Setup HoodieWriteClient and compaction now that we decided on schema
      setupWriteClient(inputBatch.getBatch(), metaClient);
    } else {
      Schema newSourceSchema = inputBatch.getSchemaProvider().getSourceSchema();
      Schema newTargetSchema = inputBatch.getSchemaProvider().getTargetSchema();
      if ((newSourceSchema != null && !processedSchema.isSchemaPresent(newSourceSchema))
          || (newTargetSchema != null && !processedSchema.isSchemaPresent(newTargetSchema))) {
        String sourceStr = newSourceSchema == null ? NULL_PLACEHOLDER : newSourceSchema.toString(true);
        String targetStr = newTargetSchema == null ? NULL_PLACEHOLDER : newTargetSchema.toString(true);
        LOG.info("Seeing new schema. Source: {}, Target: {}", sourceStr, targetStr);
        // We need to recreate write client with new schema and register them.
        reInitWriteClient(newSourceSchema, newTargetSchema, inputBatch.getBatch(), metaClient);
        if (newSourceSchema != null) {
          processedSchema.addSchema(newSourceSchema);
        }
        if (newTargetSchema != null) {
          processedSchema.addSchema(newTargetSchema);
        }
      }
    }

    // complete the pending compaction before writing to sink
    if (cfg.retryLastPendingInlineCompactionJob && writeClient.getConfig().inlineCompactionEnabled()) {
      Option<String> pendingCompactionInstant = getLastPendingCompactionInstant(allCommitsTimelineOpt);
      if (pendingCompactionInstant.isPresent()) {
        HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetadata = writeClient.compact(pendingCompactionInstant.get());
        writeClient.commitCompaction(pendingCompactionInstant.get(), writeMetadata, Option.empty());
        initializeMetaClientAndRefreshTimeline();
        reInitWriteClient(schemaProvider.getSourceSchema(), schemaProvider.getTargetSchema(), null, metaClient);
      }
    } else if (cfg.retryLastPendingInlineClusteringJob && writeClient.getConfig().inlineClusteringEnabled()) {
      // complete the pending clustering before writing to sink
      Option<String> pendingClusteringInstant = getLastPendingClusteringInstant(allCommitsTimelineOpt);
      if (pendingClusteringInstant.isPresent()) {
        writeClient.cluster(pendingClusteringInstant.get());
      }
    }
  }

  private Option<String> getLastPendingClusteringInstant(Option<HoodieTimeline> commitTimelineOpt) {
    if (commitTimelineOpt.isPresent()) {
      Option<HoodieInstant> pendingClusteringInstant = commitTimelineOpt.get().getLastPendingClusterInstant();
      return pendingClusteringInstant.isPresent() ? Option.of(pendingClusteringInstant.get().requestedTime()) : Option.empty();
    }
    return Option.empty();
  }

  private Option<String> getLastPendingCompactionInstant(Option<HoodieTimeline> commitTimelineOpt) {
    if (commitTimelineOpt.isPresent()) {
      Option<HoodieInstant> pendingCompactionInstant = commitTimelineOpt.get().filterPendingCompactionTimeline().lastInstant();
      return pendingCompactionInstant.isPresent() ? Option.of(pendingCompactionInstant.get().requestedTime()) : Option.empty();
    }
    return Option.empty();
  }

  /**
   * Read from Upstream Source and apply transformation if needed.
   *
   * @return Pair<InputBatch and Boolean> Input data read from upstream source, and boolean is true if the result should use the row writer path.
   * @throws Exception in case of any Exception
   */
  public Pair<InputBatch, Boolean> readFromSource(HoodieTableMetaClient metaClient) throws IOException {
    // Retrieve the previous round checkpoints, if any
    Option<Checkpoint> checkpointToResume = StreamerCheckpointUtils.resolveCheckpointToResumeFrom(commitsTimelineOpt, cfg, props, metaClient);
    LOG.info("Checkpoint to resume from : {}", checkpointToResume);

    int maxRetryCount = cfg.retryOnSourceFailures ? cfg.maxRetryCount : 1;
    int curRetryCount = 0;
    Pair<InputBatch, Boolean> sourceDataToSync = null;
    while (curRetryCount++ < maxRetryCount && sourceDataToSync == null) {
      try {
        sourceDataToSync = fetchFromSourceAndPrepareRecords(checkpointToResume, metaClient);
      } catch (HoodieSourceTimeoutException e) {
        if (curRetryCount >= maxRetryCount) {
          throw e;
        }
        try {
          LOG.error("Exception thrown while fetching data from source. Msg : " + e.getMessage() + ", class : " + e.getClass() + ", cause : " + e.getCause());
          LOG.error("Sleeping for " + (cfg.retryIntervalSecs) + " before retrying again. Current retry count " + curRetryCount + ", max retry count " + cfg.maxRetryCount);
          Thread.sleep(cfg.retryIntervalSecs * 1000);
        } catch (InterruptedException ex) {
          LOG.error("Ignoring InterruptedException while waiting to retry on source failure " + e.getMessage());
        }
      }
    }
    return sourceDataToSync;
  }

  private Pair<InputBatch, Boolean> fetchFromSourceAndPrepareRecords(Option<Checkpoint> resumeCheckpoint, HoodieTableMetaClient metaClient) {
    hoodieSparkContext.setJobStatus(this.getClass().getSimpleName(), "Fetching next batch: " + cfg.targetTableName);
    HoodieRecordType recordType = createRecordMerger(props).getRecordType();
    if (recordType == HoodieRecordType.SPARK && HoodieTableType.valueOf(cfg.tableType) == HoodieTableType.MERGE_ON_READ
        && !cfg.operation.equals(WriteOperationType.BULK_INSERT)
        && HoodieLogBlock.HoodieLogBlockType.fromId(props.getProperty(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "avro"))
        != HoodieLogBlock.HoodieLogBlockType.PARQUET_DATA_BLOCK) {
      throw new UnsupportedOperationException("Spark record only support parquet log.");
    }

    Pair<InputBatch, Boolean> inputBatchAndRowWriterEnabled = fetchNextBatchFromSource(resumeCheckpoint, metaClient);
    InputBatch inputBatch = inputBatchAndRowWriterEnabled.getLeft();
    boolean useRowWriter = inputBatchAndRowWriterEnabled.getRight();
    final Checkpoint checkpoint = inputBatch.getCheckpointForNextBatch();

    // handle no new data and no change in checkpoint
    if (!cfg.allowCommitOnNoCheckpointChange && checkpoint.equals(resumeCheckpoint.orElse(null))) {
      LOG.info("No new data, source checkpoint has not changed. Nothing to commit. Old checkpoint=("
          + resumeCheckpoint + "). New Checkpoint=(" + checkpoint + ")");
      String commitActionType = CommitUtils.getCommitActionType(cfg.operation, HoodieTableType.valueOf(cfg.tableType));
      hoodieMetrics.updateMetricsForEmptyData(commitActionType);
      return null;
    }

    // handle empty batch with change in checkpoint
    hoodieSparkContext.setJobStatus(this.getClass().getSimpleName(), "Checking if input is empty: " + cfg.targetTableName);
    return Pair.of(inputBatch, useRowWriter);
  }

  @VisibleForTesting
  boolean canUseRowWriter(Schema targetSchema) {
    // enable row writer only when operation is BULK_INSERT, and source is ROW type and if row writer is not explicitly disabled.
    boolean rowWriterEnabled = isRowWriterEnabled();
    return rowWriterEnabled && targetSchema != null;
  }

  @VisibleForTesting
  boolean isRowWriterEnabled() {
    return cfg.operation == WriteOperationType.BULK_INSERT && formatAdapter.getSource().getSourceType() == Source.SourceType.ROW
        && this.props.getBoolean("hoodie.streamer.write.row.writer.enable", false);
  }

  /**
   * Fetch data from source, apply transformations if any, align with schema from schema provider if need be and return the input batch.
   * @param resumeCheckpoint checkpoint to resume from source.
   * @return Pair with {@link InputBatch} containing the new batch of data from source along with new checkpoint and schema provider instance to use,
   *         and a boolean set to `true` if row writer can be used.
   */
  @VisibleForTesting
  Pair<InputBatch, Boolean> fetchNextBatchFromSource(Option<Checkpoint> resumeCheckpoint, HoodieTableMetaClient metaClient) {
    Option<JavaRDD<GenericRecord>> avroRDDOptional = null;
    Checkpoint checkpoint = null;
    SchemaProvider schemaProvider = null;
    InputBatch inputBatchForWriter = null; // row writer
    boolean reconcileSchema = props.getBoolean(DataSourceWriteOptions.RECONCILE_SCHEMA().key());
    if (transformer.isPresent()) {
      // Transformation is needed. Fetch New rows in Row Format, apply transformation and then convert them
      // to generic records for writing
      InputBatch<Dataset<Row>> dataAndCheckpoint =
          formatAdapter.fetchNewDataInRowFormat(resumeCheckpoint, cfg.sourceLimit);

      Option<Dataset<Row>> transformed =
          dataAndCheckpoint.getBatch().map(data -> transformer.get().apply(hoodieSparkContext.jsc(), sparkSession, data, props));

      transformed = formatAdapter.processErrorEvents(transformed,
          ErrorEvent.ErrorReason.CUSTOM_TRANSFORMER_FAILURE);

      checkpoint = dataAndCheckpoint.getCheckpointForNextBatch();
      if (this.userProvidedSchemaProvider != null && this.userProvidedSchemaProvider.getTargetSchema() != null
          && this.userProvidedSchemaProvider.getTargetSchema() != InputBatch.NULL_SCHEMA) {
        // Let's deduce the schema provider for writer side first!
        schemaProvider = getDeducedSchemaProvider(this.userProvidedSchemaProvider.getTargetSchema(), this.userProvidedSchemaProvider, metaClient);
        boolean useRowWriter = canUseRowWriter(schemaProvider.getTargetSchema());
        if (useRowWriter) {
          inputBatchForWriter = new InputBatch(transformed, checkpoint, schemaProvider);
        } else {
          // non row writer path
          SchemaProvider finalSchemaProvider = schemaProvider;
          // If the target schema is specified through Avro schema,
          // pass in the schema for the Row-to-Avro conversion
          // to avoid nullability mismatch between Avro schema and Row schema
          if (errorTableWriter.isPresent()
              && props.getBoolean(HoodieErrorTableConfig.ERROR_ENABLE_VALIDATE_TARGET_SCHEMA.key(),
              HoodieErrorTableConfig.ERROR_ENABLE_VALIDATE_TARGET_SCHEMA.defaultValue())) {
            // If the above conditions are met, trigger error events for the rows whose conversion to
            // avro records fails.
            avroRDDOptional = transformed.map(
                rowDataset -> {
                  Tuple2<RDD<GenericRecord>, RDD<String>> safeCreateRDDs = HoodieSparkUtils.safeCreateRDD(rowDataset,
                      HOODIE_RECORD_STRUCT_NAME, HOODIE_RECORD_NAMESPACE, reconcileSchema,
                      Option.of(finalSchemaProvider.getTargetSchema()));
                  errorTableWriter.get().addErrorEvents(safeCreateRDDs._2().toJavaRDD()
                      .map(evStr -> new ErrorEvent<>(evStr,
                          ErrorEvent.ErrorReason.AVRO_DESERIALIZATION_FAILURE)));
                  return safeCreateRDDs._1.toJavaRDD();
                });
          } else {
            avroRDDOptional = transformed.map(
                rowDataset -> getTransformedRDD(rowDataset, reconcileSchema, finalSchemaProvider.getTargetSchema()));
          }
        }
      } else {
        // Deduce proper target (writer's) schema for the input dataset, reconciling its
        // schema w/ the table's one
        Schema incomingSchema = transformed.map(df ->
                AvroConversionUtils.convertStructTypeToAvroSchema(df.schema(), getAvroRecordQualifiedName(cfg.targetTableName)))
            .orElseGet(dataAndCheckpoint.getSchemaProvider()::getTargetSchema);
        schemaProvider = getDeducedSchemaProvider(incomingSchema, dataAndCheckpoint.getSchemaProvider(), metaClient);

        if (canUseRowWriter(schemaProvider.getTargetSchema())) {
          inputBatchForWriter = new InputBatch(transformed, checkpoint, schemaProvider);
        } else {
          // Rewrite transformed records into the expected target schema
          SchemaProvider finalSchemaProvider = schemaProvider;
          avroRDDOptional = transformed.map(t -> getTransformedRDD(t, reconcileSchema, finalSchemaProvider.getTargetSchema()));
        }
      }
    } else {
      if (isRowWriterEnabled()) {
        InputBatch inputBatchNeedsDeduceSchema = formatAdapter.fetchNewDataInRowFormat(resumeCheckpoint, cfg.sourceLimit);
        if (canUseRowWriter(inputBatchNeedsDeduceSchema.getSchemaProvider().getTargetSchema())) {
          inputBatchForWriter = new InputBatch<>(inputBatchNeedsDeduceSchema.getBatch(), inputBatchNeedsDeduceSchema.getCheckpointForNextBatch(),
              getDeducedSchemaProvider(inputBatchNeedsDeduceSchema.getSchemaProvider().getTargetSchema(), inputBatchNeedsDeduceSchema.getSchemaProvider(), metaClient));
        } else {
          LOG.warn("Row-writer is enabled but cannot be used due to the target schema");
        }
      }
      // if row writer was enabled but the target schema prevents us from using it, do not use the row writer
      if (inputBatchForWriter == null) {
        // Pull the data from the source & prepare the write
        InputBatch<JavaRDD<GenericRecord>> dataAndCheckpoint = formatAdapter.fetchNewDataInAvroFormat(resumeCheckpoint, cfg.sourceLimit);
        checkpoint = dataAndCheckpoint.getCheckpointForNextBatch();
        // Rewrite transformed records into the expected target schema
        schemaProvider = getDeducedSchemaProvider(dataAndCheckpoint.getSchemaProvider().getTargetSchema(), dataAndCheckpoint.getSchemaProvider(), metaClient);
        String serializedTargetSchema = schemaProvider.getTargetSchema().toString();
        if (errorTableWriter.isPresent()
            && props.getBoolean(HoodieErrorTableConfig.ERROR_ENABLE_VALIDATE_TARGET_SCHEMA.key(),
            HoodieErrorTableConfig.ERROR_ENABLE_VALIDATE_TARGET_SCHEMA.defaultValue())) {
          avroRDDOptional = dataAndCheckpoint.getBatch().map(
              records -> {
                Tuple2<RDD<GenericRecord>, RDD<String>> safeCreateRDDs = HoodieSparkUtils.safeRewriteRDD(records.rdd(), serializedTargetSchema);
                errorTableWriter.get().addErrorEvents(safeCreateRDDs._2().toJavaRDD()
                    .map(evStr -> new ErrorEvent<>(evStr,
                        ErrorEvent.ErrorReason.INVALID_RECORD_SCHEMA)));
                return safeCreateRDDs._1.toJavaRDD();
              });
        } else {
          avroRDDOptional = dataAndCheckpoint.getBatch().map(t -> t.mapPartitions(iterator ->
              new LazyCastingIterator(iterator, serializedTargetSchema)));
        }
      }
    }
    if (inputBatchForWriter != null) {
      return Pair.of(inputBatchForWriter, true);
    } else {
      return Pair.of(new InputBatch(avroRDDOptional, checkpoint, schemaProvider), false);
    }
  }

  /**
   * Apply schema reconcile and schema evolution rules(schema on read) and generate new target schema provider.
   *
   * @param incomingSchema schema of the source data
   * @param sourceSchemaProvider Source schema provider.
   * @return the SchemaProvider that can be used as writer schema.
   */
  @VisibleForTesting
  SchemaProvider getDeducedSchemaProvider(Schema incomingSchema, SchemaProvider sourceSchemaProvider, HoodieTableMetaClient metaClient) {
    Option<Schema> latestTableSchemaOpt = UtilHelpers.getLatestTableSchema(hoodieSparkContext.jsc(), storage, cfg.targetBasePath, metaClient);
    Option<InternalSchema> internalSchemaOpt = HoodieConversionUtils.toJavaOption(
        HoodieSchemaUtils.getLatestTableInternalSchema(
            HoodieStreamer.Config.getProps(conf, cfg), metaClient));
    // Deduce proper target (writer's) schema for the input dataset, reconciling its
    // schema w/ the table's one
    Schema targetSchema = HoodieSchemaUtils.deduceWriterSchema(
        HoodieAvroUtils.removeMetadataFields(incomingSchema),
          latestTableSchemaOpt, internalSchemaOpt, props);

    // Override schema provider with the reconciled target schema
    return new DelegatingSchemaProvider(props, hoodieSparkContext.jsc(), sourceSchemaProvider,
                new SimpleSchemaProvider(hoodieSparkContext.jsc(), targetSchema, props));
  }

  private JavaRDD<GenericRecord> getTransformedRDD(Dataset<Row> rowDataset, boolean reconcileSchema, Schema readerSchema) {
    return HoodieSparkUtils.createRdd(rowDataset, HOODIE_RECORD_STRUCT_NAME, HOODIE_RECORD_NAMESPACE, reconcileSchema,
        Option.ofNullable(readerSchema)).toJavaRDD();
  }

  private HoodieWriteConfig prepareHoodieConfigForRowWriter(Schema writerSchema) {
    HoodieConfig hoodieConfig = new HoodieConfig(HoodieStreamer.Config.getProps(conf, cfg));
    hoodieConfig.setValue(DataSourceWriteOptions.TABLE_TYPE(), cfg.tableType);
    hoodieConfig.setValue(DataSourceWriteOptions.PAYLOAD_CLASS_NAME().key(), cfg.payloadClassName);
    hoodieConfig.setValue(DataSourceWriteOptions.RECORD_MERGE_MODE().key(), cfg.recordMergeMode.name());
    hoodieConfig.setValue(DataSourceWriteOptions.RECORD_MERGE_STRATEGY_ID().key(), cfg.recordMergeStrategyId);
    hoodieConfig.setValue(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), HoodieSparkKeyGeneratorFactory.getKeyGeneratorClassName(props));
    hoodieConfig.setValue("path", cfg.targetBasePath);
    return HoodieSparkSqlWriter.getBulkInsertRowConfig(writerSchema != InputBatch.NULL_SCHEMA ? Option.of(writerSchema) : Option.empty(),
        hoodieConfig, cfg.targetBasePath, cfg.targetTableName);
  }

  /**
   * Perform Hoodie Write. Run Cleaner, schedule compaction and syncs to hive if needed.
   *
   * @param metaClient          meta client for the table
   * @param inputBatch          input batch that contains the records, checkpoint, and schema provider
   * @param useRowWriter        whether to use row writer
   * @param metrics             Metrics
   * @param overallTimerContext Timer Context
   * @return Option Compaction instant if one is scheduled
   */
  private Pair<Option<String>, JavaRDD<WriteStatus>> writeToSinkAndDoMetaSync(HoodieTableMetaClient metaClient, InputBatch inputBatch,
                                                                              boolean useRowWriter,
                                                                              HoodieIngestionMetrics metrics,
                                                                              Timer.Context overallTimerContext) {
    boolean releaseResourcesInvoked = false;
    String instantTime = startCommit(metaClient, !autoGenerateRecordKeys);
    try {
      Option<String> scheduledCompactionInstant = Option.empty();
      // write to hudi and fetch result
      WriteClientWriteResult writeClientWriteResult = writeToSink(inputBatch, instantTime, useRowWriter);
      Map<String, List<String>> partitionToReplacedFileIds = writeClientWriteResult.getPartitionToReplacedFileIds();
      JavaRDD<WriteStatus> writeStatusRDD = writeClientWriteResult.getWriteStatusRDD();
      Option<JavaRDD<WriteStatus>> errorTableWriteStatusRDDOpt = Option.empty();
      if (errorTableWriter.isPresent() && isErrorTableWriteUnificationEnabled) {
        errorTableWriteStatusRDDOpt = errorTableWriter.map(w -> w.upsert(instantTime, getLatestCommittedInstant()));
      }

      Map<String, String> checkpointCommitMetadata = extractCheckpointMetadata(inputBatch, props, writeClient.getConfig().getWriteVersion().versionCode(), cfg);
      AtomicLong totalSuccessfulRecords = new AtomicLong(0);
      Option<String> latestCommittedInstant = getLatestCommittedInstant();
      WriteStatusValidator writeStatusValidator = new HoodieStreamerWriteStatusValidator(cfg.commitOnErrors, instantTime,
          cfg, errorTableWriter, errorTableWriteStatusRDDOpt, errorWriteFailureStrategy, isErrorTableWriteUnificationEnabled, writeClient, latestCommittedInstant,
          totalSuccessfulRecords);
      String commitActionType = CommitUtils.getCommitActionType(cfg.operation, HoodieTableType.valueOf(cfg.tableType));

      boolean success = writeClient.commit(instantTime, writeStatusRDD, Option.of(checkpointCommitMetadata), commitActionType, partitionToReplacedFileIds, Option.empty(),
          Option.of(writeStatusValidator));
      releaseResourcesInvoked = true;
      if (success) {
        LOG.info("Commit " + instantTime + " successful!");
        this.formatAdapter.getSource().onCommit(inputBatch.getCheckpointForNextBatch() != null
            ? inputBatch.getCheckpointForNextBatch().getCheckpointKey() : null);
        // Schedule compaction if needed
        if (cfg.isAsyncCompactionEnabled()) {
          scheduledCompactionInstant = writeClient.scheduleCompaction(Option.empty());
        }

        if ((totalSuccessfulRecords.get() > 0) || cfg.forceEmptyMetaSync) {
          runMetaSync();
        } else {
          LOG.info(String.format("Not running metaSync totalSuccessfulRecords=%d", totalSuccessfulRecords.get()));
        }
      } else {
        LOG.info("Commit " + instantTime + " failed!");
        throw new HoodieStreamerWriteException("Commit " + instantTime + " failed!");
      }

      long overallTimeNanos = overallTimerContext != null ? overallTimerContext.stop() : 0;

      // Send DeltaStreamer Metrics
      metrics.updateStreamerMetrics(overallTimeNanos);
      return Pair.of(scheduledCompactionInstant, writeStatusRDD);
    } finally {
      if (!releaseResourcesInvoked) {
        releaseResources(instantTime);
      }
    }
  }

  Map<String, String> extractCheckpointMetadata(InputBatch inputBatch, TypedProperties props, int versionCode, HoodieStreamer.Config cfg) {
    // If checkpoint force skip is enabled, return empty map
    if (getBooleanWithAltKeys(props, CHECKPOINT_FORCE_SKIP)) {
      return Collections.emptyMap();
    }

    // If we have a next checkpoint batch, use its metadata
    if (inputBatch.getCheckpointForNextBatch() != null) {
      return inputBatch.getCheckpointForNextBatch()
          .getCheckpointCommitMetadata(cfg.checkpoint, cfg.ignoreCheckpoint);
    }

    // Otherwise create new checkpoint based on version
    Checkpoint checkpoint = buildCheckpointFromGeneralSource(cfg.sourceClassName, versionCode, null);

    return checkpoint.getCheckpointCommitMetadata(cfg.checkpoint, cfg.ignoreCheckpoint);
  }

  /**
   * Try to start a new commit.
   * <p>
   * Exception will be thrown if it failed in 2 tries.
   *
   * @return Instant time of the commit
   */
  private String startCommit(HoodieTableMetaClient metaClient, boolean retryEnabled) {
    final int maxRetries = 2;
    int retryNum = 1;
    RuntimeException lastException = null;
    while (retryNum <= maxRetries) {
      try {
        String commitActionType = CommitUtils.getCommitActionType(cfg.operation, HoodieTableType.valueOf(cfg.tableType));
        return writeClient.startCommit(commitActionType, metaClient);
      } catch (IllegalArgumentException ie) {
        lastException = ie;
        if (!retryEnabled) {
          throw ie;
        }
        LOG.error("Got error trying to start a new commit. Retrying after sleeping for a sec", ie);
        retryNum++;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // No-Op
        }
      }
    }
    throw lastException;
  }

  private WriteClientWriteResult writeToSink(InputBatch inputBatch, String instantTime, boolean useRowWriter) {
    WriteClientWriteResult writeClientWriteResult = null;

    if (useRowWriter) {
      Dataset<Row> df = (Dataset<Row>) inputBatch.getBatch().orElseGet(() -> hoodieSparkContext.getSqlContext().emptyDataFrame());
      HoodieWriteConfig hoodieWriteConfig = prepareHoodieConfigForRowWriter(inputBatch.getSchemaProvider().getTargetSchema());
      BaseDatasetBulkInsertCommitActionExecutor executor = new HoodieStreamerDatasetBulkInsertCommitActionExecutor(hoodieWriteConfig, writeClient);
      writeClientWriteResult = new WriteClientWriteResult(executor.execute(df, !HoodieStreamerUtils.getPartitionColumns(props).isEmpty()).getWriteStatuses());
    } else {
      HoodieRecordType recordType = createRecordMerger(props).getRecordType();
      Option<JavaRDD<HoodieRecord>> recordsOption = HoodieStreamerUtils.createHoodieRecords(cfg, props, inputBatch.getBatch(), inputBatch.getSchemaProvider(),
          recordType, autoGenerateRecordKeys, instantTime, errorTableWriter);
      JavaRDD<HoodieRecord> records = recordsOption.orElseGet(() -> hoodieSparkContext.emptyRDD());
      // filter dupes if needed
      if (cfg.filterDupes) {
        records = DataSourceUtils.handleDuplicates(hoodieSparkContext, records, writeClient.getConfig(), false);
      }

      HoodieWriteResult writeResult = null;
      switch (cfg.operation) {
        case INSERT:
          writeClientWriteResult = new WriteClientWriteResult(writeClient.insert(records, instantTime));
          break;
        case UPSERT:
          writeClientWriteResult = new WriteClientWriteResult(writeClient.upsert(records, instantTime));
          break;
        case BULK_INSERT:
          writeClientWriteResult = new WriteClientWriteResult(writeClient.bulkInsert(records, instantTime, createUserDefinedBulkInsertPartitioner(writeClient.getConfig())));
          break;
        case INSERT_OVERWRITE:
          writeResult = writeClient.insertOverwrite(records, instantTime);
          writeClientWriteResult = new WriteClientWriteResult(writeResult.getWriteStatuses());
          writeClientWriteResult.setPartitionToReplacedFileIds(writeResult.getPartitionToReplaceFileIds());
          break;
        case INSERT_OVERWRITE_TABLE:
          writeResult = writeClient.insertOverwriteTable(records, instantTime);
          writeClientWriteResult = new WriteClientWriteResult(writeResult.getWriteStatuses());
          writeClientWriteResult.setPartitionToReplacedFileIds(writeResult.getPartitionToReplaceFileIds());
          break;
        case DELETE_PARTITION:
          List<String> partitions = records.map(record -> record.getPartitionPath()).distinct().collect();
          writeResult = writeClient.deletePartitions(partitions, instantTime);
          writeClientWriteResult = new WriteClientWriteResult(writeResult.getWriteStatuses());
          writeClientWriteResult.setPartitionToReplacedFileIds(writeResult.getPartitionToReplaceFileIds());
          break;
        default:
          throw new HoodieStreamerException("Unknown operation : " + cfg.operation);
      }
    }
    return writeClientWriteResult;
  }

  private String getSyncClassShortName(String syncClassName) {
    return syncClassName.substring(syncClassName.lastIndexOf(".") + 1);
  }

  public void runMetaSync() {
    List<String> syncClientToolClasses = Arrays.stream(cfg.syncClientToolClassNames.split(",")).distinct().collect(Collectors.toList());
    // for backward compatibility
    if (cfg.enableHiveSync) {
      cfg.enableMetaSync = true;
      syncClientToolClasses.add(HiveSyncTool.class.getName());
      LOG.info("When set --enable-hive-sync will use HiveSyncTool for backward compatibility");
    }
    if (cfg.enableMetaSync && !syncClientToolClasses.isEmpty()) {
      LOG.debug("[MetaSync] Starting sync");
      HoodieTableMetaClient metaClient;
      try {
        metaClient = initializeMetaClient();
      } catch (IOException ex) {
        throw new HoodieIOException("Failed to load meta client", ex);
      }
      FileSystem fs = HadoopFSUtils.getFs(cfg.targetBasePath, hoodieSparkContext.hadoopConfiguration());

      TypedProperties metaProps = new TypedProperties();
      metaProps.putAll(props);
      metaProps.putAll(writeClient.getConfig().getProps());
      if (props.getBoolean(HIVE_SYNC_BUCKET_SYNC.key(), HIVE_SYNC_BUCKET_SYNC.defaultValue())) {
        metaProps.put(HIVE_SYNC_BUCKET_SYNC_SPEC.key(), HiveSyncConfig.getBucketSpec(props.getString(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key()),
            props.getInteger(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key())));
      }
      // Pass remote file system view properties to meta sync if present
      if (writeClient.getConfig().getViewStorageConfig() != null) {
        metaProps.putAll(writeClient.getConfig().getViewStorageConfig().getProps());
      }

      Map<String, HoodieException> failedMetaSyncs = new HashMap<>();
      for (String impl : syncClientToolClasses) {
        if (impl.trim().isEmpty()) {
          LOG.warn("Cannot run MetaSync with empty class name");
          continue;
        }

        Timer.Context syncContext = metrics.getMetaSyncTimerContext();
        Option<HoodieMetaSyncException> metaSyncException = Option.empty();
        try {
          SyncUtilHelpers.runHoodieMetaSync(impl.trim(), metaProps, conf, fs, cfg.targetBasePath, cfg.baseFileFormat, Option.of(metaClient));
        } catch (HoodieMetaSyncException e) {
          metaSyncException = Option.of(e);
        }
        logMetaSync(impl, syncContext, failedMetaSyncs,  metaSyncException);
      }
      if (!failedMetaSyncs.isEmpty()) {
        throw getHoodieMetaSyncException(failedMetaSyncs);
      }
    }
  }

  private void logMetaSync(String impl, Timer.Context syncContext, Map<String, HoodieException> failedMetaSyncs, Option<HoodieMetaSyncException> metaSyncException) {
    long metaSyncTimeNanos = syncContext != null ? syncContext.stop() : 0;
    metrics.updateStreamerMetaSyncMetrics(getSyncClassShortName(impl), metaSyncTimeNanos);
    long timeMs = metaSyncTimeNanos / 1000000L;
    String timeString = String.format("and took %d s %d ms ", timeMs / 1000L, timeMs % 1000L);
    if (metaSyncException.isPresent()) {
      LOG.error("[MetaSync] SyncTool class {} failed with exception {} {}", impl.trim(), metaSyncException.get(), timeString);
      failedMetaSyncs.put(impl, metaSyncException.get());
    } else {
      LOG.info("[MetaSync] SyncTool class {} completed successfully {}", impl.trim(), timeString);
    }
  }

  /**
   * Note that depending on configs and source-type, schemaProvider could either be eagerly or lazily created.
   * SchemaProvider creation is a precursor to HoodieWriteClient and AsyncCompactor creation. This method takes care of
   * this constraint.
   */
  private void setupWriteClient(Option<JavaRDD<HoodieRecord>> recordsOpt, HoodieTableMetaClient metaClient) throws IOException {
    if (null != schemaProvider) {
      Schema sourceSchema = schemaProvider.getSourceSchema();
      Schema targetSchema = schemaProvider.getTargetSchema();
      reInitWriteClient(sourceSchema, targetSchema, recordsOpt, metaClient);
    }
  }

  private void reInitWriteClient(Schema sourceSchema, Schema targetSchema, Option<JavaRDD<HoodieRecord>> recordsOpt, HoodieTableMetaClient metaClient) throws IOException {
    LOG.info("Setting up new Hoodie Write Client");
    if (HoodieStreamerUtils.isDropPartitionColumns(props)) {
      targetSchema = HoodieAvroUtils.removeFields(targetSchema, HoodieStreamerUtils.getPartitionColumns(props));
    }
    final Pair<HoodieWriteConfig, Schema> initialWriteConfigAndSchema = getHoodieClientConfigAndWriterSchema(targetSchema, true, metaClient);
    final HoodieWriteConfig initialWriteConfig = initialWriteConfigAndSchema.getLeft();
    registerAvroSchemas(sourceSchema, initialWriteConfigAndSchema.getRight());
    final HoodieWriteConfig writeConfig = SparkSampleWritesUtils
        .getWriteConfigWithRecordSizeEstimate(hoodieSparkContext.jsc(), recordsOpt, initialWriteConfig)
        .orElse(initialWriteConfig);

    if (writeConfig.isEmbeddedTimelineServerEnabled()) {
      if (!embeddedTimelineService.isPresent()) {
        embeddedTimelineService = Option.of(EmbeddedTimelineServerHelper.createEmbeddedTimelineService(hoodieSparkContext, writeConfig));
      } else {
        EmbeddedTimelineServerHelper.updateWriteConfigWithTimelineServer(embeddedTimelineService.get(), writeConfig);
      }
    }

    if (writeClient != null) {
      // Close Write client.
      writeClient.close();
    }
    writeClient = new SparkRDDWriteClient<>(hoodieSparkContext, writeConfig, embeddedTimelineService);
    onInitializingHoodieWriteClient.apply(writeClient);
  }

  protected void releaseResources(String instantTime) {
    // if commitStats is not invoked, lets release resources from StreamSync layer so that we close all corresponding resources like stopping heart beats for failed writes.
    writeClient.releaseResources(instantTime);
  }

  /**
   * Helper to construct Write Client config without a schema.
   */
  private HoodieWriteConfig getHoodieClientConfig() {
    return getHoodieClientConfigAndWriterSchema(null, false, null).getLeft();
  }

  /**
   * Helper to construct Write Client config.
   *
   * @param schema initial writer schema. If null or Avro Null type, the schema will be fetched from previous commit metadata for the table.
   * @param requireSchemaInConfig whether the schema should be present in the config. This is an optimization to avoid fetching schema from previous commits if not needed.
   *
   * @return Pair of HoodieWriteConfig and writer schema.
   */
  private Pair<HoodieWriteConfig, Schema> getHoodieClientConfigAndWriterSchema(Schema schema, boolean requireSchemaInConfig, HoodieTableMetaClient metaClient) {
    final boolean combineBeforeUpsert = true;

    // NOTE: Provided that we're injecting combined properties
    //       (from {@code props}, including CLI overrides), there's no
    //       need to explicitly set up some configuration aspects that
    //       are based on these (for ex Clustering configuration)
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder()
            .withPath(cfg.targetBasePath)
            .combineInput(cfg.filterDupes, combineBeforeUpsert)
            .withCompactionConfig(
                HoodieCompactionConfig.newBuilder()
                    .withInlineCompaction(cfg.isInlineCompactionEnabled())
                    .build()
            )
            .withPayloadConfig(
                HoodiePayloadConfig.newBuilder()
                    .withPayloadClass(cfg.payloadClassName)
                    .withPayloadOrderingFields(cfg.sourceOrderingFields)
                    .build())
            .withRecordMergeMode(cfg.recordMergeMode)
            .withRecordMergeStrategyId(cfg.recordMergeStrategyId)
            .withRecordMergeImplClasses(cfg.recordMergeImplClasses)
            .forTable(cfg.targetTableName)
            .withProps(props);

    // If schema is required in the config, we need to handle the case where the target schema is null and should be fetched from previous commits
    final Schema returnSchema;
    if (requireSchemaInConfig) {
      returnSchema = getSchemaForWriteConfig(schema, metaClient);
      builder.withSchema(returnSchema.toString());
    } else {
      returnSchema = schema;
    }

    HoodieWriteConfig config = builder.build();

    if (config.writeCommitCallbackOn()) {
      // set default value for {@link HoodieWriteCommitKafkaCallbackConfig} if needed.
      if (HoodieWriteCommitKafkaCallback.class.getName().equals(config.getCallbackClass())) {
        HoodieWriteCommitKafkaCallbackConfig.setCallbackKafkaConfigIfNeeded(config);
      }

      // set default value for {@link HoodieWriteCommitPulsarCallbackConfig} if needed.
      if (HoodieWriteCommitPulsarCallback.class.getName().equals(config.getCallbackClass())) {
        HoodieWriteCommitPulsarCallbackConfig.setCallbackPulsarConfigIfNeeded(config);
      }
    }

    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.from(props);

    // Validate what deltastreamer assumes of write-config to be really safe
    ValidationUtils.checkArgument(config.inlineCompactionEnabled() == cfg.isInlineCompactionEnabled(),
        String.format("%s should be set to %s", INLINE_COMPACT.key(), cfg.isInlineCompactionEnabled()));
    ValidationUtils.checkArgument(config.inlineClusteringEnabled() == clusteringConfig.isInlineClusteringEnabled(),
        String.format("%s should be set to %s", INLINE_CLUSTERING.key(), clusteringConfig.isInlineClusteringEnabled()));
    ValidationUtils.checkArgument(config.isAsyncClusteringEnabled() == clusteringConfig.isAsyncClusteringEnabled(),
        String.format("%s should be set to %s", ASYNC_CLUSTERING_ENABLE.key(), clusteringConfig.isAsyncClusteringEnabled()));
    ValidationUtils.checkArgument(config.shouldCombineBeforeInsert() == cfg.filterDupes,
        String.format("%s should be set to %s", COMBINE_BEFORE_INSERT.key(), cfg.filterDupes));
    ValidationUtils.checkArgument(config.shouldCombineBeforeUpsert(),
        String.format("%s should be set to %s", COMBINE_BEFORE_UPSERT.key(), combineBeforeUpsert));
    return Pair.of(config, returnSchema);
  }

  private Schema getSchemaForWriteConfig(Schema targetSchema, HoodieTableMetaClient metaClient) {
    Schema newWriteSchema = targetSchema;
    try {
      // check if targetSchema is equal to NULL schema
      if (targetSchema == null || (SchemaCompatibility.checkReaderWriterCompatibility(targetSchema, InputBatch.NULL_SCHEMA).getType() == SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE
          && SchemaCompatibility.checkReaderWriterCompatibility(InputBatch.NULL_SCHEMA, targetSchema).getType() == SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE)) {
        // target schema is null. fetch schema from commit metadata and use it
        int totalCompleted = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants();
        if (totalCompleted > 0) {
          TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
          Option<Schema> tableSchema = schemaResolver.getTableAvroSchemaIfPresent(false);
          if (tableSchema.isPresent()) {
            newWriteSchema = tableSchema.get();
          } else {
            LOG.warn("Could not fetch schema from table. Falling back to using target schema from schema provider");
          }
        }
      }
      return newWriteSchema;
    } catch (Exception e) {
      throw new HoodieSchemaFetchException("Failed to fetch schema from table", e);
    }
  }

  /**
   * Register Avro Schemas.
   *
   * @param schemaProvider Schema Provider
   */
  private void registerAvroSchemas(SchemaProvider schemaProvider) {
    if (null != schemaProvider) {
      registerAvroSchemas(schemaProvider.getSourceSchema(), schemaProvider.getTargetSchema());
    }
  }

  /**
   * Register Avro Schemas.
   *
   * @param sourceSchema Source Schema
   * @param targetSchema Target Schema
   */
  private void registerAvroSchemas(Schema sourceSchema, Schema targetSchema) {
    // register the schemas, so that shuffle does not serialize the full schemas
    List<Schema> schemas = new ArrayList<>();
    if (sourceSchema != null) {
      schemas.add(sourceSchema);
    }
    if (targetSchema != null) {
      schemas.add(targetSchema);
    }
    if (!schemas.isEmpty()) {
      LOG.debug("Registering Schema: {}", schemas);
      // Use the underlying spark context in case the java context is changed during runtime
      hoodieSparkContext.getJavaSparkContext().sc().getConf().registerAvroSchemas(JavaScalaConverters.convertJavaListToScalaList(schemas).toList());
    }
  }

  /**
   * Close all resources.
   */
  @Override
  public void close() {
    if (writeClient != null) {
      writeClient.close();
      writeClient = null;
    }

    if (formatAdapter != null) {
      formatAdapter.close();
    }

    LOG.info("Shutting down embedded timeline server");
    if (embeddedTimelineService.isPresent()) {
      embeddedTimelineService.get().stopForBasePath(cfg.targetBasePath);
    }

    if (metrics != null) {
      metrics.shutdown();
    }

  }

  public HoodieStorage getStorage() {
    return storage;
  }

  public TypedProperties getProps() {
    return props;
  }

  public Config getCfg() {
    return cfg;
  }

  public Option<HoodieTimeline> getCommitsTimelineOpt() {
    return commitsTimelineOpt;
  }

  public HoodieIngestionMetrics getMetrics() {
    return metrics;
  }

  /**
   * Schedule clustering.
   * Called from {@link HoodieStreamer} when async clustering is enabled.
   *
   * @return Requested clustering instant.
   */
  public Option<String> getClusteringInstantOpt() {
    if (writeClient != null) {
      return writeClient.scheduleClustering(Option.empty());
    } else {
      return Option.empty();
    }
  }

  private Option<String> getLatestCommittedInstant() {
    try {
      // If timelineLayout version changes, initialize the meta client again.
      if (commitsTimelineOpt.get().getTimelineLayoutVersion() != writeClient.getConfig().getWriteVersion().getTimelineLayoutVersion()) {
        initializeMetaClientAndRefreshTimeline();
      }
      return getLatestInstantWithValidCheckpointInfo(commitsTimelineOpt);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to load meta client", e);
    }
  }

  class WriteClientWriteResult {
    private Map<String, List<String>> partitionToReplacedFileIds = Collections.emptyMap();
    private final JavaRDD<WriteStatus> writeStatusRDD;

    public WriteClientWriteResult(JavaRDD<WriteStatus> writeStatusRDD) {
      this.writeStatusRDD = writeStatusRDD;
    }

    public Map<String, List<String>> getPartitionToReplacedFileIds() {
      return partitionToReplacedFileIds;
    }

    public void setPartitionToReplacedFileIds(Map<String, List<String>> partitionToReplacedFileIds) {
      this.partitionToReplacedFileIds = partitionToReplacedFileIds;
    }

    public JavaRDD<WriteStatus> getWriteStatusRDD() {
      return writeStatusRDD;
    }
  }

  /**
   * WriteStatus Validator for commits to hoodie streamer data table.
   * The writes to error table is taken care as well.
   */
  static class HoodieStreamerWriteStatusValidator implements WriteStatusValidator {

    private final boolean commitOnErrors;
    private final String instantTime;
    private final HoodieStreamer.Config cfg;
    private final Option<BaseErrorTableWriter> errorTableWriter;
    private final Option<JavaRDD<WriteStatus>> errorTableWriteStatusRDDOpt;
    private final HoodieErrorTableConfig.ErrorWriteFailureStrategy errorWriteFailureStrategy;
    private final boolean isErrorTableWriteUnificationEnabled;
    private final SparkRDDWriteClient writeClient;
    private final Option<String> latestCommittedInstant;
    private final AtomicLong totalSuccessfulRecords;

    HoodieStreamerWriteStatusValidator(boolean commitOnErrors,
                                       String instantTime,
                                       HoodieStreamer.Config cfg,
                                       Option<BaseErrorTableWriter> errorTableWriter,
                                       Option<JavaRDD<WriteStatus>> errorTableWriteStatusRDDOpt,
                                       HoodieErrorTableConfig.ErrorWriteFailureStrategy errorWriteFailureStrategy,
                                       boolean isErrorTableWriteUnificationEnabled,
                                       SparkRDDWriteClient writeClient,
                                       Option<String> latestCommittedInstant,
                                       AtomicLong totalSuccessfulRecords) {
      this.commitOnErrors = commitOnErrors;
      this.instantTime = instantTime;
      this.cfg = cfg;
      this.errorTableWriter = errorTableWriter;
      this.errorTableWriteStatusRDDOpt = errorTableWriteStatusRDDOpt;
      this.errorWriteFailureStrategy = errorWriteFailureStrategy;
      this.isErrorTableWriteUnificationEnabled = isErrorTableWriteUnificationEnabled;
      this.writeClient = writeClient;
      this.latestCommittedInstant = latestCommittedInstant;
      this.totalSuccessfulRecords = totalSuccessfulRecords;
    }

    @Override
    public boolean validate(long tableTotalRecords, long tableTotalErroredRecords, Option<HoodieData<WriteStatus>> writeStatusesOpt) {

      long totalRecords = tableTotalRecords;
      long totalErroredRecords = tableTotalErroredRecords;
      if (isErrorTableWriteUnificationEnabled) {
        totalRecords += errorTableWriteStatusRDDOpt.map(status -> status.mapToDouble(WriteStatus::getTotalRecords).sum().longValue()).orElse(0L);
        totalErroredRecords += errorTableWriteStatusRDDOpt.map(status -> status.mapToDouble(WriteStatus::getTotalErrorRecords).sum().longValue()).orElse(0L);
      }
      long totalSuccessfulRecords = totalRecords - totalErroredRecords;
      this.totalSuccessfulRecords.set(totalSuccessfulRecords);
      LOG.info("instantTime={}, totalRecords={}, totalErrorRecords={}, totalSuccessfulRecords={}",
          instantTime, totalRecords, totalErroredRecords, totalSuccessfulRecords);
      if (totalRecords == 0) {
        LOG.info("No new data, perform empty commit.");
      }
      boolean hasErrorRecords = totalErroredRecords > 0;
      if (!hasErrorRecords || commitOnErrors) {
        if (hasErrorRecords) {
          LOG.warn("Some records failed to be merged but forcing commit since commitOnErrors set. Errors/Total="
              + totalErroredRecords + "/" + totalRecords);
        }
      }

      if (errorTableWriter.isPresent()) {
        boolean errorTableSuccess = true;
        // Commit the error events triggered so far to the error table
        if (isErrorTableWriteUnificationEnabled && errorTableWriteStatusRDDOpt.isPresent()) {
          errorTableSuccess = errorTableWriter.get().commit(errorTableWriteStatusRDDOpt.get());
        } else if (!isErrorTableWriteUnificationEnabled) {
          errorTableSuccess = errorTableWriter.get().upsertAndCommit(instantTime, latestCommittedInstant);
        }
        if (!errorTableSuccess) {
          switch (errorWriteFailureStrategy) {
            case ROLLBACK_COMMIT:
              LOG.info("Commit " + instantTime + " failed!");
              writeClient.rollback(instantTime);
              throw new HoodieStreamerWriteException("Error table commit failed");
            case LOG_ERROR:
              LOG.error("Error Table write failed for instant " + instantTime);
              break;
            default:
              throw new HoodieStreamerWriteException("Write failure strategy not implemented for " + errorWriteFailureStrategy);
          }
        }
      }
      boolean canProceed = !hasErrorRecords || commitOnErrors;
      if (canProceed) {
        return canProceed;
      } else {
        LOG.error("Delta Sync found errors when writing. Errors/Total=" + totalErroredRecords + "/" + totalRecords);
        LOG.error("Printing out the top 100 errors");
        ValidationUtils.checkArgument(writeStatusesOpt.isPresent(), "RDD <WriteStatus> is expected to be present when there are errors ");
        HoodieJavaRDD.getJavaRDD(writeStatusesOpt.get()).filter(WriteStatus::hasErrors).take(100).forEach(writeStatus ->  {
          LOG.error("Global error " + writeStatus.getGlobalError());
          if (!writeStatus.getErrors().isEmpty()) {
            writeStatus.getErrors().forEach((k,v) -> {
              LOG.trace("Error for key %s : %s ", k, v);
            });
          }
        });
        // Rolling back instant
        writeClient.rollback(instantTime);
        throw new HoodieStreamerWriteException("Commit " + instantTime + " failed and rolled-back !");
      }
    }
  }
}
