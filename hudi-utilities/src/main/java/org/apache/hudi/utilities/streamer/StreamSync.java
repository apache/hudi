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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieMetaSyncException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.sync.common.util.SyncUtilHelpers;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.util.SparkKeyGenUtils;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallback;
import org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallbackConfig;
import org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallback;
import org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig;
import org.apache.hudi.utilities.config.KafkaSourceConfig;
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
import org.apache.hadoop.fs.Path;
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
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.JavaConversions;

import static org.apache.hudi.avro.AvroSchemaUtils.getAvroRecordQualifiedName;
import static org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER;
import static org.apache.hudi.common.table.HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE;
import static org.apache.hudi.common.table.HoodieTableConfig.URL_ENCODE_PARTITIONING;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.removeConfigFromProps;
import static org.apache.hudi.config.HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE;
import static org.apache.hudi.config.HoodieClusteringConfig.INLINE_CLUSTERING;
import static org.apache.hudi.config.HoodieCompactionConfig.INLINE_COMPACT;
import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_ENABLED;
import static org.apache.hudi.config.HoodieWriteConfig.AUTO_COMMIT_ENABLE;
import static org.apache.hudi.config.HoodieWriteConfig.COMBINE_BEFORE_INSERT;
import static org.apache.hudi.config.HoodieWriteConfig.COMBINE_BEFORE_UPSERT;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_BUCKET_SYNC;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_BUCKET_SYNC_SPEC;
import static org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory.getKeyGeneratorClassName;
import static org.apache.hudi.sync.common.util.SyncUtilHelpers.getHoodieMetaSyncException;
import static org.apache.hudi.utilities.UtilHelpers.createRecordMerger;
import static org.apache.hudi.utilities.config.HoodieStreamerConfig.CHECKPOINT_FORCE_SKIP;
import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.CHECKPOINT_KEY;
import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.CHECKPOINT_RESET_KEY;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME;

/**
 * Sync's one batch of data to hoodie table.
 */
public class StreamSync implements Serializable, Closeable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(StreamSync.class);
  private static final String NULL_PLACEHOLDER = "[null]";

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

  private String keyGenClassName;

  /**
   * Filesystem used.
   */
  private transient FileSystem fs;

  /**
   * Spark context Wrapper.
   */
  private final transient HoodieSparkEngineContext hoodieSparkContext;

  /**
   * Spark Session.
   */
  private transient SparkSession sparkSession;

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

  private final boolean useRowWriter;

  @Deprecated
  public StreamSync(HoodieStreamer.Config cfg, SparkSession sparkSession, SchemaProvider schemaProvider,
                    TypedProperties props, JavaSparkContext jssc, FileSystem fs, Configuration conf,
                    Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient) throws IOException {
    this(cfg, sparkSession, schemaProvider, props, new HoodieSparkEngineContext(jssc), fs, conf, onInitializingHoodieWriteClient);
  }

  public StreamSync(HoodieStreamer.Config cfg, SparkSession sparkSession, SchemaProvider schemaProvider,
                    TypedProperties props, HoodieSparkEngineContext hoodieSparkContext, FileSystem fs, Configuration conf,
                    Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient) throws IOException {
    this.cfg = cfg;
    this.hoodieSparkContext = hoodieSparkContext;
    this.sparkSession = sparkSession;
    this.fs = fs;
    this.onInitializingHoodieWriteClient = onInitializingHoodieWriteClient;
    this.props = props;
    this.userProvidedSchemaProvider = schemaProvider;
    this.processedSchema = new SchemaSet();
    this.autoGenerateRecordKeys = KeyGenUtils.enableAutoGenerateRecordKeys(props);
    this.keyGenClassName = getKeyGeneratorClassName(new TypedProperties(props));
    refreshTimeline();
    // Register User Provided schema first
    registerAvroSchemas(schemaProvider);


    this.metrics = (HoodieIngestionMetrics) ReflectionUtils.loadClass(cfg.ingestionMetricsClass, getHoodieClientConfig(this.schemaProvider));
    this.hoodieMetrics = new HoodieMetrics(getHoodieClientConfig(this.schemaProvider));
    this.conf = conf;
    if (props.getBoolean(ERROR_TABLE_ENABLED.key(), ERROR_TABLE_ENABLED.defaultValue())) {
      this.errorTableWriter = ErrorTableUtils.getErrorTableWriter(cfg, sparkSession, props, hoodieSparkContext, fs);
      this.errorWriteFailureStrategy = ErrorTableUtils.getErrorWriteFailureStrategy(props);
    }
    Source source = UtilHelpers.createSource(cfg.sourceClassName, props, hoodieSparkContext.jsc(), sparkSession, schemaProvider, metrics);
    this.formatAdapter = new SourceFormatAdapter(source, this.errorTableWriter, Option.of(props));

    this.transformer = UtilHelpers.createTransformer(Option.ofNullable(cfg.transformerClassNames),
        Option.ofNullable(schemaProvider).map(SchemaProvider::getSourceSchema), this.errorTableWriter.isPresent());
    if (this.cfg.operation == WriteOperationType.BULK_INSERT && source.getSourceType() == Source.SourceType.ROW
        && this.props.getBoolean(DataSourceWriteOptions.ENABLE_ROW_WRITER().key(), false)) {
      // enable row writer only when operation is BULK_INSERT, and source is ROW type and if row writer is not explicitly disabled.
      this.useRowWriter = true;
    } else {
      this.useRowWriter = false;
    }
  }

  /**
   * Refresh Timeline.
   *
   * @throws IOException in case of any IOException
   */
  public void refreshTimeline() throws IOException {
    if (fs.exists(new Path(cfg.targetBasePath))) {
      try {
        HoodieTableMetaClient meta = HoodieTableMetaClient.builder()
            .setConf(new Configuration(fs.getConf()))
            .setBasePath(cfg.targetBasePath)
            .setPayloadClassName(cfg.payloadClassName)
            .setRecordMergerStrategy(props.getProperty(HoodieWriteConfig.RECORD_MERGER_STRATEGY.key(), HoodieWriteConfig.RECORD_MERGER_STRATEGY.defaultValue()))
            .build();
        switch (meta.getTableType()) {
          case COPY_ON_WRITE:
          case MERGE_ON_READ:
            // we can use getCommitsTimeline for both COW and MOR here, because for COW there is no deltacommit
            this.commitsTimelineOpt = Option.of(meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
            this.allCommitsTimelineOpt = Option.of(meta.getActiveTimeline().getAllCommitsTimeline());
            break;
          default:
            throw new HoodieException("Unsupported table type :" + meta.getTableType());
        }
      } catch (HoodieIOException e) {
        LOG.warn("Full exception msg " + e.getMessage());
        if (e.getMessage().contains("Could not load Hoodie properties") && e.getMessage().contains(HoodieTableConfig.HOODIE_PROPERTIES_FILE)) {
          String basePathWithForwardSlash = cfg.targetBasePath.endsWith("/") ? cfg.targetBasePath : String.format("%s/", cfg.targetBasePath);
          String pathToHoodieProps = String.format("%s%s/%s", basePathWithForwardSlash, HoodieTableMetaClient.METAFOLDER_NAME, HoodieTableConfig.HOODIE_PROPERTIES_FILE);
          String pathToHoodiePropsBackup = String.format("%s%s/%s", basePathWithForwardSlash, HoodieTableMetaClient.METAFOLDER_NAME, HoodieTableConfig.HOODIE_PROPERTIES_FILE_BACKUP);
          boolean hoodiePropertiesExists = fs.exists(new Path(basePathWithForwardSlash))
              && fs.exists(new Path(pathToHoodieProps))
              && fs.exists(new Path(pathToHoodiePropsBackup));
          if (!hoodiePropertiesExists) {
            LOG.warn("Base path exists, but table is not fully initialized. Re-initializing again");
            initializeEmptyTable();
            // reload the timeline from metaClient and validate that its empty table. If there are any instants found, then we should fail the pipeline, bcoz hoodie.properties got deleted by mistake.
            HoodieTableMetaClient metaClientToValidate = HoodieTableMetaClient.builder().setConf(new Configuration(fs.getConf())).setBasePath(cfg.targetBasePath).build();
            if (metaClientToValidate.reloadActiveTimeline().countInstants() > 0) {
              // Deleting the recreated hoodie.properties and throwing exception.
              fs.delete(new Path(String.format("%s%s/%s", basePathWithForwardSlash, HoodieTableMetaClient.METAFOLDER_NAME, HoodieTableConfig.HOODIE_PROPERTIES_FILE)));
              throw new HoodieIOException("hoodie.properties is missing. Likely due to some external entity. Please populate the hoodie.properties and restart the pipeline. ",
                  e.getIOException());
            }
          }
        } else {
          throw e;
        }
      }
    } else {
      initializeEmptyTable();
    }
  }

  private void initializeEmptyTable() throws IOException {
    this.commitsTimelineOpt = Option.empty();
    this.allCommitsTimelineOpt = Option.empty();
    String partitionColumns = SparkKeyGenUtils.getPartitionColumns(props);
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(cfg.tableType)
        .setTableName(cfg.targetTableName)
        .setArchiveLogFolder(ARCHIVELOG_FOLDER.defaultValue())
        .setPayloadClassName(cfg.payloadClassName)
        .setBaseFileFormat(cfg.baseFileFormat)
        .setPartitionFields(partitionColumns)
        .setRecordKeyFields(props.getProperty(DataSourceWriteOptions.RECORDKEY_FIELD().key()))
        .setPopulateMetaFields(props.getBoolean(HoodieTableConfig.POPULATE_META_FIELDS.key(),
            HoodieTableConfig.POPULATE_META_FIELDS.defaultValue()))
        .setKeyGeneratorClassProp(keyGenClassName)
        .setPreCombineField(cfg.sourceOrderingField)
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
        .initTable(new Configuration(hoodieSparkContext.hadoopConfiguration()),
            cfg.targetBasePath);
  }

  /**
   * Run one round of delta sync and return new compaction instant if one got scheduled.
   */
  public Pair<Option<String>, JavaRDD<WriteStatus>> syncOnce() throws IOException {
    Pair<Option<String>, JavaRDD<WriteStatus>> result = null;
    Timer.Context overallTimerContext = metrics.getOverallTimerContext();

    // Refresh Timeline
    refreshTimeline();
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(new Configuration(fs.getConf()))
        .setBasePath(cfg.targetBasePath)
        .setRecordMergerStrategy(props.getProperty(HoodieWriteConfig.RECORD_MERGER_STRATEGY.key(), HoodieWriteConfig.RECORD_MERGER_STRATEGY.defaultValue()))
        .setTimeGeneratorConfig(HoodieTimeGeneratorConfig.newBuilder().fromProperties(props).withPath(cfg.targetBasePath).build())
        .build();
    String instantTime = metaClient.createNewInstantTime();

    InputBatch inputBatch = readFromSource(instantTime, metaClient);

    if (inputBatch != null) {

      // this is the first input batch. If schemaProvider not set, use it and register Avro Schema and start
      // compactor
      if (writeClient == null) {
        this.schemaProvider = inputBatch.getSchemaProvider();
        // Setup HoodieWriteClient and compaction now that we decided on schema
        setupWriteClient(inputBatch.getBatch());
      } else {
        Schema newSourceSchema = inputBatch.getSchemaProvider().getSourceSchema();
        Schema newTargetSchema = inputBatch.getSchemaProvider().getTargetSchema();
        if ((newSourceSchema != null && !processedSchema.isSchemaPresent(newSourceSchema))
            || (newTargetSchema != null && !processedSchema.isSchemaPresent(newTargetSchema))) {
          String sourceStr = newSourceSchema == null ? NULL_PLACEHOLDER : newSourceSchema.toString(true);
          String targetStr = newTargetSchema == null ? NULL_PLACEHOLDER : newTargetSchema.toString(true);
          LOG.info("Seeing new schema. Source: {0}, Target: {1}", sourceStr, targetStr);
          // We need to recreate write client with new schema and register them.
          reInitWriteClient(newSourceSchema, newTargetSchema, inputBatch.getBatch());
          if (newSourceSchema != null) {
            processedSchema.addSchema(newSourceSchema);
          }
          if (newTargetSchema != null) {
            processedSchema.addSchema(newTargetSchema);
          }
        }
      }

      // complete the pending compaction before writing to sink
      if (cfg.retryLastPendingInlineCompactionJob && getHoodieClientConfig(this.schemaProvider).inlineCompactionEnabled()) {
        Option<String> pendingCompactionInstant = getLastPendingCompactionInstant(allCommitsTimelineOpt);
        if (pendingCompactionInstant.isPresent()) {
          HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetadata = writeClient.compact(pendingCompactionInstant.get());
          writeClient.commitCompaction(pendingCompactionInstant.get(), writeMetadata.getCommitMetadata().get(), Option.empty());
          refreshTimeline();
          reInitWriteClient(schemaProvider.getSourceSchema(), schemaProvider.getTargetSchema(), null);
        }
      } else if (cfg.retryLastPendingInlineClusteringJob && getHoodieClientConfig(this.schemaProvider).inlineClusteringEnabled()) {
        // complete the pending clustering before writing to sink
        Option<String> pendingClusteringInstant = getLastPendingClusteringInstant(allCommitsTimelineOpt);
        if (pendingClusteringInstant.isPresent()) {
          writeClient.cluster(pendingClusteringInstant.get());
        }
      }

      result = writeToSinkAndDoMetaSync(instantTime, inputBatch, metrics, overallTimerContext);
    }

    metrics.updateStreamerSyncMetrics(System.currentTimeMillis());
    return result;
  }

  private Option<String> getLastPendingClusteringInstant(Option<HoodieTimeline> commitTimelineOpt) {
    if (commitTimelineOpt.isPresent()) {
      Option<HoodieInstant> pendingClusteringInstant = commitTimelineOpt.get().filterPendingReplaceTimeline().lastInstant();
      return pendingClusteringInstant.isPresent() ? Option.of(pendingClusteringInstant.get().getTimestamp()) : Option.empty();
    }
    return Option.empty();
  }

  private Option<String> getLastPendingCompactionInstant(Option<HoodieTimeline> commitTimelineOpt) {
    if (commitTimelineOpt.isPresent()) {
      Option<HoodieInstant> pendingCompactionInstant = commitTimelineOpt.get().filterPendingCompactionTimeline().lastInstant();
      return pendingCompactionInstant.isPresent() ? Option.of(pendingCompactionInstant.get().getTimestamp()) : Option.empty();
    }
    return Option.empty();
  }

  /**
   * Read from Upstream Source and apply transformation if needed.
   *
   * @return Pair<InputBatch and Boolean> Input data read from upstream source, and boolean is true if empty.
   * @throws Exception in case of any Exception
   */

  public InputBatch readFromSource(String instantTime, HoodieTableMetaClient metaClient) throws IOException {
    // Retrieve the previous round checkpoints, if any
    Option<String> resumeCheckpointStr = Option.empty();
    if (commitsTimelineOpt.isPresent()) {
      resumeCheckpointStr = getCheckpointToResume(commitsTimelineOpt);
    }

    LOG.debug("Checkpoint from config: " + cfg.checkpoint);
    if (!resumeCheckpointStr.isPresent() && cfg.checkpoint != null) {
      resumeCheckpointStr = Option.of(cfg.checkpoint);
    }
    LOG.info("Checkpoint to resume from : " + resumeCheckpointStr);

    int maxRetryCount = cfg.retryOnSourceFailures ? cfg.maxRetryCount : 1;
    int curRetryCount = 0;
    InputBatch sourceDataToSync = null;
    while (curRetryCount++ < maxRetryCount && sourceDataToSync == null) {
      try {
        sourceDataToSync = fetchFromSourceAndPrepareRecords(resumeCheckpointStr, instantTime, metaClient);
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

  private InputBatch fetchFromSourceAndPrepareRecords(Option<String> resumeCheckpointStr, String instantTime,
        HoodieTableMetaClient metaClient) {
    HoodieRecordType recordType = createRecordMerger(props).getRecordType();
    if (recordType == HoodieRecordType.SPARK && HoodieTableType.valueOf(cfg.tableType) == HoodieTableType.MERGE_ON_READ
        && !cfg.operation.equals(WriteOperationType.BULK_INSERT)
        && HoodieLogBlockType.fromId(props.getProperty(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "avro"))
        != HoodieLogBlockType.PARQUET_DATA_BLOCK) {
      throw new UnsupportedOperationException("Spark record only support parquet log.");
    }

    InputBatch inputBatch = fetchNextBatchFromSource(resumeCheckpointStr, metaClient);
    final String checkpointStr = inputBatch.getCheckpointForNextBatch();
    final SchemaProvider schemaProvider = inputBatch.getSchemaProvider();

    // handle no new data and no change in checkpoint
    if (!cfg.allowCommitOnNoCheckpointChange && Objects.equals(checkpointStr, resumeCheckpointStr.orElse(null))) {
      LOG.info("No new data, source checkpoint has not changed. Nothing to commit. Old checkpoint=("
          + resumeCheckpointStr + "). New Checkpoint=(" + checkpointStr + ")");
      String commitActionType = CommitUtils.getCommitActionType(cfg.operation, HoodieTableType.valueOf(cfg.tableType));
      hoodieMetrics.updateMetricsForEmptyData(commitActionType);
      return null;
    }

    // handle empty batch with change in checkpoint
    hoodieSparkContext.setJobStatus(this.getClass().getSimpleName(), "Checking if input is empty");


    if (useRowWriter) { // no additional processing required for row writer.
      return inputBatch;
    } else {
      Option<JavaRDD<HoodieRecord>> recordsOpt = HoodieStreamerUtils.createHoodieRecords(cfg, props, inputBatch.getBatch(), schemaProvider,
          recordType, autoGenerateRecordKeys, instantTime);
      return new InputBatch(recordsOpt, checkpointStr, schemaProvider);
    }
  }

  /**
   * Fetch data from source, apply transformations if any, align with schema from schema provider if need be and return the input batch.
   * @param resumeCheckpointStr checkpoint to resume from source.
   * @return {@link InputBatch} containing the new batch of data from source along with new checkpoint and schema provider instance to use.
   */
  private InputBatch fetchNextBatchFromSource(Option<String> resumeCheckpointStr, HoodieTableMetaClient metaClient) {
    Option<JavaRDD<GenericRecord>> avroRDDOptional = null;
    String checkpointStr = null;
    SchemaProvider schemaProvider = null;
    InputBatch inputBatchForWriter = null; // row writer
    boolean reconcileSchema = props.getBoolean(DataSourceWriteOptions.RECONCILE_SCHEMA().key());
    if (transformer.isPresent()) {
      // Transformation is needed. Fetch New rows in Row Format, apply transformation and then convert them
      // to generic records for writing
      InputBatch<Dataset<Row>> dataAndCheckpoint =
          formatAdapter.fetchNewDataInRowFormat(resumeCheckpointStr, cfg.sourceLimit);

      Option<Dataset<Row>> transformed =
          dataAndCheckpoint.getBatch().map(data -> transformer.get().apply(hoodieSparkContext.jsc(), sparkSession, data, props));

      transformed = formatAdapter.processErrorEvents(transformed,
          ErrorEvent.ErrorReason.CUSTOM_TRANSFORMER_FAILURE);

      checkpointStr = dataAndCheckpoint.getCheckpointForNextBatch();
      if (this.userProvidedSchemaProvider != null && this.userProvidedSchemaProvider.getTargetSchema() != null
          && this.userProvidedSchemaProvider.getTargetSchema() != InputBatch.NULL_SCHEMA) {
        if (useRowWriter) {
          inputBatchForWriter = new InputBatch(transformed, checkpointStr, this.userProvidedSchemaProvider);
        } else {
          // non row writer path
          // Let's deduce the schema provider for writer side first!
          schemaProvider = getDeducedSchemaProvider(this.userProvidedSchemaProvider.getTargetSchema(), this.userProvidedSchemaProvider, metaClient);
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
        Option<Schema> incomingSchemaOpt = transformed.map(df ->
            AvroConversionUtils.convertStructTypeToAvroSchema(df.schema(), getAvroRecordQualifiedName(cfg.targetTableName)));

        schemaProvider = incomingSchemaOpt.map(incomingSchema -> getDeducedSchemaProvider(incomingSchema, dataAndCheckpoint.getSchemaProvider(), metaClient))
            .orElse(dataAndCheckpoint.getSchemaProvider());

        if (useRowWriter) {
          inputBatchForWriter = new InputBatch(transformed, checkpointStr, schemaProvider);
        } else {
          // Rewrite transformed records into the expected target schema
          SchemaProvider finalSchemaProvider = schemaProvider;
          avroRDDOptional = transformed.map(t -> getTransformedRDD(t, reconcileSchema, finalSchemaProvider.getTargetSchema()));
        }
      }
    } else {
      if (useRowWriter) {
        inputBatchForWriter = formatAdapter.fetchNewDataInRowFormat(resumeCheckpointStr, cfg.sourceLimit);
      } else {
        // Pull the data from the source & prepare the write
        InputBatch<JavaRDD<GenericRecord>> dataAndCheckpoint = formatAdapter.fetchNewDataInAvroFormat(resumeCheckpointStr, cfg.sourceLimit);
        checkpointStr = dataAndCheckpoint.getCheckpointForNextBatch();
        // Rewrite transformed records into the expected target schema
        schemaProvider = getDeducedSchemaProvider(dataAndCheckpoint.getSchemaProvider().getTargetSchema(), dataAndCheckpoint.getSchemaProvider(), metaClient);
        String serializedTargetSchema = schemaProvider.getTargetSchema().toString();
        avroRDDOptional = dataAndCheckpoint.getBatch().map(t -> t.mapPartitions(iterator ->
            new LazyCastingIterator(iterator, serializedTargetSchema)));
      }
    }
    if (useRowWriter) {
      return inputBatchForWriter;
    } else {
      return new InputBatch(avroRDDOptional, checkpointStr, schemaProvider);
    }
  }

  /**
   * Apply schema reconcile and schema evolution rules(schema on read) and generate new target schema provider.
   *
   * @param incomingSchema schema of the source data
   * @param sourceSchemaProvider Source schema provider.
   * @return the SchemaProvider that can be used as writer schema.
   */
  private SchemaProvider getDeducedSchemaProvider(Schema incomingSchema, SchemaProvider sourceSchemaProvider, HoodieTableMetaClient metaClient) {
    Option<Schema> latestTableSchemaOpt = UtilHelpers.getLatestTableSchema(hoodieSparkContext.jsc(), fs, cfg.targetBasePath, metaClient);
    Option<InternalSchema> internalSchemaOpt = HoodieConversionUtils.toJavaOption(
        HoodieSchemaUtils.getLatestTableInternalSchema(
            new HoodieConfig(HoodieStreamer.Config.getProps(fs, cfg)), metaClient));
    // Deduce proper target (writer's) schema for the input dataset, reconciling its
    // schema w/ the table's one
    Schema targetSchema = HoodieSparkSqlWriter.deduceWriterSchema(
          incomingSchema,
          HoodieConversionUtils.toScalaOption(latestTableSchemaOpt),
          HoodieConversionUtils.toScalaOption(internalSchemaOpt), props);

    // Override schema provider with the reconciled target schema
    return new DelegatingSchemaProvider(props, hoodieSparkContext.jsc(), sourceSchemaProvider,
                new SimpleSchemaProvider(hoodieSparkContext.jsc(), targetSchema, props));
  }

  private JavaRDD<GenericRecord> getTransformedRDD(Dataset<Row> rowDataset, boolean reconcileSchema, Schema readerSchema) {
    return HoodieSparkUtils.createRdd(rowDataset, HOODIE_RECORD_STRUCT_NAME, HOODIE_RECORD_NAMESPACE, reconcileSchema,
        Option.ofNullable(readerSchema)).toJavaRDD();
  }

  /**
   * Process previous commit metadata and checkpoint configs set by user to determine the checkpoint to resume from.
   *
   * @param commitsTimelineOpt commits timeline of interest, including .commit and .deltacommit.
   * @return the checkpoint to resume from if applicable.
   * @throws IOException
   */
  private Option<String> getCheckpointToResume(Option<HoodieTimeline> commitsTimelineOpt) throws IOException {
    Option<String> resumeCheckpointStr = Option.empty();
    // try get checkpoint from commits(including commit and deltacommit)
    // in COW migrating to MOR case, the first batch of the deltastreamer will lost the checkpoint from COW table, cause the dataloss
    HoodieTimeline deltaCommitTimeline = commitsTimelineOpt.get().filter(instant -> instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION));
    // has deltacommit and this is a MOR table, then we should get checkpoint from .deltacommit
    // if changing from mor to cow, before changing we must do a full compaction, so we can only consider .commit in such case
    if (cfg.tableType.equals(HoodieTableType.MERGE_ON_READ.name()) && !deltaCommitTimeline.empty()) {
      commitsTimelineOpt = Option.of(deltaCommitTimeline);
    }
    Option<HoodieInstant> lastCommit = commitsTimelineOpt.get().lastInstant();
    if (lastCommit.isPresent()) {
      // if previous commit metadata did not have the checkpoint key, try traversing previous commits until we find one.
      Option<HoodieCommitMetadata> commitMetadataOption = getLatestCommitMetadataWithValidCheckpointInfo(commitsTimelineOpt.get());
      if (commitMetadataOption.isPresent()) {
        HoodieCommitMetadata commitMetadata = commitMetadataOption.get();
        LOG.debug("Checkpoint reset from metadata: " + commitMetadata.getMetadata(CHECKPOINT_RESET_KEY));
        if (cfg.checkpoint != null && (StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_RESET_KEY))
            || !cfg.checkpoint.equals(commitMetadata.getMetadata(CHECKPOINT_RESET_KEY)))) {
          resumeCheckpointStr = Option.of(cfg.checkpoint);
        } else if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_KEY))) {
          //if previous checkpoint is an empty string, skip resume use Option.empty()
          String value = commitMetadata.getMetadata(CHECKPOINT_KEY);
          resumeCheckpointStr = Option.of(value);
        } else if (HoodieTimeline.compareTimestamps(HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS,
            HoodieTimeline.LESSER_THAN, lastCommit.get().getTimestamp())) {
          throw new HoodieStreamerException(
              "Unable to find previous checkpoint. Please double check if this table "
                  + "was indeed built via delta streamer. Last Commit :" + lastCommit + ", Instants :"
                  + commitsTimelineOpt.get().getInstants());
        }
        // KAFKA_CHECKPOINT_TYPE will be honored only for first batch.
        if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_RESET_KEY))) {
          removeConfigFromProps(props, KafkaSourceConfig.KAFKA_CHECKPOINT_TYPE);
        }
      } else if (cfg.checkpoint != null) { // getLatestCommitMetadataWithValidCheckpointInfo(commitTimelineOpt.get()) will never return a commit metadata w/o any checkpoint key set.
        resumeCheckpointStr = Option.of(cfg.checkpoint);
      }
    }
    return resumeCheckpointStr;
  }

  protected Option<Pair<String, HoodieCommitMetadata>> getLatestInstantAndCommitMetadataWithValidCheckpointInfo(HoodieTimeline timeline) throws IOException {
    return (Option<Pair<String, HoodieCommitMetadata>>) timeline.getReverseOrderedInstants().map(instant -> {
      try {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
            .fromBytes(timeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
        if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_KEY)) || !StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_RESET_KEY))) {
          return Option.of(Pair.of(instant.toString(), commitMetadata));
        } else {
          return Option.empty();
        }
      } catch (IOException e) {
        throw new HoodieIOException("Failed to parse HoodieCommitMetadata for " + instant.toString(), e);
      }
    }).filter(Option::isPresent).findFirst().orElse(Option.empty());
  }

  protected Option<HoodieCommitMetadata> getLatestCommitMetadataWithValidCheckpointInfo(HoodieTimeline timeline) throws IOException {
    return getLatestInstantAndCommitMetadataWithValidCheckpointInfo(timeline).map(pair -> pair.getRight());
  }

  protected Option<String> getLatestInstantWithValidCheckpointInfo(Option<HoodieTimeline> timelineOpt) {
    return timelineOpt.map(timeline -> {
      try {
        return getLatestInstantAndCommitMetadataWithValidCheckpointInfo(timeline).map(pair -> pair.getLeft());
      } catch (IOException e) {
        throw new HoodieIOException("failed to get latest instant with ValidCheckpointInfo", e);
      }
    }).orElse(Option.empty());
  }

  private HoodieWriteConfig prepareHoodieConfigForRowWriter(Schema writerSchema) {
    HoodieConfig hoodieConfig = new HoodieConfig(HoodieStreamer.Config.getProps(fs, cfg));
    hoodieConfig.setValue(DataSourceWriteOptions.TABLE_TYPE(), cfg.tableType);
    hoodieConfig.setValue(DataSourceWriteOptions.PAYLOAD_CLASS_NAME().key(), cfg.payloadClassName);
    hoodieConfig.setValue(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), HoodieSparkKeyGeneratorFactory.getKeyGeneratorClassName(props));
    hoodieConfig.setValue("path", cfg.targetBasePath);
    return HoodieSparkSqlWriter.getBulkInsertRowConfig(writerSchema, hoodieConfig, cfg.targetBasePath, cfg.targetTableName);
  }

  /**
   * Perform Hoodie Write. Run Cleaner, schedule compaction and syncs to hive if needed.
   *
   * @param instantTime         instant time to use for ingest.
   * @param inputBatch          input batch that contains the records, checkpoint, and schema provider
   * @param metrics             Metrics
   * @param overallTimerContext Timer Context
   * @return Option Compaction instant if one is scheduled
   */
  private Pair<Option<String>, JavaRDD<WriteStatus>> writeToSinkAndDoMetaSync(String instantTime, InputBatch inputBatch,
                                                                              HoodieIngestionMetrics metrics,
                                                                              Timer.Context overallTimerContext) {
    Option<String> scheduledCompactionInstant = Option.empty();
    // write to hudi and fetch result
    WriteClientWriteResult  writeClientWriteResult = writeToSink(inputBatch, instantTime);
    JavaRDD<WriteStatus> writeStatusRDD = writeClientWriteResult.getWriteStatusRDD();
    Map<String, List<String>> partitionToReplacedFileIds = writeClientWriteResult.getPartitionToReplacedFileIds();

    // process write status
    long totalErrorRecords = writeStatusRDD.mapToDouble(WriteStatus::getTotalErrorRecords).sum().longValue();
    long totalRecords = writeStatusRDD.mapToDouble(WriteStatus::getTotalRecords).sum().longValue();
    long totalSuccessfulRecords = totalRecords - totalErrorRecords;
    LOG.info(String.format("instantTime=%s, totalRecords=%d, totalErrorRecords=%d, totalSuccessfulRecords=%d",
        instantTime, totalRecords, totalErrorRecords, totalSuccessfulRecords));
    if (totalRecords == 0) {
      LOG.info("No new data, perform empty commit.");
    }
    boolean hasErrors = totalErrorRecords > 0;
    if (!hasErrors || cfg.commitOnErrors) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      if (!getBooleanWithAltKeys(props, CHECKPOINT_FORCE_SKIP)) {
        if (inputBatch.getCheckpointForNextBatch() != null) {
          checkpointCommitMetadata.put(CHECKPOINT_KEY, inputBatch.getCheckpointForNextBatch());
        }
        if (cfg.checkpoint != null) {
          checkpointCommitMetadata.put(CHECKPOINT_RESET_KEY, cfg.checkpoint);
        }
      }

      if (hasErrors) {
        LOG.warn("Some records failed to be merged but forcing commit since commitOnErrors set. Errors/Total="
            + totalErrorRecords + "/" + totalRecords);
      }
      String commitActionType = CommitUtils.getCommitActionType(cfg.operation, HoodieTableType.valueOf(cfg.tableType));
      if (errorTableWriter.isPresent()) {
        // Commit the error events triggered so far to the error table
        Option<String> commitedInstantTime = getLatestInstantWithValidCheckpointInfo(commitsTimelineOpt);
        boolean errorTableSuccess = errorTableWriter.get().upsertAndCommit(instantTime, commitedInstantTime);
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
      boolean success = writeClient.commit(instantTime, writeStatusRDD, Option.of(checkpointCommitMetadata), commitActionType, partitionToReplacedFileIds, Option.empty());
      if (success) {
        LOG.info("Commit " + instantTime + " successful!");
        this.formatAdapter.getSource().onCommit(inputBatch.getCheckpointForNextBatch());
        // Schedule compaction if needed
        if (cfg.isAsyncCompactionEnabled()) {
          scheduledCompactionInstant = writeClient.scheduleCompaction(Option.empty());
        }

        if ((totalSuccessfulRecords > 0) || cfg.forceEmptyMetaSync) {
          runMetaSync();
        } else {
          LOG.info(String.format("Not running metaSync totalSuccessfulRecords=%d", totalSuccessfulRecords));
        }
      } else {
        LOG.info("Commit " + instantTime + " failed!");
        throw new HoodieStreamerWriteException("Commit " + instantTime + " failed!");
      }
    } else {
      LOG.error("Delta Sync found errors when writing. Errors/Total=" + totalErrorRecords + "/" + totalRecords);
      LOG.error("Printing out the top 100 errors");
      writeStatusRDD.filter(WriteStatus::hasErrors).take(100).forEach(ws -> {
        LOG.error("Global error :", ws.getGlobalError());
        if (ws.getErrors().size() > 0) {
          ws.getErrors().forEach((key, value) -> LOG.trace("Error for key:" + key + " is " + value));
        }
      });
      // Rolling back instant
      writeClient.rollback(instantTime);
      throw new HoodieStreamerWriteException("Commit " + instantTime + " failed and rolled-back !");
    }
    long overallTimeMs = overallTimerContext != null ? overallTimerContext.stop() : 0;

    // Send DeltaStreamer Metrics
    metrics.updateStreamerMetrics(overallTimeMs);
    return Pair.of(scheduledCompactionInstant, writeStatusRDD);
  }

  /**
   * Try to start a new commit.
   * <p>
   * Exception will be thrown if it failed in 2 tries.
   *
   * @return Instant time of the commit
   */
  private String startCommit(String instantTime, boolean retryEnabled) {
    final int maxRetries = 2;
    int retryNum = 1;
    RuntimeException lastException = null;
    while (retryNum <= maxRetries) {
      try {
        String commitActionType = CommitUtils.getCommitActionType(cfg.operation, HoodieTableType.valueOf(cfg.tableType));
        writeClient.startCommitWithTime(instantTime, commitActionType);
        return instantTime;
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
      instantTime = writeClient.createNewInstantTime();
    }
    throw lastException;
  }

  private WriteClientWriteResult writeToSink(InputBatch inputBatch, String instantTime) {
    WriteClientWriteResult writeClientWriteResult = null;
    instantTime = startCommit(instantTime, !autoGenerateRecordKeys);

    if (useRowWriter) {
      Dataset<Row> df = (Dataset<Row>) inputBatch.getBatch().orElse(hoodieSparkContext.getSqlContext().emptyDataFrame());
      HoodieWriteConfig hoodieWriteConfig = prepareHoodieConfigForRowWriter(inputBatch.getSchemaProvider().getTargetSchema());
      BaseDatasetBulkInsertCommitActionExecutor executor = new HoodieStreamerDatasetBulkInsertCommitActionExecutor(hoodieWriteConfig, writeClient, instantTime);
      writeClientWriteResult = new WriteClientWriteResult(executor.execute(df, !HoodieStreamerUtils.getPartitionColumns(props).isEmpty()).getWriteStatuses());
    } else {
      JavaRDD<HoodieRecord> records = (JavaRDD<HoodieRecord>) inputBatch.getBatch().orElse(hoodieSparkContext.emptyRDD());
      // filter dupes if needed
      if (cfg.filterDupes) {
        records = DataSourceUtils.dropDuplicates(hoodieSparkContext.jsc(), records, writeClient.getConfig());
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
          writeClientWriteResult = new WriteClientWriteResult(writeClient.bulkInsert(records, instantTime));
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
    if (cfg.enableMetaSync) {
      LOG.debug("[MetaSync] Starting sync");
      FileSystem fs = FSUtils.getFs(cfg.targetBasePath, hoodieSparkContext.hadoopConfiguration());

      TypedProperties metaProps = new TypedProperties();
      metaProps.putAll(props);
      metaProps.putAll(writeClient.getConfig().getProps());
      if (props.getBoolean(HIVE_SYNC_BUCKET_SYNC.key(), HIVE_SYNC_BUCKET_SYNC.defaultValue())) {
        metaProps.put(HIVE_SYNC_BUCKET_SYNC_SPEC.key(), HiveSyncConfig.getBucketSpec(props.getString(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key()),
            props.getInteger(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key())));
      }

      Map<String, HoodieException> failedMetaSyncs = new HashMap<>();
      for (String impl : syncClientToolClasses) {
        Timer.Context syncContext = metrics.getMetaSyncTimerContext();
        boolean success = false;
        try {
          SyncUtilHelpers.runHoodieMetaSync(impl.trim(), metaProps, conf, fs, cfg.targetBasePath, cfg.baseFileFormat);
          success = true;
        } catch (HoodieMetaSyncException e) {
          LOG.error("SyncTool class {0} failed with exception {1}",  impl.trim(), e);
          failedMetaSyncs.put(impl, e);
        }
        long metaSyncTimeMs = syncContext != null ? syncContext.stop() : 0;
        metrics.updateStreamerMetaSyncMetrics(getSyncClassShortName(impl), metaSyncTimeMs);
        if (success) {
          LOG.info("[MetaSync] SyncTool class {0} completed successfully and took {1} ", impl.trim(), metaSyncTimeMs);
        }
      }
      if (!failedMetaSyncs.isEmpty()) {
        throw getHoodieMetaSyncException(failedMetaSyncs);
      }
    }
  }

  /**
   * Note that depending on configs and source-type, schemaProvider could either be eagerly or lazily created.
   * SchemaProvider creation is a precursor to HoodieWriteClient and AsyncCompactor creation. This method takes care of
   * this constraint.
   */
  private void setupWriteClient(Option<JavaRDD<HoodieRecord>> recordsOpt) throws IOException {
    if ((null != schemaProvider)) {
      Schema sourceSchema = schemaProvider.getSourceSchema();
      Schema targetSchema = schemaProvider.getTargetSchema();
      reInitWriteClient(sourceSchema, targetSchema, recordsOpt);
    }
  }

  private void reInitWriteClient(Schema sourceSchema, Schema targetSchema, Option<JavaRDD<HoodieRecord>> recordsOpt) throws IOException {
    LOG.info("Setting up new Hoodie Write Client");
    if (HoodieStreamerUtils.isDropPartitionColumns(props)) {
      targetSchema = HoodieAvroUtils.removeFields(targetSchema, HoodieStreamerUtils.getPartitionColumns(props));
    }
    registerAvroSchemas(sourceSchema, targetSchema);
    final HoodieWriteConfig initialWriteConfig = getHoodieClientConfig(targetSchema);
    final HoodieWriteConfig writeConfig = SparkSampleWritesUtils
        .getWriteConfigWithRecordSizeEstimate(hoodieSparkContext.jsc(), recordsOpt, initialWriteConfig)
        .orElse(initialWriteConfig);

    if (writeConfig.isEmbeddedTimelineServerEnabled()) {
      if (!embeddedTimelineService.isPresent()) {
        embeddedTimelineService = EmbeddedTimelineServerHelper.createEmbeddedTimelineService(hoodieSparkContext, writeConfig);
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

  /**
   * Helper to construct Write Client config.
   *
   * @param schemaProvider Schema Provider
   */
  private HoodieWriteConfig getHoodieClientConfig(SchemaProvider schemaProvider) {
    return getHoodieClientConfig(schemaProvider != null ? schemaProvider.getTargetSchema() : null);
  }

  /**
   * Helper to construct Write Client config.
   *
   * @param schema Schema
   */
  private HoodieWriteConfig getHoodieClientConfig(Schema schema) {
    final boolean combineBeforeUpsert = true;
    final boolean autoCommit = false;

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
                    .withPayloadOrderingField(cfg.sourceOrderingField)
                    .build())
            .forTable(cfg.targetTableName)
            .withAutoCommit(autoCommit)
            .withProps(props);

    if (schema != null) {
      builder.withSchema(getSchemaForWriteConfig(schema).toString());
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
    ValidationUtils.checkArgument(!config.shouldAutoCommit(),
        String.format("%s should be set to %s", AUTO_COMMIT_ENABLE.key(), autoCommit));
    ValidationUtils.checkArgument(config.shouldCombineBeforeInsert() == cfg.filterDupes,
        String.format("%s should be set to %s", COMBINE_BEFORE_INSERT.key(), cfg.filterDupes));
    ValidationUtils.checkArgument(config.shouldCombineBeforeUpsert(),
        String.format("%s should be set to %s", COMBINE_BEFORE_UPSERT.key(), combineBeforeUpsert));
    return config;
  }

  private Schema getSchemaForWriteConfig(Schema targetSchema) {
    Schema newWriteSchema = targetSchema;
    try {
      if (targetSchema != null) {
        // check if targetSchema is equal to NULL schema
        if (SchemaCompatibility.checkReaderWriterCompatibility(targetSchema, InputBatch.NULL_SCHEMA).getType() == SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE
            && SchemaCompatibility.checkReaderWriterCompatibility(InputBatch.NULL_SCHEMA, targetSchema).getType() == SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE) {
          // target schema is null. fetch schema from commit metadata and use it
          HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(new Configuration(fs.getConf()))
              .setBasePath(cfg.targetBasePath)
              .setPayloadClassName(cfg.payloadClassName)
              .build();
          int totalCompleted = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants();
          if (totalCompleted > 0) {
            TableSchemaResolver schemaResolver = new TableSchemaResolver(meta);
            Option<Schema> tableSchema = schemaResolver.getTableAvroSchemaIfPresent(false);
            if (tableSchema.isPresent()) {
              newWriteSchema = tableSchema.get();
            } else {
              LOG.warn("Could not fetch schema from table. Falling back to using target schema from schema provider");
            }
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("Registering Schema: " + schemas);
      }
      // Use the underlying spark context in case the java context is changed during runtime
      hoodieSparkContext.getJavaSparkContext().sc().getConf().registerAvroSchemas(JavaConversions.asScalaBuffer(schemas).toList());
    }
  }

  /**
   * Close all resources.
   */
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

  public FileSystem getFs() {
    return fs;
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

  class WriteClientWriteResult {
    private Map<String, List<String>> partitionToReplacedFileIds = Collections.emptyMap();
    private JavaRDD<WriteStatus> writeStatusRDD;

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

}
