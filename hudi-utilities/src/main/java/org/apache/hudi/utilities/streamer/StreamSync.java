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
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineServerHelper;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieSparkRecord;
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
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
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
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
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
import org.apache.hudi.utilities.streamer.HoodieStreamer.Config;
import org.apache.hudi.utilities.transform.Transformer;

import com.codahale.metrics.Timer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.HoodieAvroDeserializer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.JavaConversions;

import static org.apache.hudi.avro.AvroSchemaUtils.getAvroRecordQualifiedName;
import static org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER;
import static org.apache.hudi.common.table.HoodieTableConfig.DROP_PARTITION_COLUMNS;
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
    this.formatAdapter = new SourceFormatAdapter(
        UtilHelpers.createSource(cfg.sourceClassName, props, hoodieSparkContext.jsc(), sparkSession, schemaProvider, metrics),
        this.errorTableWriter, Option.of(props));

    this.transformer = UtilHelpers.createTransformer(Option.ofNullable(cfg.transformerClassNames),
        Option.ofNullable(schemaProvider).map(SchemaProvider::getSourceSchema), this.errorTableWriter.isPresent());

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
        .setShouldDropPartitionColumns(isDropPartitionColumns())
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

    Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> srcRecordsWithCkpt = readFromSource(instantTime, metaClient);

    if (srcRecordsWithCkpt != null) {
      final JavaRDD<HoodieRecord> recordsFromSource = srcRecordsWithCkpt.getRight().getRight();
      // this is the first input batch. If schemaProvider not set, use it and register Avro Schema and start
      // compactor
      if (writeClient == null) {
        this.schemaProvider = srcRecordsWithCkpt.getKey();
        // Setup HoodieWriteClient and compaction now that we decided on schema
        setupWriteClient(recordsFromSource);
      } else {
        Schema newSourceSchema = srcRecordsWithCkpt.getKey().getSourceSchema();
        Schema newTargetSchema = srcRecordsWithCkpt.getKey().getTargetSchema();
        if (!(processedSchema.isSchemaPresent(newSourceSchema))
            || !(processedSchema.isSchemaPresent(newTargetSchema))) {
          LOG.info("Seeing new schema. Source :" + newSourceSchema.toString(true)
              + ", Target :" + newTargetSchema.toString(true));
          // We need to recreate write client with new schema and register them.
          reInitWriteClient(newSourceSchema, newTargetSchema, recordsFromSource);
          processedSchema.addSchema(newSourceSchema);
          processedSchema.addSchema(newTargetSchema);
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

      result = writeToSink(instantTime, recordsFromSource,
          srcRecordsWithCkpt.getRight().getLeft(), metrics, overallTimerContext);
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
   * @return Pair<SchemaProvider, Pair < String, JavaRDD < HoodieRecord>>> Input data read from upstream source, consists
   * of schemaProvider, checkpointStr and hoodieRecord
   * @throws Exception in case of any Exception
   */
  public Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> readFromSource(String instantTime, HoodieTableMetaClient metaClient) throws IOException {
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
    Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> sourceDataToSync = null;
    while (curRetryCount++ < maxRetryCount && sourceDataToSync == null) {
      try {
        sourceDataToSync = fetchFromSource(resumeCheckpointStr, instantTime, metaClient);
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

  private Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> fetchFromSource(Option<String> resumeCheckpointStr, String instantTime,
                                                                                    HoodieTableMetaClient metaClient) {
    HoodieRecordType recordType = createRecordMerger(props).getRecordType();
    if (recordType == HoodieRecordType.SPARK && HoodieTableType.valueOf(cfg.tableType) == HoodieTableType.MERGE_ON_READ
        && HoodieLogBlockType.fromId(props.getProperty(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "avro"))
        != HoodieLogBlockType.PARQUET_DATA_BLOCK) {
      throw new UnsupportedOperationException("Spark record only support parquet log.");
    }

    final Option<JavaRDD<GenericRecord>> avroRDDOptional;
    final String checkpointStr;
    SchemaProvider schemaProvider;
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
      if (this.userProvidedSchemaProvider != null && this.userProvidedSchemaProvider.getTargetSchema() != null) {
        // Let's deduce the schema provider for writer side first!
        schemaProvider = getDeducedSchemaProvider(this.userProvidedSchemaProvider.getTargetSchema(), this.userProvidedSchemaProvider, metaClient);
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
                    Option.of(schemaProvider.getTargetSchema()));
                errorTableWriter.get().addErrorEvents(safeCreateRDDs._2().toJavaRDD()
                    .map(evStr -> new ErrorEvent<>(evStr,
                        ErrorEvent.ErrorReason.AVRO_DESERIALIZATION_FAILURE)));
                return safeCreateRDDs._1.toJavaRDD();
              });
        } else {
          avroRDDOptional = transformed.map(
              rowDataset -> getTransformedRDD(rowDataset, reconcileSchema, schemaProvider.getTargetSchema()));
        }
      } else {
        // Deduce proper target (writer's) schema for the input dataset, reconciling its
        // schema w/ the table's one
        Option<Schema> incomingSchemaOpt = transformed.map(df ->
            AvroConversionUtils.convertStructTypeToAvroSchema(df.schema(), getAvroRecordQualifiedName(cfg.targetTableName)));

        schemaProvider = incomingSchemaOpt.map(incomingSchema -> getDeducedSchemaProvider(incomingSchema, dataAndCheckpoint.getSchemaProvider(), metaClient))
            .orElse(dataAndCheckpoint.getSchemaProvider());
        // Rewrite transformed records into the expected target schema
        avroRDDOptional = transformed.map(t -> getTransformedRDD(t, reconcileSchema, schemaProvider.getTargetSchema()));
      }
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

    if (!cfg.allowCommitOnNoCheckpointChange && Objects.equals(checkpointStr, resumeCheckpointStr.orElse(null))) {
      LOG.info("No new data, source checkpoint has not changed. Nothing to commit. Old checkpoint=("
          + resumeCheckpointStr + "). New Checkpoint=(" + checkpointStr + ")");
      String commitActionType = CommitUtils.getCommitActionType(cfg.operation, HoodieTableType.valueOf(cfg.tableType));
      hoodieMetrics.updateMetricsForEmptyData(commitActionType);
      return null;
    }

    hoodieSparkContext.setJobStatus(this.getClass().getSimpleName(), "Checking if input is empty");
    if ((!avroRDDOptional.isPresent()) || (avroRDDOptional.get().isEmpty())) {
      LOG.info("No new data, perform empty commit.");
      return Pair.of(schemaProvider, Pair.of(checkpointStr, hoodieSparkContext.emptyRDD()));
    }

    boolean shouldCombine = cfg.filterDupes || cfg.operation.equals(WriteOperationType.UPSERT);
    Set<String> partitionColumns = getPartitionColumns(props);
    JavaRDD<GenericRecord> avroRDD = avroRDDOptional.get();

    JavaRDD<HoodieRecord> records;
    SerializableSchema avroSchema = new SerializableSchema(schemaProvider.getTargetSchema());
    SerializableSchema processedAvroSchema = new SerializableSchema(isDropPartitionColumns() ? HoodieAvroUtils.removeMetadataFields(avroSchema.get()) : avroSchema.get());
    if (recordType == HoodieRecordType.AVRO) {
      records = avroRDD.mapPartitions(
          (FlatMapFunction<Iterator<GenericRecord>, HoodieRecord>) genericRecordIterator -> {
            if (autoGenerateRecordKeys) {
              props.setProperty(KeyGenUtils.RECORD_KEY_GEN_PARTITION_ID_CONFIG, String.valueOf(TaskContext.getPartitionId()));
              props.setProperty(KeyGenUtils.RECORD_KEY_GEN_INSTANT_TIME_CONFIG, instantTime);
            }
            BuiltinKeyGenerator builtinKeyGenerator = (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
            List<HoodieRecord> avroRecords = new ArrayList<>();
            while (genericRecordIterator.hasNext()) {
              GenericRecord genRec = genericRecordIterator.next();
              HoodieKey hoodieKey = new HoodieKey(builtinKeyGenerator.getRecordKey(genRec), builtinKeyGenerator.getPartitionPath(genRec));
              GenericRecord gr = isDropPartitionColumns() ? HoodieAvroUtils.removeFields(genRec, partitionColumns) : genRec;
              HoodieRecordPayload payload = shouldCombine ? DataSourceUtils.createPayload(cfg.payloadClassName, gr,
                  (Comparable) HoodieAvroUtils.getNestedFieldVal(gr, cfg.sourceOrderingField, false, props.getBoolean(
                      KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
                      Boolean.parseBoolean(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()))))
                  : DataSourceUtils.createPayload(cfg.payloadClassName, gr);
              avroRecords.add(new HoodieAvroRecord<>(hoodieKey, payload));
            }
            return avroRecords.iterator();
          });
    } else if (recordType == HoodieRecordType.SPARK) {
      // TODO we should remove it if we can read InternalRow from source.
      records = avroRDD.mapPartitions(itr -> {
        if (autoGenerateRecordKeys) {
          props.setProperty(KeyGenUtils.RECORD_KEY_GEN_PARTITION_ID_CONFIG, String.valueOf(TaskContext.getPartitionId()));
          props.setProperty(KeyGenUtils.RECORD_KEY_GEN_INSTANT_TIME_CONFIG, instantTime);
        }
        BuiltinKeyGenerator builtinKeyGenerator = (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
        StructType baseStructType = AvroConversionUtils.convertAvroSchemaToStructType(processedAvroSchema.get());
        StructType targetStructType = isDropPartitionColumns() ? AvroConversionUtils
            .convertAvroSchemaToStructType(HoodieAvroUtils.removeFields(processedAvroSchema.get(), partitionColumns)) : baseStructType;
        HoodieAvroDeserializer deserializer = SparkAdapterSupport$.MODULE$.sparkAdapter().createAvroDeserializer(processedAvroSchema.get(), baseStructType);

        return new CloseableMappingIterator<>(ClosableIterator.wrap(itr), rec -> {
          InternalRow row = (InternalRow) deserializer.deserialize(rec).get();
          String recordKey = builtinKeyGenerator.getRecordKey(row, baseStructType).toString();
          String partitionPath = builtinKeyGenerator.getPartitionPath(row, baseStructType).toString();
          return new HoodieSparkRecord(new HoodieKey(recordKey, partitionPath),
              HoodieInternalRowUtils.getCachedUnsafeProjection(baseStructType, targetStructType).apply(row), targetStructType, false);
        });
      });
    } else {
      throw new UnsupportedOperationException(recordType.name());
    }

    return Pair.of(schemaProvider, Pair.of(checkpointStr, records));
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
    // has deltacommit means this is a MOR table, we should get .deltacommit as before
    if (!deltaCommitTimeline.empty()) {
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

  /**
   * Perform Hoodie Write. Run Cleaner, schedule compaction and syncs to hive if needed.
   *
   * @param instantTime         instant time to use for ingest.
   * @param records             Input Records
   * @param checkpointStr       Checkpoint String
   * @param metrics             Metrics
   * @param overallTimerContext Timer Context
   * @return Option Compaction instant if one is scheduled
   */
  private Pair<Option<String>, JavaRDD<WriteStatus>> writeToSink(String instantTime, JavaRDD<HoodieRecord> records, String checkpointStr,
                                                                 HoodieIngestionMetrics metrics,
                                                                 Timer.Context overallTimerContext) {
    Option<String> scheduledCompactionInstant = Option.empty();
    // filter dupes if needed
    if (cfg.filterDupes) {
      records = DataSourceUtils.dropDuplicates(hoodieSparkContext.jsc(), records, writeClient.getConfig());
    }

    boolean isEmpty = records.isEmpty();
    instantTime = startCommit(instantTime, !autoGenerateRecordKeys);
    LOG.info("Starting commit  : " + instantTime);

    HoodieWriteResult writeResult;
    Map<String, List<String>> partitionToReplacedFileIds = Collections.emptyMap();
    JavaRDD<WriteStatus> writeStatusRDD;
    switch (cfg.operation) {
      case INSERT:
        writeStatusRDD = writeClient.insert(records, instantTime);
        break;
      case UPSERT:
        writeStatusRDD = writeClient.upsert(records, instantTime);
        break;
      case BULK_INSERT:
        writeStatusRDD = writeClient.bulkInsert(records, instantTime);
        break;
      case INSERT_OVERWRITE:
        writeResult = writeClient.insertOverwrite(records, instantTime);
        partitionToReplacedFileIds = writeResult.getPartitionToReplaceFileIds();
        writeStatusRDD = writeResult.getWriteStatuses();
        break;
      case INSERT_OVERWRITE_TABLE:
        writeResult = writeClient.insertOverwriteTable(records, instantTime);
        partitionToReplacedFileIds = writeResult.getPartitionToReplaceFileIds();
        writeStatusRDD = writeResult.getWriteStatuses();
        break;
      case DELETE_PARTITION:
        List<String> partitions = records.map(record -> record.getPartitionPath()).distinct().collect();
        writeResult = writeClient.deletePartitions(partitions, instantTime);
        partitionToReplacedFileIds = writeResult.getPartitionToReplaceFileIds();
        writeStatusRDD = writeResult.getWriteStatuses();
        break;
      default:
        throw new HoodieStreamerException("Unknown operation : " + cfg.operation);
    }

    long totalErrorRecords = writeStatusRDD.mapToDouble(WriteStatus::getTotalErrorRecords).sum().longValue();
    long totalRecords = writeStatusRDD.mapToDouble(WriteStatus::getTotalRecords).sum().longValue();
    boolean hasErrors = totalErrorRecords > 0;
    if (!hasErrors || cfg.commitOnErrors) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      if (!getBooleanWithAltKeys(props, CHECKPOINT_FORCE_SKIP)) {
        if (checkpointStr != null) {
          checkpointCommitMetadata.put(CHECKPOINT_KEY, checkpointStr);
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
        this.formatAdapter.getSource().onCommit(checkpointStr);
        // Schedule compaction if needed
        if (cfg.isAsyncCompactionEnabled()) {
          scheduledCompactionInstant = writeClient.scheduleCompaction(Option.empty());
        }

        if (!isEmpty || cfg.forceEmptyMetaSync) {
          runMetaSync();
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

  private String getSyncClassShortName(String syncClassName) {
    return syncClassName.substring(syncClassName.lastIndexOf(".") + 1);
  }

  public void runMetaSync() {
    Set<String> syncClientToolClasses = new HashSet<>(Arrays.asList(cfg.syncClientToolClassNames.split(",")));
    // for backward compatibility
    if (cfg.enableHiveSync) {
      cfg.enableMetaSync = true;
      syncClientToolClasses.add(HiveSyncTool.class.getName());
      LOG.info("When set --enable-hive-sync will use HiveSyncTool for backward compatibility");
    }
    if (cfg.enableMetaSync) {
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
        try {
          SyncUtilHelpers.runHoodieMetaSync(impl.trim(), metaProps, conf, fs, cfg.targetBasePath, cfg.baseFileFormat);
        } catch (HoodieMetaSyncException e) {
          LOG.warn("SyncTool class " + impl.trim() + " failed with exception", e);
          failedMetaSyncs.put(impl, e);
        }
        long metaSyncTimeMs = syncContext != null ? syncContext.stop() : 0;
        metrics.updateStreamerMetaSyncMetrics(getSyncClassShortName(impl), metaSyncTimeMs);
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
  private void setupWriteClient(JavaRDD<HoodieRecord> records) throws IOException {
    if ((null != schemaProvider)) {
      Schema sourceSchema = schemaProvider.getSourceSchema();
      Schema targetSchema = schemaProvider.getTargetSchema();
      reInitWriteClient(sourceSchema, targetSchema, records);
    }
  }

  private void reInitWriteClient(Schema sourceSchema, Schema targetSchema, JavaRDD<HoodieRecord> records) throws IOException {
    LOG.info("Setting up new Hoodie Write Client");
    if (isDropPartitionColumns()) {
      targetSchema = HoodieAvroUtils.removeFields(targetSchema, getPartitionColumns(props));
    }
    registerAvroSchemas(sourceSchema, targetSchema);
    final HoodieWriteConfig initialWriteConfig = getHoodieClientConfig(targetSchema);
    final HoodieWriteConfig writeConfig = SparkSampleWritesUtils
        .getWriteConfigWithRecordSizeEstimate(hoodieSparkContext.jsc(), records, initialWriteConfig)
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
            try {
              TableSchemaResolver schemaResolver = new TableSchemaResolver(meta);
              newWriteSchema = schemaResolver.getTableAvroSchema(false);
            } catch (IllegalArgumentException e) {
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
    if (null != sourceSchema) {
      List<Schema> schemas = new ArrayList<>();
      schemas.add(sourceSchema);
      if (targetSchema != null) {
        schemas.add(targetSchema);
      }

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
      embeddedTimelineService.get().stop();
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

  /**
   * Set based on hoodie.datasource.write.drop.partition.columns config.
   * When set to true, will not write the partition columns into the table.
   */
  private Boolean isDropPartitionColumns() {
    return props.getBoolean(DROP_PARTITION_COLUMNS.key(), DROP_PARTITION_COLUMNS.defaultValue());
  }

  /**
   * Get the partition columns as a set of strings.
   *
   * @param props TypedProperties
   * @return Set of partition columns.
   */
  private Set<String> getPartitionColumns(TypedProperties props) {
    String partitionColumns = SparkKeyGenUtils.getPartitionColumns(props);
    return Arrays.stream(partitionColumns.split(",")).collect(Collectors.toSet());
  }
}
