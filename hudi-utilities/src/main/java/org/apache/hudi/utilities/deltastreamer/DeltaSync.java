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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineServerHelper;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.sync.common.util.SyncUtilHelpers;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallback;
import org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallbackConfig;
import org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallback;
import org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.Config;
import org.apache.hudi.utilities.exception.HoodieDeltaStreamerException;
import org.apache.hudi.utilities.exception.HoodieSourceTimeoutException;
import org.apache.hudi.utilities.schema.DelegatingSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.schema.SchemaSet;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.transform.Transformer;

import com.codahale.metrics.Timer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import scala.collection.JavaConversions;

import static org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER;
import static org.apache.hudi.common.table.HoodieTableConfig.DROP_PARTITION_COLUMNS;
import static org.apache.hudi.config.HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE;
import static org.apache.hudi.config.HoodieClusteringConfig.INLINE_CLUSTERING;
import static org.apache.hudi.config.HoodieCompactionConfig.INLINE_COMPACT;
import static org.apache.hudi.config.HoodieWriteConfig.AUTO_COMMIT_ENABLE;
import static org.apache.hudi.config.HoodieWriteConfig.COMBINE_BEFORE_INSERT;
import static org.apache.hudi.config.HoodieWriteConfig.COMBINE_BEFORE_UPSERT;
import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.CHECKPOINT_KEY;
import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.CHECKPOINT_RESET_KEY;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME;

/**
 * Sync's one batch of data to hoodie table.
 */
public class DeltaSync implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(DeltaSync.class);

  /**
   * Delta Sync Config.
   */
  private final HoodieDeltaStreamer.Config cfg;

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

  /**
   * Extract the key for the target table.
   */
  private KeyGenerator keyGenerator;

  /**
   * Filesystem used.
   */
  private transient FileSystem fs;

  /**
   * Spark context.
   */
  private transient JavaSparkContext jssc;

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
   * Timeline with completed commits.
   */
  private transient Option<HoodieTimeline> commitTimelineOpt;

  // all commits timeline
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

  private transient HoodieDeltaStreamerMetrics metrics;

  private transient HoodieMetrics hoodieMetrics;

  public DeltaSync(HoodieDeltaStreamer.Config cfg, SparkSession sparkSession, SchemaProvider schemaProvider,
                   TypedProperties props, JavaSparkContext jssc, FileSystem fs, Configuration conf,
                   Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient) throws IOException {

    this.cfg = cfg;
    this.jssc = jssc;
    this.sparkSession = sparkSession;
    this.fs = fs;
    this.onInitializingHoodieWriteClient = onInitializingHoodieWriteClient;
    this.props = props;
    this.userProvidedSchemaProvider = schemaProvider;
    this.processedSchema = new SchemaSet();
    this.keyGenerator = HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
    refreshTimeline();
    // Register User Provided schema first
    registerAvroSchemas(schemaProvider);

    this.transformer = UtilHelpers.createTransformer(cfg.transformerClassNames);

    this.metrics = new HoodieDeltaStreamerMetrics(getHoodieClientConfig(this.schemaProvider));
    this.hoodieMetrics = new HoodieMetrics(getHoodieClientConfig(this.schemaProvider));

    this.formatAdapter = new SourceFormatAdapter(
        UtilHelpers.createSource(cfg.sourceClassName, props, jssc, sparkSession, schemaProvider, metrics));
    this.conf = conf;
  }

  /**
   * Refresh Timeline.
   *
   * @throws IOException in case of any IOException
   */
  public void refreshTimeline() throws IOException {
    if (fs.exists(new Path(cfg.targetBasePath))) {
      HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(new Configuration(fs.getConf())).setBasePath(cfg.targetBasePath).setPayloadClassName(cfg.payloadClassName).build();
      switch (meta.getTableType()) {
        case COPY_ON_WRITE:
          this.commitTimelineOpt = Option.of(meta.getActiveTimeline().getCommitTimeline().filterCompletedInstants());
          this.allCommitsTimelineOpt = Option.of(meta.getActiveTimeline().getAllCommitsTimeline());
          break;
        case MERGE_ON_READ:
          this.commitTimelineOpt = Option.of(meta.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants());
          this.allCommitsTimelineOpt = Option.of(meta.getActiveTimeline().getAllCommitsTimeline());
          break;
        default:
          throw new HoodieException("Unsupported table type :" + meta.getTableType());
      }
    } else {
      this.commitTimelineOpt = Option.empty();
      this.allCommitsTimelineOpt = Option.empty();
      String partitionColumns = HoodieSparkUtils.getPartitionColumns(keyGenerator, props);
      HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(cfg.tableType)
          .setTableName(cfg.targetTableName)
          .setArchiveLogFolder(ARCHIVELOG_FOLDER.defaultValue())
          .setPayloadClassName(cfg.payloadClassName)
          .setMergeClassName(cfg.mergeClassName)
          .setBaseFileFormat(cfg.baseFileFormat)
          .setPartitionFields(partitionColumns)
          .setRecordKeyFields(props.getProperty(DataSourceWriteOptions.RECORDKEY_FIELD().key()))
          .setPopulateMetaFields(props.getBoolean(HoodieTableConfig.POPULATE_META_FIELDS.key(),
              HoodieTableConfig.POPULATE_META_FIELDS.defaultValue()))
          .setKeyGeneratorClassProp(props.getProperty(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(),
              SimpleKeyGenerator.class.getName()))
          .setPreCombineField(cfg.sourceOrderingField)
          .setPartitionMetafileUseBaseFormat(props.getBoolean(HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.key(),
              HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.defaultValue()))
          .setShouldDropPartitionColumns(isDropPartitionColumns())
          .initTable(new Configuration(jssc.hadoopConfiguration()),
            cfg.targetBasePath);
    }
  }

  /**
   * Run one round of delta sync and return new compaction instant if one got scheduled.
   */
  public Pair<Option<String>, JavaRDD<WriteStatus>> syncOnce() throws IOException {
    Pair<Option<String>, JavaRDD<WriteStatus>> result = null;
    Timer.Context overallTimerContext = metrics.getOverallTimerContext();

    // Refresh Timeline
    refreshTimeline();

    Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> srcRecordsWithCkpt = readFromSource(commitTimelineOpt);

    if (null != srcRecordsWithCkpt) {
      // this is the first input batch. If schemaProvider not set, use it and register Avro Schema and start
      // compactor
      if (null == writeClient) {
        this.schemaProvider = srcRecordsWithCkpt.getKey();
        // Setup HoodieWriteClient and compaction now that we decided on schema
        setupWriteClient();
      } else {
        Schema newSourceSchema = srcRecordsWithCkpt.getKey().getSourceSchema();
        Schema newTargetSchema = srcRecordsWithCkpt.getKey().getTargetSchema();
        if (!(processedSchema.isSchemaPresent(newSourceSchema))
            || !(processedSchema.isSchemaPresent(newTargetSchema))) {
          LOG.info("Seeing new schema. Source :" + newSourceSchema.toString(true)
              + ", Target :" + newTargetSchema.toString(true));
          // We need to recreate write client with new schema and register them.
          reInitWriteClient(newSourceSchema, newTargetSchema);
          processedSchema.addSchema(newSourceSchema);
          processedSchema.addSchema(newTargetSchema);
        }
      }

      // complete the pending clustering before writing to sink
      if (cfg.retryLastPendingInlineClusteringJob && getHoodieClientConfig(this.schemaProvider).inlineClusteringEnabled()) {
        Option<String> pendingClusteringInstant = getLastPendingClusteringInstant(allCommitsTimelineOpt);
        if (pendingClusteringInstant.isPresent()) {
          writeClient.cluster(pendingClusteringInstant.get(), true);
        }
      }

      result = writeToSink(srcRecordsWithCkpt.getRight().getRight(),
          srcRecordsWithCkpt.getRight().getLeft(), metrics, overallTimerContext);
    }

    metrics.updateDeltaStreamerSyncMetrics(System.currentTimeMillis());

    // Clear persistent RDDs
    jssc.getPersistentRDDs().values().forEach(JavaRDD::unpersist);
    return result;
  }

  private Option<String> getLastPendingClusteringInstant(Option<HoodieTimeline> commitTimelineOpt) {
    if (commitTimelineOpt.isPresent()) {
      Option<HoodieInstant> pendingClusteringInstant = commitTimelineOpt.get().filterPendingReplaceTimeline().lastInstant();
      return pendingClusteringInstant.isPresent() ? Option.of(pendingClusteringInstant.get().getTimestamp()) : Option.empty();
    }
    return Option.empty();
  }

  /**
   * Read from Upstream Source and apply transformation if needed.
   *
   * @param commitTimelineOpt Timeline with completed commits
   * @return Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> Input data read from upstream source, consists
   * of schemaProvider, checkpointStr and hoodieRecord
   * @throws Exception in case of any Exception
   */
  public Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> readFromSource(Option<HoodieTimeline> commitTimelineOpt) throws IOException {
    // Retrieve the previous round checkpoints, if any
    Option<String> resumeCheckpointStr = Option.empty();
    if (commitTimelineOpt.isPresent()) {
      resumeCheckpointStr = getCheckpointToResume(commitTimelineOpt);
    } else {
      // initialize the table for the first time.
      String partitionColumns = HoodieSparkUtils.getPartitionColumns(keyGenerator, props);
      HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(cfg.tableType)
          .setTableName(cfg.targetTableName)
          .setArchiveLogFolder(ARCHIVELOG_FOLDER.defaultValue())
          .setPayloadClassName(cfg.payloadClassName)
          .setMergeClassName(cfg.mergeClassName)
          .setBaseFileFormat(cfg.baseFileFormat)
          .setPartitionFields(partitionColumns)
          .setRecordKeyFields(props.getProperty(DataSourceWriteOptions.RECORDKEY_FIELD().key()))
          .setPopulateMetaFields(props.getBoolean(HoodieTableConfig.POPULATE_META_FIELDS.key(),
              HoodieTableConfig.POPULATE_META_FIELDS.defaultValue()))
          .setKeyGeneratorClassProp(props.getProperty(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(),
              SimpleKeyGenerator.class.getName()))
          .setPartitionMetafileUseBaseFormat(props.getBoolean(HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.key(),
              HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.defaultValue()))
          .setShouldDropPartitionColumns(isDropPartitionColumns())
          .initTable(new Configuration(jssc.hadoopConfiguration()), cfg.targetBasePath);
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
        sourceDataToSync = fetchFromSource(resumeCheckpointStr);
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

  private Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> fetchFromSource(Option<String> resumeCheckpointStr) {
    final Option<JavaRDD<GenericRecord>> avroRDDOptional;
    final String checkpointStr;
    SchemaProvider schemaProvider;
    if (transformer.isPresent()) {
      // Transformation is needed. Fetch New rows in Row Format, apply transformation and then convert them
      // to generic records for writing
      InputBatch<Dataset<Row>> dataAndCheckpoint =
          formatAdapter.fetchNewDataInRowFormat(resumeCheckpointStr, cfg.sourceLimit);

      Option<Dataset<Row>> transformed =
          dataAndCheckpoint.getBatch().map(data -> transformer.get().apply(jssc, sparkSession, data, props));

      checkpointStr = dataAndCheckpoint.getCheckpointForNextBatch();
      boolean reconcileSchema = props.getBoolean(DataSourceWriteOptions.RECONCILE_SCHEMA().key());
      if (this.userProvidedSchemaProvider != null && this.userProvidedSchemaProvider.getTargetSchema() != null) {
        // If the target schema is specified through Avro schema,
        // pass in the schema for the Row-to-Avro conversion
        // to avoid nullability mismatch between Avro schema and Row schema
        avroRDDOptional = transformed
            .map(t -> HoodieSparkUtils.createRdd(
                t, HOODIE_RECORD_STRUCT_NAME, HOODIE_RECORD_NAMESPACE, reconcileSchema,
                Option.of(this.userProvidedSchemaProvider.getTargetSchema())
            ).toJavaRDD());
        schemaProvider = this.userProvidedSchemaProvider;
      } else {
        // Use Transformed Row's schema if not overridden. If target schema is not specified
        // default to RowBasedSchemaProvider
        schemaProvider =
            transformed
                .map(r -> {
                  // determine the targetSchemaProvider. use latestTableSchema if reconcileSchema is enabled.
                  SchemaProvider targetSchemaProvider = null;
                  if (reconcileSchema) {
                    targetSchemaProvider = UtilHelpers.createLatestSchemaProvider(r.schema(), jssc, fs, cfg.targetBasePath);
                  } else {
                    targetSchemaProvider = UtilHelpers.createRowBasedSchemaProvider(r.schema(), props, jssc);
                  }
                  return (SchemaProvider) new DelegatingSchemaProvider(props, jssc,
                      dataAndCheckpoint.getSchemaProvider(), targetSchemaProvider); })
                .orElse(dataAndCheckpoint.getSchemaProvider());
        avroRDDOptional = transformed
            .map(t -> HoodieSparkUtils.createRdd(
                t, HOODIE_RECORD_STRUCT_NAME, HOODIE_RECORD_NAMESPACE, reconcileSchema,
                Option.ofNullable(schemaProvider.getTargetSchema())
            ).toJavaRDD());
      }
    } else {
      // Pull the data from the source & prepare the write
      InputBatch<JavaRDD<GenericRecord>> dataAndCheckpoint =
          formatAdapter.fetchNewDataInAvroFormat(resumeCheckpointStr, cfg.sourceLimit);
      avroRDDOptional = dataAndCheckpoint.getBatch();
      checkpointStr = dataAndCheckpoint.getCheckpointForNextBatch();
      schemaProvider = dataAndCheckpoint.getSchemaProvider();
    }

    if (!cfg.allowCommitOnNoCheckpointChange && Objects.equals(checkpointStr, resumeCheckpointStr.orElse(null))) {
      LOG.info("No new data, source checkpoint has not changed. Nothing to commit. Old checkpoint=("
          + resumeCheckpointStr + "). New Checkpoint=(" + checkpointStr + ")");
      String commitActionType = CommitUtils.getCommitActionType(cfg.operation, HoodieTableType.valueOf(cfg.tableType));
      hoodieMetrics.updateMetricsForEmptyData(commitActionType);
      return null;
    }

    jssc.setJobGroup(this.getClass().getSimpleName(), "Checking if input is empty");
    if ((!avroRDDOptional.isPresent()) || (avroRDDOptional.get().isEmpty())) {
      LOG.info("No new data, perform empty commit.");
      return Pair.of(schemaProvider, Pair.of(checkpointStr, jssc.emptyRDD()));
    }

    boolean shouldCombine = cfg.filterDupes || cfg.operation.equals(WriteOperationType.UPSERT);
    List<String> partitionColumns = getPartitionColumns(keyGenerator, props);
    JavaRDD<GenericRecord> avroRDD = avroRDDOptional.get();
    JavaRDD<HoodieRecord> records = avroRDD.map(record -> {
      GenericRecord gr = isDropPartitionColumns() ? HoodieAvroUtils.removeFields(record, partitionColumns) : record;
      HoodieRecordPayload payload = shouldCombine ? DataSourceUtils.createPayload(cfg.payloadClassName, gr,
          (Comparable) HoodieAvroUtils.getNestedFieldVal(gr, cfg.sourceOrderingField, false, props.getBoolean(
              KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
              Boolean.parseBoolean(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()))))
          : DataSourceUtils.createPayload(cfg.payloadClassName, gr);
      return new HoodieAvroRecord<>(keyGenerator.getKey(record), payload);
    });

    return Pair.of(schemaProvider, Pair.of(checkpointStr, records));
  }

  /**
   * Process previous commit metadata and checkpoint configs set by user to determine the checkpoint to resume from.
   * @param commitTimelineOpt commit timeline of interest.
   * @return the checkpoint to resume from if applicable.
   * @throws IOException
   */
  private Option<String> getCheckpointToResume(Option<HoodieTimeline> commitTimelineOpt) throws IOException {
    Option<String> resumeCheckpointStr = Option.empty();
    Option<HoodieInstant> lastCommit = commitTimelineOpt.get().lastInstant();
    if (lastCommit.isPresent()) {
      // if previous commit metadata did not have the checkpoint key, try traversing previous commits until we find one.
      Option<HoodieCommitMetadata> commitMetadataOption = getLatestCommitMetadataWithValidCheckpointInfo(commitTimelineOpt.get());
      if (commitMetadataOption.isPresent()) {
        HoodieCommitMetadata commitMetadata = commitMetadataOption.get();
        LOG.debug("Checkpoint reset from metadata: " + commitMetadata.getMetadata(CHECKPOINT_RESET_KEY));
        if (cfg.checkpoint != null && (StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_RESET_KEY))
            || !cfg.checkpoint.equals(commitMetadata.getMetadata(CHECKPOINT_RESET_KEY)))) {
          resumeCheckpointStr = Option.of(cfg.checkpoint);
        } else if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_KEY))) {
          //if previous checkpoint is an empty string, skip resume use Option.empty()
          resumeCheckpointStr = Option.of(commitMetadata.getMetadata(CHECKPOINT_KEY));
        } else if (HoodieTimeline.compareTimestamps(HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS,
            HoodieTimeline.LESSER_THAN, lastCommit.get().getTimestamp())) {
          throw new HoodieDeltaStreamerException(
              "Unable to find previous checkpoint. Please double check if this table "
                  + "was indeed built via delta streamer. Last Commit :" + lastCommit + ", Instants :"
                  + commitTimelineOpt.get().getInstants().collect(Collectors.toList()) + ", CommitMetadata="
                  + commitMetadata.toJsonString());
        }
        // KAFKA_CHECKPOINT_TYPE will be honored only for first batch.
        if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_RESET_KEY))) {
          props.remove(KafkaOffsetGen.Config.KAFKA_CHECKPOINT_TYPE.key());
        }
      } else if (cfg.checkpoint != null) { // getLatestCommitMetadataWithValidCheckpointInfo(commitTimelineOpt.get()) will never return a commit metadata w/o any checkpoint key set.
        resumeCheckpointStr = Option.of(cfg.checkpoint);
      }
    }
    return resumeCheckpointStr;
  }

  protected Option<HoodieCommitMetadata> getLatestCommitMetadataWithValidCheckpointInfo(HoodieTimeline timeline) throws IOException {
    return (Option<HoodieCommitMetadata>) timeline.getReverseOrderedInstants().map(instant -> {
      try {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
            .fromBytes(timeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
        if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_KEY)) || !StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_RESET_KEY))) {
          return Option.of(commitMetadata);
        } else {
          return Option.empty();
        }
      } catch (IOException e) {
        throw new HoodieIOException("Failed to parse HoodieCommitMetadata for " + instant.toString(), e);
      }
    }).filter(Option::isPresent).findFirst().orElse(Option.empty());
  }

  /**
   * Perform Hoodie Write. Run Cleaner, schedule compaction and syncs to hive if needed.
   *
   * @param records             Input Records
   * @param checkpointStr       Checkpoint String
   * @param metrics             Metrics
   * @param overallTimerContext Timer Context
   * @return Option Compaction instant if one is scheduled
   */
  private Pair<Option<String>, JavaRDD<WriteStatus>> writeToSink(JavaRDD<HoodieRecord> records, String checkpointStr,
                                                                 HoodieDeltaStreamerMetrics metrics,
                                                                 Timer.Context overallTimerContext) {
    Option<String> scheduledCompactionInstant = Option.empty();
    // filter dupes if needed
    if (cfg.filterDupes) {
      records = DataSourceUtils.dropDuplicates(jssc, records, writeClient.getConfig());
    }

    boolean isEmpty = records.isEmpty();

    // try to start a new commit
    String instantTime = startCommit();
    LOG.info("Starting commit  : " + instantTime);

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
        writeStatusRDD = writeClient.insertOverwrite(records, instantTime).getWriteStatuses();
        break;
      case INSERT_OVERWRITE_TABLE:
        writeStatusRDD = writeClient.insertOverwriteTable(records, instantTime).getWriteStatuses();
        break;
      case DELETE_PARTITION:
        List<String> partitions = records.map(record -> record.getPartitionPath()).distinct().collect();
        writeStatusRDD = writeClient.deletePartitions(partitions, instantTime).getWriteStatuses();
        break;
      default:
        throw new HoodieDeltaStreamerException("Unknown operation : " + cfg.operation);
    }

    long totalErrorRecords = writeStatusRDD.mapToDouble(WriteStatus::getTotalErrorRecords).sum().longValue();
    long totalRecords = writeStatusRDD.mapToDouble(WriteStatus::getTotalRecords).sum().longValue();
    boolean hasErrors = totalErrorRecords > 0;
    if (!hasErrors || cfg.commitOnErrors) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      if (checkpointStr != null) {
        checkpointCommitMetadata.put(CHECKPOINT_KEY, checkpointStr);
      }
      if (cfg.checkpoint != null) {
        checkpointCommitMetadata.put(CHECKPOINT_RESET_KEY, cfg.checkpoint);
      }

      if (hasErrors) {
        LOG.warn("Some records failed to be merged but forcing commit since commitOnErrors set. Errors/Total="
            + totalErrorRecords + "/" + totalRecords);
      }
      String commitActionType = CommitUtils.getCommitActionType(cfg.operation, HoodieTableType.valueOf(cfg.tableType));
      boolean success = writeClient.commit(instantTime, writeStatusRDD, Option.of(checkpointCommitMetadata), commitActionType, Collections.emptyMap());
      if (success) {
        LOG.info("Commit " + instantTime + " successful!");
        this.formatAdapter.getSource().onCommit(checkpointStr);
        // Schedule compaction if needed
        if (cfg.isAsyncCompactionEnabled()) {
          scheduledCompactionInstant = writeClient.scheduleCompaction(Option.empty());
        }

        if (!isEmpty) {
          syncMeta(metrics);
        }
      } else {
        LOG.info("Commit " + instantTime + " failed!");
        throw new HoodieException("Commit " + instantTime + " failed!");
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
      throw new HoodieException("Commit " + instantTime + " failed and rolled-back !");
    }
    long overallTimeMs = overallTimerContext != null ? overallTimerContext.stop() : 0;

    // Send DeltaStreamer Metrics
    metrics.updateDeltaStreamerMetrics(overallTimeMs);
    return Pair.of(scheduledCompactionInstant, writeStatusRDD);
  }

  /**
   * Try to start a new commit.
   * <p>
   * Exception will be thrown if it failed in 2 tries.
   *
   * @return Instant time of the commit
   */
  private String startCommit() {
    final int maxRetries = 2;
    int retryNum = 1;
    RuntimeException lastException = null;
    while (retryNum <= maxRetries) {
      try {
        String instantTime = HoodieActiveTimeline.createNewInstantTime();
        String commitActionType = CommitUtils.getCommitActionType(cfg.operation, HoodieTableType.valueOf(cfg.tableType));
        writeClient.startCommitWithTime(instantTime, commitActionType);
        return instantTime;
      } catch (IllegalArgumentException ie) {
        lastException = ie;
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

  private String getSyncClassShortName(String syncClassName) {
    return syncClassName.substring(syncClassName.lastIndexOf(".") + 1);
  }

  private void syncMeta(HoodieDeltaStreamerMetrics metrics) {
    Set<String> syncClientToolClasses = new HashSet<>(Arrays.asList(cfg.syncClientToolClassNames.split(",")));
    // for backward compatibility
    if (cfg.enableHiveSync) {
      cfg.enableMetaSync = true;
      syncClientToolClasses.add(HiveSyncTool.class.getName());
      LOG.info("When set --enable-hive-sync will use HiveSyncTool for backward compatibility");
    }
    if (cfg.enableMetaSync) {
      FileSystem fs = FSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration());

      TypedProperties metaProps = new TypedProperties();
      metaProps.putAll(props);
      if (props.getBoolean(HiveSyncConfig.HIVE_SYNC_BUCKET_SYNC.key(), HiveSyncConfig.HIVE_SYNC_BUCKET_SYNC.defaultValue())) {
        metaProps.put(HiveSyncConfig.HIVE_SYNC_BUCKET_SYNC_SPEC.key(), HiveSyncConfig.getBucketSpec(props.getString(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key()),
            props.getInteger(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key())));
      }

      for (String impl : syncClientToolClasses) {
        Timer.Context syncContext = metrics.getMetaSyncTimerContext();
        SyncUtilHelpers.runHoodieMetaSync(impl.trim(), metaProps, conf, fs, cfg.targetBasePath, cfg.baseFileFormat);
        long metaSyncTimeMs = syncContext != null ? syncContext.stop() : 0;
        metrics.updateDeltaStreamerMetaSyncMetrics(getSyncClassShortName(impl), metaSyncTimeMs);
      }
    }
  }

  /**
   * Note that depending on configs and source-type, schemaProvider could either be eagerly or lazily created.
   * SchemaProvider creation is a precursor to HoodieWriteClient and AsyncCompactor creation. This method takes care of
   * this constraint.
   */
  public void setupWriteClient() throws IOException {
    if ((null != schemaProvider)) {
      Schema sourceSchema = schemaProvider.getSourceSchema();
      Schema targetSchema = schemaProvider.getTargetSchema();
      reInitWriteClient(sourceSchema, targetSchema);
    }
  }

  private void reInitWriteClient(Schema sourceSchema, Schema targetSchema) throws IOException {
    LOG.info("Setting up new Hoodie Write Client");
    if (isDropPartitionColumns()) {
      targetSchema = HoodieAvroUtils.removeFields(targetSchema, getPartitionColumns(keyGenerator, props));
    }
    registerAvroSchemas(sourceSchema, targetSchema);
    HoodieWriteConfig hoodieCfg = getHoodieClientConfig(targetSchema);
    if (hoodieCfg.isEmbeddedTimelineServerEnabled()) {
      if (!embeddedTimelineService.isPresent()) {
        embeddedTimelineService = EmbeddedTimelineServerHelper.createEmbeddedTimelineService(new HoodieSparkEngineContext(jssc), hoodieCfg);
      } else {
        EmbeddedTimelineServerHelper.updateWriteConfigWithTimelineServer(embeddedTimelineService.get(), hoodieCfg);
      }
    }

    if (null != writeClient) {
      // Close Write client.
      writeClient.close();
    }
    writeClient = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jssc), hoodieCfg, embeddedTimelineService);
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
                    .withPayloadClass(cfg.payloadClassName)
                    .withInlineCompaction(cfg.isInlineCompactionEnabled())
                    .build()
            )
            .withPayloadConfig(
                HoodiePayloadConfig.newBuilder()
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
          HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(new Configuration(fs.getConf())).setBasePath(cfg.targetBasePath).setPayloadClassName(cfg.payloadClassName).build();
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
      throw new HoodieException("Failed to fetch schema from table ", e);
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
      jssc.sc().getConf().registerAvroSchemas(JavaConversions.asScalaBuffer(schemas).toList());
    }
  }

  /**
   * Close all resources.
   */
  public void close() {
    if (null != writeClient) {
      writeClient.close();
      writeClient = null;
    }

    LOG.info("Shutting down embedded timeline server");
    if (embeddedTimelineService.isPresent()) {
      embeddedTimelineService.get().stop();
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

  public Option<HoodieTimeline> getCommitTimelineOpt() {
    return commitTimelineOpt;
  }

  /**
   * Schedule clustering.
   * Called from {@link HoodieDeltaStreamer} when async clustering is enabled.
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
   * Get the list of partition columns as a list of strings.
   *
   * @param keyGenerator KeyGenerator
   * @param props TypedProperties
   * @return List of partition columns.
   */
  private List<String> getPartitionColumns(KeyGenerator keyGenerator, TypedProperties props) {
    String partitionColumns = HoodieSparkUtils.getPartitionColumns(keyGenerator, props);
    return Arrays.asList(partitionColumns.split(","));
  }
}
