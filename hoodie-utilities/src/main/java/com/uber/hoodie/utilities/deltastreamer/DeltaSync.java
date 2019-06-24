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

package com.uber.hoodie.utilities.deltastreamer;

import static com.uber.hoodie.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE;
import static com.uber.hoodie.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME;

import com.codahale.metrics.Timer;
import com.uber.hoodie.AvroConversionUtils;
import com.uber.hoodie.DataSourceUtils;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.KeyGenerator;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.hive.HiveSyncConfig;
import com.uber.hoodie.hive.HiveSyncTool;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.utilities.UtilHelpers;
import com.uber.hoodie.utilities.deltastreamer.HoodieDeltaStreamer.Operation;
import com.uber.hoodie.utilities.exception.HoodieDeltaStreamerException;
import com.uber.hoodie.utilities.schema.RowBasedSchemaProvider;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import com.uber.hoodie.utilities.sources.InputBatch;
import com.uber.hoodie.utilities.transform.Transformer;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConversions;


/**
 * Sync's one batch of data to hoodie dataset
 */
public class DeltaSync implements Serializable {

  protected static volatile Logger log = LogManager.getLogger(DeltaSync.class);
  public static String CHECKPOINT_KEY = "deltastreamer.checkpoint.key";

  /**
   * Delta Sync Config
   */
  private final HoodieDeltaStreamer.Config cfg;

  /**
   * Source to pull deltas from
   */
  private transient SourceFormatAdapter formatAdapter;

  /**
   * Schema provider that supplies the command for reading the input and writing out the target table.
   */
  private transient SchemaProvider schemaProvider;

  /**
   * Allows transforming source to target dataset before writing
   */
  private transient Transformer transformer;

  /**
   * Extract the key for the target dataset
   */
  private KeyGenerator keyGenerator;

  /**
   * Filesystem used
   */
  private transient FileSystem fs;

  /**
   * Spark context
   */
  private transient JavaSparkContext jssc;

  /**
   * Spark Session
   */
  private transient SparkSession sparkSession;

  /**
   * Hive Config
   */
  private transient HiveConf hiveConf;

  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  private final TypedProperties props;

  /**
   * Callback when write client is instantiated
   */
  private transient Function<HoodieWriteClient, Boolean> onInitializingHoodieWriteClient;

  /**
   * Timeline with completed commits
   */
  private transient Optional<HoodieTimeline> commitTimelineOpt;

  /**
   * Write Client
   */
  private transient HoodieWriteClient writeClient;

  /**
   * Table Type
   */
  private final HoodieTableType tableType;


  public DeltaSync(HoodieDeltaStreamer.Config cfg, SparkSession sparkSession,
      SchemaProvider schemaProvider, HoodieTableType tableType, TypedProperties props,
      JavaSparkContext jssc, FileSystem fs, HiveConf hiveConf,
      Function<HoodieWriteClient, Boolean> onInitializingHoodieWriteClient)
      throws IOException {

    this.cfg = cfg;
    this.jssc = jssc;
    this.sparkSession = sparkSession;
    this.fs = fs;
    this.tableType = tableType;
    this.onInitializingHoodieWriteClient = onInitializingHoodieWriteClient;
    this.props = props;
    log.info("Creating delta streamer with configs : " + props.toString());
    this.schemaProvider = schemaProvider;

    refreshTimeline();

    this.transformer = UtilHelpers.createTransformer(cfg.transformerClassName);
    this.keyGenerator = DataSourceUtils.createKeyGenerator(cfg.keyGeneratorClass, props);

    this.formatAdapter = new SourceFormatAdapter(UtilHelpers.createSource(cfg.sourceClassName, props, jssc,
        sparkSession, schemaProvider));

    this.hiveConf = hiveConf;
    if (cfg.filterDupes) {
      cfg.operation = cfg.operation == Operation.UPSERT ? Operation.INSERT : cfg.operation;
    }

    // If schemaRegistry already resolved, setup write-client
    setupWriteClient();
  }

  /**
   * Refresh Timeline
   */
  private void refreshTimeline() throws IOException {
    if (fs.exists(new Path(cfg.targetBasePath))) {
      HoodieTableMetaClient meta = new HoodieTableMetaClient(new Configuration(fs.getConf()), cfg.targetBasePath);
      this.commitTimelineOpt = Optional.of(meta.getActiveTimeline().getCommitsTimeline()
          .filterCompletedInstants());
    } else {
      this.commitTimelineOpt = Optional.empty();
      HoodieTableMetaClient.initTableType(new Configuration(jssc.hadoopConfiguration()), cfg.targetBasePath,
          cfg.storageType, cfg.targetTableName, "archived");
    }
  }

  /**
   * Run one round of delta sync and return new compaction instant if one got scheduled
   */
  public Optional<String> syncOnce() throws Exception {
    Optional<String> scheduledCompaction = Optional.empty();
    HoodieDeltaStreamerMetrics metrics = new HoodieDeltaStreamerMetrics(getHoodieClientConfig(schemaProvider));
    Timer.Context overallTimerContext = metrics.getOverallTimerContext();

    // Refresh Timeline
    refreshTimeline();

    Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> srcRecordsWithCkpt =
        readFromSource(commitTimelineOpt);

    if (null != srcRecordsWithCkpt) {
      // this is the first input batch. If schemaProvider not set, use it and register Avro Schema and start
      // compactor
      if (null == schemaProvider) {
        // Set the schemaProvider if not user-provided
        this.schemaProvider = srcRecordsWithCkpt.getKey();
        // Setup HoodieWriteClient and compaction now that we decided on schema
        setupWriteClient();
      }

      scheduledCompaction = writeToSink(srcRecordsWithCkpt.getRight().getRight(),
          srcRecordsWithCkpt.getRight().getLeft(), metrics, overallTimerContext);
    }

    // Clear persistent RDDs
    jssc.getPersistentRDDs().values().forEach(JavaRDD::unpersist);
    return scheduledCompaction;
  }

  /**
   * Read from Upstream Source and apply transformation if needed
   */
  private Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> readFromSource(
      Optional<HoodieTimeline> commitTimelineOpt) throws Exception {
    // Retrieve the previous round checkpoints, if any
    Optional<String> resumeCheckpointStr = Optional.empty();
    if (commitTimelineOpt.isPresent()) {
      Optional<HoodieInstant> lastCommit = commitTimelineOpt.get().lastInstant();
      if (lastCommit.isPresent()) {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            commitTimelineOpt.get().getInstantDetails(lastCommit.get()).get(), HoodieCommitMetadata.class);
        if (commitMetadata.getMetadata(CHECKPOINT_KEY) != null) {
          resumeCheckpointStr = Optional.of(commitMetadata.getMetadata(CHECKPOINT_KEY));
        } else {
          throw new HoodieDeltaStreamerException(
              "Unable to find previous checkpoint. Please double check if this table "
                  + "was indeed built via delta streamer ");
        }
      }
    } else {
      HoodieTableMetaClient.initTableType(new Configuration(jssc.hadoopConfiguration()), cfg.targetBasePath,
          cfg.storageType, cfg.targetTableName, "archived");
    }
    log.info("Checkpoint to resume from : " + resumeCheckpointStr);

    final Optional<JavaRDD<GenericRecord>> avroRDDOptional;
    final String checkpointStr;
    final SchemaProvider schemaProvider;
    if (transformer != null) {
      // Transformation is needed. Fetch New rows in Row Format, apply transformation and then convert them
      // to generic records for writing
      InputBatch<Dataset<Row>> dataAndCheckpoint = formatAdapter.fetchNewDataInRowFormat(
          resumeCheckpointStr, cfg.sourceLimit);

      Optional<Dataset<Row>> transformed =
          dataAndCheckpoint.getBatch().map(data -> transformer.apply(jssc, sparkSession, data, props));
      checkpointStr = dataAndCheckpoint.getCheckpointForNextBatch();
      avroRDDOptional = transformed.map(t ->
          AvroConversionUtils.createRdd(t, HOODIE_RECORD_STRUCT_NAME, HOODIE_RECORD_NAMESPACE).toJavaRDD()
      );
      // Use Transformed Row's schema if not overridden
      schemaProvider =
          this.schemaProvider == null ? transformed.map(r -> (SchemaProvider) new RowBasedSchemaProvider(r.schema()))
              .orElse(dataAndCheckpoint.getSchemaProvider()) : this.schemaProvider;
    } else {
      // Pull the data from the source & prepare the write
      InputBatch<JavaRDD<GenericRecord>> dataAndCheckpoint =
          formatAdapter.fetchNewDataInAvroFormat(resumeCheckpointStr, cfg.sourceLimit);
      avroRDDOptional = dataAndCheckpoint.getBatch();
      checkpointStr = dataAndCheckpoint.getCheckpointForNextBatch();
      schemaProvider = dataAndCheckpoint.getSchemaProvider();
    }

    if ((!avroRDDOptional.isPresent()) || (avroRDDOptional.get().isEmpty())) {
      log.info("No new data, nothing to commit.. ");
      return null;
    }

    JavaRDD<GenericRecord> avroRDD = avroRDDOptional.get();
    JavaRDD<HoodieRecord> records = avroRDD.map(gr -> {
      HoodieRecordPayload payload = DataSourceUtils.createPayload(cfg.payloadClassName, gr,
          (Comparable) gr.get(cfg.sourceOrderingField));
      return new HoodieRecord<>(keyGenerator.getKey(gr), payload);
    });
    return Pair.of(schemaProvider, Pair.of(checkpointStr, records));
  }

  /**
   * Perform Hoodie Write. Run Cleaner, schedule compaction and syncs to hive if needed
   *
   * @param records Input Records
   * @param checkpointStr Checkpoint String
   * @param metrics Metrics
   * @return Optional Compaction instant if one is scheduled
   */
  private Optional<String> writeToSink(JavaRDD<HoodieRecord> records, String checkpointStr,
      HoodieDeltaStreamerMetrics metrics, Timer.Context overallTimerContext) throws Exception {

    Optional<String> scheduledCompactionInstant = Optional.empty();

    // filter dupes if needed
    if (cfg.filterDupes) {
      // turn upserts to insert
      cfg.operation = cfg.operation == Operation.UPSERT ? Operation.INSERT : cfg.operation;
      records = DataSourceUtils.dropDuplicates(jssc, records, writeClient.getConfig(),
          writeClient.getTimelineServer());

      if (records.isEmpty()) {
        log.info("No new data, nothing to commit.. ");
        return Optional.empty();
      }
    }

    String commitTime = startCommit();
    log.info("Starting commit  : " + commitTime);

    JavaRDD<WriteStatus> writeStatusRDD;
    if (cfg.operation == Operation.INSERT) {
      writeStatusRDD = writeClient.insert(records, commitTime);
    } else if (cfg.operation == Operation.UPSERT) {
      writeStatusRDD = writeClient.upsert(records, commitTime);
    } else if (cfg.operation == Operation.BULK_INSERT) {
      writeStatusRDD = writeClient.bulkInsert(records, commitTime);
    } else {
      throw new HoodieDeltaStreamerException("Unknown operation :" + cfg.operation);
    }

    long totalErrorRecords = writeStatusRDD.mapToDouble(ws -> ws.getTotalErrorRecords()).sum().longValue();
    long totalRecords = writeStatusRDD.mapToDouble(ws -> ws.getTotalRecords()).sum().longValue();
    boolean hasErrors = totalErrorRecords > 0;
    long hiveSyncTimeMs = 0;
    if (!hasErrors || cfg.commitOnErrors) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      checkpointCommitMetadata.put(CHECKPOINT_KEY, checkpointStr);

      if (hasErrors) {
        log.warn("Some records failed to be merged but forcing commit since commitOnErrors set. Errors/Total="
            + totalErrorRecords + "/" + totalRecords);
      }

      boolean success = writeClient.commit(commitTime, writeStatusRDD,
          Optional.of(checkpointCommitMetadata));
      if (success) {
        log.info("Commit " + commitTime + " successful!");

        // Schedule compaction if needed
        if (cfg.isAsyncCompactionEnabled()) {
          scheduledCompactionInstant = writeClient.scheduleCompaction(Optional.of(checkpointCommitMetadata));
        }

        // Sync to hive if enabled
        Timer.Context hiveSyncContext = metrics.getHiveSyncTimerContext();
        syncHive();
        hiveSyncTimeMs = hiveSyncContext != null ? hiveSyncContext.stop() : 0;
      } else {
        log.info("Commit " + commitTime + " failed!");
        throw new HoodieException("Commit " + commitTime + " failed!");
      }
    } else {
      log.error("Delta Sync found errors when writing. Errors/Total="
          + totalErrorRecords + "/" + totalRecords);
      log.error("Printing out the top 100 errors");
      writeStatusRDD.filter(ws -> ws.hasErrors()).take(100).forEach(ws -> {
        log.error("Global error :", ws.getGlobalError());
        if (ws.getErrors().size() > 0) {
          ws.getErrors().entrySet().forEach(r ->
              log.trace("Error for key:" + r.getKey() + " is " + r.getValue()));
        }
      });
      // Rolling back instant
      writeClient.rollback(commitTime);
      throw new HoodieException("Commit " + commitTime + " failed and rolled-back !");
    }
    long overallTimeMs = overallTimerContext != null ? overallTimerContext.stop() : 0;

    // Send DeltaStreamer Metrics
    metrics.updateDeltaStreamerMetrics(overallTimeMs, hiveSyncTimeMs);

    return scheduledCompactionInstant;
  }

  private String startCommit() {
    final int maxRetries = 2;
    int retryNum = 1;
    RuntimeException lastException = null;
    while (retryNum <= maxRetries) {
      try {
        return writeClient.startCommit();
      } catch (IllegalArgumentException ie) {
        lastException = ie;
        log.error("Got error trying to start a new commit. Retrying after sleeping for a sec", ie);
        retryNum++;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          //No-Op
        }
      }
    }
    throw lastException;
  }

  /**
   * Sync to Hive
   */
  private void syncHive() {
    if (cfg.enableHiveSync) {
      HiveSyncConfig hiveSyncConfig = DataSourceUtils.buildHiveSyncConfig(props, cfg.targetBasePath);
      log.info("Syncing target hoodie table with hive table(" + hiveSyncConfig.tableName
          + "). Hive metastore URL :" + hiveSyncConfig.jdbcUrl + ", basePath :" + cfg.targetBasePath);

      new HiveSyncTool(hiveSyncConfig, hiveConf, fs).syncHoodieTable();
    }
  }

  /**
   * Note that depending on configs and source-type, schemaProvider could either be eagerly or lazily created.
   * SchemaProvider creation is a precursor to HoodieWriteClient and AsyncCompactor creation. This method takes care of
   * this constraint.
   */
  public void setupWriteClient() {
    log.info("Setting up Hoodie Write Client");
    if ((null != schemaProvider) && (null == writeClient)) {
      registerAvroSchemas(schemaProvider);
      HoodieWriteConfig hoodieCfg = getHoodieClientConfig(schemaProvider);
      writeClient = new HoodieWriteClient<>(jssc, hoodieCfg, true);
      onInitializingHoodieWriteClient.apply(writeClient);
    }
  }

  /**
   * Helper to construct Write Client config
   *
   * @param schemaProvider Schema Provider
   */
  private HoodieWriteConfig getHoodieClientConfig(SchemaProvider schemaProvider) {
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder()
            .withProps(props)
            .withPath(cfg.targetBasePath)
            .combineInput(cfg.filterDupes, true)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withPayloadClass(cfg.payloadClassName)
                // Inline compaction is disabled for continuous mode. otherwise enabled for MOR
                .withInlineCompaction(cfg.isInlineCompactionEnabled()).build())
            .forTable(cfg.targetTableName)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
            .withAutoCommit(false);
    if (null != schemaProvider) {
      builder = builder.withSchema(schemaProvider.getTargetSchema().toString());
    }

    return builder.build();
  }

  /**
   * Register Avro Schemas
   *
   * @param schemaProvider Schema Provider
   */
  private void registerAvroSchemas(SchemaProvider schemaProvider) {
    // register the schemas, so that shuffle does not serialize the full schemas
    if (null != schemaProvider) {
      List<Schema> schemas = Arrays.asList(schemaProvider.getSourceSchema(), schemaProvider.getTargetSchema());
      log.info("Registering Schema :" + schemas);
      jssc.sc().getConf().registerAvroSchemas(JavaConversions.asScalaBuffer(schemas).toList());
    }
  }

  /**
   * Close all resources
   */
  public void close() {
    if (null != writeClient) {
      writeClient.close();
      writeClient = null;
    }
  }
}
