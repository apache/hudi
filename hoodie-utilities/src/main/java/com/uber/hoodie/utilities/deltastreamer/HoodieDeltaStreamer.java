/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
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
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.configs.HiveSyncJobConfig;
import com.uber.hoodie.configs.HoodieDeltaStreamerJobConfig;
import com.uber.hoodie.hive.HiveSyncTool;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.utilities.HiveIncrementalPuller;
import com.uber.hoodie.utilities.UtilHelpers;
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
 * An Utility which can incrementally take the output from {@link HiveIncrementalPuller} and apply it to the target
 * dataset. Does not maintain any state, queries at runtime to see how far behind the target dataset is from the source
 * dataset. This can be overriden to force sync from a timestamp.
 */
public class HoodieDeltaStreamer implements Serializable {

  private static volatile Logger log = LogManager.getLogger(HoodieDeltaStreamer.class);

  public static String CHECKPOINT_KEY = "deltastreamer.checkpoint.key";

  private final HoodieDeltaStreamerJobConfig cfg;

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
   * Timeline with completed commits
   */
  private transient Optional<HoodieTimeline> commitTimelineOpt;

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
  TypedProperties props;

  public HoodieDeltaStreamer(HoodieDeltaStreamerJobConfig cfg, JavaSparkContext jssc) throws IOException {
    this(cfg, jssc, FSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration()),
        getDefaultHiveConf(jssc.hadoopConfiguration()));
  }

  public HoodieDeltaStreamer(HoodieDeltaStreamerJobConfig cfg, JavaSparkContext jssc, FileSystem fs, HiveConf hiveConf)
      throws IOException {
    this.cfg = cfg;
    this.jssc = jssc;
    this.sparkSession = SparkSession.builder().config(jssc.getConf()).getOrCreate();
    this.fs = FSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration());

    if (fs.exists(new Path(cfg.targetBasePath))) {
      HoodieTableMetaClient meta = new HoodieTableMetaClient(fs.getConf(), cfg.targetBasePath);
      this.commitTimelineOpt = Optional.of(meta.getActiveTimeline().getCommitsTimeline()
          .filterCompletedInstants());
    } else {
      this.commitTimelineOpt = Optional.empty();
    }

    this.props = UtilHelpers.readConfig(fs, new Path(cfg.propsFilePath), cfg.configs).getConfig();
    log.info("Creating delta streamer with configs : " + props.toString());
    this.schemaProvider = UtilHelpers.createSchemaProvider(cfg.schemaProviderClassName, props, jssc);
    this.transformer = UtilHelpers.createTransformer(cfg.transformerClassName);
    this.keyGenerator = DataSourceUtils.createKeyGenerator(cfg.keyGeneratorClass, props);

    this.formatAdapter =
        new SourceFormatAdapter(UtilHelpers.createSource(cfg.sourceClassName, props, jssc, sparkSession,
            schemaProvider));

    this.hiveConf = hiveConf;
  }

  private static HiveConf getDefaultHiveConf(Configuration cfg) {
    HiveConf hiveConf = new HiveConf();
    hiveConf.addResource(cfg);
    return hiveConf;
  }

  public void sync() throws Exception {
    HoodieDeltaStreamerMetrics metrics = new HoodieDeltaStreamerMetrics(getHoodieClientConfig(null));
    Timer.Context overallTimerContext = metrics.getOverallTimerContext();
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
      HoodieTableMetaClient.initTableType(jssc.hadoopConfiguration(), cfg.targetBasePath,
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
      return;
    }

    registerAvroSchemas(schemaProvider);

    JavaRDD<GenericRecord> avroRDD = avroRDDOptional.get();
    JavaRDD<HoodieRecord> records = avroRDD.map(gr -> {
      HoodieRecordPayload payload = DataSourceUtils.createPayload(cfg.payloadClassName, gr,
          (Comparable) DataSourceUtils.getNestedFieldVal(gr, cfg.sourceOrderingField));
      return new HoodieRecord<>(keyGenerator.getKey(gr), payload);
    });

    // filter dupes if needed
    HoodieWriteConfig hoodieCfg = getHoodieClientConfig(schemaProvider);
    if (cfg.filterDupes) {
      // turn upserts to insert
      cfg.operation =
          cfg.operation == HoodieDeltaStreamerJobConfig.Operation.UPSERT ? HoodieDeltaStreamerJobConfig.Operation.INSERT
              : cfg.operation;
      records = DataSourceUtils.dropDuplicates(jssc, records, hoodieCfg);

      if (records.isEmpty()) {
        log.info("No new data, nothing to commit.. ");
        return;
      }
    }

    // Perform the write
    HoodieWriteClient client = new HoodieWriteClient<>(jssc, hoodieCfg, true);
    String commitTime = client.startCommit();
    log.info("Starting commit  : " + commitTime);

    JavaRDD<WriteStatus> writeStatusRDD;
    if (cfg.operation == HoodieDeltaStreamerJobConfig.Operation.INSERT) {
      writeStatusRDD = client.insert(records, commitTime);
    } else if (cfg.operation == HoodieDeltaStreamerJobConfig.Operation.UPSERT) {
      writeStatusRDD = client.upsert(records, commitTime);
    } else if (cfg.operation == HoodieDeltaStreamerJobConfig.Operation.BULK_INSERT) {
      writeStatusRDD = client.bulkInsert(records, commitTime);
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

      boolean success = client.commit(commitTime, writeStatusRDD,
          Optional.of(checkpointCommitMetadata));
      if (success) {
        log.info("Commit " + commitTime + " successful!");
        // Sync to hive if enabled
        Timer.Context hiveSyncContext = metrics.getHiveSyncTimerContext();
        syncHive();
        hiveSyncTimeMs = hiveSyncContext != null ? hiveSyncContext.stop() : 0;
      } else {
        log.info("Commit " + commitTime + " failed!");
      }
    } else {
      log.error("There are errors when ingesting records. Errors/Total="
          + totalErrorRecords + "/" + totalRecords);
      log.error("Printing out the top 100 errors");
      writeStatusRDD.filter(ws -> ws.hasErrors()).take(100).forEach(ws -> {
        log.error("Global error :", ws.getGlobalError());
        if (ws.getErrors().size() > 0) {
          ws.getErrors().entrySet().forEach(r ->
              log.trace("Error for key:" + r.getKey() + " is " + r.getValue()));
        }
      });
    }
    client.close();
    long overallTimeMs = overallTimerContext != null ? overallTimerContext.stop() : 0;

    // Send DeltaStreamer Metrics
    metrics.updateDeltaStreamerMetrics(overallTimeMs, hiveSyncTimeMs);
  }

  public void syncHive() {
    if (cfg.enableHiveSync) {
      HiveSyncJobConfig hiveSyncJobConfig = DataSourceUtils.buildHiveSyncJobConfig(props, cfg.targetBasePath);
      log.info("Syncing target hoodie table with hive table(" + hiveSyncJobConfig.tableName
          + "). Hive metastore URL :" + hiveSyncJobConfig.jdbcUrl + ", basePath :" + cfg.targetBasePath);

      new HiveSyncTool(hiveSyncJobConfig, hiveConf, fs).syncHoodieTable();
    }
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

  private HoodieWriteConfig getHoodieClientConfig(SchemaProvider schemaProvider) {
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder().withPath(cfg.targetBasePath)
            .withAutoCommit(false).combineInput(cfg.filterDupes, true)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withPayloadClass(cfg.payloadClassName)
                // turn on inline compaction by default, for MOR tables
                .withInlineCompaction(HoodieTableType.valueOf(cfg.storageType) == HoodieTableType.MERGE_ON_READ)
                .build())
            .forTable(cfg.targetTableName)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
            .withProps(props);
    if (null != schemaProvider) {
      builder = builder.withSchema(schemaProvider.getTargetSchema().toString());
    }

    return builder.build();
  }

  public static void main(String[] args) throws Exception {
    final HoodieDeltaStreamerJobConfig cfg = new HoodieDeltaStreamerJobConfig();
    cfg.parseJobConfig(args, true);

    JavaSparkContext jssc = UtilHelpers.buildSparkContext("delta-streamer-" + cfg.targetTableName, cfg.sparkMaster);
    new HoodieDeltaStreamer(cfg, jssc).sync();
  }

  public SourceFormatAdapter getFormatAdapter() {
    return formatAdapter;
  }

  public SchemaProvider getSchemaProvider() {
    return schemaProvider;
  }

  public Transformer getTransformer() {
    return transformer;
  }

  public KeyGenerator getKeyGenerator() {
    return keyGenerator;
  }

  public FileSystem getFs() {
    return fs;
  }

  public Optional<HoodieTimeline> getCommitTimelineOpt() {
    return commitTimelineOpt;
  }

  public JavaSparkContext getJssc() {
    return jssc;
  }

  public SparkSession getSparkSession() {
    return sparkSession;
  }

  public HiveConf getHiveConf() {
    return hiveConf;
  }

  public TypedProperties getProps() {
    return props;
  }
}
