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

package org.apache.hudi.utilities;

import com.codahale.metrics.Timer;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.streamer.BaseErrorTableWriter;
import org.apache.hudi.utilities.streamer.ErrorEvent;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.streamer.HoodieStreamerMetrics;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_BASE_PATH;
import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_ENABLED;
import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_INSERT_PARALLELISM_VALUE;
import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_UPSERT_PARALLELISM_VALUE;
import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TARGET_TABLE;
import static org.apache.hudi.config.HoodieWriteConfig.AUTO_COMMIT_ENABLE;
import static org.apache.hudi.config.HoodieWriteConfig.BASE_PATH;
import static org.apache.hudi.config.HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE;
import static org.apache.hudi.config.HoodieWriteConfig.INSERT_PARALLELISM_VALUE;
import static org.apache.hudi.config.HoodieWriteConfig.TBL_NAME;
import static org.apache.hudi.config.HoodieWriteConfig.UPSERT_PARALLELISM_VALUE;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME;

public class JsonQuarantineTableWriter<T extends ErrorEvent> extends BaseErrorTableWriter<T> {

  public static final String ERROR_CONFIG_PREFIX = "hoodie.errortable.writeconfig.";

  private static final Logger LOG = LoggerFactory.getLogger(JsonQuarantineTableWriter.class);
  /**
   * error record/event of base table for which quarantine table is enabled
   */
  private static final String DATA_RECORD_FIELD = "data_record";

  /**
   * last completed timestamp of base table
   * this column in further used for partitioning table
   */
  private static final String BASE_TABLE_COMMITED_INSTANT_FIELD = "base_table_commited_instant_time";

  /**
   * type of failure
   */
  private static final String FAILURE_TYPE = "failure_type";

  private static final String BASE_TABLE_EMPTY_COMMIT = "00000000";
  private static final String QUARANTINE_TABLE_AVSC = "/quarantine-table.avsc";

  public static String ERROR_TABLE_PAYLOAD_CLASS = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload";


  /**
   * Delta Sync Config.
   */
  private final HoodieStreamer.Config cfg;
  /**
   * Bag of properties with source, hoodie client, key generator etc.
   * <p>
   * NOTE: These properties are already consolidated w/ CLI provided config-overrides
   */
  private final TypedProperties props;
  private final HoodieWriteConfig quarantineTableCfg;
  private final String parentTableBasePath;
  private JavaRDD<InternalErrorEvent> errorEventsRdd = null;


  private final Option<HoodieStreamerMetrics> metricsOpt;

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

  private transient SparkRDDWriteClient quarantineTableWriteClient;
  private transient Schema schema;

  public JsonQuarantineTableWriter(HoodieStreamer.Config cfg,
                                   SparkSession sparkSession,
                                   TypedProperties props,
                                   HoodieSparkEngineContext hoodieSparkContext,
                                   FileSystem fs) throws IOException {
    this(cfg, sparkSession, props, hoodieSparkContext, fs, Option.empty());
  }

  public JsonQuarantineTableWriter(HoodieStreamer.Config cfg,
                                   SparkSession sparkSession,
                                   TypedProperties props,
                                   HoodieSparkEngineContext hoodieSparkContext,
                                   FileSystem fs,
                                   Option<HoodieStreamerMetrics> metricsOpt) throws IOException {
    super(cfg, sparkSession, props, hoodieSparkContext, fs);
    this.cfg = cfg;
    this.jssc = hoodieSparkContext.getJavaSparkContext();
    this.sparkSession = sparkSession;
    this.fs = fs;
    this.props = props;
    this.metricsOpt = metricsOpt;
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(props).build();
    this.parentTableBasePath = writeConfig.getBasePath();
    try (InputStream inputStream = this.getClass().getResourceAsStream(QUARANTINE_TABLE_AVSC)) {
      this.schema = new Schema.Parser().parse(inputStream);
    }
    this.quarantineTableCfg = getQuarantineTableWriteConfig();
    this.quarantineTableWriteClient = new SparkRDDWriteClient<>(hoodieSparkContext, quarantineTableCfg);
  }

  @Override
  public void addErrorEvents(JavaRDD<T> errorEvent) {
    errorEventsRdd =
        (errorEventsRdd == null) ? createInternalErrorEvent(errorEvent) : errorEventsRdd.union(createInternalErrorEvent(errorEvent));
  }

  @Override
  public Option<JavaRDD<HoodieAvroRecord>> getErrorEvents(String baseTableInstantTime, Option<String> committedInstantTime) {
    String committedInstantTimeStr = committedInstantTime.isPresent() ? committedInstantTime.get() : BASE_TABLE_EMPTY_COMMIT;
    final String committedInstantTimeStrFormatted;
    if (committedInstantTimeStr.startsWith("[") && committedInstantTimeStr.endsWith("]")) {
      committedInstantTimeStrFormatted = committedInstantTimeStr.substring(1, committedInstantTimeStr.length() - 1);
    } else {
      committedInstantTimeStrFormatted = committedInstantTimeStr;
    }

    return createErrorEventsRdd(errorEventsRdd.map(ev -> RowFactory.create(ev.dataRecord,
        ev.sourceTableBasePath,
        ev.failureType,
        baseTableInstantTime,
        committedInstantTimeStrFormatted,
        JavaConverters.mapAsScalaMapConverter(ev.metadata).asScala())));
  }

  public String startCommit(String instantTime) {
    try {
      if (!fs.exists(new Path(quarantineTableCfg.getBasePath()))) {
        initialiseTable();
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to check file exists in quarantineEvents processing", e);
    }
    final int maxRetries = 2;
    int retryNum = 1;
    HoodieException startCommitException = new HoodieException("Unable to start commit for Error Table");
    while (retryNum <= maxRetries) {
      try {
        String commitActionType = CommitUtils.getCommitActionType(WriteOperationType.UPSERT, HoodieTableType.COPY_ON_WRITE);
        quarantineTableWriteClient.startCommitWithTime(instantTime, commitActionType);
        return instantTime;
      } catch (IllegalArgumentException ie) {
        startCommitException = new HoodieException(ie);
        retryNum++;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.error("Interrupted while trying to write to Quarantine table", e);
          Thread.currentThread().interrupt();
        }
      }
    }
    throw startCommitException;
  }

  public HoodieWriteConfig getQuarantineTableWriteConfig() {
    return HoodieWriteConfig.newBuilder()
        .withProperties(getQuarantineWriteProperties(props))
        .withSchema(schema.toString())
        .build();
  }

  @Override
  public boolean upsertAndCommit(String baseTableInstantTime, Option<String> commitedInstantTime) {
    boolean result = true;
    Option<Timer.Context> errorTableWriteTimerContextOpt = metricsOpt.map(HoodieStreamerMetrics::getErrorTableWriteTimerContext);
    if (errorEventsRdd != null) {
      try {
        errorEventsRdd.persist(StorageLevel.MEMORY_AND_DISK());
        if (!errorEventsRdd.isEmpty()) {
          result = getErrorEvents(baseTableInstantTime, commitedInstantTime)
              .map(rdd -> {
                String instantTime = startCommit(HoodieActiveTimeline.createNewInstantTime());
                JavaRDD<WriteStatus> writeStatusJavaRDD = quarantineTableWriteClient.bulkInsert(rdd, instantTime);
                long totalRecords = writeStatusJavaRDD.mapToDouble(WriteStatus::getTotalRecords).sum().longValue();
                LOG.info("Total number of records written to Error Table {} in Commit Instant {}", totalRecords, instantTime);
                boolean success = quarantineTableWriteClient.commit(instantTime, writeStatusJavaRDD, Option.empty(),
                    COMMIT_ACTION, Collections.emptyMap());
                LOG.info("Resul of error table commit {} is {}", instantTime, success);
                return success;
              }).orElse(true);
        }
      } finally {
        errorEventsRdd.unpersist();
        errorEventsRdd = null;
      }
    }
    metricsOpt.ifPresent(x -> {
      long errorTableCommitDuration = errorTableWriteTimerContextOpt.map(Timer.Context::stop).orElse(0L);
      x.updateErrorTableCommitDuration(errorTableCommitDuration);
    });
    return result;
  }

  @Override
  public JavaRDD<WriteStatus> upsert(String errorTableInstantTime, String baseTableInstantTime, Option<String> commitedInstantTime) {
    if (errorEventsRdd != null) {
      errorEventsRdd.persist(StorageLevel.MEMORY_AND_DISK());
      return getErrorEvents(baseTableInstantTime, commitedInstantTime)
        .map(rdd -> {
          startCommit(errorTableInstantTime);
          JavaRDD<WriteStatus> writeStatusJavaRDD = quarantineTableWriteClient.bulkInsert(rdd, errorTableInstantTime);
          return writeStatusJavaRDD;
        }).get();
    }
    return null;
  }

  /**
   * caller should ensure writeStatuses is not null to avoid IllegalArgumentException during timeline operations
   * as there won't be any inflight instant to be marked as complete
   */
  @Override
  public boolean commit(String baseTableInstantTime, JavaRDD<WriteStatus> writeStatuses) {
    long totalRecords = writeStatuses.mapToDouble(WriteStatus::getTotalRecords).sum().longValue();
    LOG.info("Total number of records written to Error Table {} in Commit Instant {}", totalRecords, baseTableInstantTime);
    boolean success = quarantineTableWriteClient.commit(baseTableInstantTime, writeStatuses, Option.empty(),
        COMMIT_ACTION, Collections.emptyMap());
    LOG.info("Result of error table commit {} is {}", baseTableInstantTime, success);
    if (errorEventsRdd != null && !errorEventsRdd.getStorageLevel().equals(StorageLevel.NONE())) {
      errorEventsRdd.unpersist();
      errorEventsRdd = null;
    }
    return success;
  }

  private void initialiseTable() {
    try {
      HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(HoodieTableType.COPY_ON_WRITE)
          .setTableName(quarantineTableCfg.getTableName())
          .setArchiveLogFolder(ARCHIVELOG_FOLDER.defaultValue())
          .setPayloadClassName(quarantineTableCfg.getPayloadClass())
          .setBaseFileFormat("parquet")
          .setPartitionFields(PARTITION_PATH_METADATA_FIELD)
          .initTable(new Configuration(jssc.hadoopConfiguration()), ConfigUtils.getStringWithAltKeys(props, ERROR_TABLE_BASE_PATH));
    } catch (IOException e) {
      LOG.warn("Exception while initializing the Quarantine table", e);
    }
  }

  public Option<JavaRDD<HoodieAvroRecord>> createErrorEventsRdd(JavaRDD<Row> rowJavaRDD) {
    LOG.info("Processing createErrorEventsRdd");
    Option<JavaRDD<HoodieAvroRecord>> rddOption = Option.of(HoodieSparkUtils.createRdd(
        sparkSession.createDataFrame(rowJavaRDD, AvroConversionUtils.convertAvroSchemaToStructType(schema)), HOODIE_RECORD_STRUCT_NAME,
        HOODIE_RECORD_NAMESPACE, false, Option.empty()
    ).toJavaRDD().map(x -> {
      HoodieRecordPayload recordPayload = DataSourceUtils.createPayload(ERROR_TABLE_PAYLOAD_CLASS, x);
      String partitionPath = String.valueOf(x.get(BASE_TABLE_COMMITED_INSTANT_FIELD)).substring(0, 8);
      // For improving indexing performance, records keys are created with common prefix as FAILURE_TYPE.
      String recordKey = String.format("%s_%s", x.get(FAILURE_TYPE),
          org.apache.commons.codec.digest.DigestUtils.sha256Hex(x.get(DATA_RECORD_FIELD).toString()));
      HoodieKey key = new HoodieKey(recordKey, partitionPath);
      return new HoodieAvroRecord<>(key, recordPayload);
    }));
    LOG.info("Processing createErrorEventsRdd done.");
    return rddOption;
  }

  private JavaRDD<InternalErrorEvent> createInternalErrorEvent(JavaRDD<T> errorEvent) {
    return errorEvent.map(x -> new InternalErrorEvent((String) x.getPayload(),
        parentTableBasePath, x.getReason().name(), new HashMap<>()));
  }

  private static TypedProperties getQuarantineWriteProperties(TypedProperties props) {
    TypedProperties properties = new TypedProperties();
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      if (entry.getKey() instanceof String && ((String) entry.getKey()).startsWith(ERROR_CONFIG_PREFIX)) {
        properties.put(((String) entry.getKey()).substring(ERROR_CONFIG_PREFIX.length()), entry.getValue());
      }
    }

    //translate error table configs if corresponding is not set
    if (ConfigUtils.getStringWithAltKeys(properties, BASE_PATH, true) == null) {
      properties.put(BASE_PATH.key(), ConfigUtils.getStringWithAltKeys(props, ERROR_TABLE_BASE_PATH));
    }
    if (ConfigUtils.getStringWithAltKeys(properties, TBL_NAME, true) == null) {
      properties.put(TBL_NAME.key(), ConfigUtils.getStringWithAltKeys(props, ERROR_TARGET_TABLE));
    }
    if (ConfigUtils.getIntWithAltKeys(properties, BULKINSERT_PARALLELISM_VALUE) == 0) {
      properties.put(BULKINSERT_PARALLELISM_VALUE.key(), ConfigUtils.getIntWithAltKeys(props, ERROR_TABLE_INSERT_PARALLELISM_VALUE));
    }
    if (ConfigUtils.getIntWithAltKeys(properties, INSERT_PARALLELISM_VALUE) == 0) {
      properties.put(INSERT_PARALLELISM_VALUE.key(), ConfigUtils.getIntWithAltKeys(props, ERROR_TABLE_INSERT_PARALLELISM_VALUE));
    }
    if (ConfigUtils.getIntWithAltKeys(properties, UPSERT_PARALLELISM_VALUE) == 0) {
      properties.put(UPSERT_PARALLELISM_VALUE.key(), ConfigUtils.getIntWithAltKeys(props, ERROR_TABLE_UPSERT_PARALLELISM_VALUE));
    }
    //hardcoded to false
    properties.put(ERROR_TABLE_ENABLED.key(), false);
    properties.put(HoodieMetadataConfig.ENABLE.key(), false);
    properties.put(AUTO_COMMIT_ENABLE.key(), false);

    return properties;
  }

  private static class InternalErrorEvent implements Serializable {
    private final String dataRecord;
    private final String sourceTableBasePath;
    private final String failureType;
    private final Map<String, String> metadata;

    public InternalErrorEvent(String dataRecord, String sourceTableBasePath, String failureType, Map<String, String> metadata) {
      this.dataRecord = dataRecord;
      this.sourceTableBasePath = sourceTableBasePath;
      this.failureType = failureType;
      this.metadata = metadata;
    }
  }
}

