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

package org.apache.hudi.error;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.HoodieErrorTableConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.OverwriteWithLatestAvroSchemaPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_FILE_ID_FIELD;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_TABLE_NAME;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_RECORD_UUID;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_RECORD_TS;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_RECORD_SCHEMA;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_RECORD_RECORD;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_RECORD_MESSAGE;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_RECORD_CONTEXT;

/**
 * Writer implementation backed by an internal hudi table. Error records are saved within an internal COW table
 * called Error table.
 */
public abstract class HoodieBackedErrorTableWriter<O>  implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieBackedErrorTableWriter.class);

  protected HoodieWriteConfig errorTableWriteConfig;
  protected HoodieWriteConfig datasetWriteConfig;
  protected String tableName;

  protected HoodieTableMetaClient metaClient;
  protected SerializableConfiguration hadoopConf;
  protected final transient HoodieEngineContext engineContext;
  protected String basePath;

  protected HoodieBackedErrorTableWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig, HoodieEngineContext engineContext) {
    this.datasetWriteConfig = writeConfig;
    this.engineContext = engineContext;
    this.hadoopConf = new SerializableConfiguration(hadoopConf);

    if (writeConfig.errorTableEnabled()) {
      this.tableName = writeConfig.getTableName() + HoodieErrorTableConfig.ERROR_TABLE_NAME_SUFFIX;
      this.basePath = getErrorTableBasePath(writeConfig);
      this.errorTableWriteConfig = createErrorDataWriteConfig(writeConfig);
      try {
        bootstrapErrorTable(metaClient);
      } catch (IOException e) {
        throw new HoodieException("init hoodie error table fail!", e);
      }
    }
  }

  /**
   * Create a {@code HoodieWriteConfig} to use for the Error Table.
   *
   * @param writeConfig {@code HoodieWriteConfig} of the main dataset writer
   */
  private HoodieWriteConfig createErrorDataWriteConfig(HoodieWriteConfig writeConfig) {
    int parallelism = writeConfig.getErrorTableInsertParallelism();
    // Create the write config for the metadata table by borrowing options from the main write config.
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withEmbeddedTimelineServerEnabled(false)
        .withPath(basePath)
        .withSchema(HoodieErrorTableConfig.ERROR_TABLE_SCHEMA)
        .forTable(getErrorTableName(writeConfig))
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(writeConfig.getErrorTableCleanerCommitsRetained())
            .archiveCommitsWith(writeConfig.getErrorTableMinCommitsToKeep(),
                writeConfig.getMetadataMaxCommitsToKeep()).build())
        .withParallelism(parallelism, parallelism);

    return builder.build();
  }

  public HoodieWriteConfig getWriteConfig() {
    return errorTableWriteConfig;
  }

  /**
   *  Init if hudi error table not exit.
   * @param datasetMetaClient
   * @throws IOException
   */
  private void bootstrapErrorTable(HoodieTableMetaClient datasetMetaClient) throws IOException {

    if (datasetMetaClient != null) {
      boolean exists = datasetMetaClient.getFs().exists(new Path(errorTableWriteConfig.getBasePath(), HoodieTableMetaClient.METAFOLDER_NAME));
      if (exists) {
        return;
      }
    }

    this.metaClient = HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(tableName)
        .setArchiveLogFolder("archived")
        .setPayloadClassName(OverwriteWithLatestAvroPayload.class.getName())
        .setBaseFileFormat(HoodieFileFormat.PARQUET.toString())
        .initTable(new Configuration(hadoopConf.get()), errorTableWriteConfig.getBasePath());
  }

  private String getErrorTableBasePath(HoodieWriteConfig writeConfig) {

    if (StringUtils.isNullOrEmpty(writeConfig.getErrorTableBasePath())) {
      return writeConfig.getBasePath() + Path.SEPARATOR +  HoodieTableMetaClient.METAFOLDER_NAME + Path.SEPARATOR + "errors";
    }
    return writeConfig.getErrorTableBasePath();
  }

  private String getErrorTableName(HoodieWriteConfig writeConfig) {

    return StringUtils.isNullOrEmpty(writeConfig.getErrorTableName())
        ? writeConfig.getTableName() + HoodieErrorTableConfig.ERROR_TABLE_NAME_SUFFIX : writeConfig.getErrorTableName();
  }

  public abstract void commit(O data, String schema, String tableName);

  public HoodieRecord createErrorRecord(HoodieRecord hoodieRecord, HashMap<HoodieKey, Throwable> errorsMap, String schema, String tableName) {

    String uuid = UUID.randomUUID().toString();
    long timeMillis = System.currentTimeMillis();
    String ts = String.valueOf(timeMillis);
    DateTimeZone dateTimeZone = null;
    String partitionPath = new DateTime(timeMillis, dateTimeZone).toString("yyyy/MM/dd");

    HoodieKey hoodieKey = hoodieRecord.getKey();

    HoodieRecordLocation hoodieRecordLocation = null;
    if (hoodieRecord.getNewLocation().isPresent()) {
      hoodieRecordLocation = (HoodieRecordLocation) hoodieRecord.getNewLocation().get();
    }

    String instancTime = hoodieRecordLocation == null ? "" : hoodieRecordLocation.getInstantTime();
    String fileId = hoodieRecordLocation == null ? "" : hoodieRecordLocation.getFileId();
    String message = errorsMap.get(hoodieKey).toString();

    OverwriteWithLatestAvroSchemaPayload data = (OverwriteWithLatestAvroSchemaPayload) hoodieRecord.getData();
    GenericRecord genericRecord = null;
    try {
      genericRecord = HoodieAvroUtils.bytesToAvro(data.recordBytes, new Schema.Parser().parse(data.getSchema()));
    } catch (IOException e) {
      LOG.error("Serialization failed", e);
    }
    Map<String, String> context = new HashMap<>();
    context.put(ERROR_COMMIT_TIME_METADATA_FIELD, instancTime);
    context.put(ERROR_RECORD_KEY_METADATA_FIELD, hoodieKey.getRecordKey());
    context.put(ERROR_PARTITION_PATH_METADATA_FIELD, hoodieRecord.getPartitionPath());
    context.put(ERROR_FILE_ID_FIELD, fileId);
    context.put(ERROR_TABLE_NAME, tableName != null ? tableName : "");

    GenericRecord errorGenericRecord = new GenericData.Record(new Schema.Parser().parse(HoodieErrorTableConfig.ERROR_TABLE_SCHEMA));

    errorGenericRecord.put(ERROR_RECORD_UUID, uuid);
    errorGenericRecord.put(ERROR_RECORD_TS, ts);
    errorGenericRecord.put(ERROR_RECORD_SCHEMA, schema);
    errorGenericRecord.put(ERROR_RECORD_RECORD, genericRecord != null ? genericRecord.toString() : "");
    errorGenericRecord.put(ERROR_RECORD_MESSAGE, message);
    errorGenericRecord.put(ERROR_RECORD_CONTEXT, context);

    HoodieAvroPayload hoodieAvroPayload = new HoodieAvroPayload(Option.of(errorGenericRecord));

    HoodieKey errorHoodieKey = new HoodieKey(uuid, partitionPath);
    return new HoodieAvroRecord(errorHoodieKey, hoodieAvroPayload);
  }

  public List<HoodieRecord> createErrorRecord(List<HoodieRecord> failedRecords, HashMap<HoodieKey, Throwable> errorsMap, String schema, String tableName) {
    List<HoodieRecord> errorHoodieRecords = new ArrayList<>();
    for (HoodieRecord hoodieRecord : failedRecords) {
      errorHoodieRecords.add(createErrorRecord(hoodieRecord, errorsMap, schema, tableName));
    }
    return errorHoodieRecords;
  }
}