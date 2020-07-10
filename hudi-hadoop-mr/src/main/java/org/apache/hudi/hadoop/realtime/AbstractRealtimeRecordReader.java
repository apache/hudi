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

package org.apache.hudi.hadoop.realtime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.log.LogReaderUtils.readLatestSchemaFromLogFiles;
import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.addPartitionFields;
import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.generateProjectionSchema;
import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getNameToFieldMap;
import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.orderFields;
import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.readSchema;

/**
 * Record Reader implementation to merge fresh avro data with base parquet data, to support real time queries.
 */
public abstract class AbstractRealtimeRecordReader {
  private static final Logger LOG = LogManager.getLogger(AbstractRealtimeRecordReader.class);

  private final String basePath;
  protected final JobConf jobConf;
  protected final boolean usesCustomPayload;
  // Schema handles
  private Schema readerSchema;
  private Schema writerSchema;
  private Schema hiveSchema;

  public AbstractRealtimeRecordReader(HoodieRealtimeFileSplit split, JobConf job) {
    this.basePath = split.getBasePath();
    this.jobConf = job;
    LOG.info("cfg ==> " + job.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR));
    LOG.info("columnIds ==> " + job.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
    LOG.info("partitioningColumns ==> " + job.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, ""));
    try {
      this.usesCustomPayload = usesCustomPayload();
      LOG.info("usesCustomPayload ==> " + this.usesCustomPayload);
      String logMessage = "About to read compacted logs " + split.getDeltaLogPaths() + " for base split "
          + split.getPath() + ", projecting cols %s";
      init(split.getDeltaLogPaths(), logMessage, Option.of(split.getPath()));
    } catch (IOException e) {
      throw new HoodieIOException("Could not create HoodieRealtimeRecordReader on path " + split.getPath(), e);
    }
  }

  public AbstractRealtimeRecordReader(HoodieMORIncrementalFileSplit split, JobConf job) {
    this.basePath = split.getBasePath();
    this.jobConf = job;
    LOG.info("cfg ==> " + job.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR));
    LOG.info("columnIds ==> " + job.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
    LOG.info("partitioningColumns ==> " + job.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, ""));
    try {
      this.usesCustomPayload = usesCustomPayload();
      LOG.info("usesCustomPayload ==> " + this.usesCustomPayload);
      String logMessage = "About to read compacted logs for fileGroupId: "
          + split.getFileGroupId().toString() + ", projecting cols %s";
      String latestBaseFilePath = split.getLatestBaseFilePath();
      init(split.getLatestLogFilePaths(), logMessage, latestBaseFilePath != null ? Option.of(new Path(latestBaseFilePath)) : Option.empty());
    } catch (IOException e) {
      throw new HoodieIOException("Could not create HoodieMORIncrementalRecordReader on file group Id " + split.getFileGroupId().toString(), e);
    }
  }

  private boolean usesCustomPayload() {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jobConf, basePath);
    return !(metaClient.getTableConfig().getPayloadClass().contains(HoodieAvroPayload.class.getName())
        || metaClient.getTableConfig().getPayloadClass().contains("org.apache.hudi.OverwriteWithLatestAvroPayload"));
  }

  /**
   * Goes through the log files in reverse order and finds the schema from the last available data block. If not, falls
   * back to the schema from the latest parquet file. Finally, sets the partition column and projection fields into the
   * job conf.
   */
  private void init(List<String> deltaLogPaths, String logMessage, Option<Path> splitPath) throws IOException {
    Schema schemaFromLogFile = readLatestSchemaFromLogFiles(basePath, deltaLogPaths, jobConf);
    if (schemaFromLogFile == null && splitPath.isPresent()) {
      writerSchema = readSchema(jobConf, splitPath.get());
      LOG.debug("Writer Schema From Parquet => " + writerSchema.getFields());
    } else {
      writerSchema = schemaFromLogFile;
      LOG.debug("Writer Schema From Log => " + writerSchema.getFields());
    }
    // Add partitioning fields to writer schema for resulting row to contain null values for these fields
    String partitionFields = jobConf.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
    List<String> partitioningFields =
        partitionFields.length() > 0 ? Arrays.stream(partitionFields.split("/")).collect(Collectors.toList())
            : new ArrayList<>();
    writerSchema = addPartitionFields(writerSchema, partitioningFields);
    List<String> projectionFields = orderFields(jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR),
        jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR), partitioningFields);

    Map<String, Field> schemaFieldsMap = getNameToFieldMap(writerSchema);
    hiveSchema = constructHiveOrderedSchema(writerSchema, schemaFieldsMap);
    // TODO(vc): In the future, the reader schema should be updated based on log files & be able
    // to null out fields not present before

    readerSchema = generateProjectionSchema(writerSchema, schemaFieldsMap, projectionFields);
    LOG.info(String.format(logMessage, projectionFields));
  }

  private Schema constructHiveOrderedSchema(Schema writerSchema, Map<String, Field> schemaFieldsMap) {
    // Get all column names of hive table
    String hiveColumnString = jobConf.get(hive_metastoreConstants.META_TABLE_COLUMNS);
    LOG.info("Hive Columns : " + hiveColumnString);
    String[] hiveColumns = hiveColumnString.split(",");
    LOG.info("Hive Columns : " + hiveColumnString);
    List<Field> hiveSchemaFields = new ArrayList<>();

    for (String columnName : hiveColumns) {
      Field field = schemaFieldsMap.get(columnName.toLowerCase());

      if (field != null) {
        hiveSchemaFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()));
      } else {
        // Hive has some extra virtual columns like BLOCK__OFFSET__INSIDE__FILE which do not exist in table schema.
        // They will get skipped as they won't be found in the original schema.
        LOG.debug("Skipping Hive Column => " + columnName);
      }
    }

    Schema hiveSchema = Schema.createRecord(writerSchema.getName(), writerSchema.getDoc(), writerSchema.getNamespace(),
        writerSchema.isError());
    hiveSchema.setFields(hiveSchemaFields);
    LOG.info("HIVE Schema is :" + hiveSchema.toString(true));
    return hiveSchema;
  }

  public Schema getReaderSchema() {
    return readerSchema;
  }

  public Schema getWriterSchema() {
    return writerSchema;
  }

  public Schema getHiveSchema() {
    return hiveSchema;
  }

  public long getMaxCompactionMemoryInBytes() {
    // jobConf.getMemoryForMapTask() returns in MB
    return (long) Math
        .ceil(Double.parseDouble(jobConf.get(HoodieRealtimeConfig.COMPACTION_MEMORY_FRACTION_PROP,
            HoodieRealtimeConfig.DEFAULT_COMPACTION_MEMORY_FRACTION))
            * jobConf.getMemoryForMapTask() * 1024 * 1024L);
  }
}
