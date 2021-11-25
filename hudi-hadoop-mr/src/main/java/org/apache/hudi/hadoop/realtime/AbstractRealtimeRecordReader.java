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

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.LogReaderUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.InputSplitUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Record Reader implementation to merge fresh avro data with base parquet data, to support real time queries.
 */
public abstract class AbstractRealtimeRecordReader {
  private static final Logger LOG = LogManager.getLogger(AbstractRealtimeRecordReader.class);

  protected final RealtimeSplit split;
  protected final JobConf jobConf;
  protected final boolean usesCustomPayload;
  // Schema handles
  private Schema readerSchema;
  private Schema writerSchema;
  private Schema hiveSchema;

  public AbstractRealtimeRecordReader(RealtimeSplit split, JobConf job) {
    this.split = split;
    this.jobConf = job;
    LOG.info("cfg ==> " + job.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR));
    LOG.info("columnIds ==> " + job.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
    LOG.info("partitioningColumns ==> " + job.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, ""));
    try {
      this.usesCustomPayload = usesCustomPayload();
      LOG.info("usesCustomPayload ==> " + this.usesCustomPayload);
      init();
    } catch (IOException e) {
      throw new HoodieIOException("Could not create HoodieRealtimeRecordReader on path " + this.split.getPath(), e);
    }
  }

  private boolean usesCustomPayload() {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(jobConf).setBasePath(split.getBasePath()).build();
    return !(metaClient.getTableConfig().getPayloadClass().contains(HoodieAvroPayload.class.getName())
        || metaClient.getTableConfig().getPayloadClass().contains("org.apache.hudi.OverwriteWithLatestAvroPayload"));
  }

  /**
   * Goes through the log files in reverse order and finds the schema from the last available data block. If not, falls
   * back to the schema from the latest parquet file. Finally, sets the partition column and projection fields into the
   * job conf.
   */
  private void init() throws IOException {
    Schema schemaFromLogFile = LogReaderUtils.readLatestSchemaFromLogFiles(split.getBasePath(), split.getDeltaLogFiles(), jobConf);
    if (schemaFromLogFile == null) {
      writerSchema = InputSplitUtils.getBaseFileSchema((FileSplit)split, jobConf);
      LOG.info("Writer Schema From Parquet => " + writerSchema.getFields());
    } else {
      writerSchema = schemaFromLogFile;
      LOG.info("Writer Schema From Log => " + writerSchema.toString(true));
    }
    // Add partitioning fields to writer schema for resulting row to contain null values for these fields
    String partitionFields = jobConf.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
    List<String> partitioningFields =
        partitionFields.length() > 0 ? Arrays.stream(partitionFields.split("/")).collect(Collectors.toList())
            : new ArrayList<>();
    writerSchema = HoodieRealtimeRecordReaderUtils.addPartitionFields(writerSchema, partitioningFields);
    List<String> projectionFields = HoodieRealtimeRecordReaderUtils.orderFields(jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR),
        jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR), partitioningFields);

    Map<String, Field> schemaFieldsMap = HoodieRealtimeRecordReaderUtils.getNameToFieldMap(writerSchema);
    hiveSchema = constructHiveOrderedSchema(writerSchema, schemaFieldsMap);
    // TODO(vc): In the future, the reader schema should be updated based on log files & be able
    // to null out fields not present before

    readerSchema = HoodieRealtimeRecordReaderUtils.generateProjectionSchema(writerSchema, schemaFieldsMap, projectionFields);
    LOG.info(String.format("About to read compacted logs %s for base split %s, projecting cols %s",
        split.getDeltaLogPaths(), split.getPath(), projectionFields));
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
    LOG.debug("HIVE Schema is :" + hiveSchema.toString(true));
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

  public RealtimeSplit getSplit() {
    return split;
  }

  public JobConf getJobConf() {
    return jobConf;
  }
}
