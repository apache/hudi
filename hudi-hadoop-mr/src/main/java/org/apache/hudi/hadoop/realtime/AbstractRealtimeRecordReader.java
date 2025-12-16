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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.HoodieColumnProjectionUtils;
import org.apache.hudi.hadoop.SchemaEvolutionContext;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.utils.HiveAvroSerializer;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.avro.HoodieAvroUtils.createNewSchemaField;
import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;

/**
 * Record Reader implementation to merge fresh avro data with base parquet data, to support real time queries.
 */
public abstract class AbstractRealtimeRecordReader {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRealtimeRecordReader.class);

  protected final RealtimeSplit split;
  protected final JobConf jobConf;
  protected final boolean usesCustomPayload;
  protected TypedProperties payloadProps = new TypedProperties();
  // Schema handles
  private Schema readerSchema;
  private Schema writerSchema;
  private Schema hiveSchema;
  private final HoodieTableMetaClient metaClient;
  protected SchemaEvolutionContext schemaEvolutionContext;
  // support merge operation
  protected boolean supportPayload;
  // handle hive type to avro record
  protected HiveAvroSerializer serializer;
  private final boolean supportTimestamp;

  public AbstractRealtimeRecordReader(RealtimeSplit split, JobConf job) {
    this.split = split;
    this.jobConf = job;
    LOG.info("cfg ==> " + job.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR));
    LOG.info("columnIds ==> " + job.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
    LOG.info("partitioningColumns ==> " + job.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, ""));
    this.supportPayload = Boolean.parseBoolean(job.get("hoodie.support.payload", "true"));
    try {
      metaClient = HoodieTableMetaClient.builder()
          .setConf(HadoopFSUtils.getStorageConfWithCopy(jobConf)).setBasePath(split.getBasePath()).build();
      payloadProps.putAll(metaClient.getTableConfig().getProps(true));
      if (metaClient.getTableConfig().getOrderingFieldsStr().isPresent()) {
        this.payloadProps.setProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, metaClient.getTableConfig().getOrderingFieldsStr().orElse(null));
      }
      this.usesCustomPayload = usesCustomPayload(metaClient);
      LOG.info("usesCustomPayload ==> " + this.usesCustomPayload);

      // get timestamp columns
      supportTimestamp = HoodieColumnProjectionUtils.supportTimestamp(jobConf);

      schemaEvolutionContext = new SchemaEvolutionContext(split, job, Option.of(metaClient));
      if (schemaEvolutionContext.internalSchemaOption.isPresent()) {
        schemaEvolutionContext.doEvolutionForRealtimeInputFormat(this);
      } else {
        init();
      }
    } catch (Exception e) {
      throw new HoodieException("Could not create HoodieRealtimeRecordReader on path " + this.split.getPath(), e);
    }
    prepareHiveAvroSerializer();
  }

  private boolean usesCustomPayload(HoodieTableMetaClient metaClient) {
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    if (tableConfig.contains(HoodieTableConfig.RECORD_MERGE_MODE)
        && tableConfig.contains(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID)) {
      return tableConfig.getRecordMergeMode().equals(RecordMergeMode.CUSTOM)
          && tableConfig.getRecordMergeStrategyId().equals(HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID);
    }
    return false;
  }

  private void prepareHiveAvroSerializer() {
    try {
      // hive will append virtual columns at the end of column list. we should remove those columns.
      // eg: current table is col1, col2, col3; jobConf.get(serdeConstants.LIST_COLUMNS): col1, col2, col3 ,BLOCK__OFFSET__INSIDE__FILE ...
      Set<String> writerSchemaColNames = writerSchema.getFields().stream().map(f -> f.name().toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
      List<String> columnNameList = Arrays.stream(jobConf.get(serdeConstants.LIST_COLUMNS).split(",")).collect(Collectors.toList());
      List<TypeInfo> columnTypeList = TypeInfoUtils.getTypeInfosFromTypeString(jobConf.get(serdeConstants.LIST_COLUMN_TYPES));
      int columnNameListLen = columnNameList.size() - 1;
      for (int i = columnNameListLen; i >= 0; i--) {
        String lastColName = columnNameList.get(columnNameList.size() - 1);
        // virtual columns will only append at the end of column list. it will be ok to break the loop.
        if (writerSchemaColNames.contains(lastColName)) {
          break;
        }
        LOG.debug("remove virtual column: {}", lastColName);
        columnNameList.remove(columnNameList.size() - 1);
        columnTypeList.remove(columnTypeList.size() - 1);
      }
      StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNameList, columnTypeList);
      this.serializer = new HiveAvroSerializer(new ArrayWritableObjectInspector(rowTypeInfo), columnNameList, columnTypeList);
    } catch (Exception e) {
      // fallback to origin logical
      LOG.warn("Failed to init HiveAvroSerializer to support payload merge. Fallback to origin logical", e);
      this.supportPayload = false;
    }
  }

  /**
   * Gets schema from HoodieTableMetaClient. If not, falls
   * back to the schema from the latest parquet file. Finally, sets the partition column and projection fields into the
   * job conf.
   */
  private void init() throws Exception {
    LOG.info("Getting writer schema from table avro schema ");
    writerSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();

    // Add partitioning fields to writer schema for resulting row to contain null values for these fields
    String partitionFields = jobConf.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
    List<String> partitioningFields =
        partitionFields.length() > 0 ? Arrays.stream(partitionFields.split("/")).collect(Collectors.toList())
            : new ArrayList<>();
    writerSchema = HoodieRealtimeRecordReaderUtils.addPartitionFields(writerSchema, partitioningFields);
    List<String> projectionFields = HoodieRealtimeRecordReaderUtils.orderFields(jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, EMPTY_STRING),
        jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, EMPTY_STRING), partitioningFields);

    Map<String, Field> schemaFieldsMap = HoodieRealtimeRecordReaderUtils.getNameToFieldMap(writerSchema);
    hiveSchema = constructHiveOrderedSchema(writerSchema, schemaFieldsMap, jobConf.get(hive_metastoreConstants.META_TABLE_COLUMNS, EMPTY_STRING));
    // TODO(vc): In the future, the reader schema should be updated based on log files & be able
    // to null out fields not present before

    readerSchema = HoodieRealtimeRecordReaderUtils.generateProjectionSchema(writerSchema, schemaFieldsMap, projectionFields);
    LOG.info(String.format("About to read compacted logs %s for base split %s, projecting cols %s",
        split.getDeltaLogPaths(), split.getPath(), projectionFields));
  }

  public Schema constructHiveOrderedSchema(Schema writerSchema, Map<String, Field> schemaFieldsMap, String hiveColumnString) {
    String[] hiveColumns = hiveColumnString.isEmpty() ? new String[0] : hiveColumnString.split(",");
    LOG.info("Hive Columns : " + hiveColumnString);
    List<Field> hiveSchemaFields = new ArrayList<>();

    for (String columnName : hiveColumns) {
      Field field = schemaFieldsMap.get(columnName.toLowerCase());

      if (field != null) {
        hiveSchemaFields.add(createNewSchemaField(field));
      } else {
        // Hive has some extra virtual columns like BLOCK__OFFSET__INSIDE__FILE which do not exist in table schema.
        // They will get skipped as they won't be found in the original schema.
        LOG.debug("Skipping Hive Column => {}", columnName);
      }
    }

    Schema hiveSchema = Schema.createRecord(writerSchema.getName(), writerSchema.getDoc(), writerSchema.getNamespace(),
        writerSchema.isError());
    hiveSchema.setFields(hiveSchemaFields);
    LOG.debug("HIVE Schema is :{}", hiveSchema);
    return hiveSchema;
  }

  protected Schema getLogScannerReaderSchema() {
    return usesCustomPayload ? writerSchema : readerSchema;
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

  public boolean isSupportTimestamp() {
    return supportTimestamp;
  }

  public RealtimeSplit getSplit() {
    return split;
  }

  public JobConf getJobConf() {
    return jobConf;
  }

  public void setReaderSchema(Schema readerSchema) {
    this.readerSchema = readerSchema;
  }

  public void setWriterSchema(Schema writerSchema) {
    this.writerSchema = writerSchema;
  }

  public void setHiveSchema(Schema hiveSchema) {
    this.hiveSchema = hiveSchema;
  }
}
