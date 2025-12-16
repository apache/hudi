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

package org.apache.hudi.testutils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HadoopFSTestUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.storage.StorageConfiguration;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility methods to aid in testing MergeOnRead (workaround for HoodieReadClient for MOR).
 */
@Slf4j
public class HoodieMergeOnReadTestUtils {

  public static List<GenericRecord> getRecordsUsingInputFormat(StorageConfiguration<?> conf, List<String> inputPaths,
                                                               String basePath) {
    return getRecordsUsingInputFormat(conf, inputPaths, basePath, HadoopFSTestUtils.convertToJobConf(conf), true);
  }

  public static List<GenericRecord> getRecordsUsingInputFormat(StorageConfiguration<?> conf, List<String> inputPaths,
                                                               String basePath, JobConf jobConf, boolean realtime) {
    return getRecordsUsingInputFormat(conf, inputPaths, basePath, jobConf, realtime, true);
  }

  public static List<GenericRecord> getRecordsUsingInputFormat(StorageConfiguration<?> conf, List<String> inputPaths,
                                                               String basePath, JobConf jobConf, boolean realtime, boolean populateMetaFields) {
    Schema schema = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    return getRecordsUsingInputFormat(conf, inputPaths, basePath, jobConf, realtime, schema,
        HoodieTestDataGenerator.TRIP_HIVE_COLUMN_TYPES, false, new ArrayList<>(), populateMetaFields);
  }

  public static List<GenericRecord> getRecordsUsingInputFormat(StorageConfiguration<?> conf, List<String> inputPaths, String basePath, JobConf jobConf, boolean realtime, Schema rawSchema,
                                                               String rawHiveColumnTypes, boolean projectCols, List<String> projectedColumns) {
    return getRecordsUsingInputFormat(conf, inputPaths, basePath, jobConf, realtime, rawSchema, rawHiveColumnTypes, projectCols, projectedColumns, true);
  }

  public static List<GenericRecord> getRecordsUsingInputFormat(StorageConfiguration<?> conf, List<String> inputPaths, String basePath, JobConf jobConf, boolean realtime, Schema rawSchema,
                                                               String rawHiveColumnTypes, boolean projectCols, List<String> projectedColumns, boolean populateMetaFields) {

    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(conf, basePath);
    FileInputFormat inputFormat = HoodieInputFormatUtils.getInputFormat(metaClient.getTableConfig().getBaseFileFormat(), realtime, jobConf);

    Schema schema;
    String hiveColumnTypes;

    if (populateMetaFields) {
      schema = HoodieAvroUtils.addMetadataFields(rawSchema);
      hiveColumnTypes = HoodieAvroUtils.addMetadataColumnTypes(rawHiveColumnTypes);
    } else {
      schema = rawSchema;
      hiveColumnTypes = rawHiveColumnTypes;
    }

    setPropsForInputFormat(inputFormat, jobConf, schema, hiveColumnTypes, projectCols, projectedColumns, populateMetaFields);
    final List<Field> fields;
    if (projectCols) {
      fields = schema.getFields().stream().filter(f -> projectedColumns.contains(f.name()))
          .collect(Collectors.toList());
    } else {
      fields = schema.getFields();
    }
    final Schema projectedSchema = Schema.createRecord(fields.stream()
        .map(HoodieAvroUtils::createNewSchemaField)
        .collect(Collectors.toList()));

    List<GenericRecord> records = new ArrayList<>();
    try {
      FileInputFormat.setInputPaths(jobConf, String.join(",", inputPaths));
      InputSplit[] splits = inputFormat.getSplits(jobConf, inputPaths.size());

      for (InputSplit split : splits) {
        RecordReader recordReader = inputFormat.getRecordReader(split, jobConf, null);
        Object key = recordReader.createKey();
        ArrayWritable writable = (ArrayWritable) recordReader.createValue();
        while (recordReader.next(key, writable)) {
          GenericRecordBuilder newRecord = new GenericRecordBuilder(projectedSchema);
          // writable returns an array with [field1, field2, _hoodie_commit_time,
          // _hoodie_commit_seqno]
          Writable[] values = writable.get();
          schema.getFields().stream()
              .filter(f -> !projectCols || projectedColumns.contains(f.name()))
              .map(f -> Pair.of(projectedSchema.getFields().stream()
                  .filter(p -> f.name().equals(p.name())).findFirst().get(), f))
              .forEach(fieldsPair -> newRecord.set(fieldsPair.getKey(), values[fieldsPair.getValue().pos()]));
          records.add(newRecord.build());
        }
        recordReader.close();
      }
    } catch (IOException ie) {
      log.error("Read records error", ie);
    }
    return records;
  }

  private static void setPropsForInputFormat(FileInputFormat inputFormat, JobConf jobConf, Schema schema, String hiveColumnTypes, boolean projectCols, List<String> projectedCols,
                                             boolean populateMetaFieldsConfigValue) {
    List<Field> fields = schema.getFields();
    final List<String> projectedColNames;
    if (!projectCols) {
      projectedColNames = fields.stream().map(Field::name).collect(Collectors.toList());
    } else {
      projectedColNames = projectedCols;
    }

    String names = fields.stream()
        .filter(f -> projectedColNames.contains(f.name()))
        .map(f -> f.name()).collect(Collectors.joining(","));
    String positions = fields.stream()
        .filter(f -> projectedColNames.contains(f.name()))
        .map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));
    String hiveColumnNames = fields.stream()
        .filter(field -> !field.name().equalsIgnoreCase("datestr"))
        .map(Field::name).collect(Collectors.joining(","));
    hiveColumnNames = hiveColumnNames + ",datestr";

    StorageConfiguration<?> conf = HoodieTestUtils.getDefaultStorageConf();
    String hiveColumnTypesWithDatestr = hiveColumnTypes + ",string";
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, hiveColumnTypesWithDatestr);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, positions);
    jobConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "datestr");
    conf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, positions);
    conf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "datestr");
    conf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, hiveColumnTypesWithDatestr);
    conf.set(IOConstants.COLUMNS, hiveColumnNames);
    conf.set(IOConstants.COLUMNS_TYPES, hiveColumnTypesWithDatestr);

    // Hoodie Input formats are also configurable
    HadoopFSTestUtils.setConfForConfigurableInputFormat(inputFormat, HadoopFSTestUtils.convertToHadoopConf(conf));
    jobConf.addResource(HadoopFSTestUtils.convertToHadoopConf(conf));
  }
}
