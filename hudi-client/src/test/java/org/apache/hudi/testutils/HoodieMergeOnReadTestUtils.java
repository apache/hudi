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

import org.apache.avro.Schema.Field;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility methods to aid in testing MergeOnRead (workaround for HoodieReadClient for MOR).
 */
public class HoodieMergeOnReadTestUtils {

  public static List<GenericRecord> getRecordsUsingInputFormat(List<String> inputPaths, String basePath) {
    return getRecordsUsingInputFormat(inputPaths, basePath, new Configuration());
  }

  public static List<GenericRecord> getRecordsUsingInputFormat(List<String> inputPaths, String basePath,
      Configuration conf) {
    JobConf jobConf = new JobConf(conf);
    return getRecordsUsingInputFormat(inputPaths, basePath, jobConf, new HoodieParquetRealtimeInputFormat());
  }

  public static List<GenericRecord> getRecordsUsingInputFormat(List<String> inputPaths,
      String basePath,
      JobConf jobConf,
      HoodieParquetInputFormat inputFormat) {
    Schema schema = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    return getRecordsUsingInputFormat(inputPaths, basePath, jobConf, inputFormat, schema,
        HoodieTestDataGenerator.TRIP_HIVE_COLUMN_TYPES);
  }

  public static List<GenericRecord> getRecordsUsingInputFormat(List<String> inputPaths, String basePath,
      JobConf jobConf, HoodieParquetInputFormat inputFormat, Schema rawSchema, String rawHiveColumnTypes) {
    return getRecordsUsingInputFormat(inputPaths, basePath, jobConf, inputFormat, rawSchema, rawHiveColumnTypes,
        false, new ArrayList<>());
  }

  public static List<GenericRecord> getRecordsUsingInputFormat(List<String> inputPaths, String basePath,
        JobConf jobConf, HoodieParquetInputFormat inputFormat, Schema rawSchema, String rawHiveColumnTypes,
      boolean projectCols, List<String> projectedColumns) {
    Schema schema = HoodieAvroUtils.addMetadataFields(rawSchema);
    String hiveColumnTypes = HoodieAvroUtils.addMetadataColumnTypes(rawHiveColumnTypes);
    setPropsForInputFormat(inputFormat, jobConf, schema, hiveColumnTypes, projectCols, projectedColumns);
    final List<Field> fields;
    if (projectCols) {
      fields = schema.getFields().stream().filter(f -> projectedColumns.contains(f.name()))
          .collect(Collectors.toList());
    } else {
      fields = schema.getFields();
    }
    final Schema projectedSchema = Schema.createRecord(fields.stream()
        .map(f -> new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal()))
        .collect(Collectors.toList()));
    return inputPaths.stream().map(path -> {
      setInputPath(jobConf, path);
      List<GenericRecord> records = new ArrayList<>();
      try {
        List<InputSplit> splits = Arrays.asList(inputFormat.getSplits(jobConf, 1));
        for (InputSplit split : splits) {
          RecordReader recordReader = inputFormat.getRecordReader(split, jobConf, null);
          Object key = recordReader.createKey();
          ArrayWritable writable = (ArrayWritable) recordReader.createValue();
          while (recordReader.next(key, writable)) {
            GenericRecordBuilder newRecord = new GenericRecordBuilder(projectedSchema);
            // writable returns an array with [field1, field2, _hoodie_commit_time,
            // _hoodie_commit_seqno]
            Writable[] values = writable.get();
            assert projectedSchema.getFields().size() <= values.length;
            schema.getFields().stream()
                .filter(f -> !projectCols || projectedColumns.contains(f.name()))
                .map(f -> Pair.of(projectedSchema.getFields().stream()
                        .filter(p -> f.name().equals(p.name())).findFirst().get(), f))
                .forEach(fieldsPair -> newRecord.set(fieldsPair.getKey(), values[fieldsPair.getValue().pos()]));
            records.add(newRecord.build());
          }
        }
      } catch (IOException ie) {
        ie.printStackTrace();
      }
      return records;
    }).reduce((a, b) -> {
      a.addAll(b);
      return a;
    }).orElse(new ArrayList<GenericRecord>());
  }

  private static void setPropsForInputFormat(HoodieParquetInputFormat inputFormat, JobConf jobConf,
      Schema schema, String hiveColumnTyps, boolean projectCols, List<String> projectedCols) {
    List<Schema.Field> fields = schema.getFields();
    final List<String> projectedColNames;
    if (!projectCols) {
      projectedColNames = fields.stream().map(f -> f.name().toString()).collect(Collectors.toList());
    } else {
      projectedColNames = projectedCols;
    }

    String names = fields.stream()
        .filter(f -> projectedColNames.contains(f.name().toString()))
        .map(f -> f.name().toString()).collect(Collectors.joining(","));
    String postions = fields.stream()
        .filter(f -> projectedColNames.contains(f.name().toString()))
        .map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));
    String hiveColumnNames = fields.stream()
        .filter(field -> !field.name().equalsIgnoreCase("datestr"))
        .map(Schema.Field::name).collect(Collectors.joining(","));
    hiveColumnNames = hiveColumnNames + ",datestr";

    Configuration conf = HoodieTestUtils.getDefaultHadoopConf();
    String hiveColumnTypes = hiveColumnTyps;
    hiveColumnTypes = hiveColumnTypes + ",string";
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, hiveColumnTypes);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, postions);
    jobConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "datestr");
    conf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, postions);
    conf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "datestr");
    conf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, hiveColumnTypes);
    conf.set(IOConstants.COLUMNS, hiveColumnNames);
    conf.get(IOConstants.COLUMNS_TYPES, hiveColumnTypes);
    inputFormat.setConf(conf);
    jobConf.addResource(conf);
  }

  private static void setInputPath(JobConf jobConf, String inputPath) {
    jobConf.set("mapreduce.input.fileinputformat.inputdir", inputPath);
    jobConf.set("mapreduce.input.fileinputformat.inputdir", inputPath);
    jobConf.set("map.input.dir", inputPath);
  }
}
