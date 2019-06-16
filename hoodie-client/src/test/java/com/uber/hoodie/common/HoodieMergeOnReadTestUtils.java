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

package com.uber.hoodie.common;

import static com.uber.hoodie.common.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;

import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Utility methods to aid in testing MergeOnRead (workaround for HoodieReadClient for MOR)
 */
public class HoodieMergeOnReadTestUtils {

  public static List<GenericRecord> getRecordsUsingInputFormat(List<String> inputPaths, String basePath)
      throws IOException {
    JobConf jobConf = new JobConf();
    Schema schema = HoodieAvroUtils.addMetadataFields(Schema.parse(TRIP_EXAMPLE_SCHEMA));
    HoodieRealtimeInputFormat inputFormat = new HoodieRealtimeInputFormat();
    setPropsForInputFormat(inputFormat, jobConf, schema, basePath);
    return inputPaths.stream().map(path -> {
      setInputPath(jobConf, path);
      List<GenericRecord> records = new ArrayList<>();
      try {
        List<InputSplit> splits = Arrays.asList(inputFormat.getSplits(jobConf, 1));
        RecordReader recordReader = inputFormat.getRecordReader(splits.get(0), jobConf, null);
        Void key = (Void) recordReader.createKey();
        ArrayWritable writable = (ArrayWritable) recordReader.createValue();
        while (recordReader.next(key, writable)) {
          GenericRecordBuilder newRecord = new GenericRecordBuilder(schema);
          // writable returns an array with [field1, field2, _hoodie_commit_time,
          // _hoodie_commit_seqno]
          Writable[] values = writable.get();
          schema.getFields().forEach(field -> {
            newRecord.set(field, values[2]);
          });
          records.add(newRecord.build());
        }
      } catch (IOException ie) {
        ie.printStackTrace();
      }
      return records;
    }).reduce((a, b) -> {
      a.addAll(b);
      return a;
    }).get();
  }

  private static void setPropsForInputFormat(HoodieRealtimeInputFormat inputFormat, JobConf jobConf, Schema schema,
      String basePath) {
    List<Schema.Field> fields = schema.getFields();
    String names = fields.stream().map(f -> f.name().toString()).collect(Collectors.joining(","));
    String postions = fields.stream().map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));
    Configuration conf = HoodieTestUtils.getDefaultHadoopConf();
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, postions);
    jobConf.set("partition_columns", "datestr");
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, postions);
    conf.set("partition_columns", "datestr");
    inputFormat.setConf(conf);
    jobConf.addResource(conf);
  }

  private static void setInputPath(JobConf jobConf, String inputPath) {
    jobConf.set("mapreduce.input.fileinputformat.inputdir", inputPath);
    jobConf.set("mapreduce.input.fileinputformat.inputdir", inputPath);
    jobConf.set("map.input.dir", inputPath);
  }
}
