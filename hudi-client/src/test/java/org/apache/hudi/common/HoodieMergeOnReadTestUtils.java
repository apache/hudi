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

package org.apache.hudi.common;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.hadoop.InputFormatTestUtil;
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
    Schema schema = HoodieAvroUtils.addMetadataFields(
        new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA));
    HoodieParquetRealtimeInputFormat inputFormat = new HoodieParquetRealtimeInputFormat();
    InputFormatTestUtil.setPropsForInputFormat(jobConf, schema, HoodieTestDataGenerator.TRIP_HIVE_COLUMN_TYPES);
    return inputPaths.stream().map(path -> {
      InputFormatTestUtil.setInputPath(jobConf, path);
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
          final int[] fieldIndex = {0};
          assert schema.getFields().size() <= values.length;
          schema.getFields().forEach(field -> {
            newRecord.set(field, values[fieldIndex[0]++]);
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
    }).orElse(new ArrayList<GenericRecord>());
  }

}
