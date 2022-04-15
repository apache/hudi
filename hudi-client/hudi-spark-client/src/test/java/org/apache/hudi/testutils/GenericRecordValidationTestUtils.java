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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieRecord.COMMIT_SEQNO_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.FILENAME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.OPERATION_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.hadoop.utils.HoodieHiveUtils.HOODIE_CONSUME_COMMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GenericRecordValidationTestUtils {

  public static void assertGenericRecords(GenericRecord record1, GenericRecord record2,
                                          Schema schema, List<String> excludeFields) {
    for (Schema.Field f: schema.getFields()) {
      String fieldName = f.name();
      if (excludeFields.contains(fieldName)) {
        continue;
      }
      Object value1 = record1.get(fieldName);
      Object value2 = record2.get(fieldName);
      if (value1 != null && value2 != null) {
        if (value1 instanceof ArrayWritable) {
          assertEquals(HoodieRealtimeRecordReaderUtils.arrayWritableToString((ArrayWritable) value1),
              HoodieRealtimeRecordReaderUtils.arrayWritableToString((ArrayWritable) value2));
        } else {
          assertEquals(value1, value2, "Field name " + fieldName + " is not same."
              + " Val1: " + value1 + ", Val2:" + value2);
        }
      } else if (value1 != null || value2 != null) {
        throw new HoodieValidationException("Field name " + fieldName + " is not same."
            + " Val1: " + value1 + ", Val2:" + value2);
      }
    }
  }

  public static void assertDataInMORTable(HoodieWriteConfig config, String instant1, String instant2,
                                          Configuration hadoopConf, List<String> partitionPaths) {
    List<String> excludeFields = CollectionUtils.createImmutableList(COMMIT_TIME_METADATA_FIELD, COMMIT_SEQNO_METADATA_FIELD,
        FILENAME_METADATA_FIELD, OPERATION_METADATA_FIELD);
    assertDataInMORTable(config, instant1, instant2, hadoopConf, partitionPaths, excludeFields);
  }

  public static void assertDataInMORTable(HoodieWriteConfig config, String instant1, String instant2,
                                          Configuration hadoopConf, List<String> partitionPaths, List<String> excludeFields) {
    JobConf jobConf = new JobConf(hadoopConf);
    List<String> fullPartitionPaths = partitionPaths.stream()
        .map(partitionPath -> Paths.get(config.getBasePath(), partitionPath).toString())
        .collect(Collectors.toList());

    jobConf.set(String.format(HOODIE_CONSUME_COMMIT, config.getTableName()), instant1);
    List<GenericRecord> records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        hadoopConf, fullPartitionPaths, config.getBasePath(), jobConf, true);
    Map<String, GenericRecord> prevRecordsMap = records.stream()
        .collect(Collectors.toMap(rec -> rec.get(RECORD_KEY_METADATA_FIELD).toString(), Function.identity()));

    jobConf.set(String.format(HOODIE_CONSUME_COMMIT, config.getTableName()), instant2);
    List<GenericRecord> records1 = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        hadoopConf, fullPartitionPaths, config.getBasePath(), jobConf, true);
    Map<String, GenericRecord> newRecordsMap = records1.stream()
        .collect(Collectors.toMap(rec -> rec.get(RECORD_KEY_METADATA_FIELD).toString(), Function.identity()));

    // Verify row count.
    assertEquals(prevRecordsMap.size(), newRecordsMap.size());

    Schema readerSchema = HoodieAvroUtils.addMetadataFields(
        new Schema.Parser().parse(config.getSchema()), config.allowOperationMetadataField());

    // Verify every field.
    prevRecordsMap.forEach((key, value) -> {
      assertTrue(newRecordsMap.containsKey(key));
      assertGenericRecords(value, newRecordsMap.get(key), readerSchema, excludeFields);
    });
  }

  public static Map<String, GenericRecord> getRecordsMap(HoodieWriteConfig config, Configuration hadoopConf,
                                                         HoodieTestDataGenerator dataGen) {
    JobConf jobConf = new JobConf(hadoopConf);
    List<String> fullPartitionPaths = Arrays.stream(dataGen.getPartitionPaths())
        .map(partitionPath -> Paths.get(config.getBasePath(), partitionPath).toString())
        .collect(Collectors.toList());
    return HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        hadoopConf, fullPartitionPaths, config.getBasePath(), jobConf, true).stream()
        .collect(Collectors.toMap(rec -> rec.get(RECORD_KEY_METADATA_FIELD).toString(), Function.identity()));
  }

}
