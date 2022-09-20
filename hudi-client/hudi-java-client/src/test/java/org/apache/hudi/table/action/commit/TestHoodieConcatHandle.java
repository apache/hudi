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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLegacyAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.testutils.HoodieJavaClientTestBase;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestTable.makeNewCommitTime;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieConcatHandle extends HoodieJavaClientTestBase {
  private static final Schema SCHEMA = getSchemaFromResource(TestJavaCopyOnWriteActionExecutor.class, "/exampleSchema.avsc");

  private HoodieWriteConfig.Builder makeHoodieClientConfigBuilder() {
    return makeHoodieClientConfigBuilder(SCHEMA.toString());
  }

  private HoodieWriteConfig.Builder makeHoodieClientConfigBuilder(String schema) {
    // Prepare the AvroParquetIO
    return HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withPath(basePath)
        .withSchema(schema);
  }

  private FileStatus[] getIncrementalFiles(String partitionPath, String startCommitTime, int numCommitsToPull)
      throws Exception {
    // initialize parquet input format
    HoodieParquetInputFormat hoodieInputFormat = new HoodieParquetInputFormat();
    JobConf jobConf = new JobConf(hadoopConf);
    hoodieInputFormat.setConf(jobConf);
    HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE);
    setupIncremental(jobConf, startCommitTime, numCommitsToPull);
    FileInputFormat.setInputPaths(jobConf, Paths.get(basePath, partitionPath).toString());
    return hoodieInputFormat.listStatus(jobConf);
  }

  private void setupIncremental(JobConf jobConf, String startCommit, int numberOfCommitsToPull) {
    String modePropertyName =
        String.format(HoodieHiveUtils.HOODIE_CONSUME_MODE_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(modePropertyName, HoodieHiveUtils.INCREMENTAL_SCAN_MODE);

    String startCommitTimestampName =
        String.format(HoodieHiveUtils.HOODIE_START_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(startCommitTimestampName, startCommit);

    String maxCommitPulls =
        String.format(HoodieHiveUtils.HOODIE_MAX_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setInt(maxCommitPulls, numberOfCommitsToPull);
  }

  @Test
  public void testInsert() throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder().withMergeAllowDuplicateOnInserts(true).build();

    HoodieJavaWriteClient writeClient = getHoodieWriteClient(config);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    BaseFileUtils fileUtils = BaseFileUtils.getInstance(metaClient);

    // Get some records belong to the same partition (2021/09/11)
    String insertRecordStr1 = "{\"_row_key\":\"1\","
        + "\"time\":\"2021-09-11T16:16:41.415Z\",\"number\":1}";
    String insertRecordStr2 = "{\"_row_key\":\"2\","
        + "\"time\":\"2021-09-11T16:16:41.415Z\",\"number\":2}";
    List<HoodieRecord> records1 = new ArrayList<>();
    RawTripTestPayload insertRow1 = new RawTripTestPayload(insertRecordStr1);
    RawTripTestPayload insertRow2 = new RawTripTestPayload(insertRecordStr2);
    records1.add(new HoodieLegacyAvroRecord(new HoodieKey(insertRow1.getRowKey(), insertRow1.getPartitionPath()), insertRow1));
    records1.add(new HoodieLegacyAvroRecord(new HoodieKey(insertRow2.getRowKey(), insertRow2.getPartitionPath()), insertRow2));

    int startInstant = 1;
    String firstCommitTime = makeNewCommitTime(startInstant++, "%09d");
    // First insert
    writeClient.startCommitWithTime(firstCommitTime);
    writeClient.insert(records1, firstCommitTime);

    String partitionPath = "2021/09/11";
    FileStatus[] allFiles = getIncrementalFiles(partitionPath, "0", -1);
    assertEquals(1, allFiles.length);

    // Read out the bloom filter and make sure filter can answer record exist or not
    Path filePath = allFiles[0].getPath();
    BloomFilter filter = fileUtils.readBloomFilterFromMetadata(hadoopConf, filePath);
    for (HoodieRecord record : records1) {
      assertTrue(filter.mightContain(record.getRecordKey()));
    }

    insertRecordStr1 = "{\"_row_key\":\"1\","
        + "\"time\":\"2021-09-11T16:39:41.415Z\",\"number\":3}";
    insertRecordStr2 = "{\"_row_key\":\"2\","
        + "\"time\":\"2021-09-11T16:39:41.415Z\",\"number\":4}";

    List<HoodieRecord> records2 = new ArrayList<>();
    insertRow1 = new RawTripTestPayload(insertRecordStr1);
    insertRow2 = new RawTripTestPayload(insertRecordStr2);
    // The recordKey of records2 and records1 are the same, but the values of other fields are different
    records2.add(new HoodieLegacyAvroRecord(new HoodieKey(insertRow1.getRowKey(), insertRow1.getPartitionPath()), insertRow1));
    records2.add(new HoodieLegacyAvroRecord(new HoodieKey(insertRow2.getRowKey(), insertRow2.getPartitionPath()), insertRow2));

    String newCommitTime = makeNewCommitTime(startInstant++, "%09d");
    writeClient.startCommitWithTime(newCommitTime);
    // Second insert is the same as the _row_key of the first one,test allowDuplicateInserts
    writeClient.insert(records2, newCommitTime);

    allFiles = getIncrementalFiles(partitionPath, firstCommitTime, -1);
    assertEquals(1, allFiles.length);
    // verify new incremental file group is same as the previous one
    assertEquals(FSUtils.getFileId(filePath.getName()), FSUtils.getFileId(allFiles[0].getPath().getName()));

    filePath = allFiles[0].getPath();
    // The final result should be a collection of records1 and records2
    records1.addAll(records2);

    // Read the base file, check the record content
    List<GenericRecord> fileRecords = fileUtils.readAvroRecords(hadoopConf, filePath);
    int index = 0;
    for (GenericRecord record : fileRecords) {
      assertEquals(records1.get(index).getRecordKey(), record.get("_row_key").toString());
      assertEquals(index + 1, record.get("number"));
      index++;
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testInsertWithDataGenerator(boolean mergeAllowDuplicateOnInsertsEnable) throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder(TRIP_EXAMPLE_SCHEMA)
        .withMergeAllowDuplicateOnInserts(mergeAllowDuplicateOnInsertsEnable).build();

    HoodieJavaWriteClient writeClient = getHoodieWriteClient(config);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    BaseFileUtils fileUtils = BaseFileUtils.getInstance(metaClient);

    String partitionPath = "2021/09/11";
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[]{partitionPath});

    int startInstant = 1;
    String firstCommitTime = makeNewCommitTime(startInstant++, "%09d");
    List<HoodieRecord> records1 = dataGenerator.generateInserts(firstCommitTime, 100);

    // First insert
    writeClient.startCommitWithTime(firstCommitTime);
    writeClient.insert(records1, firstCommitTime);

    FileStatus[] allFiles = getIncrementalFiles(partitionPath, "0", -1);
    assertEquals(1, allFiles.length);

    // Read out the bloom filter and make sure filter can answer record exist or not
    Path filePath = allFiles[0].getPath();
    BloomFilter filter = fileUtils.readBloomFilterFromMetadata(hadoopConf, filePath);
    for (HoodieRecord record : records1) {
      assertTrue(filter.mightContain(record.getRecordKey()));
    }

    String newCommitTime = makeNewCommitTime(startInstant++, "%09d");
    List<HoodieRecord> records2 = dataGenerator.generateUpdates(newCommitTime, 100);
    writeClient.startCommitWithTime(newCommitTime);
    // Second insert is the same as the _row_key of the first one,test allowDuplicateInserts
    writeClient.insert(records2, newCommitTime);

    allFiles = getIncrementalFiles(partitionPath, firstCommitTime, -1);
    assertEquals(1, allFiles.length);
    // verify new incremental file group is same as the previous one
    assertEquals(FSUtils.getFileId(filePath.getName()), FSUtils.getFileId(allFiles[0].getPath().getName()));

    filePath = allFiles[0].getPath();
    // If mergeAllowDuplicateOnInsertsEnable is true, the final result should be a collection of records1 and records2
    records1.addAll(records2);

    // Read the base file, check the record content
    List<GenericRecord> fileRecords = fileUtils.readAvroRecords(hadoopConf, filePath);
    assertEquals(fileRecords.size(), mergeAllowDuplicateOnInsertsEnable ? records1.size() : records2.size());

    int index = 0;
    for (GenericRecord record : fileRecords) {
      assertEquals(records1.get(index).getRecordKey(), record.get("_row_key").toString());
      index++;
    }
  }
}
