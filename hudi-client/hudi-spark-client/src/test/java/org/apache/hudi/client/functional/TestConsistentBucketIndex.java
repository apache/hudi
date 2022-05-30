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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.RealtimeFileStatus;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test consistent hashing index
 */
@Tag("functional")
public class TestConsistentBucketIndex extends HoodieClientTestHarness {

  private final Random random = new Random(1);
  private HoodieIndex index;
  private HoodieWriteConfig config;

  private static Stream<Arguments> configParams() {
    // preserveMetaField, partitioned
    Object[][] data = new Object[][] {
        {true, false},
        {false, false},
        {true, true},
        {false, true},
    };
    return Stream.of(data).map(Arguments::of);
  }

  private void setUp(boolean populateMetaFields, boolean partitioned) throws Exception {
    initPath();
    initSparkContexts();
    if (partitioned) {
      initTestDataGenerator();
    } else {
      initTestDataGenerator(new String[] {""});
    }
    initFileSystem();
    Properties props = populateMetaFields ? new Properties() : getPropertiesForKeyGen();
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ, props);
    config = getConfigBuilder()
        .withProperties(props)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withIndexKeyField("_row_key")
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING)
            .build())
        .withAutoCommit(false)
        .build();
    writeClient = getHoodieWriteClient(config);
    index = writeClient.getIndex();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  /**
   * Test bucket index tagging (always tag regardless of the write status)
   * Test bucket index tagging consistency, two tagging result should be same
   *
   * @param populateMetaFields
   * @param partitioned
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("configParams")
  public void testTagLocation(boolean populateMetaFields, boolean partitioned) throws Exception {
    setUp(populateMetaFields, partitioned);
    String newCommitTime = "001";
    int totalRecords = 20 + random.nextInt(20);
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, totalRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 2);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

    // The records should be tagged anyway, even though it is the first time doing tagging
    List<HoodieRecord> taggedRecord = tagLocation(index, writeRecords, hoodieTable).collect();
    Assertions.assertTrue(taggedRecord.stream().allMatch(r -> r.isCurrentLocationKnown()));

    // Tag again, the records should get the same location (hashing metadata has been persisted after the first tagging)
    List<HoodieRecord> taggedRecord2 = tagLocation(index, writeRecords, hoodieTable).collect();
    for (HoodieRecord ref : taggedRecord) {
      for (HoodieRecord record : taggedRecord2) {
        if (ref.getRecordKey().equals(record.getRecordKey())) {
          Assertions.assertEquals(ref.getCurrentLocation(), record.getCurrentLocation());
          break;
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("configParams")
  public void testWriteData(boolean populateMetaFields, boolean partitioned) throws Exception {
    setUp(populateMetaFields, partitioned);
    String newCommitTime = "001";
    int totalRecords = 20 + random.nextInt(20);
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, totalRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 2);

    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Insert totalRecords records
    writeClient.startCommitWithTime(newCommitTime);
    List<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime).collect();
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatues);
    boolean success = writeClient.commitStats(newCommitTime, writeStatues.stream()
        .map(WriteStatus::getStat)
        .collect(Collectors.toList()), Option.empty(), metaClient.getCommitActionType());
    Assertions.assertTrue(success);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    // The number of distinct fileId should be the same as total log file numbers
    Assertions.assertEquals(writeStatues.stream().map(WriteStatus::getFileId).distinct().count(),
        Arrays.stream(dataGen.getPartitionPaths()).mapToInt(p -> Objects.requireNonNull(listStatus(p, true)).length).sum());
    Assertions.assertEquals(totalRecords, readRecords(dataGen.getPartitionPaths(), populateMetaFields).size());

    // Upsert the same set of records, the number of records should be same
    newCommitTime = "002";
    writeClient.startCommitWithTime(newCommitTime);
    writeStatues = writeClient.upsert(writeRecords, newCommitTime).collect();
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatues);
    success = writeClient.commitStats(newCommitTime, writeStatues.stream()
        .map(WriteStatus::getStat)
        .collect(Collectors.toList()), Option.empty(), metaClient.getCommitActionType());
    Assertions.assertTrue(success);
    // The number of log file should double after this insertion
    long numberOfLogFiles = Arrays.stream(dataGen.getPartitionPaths())
        .mapToInt(p -> {
          return Arrays.stream(listStatus(p, true)).mapToInt(fs ->
              fs instanceof RealtimeFileStatus ? ((RealtimeFileStatus) fs).getDeltaLogFiles().size() : 1).sum();
        }).sum();
    Assertions.assertEquals(writeStatues.stream().map(WriteStatus::getFileId).distinct().count() * 2, numberOfLogFiles);
    // The record number should remain same because of deduplication
    Assertions.assertEquals(totalRecords, readRecords(dataGen.getPartitionPaths(), populateMetaFields).size());

    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Upsert new set of records, and validate the total number of records
    newCommitTime = "003";
    records = dataGen.generateInserts(newCommitTime, totalRecords);
    writeRecords = jsc.parallelize(records, 2);
    writeClient.startCommitWithTime(newCommitTime);
    writeStatues = writeClient.upsert(writeRecords, newCommitTime).collect();
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatues);
    success = writeClient.commitStats(newCommitTime, writeStatues.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());
    Assertions.assertTrue(success);
    Assertions.assertEquals(totalRecords * 2, readRecords(dataGen.getPartitionPaths(), populateMetaFields).size());
  }

  private List<GenericRecord> readRecords(String[] partitions, boolean populateMetaFields) {
    return HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf,
        Arrays.stream(partitions).map(p -> Paths.get(basePath, p).toString()).collect(Collectors.toList()),
        basePath, new JobConf(hadoopConf), true, populateMetaFields);
  }

  private FileStatus[] listStatus(String p, boolean realtime) {
    JobConf jobConf = new JobConf(hadoopConf);
    FileInputFormat.setInputPaths(jobConf, Paths.get(basePath, p).toString());
    FileInputFormat format = HoodieInputFormatUtils.getInputFormat(HoodieFileFormat.PARQUET, realtime, jobConf);
    try {
      if (realtime) {
        return ((HoodieParquetRealtimeInputFormat) format).listStatus(jobConf);
      } else {
        return ((HoodieParquetInputFormat) format).listStatus(jobConf);
      }
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  private HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build());
  }
}
