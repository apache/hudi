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

package org.apache.hudi.io;

import org.apache.hudi.HoodieClientTestHarness;
import org.apache.hudi.HoodieWriteClient;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieMemoryConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bloom.HoodieBloomIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHoodieCompactor extends HoodieClientTestHarness {

  private Configuration hadoopConf;
  private HoodieTableMetaClient metaClient;

  @Before
  public void setUp() throws Exception {
    // Initialize a local spark env
    initSparkContexts("TestHoodieCompactor");

    // Create a temp folder as the base path
    initPath();
    hadoopConf = HoodieTestUtils.getDefaultHadoopConf();
    fs = FSUtils.getFs(basePath, hadoopConf);
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ);
    initTestDataGenerator();
  }

  @After
  public void tearDown() throws Exception {
    cleanupFileSystem();
    cleanupTestDataGenerator();
    cleanupSparkContexts();
  }

  private HoodieWriteClient getWriteClient(HoodieWriteConfig config) throws Exception {
    return new HoodieWriteClient(jsc, config);
  }

  private HoodieWriteConfig getConfig() {
    return getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
  }

  private HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024)
            .withInlineCompaction(false).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
        .withMemoryConfig(HoodieMemoryConfig.newBuilder().withMaxDFSStreamBufferSize(1 * 1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build());
  }

  @Test(expected = HoodieNotSupportedException.class)
  public void testCompactionOnCopyOnWriteFail() throws Exception {
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig(), jsc);
    String compactionInstantTime = HoodieActiveTimeline.createNewCommitTime();
    table.compact(jsc, compactionInstantTime, table.scheduleCompaction(jsc, compactionInstantTime));
  }

  @Test
  public void testCompactionEmpty() throws Exception {
    HoodieWriteConfig config = getConfig();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    try (HoodieWriteClient writeClient = getWriteClient(config);) {

      String newCommitTime = writeClient.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      String compactionInstantTime = HoodieActiveTimeline.createNewCommitTime();
      JavaRDD<WriteStatus> result =
          table.compact(jsc, compactionInstantTime, table.scheduleCompaction(jsc, compactionInstantTime));
      assertTrue("If there is nothing to compact, result will be empty", result.isEmpty());
    }
  }

  @Test
  public void testWriteStatusContentsAfterCompaction() throws Exception {
    // insert 100 records
    HoodieWriteConfig config = getConfig();
    try (HoodieWriteClient writeClient = getWriteClient(config);) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      List<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime).collect();

      // Update all the 100 records
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);

      newCommitTime = "101";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
      JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);
      HoodieIndex index = new HoodieBloomIndex<>(config);
      updatedRecords = index.tagLocation(updatedRecordsRDD, jsc, table).collect();

      // Write them to corresponding avro logfiles
      HoodieTestUtils.writeRecordsToLogFiles(fs, metaClient.getBasePath(),
          HoodieTestDataGenerator.avroSchemaWithMetadataFields, updatedRecords);

      // Verify that all data file has one log file
      metaClient = HoodieTableMetaClient.reload(metaClient);
      table = HoodieTable.getHoodieTable(metaClient, config, jsc);
      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> groupedLogFiles =
            table.getRTFileSystemView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
        for (FileSlice fileSlice : groupedLogFiles) {
          assertEquals("There should be 1 log file written for every data file", 1, fileSlice.getLogFiles().count());
        }
      }

      // Do a compaction
      metaClient = HoodieTableMetaClient.reload(metaClient);
      table = HoodieTable.getHoodieTable(metaClient, config, jsc);

      String compactionInstantTime = HoodieActiveTimeline.createNewCommitTime();
      JavaRDD<WriteStatus> result =
          table.compact(jsc, compactionInstantTime, table.scheduleCompaction(jsc, compactionInstantTime));

      // Verify that all partition paths are present in the WriteStatus result
      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<WriteStatus> writeStatuses = result.collect();
        assertTrue(writeStatuses.stream()
            .filter(writeStatus -> writeStatus.getStat().getPartitionPath().contentEquals(partitionPath)).count() > 0);
      }
    }
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  // TODO - after modifying HoodieReadClient to support realtime tables - add more tests to make
  // sure the data read is the updated data (compaction correctness)
  // TODO - add more test cases for compactions after a failed commit/compaction
}
