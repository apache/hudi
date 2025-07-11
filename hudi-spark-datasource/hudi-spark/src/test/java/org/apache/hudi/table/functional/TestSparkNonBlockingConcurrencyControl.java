/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.BucketIndexConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner;
import org.apache.hudi.table.storage.HoodieStorageLayout;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.TYPE;
import static org.apache.hudi.config.HoodieWriteConfig.ENABLE_SCHEMA_CONFLICT_RESOLUTION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestSparkNonBlockingConcurrencyControl extends SparkClientFunctionalTestHarness {

  String jsonSchema = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"partialRecord\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"_hoodie_commit_time\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_commit_seqno\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_record_key\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_partition_path\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_file_name\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"name\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"age\", \"type\": [\"null\", \"int\"]},\n"
      + "    {\"name\": \"ts\", \"type\": [\"null\", \"long\"]},\n"
      + "    {\"name\": \"part\", \"type\": [\"null\", \"string\"]}\n"
      + "  ]\n"
      + "}";

  private Schema schema;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws Exception {
    schema = new Schema.Parser().parse(jsonSchema);
  }

  @Test
  public void testNonBlockingConcurrencyControlWithPartialUpdatePayload() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, config.getProps());

    // start the 1st txn and insert record: [id1,Danny,null,1,par1], suspend the tx commit
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    SparkRDDWriteClient client1 = getHoodieWriteClient(config);
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    // start the 2nd txn and insert record: [id1,null,23,2,par1], suspend the tx commit
    SparkRDDWriteClient client2 = getHoodieWriteClient(config);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client2, insertTime2, dataset2, false, WriteOperationType.INSERT);

    // step to commit the 1st txn
    client1.commitStats(
        insertTime1,
        writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // step to commit the 2nd txn
    client2.commitStats(
        insertTime2,
        writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // There is no base file in partition dir because there is no compaction yet.
    assertFalse(fileExists(), "No base data files should have been created");

    // do compaction
    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata writeMetadata = client1.compact(compactionTime);
    client1.commitCompaction(compactionTime, writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));

    // result is [(id1,Danny,23,2,par1)]
    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    checkWrittenData(result, 1);
    client1.close();
    client2.close();
  }

  @Test
  public void testNonBlockingConcurrencyControlWithInflightInstant() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, config.getProps());

    // start the 1st txn and insert record: [id1,Danny,null,1,par1], suspend the tx commit
    SparkRDDWriteClient client1 = getHoodieWriteClient(config);
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    // start the 2nd txn and insert record: [id1,null,23,2,par1], suspend the tx commit
    SparkRDDWriteClient client2 = getHoodieWriteClient(config);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    writeData(client2, insertTime2, dataset2, false, WriteOperationType.INSERT);

    // step to commit the 1st txn
    client1.commitStats(
        insertTime1,
        writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // schedule compaction
    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();

    // step to commit the 3rd txn, insert record: [id3,Julian,53,4,par1] and commit 3rd txn
    List<String> dataset3 = Collections.singletonList("id3,Julian,53,4,par1");
    String insertTime3 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses3 = writeData(client1, insertTime3, dataset3, false, WriteOperationType.INSERT);
    client1.commitStats(
        insertTime3,
        writeStatuses3.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // do compaction
    HoodieWriteMetadata writeMetadata = client1.compact(compactionTime);
    client1.commitCompaction(compactionTime, writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));

    // read optimized result is [(id1,Danny,23,1,par1)]
    // because 2nd commit is in inflight state and
    // the data files belongs 3rd commit is not included in the last compaction.
    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,null,1,par1]");
    checkWrittenData(result, 1);
    client1.close();
    client2.close();
  }

  // Validate that multiple writers will only produce base files for bulk insert
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMultiBaseFile(boolean bulkInsertFirst) throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig(true);
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, config.getProps());
    // there should only be a single filegroup, so we will verify that it is consistent
    String fileID = null;

    // if there is not a bulk insert first, then we will write to log files for a filegroup
    // without a base file. Having a base file adds the possibility of small file handling
    // which we want to ensure doesn't happen.
    if (bulkInsertFirst) {
      SparkRDDWriteClient client0 = getHoodieWriteClient(config);
      List<String> dataset0 = Collections.singletonList("id0,Danny,0,0,par1");
      String insertTime0 = WriteClientTestUtils.createNewInstantTime();
      List<WriteStatus> writeStatuses0 = writeData(client0, insertTime0, dataset0, false, WriteOperationType.BULK_INSERT, true);
      client0.commitStats(
          insertTime0,
          writeStatuses0.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
          Option.empty(),
          metaClient.getCommitActionType());
      for (WriteStatus status : writeStatuses0) {
        if (fileID == null) {
          fileID = status.getFileId();
        } else {
          assertEquals(fileID, status.getFileId());
        }
        assertFalse(FSUtils.isLogFile(new StoragePath(status.getStat().getPath()).getName()));
      }
      client0.close();
    }

    SparkRDDWriteClient client1 = getHoodieWriteClient(config);
    List<String> dataset1 = Collections.singletonList("id1,Danny,22,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT, true);
    for (WriteStatus status : writeStatuses1) {
      if (fileID == null) {
        fileID = status.getFileId();
      } else {
        assertEquals(fileID, status.getFileId());
      }
      assertTrue(FSUtils.isLogFile(new StoragePath(status.getStat().getPath()).getName()));
    }

    SparkRDDWriteClient client2 = getHoodieWriteClient(config);
    List<String> dataset2 = Collections.singletonList("id1,Danny,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client2, insertTime2, dataset2, false, WriteOperationType.UPSERT, true);
    for (WriteStatus status : writeStatuses2) {
      assertEquals(fileID, status.getFileId());
      assertTrue(FSUtils.isLogFile(new StoragePath(status.getStat().getPath()).getName()));
    }

    // step to commit the 1st txn
    client1.commitStats(
        insertTime1,
        writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // step to commit the 2nd txn
    client2.commitStats(
        insertTime2,
        writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    client1.close();
    client2.close();

    metaClient.reloadActiveTimeline();
    List<HoodieInstant> instants = metaClient.getActiveTimeline().getInstants();
    if (bulkInsertFirst) {
      assertEquals(3, instants.size());
      // check that bulk insert finished before the upsert started
      assertTrue(Long.parseLong(instants.get(0).getCompletionTime()) < Long.parseLong(instants.get(1).requestedTime()));
      // check that the upserts overlapped in time
      assertTrue(Long.parseLong(instants.get(1).getCompletionTime()) > Long.parseLong(instants.get(2).requestedTime()));
      assertTrue(Long.parseLong(instants.get(2).getCompletionTime()) > Long.parseLong(instants.get(1).requestedTime()));
    } else {
      assertEquals(2, instants.size());
      // check that the upserts overlapped in time
      assertTrue(Long.parseLong(instants.get(0).getCompletionTime()) > Long.parseLong(instants.get(1).requestedTime()));
      assertTrue(Long.parseLong(instants.get(1).getCompletionTime()) > Long.parseLong(instants.get(0).requestedTime()));
    }
  }

  /**
   * case1:
   * 1. insert start
   * 2. insert commit
   * 3. bulk_insert start
   * 4. bulk_insert commit
   *
   *  |------ txn1: insert ------|
   *                             |------ txn2: bulk_insert ------|
   *
   *  both two txn would success to commit
   */
  @Test
  public void testBulkInsertAndInsertConcurrentCase1() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, config.getProps());

    // start the 1st txn and insert record: [id1,Danny,null,1,par1], commit the 1st txn
    SparkRDDWriteClient client1 = getHoodieWriteClient(config);
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    writeData(client1, insertTime1, dataset1, true, WriteOperationType.INSERT);

    // start the 2nd txn and bulk_insert record: [id1,null,23,2,par1], commit the 2nd txn
    SparkRDDWriteClient client2 = getHoodieWriteClient(config);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    writeData(client2, insertTime2, dataset2, true, WriteOperationType.BULK_INSERT);

    // do compaction
    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata writeMetadata = client1.compact(compactionTime);
    client1.commitCompaction(compactionTime, writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));

    // result is [(id1,Danny,23,2,par1)]
    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    checkWrittenData(result, 1);
    client1.close();
    client2.close();
  }

  /**
   * case2:
   * 1. insert start
   * 2. bulk_insert start
   * 3. insert commit
   * 4. bulk_insert commit
   *
   *  |------ txn1: insert ------|
   *                      |------ txn2: bulk_insert ------|
   *
   *  the txn2 should be fail to commit caused by conflict
   */
  @Test
  public void testBulkInsertAndInsertConcurrentCase2() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, config.getProps());
    SparkRDDWriteClient client1 = getHoodieWriteClient(config);

    // start the 1st txn and insert record: [id1,Danny,null,1,par1], suspend the tx commit
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    // start the 2nd txn and bulk insert record: [id1,null,23,2,par1], suspend the tx commit
    SparkRDDWriteClient client2 = getHoodieWriteClient(config);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client2, insertTime2, dataset2, false, WriteOperationType.BULK_INSERT);

    // step to commit the 1st txn
    client1.commitStats(
        insertTime1,
        writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // step to commit the 2nd txn
    assertThrows(HoodieWriteConflictException.class, () -> {
      client2.commitStats(
          insertTime2,
          writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
          Option.empty(),
          metaClient.getCommitActionType());
    });
    client1.close();
    client2.close();
  }

  /**
   * case3:
   * 1. bulk_insert start
   * 2. insert start
   * 3. insert commit
   * 4. bulk_insert commit
   *
   *          |------ txn2: insert ------|
   *    |---------- txn1: bulk_insert ----------|
   *
   *  the txn2 should be fail to commit caused by conflict
   */
  @Test
  public void testBulkInsertAndInsertConcurrentCase3() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, config.getProps());
    SparkRDDWriteClient client1 = getHoodieWriteClient(config);

    // start the 1st txn and bulk insert record: [id1,null,23,2,par1], suspend the tx commit
    SparkRDDWriteClient client2 = getHoodieWriteClient(config);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client2, insertTime2, dataset2, false, WriteOperationType.BULK_INSERT);

    // start the 2nd txn and insert record: [id1,Danny,null,1,par1], suspend the tx commit
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    // step to commit the 2nd txn
    client1.commitStats(
        insertTime1,
        writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // step to commit the 1st txn
    assertThrows(HoodieWriteConflictException.class, () -> {
      client2.commitStats(
          insertTime2,
          writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
          Option.empty(),
          metaClient.getCommitActionType());
    });
    client1.close();
    client2.close();
  }

  /**
   * case4:
   * 1. insert start
   * 2. bulk_insert start
   * 3. bulk_insert commit
   * 4. insert commit
   *
   *  |------------ txn1: insert ------------|
   *      |------ txn2: bulk_insert ------|
   *
   *  both two txn would success to commit
   */
  @Test
  public void testBulkInsertAndInsertConcurrentCase4() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, config.getProps());
    SparkRDDWriteClient client1 = getHoodieWriteClient(config);

    // start the 1st txn and insert record: [id1,Danny,null,1,par1], suspend the tx commit
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    // start the 2nd txn and bulk insert record: [id1,null,23,2,par1], suspend the tx commit
    SparkRDDWriteClient client2 = getHoodieWriteClient(config);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client2, insertTime2, dataset2, false, WriteOperationType.BULK_INSERT);

    // step to commit the 2nd txn
    client2.commitStats(
        insertTime2,
        writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // step to commit the 1st txn
    client1.commitStats(
        insertTime1,
        writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // do compaction
    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata writeMetadata = client1.compact(compactionTime);
    client1.commitCompaction(compactionTime, writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));

    // result is [(id1,Danny,23,2,par1)]
    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    checkWrittenData(result, 1);
    client1.close();
    client2.close();
  }

  /**
   * case5:
   * 1. bulk_insert start
   * 2. insert start
   * 3. bulk_insert commit
   * 4. insert commit
   *
   *                          |------ txn2: insert ------|
   *    |---------- txn1: bulk_insert ----------|
   *
   *  both two txn would success to commit
   */
  @Test
  public void testBulkInsertAndInsertConcurrentCase5() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, config.getProps());
    SparkRDDWriteClient client1 = getHoodieWriteClient(config);

    // start the 1st txn and bulk insert record: [id1,null,23,2,par1], suspend the tx commit
    SparkRDDWriteClient client2 = getHoodieWriteClient(config);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client2, insertTime2, dataset2, false, WriteOperationType.BULK_INSERT);

    // start the 2nd txn and insert record: [id1,Danny,null,1,par1], suspend the tx commit
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    // step to commit the 1st txn
    client2.commitStats(
        insertTime2,
        writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // step to commit the 2nd txn
    client1.commitStats(
        insertTime1,
        writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // do compaction
    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata writeMetadata = client1.compact(compactionTime);
    client1.commitCompaction(compactionTime, writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));

    // result is [(id1,Danny,23,2,par1)]
    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    checkWrittenData(result, 1);
    client1.close();
    client2.close();
  }

  /**
   * case6:
   * 1. bulk_insert start
   * 2. bulk_insert commit
   * 3. insert start
   * 4. insert commit
   *
   *                                   |------ txn2: insert ------|
   *  |------ txn1: bulk_insert ------|
   *
   *  both two txn would success to commit
   */
  @Test
  public void testBulkInsertAndInsertConcurrentCase6() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, config.getProps());

    // start the 1st txn and bulk insert record: [id1,Danny,null,1,par1], commit the 1st txn
    SparkRDDWriteClient client1 = getHoodieWriteClient(config);
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    writeData(client1, insertTime1, dataset1, true, WriteOperationType.BULK_INSERT);

    // start the 2nd txn and insert record: [id1,null,23,2,par1], commit the 2nd txn
    SparkRDDWriteClient client2 = getHoodieWriteClient(config);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    writeData(client2, insertTime2, dataset2, true, WriteOperationType.INSERT);

    // do compaction
    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata writeMetadata = client1.compact(compactionTime);
    client1.commitCompaction(compactionTime, writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));

    // result is [(id1,Danny,23,2,par1)]
    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    checkWrittenData(result, 1);
    client1.close();
    client2.close();
  }

  private HoodieWriteConfig createHoodieWriteConfig() {
    return createHoodieWriteConfig(false);
  }

  private HoodieWriteConfig createHoodieWriteConfig(boolean fullUpdate) {
    String payloadClassName = PartialUpdateAvroPayload.class.getName();
    if (fullUpdate) {
      payloadClassName = OverwriteWithLatestAvroPayload.class.getName();
    }
    Properties props = getPropertiesForKeyGen(true);
    props.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    props.put(ENABLE_SCHEMA_CONFLICT_RESOLUTION.key(), "false");
    String basePath = basePath();
    return HoodieWriteConfig.newBuilder()
        .withProps(Collections.singletonMap(HoodieTableConfig.PRECOMBINE_FIELD.key(), "ts"))
        .forTable("test")
        .withPath(basePath)
        .withSchema(jsonSchema)
        .withParallelism(2, 2)
        .withRecordMergeMode(RecordMergeMode.CUSTOM)
        .withPayloadConfig(
            HoodiePayloadConfig.newBuilder()
                .withPayloadClass(payloadClassName)
                .build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .parquetMaxFileSize(1024).build())
        .withLayoutConfig(HoodieLayoutConfig.newBuilder()
            .withLayoutType(HoodieStorageLayout.LayoutType.BUCKET.name())
            .withLayoutPartitioner(SparkBucketIndexPartitioner.class.getName()).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketNum("1")
            .build())
        .withPopulateMetaFields(true)
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .withConflictResolutionStrategy(new BucketIndexConcurrentFileWritesConflictResolutionStrategy())
            .build())
        .build();
  }

  private void checkWrittenData(
      Map<String, String> expected,
      int partitions) throws IOException {
    File baseFile = tempDir.toFile();
    assert baseFile.isDirectory();
    FileFilter filter = file -> !file.getName().startsWith(".");
    File[] partitionDirs = baseFile.listFiles(filter);
    assertNotNull(partitionDirs);
    assertThat(partitionDirs.length, is(partitions));
    for (File partitionDir : partitionDirs) {
      File[] dataFiles = partitionDir.listFiles(filter);
      assertNotNull(dataFiles);
      File latestDataFile = Arrays.stream(dataFiles)
          .max(Comparator.comparing(f -> FSUtils.getCommitTime(f.getName())))
          .orElse(dataFiles[0]);
      ParquetReader<GenericRecord> reader = AvroParquetReader
          .<GenericRecord>builder(new Path(latestDataFile.getAbsolutePath())).build();
      List<String> readBuffer = new ArrayList<>();
      GenericRecord nextRecord = reader.read();
      while (nextRecord != null) {
        readBuffer.add(filterOutVariables(nextRecord));
        nextRecord = reader.read();
      }
      readBuffer.sort(Comparator.naturalOrder());
      assertThat(readBuffer.toString(), is(expected.get(partitionDir.getName())));
    }
  }

  private static String filterOutVariables(GenericRecord genericRecord) {
    List<String> fields = new ArrayList<>();
    fields.add(getFieldValue(genericRecord, "_hoodie_record_key"));
    fields.add(getFieldValue(genericRecord, "_hoodie_partition_path"));
    fields.add(getFieldValue(genericRecord, "id"));
    fields.add(getFieldValue(genericRecord, "name"));
    fields.add(getFieldValue(genericRecord, "age"));
    fields.add(genericRecord.get("ts").toString());
    fields.add(genericRecord.get("part").toString());
    return String.join(",", fields);
  }

  private static String getFieldValue(GenericRecord genericRecord, String fieldName) {
    if (genericRecord.get(fieldName) != null) {
      return genericRecord.get(fieldName).toString();
    } else {
      return null;
    }
  }

  private boolean fileExists() {
    List<File> dirsToCheck = new ArrayList<>();
    dirsToCheck.add(tempDir.toFile());
    while (!dirsToCheck.isEmpty()) {
      File dir = dirsToCheck.remove(0);
      for (File file : Objects.requireNonNull(dir.listFiles())) {
        if (!file.getName().startsWith(".")) {
          if (file.isDirectory()) {
            dirsToCheck.add(file);
          } else {
            return true;
          }
        }
      }
    }
    return false;
  }

  private GenericRecord str2GenericRecord(String str) {
    GenericRecord record = new GenericData.Record(schema);
    String[] fieldValues = str.split(",");
    ValidationUtils.checkArgument(fieldValues.length == 5, "Valid record must have 5 fields");
    record.put("id", StringUtils.isNullOrEmpty(fieldValues[0]) ? null : fieldValues[0]);
    record.put("name", StringUtils.isNullOrEmpty(fieldValues[1]) ? null : fieldValues[1]);
    record.put("age", StringUtils.isNullOrEmpty(fieldValues[2]) ? null : Integer.parseInt(fieldValues[2]));
    record.put("ts", StringUtils.isNullOrEmpty(fieldValues[3]) ? null : Long.parseLong(fieldValues[3]));
    record.put("part", StringUtils.isNullOrEmpty(fieldValues[4]) ? null : fieldValues[4]);
    return record;
  }

  private List<HoodieRecord> str2HoodieRecord(List<String> records, boolean fullUpdate) {
    return records.stream().map(recordStr -> {
      GenericRecord record = str2GenericRecord(recordStr);
      OverwriteWithLatestAvroPayload payload;
      if (fullUpdate) {
        payload = new OverwriteWithLatestAvroPayload(record, (Long) record.get("ts"));
      } else {
        payload = new PartialUpdateAvroPayload(record, (Long) record.get("ts"));
      }
      return new HoodieAvroRecord<>(new HoodieKey((String) record.get("id"), (String) record.get("part")), payload);
    }).collect(Collectors.toList());
  }

  private List<WriteStatus> writeData(
      SparkRDDWriteClient client,
      String instant,
      List<String> records,
      boolean doCommit,
      WriteOperationType operationType) {
    return writeData(client, instant, records, doCommit, operationType, false);
  }

  private List<WriteStatus> writeData(
      SparkRDDWriteClient client,
      String instant,
      List<String> records,
      boolean doCommit,
      WriteOperationType operationType,
      boolean fullUpdate) {
    List<HoodieRecord> recordList = str2HoodieRecord(records, fullUpdate);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(recordList, 2);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    WriteClientTestUtils.startCommitWithTime(client, instant);
    List<WriteStatus> writeStatuses;
    switch (operationType) {
      case INSERT:
        writeStatuses = client.insert(writeRecords, instant).collect();
        break;
      case UPSERT:
        writeStatuses = client.upsert(writeRecords, instant).collect();
        break;
      case BULK_INSERT:
        writeStatuses = client.bulkInsert(writeRecords, instant).collect();
        break;
      default:
        throw new UnsupportedOperationException(operationType + " is not supported yet in this test!");
    }
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatuses);
    if (doCommit) {
      List<HoodieWriteStat> writeStats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
      boolean committed = client.commitStats(instant, writeStats, Option.empty(), metaClient.getCommitActionType());
      Assertions.assertTrue(committed);
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatuses;
  }
}
