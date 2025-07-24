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

package org.apache.hudi.sink;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.utils.TestData.insertRow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test cases for delta stream write with compaction.
 */
public class TestWriteMergeOnReadWithCompact extends TestWriteCopyOnWrite {

  @Override
  protected void setUp(Configuration conf) {
    // trigger the compaction for every finished checkpoint
    conf.set(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
  }

  @Test
  public void testPartialFailover() {
    // partial failover is only valid for append mode.
  }

  @Test
  public void testInsertAppendMode() {
    // append mode is only valid for cow table.
  }

  @Override
  public void testInsertClustering() {
    // insert clustering is only valid for cow table.
  }

  @Test
  public void testInsertAsyncClustering() {
    // insert async clustering is only valid for cow table.
  }

  @Override
  protected Map<String, String> getExpectedBeforeCheckpointComplete() {
    return EXPECTED1;
  }

  protected Map<String, String> getMiniBatchExpected() {
    Map<String, String> expected = new HashMap<>();
    // MOR mode merges the messages with the same key.
    expected.put("par1", "[id1,par1,id1,Danny,23,1,par1]");
    return expected;
  }

  @Test
  public void testNonBlockingConcurrencyControlWithPartialUpdatePayload() throws Exception {
    conf.setString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL.name());
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    conf.set(FlinkOptions.PAYLOAD_CLASS_NAME, PartialUpdateAvroPayload.class.getName());
    // disable schedule compaction in writers
    conf.set(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, false);
    conf.set(FlinkOptions.PRE_COMBINE, true);
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key(), "false"); // HUDI-8814

    // start pipeline1 and insert record: [id1,Danny,null,1,par1], suspend the tx commit
    List<RowData> dataset1 = Collections.singletonList(
        insertRow(
            StringData.fromString("id1"), StringData.fromString("Danny"), null,
            TimestampData.fromEpochMillis(1), StringData.fromString("par1")));
    TestHarness pipeline1 = preparePipeline(conf)
        .consume(dataset1)
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent();

    // start pipeline2 and insert record: [id1,null,23,1,par1], suspend the tx commit
    Configuration conf2 = conf.clone();
    conf2.set(FlinkOptions.WRITE_CLIENT_ID, "2");
    List<RowData> dataset2 = Collections.singletonList(
        insertRow(
            StringData.fromString("id1"), null, 23,
            TimestampData.fromEpochMillis(2), StringData.fromString("par1")));
    TestHarness pipeline2 = preparePipeline(conf2)
        .consume(dataset2)
        .assertEmptyBaseFiles()
        .checkpoint(1)
        .assertNextEvent();

    // step to commit the 1st txn
    pipeline1.checkpointComplete(1);

    // step to commit the 2nd txn
    pipeline2.checkpointComplete(1);

    // snapshot result is [(id1,Danny,23,2,par1)] after two writers finish to commit
    Map<String, String> tmpSnapshotResult = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    pipeline2.checkWrittenData(tmpSnapshotResult, 1);

    // There is no base file in partition dir because there is no compaction yet.
    pipeline1.assertEmptyBaseFiles();

    // schedule compaction outside all writers
    try (HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf, TestUtils.getMockRuntimeContext())) {
      Option<String> scheduleInstant = writeClient.scheduleCompaction(Option.empty());
      assertNotNull(scheduleInstant.get());
    }

    // step to commit the 3rd txn
    // it also triggers inline compactor
    List<RowData> dataset3 = Collections.singletonList(
        insertRow(
            StringData.fromString("id3"), StringData.fromString("Julian"), 53,
            TimestampData.fromEpochMillis(4), StringData.fromString("par1")));
    pipeline1.consume(dataset3)
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2);

    // snapshot read result is [(id1,Danny,23,2,par1), (id3,Julian,53,4,par1)] after three writers finish to commit
    Map<String, String> finalSnapshotResult = Collections.singletonMap(
        "par1",
        "[id1,par1,id1,Danny,23,2,par1, id3,par1,id3,Julian,53,4,par1]");
    pipeline1.checkWrittenData(finalSnapshotResult, 1);
    // read optimized read result is [(id1,Danny,23,2,par1)]
    // because the data files belongs 3rd commit is not included in the last compaction.
    Map<String, String> readOptimizedResult = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    TestData.checkWrittenData(tempFile, readOptimizedResult, 1);
    pipeline1.end();
    pipeline2.end();
  }

  @Test
  public void testNonBlockingConcurrencyControlWithInflightInstant() throws Exception {
    conf.setString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL.name());
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    // disable schedule compaction in writers
    conf.set(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, false);
    conf.set(FlinkOptions.PRE_COMBINE, true);

    // start pipeline1 and insert record: [id1,Danny,23,1,par1], suspend the tx commit
    List<RowData> dataset1 = Collections.singletonList(
        insertRow(
            StringData.fromString("id1"), StringData.fromString("Danny"), 23,
            TimestampData.fromEpochMillis(1), StringData.fromString("par1")));
    TestHarness pipeline1 = preparePipeline(conf)
        .consume(dataset1)
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent();

    // start pipeline2 and insert record: [id2,Stephen,34,2,par1], suspend the tx commit
    Configuration conf2 = conf.clone();
    conf2.set(FlinkOptions.WRITE_CLIENT_ID, "2");

    List<RowData> dataset2 = Collections.singletonList(
        insertRow(
            StringData.fromString("id2"), StringData.fromString("Stephen"), 34,
            TimestampData.fromEpochMillis(2), StringData.fromString("par1")));
    TestHarness pipeline2 = preparePipeline(conf2)
        .consume(dataset2)
        .assertEmptyBaseFiles()
        // step to flush the 2nd data, but not commit yet
        .checkpoint(1)
        .assertNextEvent();

    // step to commit the 1st txn
    pipeline1.checkpointComplete(1);

    // schedule compaction outside all writers
    try (HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf, TestUtils.getMockRuntimeContext())) {
      Option<String> scheduleInstant = writeClient.scheduleCompaction(Option.empty());
      assertNotNull(scheduleInstant.get());
    }

    // step to commit the 3rd txn and insert record: [id3,Julian,53,4,par1]
    // it also triggers inline compactor
    List<RowData> dataset3 = Collections.singletonList(
        insertRow(
            StringData.fromString("id3"), StringData.fromString("Julian"), 53,
            TimestampData.fromEpochMillis(4), StringData.fromString("par1")));
    pipeline1.consume(dataset3)
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2);

    // snapshot read result is [(id1,Danny,23,1,par1), (id3,Julian,53,4,par1)] after 1st writer and 3rd writer finish to commit
    // and the data of 2nd writer is not included because it is still in inflight state
    Map<String, String> finalSnapshotResult = Collections.singletonMap(
        "par1",
        "[id1,par1,id1,Danny,23,1,par1, id3,par1,id3,Julian,53,4,par1]");
    pipeline1.checkWrittenData(finalSnapshotResult, 1);
    // read optimized read result is [(id1,Danny,23,1,par1)]
    // because 2nd commit is in inflight state and
    // the data files belongs 3rd commit is not included in the last compaction.
    Map<String, String> readOptimizedResult = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,1,par1]");
    TestData.checkWrittenData(tempFile, readOptimizedResult, 1);
    pipeline1.end();
    pipeline2.end();
  }

  // case1: txn1 is upsert writer, txn2 is bulk_insert writer.
  //      |----------- txn1 -----------|
  //                       |----- txn2 ------|
  // the txn2 would fail to commit caused by conflict
  @Test
  public void testBulkInsertWithNonBlockingConcurrencyControl() throws Exception {
    conf.setString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL.name());
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    conf.set(FlinkOptions.PAYLOAD_CLASS_NAME, PartialUpdateAvroPayload.class.getName());
    // disable schedule compaction in writers
    conf.set(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, false);
    conf.set(FlinkOptions.PRE_COMBINE, true);

    // start pipeline1 and insert record: [id1,Danny,null,1,par1], suspend the tx commit
    List<RowData> dataset1 = Collections.singletonList(
        insertRow(
            StringData.fromString("id1"), StringData.fromString("Danny"), null,
            TimestampData.fromEpochMillis(1), StringData.fromString("par1")));
    TestHarness pipeline1 = preparePipeline(conf)
        .consume(dataset1)
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent();

    // start pipeline2 and bulk insert record: [id1,null,23,1,par1], suspend the tx commit
    Configuration conf2 = conf.clone();
    conf2.set(FlinkOptions.OPERATION, "BULK_INSERT");
    conf2.set(FlinkOptions.WRITE_CLIENT_ID, "2");
    List<RowData> dataset2 = Collections.singletonList(
        insertRow(
            StringData.fromString("id1"), null, 23,
            TimestampData.fromEpochMillis(2), StringData.fromString("par1")));
    TestHarness pipeline2 = preparePipeline(conf2)
        .consume(dataset2)
        .endInput();

    // step to commit the 1st txn
    pipeline1.checkpointComplete(1);

    // step to commit the 2nd txn, should throw exception
    pipeline2.endInputCompleteThrows(HoodieWriteConflictException.class, "Cannot resolve conflicts");
    pipeline1.end();
    pipeline2.end();
  }

  // case2: txn1 is bulk_insert writer, txn2 is upsert writer.
  //               |----- txn1 ------|
  //      |----------- txn2 -----------|
  // both two txn would success to commit
  @Test
  public void testBulkInsertInSequenceWithNonBlockingConcurrencyControl() throws Exception {
    conf.setString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL.name());
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    conf.set(FlinkOptions.PAYLOAD_CLASS_NAME, PartialUpdateAvroPayload.class.getName());
    // disable schedule compaction in writers
    conf.set(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, false);
    conf.set(FlinkOptions.PRE_COMBINE, true);
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key(), "false");

    Configuration conf1 = conf.clone();
    conf1.set(FlinkOptions.OPERATION, "BULK_INSERT");
    // start pipeline1 and bulk insert record: [id1,Danny,null,1,par1], suspend the tx commit
    List<RowData> dataset1 = Collections.singletonList(
        insertRow(
            StringData.fromString("id1"), StringData.fromString("Danny"), null,
            TimestampData.fromEpochMillis(1), StringData.fromString("par1")));
    TestHarness pipeline1 = preparePipeline(conf1)
        .consume(dataset1)
        .endInput();

    // start pipeline2 and insert record: [id1,null,23,2,par1], suspend the tx commit
    Configuration conf2 = conf.clone();
    conf2.set(FlinkOptions.WRITE_CLIENT_ID, "2");
    List<RowData> dataset2 = Collections.singletonList(
        insertRow(
            StringData.fromString("id1"), null, 23,
            TimestampData.fromEpochMillis(2), StringData.fromString("par1")));
    TestHarness pipeline2 = preparePipeline(conf2)
        .consume(dataset2)
        .checkpoint(1)
        .assertNextEvent();

    // step to commit the 1st txn
    pipeline1.endInputComplete();

    // step to commit the 2nd data
    pipeline2.checkpointComplete(1);

    // snapshot result is [(id1,Danny,23,2,par1)] after two writers finish to commit
    Map<String, String> tmpSnapshotResult = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    pipeline2.checkWrittenData(tmpSnapshotResult, 1);

    // schedule compaction outside all writers
    try (HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf, TestUtils.getMockRuntimeContext())) {
      Option<String> scheduleInstant = writeClient.scheduleCompaction(Option.empty());
      assertNotNull(scheduleInstant.get());
    }

    // step to commit the 3rd txn
    // it also triggers inline compactor
    List<RowData> dataset3 = Collections.singletonList(
        insertRow(
            StringData.fromString("id3"), StringData.fromString("Julian"), 53,
            TimestampData.fromEpochMillis(4), StringData.fromString("par1")));
    pipeline2.consume(dataset3)
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2);

    // snapshot read result is [(id1,Danny,23,2,par1), (id3,Julian,53,4,par1)] after three writers finish to commit
    Map<String, String> finalSnapshotResult = Collections.singletonMap(
        "par1",
        "[id1,par1,id1,Danny,23,2,par1, id3,par1,id3,Julian,53,4,par1]");
    pipeline2.checkWrittenData(finalSnapshotResult, 1);
    // read optimized read result is [(id1,Danny,23,2,par1)]
    // because the data files belongs 3rd commit is not included in the last compaction.
    Map<String, String> readOptimizedResult = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    TestData.checkWrittenData(tempFile, readOptimizedResult, 1);
    pipeline1.end();
    pipeline2.end();
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
