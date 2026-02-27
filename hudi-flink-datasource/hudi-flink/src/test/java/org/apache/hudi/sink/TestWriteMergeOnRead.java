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

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.utils.TestData.insertRow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test cases for delta stream write.
 */
public class TestWriteMergeOnRead extends TestWriteCopyOnWrite {

  @Override
  protected void setUp(Configuration conf) {
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
  }

  @Test
  public void testIndexStateBootstrapWithMultiFilesInOneSlice() throws Exception {
    // open the function and ingest data
    preparePipeline(conf)
        .consume(TestData.filterOddRows(TestData.DATA_SET_INSERT))
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .consume(TestData.filterEvenRows(TestData.DATA_SET_INSERT))
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2)
        .checkWrittenData(EXPECTED1, 4)
        // write another commit but does not complete it
        .consume(TestData.filterEvenRows(TestData.DATA_SET_INSERT))
        .checkpoint(3)
        .assertNextEvent()
        .end();

    // reset the config option
    conf.set(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, true);
    validateIndexLoaded();
  }

  @Test
  public void testIndexStateBootstrapWithCompactionScheduled() throws Exception {
    // sets up the delta commits as 1 to generate a new compaction plan.
    conf.set(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    // open the function and ingest data
    preparePipeline(conf)
        .consume(TestData.DATA_SET_INSERT)
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .checkWrittenData(EXPECTED1, 4)
        .end();

    // reset config options
    conf.removeConfig(FlinkOptions.COMPACTION_DELTA_COMMITS);
    // sets up index bootstrap
    conf.set(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, true);
    validateIndexLoaded();
  }

  @Test
  public void testEventTimeAvroPayloadMergeRead() throws Exception {
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    conf.set(FlinkOptions.PATH, tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    conf.set(FlinkOptions.OPERATION, "upsert");
    conf.set(FlinkOptions.CHANGELOG_ENABLED, false);
    conf.set(FlinkOptions.COMPACTION_DELTA_COMMITS, 2);
    conf.set(FlinkOptions.PRE_COMBINE, true);
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
    conf.set(FlinkOptions.PAYLOAD_CLASS_NAME, EventTimeAvroPayload.class.getName());
    HashMap<String, String> mergedExpected = new HashMap<>(EXPECTED1);
    mergedExpected.put("par1", "[id1,par1,id1,Danny,22,4,par1, id2,par1,id2,Stephen,33,2,par1]");
    TestHarness.instance().preparePipeline(tempFile, conf)
        .consume(TestData.DATA_SET_INSERT)
        .emptyEventBuffer()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .checkWrittenData(EXPECTED1, 4)
        .consume(TestData.DATA_SET_DISORDER_INSERT)
        .emptyEventBuffer()
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2)
        .checkWrittenData(mergedExpected, 4)
        .consume(TestData.DATA_SET_SINGLE_INSERT)
        .emptyEventBuffer()
        .checkpoint(3)
        .assertNextEvent()
        .checkpointComplete(3)
        .checkWrittenData(mergedExpected, 4)
        .end();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testOnlyBaseFileOrOnlyLogFileRead(int compactionDeltaCommits) throws Exception {
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    conf.set(FlinkOptions.PATH, tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    conf.set(FlinkOptions.OPERATION, "upsert");
    conf.set(FlinkOptions.CHANGELOG_ENABLED, false);
    conf.set(FlinkOptions.COMPACTION_DELTA_COMMITS, compactionDeltaCommits);
    TestHarness.instance().preparePipeline(tempFile, conf)
        .consume(TestData.DATA_SET_INSERT)
        .emptyEventBuffer()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .checkWrittenData(EXPECTED1, 4)
        .end();
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

  @Test
  public void testConsistentBucketIndex() throws Exception {
    conf.set(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf.set(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE, "CONSISTENT_HASHING");
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 4);
    conf.setString(HoodieIndexConfig.BUCKET_INDEX_MAX_NUM_BUCKETS.key(), "8");
    // Enable inline resize scheduling
    conf.set(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, true);
    // Manually set the max commits to trigger clustering quickly
    conf.setString(HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMITS.key(), "1");
    // Manually set the split threshold to trigger split in the clustering
    conf.set(FlinkOptions.WRITE_PARQUET_MAX_FILE_SIZE, 1);
    conf.setString(HoodieIndexConfig.BUCKET_SPLIT_THRESHOLD.key(), String.valueOf(1 / 1024.0 / 1024.0));
    conf.set(FlinkOptions.PRE_COMBINE, true);
    HashMap<String, String> mergedExpected = new HashMap<>(EXPECTED1);
    mergedExpected.put("par1", "[id1,par1,id1,Danny,22,4,par1, id2,par1,id2,Stephen,33,2,par1]");
    TestHarness.instance().preparePipeline(tempFile, conf)
        .consume(TestData.DATA_SET_INSERT)
        .emptyEventBuffer()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .checkWrittenData(EXPECTED1, 4)
        .consume(TestData.DATA_SET_DISORDER_INSERT)
        .emptyEventBuffer()
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2)
        .checkWrittenData(mergedExpected, 4)
        .consume(TestData.DATA_SET_SINGLE_INSERT)
        .emptyEventBuffer()
        .checkpoint(3)
        .assertNextEvent()
        .checkpointComplete(3)
        .checkWrittenData(mergedExpected, 4)
        .end();
  }

  @Test
  void testWriteMorWithSmallLogBlock() throws Exception {
    // total 5 records, average records size is 48,
    // set max block size as 128 to trigger a flush during write log data blocks
    conf.setString(HoodieStorageConfig.LOGFILE_DATA_BLOCK_MAX_SIZE.key(), "128");

    Map<String, String> expected = new HashMap<>();
    expected.put("par1", "[id1,par1,id1,Danny,23,4,par1]");

    preparePipeline()
        .consume(TestData.DATA_SET_INSERT_SAME_KEY)
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .checkWrittenData(expected, 1)
        .end();
  }

  @Test
  public void testRecommitAfterCoordinatorRestart() throws Exception {
    Map<String, String> expected = new HashMap<>();
    expected.put("par1", "[id1,par1,id1,Danny,23,1,par1]");
    expected.put("par2", "[id3,par2,id3,Julian,53,3,par2, id4,par2,id4,Fabian,31,4,par2]");
    preparePipeline(conf)
        .consume(TestData.DATA_SET_PART1)
        .emptyEventBuffer()
        .checkpoint(1)
        .assertNextEvent(1, "par1")
        .consume(TestData.DATA_SET_PART3)
        .checkpoint(2)
        // both ckp-1 and ckp-2 are not committing
        .assertNextEvent(1, "par2")
        // then simulating restarting job manually, coordinator will reset to ckp-2
        // and recommit write metadata for ckp-1
        .restartCoordinator()
        .subTaskFails(0, 0)
        // subtask will resend the write metadata event during initialize state
        // and coordinator will recommit data for ckp-2
        .assertNextEvent()
        // insert another batch of data.
        .consume(TestData.DATA_SET_PART4)
        .checkpoint(3)
        .assertNextEvent(1, "par2")
        // write metadata will be committed for ckp-3
        .checkpointComplete(3)
        // there should be 3 rows and 2 partitions
        .checkWrittenData(expected, 2)
        .end();
  }

  @Test
  public void testInsertDuplicateRecordsWithCDCMode() throws Exception {
    conf.set(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, 10_000L);
    conf.set(FlinkOptions.CDC_ENABLED, true);

    Map<String, String> expected = new HashMap<>();
    expected.put("par1", "[id1,par1,id1,Danny,23,1,par1]");

    List<RowData> insertData = List.of(
        insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
            TimestampData.fromEpochMillis(1), StringData.fromString("par1")));

    TestHarness testHarness = preparePipeline()
        .consume(insertData)
        .checkpoint(1)
        .allDataFlushed()
        .handleEvents(1);

    Thread t1 = new Thread(() -> {
      try {
        Thread.sleep(3000);
        testHarness.checkpointComplete(1);
        testHarness.checkWrittenData(expected, 1);
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    });
    t1.start();

    testHarness
        .consume(insertData)
        .checkpoint(2)
        .allDataFlushed();
    t1.join();

    testHarness.handleEvents(1)
        .checkpointComplete(2)
        .checkWrittenData(expected, 1)
        .end();

    // validate the metadata of the above two inserts, specifically the `prevCommit` in the write stat, the expected
    // result should be:
    // 1) the prev commit time for the first commit is itself.
    // 2) the prev commit time for the second commit is the first instant time, which ensures the deterministic update sequence.
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    List<HoodieInstant> completedDeltaInstants =
        metaClient.reloadActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().getInstants();
    assertEquals(2, completedDeltaInstants.size());
    HoodieInstant firstInstant = completedDeltaInstants.get(0);
    HoodieInstant secondInstant = completedDeltaInstants.get(1);
    HoodieCommitMetadata firstCommitMetadata = metaClient.getActiveTimeline().readCommitMetadata(firstInstant);
    HoodieCommitMetadata secondCommitMetadata = metaClient.getActiveTimeline().readCommitMetadata(secondInstant);

    List<HoodieWriteStat> firstWriteStats = firstCommitMetadata.getWriteStats();
    assertEquals(1, firstWriteStats.size());
    assertEquals(firstInstant.requestedTime(), firstWriteStats.get(0).getPrevCommit());

    List<HoodieWriteStat> secondWriteStats = secondCommitMetadata.getWriteStats();
    assertEquals(1, secondWriteStats.size());
    assertEquals(firstInstant.requestedTime(), secondWriteStats.get(0).getPrevCommit());
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

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
