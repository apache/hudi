/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.utils.TestWriteBase;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Test cases for stream write.
 */
public class TestWriteCopyOnWrite extends TestWriteBase {

  protected Configuration conf;

  @TempDir
  File tempFile;

  @BeforeEach
  public void before() {
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.TABLE_TYPE, getTableType().name());
    setUp(conf);
  }

  @AfterEach
  public void after() {
    conf = null;
  }

  /**
   * Override to have custom configuration.
   */
  protected void setUp(Configuration conf) {
    // for subclass extension
  }

  @Test
  public void testCheckpoint() throws Exception {
    preparePipeline()
        .consume(TestData.DATA_SET_INSERT)
        // no checkpoint, so the coordinator does not accept any events
        .emptyEventBuffer()
        .checkpoint(1)
        .assertNextEvent(4, "par1,par2,par3,par4")
        .checkpointComplete(1)
        // checkpoint for next round, no data input, so after the checkpoint,
        // there should not be REQUESTED Instant
        // this triggers the data write and event send
        .checkpoint(2)
        .assertEmptyEvent()
        .emptyCheckpoint(2)
        .end();
  }

  @Test
  public void testCheckpointFails() throws Exception {
    // reset the config option
    conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, 1L);
    preparePipeline(conf)
        // no data written and triggers checkpoint fails,
        // then we should revert the start instant
        .checkpoint(1)
        .assertEmptyEvent()
        .checkpointFails(1)
        .consume(TestData.DATA_SET_INSERT)
        //.checkpointThrows(2,
        //    "Timeout(1000ms) while waiting for instant initialize")
        // do not send the write event and fails the checkpoint,
        // behaves like the last checkpoint is successful.
        .checkpointFails(2)
        .end();
  }

  @Test
  public void testSubtaskFails() throws Exception {
    conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, 1L);
    // open the function and ingest data
    preparePipeline()
        .checkpoint(1)
        .assertEmptyEvent()
        .subTaskFails(0)
        .noCompleteInstant()
        // write a commit and check the result
        .consume(TestData.DATA_SET_INSERT)
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2)
        .checkWrittenData(EXPECTED1)
        // triggers task 0 failover, there is no pending instant that needs to recommit,
        // the task sends an empty bootstrap event to trigger initialization of a new instant.
        .subTaskFails(0, 0)
        .assertEmptyEvent()
        // rollback the last complete instant to inflight state, to simulate an instant commit failure
        // while executing the post action of a checkpoint success notification event, the whole job should then
        // trigger a failover.
        .rollbackLastCompleteInstantToInflight()
        .jobFailover()
        .assertNextEvent()
        .checkLastPendingInstantCompleted()
        .end();
  }

  @Test
  public void testAppendInsertAfterFailoverWithEmptyCheckpoint() throws Exception {
    // open the function and ingest data
    conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, 10_000L);
    conf.setString(FlinkOptions.OPERATION, "INSERT");
    preparePipeline()
        .assertEmptyDataFiles()
        // make an empty snapshot
        .checkpoint(1)
        .assertEmptyEvent()
        // trigger a partial failure
        .subTaskFails(0, 1)
        .assertNextEvent()
        // make sure coordinator send an ack event to unblock the writers.
        .assertNextSubTaskEvent()
        // write a set of data and check the result.
        .consume(TestData.DATA_SET_INSERT)
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2)
        .checkWrittenData(EXPECTED1)
        .end();
  }

  // Only when Job level fails with INSERT operationType can we roll back the unfinished instant.
  // Task level failed retry, we should reuse the unfinished Instant with INSERT operationType
  @Test
  public void testPartialFailover() throws Exception {
    conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, 1L);
    conf.setString(FlinkOptions.OPERATION, "INSERT");
    // open the function and ingest data
    preparePipeline()
        // triggers subtask failure for multiple times to simulate partial failover, for partial over,
        // we allow the task to reuse the pending instant for data flushing, no metadata event should be sent
        .subTaskFails(0, 1)
        .assertNoEvent()
        // the subtask reuses the pending instant
        .checkpoint(3)
        .assertNextEvent()
        // if the write task can not fetch any pending instant when starts up(the coordinator restarts),
        // it will send an event to the coordinator
        .restartCoordinator()
        .subTaskFails(0, 2)
        // the subtask can not fetch the instant to write until a new instant is initialized
        .checkpointThrows(4, "Timeout(1000ms) while waiting for instant initialize")
        .assertEmptyEvent()
        .subTaskFails(0, 3)
        // the last checkpoint instant was rolled back by subTaskFails(0, 2)
        // with EAGER cleaning strategy
        .assertNoEvent()
        .checkpoint(4)
        .assertNextEvent()
        .subTaskFails(0, 4)
        // the last checkpoint instant can not be rolled back by subTaskFails(0, 4) with INSERT write operationType
        // because last data has been snapshot by checkpoint complete but instant has not been committed
        // so we need re-commit it
        .assertEmptyEvent()
        .end();
  }

  @Test
  public void testInsert() throws Exception {
    // open the function and ingest data
    preparePipeline()
        .consume(TestData.DATA_SET_INSERT)
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .checkWrittenData(EXPECTED1)
        .end();
  }

  @Test
  public void testInsertDuplicates() throws Exception {
    // reset the config option
    conf.setBoolean(FlinkOptions.PRE_COMBINE, true);
    preparePipeline(conf)
        .consume(TestData.DATA_SET_INSERT_DUPLICATES)
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .checkWrittenData(EXPECTED3, 1)
        // insert duplicates again
        .consume(TestData.DATA_SET_INSERT_DUPLICATES)
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2)
        .checkWrittenData(EXPECTED3, 1)
        .end();
  }

  @Test
  public void testUpsert() throws Exception {
    // open the function and ingest data
    preparePipeline()
        .consume(TestData.DATA_SET_INSERT)
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        // upsert another data buffer
        .consume(TestData.DATA_SET_UPDATE_INSERT)
        // the data is not flushed yet
        .checkWrittenData(EXPECTED1)
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2)
        .checkWrittenData(EXPECTED2)
        .end();
  }

  @Test
  public void testUpsertWithDelete() throws Exception {
    // open the function and ingest data
    preparePipeline()
        .consume(TestData.DATA_SET_INSERT)
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .consume(TestData.DATA_SET_UPDATE_DELETE)
        .checkWrittenData(EXPECTED1)
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2)
        .checkWrittenData(getUpsertWithDeleteExpected())
        .end();
  }

  @Test
  public void testInsertWithMiniBatches() throws Exception {
    // reset the config option
    conf.setDouble(FlinkOptions.WRITE_BATCH_SIZE, 0.0008); // 839 bytes batch size

    Map<String, String> expected = getMiniBatchExpected();

    preparePipeline(conf)
        // record (operation: 'I') is 304 bytes and record (operation: 'U') is 352 bytes.
        // so 3 records expect to trigger a mini-batch write
        .consume(TestData.DATA_SET_INSERT_DUPLICATES)
        .assertDataBuffer(1, 2)
        .checkpoint(1)
        .allDataFlushed()
        .handleEvents(2)
        .checkpointComplete(1)
        .checkWrittenData(expected, 1)
        .consume(TestData.DATA_SET_INSERT_DUPLICATES)
        .checkpoint(2)
        .handleEvents(2)
        .checkpointComplete(2)
        .checkWrittenData(expected, 1)
        .end();
  }

  @Test
  public void testInsertWithDeduplication() throws Exception {
    // reset the config option
    conf.setDouble(FlinkOptions.WRITE_BATCH_SIZE, 0.0008); // 839 bytes batch size
    conf.setBoolean(FlinkOptions.PRE_COMBINE, true);

    Map<String, String> expected = new HashMap<>();
    expected.put("par1", "[id1,par1,id1,Danny,23,4,par1]");

    preparePipeline(conf)
        // record (operation: 'I') is 304 bytes and record (operation: 'U') is 352 bytes.
        // so 3 records expect to trigger a mini-batch write
        .consume(TestData.DATA_SET_INSERT_SAME_KEY)
        .assertDataBuffer(1, 2)
        .checkpoint(1)
        .allDataFlushed()
        .handleEvents(2)
        .checkpointComplete(1)
        .checkWrittenData(expected, 1)
        .consume(TestData.DATA_SET_INSERT_SAME_KEY)
        .checkpoint(2)
        .handleEvents(2)
        .checkpointComplete(2)
        .checkWrittenData(expected, 1)
        .end();
  }

  @Test
  public void testInsertAppendMode() throws Exception {
    conf.setString(FlinkOptions.OPERATION, "insert");
    preparePipeline()
        // Each record is 208 bytes. so 4 records expect to trigger a mini-batch write
        .consume(TestData.DATA_SET_INSERT_SAME_KEY)
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .checkWrittenAllData(EXPECTED4, 1)
        .consume(TestData.DATA_SET_INSERT_SAME_KEY)
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2)
        .checkWrittenDataCOW(EXPECTED5)
        .end();
  }

  /**
   * The test is almost same with {@link #testInsertWithSmallBufferSize} except that
   * it is with insert clustering mode.
   */
  @Test
  public void testInsertClustering() throws Exception {
    // reset the config option
    conf.setString(FlinkOptions.OPERATION, "insert");
    conf.setBoolean(FlinkOptions.INSERT_CLUSTER, true);
    conf.setDouble(FlinkOptions.WRITE_TASK_MAX_SIZE, 200.0008); // 839 bytes buffer size

    TestWriteMergeOnRead.TestHarness.instance()
        // record (operation: 'I') is 304 bytes and record (operation: 'U') is 352 bytes.
        // so 3 records expect to trigger a mini-batch write
        // flush the max size bucket once at a time.
        .preparePipeline(tempFile, conf)
        .consume(TestData.DATA_SET_INSERT_SAME_KEY)
        .assertDataBuffer(1, 2)
        .checkpoint(1)
        .allDataFlushed()
        .handleEvents(2)
        .checkpointComplete(1)
        .checkWrittenData(EXPECTED4, 1)
        // insert duplicates again
        .consume(TestData.DATA_SET_INSERT_SAME_KEY)
        .checkpoint(2)
        .handleEvents(2)
        .checkpointComplete(2)
        .checkWrittenDataCOW(EXPECTED5)
        .end();
  }

  @Test
  public void testInsertAsyncClustering() throws Exception {
    // reset the config option
    conf.setString(FlinkOptions.OPERATION, "insert");
    conf.setBoolean(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, true);
    conf.setBoolean(FlinkOptions.CLUSTERING_ASYNC_ENABLED, true);
    conf.setInteger(FlinkOptions.CLUSTERING_DELTA_COMMITS, 1);

    preparePipeline()
        .consume(TestData.DATA_SET_INSERT_SAME_KEY)
        .checkpoint(1)
        .handleEvents(1)
        .checkpointComplete(1)
        .checkWrittenData(EXPECTED4, 1)
        // insert duplicates again
        .consume(TestData.DATA_SET_INSERT_SAME_KEY)
        .checkpoint(2)
        .handleEvents(1)
        .checkpointComplete(2)
        .checkWrittenDataCOW(EXPECTED5)
        .end();
  }

  @Test
  public void testInsertWithSmallBufferSize() throws Exception {
    // reset the config option
    conf.setDouble(FlinkOptions.WRITE_TASK_MAX_SIZE, 200.0008); // 839 bytes buffer size

    Map<String, String> expected = getMiniBatchExpected();

    preparePipeline(conf)
        // record (operation: 'I') is 304 bytes and record (operation: 'U') is 352 bytes.
        // so 3 records expect to trigger a mini-batch write
        // flush the max size bucket once at a time.
        .consume(TestData.DATA_SET_INSERT_DUPLICATES)
        .assertDataBuffer(1, 2)
        .checkpoint(1)
        .allDataFlushed()
        .handleEvents(2)
        .checkpointComplete(1)
        .checkWrittenData(expected, 1)
        // insert duplicates again
        .consume(TestData.DATA_SET_INSERT_DUPLICATES)
        .checkpoint(2)
        .handleEvents(2)
        .checkpointComplete(2)
        // Same the original base file content.
        .checkWrittenData(expected, 1)
        .end();
  }

  @Test
  public void testCommitOnEmptyBatch() throws Exception {
    // reset the config option
    conf.setBoolean(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), true);

    preparePipeline()
        .consume(TestData.DATA_SET_INSERT)
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .checkCompletedInstantCount(1)
        // Do checkpoint without data consumption
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2)
        // The instant is committed successfully
        .checkCompletedInstantCount(2)
        // Continue to consume data
        .consume(TestData.DATA_SET_UPDATE_INSERT)
        .checkWrittenData(EXPECTED1)
        .checkpoint(3)
        .assertNextEvent()
        .checkpointComplete(3)
        .checkCompletedInstantCount(3)
        // Commit the data and check after an empty batch
        .checkWrittenData(EXPECTED2)
        // Do checkpoint without data consumption
        .checkpoint(4)
        .assertNextEvent()
        .checkpointComplete(4)
        .checkCompletedInstantCount(4)
        .checkWrittenData(EXPECTED2)
        .end();
  }

  protected Map<String, String> getMiniBatchExpected() {
    Map<String, String> expected = new HashMap<>();
    // the last 2 lines are merged
    expected.put("par1", "["
        + "id1,par1,id1,Danny,23,1,par1, "
        + "id1,par1,id1,Danny,23,1,par1, "
        + "id1,par1,id1,Danny,23,1,par1" + "]");
    return expected;
  }

  protected Map<String, String> getUpsertWithDeleteExpected() {
    Map<String, String> expected = new HashMap<>();
    // id3, id5 were deleted and id9 is ignored
    expected.put("par1", "[id1,par1,id1,Danny,24,1,par1, id2,par1,id2,Stephen,34,2,par1]");
    expected.put("par2", "[id4,par2,id4,Fabian,31,4,par2]");
    expected.put("par3", "[id6,par3,id6,Emma,20,6,par3]");
    expected.put("par4", "[id7,par4,id7,Bob,44,7,par4, id8,par4,id8,Han,56,8,par4]");
    return expected;
  }

  protected Map<String, String> getExpectedBeforeCheckpointComplete() {
    return EXPECTED2;
  }

  @Test
  public void testIndexStateBootstrap() throws Exception {
    // open the function and ingest data
    preparePipeline()
        .consume(TestData.DATA_SET_INSERT)
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .checkWrittenData(EXPECTED1, 4)
        .end();

    // reset the config option
    conf.setBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, true);
    validateIndexLoaded();
  }

  protected void validateIndexLoaded() throws Exception {
    preparePipeline(conf)
        .consume(TestData.DATA_SET_UPDATE_INSERT)
        .checkIndexLoaded(
            new HoodieKey("id1", "par1"),
            new HoodieKey("id2", "par1"),
            new HoodieKey("id3", "par2"),
            new HoodieKey("id4", "par2"),
            new HoodieKey("id5", "par3"),
            new HoodieKey("id6", "par3"),
            new HoodieKey("id7", "par4"),
            new HoodieKey("id8", "par4"),
            new HoodieKey("id9", "par3"),
            new HoodieKey("id10", "par4"),
            new HoodieKey("id11", "par4"))
        .checkpoint(1)
        .assertBootstrapped()
        .assertNextEvent()
        .checkWrittenData(getExpectedBeforeCheckpointComplete())
        .checkpointComplete(1)
        .checkWrittenData(EXPECTED2)
        .end();
  }

  @Test
  public void testWriteExactlyOnce() throws Exception {
    // reset the config option
    conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, 1L);
    conf.setDouble(FlinkOptions.WRITE_TASK_MAX_SIZE, 200.0006); // 630 bytes buffer size
    preparePipeline(conf)
        .consume(TestData.DATA_SET_INSERT)
        .emptyEventBuffer()
        .checkpoint(1)
        .assertConfirming()
        .handleEvents(4)
        .checkpointComplete(1)
        .consume(TestData.DATA_SET_INSERT)
        .assertNotConfirming()
        .checkpoint(2)
        .assertConsumeThrows(TestData.DATA_SET_INSERT,
            "Timeout(1000ms) while waiting for instant initialize")
        .end();
  }

  // case1: txn2's time range is involved in txn1
  //      |----------- txn1 -----------|
  //              | ----- txn2 ----- |
  @Test
  public void testWriteMultiWriterInvolved() throws Exception {
    conf.setString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name());
    conf.setString(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    conf.setBoolean(FlinkOptions.PRE_COMBINE, true);

    TestHarness pipeline1 = preparePipeline(conf)
        .consume(TestData.DATA_SET_INSERT_DUPLICATES)
        .assertEmptyDataFiles();
    // now start pipeline2 and commit the txn
    Configuration conf2 = conf.clone();
    conf2.setString(FlinkOptions.WRITE_CLIENT_ID, "2");
    preparePipeline(conf2)
        .consume(TestData.DATA_SET_INSERT_DUPLICATES)
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .checkWrittenData(EXPECTED3, 1)
        .end();
    // step to commit the 2nd txn, should throw exception
    // for concurrent modification of same fileGroups
    pipeline1.checkpoint(1)
        .assertNextEvent()
        .checkpointCompleteThrows(1, HoodieWriteConflictException.class, "Cannot resolve conflicts");
    pipeline1.end();
  }

  // case2: txn2's time range has partial overlap with txn1
  //      |----------- txn1 -----------|
  //                       | ----- txn2 ----- |
  @Test
  public void testWriteMultiWriterPartialOverlapping() throws Exception {
    conf.setString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name());
    conf.setString(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    conf.setBoolean(FlinkOptions.PRE_COMBINE, true);
    TestHarness pipeline1 = null;
    TestHarness pipeline2 = null;

    try {
      pipeline1 = preparePipeline(conf)
          .consume(TestData.DATA_SET_INSERT_DUPLICATES)
          .assertEmptyDataFiles();
      // now start pipeline2 and suspend the txn commit
      Configuration conf2 = conf.clone();
      conf2.setString(FlinkOptions.WRITE_CLIENT_ID, "2");
      pipeline2 = preparePipeline(conf2)
          .consume(TestData.DATA_SET_INSERT_DUPLICATES)
          .assertEmptyDataFiles();

      // step to commit the 1st txn, should succeed
      pipeline1.checkpoint(1)
          .assertNextEvent()
          .checkpoint(1)
          .assertNextEvent()
          .checkpointComplete(1)
          .checkWrittenData(EXPECTED3, 1);

      // step to commit the 2nd txn, should throw exception
      // for concurrent modification of same fileGroups
      pipeline2.checkpoint(1)
          .assertNextEvent()
          .checkpointCompleteThrows(1, HoodieWriteConflictException.class, "Cannot resolve conflicts");
    } finally {
      if (pipeline1 != null) {
        pipeline1.end();
      }
      if (pipeline2 != null) {
        pipeline2.end();
      }
    }
  }

  @Test
  public void testReuseEmbeddedServer() throws IOException {
    conf.setInteger("hoodie.filesystem.view.remote.timeout.secs", 500);
    conf.setString("hoodie.metadata.enable","true");
    HoodieFlinkWriteClient writeClient = null;
    HoodieFlinkWriteClient writeClient2 = null;

    try {
      writeClient = FlinkWriteClients.createWriteClient(conf);
      FileSystemViewStorageConfig viewStorageConfig = writeClient.getConfig().getViewStorageConfig();

      assertSame(viewStorageConfig.getStorageType(), FileSystemViewStorageType.REMOTE_FIRST);

      // get another write client
      writeClient2 = FlinkWriteClients.createWriteClient(conf);
      assertSame(writeClient2.getConfig().getViewStorageConfig().getStorageType(), FileSystemViewStorageType.REMOTE_FIRST);
      assertEquals(viewStorageConfig.getRemoteViewServerPort(), writeClient2.getConfig().getViewStorageConfig().getRemoteViewServerPort());
      assertEquals(viewStorageConfig.getRemoteTimelineClientTimeoutSecs(), 500);
    } finally {
      if (writeClient != null) {
        writeClient.close();
      }
      if (writeClient2 != null) {
        writeClient2.close();
      }
    }
  }

  @Test
  public void testRollbackFailedWritesWithLazyCleanPolicy() throws Exception {
    conf.setString(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(), HoodieFailedWritesCleaningPolicy.LAZY.name());

    preparePipeline()
        .consume(TestData.DATA_SET_INSERT)
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .subTaskFails(0, 0)
        .assertEmptyEvent()
        .rollbackLastCompleteInstantToInflight()
        .jobFailover()
        .subTaskFails(0, 1)
        // the last checkpoint instant was not rolled back by subTaskFails(0, 1)
        // with LAZY cleaning strategy because clean action could roll back failed writes.
        .assertNextEvent()
        .end();
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private TestHarness preparePipeline() throws Exception {
    return preparePipeline(conf);
  }

  protected TestHarness preparePipeline(Configuration conf) throws Exception {
    return TestHarness.instance().preparePipeline(tempFile, conf);
  }

  protected HoodieTableType getTableType() {
    return HoodieTableType.COPY_ON_WRITE;
  }
}
