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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.utils.TestWriteBase;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
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

  /**
   * Override to have custom configuration.
   */
  protected void setUp(Configuration conf) {
    // for sub-class extension
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
    // open the function and ingest data
    preparePipeline()
        .checkpoint(1)
        .assertEmptyEvent()
        .subTaskFails(0)
        .noCompleteInstant()
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
    conf.setDouble(FlinkOptions.WRITE_BATCH_SIZE, 0.00008); // 839 bytes batch size

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
    conf.setDouble(FlinkOptions.WRITE_BATCH_SIZE, 0.00008); // 839 bytes batch size
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
    prepareInsertPipeline()
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
    conf.setDouble(FlinkOptions.WRITE_TASK_MAX_SIZE, 200.00008); // 839 bytes buffer size

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

    prepareInsertPipeline(conf)
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
    conf.setDouble(FlinkOptions.WRITE_TASK_MAX_SIZE, 200.00008); // 839 bytes buffer size

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
    conf.setDouble(FlinkOptions.WRITE_TASK_MAX_SIZE, 200.00006); // 630 bytes buffer size
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

  @Test
  public void testReuseEmbeddedServer() throws IOException {
    conf.setInteger("hoodie.filesystem.view.remote.timeout.secs", 500);
    HoodieFlinkWriteClient writeClient = StreamerUtil.createWriteClient(conf);
    FileSystemViewStorageConfig viewStorageConfig = writeClient.getConfig().getViewStorageConfig();

    assertSame(viewStorageConfig.getStorageType(), FileSystemViewStorageType.REMOTE_FIRST);

    // get another write client
    writeClient = StreamerUtil.createWriteClient(conf);
    assertSame(writeClient.getConfig().getViewStorageConfig().getStorageType(), FileSystemViewStorageType.REMOTE_FIRST);
    assertEquals(viewStorageConfig.getRemoteViewServerPort(), writeClient.getConfig().getViewStorageConfig().getRemoteViewServerPort());
    assertEquals(viewStorageConfig.getRemoteTimelineClientTimeoutSecs(), 500);
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

  protected TestHarness prepareInsertPipeline() throws Exception {
    return prepareInsertPipeline(conf);
  }

  protected TestHarness prepareInsertPipeline(Configuration conf) throws Exception {
    return TestHarness.instance().preparePipeline(tempFile, conf, true);
  }

  protected HoodieTableType getTableType() {
    return HoodieTableType.COPY_ON_WRITE;
  }
}
