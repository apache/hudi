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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.utils.StreamWriteFunctionWrapper;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.table.data.RowData;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for stream write.
 */
public class TestWriteCopyOnWrite {

  protected static final Map<String, String> EXPECTED1 = new HashMap<>();

  protected static final Map<String, String> EXPECTED2 = new HashMap<>();

  protected static final Map<String, String> EXPECTED3 = new HashMap<>();

  static {
    EXPECTED1.put("par1", "[id1,par1,id1,Danny,23,1,par1, id2,par1,id2,Stephen,33,2,par1]");
    EXPECTED1.put("par2", "[id3,par2,id3,Julian,53,3,par2, id4,par2,id4,Fabian,31,4,par2]");
    EXPECTED1.put("par3", "[id5,par3,id5,Sophia,18,5,par3, id6,par3,id6,Emma,20,6,par3]");
    EXPECTED1.put("par4", "[id7,par4,id7,Bob,44,7,par4, id8,par4,id8,Han,56,8,par4]");

    EXPECTED2.put("par1", "[id1,par1,id1,Danny,24,1,par1, id2,par1,id2,Stephen,34,2,par1]");
    EXPECTED2.put("par2", "[id3,par2,id3,Julian,54,3,par2, id4,par2,id4,Fabian,32,4,par2]");
    EXPECTED2.put("par3", "[id5,par3,id5,Sophia,18,5,par3, id6,par3,id6,Emma,20,6,par3, "
        + "id9,par3,id9,Jane,19,6,par3]");
    EXPECTED2.put("par4", "[id10,par4,id10,Ella,38,7,par4, id11,par4,id11,Phoebe,52,8,par4, "
        + "id7,par4,id7,Bob,44,7,par4, id8,par4,id8,Han,56,8,par4]");

    EXPECTED3.put("par1", "[id1,par1,id1,Danny,23,1,par1]");
  }

  protected Configuration conf;

  protected StreamWriteFunctionWrapper<RowData> funcWrapper;

  @TempDir
  File tempFile;

  @BeforeEach
  public void before() throws Exception {
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.TABLE_TYPE, getTableType().name());
    setUp(conf);
    this.funcWrapper = new StreamWriteFunctionWrapper<>(tempFile.getAbsolutePath(), conf);
  }

  /**
   * Override to have custom configuration.
   */
  protected void setUp(Configuration conf) {
    // for sub-class extension
  }

  @AfterEach
  public void after() throws Exception {
    funcWrapper.close();
  }

  @Test
  public void testCheckpoint() throws Exception {
    // open the function and ingest data
    funcWrapper.openFunction();
    for (RowData rowData : TestData.DATA_SET_INSERT) {
      funcWrapper.invoke(rowData);
    }

    // no checkpoint, so the coordinator does not accept any events
    assertTrue(
        funcWrapper.getEventBuffer().length == 1
            && funcWrapper.getEventBuffer()[0] == null, "The coordinator events buffer expect to be empty");

    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);

    String instant = lastPendingInstant();

    final OperatorEvent nextEvent = funcWrapper.getNextEvent();
    MatcherAssert.assertThat("The operator expect to send an event", nextEvent, instanceOf(WriteMetadataEvent.class));
    List<WriteStatus> writeStatuses = ((WriteMetadataEvent) nextEvent).getWriteStatuses();
    assertNotNull(writeStatuses);
    MatcherAssert.assertThat(writeStatuses.size(), is(4)); // write 4 partition files
    assertThat(writeStatuses.stream()
            .map(WriteStatus::getPartitionPath).sorted(Comparator.naturalOrder())
            .collect(Collectors.joining(",")),
        is("par1,par2,par3,par4"));

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    checkInstantState(REQUESTED, instant);
    funcWrapper.checkpointComplete(1);
    // the coordinator checkpoint commits the inflight instant.
    checkInstantState(HoodieInstant.State.COMPLETED, instant);

    // checkpoint for next round, no data input, so after the checkpoint,
    // there should not be REQUESTED Instant
    // this triggers the data write and event send
    funcWrapper.checkpointFunction(2);

    String instant2 = lastPendingInstant();
    assertNotEquals(instant, instant2);

    final OperatorEvent nextEvent2 = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent2, instanceOf(WriteMetadataEvent.class));
    List<WriteStatus> writeStatuses2 = ((WriteMetadataEvent) nextEvent2).getWriteStatuses();
    assertNotNull(writeStatuses2);
    assertThat(writeStatuses2.size(), is(0)); // write empty statuses

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent2);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    funcWrapper.checkpointComplete(2);
    // started a new instant already
    checkInflightInstant();
    checkInstantState(HoodieInstant.State.COMPLETED, instant);
  }

  @Test
  public void testCheckpointFails() throws Exception {
    // reset the config option
    conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, 1L);
    funcWrapper = new StreamWriteFunctionWrapper<>(tempFile.getAbsolutePath(), conf);
    // open the function and ingest data
    funcWrapper.openFunction();
    // no data written and triggers checkpoint fails,
    // then we should revert the start instant

    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);

    String instant = lastPendingInstant();
    assertNotNull(instant);

    final OperatorEvent nextEvent = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent, instanceOf(WriteMetadataEvent.class));
    List<WriteStatus> writeStatuses = ((WriteMetadataEvent) nextEvent).getWriteStatuses();
    assertNotNull(writeStatuses);
    assertThat(writeStatuses.size(), is(0)); // no data write

    // fails the checkpoint
    funcWrapper.checkpointFails(1);
    assertFalse(funcWrapper.getCoordinatorContext().isJobFailed(),
        "The last checkpoint was aborted, ignore the events");

    // the instant metadata should be reused
    checkInstantState(REQUESTED, instant);
    checkInstantState(HoodieInstant.State.COMPLETED, null);

    for (RowData rowData : TestData.DATA_SET_INSERT) {
      funcWrapper.invoke(rowData);
    }

    // this returns early because there is no inflight instant
    assertDoesNotThrow(() -> funcWrapper.checkpointFunction(2),
        "The stream writer reuse the last instant time when waiting for the last instant commit timeout");
    // do not send the write event and fails the checkpoint,
    // behaves like the last checkpoint is successful.
    funcWrapper.checkpointFails(2);
  }

  @Test
  public void testSubtaskFails() throws Exception {
    // open the function and ingest data
    funcWrapper.openFunction();
    // no data written and triggers checkpoint fails,
    // then we should revert the start instant

    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);
    funcWrapper.getNextEvent();

    String instant1 = lastPendingInstant();
    assertNotNull(instant1);

    // fails the subtask
    funcWrapper.subTaskFails(0);

    String instant2 = lastPendingInstant();
    assertNotEquals(instant2, instant1, "The previous instant should be rolled back when starting new instant");

    checkInstantState(HoodieInstant.State.COMPLETED, null);
  }

  @Test
  public void testInsert() throws Exception {
    // open the function and ingest data
    funcWrapper.openFunction();
    for (RowData rowData : TestData.DATA_SET_INSERT) {
      funcWrapper.invoke(rowData);
    }

    assertEmptyDataFiles();
    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);

    String instant = lastPendingInstant();

    final OperatorEvent nextEvent = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent, instanceOf(WriteMetadataEvent.class));

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    checkInstantState(REQUESTED, instant);
    funcWrapper.checkpointComplete(1);
    checkWrittenData(tempFile, EXPECTED1);
    // the coordinator checkpoint commits the inflight instant.
    checkInstantState(HoodieInstant.State.COMPLETED, instant);
    checkWrittenData(tempFile, EXPECTED1);
  }

  @Test
  public void testInsertDuplicates() throws Exception {
    // reset the config option
    conf.setBoolean(FlinkOptions.INSERT_DROP_DUPS, true);
    funcWrapper = new StreamWriteFunctionWrapper<>(tempFile.getAbsolutePath(), conf);

    // open the function and ingest data
    funcWrapper.openFunction();
    for (RowData rowData : TestData.DATA_SET_INSERT_DUPLICATES) {
      funcWrapper.invoke(rowData);
    }

    assertEmptyDataFiles();
    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);

    OperatorEvent nextEvent = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent, instanceOf(WriteMetadataEvent.class));

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    funcWrapper.checkpointComplete(1);

    checkWrittenData(tempFile, EXPECTED3, 1);

    // insert duplicates again
    for (RowData rowData : TestData.DATA_SET_INSERT_DUPLICATES) {
      funcWrapper.invoke(rowData);
    }

    funcWrapper.checkpointFunction(2);

    nextEvent = funcWrapper.getNextEvent();
    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    funcWrapper.checkpointComplete(2);

    checkWrittenData(tempFile, EXPECTED3, 1);
  }

  @Test
  public void testUpsert() throws Exception {
    // open the function and ingest data
    funcWrapper.openFunction();
    for (RowData rowData : TestData.DATA_SET_INSERT) {
      funcWrapper.invoke(rowData);
    }

    assertEmptyDataFiles();
    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);

    OperatorEvent nextEvent = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent, instanceOf(WriteMetadataEvent.class));

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    funcWrapper.checkpointComplete(1);

    // upsert another data buffer
    for (RowData rowData : TestData.DATA_SET_UPDATE_INSERT) {
      funcWrapper.invoke(rowData);
    }
    // the data is not flushed yet
    checkWrittenData(tempFile, EXPECTED1);
    // this triggers the data write and event send
    funcWrapper.checkpointFunction(2);

    String instant = lastPendingInstant();

    nextEvent = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent, instanceOf(WriteMetadataEvent.class));

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    checkInstantState(REQUESTED, instant);
    funcWrapper.checkpointComplete(2);
    // the coordinator checkpoint commits the inflight instant.
    checkInstantState(HoodieInstant.State.COMPLETED, instant);
    checkWrittenData(tempFile, EXPECTED2);
  }

  @Test
  public void testUpsertWithDelete() throws Exception {
    // open the function and ingest data
    funcWrapper.openFunction();
    for (RowData rowData : TestData.DATA_SET_INSERT) {
      funcWrapper.invoke(rowData);
    }

    assertEmptyDataFiles();
    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);

    OperatorEvent nextEvent = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent, instanceOf(WriteMetadataEvent.class));

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    funcWrapper.checkpointComplete(1);

    // upsert another data buffer
    for (RowData rowData : TestData.DATA_SET_UPDATE_DELETE) {
      funcWrapper.invoke(rowData);
    }
    // the data is not flushed yet
    checkWrittenData(tempFile, EXPECTED1);
    // this triggers the data write and event send
    funcWrapper.checkpointFunction(2);

    String instant = lastPendingInstant();

    nextEvent = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent, instanceOf(WriteMetadataEvent.class));

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    checkInstantState(REQUESTED, instant);
    funcWrapper.checkpointComplete(2);
    // the coordinator checkpoint commits the inflight instant.
    checkInstantState(HoodieInstant.State.COMPLETED, instant);

    Map<String, String> expected = getUpsertWithDeleteExpected();
    checkWrittenData(tempFile, expected);
  }

  @Test
  public void testInsertWithMiniBatches() throws Exception {
    // reset the config option
    conf.setDouble(FlinkOptions.WRITE_BATCH_SIZE, 0.0008); // 839 bytes batch size
    funcWrapper = new StreamWriteFunctionWrapper<>(tempFile.getAbsolutePath(), conf);

    // open the function and ingest data
    funcWrapper.openFunction();
    // record (operation: 'I') is 304 bytes and record (operation: 'U') is 352 bytes.
    // so 3 records expect to trigger a mini-batch write
    for (RowData rowData : TestData.DATA_SET_INSERT_DUPLICATES) {
      funcWrapper.invoke(rowData);
    }

    Map<String, List<HoodieRecord>> dataBuffer = funcWrapper.getDataBuffer();
    assertThat("Should have 1 data bucket", dataBuffer.size(), is(1));
    assertThat("2 records expect to flush out as a mini-batch",
        dataBuffer.values().stream().findFirst().map(List::size).orElse(-1),
        is(2));

    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);
    dataBuffer = funcWrapper.getDataBuffer();
    assertThat("All data should be flushed out", dataBuffer.size(), is(0));

    final OperatorEvent event1 = funcWrapper.getNextEvent(); // remove the first event first
    final OperatorEvent event2 = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", event2, instanceOf(WriteMetadataEvent.class));

    funcWrapper.getCoordinator().handleEventFromOperator(0, event1);
    funcWrapper.getCoordinator().handleEventFromOperator(0, event2);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    String instant = lastPendingInstant();

    funcWrapper.checkpointComplete(1);

    Map<String, String> expected = getMiniBatchExpected();
    checkWrittenData(tempFile, expected, 1);

    // started a new instant already
    checkInflightInstant();
    checkInstantState(HoodieInstant.State.COMPLETED, instant);

    // insert duplicates again
    for (RowData rowData : TestData.DATA_SET_INSERT_DUPLICATES) {
      funcWrapper.invoke(rowData);
    }

    funcWrapper.checkpointFunction(2);

    final OperatorEvent event3 = funcWrapper.getNextEvent(); // remove the first event first
    final OperatorEvent event4 = funcWrapper.getNextEvent();
    funcWrapper.getCoordinator().handleEventFromOperator(0, event3);
    funcWrapper.getCoordinator().handleEventFromOperator(0, event4);
    funcWrapper.checkpointComplete(2);

    // Same the original base file content.
    checkWrittenData(tempFile, expected, 1);
  }

  @Test
  public void testInsertWithDeduplication() throws Exception {
    // reset the config option
    conf.setDouble(FlinkOptions.WRITE_BATCH_SIZE, 0.0008); // 839 bytes batch size
    conf.setBoolean(FlinkOptions.INSERT_DROP_DUPS, true);
    funcWrapper = new StreamWriteFunctionWrapper<>(tempFile.getAbsolutePath(), conf);

    // open the function and ingest data
    funcWrapper.openFunction();
    // record (operation: 'I') is 304 bytes and record (operation: 'U') is 352 bytes.
    // so 3 records expect to trigger a mini-batch write
    for (RowData rowData : TestData.DATA_SET_INSERT_SAME_KEY) {
      funcWrapper.invoke(rowData);
    }

    Map<String, List<HoodieRecord>> dataBuffer = funcWrapper.getDataBuffer();
    assertThat("Should have 1 data bucket", dataBuffer.size(), is(1));
    assertThat("2 records expect to flush out as a mini-batch",
        dataBuffer.values().stream().findFirst().map(List::size).orElse(-1),
        is(2));

    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);
    dataBuffer = funcWrapper.getDataBuffer();
    assertThat("All data should be flushed out", dataBuffer.size(), is(0));

    final OperatorEvent event1 = funcWrapper.getNextEvent(); // remove the first event first
    final OperatorEvent event2 = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", event2, instanceOf(WriteMetadataEvent.class));

    funcWrapper.getCoordinator().handleEventFromOperator(0, event1);
    funcWrapper.getCoordinator().handleEventFromOperator(0, event2);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    String instant = lastPendingInstant();

    funcWrapper.checkpointComplete(1);

    Map<String, String> expected = new HashMap<>();
    expected.put("par1", "[id1,par1,id1,Danny,23,4,par1]");

    checkWrittenData(tempFile, expected, 1);

    // started a new instant already
    checkInflightInstant();
    checkInstantState(HoodieInstant.State.COMPLETED, instant);

    // insert duplicates again
    for (RowData rowData : TestData.DATA_SET_INSERT_SAME_KEY) {
      funcWrapper.invoke(rowData);
    }

    funcWrapper.checkpointFunction(2);

    final OperatorEvent event3 = funcWrapper.getNextEvent(); // remove the first event first
    final OperatorEvent event4 = funcWrapper.getNextEvent();
    funcWrapper.getCoordinator().handleEventFromOperator(0, event3);
    funcWrapper.getCoordinator().handleEventFromOperator(0, event4);
    funcWrapper.checkpointComplete(2);

    // Same the original base file content.
    checkWrittenData(tempFile, expected, 1);
  }

  @Test
  public void testInsertAppendMode() throws Exception {
    // reset the config option
    conf.setDouble(FlinkOptions.WRITE_BATCH_SIZE, 0.0006); // 630 bytes batch size
    conf.setString(FlinkOptions.OPERATION, "insert");
    funcWrapper = new StreamWriteFunctionWrapper<>(tempFile.getAbsolutePath(), conf);

    // open the function and ingest data
    funcWrapper.openFunction();
    // Each record is 208 bytes. so 4 records expect to trigger a mini-batch write
    for (RowData rowData : TestData.DATA_SET_INSERT_SAME_KEY) {
      funcWrapper.invoke(rowData);
    }

    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);
    Map<String, List<HoodieRecord>> dataBuffer = funcWrapper.getDataBuffer();
    assertThat("All data should be flushed out", dataBuffer.size(), is(0));

    final OperatorEvent event1 = funcWrapper.getNextEvent(); // remove the first event first
    final OperatorEvent event2 = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", event2, instanceOf(WriteMetadataEvent.class));

    funcWrapper.getCoordinator().handleEventFromOperator(0, event1);
    funcWrapper.getCoordinator().handleEventFromOperator(0, event2);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    String instant = lastPendingInstant();

    funcWrapper.checkpointComplete(1);

    Map<String, String> expected = new HashMap<>();

    expected.put("par1", "["
        + "id1,par1,id1,Danny,23,0,par1, "
        + "id1,par1,id1,Danny,23,1,par1, "
        + "id1,par1,id1,Danny,23,2,par1, "
        + "id1,par1,id1,Danny,23,3,par1, "
        + "id1,par1,id1,Danny,23,4,par1]");

    TestData.checkWrittenAllData(tempFile, expected, 1);

    // started a new instant already
    checkInflightInstant();
    checkInstantState(HoodieInstant.State.COMPLETED, instant);

    // insert duplicates again
    for (RowData rowData : TestData.DATA_SET_INSERT_SAME_KEY) {
      funcWrapper.invoke(rowData);
    }

    funcWrapper.checkpointFunction(2);

    final OperatorEvent event3 = funcWrapper.getNextEvent(); // remove the first event first
    final OperatorEvent event4 = funcWrapper.getNextEvent();
    final OperatorEvent event5 = funcWrapper.getNextEvent();
    funcWrapper.getCoordinator().handleEventFromOperator(0, event3);
    funcWrapper.getCoordinator().handleEventFromOperator(0, event4);
    funcWrapper.getCoordinator().handleEventFromOperator(0, event5);
    funcWrapper.checkpointComplete(2);

    expected.put("par1", "["
        + "id1,par1,id1,Danny,23,0,par1, "
        + "id1,par1,id1,Danny,23,0,par1, "
        + "id1,par1,id1,Danny,23,1,par1, "
        + "id1,par1,id1,Danny,23,1,par1, "
        + "id1,par1,id1,Danny,23,2,par1, "
        + "id1,par1,id1,Danny,23,2,par1, "
        + "id1,par1,id1,Danny,23,3,par1, "
        + "id1,par1,id1,Danny,23,3,par1, "
        + "id1,par1,id1,Danny,23,4,par1, "
        + "id1,par1,id1,Danny,23,4,par1]");
    TestData.checkWrittenAllData(tempFile, expected, 1);
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
    funcWrapper = new StreamWriteFunctionWrapper<>(tempFile.getAbsolutePath(), conf);

    // open the function and ingest data
    funcWrapper.openFunction();
    // record (operation: 'I') is 304 bytes and record (operation: 'U') is 352 bytes.
    // so 3 records expect to trigger a mini-batch write
    // flush the max size bucket once at a time.
    for (RowData rowData : TestData.DATA_SET_INSERT_SAME_KEY) {
      funcWrapper.invoke(rowData);
    }

    Map<String, List<HoodieRecord>> dataBuffer = funcWrapper.getDataBuffer();
    assertThat("Should have 1 data bucket", dataBuffer.size(), is(1));
    assertThat("2 records expect to flush out as a mini-batch",
        dataBuffer.values().stream().findFirst().map(List::size).orElse(-1),
        is(2));

    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);
    dataBuffer = funcWrapper.getDataBuffer();
    assertThat("All data should be flushed out", dataBuffer.size(), is(0));

    for (int i = 0; i < 2; i++) {
      final OperatorEvent event = funcWrapper.getNextEvent(); // remove the first event first
      assertThat("The operator expect to send an event", event, instanceOf(WriteMetadataEvent.class));
      funcWrapper.getCoordinator().handleEventFromOperator(0, event);
    }
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    String instant = lastPendingInstant();

    funcWrapper.checkpointComplete(1);

    Map<String, String> expected = new HashMap<>();

    expected.put("par1", "["
        + "id1,par1,id1,Danny,23,0,par1, "
        + "id1,par1,id1,Danny,23,1,par1, "
        + "id1,par1,id1,Danny,23,2,par1, "
        + "id1,par1,id1,Danny,23,3,par1, "
        + "id1,par1,id1,Danny,23,4,par1]");
    checkWrittenData(tempFile, expected, 1);

    // started a new instant already
    checkInflightInstant();
    checkInstantState(HoodieInstant.State.COMPLETED, instant);

    // insert duplicates again
    for (RowData rowData : TestData.DATA_SET_INSERT_SAME_KEY) {
      funcWrapper.invoke(rowData);
    }

    funcWrapper.checkpointFunction(2);

    for (int i = 0; i < 2; i++) {
      final OperatorEvent event = funcWrapper.getNextEvent(); // remove the first event first
      funcWrapper.getCoordinator().handleEventFromOperator(0, event);
    }

    funcWrapper.checkpointComplete(2);

    // same with the original base file content.
    Map<String, List<String>> expected2 = new HashMap<>();
    expected2.put("par1", Arrays.asList(
        "id1,par1,id1,Danny,23,0,par1",
        "id1,par1,id1,Danny,23,0,par1",
        "id1,par1,id1,Danny,23,1,par1",
        "id1,par1,id1,Danny,23,1,par1",
        "id1,par1,id1,Danny,23,2,par1",
        "id1,par1,id1,Danny,23,2,par1",
        "id1,par1,id1,Danny,23,3,par1",
        "id1,par1,id1,Danny,23,3,par1",
        "id1,par1,id1,Danny,23,4,par1",
        "id1,par1,id1,Danny,23,4,par1"));

    // Same the original base file content.
    TestData.checkWrittenFullData(tempFile, expected2);
  }

  @Test
  public void testInsertWithSmallBufferSize() throws Exception {
    // reset the config option
    conf.setDouble(FlinkOptions.WRITE_TASK_MAX_SIZE, 200.0008); // 839 bytes buffer size
    funcWrapper = new StreamWriteFunctionWrapper<>(tempFile.getAbsolutePath(), conf);

    // open the function and ingest data
    funcWrapper.openFunction();
    // record (operation: 'I') is 304 bytes and record (operation: 'U') is 352 bytes.
    // so 3 records expect to trigger a mini-batch write
    // flush the max size bucket once at a time.
    for (RowData rowData : TestData.DATA_SET_INSERT_DUPLICATES) {
      funcWrapper.invoke(rowData);
    }

    Map<String, List<HoodieRecord>> dataBuffer = funcWrapper.getDataBuffer();
    assertThat("Should have 1 data bucket", dataBuffer.size(), is(1));
    assertThat("2 records expect to flush out as a mini-batch",
        dataBuffer.values().stream().findFirst().map(List::size).orElse(-1),
        is(2));

    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);
    dataBuffer = funcWrapper.getDataBuffer();
    assertThat("All data should be flushed out", dataBuffer.size(), is(0));

    for (int i = 0; i < 2; i++) {
      final OperatorEvent event = funcWrapper.getNextEvent(); // remove the first event first
      assertThat("The operator expect to send an event", event, instanceOf(WriteMetadataEvent.class));
      funcWrapper.getCoordinator().handleEventFromOperator(0, event);
    }
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    String instant = lastPendingInstant();

    funcWrapper.checkpointComplete(1);

    Map<String, String> expected = getMiniBatchExpected();
    checkWrittenData(tempFile, expected, 1);

    // started a new instant already
    checkInflightInstant();
    checkInstantState(HoodieInstant.State.COMPLETED, instant);

    // insert duplicates again
    for (RowData rowData : TestData.DATA_SET_INSERT_DUPLICATES) {
      funcWrapper.invoke(rowData);
    }

    funcWrapper.checkpointFunction(2);

    for (int i = 0; i < 2; i++) {
      final OperatorEvent event = funcWrapper.getNextEvent(); // remove the first event first
      funcWrapper.getCoordinator().handleEventFromOperator(0, event);
    }

    funcWrapper.checkpointComplete(2);

    // Same the original base file content.
    checkWrittenData(tempFile, expected, 1);
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
    funcWrapper.openFunction();
    for (RowData rowData : TestData.DATA_SET_INSERT) {
      funcWrapper.invoke(rowData);
    }

    assertEmptyDataFiles();
    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);

    OperatorEvent nextEvent = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent, instanceOf(WriteMetadataEvent.class));

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    funcWrapper.checkpointComplete(1);

    // the data is not flushed yet
    checkWrittenData(tempFile, EXPECTED1);

    // reset the config option
    conf.setBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, true);
    funcWrapper = new StreamWriteFunctionWrapper<>(tempFile.getAbsolutePath(), conf);

    // upsert another data buffer
    funcWrapper.openFunction();
    for (RowData rowData : TestData.DATA_SET_UPDATE_INSERT) {
      funcWrapper.invoke(rowData);
    }

    checkIndexLoaded(
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
        new HoodieKey("id11", "par4"));

    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);

    assertTrue(funcWrapper.isAlreadyBootstrap());

    String instant = lastPendingInstant();

    nextEvent = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent, instanceOf(WriteMetadataEvent.class));

    Map<String, String> expected = getExpectedBeforeCheckpointComplete();
    checkWrittenData(tempFile, expected);

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    checkInstantState(REQUESTED, instant);

    funcWrapper.checkpointComplete(1);
    // the coordinator checkpoint commits the inflight instant.
    checkInstantState(HoodieInstant.State.COMPLETED, instant);
    checkWrittenData(tempFile, EXPECTED2);
  }

  @Test
  public void testWriteExactlyOnce() throws Exception {
    // reset the config option
    conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, 1L);
    conf.setDouble(FlinkOptions.WRITE_TASK_MAX_SIZE, 200.0006); // 630 bytes buffer size
    funcWrapper = new StreamWriteFunctionWrapper<>(tempFile.getAbsolutePath(), conf);

    // open the function and ingest data

    funcWrapper.openFunction();
    for (RowData rowData : TestData.DATA_SET_INSERT) {
      funcWrapper.invoke(rowData);
    }

    // no checkpoint, so the coordinator does not accept any events
    assertTrue(
        funcWrapper.getEventBuffer().length == 1
            && funcWrapper.getEventBuffer()[0] == null, "The coordinator events buffer expect to be empty");

    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);
    assertTrue(funcWrapper.isConforming(), "The write function should be waiting for the instant to commit");

    for (int i = 0; i < 4; i++) {
      final OperatorEvent event = funcWrapper.getNextEvent(); // remove the first event first
      assertThat("The operator expect to send an event", event, instanceOf(WriteMetadataEvent.class));
      funcWrapper.getCoordinator().handleEventFromOperator(0, event);
    }

    funcWrapper.checkpointComplete(1);

    for (RowData rowData : TestData.DATA_SET_INSERT) {
      funcWrapper.invoke(rowData);
    }

    assertFalse(funcWrapper.isConforming(), "The write function should finish waiting for the instant to commit");

    // checkpoint for the next round
    funcWrapper.checkpointFunction(2);

    assertDoesNotThrow(() -> {
      for (RowData rowData : TestData.DATA_SET_INSERT) {
        funcWrapper.invoke(rowData);
      }
    }, "The stream writer reuse the last instant time when waiting for the last instant commit timeout");
  }

  @Test
  public void testReuseEmbeddedServer() throws IOException {
    HoodieFlinkWriteClient writeClient = StreamerUtil.createWriteClient(conf);
    FileSystemViewStorageConfig viewStorageConfig = writeClient.getConfig().getViewStorageConfig();

    assertSame(viewStorageConfig.getStorageType(), FileSystemViewStorageType.REMOTE_FIRST);

    // get another write client
    writeClient = StreamerUtil.createWriteClient(conf);
    assertSame(writeClient.getConfig().getViewStorageConfig().getStorageType(), FileSystemViewStorageType.REMOTE_FIRST);
    assertEquals(viewStorageConfig.getRemoteViewServerPort(), writeClient.getConfig().getViewStorageConfig().getRemoteViewServerPort());
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void checkInflightInstant() {
    final String instant = TestUtils.getLastPendingInstant(tempFile.getAbsolutePath());
    assertNotNull(instant);
  }

  private void checkInstantState(HoodieInstant.State state, String instantStr) {
    final String instant;
    switch (state) {
      case REQUESTED:
        instant = lastPendingInstant();
        break;
      case COMPLETED:
        instant = lastCompleteInstant();
        break;
      default:
        throw new AssertionError("Unexpected state");
    }
    assertThat(instant, is(instantStr));
  }

  protected String lastPendingInstant() {
    return TestUtils.getLastPendingInstant(tempFile.getAbsolutePath());
  }

  protected String lastCompleteInstant() {
    return TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
  }

  protected HoodieTableType getTableType() {
    return HoodieTableType.COPY_ON_WRITE;
  }

  protected void checkWrittenData(File baseFile, Map<String, String> expected) throws Exception {
    checkWrittenData(baseFile, expected, 4);
  }

  protected void checkWrittenData(File baseFile, Map<String, String> expected, int partitions) throws Exception {
    TestData.checkWrittenData(baseFile, expected, partitions);
  }

  /**
   * Asserts the data files are empty.
   */
  protected void assertEmptyDataFiles() {
    File[] dataFiles = tempFile.listFiles(file -> !file.getName().startsWith("."));
    assertNotNull(dataFiles);
    assertThat(dataFiles.length, is(0));
  }

  private void checkIndexLoaded(HoodieKey... keys) {
    for (HoodieKey key : keys) {
      assertTrue(funcWrapper.isKeyInState(key),
          "Key: " + key + " assumes to be in the index state");
    }
  }
}
