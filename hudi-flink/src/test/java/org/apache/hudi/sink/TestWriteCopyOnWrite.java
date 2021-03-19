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
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.event.BatchWriteSuccessEvent;
import org.apache.hudi.sink.utils.StreamWriteFunctionWrapper;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.table.data.RowData;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for StreamingSinkFunction.
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
    final String basePath = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(basePath);
    conf.setString(FlinkOptions.TABLE_TYPE, getTableType());
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

    String instant = funcWrapper.getWriteClient()
        .getInflightAndRequestedInstant(getTableType());

    final OperatorEvent nextEvent = funcWrapper.getNextEvent();
    MatcherAssert.assertThat("The operator expect to send an event", nextEvent, instanceOf(BatchWriteSuccessEvent.class));
    List<WriteStatus> writeStatuses = ((BatchWriteSuccessEvent) nextEvent).getWriteStatuses();
    assertNotNull(writeStatuses);
    MatcherAssert.assertThat(writeStatuses.size(), is(4)); // write 4 partition files
    assertThat(writeStatuses.stream()
            .map(WriteStatus::getPartitionPath).sorted(Comparator.naturalOrder())
            .collect(Collectors.joining(",")),
        is("par1,par2,par3,par4"));

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    checkInstantState(funcWrapper.getWriteClient(), HoodieInstant.State.REQUESTED, instant);
    funcWrapper.checkpointComplete(1);
    // the coordinator checkpoint commits the inflight instant.
    checkInstantState(funcWrapper.getWriteClient(), HoodieInstant.State.COMPLETED, instant);

    // checkpoint for next round, no data input, so after the checkpoint,
    // there should not be REQUESTED Instant
    // this triggers the data write and event send
    funcWrapper.checkpointFunction(2);

    String instant2 = funcWrapper.getWriteClient()
        .getInflightAndRequestedInstant(getTableType());
    assertNotEquals(instant, instant2);

    final OperatorEvent nextEvent2 = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent2, instanceOf(BatchWriteSuccessEvent.class));
    List<WriteStatus> writeStatuses2 = ((BatchWriteSuccessEvent) nextEvent2).getWriteStatuses();
    assertNotNull(writeStatuses2);
    assertThat(writeStatuses2.size(), is(0)); // write empty statuses

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent2);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    funcWrapper.checkpointComplete(2);
    // started a new instant already
    checkInflightInstant(funcWrapper.getWriteClient());
    checkInstantState(funcWrapper.getWriteClient(), HoodieInstant.State.COMPLETED, instant);
  }

  @Test
  public void testCheckpointFails() throws Exception {
    // open the function and ingest data
    funcWrapper.openFunction();
    // no data written and triggers checkpoint fails,
    // then we should revert the start instant

    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);

    String instant = funcWrapper.getWriteClient()
        .getInflightAndRequestedInstant(getTableType());
    assertNotNull(instant);

    final OperatorEvent nextEvent = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent, instanceOf(BatchWriteSuccessEvent.class));
    List<WriteStatus> writeStatuses = ((BatchWriteSuccessEvent) nextEvent).getWriteStatuses();
    assertNotNull(writeStatuses);
    assertThat(writeStatuses.size(), is(0)); // no data write

    // fails the checkpoint
    assertThrows(HoodieException.class,
        () -> funcWrapper.checkpointFails(1),
        "The last checkpoint was aborted, roll back the last write and throw");

    // the instant metadata should be cleared
    checkInstantState(funcWrapper.getWriteClient(), HoodieInstant.State.REQUESTED, null);
    checkInstantState(funcWrapper.getWriteClient(), HoodieInstant.State.COMPLETED, null);

    for (RowData rowData : TestData.DATA_SET_INSERT) {
      funcWrapper.invoke(rowData);
    }

    // this returns early cause there is no inflight instant
    funcWrapper.checkpointFunction(2);
    // do not sent the write event and fails the checkpoint,
    // behaves like the last checkpoint is successful.
    funcWrapper.checkpointFails(2);
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

    String instant = funcWrapper.getWriteClient()
        .getInflightAndRequestedInstant(getTableType());

    final OperatorEvent nextEvent = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent, instanceOf(BatchWriteSuccessEvent.class));

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    checkInstantState(funcWrapper.getWriteClient(), HoodieInstant.State.REQUESTED, instant);
    funcWrapper.checkpointComplete(1);
    checkWrittenData(tempFile, EXPECTED1);
    // the coordinator checkpoint commits the inflight instant.
    checkInstantState(funcWrapper.getWriteClient(), HoodieInstant.State.COMPLETED, instant);
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
    assertThat("The operator expect to send an event", nextEvent, instanceOf(BatchWriteSuccessEvent.class));

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
    assertThat("The operator expect to send an event", nextEvent, instanceOf(BatchWriteSuccessEvent.class));

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

    String instant = funcWrapper.getWriteClient()
        .getInflightAndRequestedInstant(getTableType());

    nextEvent = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent, instanceOf(BatchWriteSuccessEvent.class));

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    checkInstantState(funcWrapper.getWriteClient(), HoodieInstant.State.REQUESTED, instant);
    funcWrapper.checkpointComplete(2);
    // the coordinator checkpoint commits the inflight instant.
    checkInstantState(funcWrapper.getWriteClient(), HoodieInstant.State.COMPLETED, instant);
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
    assertThat("The operator expect to send an event", nextEvent, instanceOf(BatchWriteSuccessEvent.class));

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

    String instant = funcWrapper.getWriteClient()
        .getInflightAndRequestedInstant(getTableType());

    nextEvent = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent, instanceOf(BatchWriteSuccessEvent.class));

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    checkInstantState(funcWrapper.getWriteClient(), HoodieInstant.State.REQUESTED, instant);
    funcWrapper.checkpointComplete(2);
    // the coordinator checkpoint commits the inflight instant.
    checkInstantState(funcWrapper.getWriteClient(), HoodieInstant.State.COMPLETED, instant);

    Map<String, String> expected = new HashMap<>();
    // id3, id5 were deleted and id9 is ignored
    expected.put("par1", "[id1,par1,id1,Danny,24,1,par1, id2,par1,id2,Stephen,34,2,par1]");
    expected.put("par2", "[id4,par2,id4,Fabian,31,4,par2]");
    expected.put("par3", "[id6,par3,id6,Emma,20,6,par3]");
    expected.put("par4", "[id7,par4,id7,Bob,44,7,par4, id8,par4,id8,Han,56,8,par4]");
    checkWrittenData(tempFile, expected);
  }

  @Test
  public void testInsertWithMiniBatches() throws Exception {
    // reset the config option
    conf.setDouble(FlinkOptions.WRITE_BATCH_SIZE, 0.001); // 1Kb batch size
    funcWrapper = new StreamWriteFunctionWrapper<>(tempFile.getAbsolutePath(), conf);

    // open the function and ingest data
    funcWrapper.openFunction();
    // Each record is 424 bytes. so 3 records expect to trigger a mini-batch write
    for (RowData rowData : TestData.DATA_SET_INSERT_DUPLICATES) {
      funcWrapper.invoke(rowData);
    }

    Map<String, List<HoodieRecord>> dataBuffer = funcWrapper.getDataBuffer();
    assertThat("Should have 1 data bucket", dataBuffer.size(), is(1));
    assertThat("2 records expect to flush out as a mini-batch",
        dataBuffer.values().stream().findFirst().map(List::size).orElse(-1),
        is(3));

    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);
    dataBuffer = funcWrapper.getDataBuffer();
    assertThat("All data should be flushed out", dataBuffer.size(), is(0));

    final OperatorEvent event1 = funcWrapper.getNextEvent(); // remove the first event first
    final OperatorEvent event2 = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", event2, instanceOf(BatchWriteSuccessEvent.class));

    funcWrapper.getCoordinator().handleEventFromOperator(0, event1);
    funcWrapper.getCoordinator().handleEventFromOperator(0, event2);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    String instant = funcWrapper.getWriteClient()
        .getInflightAndRequestedInstant(getTableType());

    funcWrapper.checkpointComplete(1);

    Map<String, String> expected = getMiniBatchExpected();
    checkWrittenData(tempFile, expected, 1);

    // started a new instant already
    checkInflightInstant(funcWrapper.getWriteClient());
    checkInstantState(funcWrapper.getWriteClient(), HoodieInstant.State.COMPLETED, instant);

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

  Map<String, String> getMiniBatchExpected() {
    Map<String, String> expected = new HashMap<>();
    expected.put("par1", "[id1,par1,id1,Danny,23,1,par1, "
        + "id1,par1,id1,Danny,23,1,par1, "
        + "id1,par1,id1,Danny,23,1,par1, "
        + "id1,par1,id1,Danny,23,1,par1, "
        + "id1,par1,id1,Danny,23,1,par1]");
    return expected;
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
    assertThat("The operator expect to send an event", nextEvent, instanceOf(BatchWriteSuccessEvent.class));

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    funcWrapper.checkpointComplete(1);

    // Mark the index state as not fully loaded to trigger re-load from the filesystem.
    funcWrapper.clearIndexState();

    // upsert another data buffer
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
        new HoodieKey("id8", "par4"));
    // the data is not flushed yet
    checkWrittenData(tempFile, EXPECTED1);
    // this triggers the data write and event send
    funcWrapper.checkpointFunction(2);

    String instant = funcWrapper.getWriteClient()
        .getInflightAndRequestedInstant("COPY_ON_WRITE");

    nextEvent = funcWrapper.getNextEvent();
    assertThat("The operator expect to send an event", nextEvent, instanceOf(BatchWriteSuccessEvent.class));
    checkWrittenData(tempFile, EXPECTED2);

    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    assertNotNull(funcWrapper.getEventBuffer()[0], "The coordinator missed the event");

    checkInstantState(funcWrapper.getWriteClient(), HoodieInstant.State.REQUESTED, instant);
    assertFalse(funcWrapper.isAllPartitionsLoaded(),
        "All partitions assume to be loaded into the index state");
    funcWrapper.checkpointComplete(2);
    // the coordinator checkpoint commits the inflight instant.
    checkInstantState(funcWrapper.getWriteClient(), HoodieInstant.State.COMPLETED, instant);
    checkWrittenData(tempFile, EXPECTED2);
    assertTrue(funcWrapper.isAllPartitionsLoaded(),
        "All partitions assume to be loaded into the index state");
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  @SuppressWarnings("rawtypes")
  private void checkInflightInstant(HoodieFlinkWriteClient writeClient) {
    final String instant = writeClient.getInflightAndRequestedInstant(getTableType());
    assertNotNull(instant);
  }

  @SuppressWarnings("rawtypes")
  private void checkInstantState(
      HoodieFlinkWriteClient writeClient,
      HoodieInstant.State state,
      String instantStr) {
    final String instant;
    switch (state) {
      case REQUESTED:
        instant = writeClient.getInflightAndRequestedInstant(getTableType());
        break;
      case COMPLETED:
        instant = writeClient.getLastCompletedInstant(getTableType());
        break;
      default:
        throw new AssertionError("Unexpected state");
    }
    assertThat(instant, is(instantStr));
  }

  protected String getTableType() {
    return HoodieTableType.COPY_ON_WRITE.name();
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
