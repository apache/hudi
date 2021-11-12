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

package org.apache.hudi.sink.utils;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestUtils;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.fs.FileSystem;
import org.hamcrest.MatcherAssert;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for write test cases.
 */
public class TestWriteBase {
  protected static final Map<String, String> EXPECTED1 = new HashMap<>();

  protected static final Map<String, String> EXPECTED2 = new HashMap<>();

  protected static final Map<String, String> EXPECTED3 = new HashMap<>();

  protected static final Map<String, String> EXPECTED4 = new HashMap<>();

  protected static final Map<String, List<String>> EXPECTED5 = new HashMap<>();

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

    EXPECTED4.put("par1", "["
        + "id1,par1,id1,Danny,23,0,par1, "
        + "id1,par1,id1,Danny,23,1,par1, "
        + "id1,par1,id1,Danny,23,2,par1, "
        + "id1,par1,id1,Danny,23,3,par1, "
        + "id1,par1,id1,Danny,23,4,par1]");

    EXPECTED5.put("par1", Arrays.asList(
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
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * Utils to composite the test stages.
   */
  public static class TestHarness {
    public static TestHarness instance() {
      return new TestHarness();
    }

    private File baseFile;
    private String basePath;
    private Configuration conf;
    private TestFunctionWrapper<RowData> pipeline;

    private String lastPending;
    private String lastComplete;

    public TestHarness preparePipeline(File basePath, Configuration conf) throws Exception {
      preparePipeline(basePath, conf, false);
      return this;
    }

    public TestHarness preparePipeline(File basePath, Configuration conf, boolean append) throws Exception {
      this.baseFile = basePath;
      this.basePath = this.baseFile.getAbsolutePath();
      this.conf = conf;
      this.pipeline = append
          ? new InsertFunctionWrapper<>(this.basePath, conf)
          : new StreamWriteFunctionWrapper<>(this.basePath, conf);
      // open the function and ingest data
      this.pipeline.openFunction();
      return this;
    }

    public TestHarness consume(List<RowData> inputs) throws Exception {
      for (RowData rowData : inputs) {
        this.pipeline.invoke(rowData);
      }
      return this;
    }

    public TestHarness assertConsumeThrows(List<RowData> inputs, String message) {
      assertThrows(HoodieException.class, () -> consume(inputs), message);
      return this;
    }

    /**
     * Assert the event buffer is empty.
     */
    public TestHarness emptyEventBuffer() {
      assertTrue(
          this.pipeline.getEventBuffer().length == 1
              && this.pipeline.getEventBuffer()[0] == null,
          "The coordinator events buffer expect to be empty");
      return this;
    }

    /**
     * Assert the next event exists and handle over it to the coordinator.
     */
    public TestHarness assertNextEvent() {
      final OperatorEvent nextEvent = this.pipeline.getNextEvent();
      MatcherAssert.assertThat("The operator expect to send an event", nextEvent, instanceOf(WriteMetadataEvent.class));
      this.pipeline.getCoordinator().handleEventFromOperator(0, nextEvent);
      assertNotNull(this.pipeline.getEventBuffer()[0], "The coordinator missed the event");
      return this;
    }

    /**
     * Assert the next event exists and handle over it to the coordinator.
     *
     * @param numWriteStatus The expected write status num reported by the event
     * @param partitions     The written partitions reported by the event
     */
    public TestHarness assertNextEvent(int numWriteStatus, String partitions) {
      final OperatorEvent nextEvent = this.pipeline.getNextEvent();
      MatcherAssert.assertThat("The operator expect to send an event", nextEvent, instanceOf(WriteMetadataEvent.class));
      List<WriteStatus> writeStatuses = ((WriteMetadataEvent) nextEvent).getWriteStatuses();
      assertNotNull(writeStatuses);
      MatcherAssert.assertThat(writeStatuses.size(), is(numWriteStatus));
      assertThat(writeStatuses.stream()
              .map(WriteStatus::getPartitionPath).sorted(Comparator.naturalOrder())
              .collect(Collectors.joining(",")),
          is(partitions));
      this.pipeline.getCoordinator().handleEventFromOperator(0, nextEvent);
      assertNotNull(this.pipeline.getEventBuffer()[0], "The coordinator missed the event");
      return this;
    }

    /**
     * Assert the next event exists and handle over it to the coordinator.
     *
     * <p>Validates that the write metadata reported by the event is empty.
     */
    public TestHarness assertEmptyEvent() {
      final OperatorEvent nextEvent = this.pipeline.getNextEvent();
      MatcherAssert.assertThat("The operator expect to send an event", nextEvent, instanceOf(WriteMetadataEvent.class));
      List<WriteStatus> writeStatuses = ((WriteMetadataEvent) nextEvent).getWriteStatuses();
      assertNotNull(writeStatuses);
      MatcherAssert.assertThat(writeStatuses.size(), is(0));
      this.pipeline.getCoordinator().handleEventFromOperator(0, nextEvent);
      assertNotNull(this.pipeline.getEventBuffer()[0], "The coordinator missed the event");
      return this;
    }

    /**
     * Assert the data buffer with given number of buckets and records.
     */
    public TestHarness assertDataBuffer(int numBuckets, int numRecords) {
      Map<String, List<HoodieRecord>> dataBuffer = this.pipeline.getDataBuffer();
      assertThat("Should have " + numBuckets + " data bucket", dataBuffer.size(), is(numBuckets));
      assertThat(numRecords + " records expect to flush out as a mini-batch",
          dataBuffer.values().stream().findFirst().map(List::size).orElse(-1),
          is(numRecords));
      return this;
    }

    /**
     * Checkpoints the pipeline, which triggers the data write and event send.
     */
    public TestHarness checkpoint(long checkpointId) throws Exception {
      this.pipeline.checkpointFunction(checkpointId);
      return this;
    }

    public TestHarness allDataFlushed() {
      Map<String, List<HoodieRecord>> dataBuffer = this.pipeline.getDataBuffer();
      assertThat("All data should be flushed out", dataBuffer.size(), is(0));
      return this;
    }

    /**
     * Handle the next {@code numEvents} events and handle over them to the coordinator.
     */
    public TestHarness handleEvents(int numEvents) {
      for (int i = 0; i < numEvents; i++) {
        final OperatorEvent event = this.pipeline.getNextEvent(); // remove the first event first
        assertThat("The operator expect to send an event", event, instanceOf(WriteMetadataEvent.class));
        this.pipeline.getCoordinator().handleEventFromOperator(0, event);
      }
      assertNotNull(this.pipeline.getEventBuffer()[0], "The coordinator missed the event");
      return this;
    }

    /**
     * Mark the checkpoint with id {@code checkpointId} as finished.
     */
    public TestHarness checkpointComplete(long checkpointId) {
      this.lastPending = lastPendingInstant();
      this.pipeline.checkpointComplete(checkpointId);
      // started a new instant already
      checkInflightInstant();
      checkInstantState(HoodieInstant.State.COMPLETED, lastPending);
      this.lastComplete = lastPending;
      this.lastPending = lastPendingInstant(); // refresh last pending instant
      return this;
    }

    /**
     * Mark the checkpoint finished with empty write metadata.
     */
    public TestHarness emptyCheckpoint(long checkpointId) {
      String lastPending = lastPendingInstant();
      this.pipeline.checkpointComplete(checkpointId);
      // last pending instant was reused
      assertEquals(this.lastPending, lastPending);
      checkInstantState(HoodieInstant.State.COMPLETED, lastComplete);
      return this;
    }

    /**
     * Mark the checkpoint with id {@code checkpointId} as failed.
     */
    public TestHarness checkpointFails(long checkpointId) {
      this.pipeline.checkpointFails(checkpointId);
      assertFalse(this.pipeline.getCoordinatorContext().isJobFailed(),
          "The last checkpoint was aborted, ignore the events");
      // no complete instant
      checkInstantState(HoodieInstant.State.COMPLETED, null);
      return this;
    }

    public TestHarness checkpointThrows(long checkpointId, String message) {
      // this returns early because there is no inflight instant
      assertThrows(HoodieException.class, () -> checkpoint(checkpointId), message);
      return this;
    }

    /**
     * Mark the task with id {@code taskId} as failed.
     */
    public TestHarness subTaskFails(int taskId) throws Exception {
      // fails the subtask
      String instant1 = lastPendingInstant();
      this.pipeline.subTaskFails(taskId);

      String instant2 = lastPendingInstant();
      assertNotEquals(instant2, instant1, "The previous instant should be rolled back when starting new instant");
      return this;
    }

    public TestHarness noCompleteInstant() {
      // no complete instant
      checkInstantState(HoodieInstant.State.COMPLETED, null);
      return this;
    }

    /**
     * Asserts the data files are empty.
     */
    public TestHarness assertEmptyDataFiles() {
      File[] dataFiles = baseFile.listFiles(file -> !file.getName().startsWith("."));
      assertNotNull(dataFiles);
      assertThat(dataFiles.length, is(0));
      return this;
    }

    public TestHarness checkWrittenData(Map<String, String> expected) throws Exception {
      checkWrittenData(expected, 4);
      return this;
    }

    public TestHarness checkWrittenData(
        Map<String, String> expected,
        int partitions) throws Exception {
      if (OptionsResolver.isCowTable(conf) || conf.getBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED)) {
        TestData.checkWrittenData(this.baseFile, expected, partitions);
      } else {
        checkWrittenDataMor(baseFile, expected, partitions);
      }
      return this;
    }

    private void checkWrittenDataMor(File baseFile, Map<String, String> expected, int partitions) throws Exception {
      HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(basePath);
      Schema schema = new TableSchemaResolver(metaClient).getTableAvroSchema();
      String latestInstant = lastCompleteInstant();
      FileSystem fs = FSUtils.getFs(basePath, new org.apache.hadoop.conf.Configuration());
      TestData.checkWrittenDataMOR(fs, latestInstant, baseFile, expected, partitions, schema);
    }

    public TestHarness checkWrittenFullData(Map<String, List<String>> expected) throws IOException {
      TestData.checkWrittenFullData(this.baseFile, expected);
      return this;
    }

    public TestHarness checkWrittenAllData(Map<String, String> expected, int partitions) throws IOException {
      TestData.checkWrittenAllData(baseFile, expected, partitions);
      return this;
    }

    public TestHarness checkIndexLoaded(HoodieKey... keys) {
      for (HoodieKey key : keys) {
        assertTrue(this.pipeline.isKeyInState(key),
            "Key: " + key + " assumes to be in the index state");
      }
      return this;
    }

    public TestHarness assertBootstrapped() throws Exception {
      assertTrue(this.pipeline.isAlreadyBootstrap());
      return this;
    }

    public TestHarness assertConfirming() {
      assertTrue(this.pipeline.isConforming(),
          "The write function should be waiting for the instant to commit");
      return this;
    }

    public TestHarness assertNotConfirming() {
      assertFalse(this.pipeline.isConforming(),
          "The write function should finish waiting for the instant to commit");
      return this;
    }

    public void end() throws Exception {
      this.pipeline.close();
    }

    private String lastPendingInstant() {
      return TestUtils.getLastPendingInstant(basePath);
    }

    private void checkInflightInstant() {
      final String instant = TestUtils.getLastPendingInstant(basePath);
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

    protected String lastCompleteInstant() {
      return OptionsResolver.isMorTable(conf)
          ? TestUtils.getLastDeltaCompleteInstant(basePath)
          : TestUtils.getLastCompleteInstant(basePath);
    }
  }
}
