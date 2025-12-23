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

package org.apache.hudi.source;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.table.format.mor.MergeOnReadTableState;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.tasks.mailbox.SteppingMailboxProcessor;
import org.apache.flink.streaming.util.CollectingSourceContext;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.configuration.FlinkOptions.TABLE_TYPE;
import static org.apache.hudi.configuration.FlinkOptions.TABLE_TYPE_MERGE_ON_READ;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link StreamReadOperator}.
 */
public class TestStreamReadOperator {
  private static final Map<String, String> EXPECTED = new HashMap<>();
  private static final String TIME_CHARACTERISTIC = "timechar";

  static {
    EXPECTED.put("par1", "+I[id1, Danny, 23, 1970-01-01T00:00:00.001, par1], +I[id2, Stephen, 33, 1970-01-01T00:00:00.002, par1]");
    EXPECTED.put("par2", "+I[id3, Julian, 53, 1970-01-01T00:00:00.003, par2], +I[id4, Fabian, 31, 1970-01-01T00:00:00.004, par2]");
    EXPECTED.put("par3", "+I[id5, Sophia, 18, 1970-01-01T00:00:00.005, par3], +I[id6, Emma, 20, 1970-01-01T00:00:00.006, par3]");
    EXPECTED.put("par4", "+I[id7, Bob, 44, 1970-01-01T00:00:00.007, par4], +I[id8, Han, 56, 1970-01-01T00:00:00.008, par4]");
  }

  private Configuration conf;

  @TempDir
  File tempFile;

  @BeforeEach
  public void before() throws Exception {
    final String basePath = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(TABLE_TYPE, TABLE_TYPE_MERGE_ON_READ);

    StreamerUtil.initTableIfNotExists(conf);
  }

  @Test
  void testWriteRecords() throws Exception {
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    try (OneInputStreamOperatorTestHarness<MergeOnReadInputSplit, RowData> harness = createReader()) {
      harness.setup();
      harness.open();

      SteppingMailboxProcessor processor = createLocalMailbox(harness);
      StreamReadMonitoringFunction func = TestUtils.getMonitorFunc(conf);

      List<MergeOnReadInputSplit> splits = generateSplits(func);
      assertThat("Should have 4 splits", splits.size(), is(4));
      for (MergeOnReadInputSplit split : splits) {
        // Process this element to enqueue to mail-box.
        harness.processElement(split, -1);

        // Run the mail-box once to read all records from the given split.
        assertThat("Should process 1 split", processor.runMailboxStep());
      }
      // Assert the output has expected elements.
      TestData.assertRowDataEquals(harness.extractOutputValues(), TestData.DATA_SET_INSERT);

      TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
      final List<MergeOnReadInputSplit> splits2 = generateSplits(func);
      assertThat("Should have 4 splits", splits2.size(), is(4));
      for (MergeOnReadInputSplit split : splits2) {
        // Process this element to enqueue to mail-box.
        harness.processElement(split, -1);

        // Run the mail-box once to read all records from the given split.
        assertThat("Should processed 1 split", processor.runMailboxStep());
      }
      // The result sets behaves like append only: DATA_SET_ONE + DATA_SET_TWO
      List<RowData> expected = new ArrayList<>(TestData.DATA_SET_INSERT);
      expected.addAll(TestData.DATA_SET_UPDATE_INSERT);
      TestData.assertRowDataEquals(harness.extractOutputValues(), expected);
    }
  }

  @Test
  public void testCheckpoint() throws Exception {
    // Received emitted splits: split1, split2, split3, split4, checkpoint request is triggered
    // when reading records from split1.
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    long timestamp = 0;
    try (OneInputStreamOperatorTestHarness<MergeOnReadInputSplit, RowData> harness = createReader()) {
      harness.setup();
      harness.open();

      SteppingMailboxProcessor processor = createLocalMailbox(harness);
      StreamReadMonitoringFunction func = TestUtils.getMonitorFunc(conf);

      List<MergeOnReadInputSplit> splits = generateSplits(func);
      assertThat("Should have 4 splits", splits.size(), is(4));

      for (MergeOnReadInputSplit split : splits) {
        harness.processElement(split, ++timestamp);
      }

      // Trigger snapshot state, it will start to work once all records from split0 are read.
      processor.getMainMailboxExecutor()
          .execute(() -> harness.snapshot(1, 3), "Trigger snapshot");

      assertTrue(processor.runMailboxStep(), "Should have processed the split0");
      assertTrue(processor.runMailboxStep(), "Should have processed the snapshot state action");

      assertThat(TestData.rowDataToString(harness.extractOutputValues()),
          is(getSplitExpected(Collections.singletonList(splits.get(0)), EXPECTED)));

      // Read records from split1.
      assertTrue(processor.runMailboxStep(), "Should have processed the split1");

      // Read records from split2.
      assertTrue(processor.runMailboxStep(), "Should have processed the split2");

      // Read records from split3.
      assertTrue(processor.runMailboxStep(), "Should have processed the split3");

      // Assert the output has expected elements.
      TestData.assertRowDataEquals(harness.extractOutputValues(), TestData.DATA_SET_INSERT);
    }
  }

  @Test
  public void testCheckpointRestore() throws Exception {
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    OperatorSubtaskState state;
    final List<MergeOnReadInputSplit> splits;
    try (OneInputStreamOperatorTestHarness<MergeOnReadInputSplit, RowData> harness = createReader()) {
      harness.setup();
      harness.open();

      StreamReadMonitoringFunction func = TestUtils.getMonitorFunc(conf);

      splits = generateSplits(func);
      assertThat("Should have 4 splits", splits.size(), is(4));

      // Enqueue all the splits.
      for (MergeOnReadInputSplit split : splits) {
        harness.processElement(split, -1);
      }

      // Read all records from the first 2 splits.
      SteppingMailboxProcessor localMailbox = createLocalMailbox(harness);
      for (int i = 0; i < 2; i++) {
        assertTrue(localMailbox.runMailboxStep(), "Should have processed the split#" + i);
      }

      assertThat(TestData.rowDataToString(harness.extractOutputValues()),
          is(getSplitExpected(splits.subList(0, 2), EXPECTED)));

      // Snapshot state now,  there are 2 splits left in the state.
      state = harness.snapshot(1, 1);
    }

    try (OneInputStreamOperatorTestHarness<MergeOnReadInputSplit, RowData> harness = createReader()) {
      harness.setup();
      // Recover to process the remaining splits.
      harness.initializeState(state);
      harness.open();

      SteppingMailboxProcessor localMailbox = createLocalMailbox(harness);

      for (int i = 2; i < 4; i++) {
        assertTrue(localMailbox.runMailboxStep(), "Should have processed one split#" + i);
      }

      // expect to output the left data
      assertThat(TestData.rowDataToString(harness.extractOutputValues()),
          is(getSplitExpected(splits.subList(2, 4), EXPECTED)));
    }
  }

  private static String getSplitExpected(List<MergeOnReadInputSplit> splits, Map<String, String> expected) {
    return splits.stream()
        .map(TestUtils::getSplitPartitionPath)
        .map(expected::get)
        .sorted(Comparator.naturalOrder())
        .collect(Collectors.toList()).toString();
  }

  private List<MergeOnReadInputSplit> generateSplits(StreamReadMonitoringFunction func) throws Exception {
    final List<MergeOnReadInputSplit> splits = new ArrayList<>();
    func.open(conf);
    func.monitorDirAndForwardSplits(new CollectingSourceContext<>(new Object(), splits));
    return splits;
  }

  private OneInputStreamOperatorTestHarness<MergeOnReadInputSplit, RowData> createReader() throws Exception {
    final String basePath = tempFile.getAbsolutePath();
    final HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(
        basePath, HadoopConfigurations.getHadoopConf(new Configuration()));
    final List<String> partitionKeys = Collections.singletonList("partition");

    // This input format is used to opening the emitted split.
    TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
    final HoodieSchema tableSchema;
    try {
      tableSchema = schemaResolver.getTableSchema();
    } catch (Exception e) {
      throw new HoodieException("Get table avro schema error", e);
    }
    final DataType rowDataType = AvroSchemaConverter.convertToDataType(tableSchema.getAvroSchema());
    final RowType rowType = (RowType) rowDataType.getLogicalType();
    final MergeOnReadTableState hoodieTableState = new MergeOnReadTableState(
        rowType,
        TestConfigurations.ROW_TYPE,
        tableSchema.toString(),
        AvroSchemaConverter.convertToSchema(TestConfigurations.ROW_TYPE).toString(),
        Collections.emptyList());
    MergeOnReadInputFormat inputFormat = MergeOnReadInputFormat.builder()
        .config(conf)
        .tableState(hoodieTableState)
        .fieldTypes(rowDataType.getChildren())
        .limit(1000L)
        .emitDelete(true)
        .build();

    OneInputStreamOperatorFactory<MergeOnReadInputSplit, RowData> factory = StreamReadOperator.factory(inputFormat);
    OneInputStreamOperatorTestHarness<MergeOnReadInputSplit, RowData> harness = new OneInputStreamOperatorTestHarness<>(
            factory, 1, 1, 0);
    harness.getStreamConfig().getConfiguration().setString(TIME_CHARACTERISTIC, "0");

    return harness;
  }

  private SteppingMailboxProcessor createLocalMailbox(
      OneInputStreamOperatorTestHarness<MergeOnReadInputSplit, RowData> harness) {
    return new SteppingMailboxProcessor(
        MailboxDefaultAction.Controller::suspendDefaultAction,
        harness.getTaskMailbox(),
        StreamTaskActionExecutor.IMMEDIATE);
  }
}
