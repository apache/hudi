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

package org.apache.hudi.source.flip27;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.utils.MockStateInitializationContext;
import org.apache.hudi.source.StreamReadMonitoringFunction;
import org.apache.hudi.source.StreamReadOperator;
import org.apache.hudi.source.flip27.split.MergeOnReadInputSplitSerializer;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.table.format.mor.MergeOnReadTableState;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestUtils;

import org.apache.avro.Schema;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.taskmanager.NoOpTaskOperatorEventGateway;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.OperatorEventDispatcherImpl;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.CollectingSourceContext;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test cases for {@link StreamReadOperator}.
 */
public class HoodieSourceTest extends TestLogger {
  private static final Map<String, String> EXPECTED = new HashMap<>();

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
    conf.setString(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);

    StreamerUtil.initTableIfNotExists(conf);
  }

  @Test
  void testWriteRecords() throws Exception {
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    try {
      SourceOperator<RowData, MergeOnReadInputSplit> sourceOperator = createHoodieSource();
      sourceOperator.initializeState(new MockStateInitializationContext());
      sourceOperator.open();
      StreamReadMonitoringFunction func = TestUtils.getMonitorFunc(conf);
      List<MergeOnReadInputSplit> splits = generateSplits(func);
      AddSplitEvent<MergeOnReadInputSplit> addSplitEvent = new AddSplitEvent(splits, new MergeOnReadInputSplitSerializer());
      sourceOperator.handleOperatorEvent(addSplitEvent);
      ValidatingSourceOutput output = new ValidatingSourceOutput();
      while (output.count < 8) {
        sourceOperator.getSourceReader().pollNext(output);
      }
      TestData.assertRowDataEquals(output.consumedValues.stream().collect(Collectors.toList()), TestData.DATA_SET_INSERT);
      output.validate();
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  private List<MergeOnReadInputSplit> generateSplits(StreamReadMonitoringFunction func) throws Exception {
    final List<MergeOnReadInputSplit> splits = new ArrayList<>();
    func.open(conf);
    func.monitorDirAndForwardSplits(new CollectingSourceContext<>(new Object(), splits));
    return splits;
  }

  private SourceOperator<RowData, MergeOnReadInputSplit> createHoodieSource() throws Exception {
    final String basePath = tempFile.getAbsolutePath();
    final org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(new Configuration());
    final HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(hadoopConf).setBasePath(basePath).build();
    final List<String> partitionKeys = Collections.singletonList("partition");

    // This input format is used to opening the emitted split.
    TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
    final Schema tableAvroSchema;
    try {
      tableAvroSchema = schemaResolver.getTableAvroSchema();
    } catch (Exception e) {
      throw new HoodieException("Get table avro schema error", e);
    }
    final DataType rowDataType = AvroSchemaConverter.convertToDataType(tableAvroSchema);
    final RowType rowType = (RowType) rowDataType.getLogicalType();
    final MergeOnReadTableState hoodieTableState = new MergeOnReadTableState(
        rowType,
        TestConfigurations.ROW_TYPE,
        tableAvroSchema.toString(),
        AvroSchemaConverter.convertToSchema(TestConfigurations.ROW_TYPE).toString(),
        Collections.emptyList(),
        new String[0]);
    MergeOnReadInputFormat inputFormat = MergeOnReadInputFormat.builder()
        .config(conf)
        .tableState(hoodieTableState)
        .fieldTypes(rowDataType.getChildren())
        .defaultPartName("default").limit(1000L)
        .emitDelete(true)
        .build();

    HoodieSource hoodieSource = HoodieSourceBuilder.builder()
        .mergeOnReadInputFormat(inputFormat)
        .maxParallism(1)
        .path(FilePathUtils.toFlinkPath(new Path(basePath)))
        .boundedness(Boundedness.CONTINUOUS_UNBOUNDED)
        .conf(conf)
        .requiredPartitionPaths(null)
        .build();
    SourceOperatorFactory<RowData> sourceOperatorFactory = new SourceOperatorFactory(hoodieSource, WatermarkStrategy.noWatermarks());
    SourceOperator<RowData, MergeOnReadInputSplit> sourceOperator = sourceOperatorFactory.createStreamOperator(createStreamOperatorParameters());
    return sourceOperator;
  }

  protected StreamConfig createStreamConfig(Configuration conf) {
    return new MockStreamConfig(conf, 1);
  }

  protected StreamOperatorParameters<RowData> createStreamOperatorParameters() throws Exception {

    OperatorEventDispatcher dispatcher = new OperatorEventDispatcherImpl(
        this.getClass().getClassLoader(), new NoOpTaskOperatorEventGateway());
    return new StreamOperatorParameters<>(
      new SourceOperatorStreamTask<RowData>(getTestingEnvironment()),
      createStreamConfig(conf),
      new CollectorOutput<>(new ArrayList<>()),
      TestProcessingTimeService::new,
      dispatcher);
  }

  private Environment getTestingEnvironment() {
    return new StreamMockEnvironment(
      new Configuration(),
      new Configuration(),
      new ExecutionConfig(),
      1L,
      new MockInputSplitProvider(),
      1,
      new TestTaskStateManager());
  }

  /**
   *
   */
  public static class ValidatingSourceOutput implements ReaderOutput<RowData> {
    private Set<RowData> consumedValues = new HashSet<>();
    private int count = 0;

    @Override
    public void collect(RowData element) {
      count++;
      consumedValues.add(element);
    }

    @Override
    public void collect(RowData element, long timestamp) {
      collect(element);
    }

    public void validate() {
    }

    public int count() {
      return count;
    }

    @Override
    public void emitWatermark(Watermark watermark) {
    }

    @Override
    public void markIdle() {
    }

    @Override
    public void markActive() {

    }

    @Override
    public SourceOutput<RowData> createOutputForSplit(String splitId) {
      return this;
    }

    @Override
    public void releaseOutputForSplit(String splitId) {
    }
  }

}
