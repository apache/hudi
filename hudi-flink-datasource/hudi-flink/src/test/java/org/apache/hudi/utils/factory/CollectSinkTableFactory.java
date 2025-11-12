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

package org.apache.hudi.utils.factory;

import org.apache.hudi.adapter.RichSinkFunctionAdapter;
import org.apache.hudi.adapter.SinkFunctionAdapter;
import org.apache.hudi.adapter.SinkFunctionProviderAdapter;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.util.ChangelogModes;
import org.apache.hudi.utils.RuntimeContextUtils;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Factory for CollectTableSink.
 *
 * <p>Note: The CollectTableSink collects all the data of a table into a global collection {@code RESULT},
 * so the tests should execute in single thread and the table name should be the same.
 */
public class CollectSinkTableFactory implements DynamicTableSinkFactory {
  public static final String FACTORY_ID = "collect";

  // global results to collect and query
  public static final Map<Integer, List<Row>> RESULT = new HashMap<>();

  // options
  private static final ConfigOption<Integer> SINK_EXPECTED_ROW_NUM =
      ConfigOptions.key("sink-expected-row-num").intType().defaultValue(-1);

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();

    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    int expectRowNum = helper.getOptions().get(SINK_EXPECTED_ROW_NUM);
    RESULT.clear();
    return new CollectTableSink(schema, context.getObjectIdentifier().getObjectName(), expectRowNum);
  }

  @Override
  public String factoryIdentifier() {
    return FACTORY_ID;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return new HashSet<>(Collections.singletonList(SINK_EXPECTED_ROW_NUM));
  }

  // --------------------------------------------------------------------------------------------
  // Table sinks
  // --------------------------------------------------------------------------------------------

  /**
   * Values {@link DynamicTableSink} for testing.
   */
  private static class CollectTableSink implements DynamicTableSink {

    private final ResolvedSchema schema;
    private final String tableName;
    private final int expectedRowNum;

    private CollectTableSink(
        ResolvedSchema schema,
        String tableName,
        int expectedRowNum) {
      this.schema = schema;
      this.tableName = tableName;
      this.expectedRowNum = expectedRowNum;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
      return ChangelogModes.FULL;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
      final DataType rowType = schema.toPhysicalRowDataType();
      final RowTypeInfo rowTypeInfo = (RowTypeInfo) TypeConversions.fromDataTypeToLegacyInfo(rowType);
      DataStructureConverter converter = context.createDataStructureConverter(rowType);
      if (expectedRowNum != -1) {
        return (SinkFunctionProviderAdapter) () -> new CollectSinkFunctionWithExpectedNum(converter, rowTypeInfo, expectedRowNum);
      } else {
        return (SinkFunctionProviderAdapter) () -> new CollectSinkFunction(converter, rowTypeInfo);
      }
    }

    @Override
    public DynamicTableSink copy() {
      return new CollectTableSink(schema, tableName, expectedRowNum);
    }

    @Override
    public String asSummaryString() {
      return "CollectSink";
    }
  }

  static class CollectSinkFunction extends RichSinkFunctionAdapter<RowData> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private final DynamicTableSink.DataStructureConverter converter;
    private final RowTypeInfo rowTypeInfo;

    protected transient ListState<Row> resultState;
    protected transient List<Row> localResult;

    private int taskID;

    protected CollectSinkFunction(DynamicTableSink.DataStructureConverter converter, RowTypeInfo rowTypeInfo) {
      this.converter = converter;
      this.rowTypeInfo = rowTypeInfo;
    }

    @Override
    public void invoke(RowData value, SinkFunctionAdapter.Context context) {
      Row row = (Row) converter.toExternal(value);
      assert row != null;
      row.setKind(value.getRowKind());
      RESULT.get(taskID).add(row);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      this.resultState = context.getOperatorStateStore().getListState(
          new ListStateDescriptor<>("sink-results", rowTypeInfo));
      this.localResult = new ArrayList<>();
      if (context.isRestored()) {
        for (Row value : resultState.get()) {
          localResult.add(value);
        }
      }
      this.taskID = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
      synchronized (CollectSinkTableFactory.class) {
        RESULT.put(taskID, localResult);
      }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      resultState.clear();
      List<Row> rows = RESULT.get(taskID);
      if (rows != null) {
        resultState.addAll(RESULT.get(taskID));
      }
    }
  }

  static class CollectSinkFunctionWithExpectedNum extends CollectSinkFunction {
    private final int expectRowNum;

    protected CollectSinkFunctionWithExpectedNum(
        DynamicTableSink.DataStructureConverter converter,
        RowTypeInfo rowTypeInfo,
        int expectRowNum) {
      super(converter, rowTypeInfo);
      ValidationUtils.checkArgument(expectRowNum > 0, "Expected row number should be positive.");
      this.expectRowNum = expectRowNum;
    }

    @Override
    public void invoke(RowData value, Context context) {
      super.invoke(value, context);
      if (RESULT.values().stream().mapToInt(List::size).sum() >= expectRowNum) {
        throw new SuccessException();
      }
    }
  }

  /**
   * Exception that is thrown to terminate a program and indicate success.
   */
  public static class SuccessException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public SuccessException() {
      super("Forced exception to terminate a successful sink.");
    }
  }
}
