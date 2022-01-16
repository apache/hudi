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

package org.apache.hudi.source;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import java.util.List;
import java.util.Map;

public final class HoodieFlinkSource {

  private HoodieFlinkSource() {
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private StreamExecutionEnvironment execEnv;
    private final HoodieSourceContext.Builder contextBuilder = HoodieSourceContext.builder();

    public Builder env(StreamExecutionEnvironment newExecEnv) {
      this.execEnv = newExecEnv;
      return this;
    }

    public Builder tableSchema(ResolvedSchema newSchema) {
      contextBuilder.tableSchema(newSchema);
      return this;
    }

    public Builder path(Path newPath) {
      contextBuilder.path(newPath);
      return this;
    }

    public Builder partitionKeys(List<String> newPartitionKeys) {
      contextBuilder.partitionKeys(newPartitionKeys);
      return this;
    }

    public Builder defaultPartName(String newDefaultPartName) {
      contextBuilder.defaultPartName(newDefaultPartName);
      return this;
    }

    public Builder conf(Configuration newConf) {
      contextBuilder.conf(newConf);
      return this;
    }

    public Builder requiredPartitions(List<Map<String, String>> newRequiredPartitions) {
      contextBuilder.requiredPartitions(newRequiredPartitions);
      return this;
    }

    public Builder requiredPos(int[] newRequiredPos) {
      contextBuilder.requiredPos(newRequiredPos);
      return this;
    }

    public Builder limit(Long newLimit) {
      contextBuilder.limit(newLimit);
      return this;
    }

    public Builder filters(List<Expression> newFilters) {
      contextBuilder.filters(newFilters);
      return this;
    }

    public DataStream<RowData> build() {
      Preconditions.checkNotNull(execEnv, "StreamExecutionEnvironment should not be null");

      HoodieSourceContext context = contextBuilder.build();

      @SuppressWarnings("unchecked")
      TypeInformation<RowData> typeInfo =
          (TypeInformation<RowData>) TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(context.getProducedDataType());
      if (context.getConf().getBoolean(FlinkOptions.READ_AS_STREAMING)) {
        StreamReadMonitoringFunction monitoringFunction = new StreamReadMonitoringFunction(
            context.getConf(), FilePathUtils.toFlinkPath(context.getPath()), context.getMaxCompactionMemoryInBytes(), context.getRequiredPartitionPaths());
        InputFormat<RowData, ?> inputFormat = context.getInputFormat(true);
        OneInputStreamOperatorFactory<MergeOnReadInputSplit, RowData> factory = StreamReadOperator.factory((MergeOnReadInputFormat) inputFormat);
        SingleOutputStreamOperator<RowData> source = execEnv.addSource(monitoringFunction, context.getSourceOperatorName("split_monitor"))
            .setParallelism(1)
            .transform("split_reader", typeInfo, factory)
            .setParallelism(context.getConf().getInteger(FlinkOptions.READ_TASKS));
        return new DataStreamSource<>(source);
      } else {
        InputFormatSourceFunction<RowData> func = new InputFormatSourceFunction<>(context.getInputFormat(), typeInfo);
        DataStreamSource<RowData> source = execEnv.addSource(func, asSummaryString(), typeInfo);
        return source.name(context.getSourceOperatorName("bounded_source")).setParallelism(context.getConf().getInteger(FlinkOptions.READ_TASKS));
      }
    }

    public String asSummaryString() {
      return "HoodieFlinkSource";
    }
  }
}
