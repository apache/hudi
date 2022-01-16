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

package org.apache.hudi.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.source.HoodieFlinkSource;
import org.apache.hudi.source.HoodieSourceContext;
import org.apache.hudi.util.ChangelogModes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Hoodie batch table source that always read the latest snapshot of the underneath table.
 */
public class HoodieTableSource implements
    ScanTableSource,
    SupportsPartitionPushDown,
    SupportsProjectionPushDown,
    SupportsLimitPushDown,
    SupportsFilterPushDown {

  private final HoodieSourceContext hoodieSourceContext;

  public HoodieTableSource(
      ResolvedSchema schema,
      Path path,
      List<String> partitionKeys,
      String defaultPartName,
      Configuration conf) {
    this(schema, path, partitionKeys, defaultPartName, conf, null, null, null, null);
  }

  public HoodieTableSource(
      ResolvedSchema schema,
      Path path,
      List<String> partitionKeys,
      String defaultPartName,
      Configuration conf,
      @Nullable List<Map<String, String>> requiredPartitions,
      @Nullable int[] requiredPos,
      @Nullable Long limit,
      @Nullable List<Expression> filters) {
    hoodieSourceContext = new HoodieSourceContext(schema, path, partitionKeys,
        defaultPartName, conf, requiredPartitions, requiredPos,
        limit, filters);
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    return new DataStreamScanProvider() {

      @Override
      public boolean isBounded() {
        return !hoodieSourceContext.getConf().getBoolean(FlinkOptions.READ_AS_STREAMING);
      }

      @Override
      public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
        return createDataStream(execEnv);
      }
    };
  }

  @Override
  public ChangelogMode getChangelogMode() {
    // when read as streaming and changelog mode is enabled, emit as FULL mode;
    // when all the changes are compacted or read as batch, emit as INSERT mode.
    return OptionsResolver.emitChangelog(hoodieSourceContext.getConf()) ? ChangelogModes.FULL : ChangelogMode.insertOnly();
  }

  @Override
  public DynamicTableSource copy() {
    return new HoodieTableSource(hoodieSourceContext.getSchema(), hoodieSourceContext.getPath(), hoodieSourceContext.getPartitionKeys(), hoodieSourceContext.getDefaultPartName(),
        hoodieSourceContext.getConf(), hoodieSourceContext.getRequiredPartitions(), hoodieSourceContext.getRequiredPos(), hoodieSourceContext.getLimit(), hoodieSourceContext.getFilters());
  }

  @Override
  public String asSummaryString() {
    return "HudiTableSource";
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> filters) {
    hoodieSourceContext.setFilters(new ArrayList<>(filters));
    // refuse all the filters now
    return SupportsFilterPushDown.Result.of(Collections.emptyList(), new ArrayList<>(filters));
  }

  @Override
  public Optional<List<Map<String, String>>> listPartitions() {
    List<Map<String, String>> partitions = hoodieSourceContext.getFileIndex().getPartitions(
        hoodieSourceContext.getPartitionKeys(), hoodieSourceContext.getDefaultPartName(),
        hoodieSourceContext.getConf().getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING));
    return Optional.of(partitions);
  }

  @Override
  public void applyPartitions(List<Map<String, String>> partitions) {
    hoodieSourceContext.setRequiredPartitions(partitions);
  }

  @Override
  public boolean supportsNestedProjection() {
    return false;
  }

  @Override
  public void applyProjection(int[][] projections) {
    // nested projection is not supported.
    hoodieSourceContext.setRequiredPos(
        Arrays.stream(projections).mapToInt(array -> array[0]).toArray());
  }

  @Override
  public void applyLimit(long limit) {
    hoodieSourceContext.setLimit(limit);
  }

  public Configuration getConf() {
    return hoodieSourceContext.getConf();
  }

  private DataStream<RowData> createDataStream(StreamExecutionEnvironment execEnv) {
    return HoodieFlinkSource.builder()
        .env(execEnv)
        .tableSchema(hoodieSourceContext.getSchema())
        .path(hoodieSourceContext.getPath())
        .partitionKeys(hoodieSourceContext.getPartitionKeys())
        .defaultPartName(hoodieSourceContext.getDefaultPartName())
        .conf(hoodieSourceContext.getConf())
        .requiredPartitions(hoodieSourceContext.getRequiredPartitions())
        .requiredPos(hoodieSourceContext.getRequiredPos())
        .limit(hoodieSourceContext.getLimit())
        .filters(hoodieSourceContext.getFilters())
        .build();
  }
}
