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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.source.ExpressionEvaluators;
import org.apache.hudi.source.FileIndex;
import org.apache.hudi.source.prune.DynamicPartitionPruner;
import org.apache.hudi.source.prune.PartitionPruner;
import org.apache.hudi.table.format.InternalSchemaManager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static org.apache.hudi.util.ExpressionUtils.extractPartitionPredicateList;
import static org.apache.hudi.util.ExpressionUtils.filterSimpleCallExpression;

/**
 * Hoodie stream table source which performs partition prune based on the filter conditions in the
 * runtime. Unlike {@link HoodieTableSource}, the satisfied partitions are determine in the
 * optimization phase and never change in the runtime.
 *
 */
public class HoodieContinuousPartitionTableSource
    extends AbstractHoodieTableSource {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableSource.class);

  private List<ResolvedExpression> partitionFilters;

  public HoodieContinuousPartitionTableSource(
      ResolvedSchema schema,
      Path path,
      List<String> partitionKeys,
      String defaultPartName,
      Configuration conf) {
    this(schema, path, partitionKeys, defaultPartName, conf, null, null, null, null, null, null);
  }

  public HoodieContinuousPartitionTableSource(
      ResolvedSchema schema,
      Path path,
      List<String> partitionKeys,
      String defaultPartName,
      Configuration conf,
      @Nullable FileIndex fileIndex,
      @Nullable List<ResolvedExpression> partitionFilters,
      @Nullable int[] requiredPos,
      @Nullable Long limit,
      @Nullable HoodieTableMetaClient metaClient,
      @Nullable InternalSchemaManager internalSchemaManager) {
    super(schema, path, partitionKeys, defaultPartName, conf, fileIndex, requiredPos, limit, metaClient, internalSchemaManager);
    this.partitionFilters = partitionFilters;
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> filters) {
    List<ResolvedExpression> simpleFilters = filterSimpleCallExpression(filters);
    Tuple2<List<ResolvedExpression>, List<ResolvedExpression>> splitFilters =
        extractPartitionPredicateList(
            simpleFilters,
            this.partitionKeys,
            this.tableRowType);
    this.fileIndex.setDataFilters(splitFilters.f0);
    this.partitionFilters = splitFilters.f1;
    // refuse all the filters now
    return SupportsFilterPushDown.Result.of(
        new ArrayList<>(this.partitionFilters),
        new ArrayList<>(filters));
  }

  @Override
  public DynamicTableSource copy() {
    return new HoodieContinuousPartitionTableSource(schema, path, partitionKeys, defaultPartName,
        conf, fileIndex, partitionFilters, requiredPos, limit, metaClient, internalSchemaManager);
  }

  @Override
  @Nullable
  protected PartitionPruner getPartitionPruner() {
    if (partitionFilters != null && !partitionFilters.isEmpty()) {
      StringJoiner joiner = new StringJoiner(" and ");
      partitionFilters.stream().forEach(f -> joiner.add(f.asSummaryString()));
      LOG.info("Partition pruner for hoodie source, condition is:\n" + joiner.toString());
      List<ExpressionEvaluators.Evaluator> evaluators = ExpressionEvaluators.fromExpression(partitionFilters);
      List<DataType> partitionTypes = this.partitionKeys.stream()
          .map(name -> this.schema.getColumn(name)
              .orElseThrow(() -> new HoodieValidationException("Field " + name + " does not exist")))
          .map(Column::getDataType)
          .collect(Collectors.toList());
      String defaultParName = conf.getString(FlinkOptions.PARTITION_DEFAULT_NAME);
      boolean hivePartition = conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING);
      return new DynamicPartitionPruner(evaluators, partitionKeys, partitionTypes, defaultParName, hivePartition);
    } else {
      return null;
    }
  }

  /**
   * Reset the state of the table source.
   */
  @VisibleForTesting
  public void reset() {
    super.reset();
    this.partitionFilters = null;
  }
}
