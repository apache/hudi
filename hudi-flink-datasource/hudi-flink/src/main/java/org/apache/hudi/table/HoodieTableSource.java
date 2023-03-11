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
import org.apache.hudi.source.FileIndex;
import org.apache.hudi.source.prune.PartitionPruner;
import org.apache.hudi.source.prune.StaticPartitionPruner;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.ExpressionUtils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Hoodie batch table source that always read the latest snapshot of the underneath table.
 */
public class HoodieTableSource extends AbstractHoodieTableSource
    implements SupportsPartitionPushDown {

  private List<Map<String, String>> requiredPartitions;

  public HoodieTableSource(
      ResolvedSchema schema,
      Path path,
      List<String> partitionKeys,
      String defaultPartName,
      Configuration conf) {
    this(schema, path, partitionKeys, defaultPartName, conf, null, null, null, null, null, null);
  }

  public HoodieTableSource(
      ResolvedSchema schema,
      Path path,
      List<String> partitionKeys,
      String defaultPartName,
      Configuration conf,
      @Nullable FileIndex fileIndex,
      @Nullable List<Map<String, String>> requiredPartitions,
      @Nullable int[] requiredPos,
      @Nullable Long limit,
      @Nullable HoodieTableMetaClient metaClient,
      @Nullable InternalSchemaManager internalSchemaManager) {
    super(schema, path, partitionKeys, defaultPartName, conf, fileIndex, requiredPos, limit, metaClient, internalSchemaManager);
    this.requiredPartitions = requiredPartitions;
  }

  @Override
  public DynamicTableSource copy() {
    return new HoodieTableSource(schema, path, partitionKeys, defaultPartName,
        conf, fileIndex, requiredPartitions, requiredPos, limit, metaClient, internalSchemaManager);
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> filters) {
    List<ResolvedExpression> simpleFilters = ExpressionUtils.filterSimpleCallExpression(filters);
    this.fileIndex.setDataFilters(simpleFilters);
    // refuse all the filters now
    return Result.of(Collections.emptyList(), new ArrayList<>(filters));
  }

  @Override
  public Optional<List<Map<String, String>>> listPartitions() {
    List<Map<String, String>> partitions = this.fileIndex.getPartitions(
        this.partitionKeys, defaultPartName, conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING));
    return Optional.of(partitions);
  }

  @Override
  public void applyPartitions(List<Map<String, String>> partitions) {
    this.requiredPartitions = partitions;
  }

  @Override
  @Nullable
  protected PartitionPruner getPartitionPruner() {
    if (this.requiredPartitions == null) {
      return null;
    }
    Set<String> partitionPaths = FilePathUtils.toRelativePartitionPaths(this.partitionKeys, requiredPartitions,
        conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING));
    return new StaticPartitionPruner(partitionPaths);
  }

  /**
   * Reset the state of the table source.
   */
  @VisibleForTesting
  public void reset() {
    super.reset();
    this.requiredPartitions = null;
  }
}
