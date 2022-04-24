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

import org.apache.hudi.api.HoodieSource;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import org.apache.avro.Schema;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableSource.class);

  private HoodieSource hoodieSource;

  public HoodieTableSource(HoodieSource hoodieSource) {
    this.hoodieSource = hoodieSource;
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    return hoodieSource.getScanRuntimeProvider();
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return hoodieSource.getChangelogMode();
  }

  @Override
  public DynamicTableSource copy() {
    return new HoodieTableSource(this.hoodieSource);
  }

  @Override
  public String asSummaryString() {
    return hoodieSource.asSummaryString();
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> filters) {
    return hoodieSource.applyFilters(filters);
  }

  @Override
  public Optional<List<Map<String, String>>> listPartitions() {
    return hoodieSource.listPartitions();
  }

  @Override
  public void applyPartitions(List<Map<String, String>> partitions) {
    hoodieSource.applyPartitions(partitions);
  }

  @Override
  public boolean supportsNestedProjection() {
    return false;
  }

  @Override
  public void applyProjection(int[][] projections) {
    // nested projection is not supported.
    hoodieSource.applyProjection(projections);
  }

  @Override
  public void applyLimit(long limit) {
    hoodieSource.applyLimit(limit);
  }

  @VisibleForTesting
  public Schema getTableAvroSchema() {
    return hoodieSource.getTableAvroSchema();
  }

  @VisibleForTesting
  public Configuration getConf() {
    return hoodieSource.getConf();
  }

  /**
   * Reset the state of the table source.
   */
  @VisibleForTesting
  public void reset() {
    hoodieSource.reset();
  }

  /**
   * Get the reader paths with partition path expanded.
   */
  @VisibleForTesting
  public Path[] getReadPaths() {
    return hoodieSource.getReadPaths();
  }

  public InputFormat<RowData,?> getInputFormat() {
    return hoodieSource.getInputFormat();
  }

  public HoodieTableMetaClient getMetaClient() {
    return hoodieSource.getMetaClient();
  }
}
