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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class RowSpatialCurveSortPartitioner extends SpatialCurveSortPartitionerBase<Dataset<Row>> {

  public RowSpatialCurveSortPartitioner(HoodieWriteConfig config) {
    super(config.getString(HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS), HoodieClusteringConfig.LayoutOptimizationStrategy.fromValue(
        config.getStringOrDefault(HoodieClusteringConfig.LAYOUT_OPTIMIZE_STRATEGY)
    ), HoodieClusteringConfig.SpatialCurveCompositionStrategyType.fromValue(
        config.getString(HoodieClusteringConfig.LAYOUT_OPTIMIZE_SPATIAL_CURVE_BUILD_METHOD)));
  }

  public RowSpatialCurveSortPartitioner(String[] orderByColumns,
                                        HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy,
                                        HoodieClusteringConfig.SpatialCurveCompositionStrategyType curveCompositionStrategyType) {
    super(orderByColumns, layoutOptStrategy, curveCompositionStrategyType);
  }

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> records, int outputPartitions) {
    return reorder(records, outputPartitions);
  }
}
