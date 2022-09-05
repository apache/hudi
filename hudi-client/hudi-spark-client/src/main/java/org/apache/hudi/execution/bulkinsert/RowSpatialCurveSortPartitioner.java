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
import org.apache.hudi.sort.SpaceCurveSortingHelper;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;

public class RowSpatialCurveSortPartitioner implements BulkInsertPartitioner<Dataset<Row>> {

  private final String[] orderByColumns;
  private final HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy;
  private final HoodieClusteringConfig.SpatialCurveCompositionStrategyType curveCompositionStrategyType;

  public RowSpatialCurveSortPartitioner(HoodieWriteConfig config) {
    this.layoutOptStrategy = config.getLayoutOptimizationStrategy();
    if (config.getClusteringSortColumns() != null) {
      this.orderByColumns = Arrays.stream(config.getClusteringSortColumns().split(","))
          .map(String::trim).toArray(String[]::new);
    } else {
      throw new IllegalArgumentException("The config "
          + HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key() + " must be provided");
    }
    this.curveCompositionStrategyType = config.getLayoutOptimizationCurveBuildMethod();
  }

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> records, int outputPartitions) {
    return reorder(records, outputPartitions);
  }

  private Dataset<Row> reorder(Dataset<Row> dataset, int numOutputGroups) {
    if (orderByColumns.length == 0) {
      // No-op
      return dataset;
    }

    List<String> orderedCols = Arrays.asList(orderByColumns);

    switch (curveCompositionStrategyType) {
      case DIRECT:
        return SpaceCurveSortingHelper.orderDataFrameByMappingValues(dataset, layoutOptStrategy, orderedCols, numOutputGroups);
      case SAMPLE:
        return SpaceCurveSortingHelper.orderDataFrameBySamplingValues(dataset, layoutOptStrategy, orderedCols, numOutputGroups);
      default:
        throw new UnsupportedOperationException(String.format("Unsupported space-curve curve building strategy (%s)", curveCompositionStrategyType));
    }
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
