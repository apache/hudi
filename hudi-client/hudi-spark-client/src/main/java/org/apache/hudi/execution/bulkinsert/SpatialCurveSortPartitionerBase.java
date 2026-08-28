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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sort.SpaceCurveSortingHelper;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public abstract class SpatialCurveSortPartitionerBase<T> implements BulkInsertPartitioner<T> {

  private final String[] orderByColumns;
  private final HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy;
  private final HoodieClusteringConfig.SpatialCurveCompositionStrategyType curveCompositionStrategyType;

  public SpatialCurveSortPartitionerBase(String orderByColumns,
                                         HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy,
                                         HoodieClusteringConfig.SpatialCurveCompositionStrategyType curveCompositionStrategyType) {
    if (orderByColumns != null) {
      this.orderByColumns = Arrays.stream(orderByColumns.split(","))
          .map(String::trim).toArray(String[]::new);
    } else {
      throw new IllegalArgumentException("The config "
          + HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key() + " must be provided");
    }
    this.layoutOptStrategy = layoutOptStrategy;
    this.curveCompositionStrategyType = curveCompositionStrategyType;
  }

  public SpatialCurveSortPartitionerBase(String[] orderByColumns,
                                         HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy,
                                         HoodieClusteringConfig.SpatialCurveCompositionStrategyType curveCompositionStrategyType) {
    this.orderByColumns = orderByColumns;
    this.layoutOptStrategy = layoutOptStrategy;
    this.curveCompositionStrategyType = curveCompositionStrategyType;
  }

  /**
   * Rejects a sort column that the space-curve helpers cannot look up, before the clustering job
   * is submitted. Both curve-building methods match a sort column against the TOP-LEVEL fields of
   * the frame's schema, by exact name, and neither reports a miss in a way a user can act on:
   * {@link SpaceCurveSortingHelper#orderDataFrameByMappingValues} logs an error and hands back the
   * frame UNORDERED, so the clustering quietly writes an unsorted layout, and the sampling method
   * ends in a bare NoSuchElementException from RangeSample's column map, which does not name the
   * column. So a dotted path, a case-mismatched name or a typo has to fail here instead.
   *
   * <p>This is the space-curve analogue of
   * {@link org.apache.hudi.common.util.SortUtils#validateSortableColumns(String[], HoodieSchema)},
   * which is deliberately laxer - it resolves dotted paths and falls back to a case-insensitive
   * match - because the linear partitioners do sort a nested path (Spark's {@code Column(name)} and
   * getNestedFieldVal both resolve one). The base constructors cannot run this check themselves:
   * they are handed the column names alone, with no schema to resolve them against, so the caller
   * that holds the schema ({@code MultipleSparkJobExecutionStrategy.getPartitioner}) runs it.
   *
   * @param orderByColumns    the configured sort columns, may be null or empty
   * @param schema            schema of the frame the curve is built over, i.e. with the metadata fields
   * @param layoutOptStrategy the space-curve strategy in force, named in the error
   */
  public static void validateOrderByColumns(String[] orderByColumns,
                                            HoodieSchema schema,
                                            HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy) {
    if (orderByColumns == null || orderByColumns.length == 0
        || schema == null || schema.getType() != HoodieSchemaType.RECORD) {
      return;
    }
    // getField is exact-match too, but it throws on an empty name; the field names cover a blank
    // sort column with the same error every other miss gets.
    List<String> topLevelNames = schema.getFields().stream()
        .map(HoodieSchemaField::name).collect(Collectors.toList());
    for (String orderByColumn : orderByColumns) {
      String columnName = orderByColumn.trim();
      if (topLevelNames.stream().noneMatch(name -> name.equals(columnName))) {
        throw new HoodieException(String.format(
            "Sort column '%s' is not a top-level column of the schema; %s layout optimization orders by "
                + "top-level columns only, matched by exact name (SpaceCurveSortingHelper), and would "
                + "otherwise drop it silently. Use a top-level column, or the LINEAR strategy for a nested path.",
            columnName, layoutOptStrategy));
      }
    }
  }

  /**
   * Mapping specified multi need-to-order columns to one dimension while preserving data locality.
   */
  protected Dataset<Row> reorder(Dataset<Row> dataset, int numOutputGroups) {
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

  /**
   * The data is sorted using a function that maps multiple columns into a single dimension.
   * Therefore, it is not sorted by partition.
   */
  @Override
  public boolean arePartitionRecordsSorted() {
    return false;
  }
}
