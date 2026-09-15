/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.execution;

import org.apache.hudi.common.util.BinaryUtil;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.sort.SpaceCurveSortingHelper;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.util.JavaScalaConverters;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.spark_partition_id;

class TestRangeSampleSort extends HoodieClientTestBase {

  @Test
  void sortDataFrameBySampleSupportAllTypes() {
    Dataset<Row> df = this.context.getSqlContext().sql("select 1 as id, array(2) as content");
    for (int i = 0; i < 2; i++) {
      final int limit = i;
      Assertions.assertDoesNotThrow(() ->
          RangeSampleSort$.MODULE$.sortDataFrameBySampleSupportAllTypes(df.limit(limit),
              JavaScalaConverters.convertJavaListToScalaSeq(Arrays.asList("id", "content")), 1), "range sort shall not fail when 0 or 1 record incoming");
    }
  }

  @Test
  void sortDataFrameBySample() {
    HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy = HoodieClusteringConfig.LayoutOptimizationStrategy.HILBERT;
    Dataset<Row> df = this.context.getSqlContext().sql("select 1 as id, 2 as content");
    for (int i = 0; i < 2; i++) {
      final int limit = i;
      Assertions.assertDoesNotThrow(() ->
          RangeSampleSort$.MODULE$.sortDataFrameBySample(df.limit(limit), layoutOptStrategy,
              JavaScalaConverters.convertJavaListToScalaSeq(Arrays.asList("id", "content")), 1), "range sort shall not fail when 0 or 1 record incoming");
    }
  }

  /**
   * Builds a two-column integer frame; call sites pass an explicit output partition
   * count so the resulting ordering is deterministic and can be asserted row by row.
   */
  private Dataset<Row> buildTwoIntColumnFrame(List<Row> rows) {
    StructType schema = new StructType(new StructField[] {
        new StructField("c1", DataTypes.IntegerType, true, Metadata.empty()),
        new StructField("c2", DataTypes.IntegerType, true, Metadata.empty())
    });
    return sparkSession.createDataFrame(rows, schema);
  }

  /**
   * Recomputes the Z-curve ordinal for a two-int row using the same public byte mapping
   * the helper relies on, so the expected ordering is derived independently.
   */
  private byte[] expectedZOrdinal(Integer c1, Integer c2) {
    byte[] b1 = BinaryUtil.intTo8Byte(c1 == null ? Integer.MAX_VALUE : c1);
    byte[] b2 = BinaryUtil.intTo8Byte(c2 == null ? Integer.MAX_VALUE : c2);
    return BinaryUtil.interleaving(new byte[][] {b1, b2}, 8);
  }

  @Test
  void orderByMappingValuesZOrderPreservesSchemaAndSortsByCurve() {
    List<Row> rows = Arrays.asList(
        RowFactory.create(5, 5),
        RowFactory.create(1, 9),
        RowFactory.create(9, 1),
        RowFactory.create(1, 1));
    Dataset<Row> df = buildTwoIntColumnFrame(rows);

    Dataset<Row> ordered = SpaceCurveSortingHelper.orderDataFrameByMappingValues(
        df, HoodieClusteringConfig.LayoutOptimizationStrategy.ZORDER, Arrays.asList("c1", "c2"), 1);

    // The helper drops the internal Index column, so the output schema must match the input exactly.
    Assertions.assertArrayEquals(
        new String[] {"c1", "c2"}, ordered.schema().fieldNames(),
        "z-order output must retain only the original columns");

    List<Row> result = ordered.collectAsList();
    Assertions.assertEquals(rows.size(), result.size(), "no rows should be dropped");

    // Every emitted row must be non-decreasing under the independently recomputed Z-curve ordinal.
    for (int i = 1; i < result.size(); i++) {
      byte[] prev = expectedZOrdinal(result.get(i - 1).getInt(0), result.get(i - 1).getInt(1));
      byte[] curr = expectedZOrdinal(result.get(i).getInt(0), result.get(i).getInt(1));
      Assertions.assertTrue(BinaryUtil.compareTo(prev, 0, prev.length, curr, 0, curr.length) <= 0,
          "row " + i + " violates ascending Z-curve order");
    }

    // (1,1) has the smallest interleaved ordinal, so it must sort first.
    Row first = result.get(0);
    Assertions.assertEquals(1, first.getInt(0));
    Assertions.assertEquals(1, first.getInt(1));
  }

  @Test
  void orderByMappingValuesZOrderPlacesNullLast() {
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(2, 2));
    rows.add(RowFactory.create(null, null));
    rows.add(RowFactory.create(1, 1));
    Dataset<Row> df = buildTwoIntColumnFrame(rows);

    Dataset<Row> ordered = SpaceCurveSortingHelper.orderDataFrameByMappingValues(
        df, HoodieClusteringConfig.LayoutOptimizationStrategy.ZORDER, Arrays.asList("c1", "c2"), 1);

    List<Row> result = ordered.collectAsList();
    Assertions.assertEquals(3, result.size());
    // Nulls map to Integer.MAX_VALUE in the byte mapping, so the all-null row sorts last.
    Row last = result.get(result.size() - 1);
    Assertions.assertTrue(last.isNullAt(0) && last.isNullAt(1),
        "the all-null row must sort last under Z-curve mapping");
  }

  @Test
  void orderByMappingValuesHilbertPreservesRowsAndSchema() {
    List<Row> rows = Arrays.asList(
        RowFactory.create(7, 3),
        RowFactory.create(0, 0),
        RowFactory.create(4, 4));
    Dataset<Row> df = buildTwoIntColumnFrame(rows);

    Dataset<Row> ordered = SpaceCurveSortingHelper.orderDataFrameByMappingValues(
        df, HoodieClusteringConfig.LayoutOptimizationStrategy.HILBERT, Arrays.asList("c1", "c2"), 1);

    Assertions.assertArrayEquals(new String[] {"c1", "c2"}, ordered.schema().fieldNames(),
        "hilbert output must retain only the original columns");
    List<Row> result = ordered.collectAsList();
    Assertions.assertEquals(rows.size(), result.size(), "hilbert ordering must not drop rows");
    // Origin (0,0) maps to the smallest Hilbert index, so it must sort first.
    Assertions.assertEquals(0, result.get(0).getInt(0));
    Assertions.assertEquals(0, result.get(0).getInt(1));
  }

  @Test
  void orderByMappingValuesSingleColumnRangePartitionsRows() {
    List<Row> rows = Arrays.asList(
        RowFactory.create(3, 30),
        RowFactory.create(1, 10),
        RowFactory.create(2, 20),
        RowFactory.create(4, 40));
    Dataset<Row> df = buildTwoIntColumnFrame(rows);

    // A single ordering column short-circuits space-curve mapping into a plain range
    // repartition. Spark's repartitionByRange does not sort rows within a partition,
    // so only the cross-partition range contract can be asserted.
    Dataset<Row> ordered = SpaceCurveSortingHelper.orderDataFrameByMappingValues(
        df, HoodieClusteringConfig.LayoutOptimizationStrategy.ZORDER, Arrays.asList("c1"), 2);

    Assertions.assertArrayEquals(new String[] {"c1", "c2"}, ordered.schema().fieldNames(),
        "single-column ordering must not add an Index column");

    List<Row> result = ordered.withColumn("pid", spark_partition_id()).collectAsList();
    Assertions.assertEquals(rows.size(), result.size(), "no rows should be dropped");

    // Range partitioning must not overlap ranges across partitions: every c1 in a
    // lower-numbered partition is <= every c1 in a higher-numbered partition.
    for (Row left : result) {
      for (Row right : result) {
        if (left.getInt(2) < right.getInt(2)) {
          Assertions.assertTrue(left.getInt(0) <= right.getInt(0),
              "partition ranges must not overlap");
        }
      }
    }

    // With four distinct keys and two target partitions the deterministic range
    // bounds split the rows across both partitions, so the check above is not vacuous.
    Assertions.assertEquals(2,
        result.stream().map(r -> r.getInt(2)).collect(Collectors.toSet()).size(),
        "rows must be spread across both requested partitions");

    // The row multiset must be preserved: each c1 appears once with its matching c2.
    List<Integer> c1Values = new ArrayList<>();
    for (Row r : result) {
      Assertions.assertEquals(r.getInt(0) * 10, r.getInt(1), "row content must be unchanged");
      c1Values.add(r.getInt(0));
    }
    c1Values.sort(Integer::compareTo);
    Assertions.assertEquals(Arrays.asList(1, 2, 3, 4), c1Values,
        "range repartition must preserve all rows");
  }

  @Test
  void orderByMappingValuesUnknownColumnReturnsInputUnchanged() {
    List<Row> rows = Arrays.asList(RowFactory.create(2, 2), RowFactory.create(1, 1));
    Dataset<Row> df = buildTwoIntColumnFrame(rows);

    // Ordering by a column absent from the schema must be a no-op that returns the same frame.
    Dataset<Row> ordered = SpaceCurveSortingHelper.orderDataFrameByMappingValues(
        df, HoodieClusteringConfig.LayoutOptimizationStrategy.ZORDER, Arrays.asList("missing"), 1);

    Assertions.assertSame(df, ordered, "missing order column must return the untouched input frame");
  }
}
