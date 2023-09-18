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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.utilities.sources.ParquetDFSSource;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Objects;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Add test cases for out of the box schema evolution for deltastreamer:
 * https://hudi.apache.org/docs/schema_evolution#out-of-the-box-schema-evolution
 */
public class TestHoodieDeltaStreamerSchemaEvolution extends HoodieDeltaStreamerTestBase {

  private HoodieDeltaStreamer.Config setupDeltaStreamer(String tableType, String tableBasePath, Boolean shouldCluster, Boolean shouldCompact) throws IOException {
    TypedProperties extraProps = new TypedProperties();
    extraProps.setProperty("hoodie.datasource.write.table.type", tableType);

    if (shouldCompact) {
      extraProps.setProperty(HoodieCompactionConfig.INLINE_COMPACT.key(), "true");
      extraProps.setProperty(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1");
    }

    if (shouldCluster) {
      extraProps.setProperty(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true");
      extraProps.setProperty(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "2");
      extraProps.setProperty(HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key(), "_row_key");
    }

    prepareParquetDFSSource(false, false, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", "", extraProps);
    return TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, ParquetDFSSource.class.getName(),
        null, PROPS_FILENAME_TEST_PARQUET, false,
        false, 100000, false, null, tableType, "timestamp", null);
  }

  private void testBase(String tableType, Boolean shouldCluster, Boolean shouldCompact, String updateFile, String updateColumn, String condition, int count) throws Exception {
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;
    String tableBasePath = basePath + "test_parquet_table" + testNum;
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(setupDeltaStreamer(tableType, tableBasePath, shouldCluster, shouldCompact), jsc);

    //first write
    String datapath = String.class.getResource("/data/schema-evolution/start.json").getPath();
    sparkSession.read().json(datapath).write().format("parquet").save(PARQUET_SOURCE_ROOT);
    deltaStreamer.sync();
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(10, tableBasePath, sqlContext);

    //second write
    datapath = String.class.getResource("/data/schema-evolution/" + updateFile).getPath();
    sparkSession.read().json(datapath).write().format("parquet").mode(SaveMode.Append).save(PARQUET_SOURCE_ROOT);
    deltaStreamer.sync();
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(10, tableBasePath, sqlContext);
    sparkSession.read().format("hudi").load(tableBasePath).select(updateColumn).show(10);
    assertEquals(count, sparkSession.read().format("hudi").load(tableBasePath).filter(condition).count());
  }

  private static Stream<Arguments> testArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    b.add(Arguments.of("COPY_ON_WRITE", false, false));
    b.add(Arguments.of("COPY_ON_WRITE", true, false));
    b.add(Arguments.of("MERGE_ON_READ", false, false));
    //b.add(Arguments.of("MERGE_ON_READ", true, false));
    b.add(Arguments.of("MERGE_ON_READ", false, true));
    return b.build();
  }

  /**
   * Add a new nullable column at root level at the end
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddNullableColRoot(String tableType, Boolean shouldCluster, Boolean shouldCompact) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, "testAddNullableColRoot.json", "zextra_nullable_col", "zextra_nullable_col = 'yes'", 2);
  }

  /**
   * Add a custom nullable Hudi meta column
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddNullableMetaCol(String tableType, Boolean shouldCluster, Boolean shouldCompact) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, "testAddNullableMetaCol.json", "_extra_nullable_col", "_extra_nullable_col = 'yes'", 2);
  }

  /**
   * Add a new nullable column to inner struct (at the end)
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddNullableColStruct(String tableType, Boolean shouldCluster, Boolean shouldCompact) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, "testAddNullableColStruct.json", "tip_history.zextra_nullable_col", "tip_history[0].zextra_nullable_col = 'yes'", 2);
  }

  /**
   * Add a new complex type field with default (array)
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddComplexField(String tableType, Boolean shouldCluster, Boolean shouldCompact) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, "testAddComplexField.json", "zcomplex_array", "size(zcomplex_array) > 0", 2);
  }

  /**
   * Add a new nullable column and change the ordering of fields
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddNullableColChangeOrder(String tableType, Boolean shouldCluster, Boolean shouldCompact) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, "testAddNullableColChangeOrderAllFiles.json", "extra_nullable_col", "extra_nullable_col = 'yes'", 2);
    //according to the docs, this should fail. But it doesn't
    testBase(tableType, shouldCluster, shouldCompact, "testAddNullableColChangeOrderSomeFiles.json", "extra_nullable_col", "extra_nullable_col = 'yes'", 1);
  }

  private void testTypePromotionBase(String tableType, Boolean shouldCluster, Boolean shouldCompact, String colName, DataType startType, DataType endType) throws Exception {
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;
    String tableBasePath = basePath + "test_parquet_table" + testNum;
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(setupDeltaStreamer(tableType,tableBasePath, shouldCluster, shouldCompact), jsc);

    //first write
    String datapath = String.class.getResource("/data/schema-evolution/start.json").getPath();
    Dataset<Row> df = sparkSession.read().json(datapath);
    Column col = df.col(colName);
    df = df.withColumn(colName, col.cast(startType));
    df.write().format("parquet").save(PARQUET_SOURCE_ROOT);
    deltaStreamer.sync();
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(10, tableBasePath, sqlContext);
    assertEquals(startType, sparkSession.read().format("hudi").load(tableBasePath).select(colName).schema().fields()[0].dataType());

    //second write
    datapath = Objects.requireNonNull(String.class.getResource("/data/schema-evolution/testTypePromotion.json")).getPath();
    df = sparkSession.read().json(datapath);
    col = df.col(colName);
    df = df.withColumn(colName, col.cast(endType));
    df.write().format("parquet").mode(SaveMode.Append).save(PARQUET_SOURCE_ROOT);
    deltaStreamer.sync();
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(10, tableBasePath, sqlContext);
    sparkSession.read().format("hudi").load(tableBasePath).select(colName).show(10);
    assertEquals(endType, sparkSession.read().format("hudi").load(tableBasePath).select(colName).schema().fields()[0].dataType());
  }

  private static Stream<Arguments> testTypePromotionArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    b.add(Arguments.of("COPY_ON_WRITE", false, false));
    //b.add(Arguments.of("COPY_ON_WRITE", true, false));
    b.add(Arguments.of("MERGE_ON_READ", false, false));
    //b.add(Arguments.of("MERGE_ON_READ", true, false));
    b.add(Arguments.of("MERGE_ON_READ", false, true));
    return b.build();
  }

  /**
   * Test type promotion for root level fields
   */
  @ParameterizedTest
  @MethodSource("testTypePromotionArgs")
  public void testTypePromotion(String tableType, Boolean shouldCluster, Boolean shouldCompact) throws Exception {
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, "distance_in_meters", DataTypes.IntegerType, DataTypes.LongType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, "distance_in_meters", DataTypes.IntegerType, DataTypes.FloatType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, "distance_in_meters", DataTypes.IntegerType, DataTypes.DoubleType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, "distance_in_meters", DataTypes.IntegerType, DataTypes.StringType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, "distance_in_meters", DataTypes.LongType, DataTypes.FloatType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, "distance_in_meters", DataTypes.LongType, DataTypes.DoubleType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, "distance_in_meters", DataTypes.LongType, DataTypes.StringType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, "begin_lat", DataTypes.FloatType, DataTypes.DoubleType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, "begin_lat", DataTypes.FloatType, DataTypes.StringType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, "rider", DataTypes.StringType, DataTypes.BinaryType);
    //testTypePromotionBase(tableType, shouldCluster, shouldCompact, "rider", DataTypes.BinaryType, DataTypes.StringType);
  }

}
