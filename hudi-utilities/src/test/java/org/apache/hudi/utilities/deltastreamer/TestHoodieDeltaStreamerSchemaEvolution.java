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

import org.apache.hudi.TestHoodieSparkUtils;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.utilities.sources.ParquetDFSSource;

import org.apache.spark.SparkException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Objects;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Add test cases for out of the box schema evolution for deltastreamer:
 * https://hudi.apache.org/docs/schema_evolution#out-of-the-box-schema-evolution
 */
public class TestHoodieDeltaStreamerSchemaEvolution extends HoodieDeltaStreamerTestBase {

  private HoodieDeltaStreamer.Config setupDeltaStreamer(String tableType, String tableBasePath, Boolean shouldCluster, Boolean shouldCompact, Boolean reconcileSchema) throws IOException {
    TypedProperties extraProps = new TypedProperties();
    extraProps.setProperty("hoodie.datasource.write.table.type", tableType);
    extraProps.setProperty(HoodieCommonConfig.RECONCILE_SCHEMA.key(), reconcileSchema.toString());

    if (shouldCompact) {
      extraProps.setProperty(HoodieCompactionConfig.INLINE_COMPACT.key(), "true");
      extraProps.setProperty(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1");
    }

    if (shouldCluster) {
      extraProps.setProperty(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true");
      extraProps.setProperty(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "2");
      extraProps.setProperty(HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key(), "_row_key");
    }

    prepareParquetDFSSource(false, false, "", "", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", "", extraProps);
    return TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, ParquetDFSSource.class.getName(),
        null, PROPS_FILENAME_TEST_PARQUET, false,
        false, 100000, false, null, tableType, "timestamp", null);
  }

  private void testBase(String tableType, Boolean shouldCluster, Boolean shouldCompact, String updateFile, String updateColumn, String condition, int count) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, updateFile, updateColumn, condition, count, true, false);
    testBase(tableType, shouldCluster, shouldCompact, updateFile, updateColumn, condition, count, true, true);
    //adding non-nullable cols should fail, but instead it is adding nullable cols
    //assertThrows(Exception.class, () -> testBase(tableType, shouldCluster, shouldCompact, updateFile, updateColumn, condition, count, false));
  }

  private void testBase(String tableType, Boolean shouldCluster, Boolean shouldCompact, String updateFile, String updateColumn,
                        String condition, int count, Boolean nullable, Boolean reconcileSchema) throws Exception {
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;
    String tableBasePath = basePath + "test_parquet_table" + testNum;
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(setupDeltaStreamer(tableType, tableBasePath, shouldCluster, shouldCompact, reconcileSchema), jsc);

    //first write
    String datapath = String.class.getResource("/data/schema-evolution/start.json").getPath();
    sparkSession.read().json(datapath).write().format("parquet").save(PARQUET_SOURCE_ROOT);
    deltaStreamer.sync();
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(10, tableBasePath, sqlContext);

    //second write
    datapath = String.class.getResource("/data/schema-evolution/" + updateFile).getPath();
    Dataset<Row> df = sparkSession.read().json(datapath);
    if (!nullable) {
      df = TestHoodieSparkUtils.setColumnNotNullable(df, updateColumn);
    }
    df.write().format("parquet").mode(SaveMode.Append).save(PARQUET_SOURCE_ROOT);
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
   * Add a new column at root level at the end
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddColRoot(String tableType, Boolean shouldCluster, Boolean shouldCompact) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, "testAddColRoot.json", "zextra_col", "zextra_col = 'yes'", 2);
  }

  /**
   * Add a custom Hudi meta column
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddMetaCol(String tableType, Boolean shouldCluster, Boolean shouldCompact) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, "testAddMetaCol.json", "_extra_col", "_extra_col = 'yes'", 2);
  }

  /**
   * Add a new column to inner struct (at the end)
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddColStruct(String tableType, Boolean shouldCluster, Boolean shouldCompact) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, "testAddColStruct.json", "tip_history.zextra_col", "tip_history[0].zextra_col = 'yes'", 2);
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
   * Add a new column and change the ordering of fields
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddColChangeOrder(String tableType, Boolean shouldCluster, Boolean shouldCompact) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, "testAddColChangeOrderAllFiles.json", "extra_col", "extra_col = 'yes'", 2);
    //according to the docs, this should fail. But it doesn't
    //assertThrows(Exception.class, () -> testBase(tableType, shouldCluster, shouldCompact, "testAddColChangeOrderSomeFiles.json", "extra_col", "extra_col = 'yes'", 1));
  }

  private void testTypePromotionBase(String tableType, Boolean shouldCluster, Boolean shouldCompact, Boolean reconcileSchema, String colName, DataType startType, DataType endType) throws Exception {
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;
    String tableBasePath = basePath + "test_parquet_table" + testNum;
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(setupDeltaStreamer(tableType,tableBasePath, shouldCluster, shouldCompact, reconcileSchema), jsc);

    //first write
    String datapath = String.class.getResource("/data/schema-evolution/startTypePromotion.json").getPath();
    Dataset<Row> df = sparkSession.read().json(datapath);
    Column col = df.col(colName);
    df = df.withColumn(colName, col.cast(startType));
    df.write().format("parquet").save(PARQUET_SOURCE_ROOT);
    deltaStreamer.sync();
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(10, tableBasePath, sqlContext);
    assertEquals(startType, sparkSession.read().format("hudi").load(tableBasePath).select(colName).schema().fields()[0].dataType());

    //second write
    datapath = Objects.requireNonNull(String.class.getResource("/data/schema-evolution/endTypePromotion.json")).getPath();
    df = sparkSession.read().json(datapath);
    col = df.col(colName);
    df = df.withColumn(colName, col.cast(endType));
    df.write().format("parquet").mode(SaveMode.Append).save(PARQUET_SOURCE_ROOT);
    deltaStreamer.sync();
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(10, tableBasePath, sqlContext);
    sparkSession.read().format("hudi").load(tableBasePath).select(colName).show(10);
    assertEquals(reconcileSchema ? startType : endType, sparkSession.read().format("hudi").load(tableBasePath).select(colName).schema().fields()[0].dataType());
  }

  private static Stream<Arguments> testTypePromotionArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    b.add(Arguments.of("COPY_ON_WRITE", false, false, false));
    b.add(Arguments.of("MERGE_ON_READ", false, false, false));
    /*
    clustering fails for MOR and COW
    b.add(Arguments.of("COPY_ON_WRITE", true, false, false));
    b.add(Arguments.of("MERGE_ON_READ", true, false, false));
    */
    b.add(Arguments.of("MERGE_ON_READ", false, true, false));
    return b.build();
  }

  /**
   * Test type promotion for root level fields
   */
  @ParameterizedTest
  @MethodSource("testTypePromotionArgs")
  public void testTypePromotion(String tableType, Boolean shouldCluster, Boolean shouldCompact, Boolean reconcileSchema) throws Exception {
    //root data type promotions
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "distance_in_meters", DataTypes.IntegerType, DataTypes.LongType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "distance_in_meters", DataTypes.IntegerType, DataTypes.FloatType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "distance_in_meters", DataTypes.IntegerType, DataTypes.DoubleType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "distance_in_meters", DataTypes.IntegerType, DataTypes.StringType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "distance_in_meters", DataTypes.LongType, DataTypes.FloatType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "distance_in_meters", DataTypes.LongType, DataTypes.DoubleType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "distance_in_meters", DataTypes.LongType, DataTypes.StringType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "begin_lat", DataTypes.FloatType, DataTypes.DoubleType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "begin_lat", DataTypes.FloatType, DataTypes.StringType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "rider", DataTypes.StringType, DataTypes.BinaryType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "tip_history",
        DataTypes.createArrayType(DataTypes.IntegerType), DataTypes.createArrayType(DataTypes.LongType));
    //Seems to be supported for datasource. See org.apache.hudi.TestAvroSchemaResolutionSupport.testDataTypePromotions
    //testTypePromotionBase(tableType, shouldCluster, shouldCompact, "rider", DataTypes.BinaryType, DataTypes.StringType);

    //nested data type promotions
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "fare", createFareStruct(DataTypes.FloatType), createFareStruct(DataTypes.DoubleType));
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "fare", createFareStruct(DataTypes.FloatType), createFareStruct(DataTypes.StringType));

    //complex data type promotion
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "tip_history",
        DataTypes.createArrayType(DataTypes.IntegerType), DataTypes.createArrayType(DataTypes.LongType));
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "tip_history",
        DataTypes.createArrayType(DataTypes.IntegerType), DataTypes.createArrayType(DataTypes.StringType));

    //test illegal type promotions
    if (tableType.equals("COPY_ON_WRITE")) {
      //illegal root data type promotion
      SparkException e = assertThrows(SparkException.class,
          () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "distance_in_meters", DataTypes.LongType, DataTypes.IntegerType));
      assertTrue(e.getCause().getCause().getMessage().contains("cannot support rewrite value for schema type: \"int\" since the old schema type is: \"long\""));
      //illegal nested data type promotion
      e = assertThrows(SparkException.class,
          () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "fare", createFareStruct(DataTypes.DoubleType), createFareStruct(DataTypes.FloatType)));
      assertTrue(e.getCause().getCause().getMessage().contains("cannot support rewrite value for schema type: \"float\" since the old schema type is: \"double\""));
      //illegal complex data type promotion
      e = assertThrows(SparkException.class,
          () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "tip_history",
              DataTypes.createArrayType(DataTypes.LongType), DataTypes.createArrayType(DataTypes.IntegerType)));
      assertTrue(e.getCause().getCause().getMessage().contains("cannot support rewrite value for schema type: \"int\" since the old schema type is: \"long\""));
    } else {
      //illegal root data type promotion
      if (shouldCompact) {
        assertThrows(HoodieCompactionException.class,
            () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "distance_in_meters", DataTypes.LongType, DataTypes.IntegerType));
      } else {
        SparkException e = assertThrows(SparkException.class,
            () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "distance_in_meters", DataTypes.LongType, DataTypes.IntegerType));
        assertTrue(e.getCause() instanceof NullPointerException);
      }

      //nested and complex do not fail even though they should
    }
  }

  private StructType createFareStruct(DataType amountType) {
    return DataTypes.createStructType(new StructField[]{new StructField("amount", amountType, true, Metadata.empty()),
        new StructField("currency", DataTypes.StringType, true, Metadata.empty())});
  }
}
