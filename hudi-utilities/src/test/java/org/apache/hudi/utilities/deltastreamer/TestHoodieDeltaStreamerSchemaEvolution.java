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
import org.apache.hudi.utilities.streamer.HoodieStreamer;

import org.apache.parquet.io.ParquetDecodingException;
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

  private String tableBasePath;

  private HoodieDeltaStreamer.Config setupDeltaStreamer(String tableType,
                                                        String tableBasePath,
                                                        Boolean shouldCluster,
                                                        Boolean shouldCompact,
                                                        Boolean reconcileSchema,
                                                        Boolean rowWriterEnabled,
                                                        Boolean addFilegroups,
                                                        Boolean multiLogFiles) throws IOException {
    TypedProperties extraProps = new TypedProperties();
    extraProps.setProperty("hoodie.datasource.write.table.type", tableType);
    extraProps.setProperty(HoodieCommonConfig.RECONCILE_SCHEMA.key(), reconcileSchema.toString());
    extraProps.setProperty("hoodie.datasource.write.row.writer.enable", rowWriterEnabled.toString());
    extraProps.setProperty("hoodie.parquet.small.file.limit", "0");

    extraProps.setProperty(HoodieCompactionConfig.INLINE_COMPACT.key(), shouldCompact.toString());
    if (shouldCompact) {
      extraProps.setProperty(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1");
    }

    if (shouldCluster) {
      int maxCommits = 2;
      if (addFilegroups) {
        maxCommits++;
      }
      if (multiLogFiles) {
        maxCommits++;
      }
      extraProps.setProperty(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true");
      extraProps.setProperty(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), Integer.toString(maxCommits));
      extraProps.setProperty(HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key(), "_row_key");
    }

    prepareParquetDFSSource(false, false, "", "", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", "", extraProps);
    HoodieDeltaStreamer.Config cfg = TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, ParquetDFSSource.class.getName(),
        null, PROPS_FILENAME_TEST_PARQUET, false,
        false, 100000, false, null, tableType, "timestamp", null);
    cfg.forceDisableCompaction = !shouldCompact;
    return cfg;
  }

  private void assertFileNumber(int expected, boolean isCow) {
    if (isCow) {
      assertBaseFileOnlyNumber(expected);
    } else {
      assertEquals(expected, sparkSession.read().format("hudi").load(tableBasePath).select("_hoodie_commit_time", "_hoodie_file_name").distinct().count());
    }
  }

  private void assertBaseFileOnlyNumber(int expected) {
    Dataset<Row> df = sparkSession.read().format("hudi").load(tableBasePath).select("_hoodie_file_name");
    df.createOrReplaceTempView("assertFileNumberPostCompactCluster");
    assertEquals(df.count(), sparkSession.sql("select * from assertFileNumberPostCompactCluster where _hoodie_file_name  like '%.parquet'").count());
    assertEquals(expected, df.distinct().count());
  }

  private void assertRecordCount(int expected) {
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(expected, tableBasePath, sqlContext);
  }

  private void testBase(String tableType, Boolean shouldCluster, Boolean shouldCompact, Boolean reconcileSchema,
                        Boolean rowWriterEnable, Boolean addFilegroups,  Boolean multiLogFiles, String updateFile, String updateColumn, String condition, int count) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, addFilegroups, multiLogFiles, updateFile, updateColumn, condition, count, true);

    //adding non-nullable cols should fail, but instead it is adding nullable cols
    //assertThrows(Exception.class, () -> testBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, updateFile, updateColumn, condition, count, false));
  }

  private void testBase(String tableType, Boolean shouldCluster, Boolean shouldCompact, Boolean reconcileSchema, Boolean rowWriterEnable, Boolean addFilegroups,
                        Boolean multiLogFiles, String updateFile, String updateColumn, String condition, int count, Boolean nullable) throws Exception {
    boolean isCow = tableType.equals("COPY_ON_WRITE");
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;
    tableBasePath = basePath + "test_parquet_table" + testNum;
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(setupDeltaStreamer(tableType, tableBasePath, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, addFilegroups, multiLogFiles), jsc);

    //first write
    String datapath = String.class.getResource("/data/schema-evolution/start.json").getPath();
    sparkSession.read().json(datapath).write().format("parquet").save(PARQUET_SOURCE_ROOT);
    deltaStreamer.sync();
    int numRecords = 6;
    int numFiles = 3;
    assertRecordCount(numRecords);
    assertFileNumber(numFiles, isCow);


    //add extra log files
    if (multiLogFiles) {
      datapath = String.class.getResource("/data/schema-evolution/extraLogFiles.json").getPath();
      sparkSession.read().json(datapath).write().format("parquet").mode(SaveMode.Append).save(PARQUET_SOURCE_ROOT);
      deltaStreamer.sync();
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, false);
    }

    //make other filegroups
    if (addFilegroups) {
      datapath = String.class.getResource("/data/schema-evolution/newFileGroups.json").getPath();
      sparkSession.read().json(datapath).write().format("parquet").mode(SaveMode.Append).save(PARQUET_SOURCE_ROOT);
      deltaStreamer.sync();
      numRecords += 3;
      numFiles += 3;
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, isCow);
    }

    //write updates
    datapath = String.class.getResource("/data/schema-evolution/" + updateFile).getPath();
    Dataset<Row> df = sparkSession.read().json(datapath);
    if (!nullable) {
      df = TestHoodieSparkUtils.setColumnNotNullable(df, updateColumn);
    }
    df.write().format("parquet").mode(SaveMode.Append).save(PARQUET_SOURCE_ROOT);
    deltaStreamer.sync();
    if (shouldCluster) {
      //everything combines into 1 file per partition
      assertBaseFileOnlyNumber(3);
    } else if (shouldCompact || isCow) {
      assertBaseFileOnlyNumber(numFiles);
    } else {
      numFiles += 2;
      assertFileNumber(numFiles, false);
    }
    assertRecordCount(numRecords);

    sparkSession.read().format("hudi").load(tableBasePath).select(updateColumn).show(9);
    assertEquals(count, sparkSession.read().format("hudi").load(tableBasePath).filter(condition).count());
  }

  private static Stream<Arguments> testArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    //b.add(Arguments.of("MERGE_ON_READ", false, false, false, true, false, true));
    for (Boolean reconcileSchema : new Boolean[]{false, true}) {
      for (Boolean rowWriterEnable : new Boolean[]{true}) {
        for (Boolean addFilegroups : new Boolean[]{false, true}) {
          for (Boolean multiLogFiles : new Boolean[]{false, true}) {
            for (Boolean shouldCluster : new Boolean[]{false, true}) {
              for (String tableType : new String[]{"COPY_ON_WRITE", "MERGE_ON_READ"}) {
                if (!multiLogFiles || tableType.equals("MERGE_ON_READ")) {
                  b.add(Arguments.of(tableType, shouldCluster, false, reconcileSchema, rowWriterEnable, addFilegroups, multiLogFiles));
                }
              }
            }
            b.add(Arguments.of("MERGE_ON_READ", false, true, reconcileSchema, rowWriterEnable, addFilegroups, multiLogFiles));
          }
        }
      }
    }
    return b.build();
  }

  /**
   * Add a new column at root level at the end
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddColRoot(String tableType, Boolean shouldCluster, Boolean shouldCompact, Boolean reconcileSchema, Boolean rowWriterEnable, Boolean addFilegroups, Boolean multiLogFiles) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, addFilegroups, multiLogFiles, "testAddColRoot.json", "zextra_col", "zextra_col = 'yes'", 2);
  }

  /**
   * Add a custom Hudi meta column
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddMetaCol(String tableType, Boolean shouldCluster, Boolean shouldCompact, Boolean reconcileSchema, Boolean rowWriterEnable, Boolean addFilegroups, Boolean multiLogFiles) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, addFilegroups, multiLogFiles, "testAddMetaCol.json", "_extra_col", "_extra_col = 'yes'", 2);
  }

  /**
   * Add a new column to inner struct (at the end)
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddColStruct(String tableType, Boolean shouldCluster, Boolean shouldCompact, Boolean reconcileSchema, Boolean rowWriterEnable, Boolean addFilegroups, Boolean multiLogFiles) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, addFilegroups, multiLogFiles,
        "testAddColStruct.json", "tip_history.zextra_col", "tip_history[0].zextra_col = 'yes'", 2);
  }

  /**
   * Add a new complex type field with default (array)
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddComplexField(String tableType, Boolean shouldCluster, Boolean shouldCompact, Boolean reconcileSchema, Boolean rowWriterEnable, Boolean addFilegroups, Boolean multiLogFiles) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, addFilegroups, multiLogFiles,
        "testAddComplexField.json", "zcomplex_array", "size(zcomplex_array) > 0", 2);
  }

  /**
   * Add a new column and change the ordering of fields
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddColChangeOrder(String tableType, Boolean shouldCluster, Boolean shouldCompact, Boolean reconcileSchema, Boolean rowWriterEnable, Boolean addFilegroups, Boolean multiLogFiles) throws Exception {
    testBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, addFilegroups, multiLogFiles,
        "testAddColChangeOrderAllFiles.json", "extra_col", "extra_col = 'yes'", 2);
    //according to the docs, this should fail. But it doesn't
    //assertThrows(Exception.class, () -> testBase(tableType, shouldCluster, shouldCompact, "testAddColChangeOrderSomeFiles.json", "extra_col", "extra_col = 'yes'", 1));
  }

  private void testTypePromotionBase(String tableType, Boolean shouldCluster, Boolean shouldCompact, Boolean reconcileSchema, Boolean rowWriterEnable,
                                     String colName, DataType startType, DataType endType) throws Exception {
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;
    String tableBasePath = basePath + "test_parquet_table" + testNum;
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(setupDeltaStreamer(tableType, tableBasePath, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, false, false), jsc);

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
    /**
     * Failing test cases
     * MOR - yes cluster -  no compact -  no reconcile - yes row writer
     *    nothing was thrown when promoting "distance_in_meters" from DataTypes.LongType to DataTypes.IntegerType
     * MOR -  no cluster -  no compact -  no reconcile - yes row writer
     *    nothing was thrown when promoting "fare.amount" from double to float
     * MOR -  no cluster -  no compact -  no reconcile -  no row writer
     *    nothing was thrown when promoting "fare.amount" from double to float
     * MOR - yes cluster -  no compact -  no reconcile -  no row writer
     *    can't promote from primitive to string
     * COW - yes cluster -  no compact -  no reconcile -  no row writer
     *    can't promote from primitive to string
     */
    Stream.Builder<Arguments> b = Stream.builder();
    Boolean[] rwe = {true,false};
    Boolean[] sc = {true,false};
    String[] tt = {"COPY_ON_WRITE", "MERGE_ON_READ"};

    for (Boolean rowWriterEnable : rwe) {
      for (Boolean shouldCluster : sc) {
        for (String tableType : tt) {
          b.add(Arguments.of(tableType, shouldCluster, false, false, rowWriterEnable));
        }
      }
      b.add(Arguments.of("MERGE_ON_READ", false, true, false, rowWriterEnable));
    }
    return b.build();
  }

  /**
   * Test type promotion for root level fields
   */
  @ParameterizedTest
  @MethodSource("testTypePromotionArgs")
  public void testTypePromotion(String tableType, Boolean shouldCluster, Boolean shouldCompact, Boolean reconcileSchema, Boolean rowWriterEnable) throws Exception {
    //root data type promotions
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable,"distance_in_meters", DataTypes.IntegerType, DataTypes.LongType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "distance_in_meters", DataTypes.IntegerType, DataTypes.FloatType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "distance_in_meters", DataTypes.IntegerType, DataTypes.DoubleType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "distance_in_meters", DataTypes.IntegerType, DataTypes.StringType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "distance_in_meters", DataTypes.LongType, DataTypes.FloatType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "distance_in_meters", DataTypes.LongType, DataTypes.DoubleType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "distance_in_meters", DataTypes.LongType, DataTypes.StringType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "begin_lat", DataTypes.FloatType, DataTypes.DoubleType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "begin_lat", DataTypes.FloatType, DataTypes.StringType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "rider", DataTypes.StringType, DataTypes.BinaryType);
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "tip_history",
        DataTypes.createArrayType(DataTypes.IntegerType), DataTypes.createArrayType(DataTypes.LongType));
    //Seems to be supported for datasource. See org.apache.hudi.TestAvroSchemaResolutionSupport.testDataTypePromotions
    //testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, "rider", DataTypes.BinaryType, DataTypes.StringType);

    //nested data type promotions
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable,
        "fare", createFareStruct(DataTypes.FloatType), createFareStruct(DataTypes.DoubleType));
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable,
        "fare", createFareStruct(DataTypes.FloatType), createFareStruct(DataTypes.StringType));

    //complex data type promotion
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "tip_history",
        DataTypes.createArrayType(DataTypes.IntegerType), DataTypes.createArrayType(DataTypes.LongType));
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "tip_history",
        DataTypes.createArrayType(DataTypes.IntegerType), DataTypes.createArrayType(DataTypes.DoubleType));
    testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "tip_history",
        DataTypes.createArrayType(DataTypes.IntegerType), DataTypes.createArrayType(DataTypes.StringType));

    //test illegal type promotions
    if (tableType.equals("COPY_ON_WRITE")) {
      //illegal root data type promotion
      SparkException e = assertThrows(SparkException.class,
          () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "distance_in_meters", DataTypes.LongType, DataTypes.IntegerType));
      assertTrue(e.getCause().getCause().getMessage().contains("cannot support rewrite value for schema type: \"int\" since the old schema type is: \"long\""));
      //illegal nested data type promotion
      e = assertThrows(SparkException.class, () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable,
          "fare", createFareStruct(DataTypes.DoubleType), createFareStruct(DataTypes.FloatType)));
      assertTrue(e.getCause().getCause().getMessage().contains("cannot support rewrite value for schema type: \"float\" since the old schema type is: \"double\""));
      //illegal complex data type promotion
      e = assertThrows(SparkException.class,
          () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "tip_history",
              DataTypes.createArrayType(DataTypes.LongType), DataTypes.createArrayType(DataTypes.IntegerType)));
      assertTrue(e.getCause().getCause().getMessage().contains("cannot support rewrite value for schema type: \"int\" since the old schema type is: \"long\""));
    } else {
      //illegal root data type promotion
      if (shouldCompact) {
        assertThrows(HoodieCompactionException.class,
            () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "distance_in_meters", DataTypes.LongType, DataTypes.IntegerType));
        assertThrows(HoodieCompactionException.class, () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable,
            "fare", createFareStruct(DataTypes.DoubleType), createFareStruct(DataTypes.FloatType)));
        assertThrows(HoodieCompactionException.class, () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable,
            "tip_history", DataTypes.createArrayType(DataTypes.LongType), DataTypes.createArrayType(DataTypes.IntegerType)));
        assertThrows(HoodieCompactionException.class, () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable,
            "fare", createFareStruct(DataTypes.DoubleType), createFareStruct(DataTypes.FloatType)));
        assertThrows(HoodieCompactionException.class, () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable,
            "tip_history", DataTypes.createArrayType(DataTypes.LongType), DataTypes.createArrayType(DataTypes.IntegerType)));
      } else {
        SparkException e = assertThrows(SparkException.class,
            () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "distance_in_meters", DataTypes.LongType, DataTypes.IntegerType));
        if (shouldCluster) {
          assertTrue(e.getCause().getCause() instanceof ParquetDecodingException);
        } else {
          assertTrue(e.getCause() instanceof NullPointerException);
        }

        e = assertThrows(SparkException.class, () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable,
            "fare", createFareStruct(DataTypes.DoubleType), createFareStruct(DataTypes.FloatType)));
        assertTrue(e.getCause().getCause() instanceof ParquetDecodingException);
        e = assertThrows(SparkException.class, () -> testTypePromotionBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, "tip_history",
            DataTypes.createArrayType(DataTypes.LongType), DataTypes.createArrayType(DataTypes.IntegerType)));
        assertTrue(e.getCause().getCause() instanceof ParquetDecodingException);
      }
    }
  }

  private StructType createFareStruct(DataType amountType) {
    return DataTypes.createStructType(new StructField[]{new StructField("amount", amountType, true, Metadata.empty()),
        new StructField("currency", DataTypes.StringType, true, Metadata.empty())});
  }
}
