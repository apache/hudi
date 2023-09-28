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
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Add test cases for out of the box schema evolution for deltastreamer:
 * https://hudi.apache.org/docs/schema_evolution#out-of-the-box-schema-evolution
 */
public class TestHoodieDeltaStreamerSchemaEvolution extends HoodieDeltaStreamerTestBase {

  private String tableType;
  private String tableBasePath;
  private Boolean shouldCluster;
  private Boolean shouldCompact;
  private Boolean reconcileSchema;
  private Boolean rowWriterEnable;
  private Boolean addFilegroups;
  private Boolean multiLogFiles;

  private HoodieDeltaStreamer deltaStreamer;

  private HoodieDeltaStreamer.Config getDeltaStreamerConfig() throws IOException {
    TypedProperties extraProps = new TypedProperties();
    extraProps.setProperty("hoodie.datasource.write.table.type", tableType);
    extraProps.setProperty(HoodieCommonConfig.RECONCILE_SCHEMA.key(), reconcileSchema.toString());
    extraProps.setProperty("hoodie.datasource.write.row.writer.enable", rowWriterEnable.toString());
    extraProps.setProperty("hoodie.parquet.small.file.limit", "0");
    int maxCommits = 2;
    if (addFilegroups) {
      maxCommits++;
    }
    if (multiLogFiles) {
      maxCommits++;
    }

    extraProps.setProperty(HoodieCompactionConfig.INLINE_COMPACT.key(), shouldCompact.toString());
    if (shouldCompact) {
      extraProps.setProperty(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), Integer.toString(maxCommits));
    }

    if (shouldCluster) {
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

  private void testBase(String updateFile, String updateColumn, String condition, int count) throws Exception {
    testBase(updateFile, updateColumn, condition, count, true);

    //adding non-nullable cols should fail, but instead it is adding nullable cols
    //assertThrows(Exception.class, () -> testBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, updateFile, updateColumn, condition, count, false));
  }

  private void doFirstDeltaWrite() throws Exception {
    doDeltaWriteBase("start.json", true, false,null);
  }

  private void doFirstDeltaWriteTypePromo(String colName, DataType colType) throws Exception {
    doDeltaWriteBase("startTypePromotion.json", true, false, true, colName, colType);
  }

  private void doDeltaWriteTypePromo(String resourceString, String colName, DataType colType) throws Exception {
    doDeltaWriteBase(resourceString, false, false, true, colName, colType);

  }

  private void doNonNullableDeltaWrite(String resourceString, String colName) throws Exception {
    doDeltaWriteBase(resourceString, false, true, colName);
  }

  private void doDeltaWrite(String resourceString) throws Exception {
    doDeltaWriteBase(resourceString, false, false,null);
  }

  private void doDeltaWriteBase(String resourceString, Boolean isFirst, Boolean nonNullable, String colName) throws Exception {
    doDeltaWriteBase(resourceString, isFirst, nonNullable, false, colName, null);
  }

  private void doDeltaWriteBase(String resourceString, Boolean isFirst, Boolean nonNullable, Boolean castColumn, String colName, DataType colType) throws Exception {
    String datapath = String.class.getResource("/data/schema-evolution/" + resourceString).getPath();
    Dataset<Row> df = sparkSession.read().json(datapath);
    if (nonNullable) {
      df = TestHoodieSparkUtils.setColumnNotNullable(df, colName);
    }
    if (castColumn) {
      Column col = df.col(colName);
      df = df.withColumn(colName, col.cast(colType));
    }
    df.write().format("parquet").mode(isFirst ? SaveMode.Overwrite : SaveMode.Append).save(PARQUET_SOURCE_ROOT);
    deltaStreamer.sync();
  }

  private void testBase(String updateFile, String updateColumn, String condition, int count, Boolean nullable) throws Exception {
    boolean isCow = tableType.equals("COPY_ON_WRITE");
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;
    tableBasePath = basePath + "test_parquet_table" + testNum;
    this.deltaStreamer = new HoodieDeltaStreamer(getDeltaStreamerConfig(), jsc);

    //first write
    doFirstDeltaWrite();
    int numRecords = 6;
    int numFiles = 3;
    assertRecordCount(numRecords);
    assertFileNumber(numFiles, isCow);


    //add extra log files
    if (multiLogFiles) {
      doDeltaWrite("extraLogFiles.json");
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, false);
    }

    //make other filegroups
    if (addFilegroups) {
      doDeltaWrite("newFileGroups.json");
      numRecords += 3;
      numFiles += 3;
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, isCow);
    }

    //write updates
    if (!nullable) {
      doNonNullableDeltaWrite(updateFile, updateColumn);
    } else {
      doDeltaWrite(updateFile);
    }
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
  public void testAddColRoot(String tableType,
                             Boolean shouldCluster,
                             Boolean shouldCompact,
                             Boolean reconcileSchema,
                             Boolean rowWriterEnable,
                             Boolean addFilegroups,
                             Boolean multiLogFiles) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.reconcileSchema = reconcileSchema;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    testBase("testAddColRoot.json", "zextra_col", "zextra_col = 'yes'", 2);
  }

  /**
   * Add a custom Hudi meta column
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddMetaCol(String tableType,
                             Boolean shouldCluster,
                             Boolean shouldCompact,
                             Boolean reconcileSchema,
                             Boolean rowWriterEnable,
                             Boolean addFilegroups,
                             Boolean multiLogFiles) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.reconcileSchema = reconcileSchema;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    testBase("testAddMetaCol.json", "_extra_col", "_extra_col = 'yes'", 2);
  }

  /**
   * Add a new column to inner struct (at the end)
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddColStruct(String tableType,
                               Boolean shouldCluster,
                               Boolean shouldCompact,
                               Boolean reconcileSchema,
                               Boolean rowWriterEnable,
                               Boolean addFilegroups,
                               Boolean multiLogFiles) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.reconcileSchema = reconcileSchema;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    testBase("testAddColStruct.json", "tip_history.zextra_col", "tip_history[0].zextra_col = 'yes'", 2);
  }

  /**
   * Add a new complex type field with default (array)
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddComplexField(String tableType,
                                  Boolean shouldCluster,
                                  Boolean shouldCompact,
                                  Boolean reconcileSchema,
                                  Boolean rowWriterEnable,
                                  Boolean addFilegroups,
                                  Boolean multiLogFiles) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.reconcileSchema = reconcileSchema;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    testBase("testAddComplexField.json", "zcomplex_array", "size(zcomplex_array) > 0", 2);
  }

  /**
   * Add a new column and change the ordering of fields
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddColChangeOrder(String tableType,
                                    Boolean shouldCluster,
                                    Boolean shouldCompact,
                                    Boolean reconcileSchema,
                                    Boolean rowWriterEnable,
                                    Boolean addFilegroups,
                                    Boolean multiLogFiles) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.reconcileSchema = reconcileSchema;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    testBase("testAddColChangeOrderAllFiles.json", "extra_col", "extra_col = 'yes'", 2);
    //according to the docs, this should fail. But it doesn't
    //assertThrows(Exception.class, () -> testBase("testAddColChangeOrderSomeFiles.json", "extra_col", "extra_col = 'yes'", 1));
  }

  private void assertDataType(String colName, DataType expectedType) {
    assertEquals(expectedType, sparkSession.read().format("hudi").load(tableBasePath).select(colName).schema().fields()[0].dataType());
  }

  private void testTypePromotionBase(String colName, DataType startType, DataType endType) throws Exception {
    boolean isCow = tableType.equals("COPY_ON_WRITE");
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;
    tableBasePath = basePath + "test_parquet_table" + testNum;
    this.deltaStreamer = new HoodieDeltaStreamer(getDeltaStreamerConfig(), jsc);

    //first write
    doFirstDeltaWriteTypePromo(colName, startType);
    int numRecords = 6;
    int numFiles = 3;
    assertRecordCount(numRecords);
    assertFileNumber(numFiles, isCow);
    assertDataType(colName, startType);

    //add extra log files
    if (multiLogFiles) {
      doDeltaWriteTypePromo("extraLogFilesTypePromo.json", colName, startType);
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, false);
    }

    //make other filegroups
    if (addFilegroups) {
      doDeltaWriteTypePromo("newFileGroupsTypePromo.json", colName, startType);
      numRecords += 3;
      numFiles += 3;
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, isCow);
    }

    //write updates
    doDeltaWriteTypePromo("endTypePromotion.json", colName, endType);
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
    sparkSession.read().format("hudi").load(tableBasePath).select(colName).show(9);
    assertDataType(colName, reconcileSchema ? startType : endType);
  }

  /**
   * Test type promotion for root level fields
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testTypePromotion(String tableType,
                                Boolean shouldCluster,
                                Boolean shouldCompact,
                                Boolean reconcileSchema,
                                Boolean rowWriterEnable,
                                Boolean addFilegroups,
                                Boolean multiLogFiles) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.reconcileSchema = reconcileSchema;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    //root data type promotions
    testTypePromotionBase("distance_in_meters", DataTypes.IntegerType, DataTypes.LongType);
    testTypePromotionBase("distance_in_meters", DataTypes.IntegerType, DataTypes.FloatType);
    testTypePromotionBase("distance_in_meters", DataTypes.IntegerType, DataTypes.DoubleType);
    testTypePromotionBase("distance_in_meters", DataTypes.IntegerType, DataTypes.StringType);
    testTypePromotionBase("distance_in_meters", DataTypes.LongType, DataTypes.FloatType);
    testTypePromotionBase("distance_in_meters", DataTypes.LongType, DataTypes.DoubleType);
    testTypePromotionBase("distance_in_meters", DataTypes.LongType, DataTypes.StringType);
    testTypePromotionBase("begin_lat", DataTypes.FloatType, DataTypes.DoubleType);
    testTypePromotionBase("begin_lat", DataTypes.FloatType, DataTypes.StringType);
    testTypePromotionBase("rider", DataTypes.StringType, DataTypes.BinaryType);
    testTypePromotionBase("tip_history", DataTypes.createArrayType(DataTypes.IntegerType), DataTypes.createArrayType(DataTypes.LongType));
    //Seems to be supported for datasource. See org.apache.hudi.TestAvroSchemaResolutionSupport.testDataTypePromotions
    //testTypePromotionBase("rider", DataTypes.BinaryType, DataTypes.StringType);

    //nested data type promotions
    testTypePromotionBase("fare", createFareStruct(DataTypes.FloatType), createFareStruct(DataTypes.DoubleType));
    testTypePromotionBase("fare", createFareStruct(DataTypes.FloatType), createFareStruct(DataTypes.StringType));

    //complex data type promotion
    testTypePromotionBase("tip_history", DataTypes.createArrayType(DataTypes.IntegerType), DataTypes.createArrayType(DataTypes.LongType));
    testTypePromotionBase("tip_history", DataTypes.createArrayType(DataTypes.IntegerType), DataTypes.createArrayType(DataTypes.DoubleType));
    testTypePromotionBase("tip_history", DataTypes.createArrayType(DataTypes.IntegerType), DataTypes.createArrayType(DataTypes.StringType));

    //test illegal type promotions
    if (tableType.equals("COPY_ON_WRITE")) {
      //illegal root data type promotion
      SparkException e = assertThrows(SparkException.class,
          () -> testTypePromotionBase("distance_in_meters", DataTypes.LongType, DataTypes.IntegerType));
      assertTrue(e.getCause().getCause().getMessage().contains("cannot support rewrite value for schema type: \"int\" since the old schema type is: \"long\""));
      //illegal nested data type promotion
      e = assertThrows(SparkException.class,
          () -> testTypePromotionBase("fare", createFareStruct(DataTypes.DoubleType), createFareStruct(DataTypes.FloatType)));
      assertTrue(e.getCause().getCause().getMessage().contains("cannot support rewrite value for schema type: \"float\" since the old schema type is: \"double\""));
      //illegal complex data type promotion
      e = assertThrows(SparkException.class,
          () -> testTypePromotionBase("tip_history", DataTypes.createArrayType(DataTypes.LongType), DataTypes.createArrayType(DataTypes.IntegerType)));
      assertTrue(e.getCause().getCause().getMessage().contains("cannot support rewrite value for schema type: \"int\" since the old schema type is: \"long\""));
    } else {
      //illegal root data type promotion
      if (shouldCompact) {
        assertThrows(HoodieCompactionException.class,
            () -> testTypePromotionBase("distance_in_meters", DataTypes.LongType, DataTypes.IntegerType));
        assertThrows(HoodieCompactionException.class,
            () -> testTypePromotionBase("fare", createFareStruct(DataTypes.DoubleType), createFareStruct(DataTypes.FloatType)));
        assertThrows(HoodieCompactionException.class,
            () -> testTypePromotionBase("tip_history", DataTypes.createArrayType(DataTypes.LongType), DataTypes.createArrayType(DataTypes.IntegerType)));
        assertThrows(HoodieCompactionException.class,
            () -> testTypePromotionBase("fare", createFareStruct(DataTypes.DoubleType), createFareStruct(DataTypes.FloatType)));
        assertThrows(HoodieCompactionException.class,
            () -> testTypePromotionBase("tip_history", DataTypes.createArrayType(DataTypes.LongType), DataTypes.createArrayType(DataTypes.IntegerType)));
      } else {
        SparkException e = assertThrows(SparkException.class,
            () -> testTypePromotionBase("distance_in_meters", DataTypes.LongType, DataTypes.IntegerType));
        if (shouldCluster) {
          assertTrue(e.getCause().getCause() instanceof ParquetDecodingException);
        } else {
          assertTrue(e.getCause() instanceof NullPointerException);
        }

        e = assertThrows(SparkException.class,
            () -> testTypePromotionBase("fare", createFareStruct(DataTypes.DoubleType), createFareStruct(DataTypes.FloatType)));
        assertTrue(e.getCause().getCause() instanceof ParquetDecodingException);
        e = assertThrows(SparkException.class,
            () -> testTypePromotionBase("tip_history", DataTypes.createArrayType(DataTypes.LongType), DataTypes.createArrayType(DataTypes.IntegerType)));
        assertTrue(e.getCause().getCause() instanceof ParquetDecodingException);
      }
    }
  }

  private StructType createFareStruct(DataType amountType) {
    return DataTypes.createStructType(new StructField[]{new StructField("amount", amountType, true, Metadata.empty()),
        new StructField("currency", DataTypes.StringType, true, Metadata.empty())});
  }
}
