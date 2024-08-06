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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.streamer.ErrorEvent;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * Takes hours to run. Use to debug schema evolution. Don't enable for ci
 */
@Disabled
public class TestHoodieDeltaStreamerSchemaEvolutionExtensive extends TestHoodieDeltaStreamerSchemaEvolutionBase {

  protected void testBase(String updateFile, String updateColumn, String condition, int count) throws Exception {
    testBase(updateFile, updateColumn, condition, count, null);
  }

  protected void testBase(String updateFile, String updateColumn, String condition, int count, ErrorEvent.ErrorReason reason) throws Exception {
    Map<String,Integer> conditions = new HashMap<>();
    conditions.put(condition, count);
    testBase(updateFile, updateColumn, conditions, true, reason);

    //adding non-nullable cols should fail, but instead it is adding nullable cols
    //assertThrows(Exception.class, () -> testBase(tableType, shouldCluster, shouldCompact, reconcileSchema, rowWriterEnable, updateFile, updateColumn, condition, count, false));
  }

  protected void testBase(String updateFile, String updateColumn, Map<String,Integer> conditions) throws Exception {
    testBase(updateFile, updateColumn, conditions, null);
  }

  protected void testBase(String updateFile, String updateColumn, Map<String,Integer> conditions, ErrorEvent.ErrorReason reason) throws Exception {
    testBase(updateFile, updateColumn, conditions, true, reason);
  }

  protected void doFirstDeltaWrite() throws Exception {
    doDeltaWriteBase("start.json", true, false,null);
  }

  protected void doFirstDeltaWriteTypePromo(String colName, DataType colType) throws Exception {
    doDeltaWriteBase("startTypePromotion.json", true, false, true, colName, colType);
  }

  protected void doDeltaWriteTypePromo(String resourceString, String colName, DataType colType) throws Exception {
    doDeltaWriteBase(resourceString, false, false, true, colName, colType);

  }

  protected void doNonNullableDeltaWrite(String resourceString, String colName) throws Exception {
    doDeltaWriteBase(resourceString, false, true, colName);
  }

  protected void doDeltaWrite(String resourceString) throws Exception {
    doDeltaWriteBase(resourceString, false, false,null);
  }

  protected void doDeltaWriteBase(String resourceString, Boolean isFirst, Boolean nonNullable, String colName) throws Exception {
    doDeltaWriteBase(resourceString, isFirst, nonNullable, false, colName, null);
  }

  protected void doDeltaWriteBase(String resourceString, Boolean isFirst, Boolean nonNullable, Boolean castColumn, String colName, DataType colType) throws Exception {
    String datapath = String.class.getResource("/data/schema-evolution/" + resourceString).getPath();
    Dataset<Row> df = sparkSession.read().json(datapath);
    if (nonNullable) {
      df = TestHoodieSparkUtils.setColumnNotNullable(df, colName);
    }
    if (castColumn) {
      Column col = df.col(colName);
      df = df.withColumn(colName, col.cast(colType));
    }

    addData(df, isFirst);
    deltaStreamer.sync();
  }

  /**
   * Main testing logic for non-type promotion tests
   */
  protected void testBase(String updateFile, String updateColumn, Map<String,Integer> conditions, Boolean nullable, ErrorEvent.ErrorReason reason) throws Exception {
    boolean isCow = tableType.equals("COPY_ON_WRITE");
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;
    tableName = "test_parquet_table" + testNum;
    tableBasePath = basePath + tableName;
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
      //this write contains updates for the 6 records from the first write, so
      //although we have 2 files for each filegroup, we only see the log files
      //represented in the read. So that is why numFiles is 3, not 6
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
      if (updateFile.equals("testAddColChangeOrderAllFiles.json")) {
        //this test updates all 3 partitions instead of 2 like the rest of the tests
        numFiles++;
      } else if (withErrorTable) {
        numFiles--;
      }
      assertFileNumber(numFiles, false);
    }
    assertRecordCount(numRecords);

    Dataset<Row> df = sparkSession.read().format("hudi").load(tableBasePath);
    df.show(9,false);
    df.select(updateColumn).show(9);
    for (String condition : conditions.keySet()) {
      assertEquals(conditions.get(condition).intValue(), df.filter(condition).count());
    }

    if (withErrorTable) {
      List<ErrorEvent> recs = new ArrayList<>();
      for (String key : TestErrorTable.commited.keySet()) {
        Option<JavaRDD> errors = TestErrorTable.commited.get(key);
        if (errors.isPresent()) {
          if (!errors.get().isEmpty()) {
            recs.addAll(errors.get().collect());
          }
        }
      }
      assertEquals(1, recs.size());
      assertEquals(recs.get(0).getReason(), reason);
    }
  }

  protected static Stream<Arguments> testArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    //only testing row-writer enabled for now
    for (Boolean rowWriterEnable : new Boolean[]{false, true}) {
      for (Boolean addFilegroups : new Boolean[]{false, true}) {
        for (Boolean multiLogFiles : new Boolean[]{false, true}) {
          for (Boolean shouldCluster : new Boolean[]{false, true}) {
            for (String tableType : new String[]{"COPY_ON_WRITE", "MERGE_ON_READ"}) {
              if (!multiLogFiles || tableType.equals("MERGE_ON_READ")) {
                b.add(Arguments.of(tableType, shouldCluster, false, rowWriterEnable, addFilegroups, multiLogFiles));
              }
            }
          }
          b.add(Arguments.of("MERGE_ON_READ", false, true, rowWriterEnable, addFilegroups, multiLogFiles));
        }
      }
    }
    return b.build();
  }

  @ParameterizedTest
  @MethodSource("testArgs")
  public void testErrorTable(String tableType,
                             Boolean shouldCluster,
                             Boolean shouldCompact,
                             Boolean rowWriterEnable,
                             Boolean addFilegroups,
                             Boolean multiLogFiles) throws Exception {
    this.withErrorTable = true;
    this.useSchemaProvider = false;
    this.useTransformer = false;
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    testBase("testMissingRecordKey.json", "driver", "driver = 'driver-003'", 1, ErrorEvent.ErrorReason.RECORD_CREATION);
  }

  @ParameterizedTest
  @MethodSource("testArgs")
  public void testErrorTableWithSchemaProvider(String tableType,
                                               Boolean shouldCluster,
                                               Boolean shouldCompact,
                                               Boolean rowWriterEnable,
                                               Boolean addFilegroups,
                                               Boolean multiLogFiles) throws Exception {
    this.withErrorTable = true;
    this.useSchemaProvider = true;
    this.useTransformer = false;
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    testBase("testMissingRecordKey.json", "driver", "driver = 'driver-003'", 1, ErrorEvent.ErrorReason.INVALID_RECORD_SCHEMA);
  }

  @ParameterizedTest
  @MethodSource("testArgs")
  public void testErrorTableWithTransformer(String tableType,
                             Boolean shouldCluster,
                             Boolean shouldCompact,
                             Boolean rowWriterEnable,
                             Boolean addFilegroups,
                             Boolean multiLogFiles) throws Exception {
    this.withErrorTable = true;
    this.useSchemaProvider = true;
    this.useTransformer = true;
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    testBase("testMissingRecordKey.json", "driver", "driver = 'driver-003'", 1, ErrorEvent.ErrorReason.AVRO_DESERIALIZATION_FAILURE);
  }

  /**
   * Add a new column at root level at the end
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddColRoot(String tableType,
                             Boolean shouldCluster,
                             Boolean shouldCompact,
                             Boolean rowWriterEnable,
                             Boolean addFilegroups,
                             Boolean multiLogFiles) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    testBase("testAddColRoot.json", "zextra_col", "zextra_col = 'yes'", 2);
  }

  /**
   * Drop a root column
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testDropColRoot(String tableType,
                              Boolean shouldCluster,
                              Boolean shouldCompact,
                              Boolean rowWriterEnable,
                              Boolean addFilegroups,
                              Boolean multiLogFiles) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    testBase("testDropColRoot.json", "trip_type", "trip_type is NULL", 2);
  }

  /**
   * Add a custom Hudi meta column
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddMetaCol(String tableType,
                             Boolean shouldCluster,
                             Boolean shouldCompact,
                             Boolean rowWriterEnable,
                             Boolean addFilegroups,
                             Boolean multiLogFiles) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
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
                               Boolean rowWriterEnable,
                               Boolean addFilegroups,
                               Boolean multiLogFiles) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    testBase("testAddColStruct.json", "tip_history.zextra_col", "tip_history[0].zextra_col = 'yes'", 2);
  }

  /**
   * Drop a root column
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testDropColStruct(String tableType,
                                Boolean shouldCluster,
                                Boolean shouldCompact,
                                Boolean rowWriterEnable,
                                Boolean addFilegroups,
                                Boolean multiLogFiles) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    testBase("testDropColStruct.json", "tip_history.currency", "tip_history[0].currency is NULL", 2);
  }

  /**
   * Add a new complex type field with default (array)
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddComplexField(String tableType,
                                  Boolean shouldCluster,
                                  Boolean shouldCompact,
                                  Boolean rowWriterEnable,
                                  Boolean addFilegroups,
                                  Boolean multiLogFiles) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
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
                                    Boolean rowWriterEnable,
                                    Boolean addFilegroups,
                                    Boolean multiLogFiles) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    testBase("testAddColChangeOrderAllFiles.json", "extra_col", "extra_col = 'yes'", 2);
    //according to the docs, this should fail. But it doesn't
    //assertThrows(Exception.class, () -> testBase("testAddColChangeOrderSomeFiles.json", "extra_col", "extra_col = 'yes'", 1));
  }

  /**
   * Add and drop cols in the same write
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testAddAndDropCols(String tableType,
                                 Boolean shouldCluster,
                                 Boolean shouldCompact,
                                 Boolean rowWriterEnable,
                                 Boolean addFilegroups,
                                 Boolean multiLogFiles) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    Map<String,Integer> conditions = new HashMap<>();
    conditions.put("distance_in_meters is NULL", 2);
    conditions.put("tip_history[0].currency is NULL", 2);
    conditions.put("tip_history[0].zextra_col_nest = 'yes'", 2);
    conditions.put("zextra_col = 'yes'", 2);
    testBase("testAddAndDropCols.json", "tip_history",  conditions);
  }

  protected String typePromoUpdates;

  protected void assertDataType(String colName, DataType expectedType) {
    assertEquals(expectedType, sparkSession.read().format("hudi").load(tableBasePath).select(colName).schema().fields()[0].dataType());
  }

  protected void testTypePromotionBase(String colName, DataType startType, DataType updateType) throws Exception {
    testTypePromotionBase(colName, startType, updateType, updateType);
  }

  protected void testTypeDemotionBase(String colName, DataType startType, DataType updateType) throws Exception {
    testTypePromotionBase(colName, startType, updateType,  startType);
  }

  protected void testTypePromotionBase(String colName, DataType startType, DataType updateType, DataType endType) throws Exception {
    boolean isCow = tableType.equals("COPY_ON_WRITE");
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;
    tableName = "test_parquet_table" + testNum;
    tableBasePath = basePath + tableName;
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
      //this write contains updates for the 6 records from the first write, so
      //although we have 2 files for each filegroup, we only see the log files
      //represented in the read. So that is why numFiles is 3, not 6
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
    doDeltaWriteTypePromo(typePromoUpdates, colName, updateType);
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
    assertDataType(colName, endType);
  }

  /**
   * Test type promotion for fields
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testTypePromotion(String tableType,
                                Boolean shouldCluster,
                                Boolean shouldCompact,
                                Boolean rowWriterEnable,
                                Boolean addFilegroups,
                                Boolean multiLogFiles) throws Exception {
    testTypePromotion(tableType, shouldCluster, shouldCompact, rowWriterEnable, addFilegroups, multiLogFiles, false);
  }


  /**
   * Test type promotion for fields
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testTypePromotionDropCols(String tableType,
                                        Boolean shouldCluster,
                                        Boolean shouldCompact,
                                        Boolean rowWriterEnable,
                                        Boolean addFilegroups,
                                        Boolean multiLogFiles) throws Exception {
    testTypePromotion(tableType, shouldCluster, shouldCompact, rowWriterEnable, addFilegroups, multiLogFiles, true);
  }

  public void testTypePromotion(String tableType,
                                Boolean shouldCluster,
                                Boolean shouldCompact,
                                Boolean rowWriterEnable,
                                Boolean addFilegroups,
                                Boolean multiLogFiles,
                                Boolean dropCols) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    if (dropCols) {
      this.typePromoUpdates = "endTypePromotionDropCols.json";
    } else {
      this.typePromoUpdates = "endTypePromotion.json";
    }


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
    testTypePromotionBase("begin_lat", DataTypes.DoubleType, DataTypes.StringType);
    //should stay with the original
    testTypeDemotionBase("rider", DataTypes.StringType, DataTypes.BinaryType);
    testTypeDemotionBase("rider", DataTypes.BinaryType, DataTypes.StringType);

    //nested data type promotions
    testTypePromotionBase("fare", createFareStruct(DataTypes.FloatType), createFareStruct(DataTypes.DoubleType, dropCols), createFareStruct(DataTypes.DoubleType));
    testTypePromotionBase("fare", createFareStruct(DataTypes.FloatType), createFareStruct(DataTypes.StringType, dropCols), createFareStruct(DataTypes.StringType));

    //complex data type promotion
    testTypePromotionBase("tip_history", DataTypes.createArrayType(DataTypes.IntegerType), DataTypes.createArrayType(DataTypes.LongType));
    testTypePromotionBase("tip_history", DataTypes.createArrayType(DataTypes.IntegerType), DataTypes.createArrayType(DataTypes.DoubleType));
    testTypePromotionBase("tip_history", DataTypes.createArrayType(DataTypes.IntegerType), DataTypes.createArrayType(DataTypes.StringType));

    //test type demotions
    //root data type demotion
    testTypeDemotionBase("distance_in_meters", DataTypes.LongType, DataTypes.IntegerType);
    testTypeDemotionBase("distance_in_meters", DataTypes.StringType, DataTypes.LongType);
    //nested data type demotion
    testTypePromotionBase("fare", createFareStruct(DataTypes.DoubleType), createFareStruct(DataTypes.FloatType, dropCols), createFareStruct(DataTypes.DoubleType));
    testTypePromotionBase("fare", createFareStruct(DataTypes.StringType), createFareStruct(DataTypes.DoubleType, dropCols), createFareStruct(DataTypes.StringType));
    //complex data type demotion
    testTypeDemotionBase("tip_history", DataTypes.createArrayType(DataTypes.LongType), DataTypes.createArrayType(DataTypes.IntegerType));
    testTypeDemotionBase("tip_history", DataTypes.createArrayType(DataTypes.StringType), DataTypes.createArrayType(DataTypes.LongType));
  }
}
