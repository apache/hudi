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

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieDeltaStreamerSchemaEvolutionQuick extends TestHoodieDeltaStreamerSchemaEvolutionBase {

  protected static Stream<Arguments> testArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    //only testing row-writer enabled for now
    for (Boolean rowWriterEnable : new Boolean[]{true}) {
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

  /**
   * Main testing logic for non-type promotion tests
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testBase(String tableType,
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
    boolean isCow = tableType.equals("COPY_ON_WRITE");
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;
    tableBasePath = basePath + "test_parquet_table" + testNum;
    this.deltaStreamer = new HoodieDeltaStreamer(getDeltaStreamerConfig(), jsc);

    //first write
    String datapath = String.class.getResource("/data/schema-evolution/startTestEverything.json").getPath();
    Dataset<Row> df = sparkSession.read().json(datapath);
    df.write().format("parquet").mode(SaveMode.Overwrite).save(PARQUET_SOURCE_ROOT);
    deltaStreamer.sync();
    int numRecords = 6;
    int numFiles = 3;
    assertRecordCount(numRecords);
    assertFileNumber(numFiles, isCow);


    //add extra log files
    if (multiLogFiles) {
      datapath = String.class.getResource("/data/schema-evolution/extraLogFilesTestEverything.json").getPath();
      df = sparkSession.read().json(datapath);
      df.write().format("parquet").mode(SaveMode.Append).save(PARQUET_SOURCE_ROOT);
      deltaStreamer.sync();
      //this write contains updates for the 6 records from the first write, so
      //although we have 2 files for each filegroup, we only see the log files
      //represented in the read. So that is why numFiles is 3, not 6
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, false);
    }

    //make other filegroups
    if (addFilegroups) {
      datapath = String.class.getResource("/data/schema-evolution/newFileGroupsTestEverything.json").getPath();
      df = sparkSession.read().json(datapath);
      df.write().format("parquet").mode(SaveMode.Append).save(PARQUET_SOURCE_ROOT);
      deltaStreamer.sync();
      numRecords += 3;
      numFiles += 3;
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, isCow);
    }

    //write updates
    datapath = String.class.getResource("/data/schema-evolution/endTestEverything.json").getPath();
    df = sparkSession.read().json(datapath);
    //do casting
    Column col = df.col("tip_history");
    df = df.withColumn("tip_history", col.cast(DataTypes.createArrayType(DataTypes.LongType)));
    col = df.col("fare");
    df = df.withColumn("fare", col.cast(DataTypes.createStructType(new StructField[]{
        new StructField("amount", DataTypes.StringType, true, Metadata.empty()),
        new StructField("currency", DataTypes.StringType, true, Metadata.empty()),
        new StructField("zextra_col_nested", DataTypes.StringType, true, Metadata.empty())
    })));
    col = df.col("begin_lat");
    df = df.withColumn("begin_lat", col.cast(DataTypes.DoubleType));
    col = df.col("end_lat");
    df = df.withColumn("end_lat", col.cast(DataTypes.StringType));
    col = df.col("distance_in_meters");
    df = df.withColumn("distance_in_meters", col.cast(DataTypes.FloatType));
    col = df.col("seconds_since_epoch");
    df = df.withColumn("seconds_since_epoch", col.cast(DataTypes.StringType));
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

    df = sparkSession.read().format("hudi").load(tableBasePath);
    df.show(100,false);
    df.cache();
    assertDataType(df, "tip_history", DataTypes.createArrayType(DataTypes.LongType));
    assertDataType(df, "fare", DataTypes.createStructType(new StructField[]{
        new StructField("amount", DataTypes.StringType, true, Metadata.empty()),
        new StructField("currency", DataTypes.StringType, true, Metadata.empty()),
        new StructField("extra_col_struct", DataTypes.LongType, true, Metadata.empty()),
        new StructField("zextra_col_nested", DataTypes.StringType, true, Metadata.empty())
    }));
    assertDataType(df, "begin_lat", DataTypes.DoubleType);
    assertDataType(df, "end_lat", DataTypes.StringType);
    assertDataType(df, "distance_in_meters", DataTypes.FloatType);
    assertDataType(df, "seconds_since_epoch", DataTypes.StringType);
    assertCondition(df, "zextra_col = 'yes'", 2);
    assertCondition(df, "_extra_col = 'yes'", 2);
    assertCondition(df, "fare.zextra_col_nested = 'yes'", 2);
    assertCondition(df, "size(zcomplex_array) > 0", 2);
    assertCondition(df, "extra_col_regular is NULL", 2);
    assertCondition(df, "fare.extra_col_struct is NULL", 2);
  }

  protected void assertDataType(Dataset<Row> df, String colName, DataType expectedType) {
    assertEquals(expectedType, df.select(colName).schema().fields()[0].dataType());
  }

  protected void assertCondition(Dataset<Row> df, String condition, int count) {
    assertEquals(count, df.filter(condition).count());
  }

}
