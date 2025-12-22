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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.MissingSchemaFieldException;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.streamer.HoodieStreamer;

import org.apache.avro.Schema;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieDeltaStreamerSchemaEvolutionQuick extends TestHoodieDeltaStreamerSchemaEvolutionBase {

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
    TestSchemaProvider.resetTargetSchema();
  }

  protected static Stream<Arguments> testArgs() {
    boolean fullTest = false;
    Stream.Builder<Arguments> b = Stream.builder();
    if (fullTest) {
      //only testing row-writer enabled for now
      for (Boolean rowWriterEnable : new Boolean[] {false, true}) {
        for (Boolean nullForDeletedCols : new Boolean[] {false, true}) {
          for (Boolean useKafkaSource : new Boolean[] {false, true}) {
            for (Boolean addFilegroups : new Boolean[] {false, true}) {
              for (Boolean multiLogFiles : new Boolean[] {false, true}) {
                for (Boolean shouldCluster : new Boolean[] {false, true}) {
                  for (String tableType : new String[] {"COPY_ON_WRITE", "MERGE_ON_READ"}) {
                    if (!multiLogFiles || tableType.equals("MERGE_ON_READ")) {
                      b.add(Arguments.of(tableType, shouldCluster, false, rowWriterEnable, addFilegroups, multiLogFiles, useKafkaSource, nullForDeletedCols));
                    }
                  }
                }
                b.add(Arguments.of("MERGE_ON_READ", false, true, rowWriterEnable, addFilegroups, multiLogFiles, useKafkaSource, nullForDeletedCols));
              }
            }
          }
        }
      }
    } else {
      b.add(Arguments.of("COPY_ON_WRITE", true, false, true, false, false, true, false));
      b.add(Arguments.of("COPY_ON_WRITE", true, false, true, false, false, true, true));
      b.add(Arguments.of("COPY_ON_WRITE", true, false, false, false, false, true, true));
      b.add(Arguments.of("MERGE_ON_READ", true, false, false, true, true, true, true));
      b.add(Arguments.of("MERGE_ON_READ", false, true, true, true, true, true, true));
      b.add(Arguments.of("MERGE_ON_READ", false, true, true, true, true, true, true));
      b.add(Arguments.of("MERGE_ON_READ", false, false, true, true, true, false, true));
    }
    return b.build();
  }

  protected static Stream<Arguments> testReorderedColumn() {
    Stream.Builder<Arguments> b = Stream.builder();
    for (Boolean rowWriterEnable : new Boolean[] {false, true}) {
      for (Boolean nullForDeletedCols : new Boolean[] {false, true}) {
        for (Boolean useKafkaSource : new Boolean[] {false, true}) {
          for (String tableType : new String[] {"COPY_ON_WRITE", "MERGE_ON_READ"}) {
            b.add(Arguments.of(tableType, rowWriterEnable, useKafkaSource, nullForDeletedCols));
          }
        }
      }
    }
    return b.build();
  }

  protected static Stream<Arguments> testParamsWithSchemaTransformer() {
    boolean fullTest = false;
    Stream.Builder<Arguments> b = Stream.builder();
    if (fullTest) {
      for (Boolean useTransformer : new Boolean[] {false, true}) {
        for (Boolean setSchema : new Boolean[] {false, true}) {
          for (Boolean rowWriterEnable : new Boolean[] {false, true}) {
            for (Boolean nullForDeletedCols : new Boolean[] {false, true}) {
              for (Boolean useKafkaSource : new Boolean[] {false, true}) {
                for (String tableType : new String[] {"COPY_ON_WRITE", "MERGE_ON_READ"}) {
                  b.add(Arguments.of(tableType, rowWriterEnable, useKafkaSource, nullForDeletedCols, useTransformer, setSchema));
                }
              }
            }
          }
        }
      }
    } else {
      b.add(Arguments.of("COPY_ON_WRITE", true, true, true, true, true));
      b.add(Arguments.of("COPY_ON_WRITE", true, false, false, false, true));
      b.add(Arguments.of("COPY_ON_WRITE", false, false, false, false, true));
      b.add(Arguments.of("MERGE_ON_READ", true, true, true, false, false));
      b.add(Arguments.of("MERGE_ON_READ", true, true, false, false, false));
      b.add(Arguments.of("MERGE_ON_READ", true, false, true, true, false));
      b.add(Arguments.of("MERGE_ON_READ", false, false, true, true, false));
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
                          Boolean multiLogFiles,
                          Boolean useKafkaSource,
                          Boolean allowNullForDeletedCols) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    this.useKafkaSource = useKafkaSource;
    if (useKafkaSource) {
      this.useSchemaProvider = true;
    }
    this.useTransformer = true;
    boolean isCow = tableType.equals("COPY_ON_WRITE");
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + ++testNum;
    tableName = "test_parquet_table" + testNum;
    tableBasePath = basePath + tableName;
    this.deltaStreamer = new HoodieDeltaStreamer(getDeltaStreamerConfig(allowNullForDeletedCols), jsc);

    //first write
    String datapath = String.class.getResource("/data/schema-evolution/startTestEverything.json").getPath();
    Dataset<Row> df = sparkSession.read().json(datapath);
    addData(df, true);
    deltaStreamer.sync();
    int numRecords = 6;
    int numFiles = 3;
    assertRecordCount(numRecords);
    assertFileNumber(numFiles, isCow);

    //add extra log files
    if (multiLogFiles) {
      datapath = String.class.getResource("/data/schema-evolution/extraLogFilesTestEverything.json").getPath();
      df = sparkSession.read().json(datapath);
      addData(df, false);
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
      addData(df, false);
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

    try {
      addData(df, false);
      deltaStreamer.sync();
      assertTrue(allowNullForDeletedCols);
    } catch (MissingSchemaFieldException e) {
      assertFalse(allowNullForDeletedCols);
      return;
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


  /**
   * Main testing logic for non-type promotion tests
   */
  @ParameterizedTest
  @MethodSource("testReorderedColumn")
  public void testReorderingColumn(String tableType,
                       Boolean rowWriterEnable,
                       Boolean useKafkaSource,
                       Boolean allowNullForDeletedCols) throws Exception {
    this.tableType = tableType;
    this.rowWriterEnable = rowWriterEnable;
    this.useKafkaSource = useKafkaSource;
    this.shouldCluster = false;
    this.shouldCompact = false;
    this.addFilegroups = false;
    this.multiLogFiles = false;
    this.useTransformer = true;
    if (useKafkaSource) {
      this.useSchemaProvider = true;
    }

    boolean isCow = tableType.equals("COPY_ON_WRITE");
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + ++testNum;
    tableName =  "test_parquet_table" + testNum;
    tableBasePath = basePath + tableName;

    //first write
    String datapath = String.class.getResource("/data/schema-evolution/startTestEverything.json").getPath();
    Dataset<Row> df = sparkSession.read().json(datapath);
    resetTopicAndDeltaStreamer(allowNullForDeletedCols);
    addData(df, true);
    deltaStreamer.sync();
    int numRecords = 6;
    int numFiles = 3;
    assertRecordCount(numRecords);
    assertFileNumber(numFiles, isCow);

    //add extra log files
    if (tableType.equals("MERGE_ON_READ")) {
      datapath = String.class.getResource("/data/schema-evolution/extraLogFilesTestEverything.json").getPath();
      df = sparkSession.read().json(datapath);
      addData(df, false);
      deltaStreamer.sync();
      //this write contains updates for the 6 records from the first write, so
      //although we have 2 files for each filegroup, we only see the log files
      //represented in the read. So that is why numFiles is 3, not 6
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, false);
    }

    assertRecordCount(numRecords);
    resetTopicAndDeltaStreamer(allowNullForDeletedCols);

    HoodieStreamer.Config dsConfig = deltaStreamer.getConfig();
    HoodieTableMetaClient metaClient = getMetaClient(dsConfig);
    HoodieInstant lastInstant = metaClient.getActiveTimeline().lastInstant().get();

    //test reordering column
    datapath =
        String.class.getResource("/data/schema-evolution/startTestEverything.json").getPath();
    df = sparkSession.read().json(datapath);
    df = df.drop("rider").withColumn("rider", functions.lit("rider-003"));

    addData(df, false);
    deltaStreamer.sync();

    metaClient.reloadActiveTimeline();
    Option<Schema> latestTableSchemaOpt = UtilHelpers.getLatestTableSchema(jsc, storage,
        dsConfig.targetBasePath, metaClient);
    assertTrue(latestTableSchemaOpt.get().getField("rider").schema().getTypes()
        .stream().anyMatch(t -> t.getType().equals(Schema.Type.STRING)));
    assertTrue(metaClient.reloadActiveTimeline().lastInstant().get().compareTo(lastInstant) > 0);
  }

  @ParameterizedTest
  @MethodSource("testParamsWithSchemaTransformer")
  public void testDroppedColumn(String tableType,
                                           Boolean rowWriterEnable,
                                           Boolean useKafkaSource,
                                           Boolean allowNullForDeletedCols,
                                           Boolean useTransformer,
                                           Boolean targetSchemaSameAsTableSchema) throws Exception {
    this.tableType = tableType;
    this.rowWriterEnable = rowWriterEnable;
    this.useKafkaSource = useKafkaSource;
    this.shouldCluster = false;
    this.shouldCompact = false;
    this.addFilegroups = false;
    this.multiLogFiles = false;
    this.useTransformer = useTransformer;
    if (useKafkaSource || targetSchemaSameAsTableSchema) {
      this.useSchemaProvider = true;
    }

    boolean isCow = tableType.equals("COPY_ON_WRITE");
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + ++testNum;
    tableName = "test_parquet_table" + testNum;
    tableBasePath = basePath + tableName;

    //first write
    String datapath = String.class.getResource("/data/schema-evolution/startTestEverything.json").getPath();
    Dataset<Row> df = sparkSession.read().json(datapath);
    resetTopicAndDeltaStreamer(allowNullForDeletedCols);
    addData(df, true);
    deltaStreamer.sync();
    int numRecords = 6;
    int numFiles = 3;
    assertRecordCount(numRecords);
    assertFileNumber(numFiles, isCow);

    //add extra log files
    if (tableType.equals("MERGE_ON_READ")) {
      datapath = String.class.getResource("/data/schema-evolution/extraLogFilesTestEverything.json").getPath();
      df = sparkSession.read().json(datapath);
      addData(df, false);
      deltaStreamer.sync();
      //this write contains updates for the 6 records from the first write, so
      //although we have 2 files for each filegroup, we only see the log files
      //represented in the read. So that is why numFiles is 3, not 6
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, false);
    }

    if (targetSchemaSameAsTableSchema) {
      TestSchemaProvider.setTargetSchema(TestSchemaProvider.sourceSchema);
    }
    resetTopicAndDeltaStreamer(allowNullForDeletedCols);

    HoodieStreamer.Config dsConfig = deltaStreamer.getConfig();
    HoodieTableMetaClient metaClient = getMetaClient(dsConfig);
    HoodieInstant lastInstant = metaClient.getActiveTimeline().lastInstant().get();

    // drop column
    datapath = String.class.getResource("/data/schema-evolution/startTestEverything.json").getPath();
    df = sparkSession.read().json(datapath);
    Dataset<Row> droppedColumnDf = df.drop("rider");
    try {
      addData(droppedColumnDf, true);
      deltaStreamer.sync();
      assertTrue(allowNullForDeletedCols || targetSchemaSameAsTableSchema);

      metaClient.reloadActiveTimeline();
      Option<Schema> latestTableSchemaOpt = UtilHelpers.getLatestTableSchema(jsc, storage,
          dsConfig.targetBasePath, metaClient);
      assertTrue(latestTableSchemaOpt.get().getField("rider").schema().getTypes()
          .stream().anyMatch(t -> t.getType().equals(Schema.Type.STRING)));
      assertTrue(metaClient.reloadActiveTimeline().lastInstant().get().compareTo(lastInstant) > 0);
    } catch (MissingSchemaFieldException e) {
      assertFalse(allowNullForDeletedCols || targetSchemaSameAsTableSchema);
    }
  }

  @ParameterizedTest
  @MethodSource("testParamsWithSchemaTransformer")
  public void testTypePromotion(String tableType,
                                Boolean rowWriterEnable,
                                Boolean useKafkaSource,
                                Boolean allowNullForDeletedCols,
                                Boolean useTransformer,
                                Boolean targetSchemaSameAsTableSchema) throws Exception {
    this.tableType = tableType;
    this.rowWriterEnable = rowWriterEnable;
    this.useKafkaSource = useKafkaSource;
    this.shouldCluster = false;
    this.shouldCompact = false;
    this.addFilegroups = false;
    this.multiLogFiles = false;
    this.useTransformer = useTransformer;
    if (useKafkaSource || targetSchemaSameAsTableSchema) {
      this.useSchemaProvider = true;
    }

    boolean isCow = tableType.equals("COPY_ON_WRITE");
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + ++testNum;
    tableName = "test_parquet_table" + testNum;
    tableBasePath = basePath + tableName;

    //first write
    String datapath = String.class.getResource("/data/schema-evolution/startTestEverything.json").getPath();
    Dataset<Row> df = sparkSession.read().json(datapath);
    resetTopicAndDeltaStreamer(allowNullForDeletedCols);
    addData(df, true);
    deltaStreamer.sync();
    int numRecords = 6;
    int numFiles = 3;
    assertRecordCount(numRecords);
    assertFileNumber(numFiles, isCow);

    //add extra log files
    if (tableType.equals("MERGE_ON_READ")) {
      datapath = String.class.getResource("/data/schema-evolution/extraLogFilesTestEverything.json").getPath();
      df = sparkSession.read().json(datapath);
      addData(df, false);
      deltaStreamer.sync();
      //this write contains updates for the 6 records from the first write, so
      //although we have 2 files for each filegroup, we only see the log files
      //represented in the read. So that is why numFiles is 3, not 6
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, false);
    }

    if (targetSchemaSameAsTableSchema) {
      TestSchemaProvider.setTargetSchema(TestSchemaProvider.sourceSchema);
    }
    resetTopicAndDeltaStreamer(allowNullForDeletedCols);

    HoodieStreamer.Config dsConfig = deltaStreamer.getConfig();
    HoodieTableMetaClient metaClient = getMetaClient(dsConfig);
    HoodieInstant lastInstant = metaClient.getActiveTimeline().lastInstant().get();

    // type promotion for dataset (int -> long)
    datapath = String.class.getResource("/data/schema-evolution/startTestEverything.json").getPath();
    df = sparkSession.read().json(datapath);
    Column col = df.col("distance_in_meters");
    Dataset<Row> typePromotionDf = df.withColumn("distance_in_meters", col.cast(DataTypes.DoubleType));
    try {
      addData(typePromotionDf, true);
      deltaStreamer.sync();
      assertFalse(targetSchemaSameAsTableSchema);

      metaClient.reloadActiveTimeline();
      Option<Schema> latestTableSchemaOpt = UtilHelpers.getLatestTableSchema(jsc, storage,
          dsConfig.targetBasePath, metaClient);
      assertTrue(latestTableSchemaOpt.get().getField("distance_in_meters").schema().getTypes()
              .stream().anyMatch(t -> t.getType().equals(Schema.Type.DOUBLE)),
          latestTableSchemaOpt.get().getField("distance_in_meters").schema().toString());
      assertTrue(metaClient.reloadActiveTimeline().lastInstant().get().compareTo(lastInstant) > 0);
    } catch (Exception e) {
      assertTrue(targetSchemaSameAsTableSchema);
      if (!useKafkaSource) {
        assertTrue(containsErrorMessage(e, "Incoming batch schema is not compatible with the table's one",
                "org.apache.spark.sql.catalyst.expressions.MutableDouble cannot be cast to org.apache.spark.sql.catalyst.expressions.MutableLong",
                "cannot support rewrite value for schema type: \"long\" since the old schema type is: \"double\""),
            e.getMessage());
      } else {
        assertTrue(containsErrorMessage(e, "Incoming batch schema is not compatible with the table's one",
                "cannot support rewrite value for schema type: \"long\" since the old schema type is: \"double\""),
            e.getMessage());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("testParamsWithSchemaTransformer")
  public void testTypeDemotion(String tableType,
                                Boolean rowWriterEnable,
                                Boolean useKafkaSource,
                                Boolean allowNullForDeletedCols,
                                Boolean useTransformer,
                                Boolean targetSchemaSameAsTableSchema) throws Exception {
    this.tableType = tableType;
    this.rowWriterEnable = rowWriterEnable;
    this.useKafkaSource = useKafkaSource;
    this.shouldCluster = false;
    this.shouldCompact = false;
    this.addFilegroups = false;
    this.multiLogFiles = false;
    this.useTransformer = useTransformer;
    if (useKafkaSource || targetSchemaSameAsTableSchema) {
      this.useSchemaProvider = true;
    }

    boolean isCow = tableType.equals("COPY_ON_WRITE");
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + ++testNum;
    tableName = "test_parquet_table" + testNum;
    tableBasePath = basePath + tableName;

    //first write
    String datapath = String.class.getResource("/data/schema-evolution/startTestEverything.json").getPath();
    Dataset<Row> df = sparkSession.read().json(datapath);
    resetTopicAndDeltaStreamer(allowNullForDeletedCols);
    addData(df, true);
    deltaStreamer.sync();
    int numRecords = 6;
    int numFiles = 3;
    assertRecordCount(numRecords);
    assertFileNumber(numFiles, isCow);

    //add extra log files
    if (tableType.equals("MERGE_ON_READ")) {
      datapath = String.class.getResource("/data/schema-evolution/extraLogFilesTestEverything.json").getPath();
      df = sparkSession.read().json(datapath);
      addData(df, false);
      deltaStreamer.sync();
      //this write contains updates for the 6 records from the first write, so
      //although we have 2 files for each filegroup, we only see the log files
      //represented in the read. So that is why numFiles is 3, not 6
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, false);
    }

    if (targetSchemaSameAsTableSchema) {
      TestSchemaProvider.setTargetSchema(TestSchemaProvider.sourceSchema);
    }
    resetTopicAndDeltaStreamer(allowNullForDeletedCols);

    HoodieStreamer.Config dsConfig = deltaStreamer.getConfig();
    HoodieTableMetaClient metaClient = getMetaClient(dsConfig);
    HoodieInstant lastInstant = metaClient.getActiveTimeline().lastInstant().get();

    // type demotion
    datapath =
        String.class.getResource("/data/schema-evolution/startTestEverything.json").getPath();
    df = sparkSession.read().json(datapath);
    Column col = df.col("current_ts");
    Dataset<Row> typeDemotionDf = df.withColumn("current_ts", col.cast(DataTypes.IntegerType));
    addData(typeDemotionDf, true);
    deltaStreamer.sync();

    metaClient.reloadActiveTimeline();
    Option<Schema> latestTableSchemaOpt = UtilHelpers.getLatestTableSchema(jsc, storage,
        dsConfig.targetBasePath, metaClient);
    assertTrue(latestTableSchemaOpt.get().getField("current_ts").schema().getTypes()
        .stream().anyMatch(t -> t.getType().equals(Schema.Type.LONG)));
    assertTrue(metaClient.reloadActiveTimeline().lastInstant().get().compareTo(lastInstant) > 0);
  }

  private static HoodieTableMetaClient getMetaClient(HoodieStreamer.Config dsConfig) {
    return HoodieTableMetaClient.builder()
        .setConf(storage.getConf().newInstance())
        .setBasePath(dsConfig.targetBasePath)
        .setPayloadClassName(dsConfig.payloadClassName)
        .build();
  }

  private void resetTopicAndDeltaStreamer(Boolean allowNullForDeletedCols) throws IOException {
    topicName = "topic" + ++testNum;
    if (this.deltaStreamer != null) {
      this.deltaStreamer.shutdownGracefully();
    }
    String[] transformerClassNames = useTransformer ? new String[] {TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()}
        : new String[0];
    TypedProperties extraProps = new TypedProperties();
    extraProps.setProperty("hoodie.streamer.checkpoint.force.skip", "true");
    HoodieDeltaStreamer.Config deltaStreamerConfig = getDeltaStreamerConfig(transformerClassNames, allowNullForDeletedCols, extraProps);
    deltaStreamerConfig.checkpoint = "0";
    this.deltaStreamer = new HoodieDeltaStreamer(deltaStreamerConfig, jsc);
  }

  private boolean containsErrorMessage(Throwable e, String... messages) {
    while (e != null) {
      for (String msg : messages) {
        if (e.getMessage().contains(msg)) {
          return true;
        }
      }
      e = e.getCause();
    }

    return false;
  }

  protected void assertDataType(Dataset<Row> df, String colName, DataType expectedType) {
    assertEquals(expectedType, df.select(colName).schema().fields()[0].dataType());
  }

  protected void assertCondition(Dataset<Row> df, String condition, int count) {
    assertEquals(count, df.filter(condition).count());
  }

}
