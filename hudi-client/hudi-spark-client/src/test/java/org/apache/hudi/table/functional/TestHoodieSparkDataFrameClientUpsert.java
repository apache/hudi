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

package org.apache.hudi.table.functional;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.client.HoodieRowWriteStatus;
import org.apache.hudi.client.SparkDataFrameWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.HoodieDataFrameGenerator;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import scala.collection.Seq;

import static org.apache.hudi.client.utils.ScalaConversions.toSeq;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.testutils.FixtureUtils.prepareFixtureTable;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.testutils.HoodieDataFrameGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.spark.sql.functions.callUDF;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieSparkDataFrameClientUpsert extends SparkClientFunctionalTestHarness {

  private static final String TABLE_NAME = "dataframe_test_table";

  public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("'year='yyyy/'month='MM/'day='dd");

  public static final Column[] JOIN_COLS = Stream.of(PARTITION_PATH_METADATA_FIELD, RECORD_KEY_METADATA_FIELD)
      .map(Column::new).toArray(Column[]::new);
  public static final Seq<String> JOIN_COL_NAMES_SEQ = toSeq(Arrays.stream(JOIN_COLS).map(Column::toString));
  public static final Column[] HOODIE_META_COLS = HOODIE_META_COLUMNS.stream().map(Column::new)
      .toArray(Column[]::new);
  public static final String[] HOODIE_META_COL_NAMES = HOODIE_META_COLUMNS.toArray(new String[0]);

  HoodieWriteConfig writeConfig;
  String tableBasePath;
  HoodieDataFrameGenerator<HoodieAvroPayload> dataGen;
  int nBaseRecords;
  String[] partitionPaths;
  int numPartitions;

  @BeforeEach
  public void setUp() throws IOException {
    spark().sparkContext().getConf().registerKryoClasses(
        new Class[]{org.apache.avro.generic.GenericData.class, org.apache.avro.Schema.class});
    registerUDFs(spark().udf());
    jsc().setLogLevel("WARN");
    dataGen = setUpTestTable(HoodieTableType.COPY_ON_WRITE);
    nBaseRecords = dataGen.getNumExistingKeys();
    partitionPaths = dataGen.getPartitionPaths();
    numPartitions = partitionPaths.length;
    writeConfig = getHoodieWriteConfig(tableBasePath, TABLE_NAME, numPartitions, numPartitions, new Properties());
  }

  private HoodieDataFrameGenerator<HoodieAvroPayload> setUpTestTable(HoodieTableType tableType) throws IOException {
    String basePath = Paths.get(URI.create(basePath().replaceAll("/$", ""))).toString();
    String fixtureName = String.format("fixtures/testHoodieSparkDataFrameWriteClient.%s.zip", tableType.name());
    tableBasePath = prepareFixtureTable(Objects.requireNonNull(getClass()
        .getClassLoader().getResource(fixtureName)), Paths.get(basePath)).toString();
    return initDataGen(tableBasePath + "/*/*/*/*.parquet");
  }

  public HoodieDataFrameGenerator<HoodieAvroPayload> initDataGen(String globParquetPath) {
    List<Row> rows = sqlContext().read().parquet(globParquetPath)
        .select("uuid", "_hoodie_partition_path", "ts")
        .collectAsList();
    Map<Integer, HoodieDataFrameGenerator.KeyPartition> keyPartitionMap = IntStream
        .range(0, rows.size()).boxed()
        .collect(Collectors.toMap(Function.identity(), i -> {
          Row r = rows.get(i);
          HoodieDataFrameGenerator.KeyPartition kp = new HoodieDataFrameGenerator.KeyPartition();
          kp.key = new HoodieKey(r.getString(0), r.getString(1));
          kp.partitionPath = r.getString(1);
          kp.ts = r.getLong(2);
          return kp;
        }));
    String[] partitions = rows.stream().map(r -> r.getString(1)).distinct().toArray(String[]::new);
    return new HoodieDataFrameGenerator<>(partitions, keyPartitionMap);
  }

  @Test
  public void upsert() {
    final Dataset<Row> upserts = generateUpserts(dataGen, jsc(), spark(), numPartitions);
    final Schema avroSchema = deriveAvroSchema(upserts, TABLE_NAME);
    spark().sparkContext().getConf().registerAvroSchemas(toSeq(avroSchema));
    writeConfig.setSchema(avroSchema.toString());
    try (SparkDataFrameWriteClient writeClient = new SparkDataFrameWriteClient(context(), writeConfig)) {
      List<HoodieRowWriteStatus> writeStatuses = writeClient.upsert(upserts, dataGen.getLastCommitTime()).collectAsList();
      assertNoWriteErrors(writeStatuses);
      assertEquals(5, writeStatuses.stream().map(ws -> ws.getStat().getNumInserts()).mapToLong(Long::new).sum());
      assertEquals(5, writeStatuses.stream().map(ws -> ws.getStat().getNumUpdateWrites()).mapToLong(Long::new).sum());
    }
  }

  private static Dataset<Row> generateUpserts(HoodieDataFrameGenerator<HoodieAvroPayload> dataGen, JavaSparkContext jsc, SparkSession spark, int numPartitions) {
    // generate incoming records
    final String commit1 = HoodieActiveTimeline.createNewInstantTime();
    final int nUpserts = 10;
    final int nUpdates = 5;
    final int nInserts = nUpserts - nUpdates;
    Dataset<Row> upserts = addYearMonthDay(dataGen.generateUpdates(commit1, nUpdates, jsc, spark, numPartitions))
        .union(addYearMonthDay(dataGen.generateInserts(commit1, nInserts, jsc, spark, numPartitions)));
    upserts.collect(); // force to load data in mem to mimic incoming data
    return upserts;
  }

  public static Dataset<Row> addYearMonthDay(Dataset<Row> df) {
    return df.withColumn("year", callUDF("getYear", df.col("ts")))
        .withColumn("month", callUDF("getMonth", df.col("ts")))
        .withColumn("day", callUDF("getDay", df.col("ts")));
  }

  public static Schema deriveAvroSchema(Dataset<Row> df, String tableName) {
    return AvroConversionUtils.convertStructTypeToAvroSchema(df.schema(), String.format("%s_record", tableName),
        String.format("hoodie.%s", tableName));
  }

  public static void registerUDFs(UDFRegistration udfReg) {
    udfReg.register("getKey", (UDF2<String, String, String>) (prefix, id) -> prefix + ":" + id,
        DataTypes.StringType);
    udfReg.register("getPartition",
        (UDF1<Long, String>) ts -> FORMATTER.format(LocalDateTime.ofEpochSecond(ts, 0, ZoneOffset.UTC)),
        DataTypes.StringType);
    udfReg.register("getYear", (UDF1<Long, String>) ts -> String.format("%04d",
        LocalDateTime.ofEpochSecond(ts, 0, ZoneOffset.UTC).getYear()), DataTypes.StringType);
    udfReg.register("getMonth", (UDF1<Long, String>) ts -> String.format("%02d",
        LocalDateTime.ofEpochSecond(ts, 0, ZoneOffset.UTC).getMonthValue()), DataTypes.StringType);
    udfReg.register("getDay", (UDF1<Long, String>) ts -> String.format("%02d",
        LocalDateTime.ofEpochSecond(ts, 0, ZoneOffset.UTC).getDayOfMonth()), DataTypes.StringType);
  }

  private static HoodieWriteConfig getHoodieWriteConfig(String basePath, String tableName, int parallelism,
      int numPartitions, Properties overwritingProps) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).forTable(tableName)
        .withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(parallelism, parallelism)
        .withBulkInsertParallelism(numPartitions).withDeleteParallelism(parallelism)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withPayloadClass(OverwriteWithLatestAvroPayload.class.getName()).withInlineCompaction(false)
            .archiveCommitsWith(20, 30).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder().withInlineClustering(false)
            .withAsyncClustering(false).build())
        .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadOrderingField("ts").build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .withProps(overwritingProps).build();
  }

}
