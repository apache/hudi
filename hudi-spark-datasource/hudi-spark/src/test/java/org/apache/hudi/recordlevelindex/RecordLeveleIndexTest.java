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

package org.apache.hudi.recordlevelindex;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieDataSourceHelpers;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.SimpleKeyGenerator;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Sample program that writes & reads hoodie tables via the Spark datasource.
 */
public class RecordLeveleIndexTest {
  private String tablePath = "file:///tmp/hoodie/hoodie_test";
  private String tableName = "hoodie_test";
  private Schema schema = new Schema.Parser().parse("{\n"
      + "    \"type\":\"record\",\n"
      + "    \"name\":\"test\",\n"
      + "    \"fields\":[\n"
      + "        {\n"
      + "            \"name\":\"id\",\n"
      + "            \"type\":\"string\"\n"
      + "        },\n"
      + "        {\n"
      + "            \"name\":\"name\",\n"
      + "            \"type\":\"string\"\n"
      + "        },\n"
      + "        {\n"
      + "            \"name\":\"ts\",\n"
      + "            \"type\":\"string\"\n"
      + "        },\n"
      + "        {\n"
      + "            \"name\":\"dt\",\n"
      + "            \"type\":\"string\"\n"
      + "        }\n"
      + "    ]\n"
      + "}");
  private static final Logger LOG = LogManager.getLogger(RecordLeveleIndexTest.class);

  @Test
  public void writeTest() throws Exception {

    SparkSession spark = SparkSession.builder().appName("Hoodie Spark APP").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[1]").getOrCreate();
    JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
    spark.sparkContext().setLogLevel("WARN");
    FileSystem fs = FileSystem.get(jssc.hadoopConfiguration());
    fs.delete(new Path(tablePath), true);

    List<String> dataList = getHoodieRecords(0, 9, "p1");
    writeHudi(spark, jssc, fs, dataList);
    checkData(spark);

    // Partition change
    dataList = getHoodieRecords(0, 9, "p2");
    writeHudi(spark, jssc, fs, dataList);
    checkData(spark);
  }

  @Test
  public void compactionTest() throws Exception {
    SparkSession spark = SparkSession.builder().appName("Hoodie Spark APP").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[1]").getOrCreate();
    JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
    spark.sparkContext().setLogLevel("WARN");
    FileSystem fs = FileSystem.get(jssc.hadoopConfiguration());
    fs.delete(new Path(tablePath), true);
    List<String> dataList = getHoodieRecords(0, 20, "p1");
    writeHudi(spark, jssc, fs, dataList);
    for (int i = 0; i <= 20; i++) {
      dataList = getHoodieRecords(i, i, "p2");
      writeHudi(spark, jssc, fs, dataList);
      checkData(spark);
    }
  }

  @Test
  public void initIndexTest() throws Exception {
    SparkSession spark = SparkSession.builder().appName("Hoodie Spark APP").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[1]").getOrCreate();
    JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
    spark.sparkContext().setLogLevel("WARN");
    FileSystem fs = FileSystem.get(jssc.hadoopConfiguration());
    fs.delete(new Path(tablePath), true);
    List<String> dataList = getHoodieRecords(0, 9, "p1");
    writeHudi(spark, jssc, fs, dataList);

    // Delete metadata
    fs.delete(new Path(tablePath + "/.hoodie/metadata"), true);

    dataList = getHoodieRecords(0, 9, "p2");
    writeHudi(spark, jssc, fs, dataList);
    checkData(spark);
  }

  private static void checkData(SparkSession spark) {
    spark.sql("select * from dataset ").show(100, false);
    spark.sql("select * from mv ").show(100, false);
    Row[] dvCountRow = (Row[]) spark.sql("select count(1) from dv d ").collect();
    GenericRowWithSchema dvCount = (GenericRowWithSchema) dvCountRow[0];

    Row[] checkCountRow =
        (Row[]) spark.sql("select count(1) from dv d inner join mv as m " + "on d.key=m.key where d.partition = m.partition  and  locate(m.fileId, d.fileId) > 0 and m.isDeleted = false").collect();
    GenericRowWithSchema checkCount = (GenericRowWithSchema) checkCountRow[0];
    assert checkCount.getLong(0) == dvCount.getLong(0);
  }

  @Test
  public void testCommitDatasetFailed() throws Exception {
    SparkSession spark = SparkSession.builder().appName("Hoodie Spark APP").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[1]").getOrCreate();
    JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
    spark.sparkContext().setLogLevel("WARN");
    FileSystem fs = FileSystem.get(jssc.hadoopConfiguration());
    fs.delete(new Path(tablePath), true);
    List<String> dataList = getHoodieRecords(0, 9, "p1");
    String time1 = writeHudi(spark, jssc, fs, dataList);

    // Delete commit
    fs.delete(new Path(tablePath + "/.hoodie/" + time1 + ".commit"), true);

    Row[] dvCountRow = (Row[]) spark.sql("select count(1) from mv ").collect();
    GenericRowWithSchema dvCount = (GenericRowWithSchema) dvCountRow[0];
    assert 0 == dvCount.getLong(0);

    dataList = getHoodieRecords(0, 9, "p2");
    writeHudi(spark, jssc, fs, dataList);
    checkData(spark);
  }

  @Test
  public void testCommitMetdataFailed() throws Exception {
    SparkSession spark = SparkSession.builder().appName("Hoodie Spark APP").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[1]").getOrCreate();
    JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
    spark.sparkContext().setLogLevel("WARN");
    FileSystem fs = FileSystem.get(jssc.hadoopConfiguration());
    fs.delete(new Path(tablePath), true);
    List<String> dataList = getHoodieRecords(0, 9, "p1");
    String time1 = writeHudi(spark, jssc, fs, dataList);

    // Delete commit
    fs.delete(new Path(tablePath + "/.hoodie/" + time1 + ".commit"), true);
    fs.delete(new Path(tablePath + "/.hoodie/metadata/.hoodie/" + time1 + ".deltacommit"), true);

    Row[] dvCountRow = (Row[]) spark.sql("select count(1) from mv ").collect();
    GenericRowWithSchema dvCount = (GenericRowWithSchema) dvCountRow[0];
    assert 0 == dvCount.getLong(0);

    dataList = getHoodieRecords(0, 9, "p2");
    writeHudi(spark, jssc, fs, dataList);
    checkData(spark);
  }

  private String writeHudi(SparkSession spark, JavaSparkContext jssc, FileSystem fs, List<String> dataList) {
    Dataset<Row> inputDF1 = spark.read().json(jssc.parallelize(dataList, 2));
    DataFrameWriter<Row> writer =
        inputDF1.write().format("org.apache.hudi").option("hoodie.insert.shuffle.parallelism", "2").option("hoodie.upsert.shuffle.parallelism", "2").option("hoodie.index.type", "RECORD_LEVEL")
            .option(HoodieMetadataConfig.ENABLE_METADATA_INDEX_RECORD_LEVEL_INDEX.key(), true).option(HoodieMetadataConfig.METADATA_INDEX_RECORD_INDEX_FILE_GROUP_COUNT.key(), 2)
            .option(HoodieMetadataConfig.RECORD_LEVEL_INDEX_PARALLELISM.key(), 2).option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "id")
            .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "dt").option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "ts").option(HoodieWriteConfig.TBL_NAME.key(), tableName)
            .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), SimpleKeyGenerator.class.getCanonicalName()).option(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE().key(), "false")
            .option(HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key(), "false")
            .option(DataSourceWriteOptions.ASYNC_CLUSTERING_ENABLE().key(), "true").mode(SaveMode.Append);

    // new dataset if needed
    writer.save(tablePath); // ultimately where the dataset will be placed
    String commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
    LOG.info("First commit at instant time :" + commitInstantTime1);


    registerTempTable(spark);
    return commitInstantTime1;
  }

  private void registerTempTable(SparkSession spark) {
    spark.sql("drop table if exists  dataset");
    Dataset<Row> dataset = spark.read().format("org.apache.hudi").load(tablePath + "/");
    dataset.registerTempTable("dataset");

    spark.sql("drop table if exists  meta");
    Dataset<Row> meta = spark.read().format("org.apache.hudi").load(tablePath + "/.hoodie/metadata");
    meta.registerTempTable("meta");

    spark.sql("drop view if exists  mv");
    spark.sql("create temporary view  mv as " + "select key, " + "recordLevelIndexMetadata['partition'] as partition, " + "recordLevelIndexMetadata['fileId'] as fileId, "
        + "recordLevelIndexMetadata['isDeleted'] as isDeleted, " + "recordLevelIndexMetadata['commitTime'] as commitTime, "
        + "recordLevelIndexMetadata['rowGroupIndex'] as rowGroupIndex from meta where type = 5");

    spark.sql("drop view if exists  dv");
    spark.sql("create temporary view  dv as select " + "_hoodie_record_key     as key," + "_hoodie_partition_path as partition, " + "_hoodie_file_name     as fileId,"
        + "_hoodie_commit_time    as commitTime from dataset");
  }

  @NotNull
  private List<String> getHoodieRecords(int start, int end, String partition) throws IOException {
    List<HoodieRecord> dataList = new ArrayList<>();
    for (int i = start; i <= end; i++) {
      String id = String.valueOf(i);
      RawTripTestPayload payload = new RawTripTestPayload("{\"id\":\" " + id + "\",\"name\":\"a1\",\"ts\":\"001\",\"dt\":\"" + partition + "\"}", id, partition, schema.toString());
      HoodieKey hoodieKey = new HoodieKey(id, partition);
      HoodieRecord<RawTripTestPayload> hoodieRecord = new HoodieAvroRecord(hoodieKey, payload);
      dataList.add(hoodieRecord);
    }
    List<String> records = (List<String>) dataList.stream().map(x -> {
      try {
        String jsonData = ((RawTripTestPayload) x.getData()).getJsonData();
        return Option.of(jsonData);
      } catch (IOException e) {
        return Option.empty();
      }
    }).filter(Option::isPresent).map(Option::get).collect(Collectors.toList());
    return records;
  }

}