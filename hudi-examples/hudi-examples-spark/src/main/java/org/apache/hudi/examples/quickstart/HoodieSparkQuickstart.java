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

package org.apache.hudi.examples.quickstart;

import org.apache.hudi.QuickstartUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.examples.common.HoodieExampleDataGenerator;
import org.apache.hudi.examples.common.HoodieExampleSparkUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.hudi.config.HoodieWriteConfig.TBL_NAME;
import static org.apache.spark.sql.SaveMode.Append;
import static org.apache.spark.sql.SaveMode.Overwrite;

public final class HoodieSparkQuickstart {

  private HoodieSparkQuickstart() {
  }

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: HoodieWriteClientExample <tablePath> <tableName>");
      System.exit(1);
    }
    String tablePath = args[0];
    String tableName = args[1];

    SparkSession spark = HoodieExampleSparkUtils.defaultSparkSession("Hudi Spark basic example");
    SparkConf sparkConf = HoodieExampleSparkUtils.defaultSparkConf("hoodie-client-example");

    try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
      runQuickstart(jsc, spark, tableName, tablePath);
    }
  }

  /**
   * Visible for testing
   */
  public static void runQuickstart(JavaSparkContext jsc, SparkSession spark, String tableName, String tablePath) {
    final HoodieExampleDataGenerator<HoodieAvroPayload> dataGen = new HoodieExampleDataGenerator<>();

    Dataset<Row> insertQueryDataIn = insertData(spark, jsc, tablePath, tableName, dataGen);
    queryData(spark, jsc, tablePath, tableName, dataGen);
    assert insertQueryDataIn.except(spark.sql("SELECT begin_lat, begin_lon, driver, end_lat, end_lon, fare, partitionpath, rider, ts, uuid FROM hudi_ro_table")).count() == 0;

    Dataset<Row> updateQueryDataIn = updateData(spark, jsc, tablePath, tableName, dataGen);
    queryData(spark, jsc, tablePath, tableName, dataGen);
    assert spark.sql("SELECT begin_lat, begin_lon, driver, end_lat, end_lon, fare, partitionpath, rider, ts, uuid FROM hudi_ro_table").except(insertQueryDataIn).except(updateQueryDataIn).count() == 0;

    incrementalQuery(spark, tablePath, tableName);
    pointInTimeQuery(spark, tablePath, tableName);

    Dataset<Row> beforeDelete = spark.sql("SELECT begin_lat, begin_lon, driver, end_lat, end_lon, fare, partitionpath, rider, ts, uuid FROM hudi_ro_table");
    Dataset<Row> deleteQueryIn = delete(spark, tablePath, tableName);
    queryData(spark, jsc, tablePath, tableName, dataGen);
    assert beforeDelete.except(deleteQueryIn).except(spark.sql("SELECT begin_lat, begin_lon, driver, end_lat, end_lon, fare, partitionpath, rider, ts, uuid FROM hudi_ro_table")).count() == 0;

    Dataset<Row> beforeOverwrite = spark.sql("SELECT begin_lat, begin_lon, driver, end_lat, end_lon, fare, partitionpath, rider, ts, uuid FROM hudi_ro_table");
    Dataset<Row> overwriteDataIn = insertOverwriteData(spark, jsc, tablePath, tableName, dataGen);
    Dataset<Row> afterOverwrite = spark.sql("SELECT begin_lat, begin_lon, driver, end_lat, end_lon, fare, partitionpath, rider, ts, uuid FROM hudi_ro_table");
    Dataset<Row> overwriteIntersect = beforeOverwrite.intersect(afterOverwrite);
    assert afterOverwrite.except(overwriteIntersect).except(overwriteDataIn).count() == 0;
    queryData(spark, jsc, tablePath, tableName, dataGen);

    Dataset<Row> beforeDeleteByPartition = spark.sql(
        "SELECT begin_lat, begin_lon, driver, end_lat, end_lon, fare, partitionpath, rider, ts, uuid FROM hudi_ro_table WHERE partitionpath NOT IN ("
            + String.join(", ", HoodieExampleDataGenerator.DEFAULT_PARTITION_PATHS) + ")");
    deleteByPartition(spark, tablePath, tableName);
    queryData(spark, jsc, tablePath, tableName, dataGen);
    assert spark.sql("SELECT begin_lat, begin_lon, driver, end_lat, end_lon, fare, partitionpath, rider, ts, uuid FROM hudi_ro_table").except(beforeDeleteByPartition).count() == 0;
  }

  /**
   * Generate some new trips, load them into a DataFrame and write the DataFrame into the Hudi dataset as below.
   */
  public static Dataset<Row> insertData(SparkSession spark, JavaSparkContext jsc, String tablePath, String tableName,
                                        HoodieExampleDataGenerator<HoodieAvroPayload> dataGen) {
    String commitTime = Long.toString(System.currentTimeMillis());
    List<String> inserts = dataGen.convertToStringList(dataGen.generateInserts(commitTime, 20));
    Dataset<Row> df = spark.read().json(jsc.parallelize(inserts, 1));

    df.write().format("org.apache.hudi")
        .options(QuickstartUtils.getQuickstartWriteConfigs())
        .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "ts")
        .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "uuid")
        .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partitionpath")
        .option(TBL_NAME.key(), tableName)
        .mode(Overwrite)
        .save(tablePath);
    return df;
  }

  /**
   * Generate new records, load them into a {@link Dataset} and insert-overwrite it into the Hudi dataset
   */
  public static Dataset<Row> insertOverwriteData(SparkSession spark, JavaSparkContext jsc, String tablePath, String tableName,
                                                 HoodieExampleDataGenerator<HoodieAvroPayload> dataGen) {
    String commitTime = Long.toString(System.currentTimeMillis());
    List<String> inserts = dataGen.convertToStringList(dataGen.generateInserts(commitTime, 20));
    Dataset<Row> df = spark.read().json(jsc.parallelize(inserts, 1));

    df.write().format("org.apache.hudi")
        .options(QuickstartUtils.getQuickstartWriteConfigs())
        .option("hoodie.datasource.write.operation", WriteOperationType.INSERT_OVERWRITE.name())
        .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "ts")
        .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "uuid")
        .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partitionpath")
        .option(TBL_NAME.key(), tableName)
        .mode(Append)
        .save(tablePath);
    return df;
  }

  /**
   * Load the data files into a DataFrame.
   */
  public static void queryData(SparkSession spark, JavaSparkContext jsc, String tablePath, String tableName,
                               HoodieExampleDataGenerator<HoodieAvroPayload> dataGen) {
    Dataset<Row> roViewDF = spark
        .read()
        .format("org.apache.hudi")
        .load(tablePath + "/*/*/*/*");

    roViewDF.createOrReplaceTempView("hudi_ro_table");

    spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_ro_table where fare > 20.0").show();
    //  +-----------------+-------------------+-------------------+---+
    //  |             fare|          begin_lon|          begin_lat| ts|
    //  +-----------------+-------------------+-------------------+---+
    //  |98.88075495133515|0.39556048623031603|0.17851135255091155|0.0|
    //  ...

    spark.sql(
            "select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_ro_table")
        .show();
    //  +-------------------+--------------------+----------------------+-------------------+--------------------+------------------+
    //  |_hoodie_commit_time|  _hoodie_record_key|_hoodie_partition_path|              rider|              driver|              fare|
    //  +-------------------+--------------------+----------------------+-------------------+--------------------+------------------+
    //  |     20191231181501|31cafb9f-0196-4b1...|            2020/01/02|rider-1577787297889|driver-1577787297889| 98.88075495133515|
    //  ...
  }

  /**
   * This is similar to inserting new data. Generate updates to existing trips using the data generator,
   * load into a DataFrame and write DataFrame into the hudi dataset.
   */
  public static Dataset<Row> updateData(SparkSession spark, JavaSparkContext jsc, String tablePath, String tableName,
                                        HoodieExampleDataGenerator<HoodieAvroPayload> dataGen) {

    String commitTime = Long.toString(System.currentTimeMillis());
    List<String> updates = dataGen.convertToStringList(dataGen.generateUpdates(commitTime, 10));
    Dataset<Row> df = spark.read().json(jsc.parallelize(updates, 1));
    df.write().format("org.apache.hudi")
        .options(QuickstartUtils.getQuickstartWriteConfigs())
        .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "ts")
        .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "uuid")
        .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partitionpath")
        .option(TBL_NAME.key(), tableName)
        .mode(Append)
        .save(tablePath);
    return df;
  }

  /**
   * Deleta data based in data information.
   */
  public static Dataset<Row> delete(SparkSession spark, String tablePath, String tableName) {

    Dataset<Row> roViewDF = spark.read().format("org.apache.hudi").load(tablePath + "/*/*/*/*");
    roViewDF.createOrReplaceTempView("hudi_ro_table");
    //Dataset<Row> df = spark.sql("select uuid, partitionpath, ts from  hudi_ro_table limit 2");
    Dataset<Row> ret = spark.sql("SELECT begin_lat, begin_lon, driver, end_lat, end_lon, fare, partitionpath, rider, ts, uuid FROM hudi_ro_table limit 2");
    Dataset<Row> df = ret.drop("begin_lat", "begin_lon", "driver", "end_lat", "end_lon", "fare", "rider");

    df.write().format("org.apache.hudi")
        .options(QuickstartUtils.getQuickstartWriteConfigs())
        .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "ts")
        .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "uuid")
        .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partitionpath")
        .option(TBL_NAME.key(), tableName)
        .option("hoodie.datasource.write.operation", WriteOperationType.DELETE.value())
        .mode(Append)
        .save(tablePath);
    return ret;
  }

  /**
   * Delete the data of a single or multiple partitions.
   */
  public static void deleteByPartition(SparkSession spark, String tablePath, String tableName) {
    Dataset<Row> df = spark.emptyDataFrame();
    df.write().format("org.apache.hudi")
        .options(QuickstartUtils.getQuickstartWriteConfigs())
        .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "ts")
        .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "uuid")
        .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partitionpath")
        .option(TBL_NAME.key(), tableName)
        .option("hoodie.datasource.write.operation", WriteOperationType.DELETE.value())
        .option("hoodie.datasource.write.partitions.to.delete",
            String.join(", ", HoodieExampleDataGenerator.DEFAULT_PARTITION_PATHS))
        .mode(Append)
        .save(tablePath);
  }

  /**
   * Hudi also provides capability to obtain a stream of records that changed since given commit timestamp.
   * This can be achieved using Hudi’s incremental view and providing a begin time from which changes need to be streamed.
   * We do not need to specify endTime, if we want all changes after the given commit (as is the common case).
   */
  public static void incrementalQuery(SparkSession spark, String tablePath, String tableName) {
    List<String> commits =
        spark.sql("select distinct(_hoodie_commit_time) as commitTime from hudi_ro_table order by commitTime")
            .toJavaRDD()
            .map((Function<Row, String>) row -> row.getString(0))
            .take(50);

    String beginTime = commits.get(commits.size() - 2); // commit time we are interested in

    // incrementally query data
    Dataset<Row> incViewDF = spark
        .read()
        .format("org.apache.hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.begin.instanttime", beginTime)
        .load(tablePath);

    incViewDF.createOrReplaceTempView("hudi_incr_table");
    spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from hudi_incr_table where fare > 20.0")
        .show();
  }

  /**
   * Lets look at how to query data as of a specific time.
   * The specific time can be represented by pointing endTime to a specific commit time
   * and beginTime to “000” (denoting earliest possible commit time).
   */
  public static void pointInTimeQuery(SparkSession spark, String tablePath, String tableName) {
    List<String> commits =
        spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_ro_table order by commitTime")
            .toJavaRDD()
            .map((Function<Row, String>) row -> row.getString(0))
            .take(50);
    String beginTime = "000"; // Represents all commits > this time.
    String endTime = commits.get(commits.size() - 2); // commit time we are interested in

    //incrementally query data
    Dataset<Row> incViewDF = spark.read().format("org.apache.hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.begin.instanttime", beginTime)
        .option("hoodie.datasource.read.end.instanttime", endTime)
        .load(tablePath);

    incViewDF.createOrReplaceTempView("hudi_incr_table");
    spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_incr_table where fare > 20.0")
        .show();
  }
}
