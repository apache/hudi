/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional.cdc

import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.config.HoodieWriteConfig

import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.{Column, Dataset, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{Add, If, Literal}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class TestCDCStreamingSuite extends TestCDCBase {

  /**
   * Here we simulate a more complex streaming data ETL of a real scenario that uses CDC.
   *
   * there are three tables/data sources:
   *  -- a streaming data source with three fields userid, country, ts;
   *  -- a hudi table user_to_country_tbl(userid, country, ts) that enable cdc.
   *     a streaming can receive the streaming data source and merge into this table;
   *  -- another hudi table country_to_population_tbl(country, cnt) that not enable cdc;
   *     and another streaming read the cdc data from user_to_country_tbl and do some aggregation
   *     and write to country_to_population_tbl.
   */
  @ParameterizedTest
  @CsvSource(Array("false", "true"))
  def cdcStreaming(cdcSupplementalLogging: Boolean): Unit = {
    val commonOptions = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1"
    )

    val _spark = spark
    import _spark.implicits._

    val userToCountryTblPath = s"$basePath/user_to_country_table"
    val countryToPopulationTblPath = s"$basePath/country_to_population_table"

    // define user_to_country_tbl and enable CDC.
    // assume that there already are some records in user_to_country_tbl.
    val userToCountryDF = Seq(
      (1, "US", "1000"), (2, "US", "1000"),
      (3, "China", "1000"), (4, "Singapore", "1000")
    ).toDF("userid", "country", "ts")
    userToCountryDF.write.format("hudi")
      .options(commonOptions)
      .option(HoodieTableConfig.CDC_ENABLED.key, "true")
      .option(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_ENABLED.key, cdcSupplementalLogging)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "userid")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(HoodieWriteConfig.TBL_NAME.key, "user_to_country")
      .save(userToCountryTblPath)

    // define country_to_population_tbl as a normal hudi table.
    // assume that there already are some records in country_to_population_tbl.
    val countryToPopulationDF = Seq(
      ("US", 200, "1000"), ("China", 50, "1000"), ("Singapore", 20, "1000")
    ).toDF("country", "population", "ts")
    countryToPopulationDF.write.format("hudi")
      .options(commonOptions)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "country")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(HoodieWriteConfig.TBL_NAME.key, "country_to_population")
      .save(countryToPopulationTblPath)

    val hadoopConf = spark.sessionState.newHadoopConf()
    val userToCountryMetaClient = HoodieTableMetaClient.builder()
      .setBasePath(userToCountryTblPath)
      .setConf(hadoopConf)
      .build()

    val inputData = new MemoryStream[(Int, String, String)](100, spark.sqlContext)
    val df = inputData.toDS().toDF("userid", "country", "ts")
    // stream1: from upstream data source to user_to_country_tbl
    val stream1 = df.writeStream
      .format("hudi")
      .foreachBatch { (batch: Dataset[Row], id: Long) =>
        batch.write.format("hudi")
          .options(commonOptions)
          .option(HoodieTableConfig.CDC_ENABLED.key, "true")
          .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "userid")
          .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
          .option(HoodieWriteConfig.TBL_NAME.key, "user_to_country")
          .mode(SaveMode.Append)
          .save(userToCountryTblPath)
      }
      .start()

    // stream2: extract the change data from user_to_country_tbl and merge into country_to_population_tbl
    val dec = typedLit(-1).expr
    val inc = typedLit(1).expr
    val zero = typedLit(0).expr
    val beforeCntExpr = If(isnull(col("bcountry")).expr, zero, dec)
    val afterCntExpr = If(isnull(col("acountry")).expr, zero, inc)
    val stream2 = spark.readStream.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.INCREMENTAL_OUTPUT_FORMAT.key, DataSourceReadOptions.INCREMENTAL_OUTPUT_FORMAT_CDC_VAL)
      .load(userToCountryTblPath)
      .writeStream
      .format("hudi")
      .foreachBatch { (batch: Dataset[Row], id: Long) =>
        val current = spark.read.format("hudi").load(countryToPopulationTblPath)
        batch
          // extract the value of the country field from the`before` field and `after` field.
          .select(
            get_json_object(col("before"), "$.country").as("bcountry"),
            get_json_object(col("after"), "$.country").as("acountry"),
            get_json_object(col("after"), "$.ts").as("ts")
          )
          // aggregate data by country, get the delta change about the population of a country.
          .withColumn("bcnt", new Column(beforeCntExpr)).withColumn("acnt", new Column(afterCntExpr))
          .select(
            explode(array(Array(
              struct(col("bcountry").as("country"), col("bcnt").as("cnt"), col("ts")),
              struct(col("acountry").as("country"), col("acnt").as("cnt"), col("ts"))): _*))
          )
          .select(col("col.country").as("country"), col("col.cnt").as("cnt"), col("col.ts").as("ts"))
          .where("country is not null").groupBy("country")
          .agg(("cnt" -> "sum"), ("ts" -> "max"))
          // join with the current data of country_to_population_tbl, get the current population for each of country.
          .join(current, Seq("country"), "left")
          .select(
            col("country"),
            new Column(
              Add(col("sum(cnt)").expr, If(isnull(col("population")).expr, Literal(0), col("population").expr))).as("population"),
            col("max(ts)").as("ts")
          )
          .write.format("hudi")
          .options(commonOptions)
          .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "country")
          .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
          .option(HoodieWriteConfig.TBL_NAME.key, "country_to_population")
          .mode(SaveMode.Append)
          .save(countryToPopulationTblPath)
      }
      .start()

    // fake upstream batch1
    inputData.addData(Seq((3, "US", "1100"), (4, "US", "1100"), (5, "US", "1100")))
    stream1.processAllAvailable()
    stream2.processAllAvailable()

    // check the change data about user_to_country_tbl for batch1
    val detailOutput1 = spark.read.format("hudi").load(userToCountryTblPath)
    assert(detailOutput1.where("country = 'US'").count() == 5)
    val ucTs1 = userToCountryMetaClient.reloadActiveTimeline().lastInstant.get.getTimestamp
    val ucDdcData1 = cdcDataFrame(userToCountryTblPath, (ucTs1.toLong - 1).toString, null)
    ucDdcData1.show(false)
    assertCDCOpCnt(ucDdcData1, 1, 2, 0)

    // check the final data of country_to_population_tbl for batch1
    val countryRes1 = Seq(
      Row("China", 50),
      Row("Singapore", 20),
      Row("US", 205)
    )
    var currentCP = spark.read.format("hudi")
      .load(countryToPopulationTblPath)
      .select("country", "population")
      .sort("country")
    checkAnswer(currentCP, countryRes1)

    // fake upstream batch2
    inputData.addData(Seq((3, "Singapore", "1200"), (7, "Canada", "1200"), (8, "Singapore", "1200")))
    stream1.processAllAvailable()
    stream2.processAllAvailable()

    // check the change data about user_to_country_tbl for batch2
    val ts2 = userToCountryMetaClient.reloadActiveTimeline().lastInstant.get.getTimestamp
    val cdcData2 = cdcDataFrame(userToCountryTblPath, (ts2.toLong - 1).toString, null)
    cdcData2.show(false)
    assertCDCOpCnt(cdcData2, 2, 1, 0)

    // check the final data of country_to_population_tbl for batch2
    val countryRes2 = Seq(
      Row("Canada", 1),
      Row("China", 50),
      Row("Singapore", 22),
      Row("US", 204)
    )
    currentCP = spark.read.format("hudi")
      .load(countryToPopulationTblPath)
      .select("country", "population")
      .sort("country")
    checkAnswer(currentCP, countryRes2)

    stream1.stop()
    stream2.stop()
  }
}
