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

package org.apache.spark.sql.hudi.feature.v2

import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.assertEquals

/**
 * Functional tests for COW snapshot reading via the DSv2 path.
 */
@Tag("functional")
class TestDSv2CowSnapshotRead extends SparkClientFunctionalTestHarness {

  override def conf: SparkConf = conf(getSparkSqlConf)

  @Test
  def testCowReadViaDataFrameApi(): Unit = {
    val path = basePath() + "/cow_df_read"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_df_read")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val df = spark.read.format("hudi_v2").load(path)
    assertEquals(3, df.count())

    val names = df.select("name").collect().map(_.getString(0)).sorted
    assertEquals(Seq("Alice", "Bob", "Charlie"), names.toSeq)

    // Verify values match DSv1 read
    val v1Names = spark.read.format("hudi").load(path)
      .select("name").collect().map(_.getString(0)).sorted
    assertEquals(v1Names.toSeq, names.toSeq)
  }

  @Test
  def testCowReadViaSqlCatalog(): Unit = {
    val tableName = "cow_sql_read"
    val tablePath = basePath() + "/" + tableName
    spark.sql(
      s"""CREATE TABLE $tableName (
         |  id INT,
         |  name STRING,
         |  amount DOUBLE,
         |  ts LONG
         |) USING hudi
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  orderingFields = 'ts'
         |)
         |LOCATION '$tablePath'
         """.stripMargin)

    spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1), (2, 'Bob', 200.0, 2), (3, 'Charlie', 300.0, 3)")

    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "true")
    try {
      val v2Df = spark.sql(s"SELECT * FROM $tableName")
      assertEquals(3, v2Df.count())

      spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "false")
      val v1Df = spark.sql(s"SELECT * FROM $tableName")
      assertEquals(3, v1Df.count())

      // Compare values (exclude meta-fields from V1)
      val v2Names = v2Df.select("name").collect().map(_.getString(0)).sorted
      val v1Names = v1Df.select("name").collect().map(_.getString(0)).sorted
      assertEquals(v1Names.toSeq, v2Names.toSeq)
    } finally {
      spark.sessionState.conf.unsetConf(DataSourceReadOptions.USE_V2_READ.key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testColumnPruning(): Unit = {
    val path = basePath() + "/cow_col_pruning"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_col_pruning")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val df = spark.read.format("hudi_v2").load(path).select("id", "name")
    assertEquals(2, df.schema.fields.length)
    assertEquals(3, df.count())

    val rows = df.collect().map(r => (r.getInt(0), r.getString(1))).sortBy(_._1)
    assertEquals(Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")), rows.toSeq)
  }

  @Test
  def testNonPartitionedTable(): Unit = {
    val path = basePath() + "/cow_non_partitioned"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_non_partitioned")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val df = spark.read.format("hudi_v2").load(path)
    assertEquals(2, df.count())

    val ids = df.select("id").collect().map(_.getInt(0)).sorted
    assertEquals(Seq(1, 2), ids.toSeq)
  }

  @Test
  def testPartitionedTable(): Unit = {
    val path = basePath() + "/cow_partitioned"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", "US"), (2, "Bob", "UK"), (3, "Charlie", "US"))
      .toDF("id", "name", "country")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_partitioned")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "country")
      .mode(SaveMode.Overwrite)
      .save(path)

    val df = spark.read.format("hudi_v2").load(path)
    assertEquals(3, df.count())

    val countries = df.select("country").collect().map(_.getString(0)).sorted
    assertEquals(Seq("UK", "US", "US"), countries.toSeq)

    val names = df.select("name").collect().map(_.getString(0)).sorted
    assertEquals(Seq("Alice", "Bob", "Charlie"), names.toSeq)
  }

  @Test
  def testMultipleCommits(): Unit = {
    val path = basePath() + "/cow_multi_commit"
    val _spark = spark
    import _spark.implicits._

    // Batch 1: insert
    Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_multi_commit")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    // Batch 2: upsert (update Alice's amount, insert Charlie)
    Seq((1, "Alice", 150.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_multi_commit")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    val df = spark.read.format("hudi_v2").load(path)
    assertEquals(3, df.count())

    val rows = df.select("id", "name", "amount").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)
    assertEquals(Seq((1, "Alice", 150.0), (2, "Bob", 200.0), (3, "Charlie", 300.0)), rows.toSeq)
  }

  @Test
  def testDsv1VsDsv2ResultComparison(): Unit = {
    val path = basePath() + "/cow_v1_v2_compare"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Charlie", 300.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_v1_v2_compare")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val v1Df = spark.read.format("hudi").load(path).select("id", "name", "amount")
    val v2Df = spark.read.format("hudi_v2").load(path).select("id", "name", "amount")

    val v1Rows = v1Df.collect().map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)
    val v2Rows = v2Df.collect().map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1)
    assertEquals(v1Rows.toSeq, v2Rows.toSeq)
  }

  @Test
  def testSelectPartitionColumnOnly(): Unit = {
    val path = basePath() + "/cow_select_part_only"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", "US"), (2, "Bob", "UK"), (3, "Charlie", "US"))
      .toDF("id", "name", "country")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_select_part_only")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "country")
      .mode(SaveMode.Overwrite)
      .save(path)

    val df = spark.read.format("hudi_v2").load(path).select("country")
    assertEquals(3, df.count())

    val countries = df.collect().map(_.getString(0)).sorted
    assertEquals(Seq("UK", "US", "US"), countries.toSeq)
  }

  @Test
  def testMultiplePartitions(): Unit = {
    val path = basePath() + "/cow_multi_partitions"
    val _spark = spark
    import _spark.implicits._

    Seq(
      (1, "Alice", "US"),
      (2, "Bob", "UK"),
      (3, "Charlie", "DE"),
      (4, "Diana", "US"),
      (5, "Eve", "UK")
    ).toDF("id", "name", "country")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_multi_partitions")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "country")
      .mode(SaveMode.Overwrite)
      .save(path)

    val df = spark.read.format("hudi_v2").load(path)
    assertEquals(5, df.count())

    val distinctCountries = df.select("country").distinct().collect().map(_.getString(0)).sorted
    assertEquals(Seq("DE", "UK", "US"), distinctCountries.toSeq)

    // Verify counts per partition
    val countsByCountry = df.groupBy("country").count().collect()
      .map(r => (r.getString(0), r.getLong(1))).toMap
    assertEquals(2L, countsByCountry("US"))
    assertEquals(2L, countsByCountry("UK"))
    assertEquals(1L, countsByCountry("DE"))
  }
}
