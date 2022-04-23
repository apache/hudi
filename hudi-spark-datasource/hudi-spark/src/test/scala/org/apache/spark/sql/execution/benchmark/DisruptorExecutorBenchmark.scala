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

package org.apache.spark.sql.execution.benchmark

import java.sql.{Date, Timestamp}

import org.apache.hadoop.fs.Path
import org.apache.hudi.HoodieSparkUtils
import org.apache.spark.SparkConf
import org.apache.spark.hudi.benchmark.{HoodieBenchmark, HoodieBenchmarkBase}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, RowFactory, SparkSession}

import scala.util.Random

object DisruptorExecutorBenchmark extends HoodieBenchmarkBase {

  protected val spark: SparkSession = getSparkSession

  def getSparkSession: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName(this.getClass.getCanonicalName)
    .withExtensions(new HoodieSparkSessionExtension)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("hoodie.insert.shuffle.parallelism", "2")
    .config("hoodie.datasource.write.operation", "bulk_insert")
    .config("hoodie.datasource.write.row.writer.enable", "false")
    .config("hoodie.bulkinsert.shuffle.parallelism", "10")
    .config("hoodie.write.executor.type", "DISRUPTOR_EXECUTOR")
    .config("hoodie.upsert.shuffle.parallelism", "2")
    .config("hoodie.delete.shuffle.parallelism", "2")
    .config("spark.sql.session.timeZone", "CTT")
    .config(sparkConf())
    .getOrCreate()

  def sparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
    if (HoodieSparkUtils.gteqSpark3_2) {
      sparkConf.set("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    }
    sparkConf
  }

  def createHoodieCowTable(tableName: String, tablePath: String) = {
    createDataFrame(10000000).registerTempTable("ds")
    spark.sql(
      s"""
         |create table $tableName using hudi
         |tblproperties(primaryKey = 'c1')
         |location '${tablePath}'
         |As
         |select * from ds
       """.stripMargin)
  }

  private def createDataFrame(number: Int): DataFrame = {
    val schema = new StructType()
      .add("c1", IntegerType)
      .add("c11", IntegerType)
      .add("c12", IntegerType)
      .add("c2", StringType)
      .add("c3", DecimalType(38, 10))
      .add("c4", TimestampType)
      .add("c5", ShortType)
      .add("c6", DateType)
      .add("c7", BinaryType)
      .add("c9", ByteType)

    val rdd = spark.sparkContext.parallelize(0 to number, 2).map { item =>
      val c1 = Integer.valueOf(item)
      val c11 = Integer.valueOf(Random.nextInt(10000))
      val c12 = Integer.valueOf(Random.nextInt(10000))
      val c2 = s" ${item}abc"
      val c3 = new java.math.BigDecimal(s"${Random.nextInt(1000)}.${Random.nextInt(100)}")
      val c4 = new Timestamp(System.currentTimeMillis())
      val c5 = java.lang.Short.valueOf(s"${16}")
      val c6 = Date.valueOf(s"${2020}-${item % 11 + 1}-${item % 28 + 1}")
      val c7 = Array(item).map(_.toByte)
      val c8 = java.lang.Byte.valueOf("9")
      RowFactory.create(c1, c11, c12, c2, c3, c4, c5, c6, c7, c8)
    }
    spark.createDataFrame(rdd, schema)
  }

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  private def cowTableDisruptorExecutorBenchmark(tableName: String = "executorBenchmark"): Unit = {
    withTempDir {f =>
      withTempTable(tableName) {
        val benchmark = new HoodieBenchmark("COW Ingestion", 10000000)
        benchmark.addCase("Disruptor Executor") { _ =>
          createHoodieCowTable(tableName, new Path(f.getCanonicalPath, tableName).toUri.toString)
        }
        benchmark.run()
      }
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    cowTableDisruptorExecutorBenchmark()
  }
}
