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

import org.apache.hadoop.fs.Path
import org.apache.hudi.HoodieSparkUtils
import org.apache.spark.SparkConf
import org.apache.spark.hudi.benchmark.{HoodieBenchmark, HoodieBenchmarkBase}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, RowFactory, SaveMode, SparkSession}

import scala.util.Random

object BoundInMemoryExecutorBenchmark extends HoodieBenchmarkBase {

  protected val spark: SparkSession = getSparkSession

  val recordNumber = 1000000

  def getSparkSession: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName(this.getClass.getCanonicalName)
    .withExtensions(new HoodieSparkSessionExtension)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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

  private def createDataFrame(number: Int): DataFrame = {
    val schema = new StructType()
      .add("c1", IntegerType)
      .add("c2", StringType)

    val rdd = spark.sparkContext.parallelize(0 to number, 2).map { item =>
      val c1 = Integer.valueOf(item)
      val c2 = s"abc"
      RowFactory.create(c1, c2)
    }
    spark.createDataFrame(rdd, schema)
  }

  /**
   * OpenJDK 64-Bit Server VM 1.8.0_161-b14 on Linux 3.10.0-693.21.1.el7.x86_64
   * Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz
   * COW Ingestion:                            Best Time(ms)   Avg Time(ms)   Stdev(ms)    Rate(M/s)   Per Row(ns)   Relative
   * ------------------------------------------------------------------------------------------------------------------------
   * BoundInMemory Executor                             5629           5765         192          0.2        5628.9       1.0X
   * Disruptor Executor                                 2772           2862         127          0.4        2772.2       2.0X
   *
   */
  private def cowTableDisruptorExecutorBenchmark(tableName: String = "executorBenchmark"): Unit = {
    val df = createDataFrame(recordNumber)
    withTempDir {f =>
      val benchmark = new HoodieBenchmark("COW Ingestion", recordNumber)
      benchmark.addCase("BoundInMemory Executor") { _ =>
        val finalTableName = tableName + Random.nextInt(10000)
        df.write.format("hudi")
          .mode(SaveMode.Overwrite)
          .option("hoodie.datasource.write.recordkey.field", "c1")
          .option("hoodie.datasource.write.partitionpath.field", "c2")
          .option("hoodie.table.name", finalTableName)
          .option("hoodie.metadata.enable", "false")
          .option("hoodie.clean.automatic", "false")
          .option("hoodie.bulkinsert.sort.mode", "NONE")
          .option("hoodie.insert.shuffle.parallelism", "2")
          .option("hoodie.datasource.write.operation", "bulk_insert")
          .option("hoodie.datasource.write.row.writer.enable", "false")
          .option("hoodie.bulkinsert.shuffle.parallelism", "1")
          .option("hoodie.upsert.shuffle.parallelism", "2")
          .option("hoodie.delete.shuffle.parallelism", "2")
          .option("hoodie.populate.meta.fields", "false")
          .option("hoodie.table.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator")
          .save(new Path(f.getCanonicalPath, finalTableName).toUri.toString)
      }

      benchmark.addCase("Disruptor Executor") { _ =>
        val finalTableName = tableName + Random.nextInt(10000)
        df.write.format("hudi")
          .mode(SaveMode.Overwrite)
          .option("hoodie.datasource.write.recordkey.field", "c1")
          .option("hoodie.datasource.write.partitionpath.field", "c2")
          .option("hoodie.table.name", finalTableName)
          .option("hoodie.metadata.enable", "false")
          .option("hoodie.clean.automatic", "false")
          .option("hoodie.bulkinsert.sort.mode", "NONE")
          .option("hoodie.insert.shuffle.parallelism", "2")
          .option("hoodie.datasource.write.operation", "bulk_insert")
          .option("hoodie.datasource.write.row.writer.enable", "false")
          .option("hoodie.bulkinsert.shuffle.parallelism", "1")
          .option("hoodie.upsert.shuffle.parallelism", "2")
          .option("hoodie.delete.shuffle.parallelism", "2")
          .option("hoodie.write.executor.type", "DISRUPTOR")
          .option("hoodie.populate.meta.fields", "false")
          .option("hoodie.table.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator")

          .save(new Path(f.getCanonicalPath, finalTableName).toUri.toString)
      }
      benchmark.run()
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    cowTableDisruptorExecutorBenchmark()
  }
}
