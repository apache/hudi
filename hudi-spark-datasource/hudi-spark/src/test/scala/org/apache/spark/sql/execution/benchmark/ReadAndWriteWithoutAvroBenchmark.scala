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
import org.apache.hudi.{HoodieSparkRecordMerger, HoodieSparkUtils}
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.model.HoodieAvroRecordMerger
import org.apache.hudi.config.HoodieWriteConfig

import org.apache.spark.SparkConf
import org.apache.spark.hudi.benchmark.{HoodieBenchmark, HoodieBenchmarkBase}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, map, split, struct}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension

object ReadAndWriteWithoutAvroBenchmark extends HoodieBenchmarkBase {

  protected val spark: SparkSession = getSparkSession
  private val avroTable = "avro_merger_table"
  private val sparkTable = "spark_merger_table"

  def getSparkSession: SparkSession = SparkSession
    .builder()
    .master("local[4]")
    .appName(this.getClass.getCanonicalName)
    .withExtensions(new HoodieSparkSessionExtension)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("hoodie.insert.shuffle.parallelism", "2")
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

  private def createSimpleDataFrame(rowNum: Long, colNum: Int): DataFrame = {
    val df = spark.range(0, rowNum).toDF("id")
      .withColumn("t1", lit(1))
      .withColumn("d1", lit(12.99d))
    (0 to colNum).foreach(i => df.withColumn(s"c$i", lit(s"c$i")))
    df
  }

  private def createComplexDataFrame(rowNum: Long, colNum: Int): DataFrame = {
    var df = spark.range(0, rowNum).toDF("id")
      .withColumn("t1", lit(1))
      .withColumn("d1", lit(12.99d))
      .withColumn("s1", lit("ReadAndWriteWithoutAvroBenchmark1 ReadAndWriteWithoutAvroBenchmark1 ReadAndWriteWithoutAvroBenchmark1 ReadAndWriteWithoutAvroBenchmark1 ReadAndWriteWithoutAvroBenchmark1"))
      .withColumn("s2", lit("ReadAndWriteWithoutAvroBenchmark2 ReadAndWriteWithoutAvroBenchmark2 ReadAndWriteWithoutAvroBenchmark2 ReadAndWriteWithoutAvroBenchmark2 ReadAndWriteWithoutAvroBenchmark2"))
      .withColumn("s3", lit("ReadAndWriteWithoutAvroBenchmark3 ReadAndWriteWithoutAvroBenchmark3 ReadAndWriteWithoutAvroBenchmark3 ReadAndWriteWithoutAvroBenchmark3 ReadAndWriteWithoutAvroBenchmark3"))
    for(i <- 0 to colNum) {
      df = df.withColumn(s"struct$i", struct(col("s1").as("st1"), col("s2").as("st2"), col("s3").as("st3")))
      df = df.withColumn(s"map$i", map(col("s1"), col("s1"), col("s2"), col("s2"), col("s3"), col("s3")))
      df = df.withColumn(s"array$i", split(col("s1"), " "))
    }
    df
  }

  private def prepareHoodieTable(tableName: String, path: String, tableType: String, mergerType: String, df: DataFrame): Unit = {
    df.createOrReplaceTempView("input_df")
    if (spark.catalog.tableExists(tableName)) {
      spark.sql(s"drop table if exists $tableName")
    }
    spark.sql(
      s"""
         |create table $tableName using hudi
         |tblproperties(
         |  primaryKey = 'id',
         |  type = '$tableType',
         |  ${HoodieWriteConfig.MERGER_IMPLS.key} = '$mergerType',
         |  ${HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key} = 'parquet')
         |location '$path'
         |As
         |select * from input_df
   """.stripMargin)
  }

  private def overwriteBenchmark(): Unit = {
    val df = createComplexDataFrame(100000, 5)
    val benchmark = new HoodieBenchmark("pref insert overwrite", 100000, 3)
    Seq(classOf[HoodieAvroRecordMerger].getName, classOf[HoodieSparkRecordMerger].getName).zip(Seq(avroTable, sparkTable)).foreach {
      case (merger, tableName) => benchmark.addCase(merger) { _ =>
        withTempDir { f =>
          prepareHoodieTable(tableName, new Path(f.getCanonicalPath, tableName).toUri.toString, "mor", merger, df)
        }
      }
    }
    benchmark.run()
  }

  private def upsertThenReadBenchmark(): Unit = {
    val avroMerger = classOf[HoodieAvroRecordMerger].getName
    val sparkMerger = classOf[HoodieSparkRecordMerger].getName
    val df = createComplexDataFrame(100000, 5)
    withTempDir { avroPath =>
      withTempDir { sparkPath =>
        val upsertBenchmark = new HoodieBenchmark("pref upsert", 100000, 3)
        prepareHoodieTable(avroTable, new Path(avroPath.getCanonicalPath, avroTable).toUri.toString, "mor", avroMerger, df)
        prepareHoodieTable(sparkTable, new Path(sparkPath.getCanonicalPath, sparkTable).toUri.toString, "mor", sparkMerger, df)
        df.createOrReplaceTempView("input_df")
        Seq(avroMerger, sparkMerger).zip(Seq(avroTable, sparkTable)).foreach {
          case (merger, tableName) => upsertBenchmark.addCase(merger) { _ =>
            spark.sql(s"insert into $tableName select * from input_df")
          }
        }
        upsertBenchmark.run()

        val readBenchmark = new HoodieBenchmark("pref read", 100000, 3)
        Seq(avroMerger, sparkMerger).zip(Seq(avroTable, sparkTable)).foreach {
          case (merger, tableName) => readBenchmark.addCase(merger) { _ =>
            spark.sql(s"select * from $tableName").collect()
          }
        }
        readBenchmark.run()

      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    overwriteBenchmark()
    upsertThenReadBenchmark()
  }
}
