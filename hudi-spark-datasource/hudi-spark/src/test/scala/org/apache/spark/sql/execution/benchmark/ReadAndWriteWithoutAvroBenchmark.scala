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
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.model.HoodieAvroRecordMerger
import org.apache.hudi.config.HoodieCompactionConfig
import org.apache.hudi.{HoodieSparkRecordMerger, HoodieSparkUtils}

import org.apache.spark.SparkConf
import org.apache.spark.hudi.benchmark.{HoodieBenchmark, HoodieBenchmarkBase}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadAndWriteWithoutAvroBenchmark extends HoodieBenchmarkBase {

  protected val spark: SparkSession = getSparkSession
  private val avroTable = "avro_merger_table"
  private val sparkTable = "spark_merger_table"

  def getSparkSession: SparkSession = SparkSession
    .builder()
    .master("local[4]")
    .appName(this.getClass.getCanonicalName)
    .withExtensions(new HoodieSparkSessionExtension)
    .config("spark.driver.memory", "4G")
    .config("spark.executor.memory", "4G")
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

  private def createComplexDataFrame(rowNum: Long): DataFrame = {
    var df = spark.range(0, rowNum).toDF("id")
      .withColumn("t1", lit(1))
      .withColumn("d1", lit(12.99d))
      .withColumn("s1", lit("s1"))
      .withColumn("s2", lit("s2"))
      .withColumn("s3", lit("s3"))
    for (i <- 0 to 1) {
      df = df.withColumn(s"struct$i", struct(col("s1").as("st1"), col("s2").as("st2"), col("s3").as("st3")))
        .withColumn(s"map$i", map(col("s1"), col("s2")))
        .withColumn(s"array$i", array(col("s1")))
    }
    df
  }

  private def prepareHoodieTable(tableName: String, path: String, tableType: String, mergerImpl: String, df: DataFrame): Unit = {
    df.collect()
    df.createOrReplaceTempView("input_df")
    if (spark.catalog.tableExists(tableName)) {
      spark.sql(s"drop table if exists $tableName")
    }
    spark.sql(s"set ${HoodieWriteConfig.MERGER_IMPLS.key} = $mergerImpl")
    spark.sql(
      s"""
         |create table $tableName(
         |id long,
         |t1 int,
         |d1 double,
         |s1 string,
         |s2 string,
         |s3 string,
         |struct0 struct<st1:string, st2:string, st3:string>,
         |map0 map<string, string>,
         |array0 array<string>,
         |struct1 struct<st1:string, st2:string, st3:string>,
         |map1 map<string, string>,
         |array1 array<string>
         |) using hudi
         |tblproperties(
         |  primaryKey = 'id',
         |  preCombineField = 's1',
         |  type = '$tableType',
         |  ${HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key} = 'parquet',
         |  ${HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key()} = '10')
         |location '$path'
   """.stripMargin)
    spark.sql(s"insert overwrite table $tableName select * from input_df")
  }

  /**
   * OpenJDK 64-Bit Server VM 1.8.0_345-b01 on Mac OS X 12.4
   *  Apple M1 Pro
   *  pref insert overwrite:                               Best Time(ms)   Avg Time(ms)   Stdev(ms)    Rate(M/s)   Per Row(ns)   Relative
   *  -----------------------------------------------------------------------------------------------------------------------------------
   *  org.apache.hudi.common.model.HoodieAvroRecordMerger          16714          17107         353          0.1       16714.5       1.0X
   *  org.apache.hudi.HoodieSparkRecordMerger                      12654          13924        1100          0.1       12653.8       1.3X
   */
  private def overwriteBenchmark(): Unit = {
    val df = createComplexDataFrame(1000000)
    val benchmark = new HoodieBenchmark("pref insert overwrite", 1000000, 3)
    Seq(classOf[HoodieAvroRecordMerger].getName, classOf[HoodieSparkRecordMerger].getName).zip(Seq(avroTable, sparkTable)).foreach {
      case (merger, tableName) => benchmark.addCase(merger) { _ =>
        withTempDir { f =>
          prepareHoodieTable(tableName, new Path(f.getCanonicalPath, tableName).toUri.toString, "mor", merger, df)
        }
      }
    }
    benchmark.run()
  }

  /**
   * Java HotSpot(TM) 64-Bit Server VM 1.8.0_211-b12 on Mac OS X 10.16
   * Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
   * pref upsert:                                         Best Time(ms)   Avg Time(ms)   Stdev(ms)    Rate(M/s)   Per Row(ns)   Relative
   * -----------------------------------------------------------------------------------------------------------------------------------
   * org.apache.hudi.common.model.HoodieAvroRecordMerger           6108           6383         257          0.0      610785.6       1.0X
   * org.apache.hudi.HoodieSparkRecordMerger                       4833           5468         614          0.0      483300.0       1.3X
   *
   * Java HotSpot(TM) 64-Bit Server VM 1.8.0_211-b12 on Mac OS X 10.16
   * Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
   * pref read:                                           Best Time(ms)   Avg Time(ms)   Stdev(ms)    Rate(M/s)   Per Row(ns)   Relative
   * -----------------------------------------------------------------------------------------------------------------------------------
   * org.apache.hudi.common.model.HoodieAvroRecordMerger            813            818           8          0.0       81302.1       1.0X
   * org.apache.hudi.HoodieSparkRecordMerger                        604            616          18          0.0       60430.1       1.3X
   */
  private def upsertThenReadBenchmark(): Unit = {
    val avroMergerImpl = classOf[HoodieAvroRecordMerger].getName
    val sparkMergerImpl = classOf[HoodieSparkRecordMerger].getName
    val df = createComplexDataFrame(10000)
    withTempDir { avroPath =>
      withTempDir { sparkPath =>
        val upsertBenchmark = new HoodieBenchmark("pref upsert", 10000, 3)
        prepareHoodieTable(avroTable, new Path(avroPath.getCanonicalPath, avroTable).toUri.toString, "mor", avroMergerImpl, df)
        prepareHoodieTable(sparkTable, new Path(sparkPath.getCanonicalPath, sparkTable).toUri.toString, "mor", sparkMergerImpl, df)
        Seq(avroMergerImpl, sparkMergerImpl).zip(Seq(avroTable, sparkTable)).foreach {
          case (mergerImpl, tableName) => upsertBenchmark.addCase(mergerImpl) { _ =>
            spark.sql(s"set ${HoodieWriteConfig.MERGER_IMPLS.key} = $mergerImpl")
            spark.sql(s"update $tableName set s1 = 's1_new_1' where id > 0")
          }
        }
        upsertBenchmark.run()

        val readBenchmark = new HoodieBenchmark("pref read", 10000, 3)
        Seq(avroMergerImpl, sparkMergerImpl).zip(Seq(avroTable, sparkTable)).foreach {
          case (mergerImpl, tableName) => readBenchmark.addCase(mergerImpl) { _ =>
            spark.sql(s"set ${HoodieWriteConfig.MERGER_IMPLS.key} = $mergerImpl")
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
