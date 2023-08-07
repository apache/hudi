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

package org.apache.hudi

import org.apache.spark.sql._
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.util.{AccumulatorV2}
import org.apache.spark.SparkContext

import org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.common.model.{HoodieTableType, WriteOperationType}


import org.junit.jupiter.api.Assertions.{assertEquals}
import org.junit.jupiter.api.{BeforeEach}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{EnumSource}

class TestHoodieParquetBloomFilter {

  var spark: SparkSession = _
  var sqlContext: SQLContext = _
  var sc: SparkContext = _

  def initSparkContext(): Unit = {
    val sparkConf = getSparkConfForTest(getClass.getSimpleName)

    spark = SparkSession.builder()
      .withExtensions(new HoodieSparkSessionExtension)
      .config(sparkConf)
      .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sqlContext = spark.sqlContext
  }

  @BeforeEach
  def setUp() {
    initSparkContext()
  }

  @ParameterizedTest
  @EnumSource(value = classOf[WriteOperationType], names = Array("BULK_INSERT", "INSERT", "UPSERT", "INSERT_OVERWRITE"))
  def testBloomFilter(operation: WriteOperationType): Unit = {
    // setup hadoop conf with bloom col enabled
    spark.sparkContext.hadoopConfiguration.set("parquet.bloom.filter.enabled#bloom_col", "true")
    spark.sparkContext.hadoopConfiguration.set("parquet.bloom.filter.expected.ndv#bloom_col", "2")
    // ensure nothing but bloom can trigger read skip
    spark.sql("set parquet.filter.columnindex.enabled=false")
    spark.sql("set parquet.filter.stats.enabled=false")

    val basePath = java.nio.file.Files.createTempDirectory("hoodie_bloom_source_path").toAbsolutePath.toString
    val opts = Map(
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_bloom",
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.COPY_ON_WRITE.toString,
      DataSourceWriteOptions.OPERATION.key -> operation.toString,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition"
    )
    val inputDF = spark.sql(
      """select '0' as _row_key, '1' as bloom_col, '2' as partition, '3' as ts
        |union
        |select '1', '2', '3', '4'
        |""".stripMargin)
    inputDF.write.format("hudi")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val accu = new NumRowGroupsAcc
    spark.sparkContext.register(accu)

    // this one shall skip partition scanning thanks to bloom when spark >=3
    spark.read.format("hudi").load(basePath).filter("bloom_col = '3'").foreachPartition((it: Iterator[Row]) => it.foreach(_ => accu.add(0)))
    assertEquals(if (currentSparkSupportParquetBloom()) 0 else 1, accu.value)

    // this one will trigger one partition scan
    spark.read.format("hudi").load(basePath).filter("bloom_col = '2'").foreachPartition((it: Iterator[Row]) => it.foreach(_ => accu.add(0)))
    assertEquals(1, accu.value)
  }

  def currentSparkSupportParquetBloom(): Boolean = {
    Integer.valueOf(spark.version.charAt(0)) >= 3
  }
}

class NumRowGroupsAcc extends AccumulatorV2[Integer, Integer] {
  private var _sum = 0

  override def isZero: Boolean = _sum == 0

  override def copy(): AccumulatorV2[Integer, Integer] = {
    val acc = new NumRowGroupsAcc()
    acc._sum = _sum
    acc
  }

  override def reset(): Unit = _sum = 0

  override def add(v: Integer): Unit = _sum += v

  override def merge(other: AccumulatorV2[Integer, Integer]): Unit = other match {
    case a: NumRowGroupsAcc => _sum += a._sum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: Integer = _sum
}
