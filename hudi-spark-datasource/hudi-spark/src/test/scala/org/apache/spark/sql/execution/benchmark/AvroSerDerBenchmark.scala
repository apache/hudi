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

import org.apache.hudi.{AvroConversionUtils, HoodieSchemaConversionUtils, HoodieSparkUtils}

import org.apache.spark.hudi.benchmark.{HoodieBenchmark, HoodieBenchmarkBase}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

/**
 * Benchmark to measure Avro SerDer performance.
 */
object AvroSerDerBenchmark extends HoodieBenchmarkBase {
  protected val spark: SparkSession = getSparkSession

  def getSparkSession: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .config("spark.driver.memory", "8G")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()

  def getDataFrame(numbers: Long): DataFrame = {
    spark.range(0, numbers).toDF("id")
      .withColumn("c1", lit("AvroSerDerBenchmark"))
      .withColumn("c2", lit(12.99d))
      .withColumn("c3", lit(1))
  }

  /**
   * Java HotSpot(TM) 64-Bit Server VM 1.8.0_92-b14 on Windows 10 10.0
   * Intel64 Family 6 Model 94 Stepping 3, GenuineIntel
   * perf avro serializer for hoodie:          Best Time(ms)   Avg Time(ms)   Stdev(ms)    Rate(M/s)   Per Row(ns)   Relative
   * ------------------------------------------------------------------------------------------------------------------------
   * serialize internalRow to avro Record               6391           6683         413          7.8         127.8       1.0X
   */
  private def avroSerializerBenchmark: Unit = {
    val benchmark = new HoodieBenchmark(s"perf avro serializer for hoodie", 50000000)
    benchmark.addCase("serialize internalRow to avro Record") { _ =>
      val df = getDataFrame(50000000)
      val schema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(df.schema, "record", "my")
      spark.sparkContext.getConf.registerAvroSchemas(schema.toAvroSchema)
      HoodieSparkUtils.createRdd(df,"record", "my", Some(schema)).foreach(f => f)
    }
    benchmark.run()
  }

  /**
   * Java HotSpot(TM) 64-Bit Server VM 1.8.0_92-b14 on Windows 10 10.0
   * Intel64 Family 6 Model 94 Stepping 3, GenuineIntel
   * perf avro deserializer for hoodie:        Best Time(ms)   Avg Time(ms)   Stdev(ms)    Rate(M/s)   Per Row(ns)   Relative
   * ------------------------------------------------------------------------------------------------------------------------
   * deserialize avro Record to internalRow             1340           1360          27          7.5         134.0       1.0X
   */
  private def avroDeserializerBenchmark: Unit = {
    val benchmark = new HoodieBenchmark(s"perf avro deserializer for hoodie", 10000000)
    val df = getDataFrame(10000000)
    val sparkSchema = df.schema
    val schema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(df.schema, "record", "my")
    val testRdd = HoodieSparkUtils.createRdd(df,"record", "my", Some(schema))
    testRdd.cache()
    testRdd.foreach(f => f)
    spark.sparkContext.getConf.registerAvroSchemas(schema.toAvroSchema)
    benchmark.addCase("deserialize avro Record to internalRow") { _ =>
      testRdd.mapPartitions { iter =>
        val schema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(sparkSchema, "record", "my")
        val avroToRowConverter = AvroConversionUtils.createAvroToInternalRowConverter(schema, sparkSchema)
        iter.map(record => avroToRowConverter.apply(record).get)
      }.foreach(f => f)
    }
    benchmark.run()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    avroSerializerBenchmark
    avroDeserializerBenchmark
  }
}
